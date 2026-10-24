import asyncio
import os
import re
import secrets
import string
import sys
import threading
import time
import uuid
from collections import defaultdict
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional

from fastapi import (
    Depends,
    FastAPI,
    Header,
    HTTPException,
    Query,
    Request,
    Response,
    WebSocket,
    WebSocketDisconnect,
    status,
)
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field, field_validator

from app.logging_config import (
    clear_logs,
    get_recent_logs,
    request_id_ctx,
    setup_logging,
)
from app.metrics import metrics
from app.storage import (
    assemble_file,
    assemble_file_async,
    cleanup_expired_transfers,
    cleanup_transfer_chunks,
    create_transfer,
    get_chunk,
    get_transfer,
    init_db,
    list_transfers,
    sanitize_filename,
    save_chunk,
    update_transfer_status,
)
from app.transfer import (
    get_transfer_telemetry_summary,
    handle_websocket_message,
    manager,
    simulator,
)

APP_VERSION = os.environ.get("CHUNKSTREAM_VERSION", "1.0.0")
APP_START_TIME = time.time()
ENVIRONMENT = os.environ.get("ENVIRONMENT", "development").lower()
GIT_COMMIT = (
    os.environ.get("GIT_COMMIT")
    or os.environ.get("RENDER_GIT_COMMIT")
    or "HEAD"
)
MAX_CONCURRENT_TRANSFERS = int(os.environ.get("MAX_CONCURRENT_TRANSFERS", "100"))
MAX_CHUNK_UPLOAD_BYTES = int(os.environ.get("MAX_CHUNK_UPLOAD_BYTES", 10 * 1024 * 1024))  # 10 MB

# Initialize structured logging
logger = setup_logging(
    log_level=os.environ.get("LOG_LEVEL", "INFO"),
    log_dir=os.environ.get("LOG_DIR", "logs"),
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    logger.info("Starting ChunkStream v%s (commit: %s, env: %s)", APP_VERSION, GIT_COMMIT, ENVIRONMENT)
    init_db()

    # Background task for periodic cleanup of expired transfers (every hour)
    stop_event = asyncio.Event()

    async def _cleanup_worker():
        while not stop_event.is_set():
            try:
                await asyncio.sleep(3600)
                await asyncio.to_thread(cleanup_expired_transfers, 24)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.warning("Error during periodic transfer cleanup: %s", exc)

    cleanup_task = asyncio.create_task(_cleanup_worker())

    yield

    # Shutdown: graceful drainage of active WebSocket transfers
    logger.info("Initiating graceful shutdown: draining active WebSocket transfers...")
    stop_event.set()
    cleanup_task.cancel()
    try:
        await manager.close_all(reason="Server is shutting down")
    except Exception as exc:
        logger.warning("Error during WebSocket drainage: %s", exc)
    logger.info("ChunkStream shutdown complete")


app = FastAPI(
    title="ChunkStream",
    description="Resumable chunked file-transfer service designed for testing ProdPilot",
    version=APP_VERSION,
    lifespan=lifespan,
)

# CORS configuration: Fix wildcard + credentials spec conflict
allowed_origins_env = os.environ.get("ALLOWED_ORIGINS", "*")
allowed_origins = [o.strip() for o in allowed_origins_env.split(",") if o.strip()]
is_wildcard = "*" in allowed_origins

app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_credentials=False if is_wildcard else True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Rate Limiting & Security In-Memory State
_rate_limit_records: Dict[str, List[float]] = defaultdict(list)
_rate_limit_lock = threading.Lock()
RATE_LIMIT_PER_MINUTE = int(os.environ.get("RATE_LIMIT_PER_MINUTE", "600"))
RATE_LIMIT_ENABLED = os.environ.get("RATE_LIMIT_ENABLED", "true").lower() in ("1", "true", "yes")


# Security & Access Verification Dependencies
def verify_admin_or_sim_allowed(
    request: Request,
    x_admin_key: Optional[str] = Header(None, alias="X-Admin-Key"),
    admin_key: Optional[str] = Query(None),
):
    """
    Guards simulation and diagnostic control endpoints:
    - In production (ENVIRONMENT=production), ADMIN_API_KEY is required.
    - If ADMIN_API_KEY is configured in env, it is strictly validated.
    """
    configured_key = os.environ.get("ADMIN_API_KEY")
    provided_key = x_admin_key or admin_key

    if configured_key:
        if not provided_key or not secrets.compare_digest(provided_key, configured_key):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Forbidden: Invalid or missing X-Admin-Key",
            )
    elif ENVIRONMENT == "production" and not os.environ.get("ALLOW_SIM_ENDPOINTS", "").lower() in ("1", "true", "yes"):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Forbidden: Simulation endpoints disabled in production. Set ADMIN_API_KEY or ALLOW_SIM_ENDPOINTS=true",
        )


def verify_crash_allowed(
    request: Request,
    x_admin_key: Optional[str] = Header(None, alias="X-Admin-Key"),
    admin_key: Optional[str] = Query(None),
    confirm: Optional[str] = Query(None),
    force: Optional[str] = Query(None),
):
    """
    Guards destructive crash simulation endpoints (F2, OOM, Deadlock):
    Allows execution if ?confirm=true, ?force=true, ALLOW_SIM_CRASH=true, or valid ADMIN_API_KEY.
    """
    configured_key = os.environ.get("ADMIN_API_KEY")
    provided_key = x_admin_key or admin_key
    allow_crash = os.environ.get("ALLOW_SIM_CRASH", "").lower() in ("1", "true", "yes")
    is_confirmed = (confirm or "").lower() in ("1", "true", "yes") or (force or "").lower() in ("1", "true", "yes")

    if configured_key and provided_key and secrets.compare_digest(provided_key, configured_key):
        return True
    if allow_crash or is_confirmed:
        return True

    raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail="Forbidden: Destructive crash endpoint disabled. Pass ?confirm=true, set ALLOW_SIM_CRASH=true, or provide valid X-Admin-Key",
    )


# Middlewares: Request ID, Access Logging, Rate Limiting, Security Headers
@app.middleware("http")
async def request_lifecycle_middleware(request: Request, call_next):
    req_id = request.headers.get("X-Request-ID") or uuid.uuid4().hex[:12]
    token = request_id_ctx.set(req_id)
    request.state.request_id = req_id
    start_time = time.time()

    client_ip = request.client.host if request.client else "unknown"

    # Rate limiting (exempt health checks and admin key holders)
    path = request.url.path
    is_health = path in ("/health", "/api/health", "/metrics")
    is_test_client = client_ip in ("testclient", "127.0.0.1", "localhost")

    if RATE_LIMIT_ENABLED and not is_health and not is_test_client:
        now = time.time()
        with _rate_limit_lock:
            # Prune records older than 60 seconds
            history = [t for t in _rate_limit_records[client_ip] if now - t < 60]
            if len(history) >= RATE_LIMIT_PER_MINUTE:
                _rate_limit_records[client_ip] = history
                return JSONResponse(
                    status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                    content={"detail": "Too many requests. Please slow down."},
                    headers={"Retry-After": "60"},
                )
            history.append(now)
            _rate_limit_records[client_ip] = history

    try:
        response: Response = await call_next(request)
    except Exception as exc:
        latency_ms = (time.time() - start_time) * 1000
        logger.exception("[REQ:%s] %s %s ERROR (%.1fms): %s", req_id, request.method, path, latency_ms, exc)
        metrics.record_request(request.method, path, 500)
        request_id_ctx.reset(token)
        raise

    latency_ms = (time.time() - start_time) * 1000
    metrics.record_request(request.method, path, response.status_code)

    # Security Headers & Correlation Headers
    response.headers["X-Request-ID"] = req_id
    response.headers["X-Response-Time-Ms"] = f"{latency_ms:.2f}"
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "SAMEORIGIN"
    response.headers["Content-Security-Policy"] = (
        "default-src 'self'; script-src 'self' 'unsafe-inline'; "
        "style-src 'self' 'unsafe-inline'; font-src 'self' data:; "
        "connect-src 'self' ws: wss:;"
    )

    if not is_health:
        logger.info("[REQ:%s] %s %s -> %d (%.1fms)", req_id, request.method, path, response.status_code, latency_ms)

    request_id_ctx.reset(token)
    return response


def generate_transfer_id() -> str:
    """Generates a clean collision-resistant transfer code."""
    for _ in range(50):
        code = f"{secrets.randbelow(9000) + 1000}"
        if not get_transfer(code):
            return code
    # Fallback to 6 alphanumeric characters if 4-digit space is dense
    alphabet = string.ascii_uppercase + string.digits
    for _ in range(50):
        code = "".join(secrets.choice(alphabet) for _ in range(6))
        if not get_transfer(code):
            return code
    raise RuntimeError("Unable to generate unique transfer ID")


# Pydantic Models with Robust Validation
class CreateTransferRequest(BaseModel):
    filename: str
    filesize: int = Field(gt=0, le=10 * 1024 * 1024 * 1024, description="File size up to 10 GB")
    chunk_size: int = Field(default=262144, ge=1, le=10 * 1024 * 1024, description="Chunk size: 1 byte to 10 MB")
    total_chunks: int = Field(gt=0, le=500_000, description="Max 500,000 chunks")
    mime_type: Optional[str] = "application/octet-stream"
    checksum: Optional[str] = None
    transfer_id: Optional[str] = None

    @field_validator("filename")
    @classmethod
    def validate_filename(cls, v: str) -> str:
        s = sanitize_filename(v)
        if not s:
            raise ValueError("Invalid filename")
        return s


class FailureScenarioRequest(BaseModel):
    scenario: str = Field(
        ...,
        description="Scenario key: 'none', 'low_error', 'high_error', 'resume_failure', 'storage_error', 'unhealthy'",
    )


# --- Health, Metrics & Diagnostics ---


@app.get("/health", tags=["Health"])
@app.get("/api/health", tags=["Health"])
async def health_check():
    """
    Clean liveness & health check endpoint used by Render and monitors.
    Returns service health without leaking internal simulation internals.
    """
    if simulator.force_health_unhealthy:
        logger.critical("[HEALTH CHECK FAILURE] Simulated 503 Unhealthy status returned to monitor")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Simulated Service Degradation: Storage cluster or internal worker unreachable",
        )

    active_ws = manager.get_total_active_connections()
    metrics.set_active_websockets(active_ws)

    return {
        "status": "healthy",
        "service": "chunkstream",
        "version": APP_VERSION,
        "uptime_seconds": int(time.time() - APP_START_TIME),
        "active_connections": active_ws,
        "active_transfers": len(manager.active_connections),
    }


@app.get("/metrics", tags=["Observability"])
async def prometheus_metrics():
    """Exposes standard Prometheus exposition text metrics."""
    metrics.set_active_websockets(manager.get_total_active_connections())
    return Response(
        content=metrics.generate_prometheus_text(),
        media_type="text/plain; version=0.0.4; charset=utf-8",
    )


@app.get("/api/version", tags=["Health"])
async def get_version():
    """Returns application version, commit SHA, and environment information."""
    return {
        "version": APP_VERSION,
        "service": "chunkstream",
        "environment": ENVIRONMENT,
        "git_commit": GIT_COMMIT,
        "uptime": round(time.time() - APP_START_TIME, 2),
    }


@app.get("/api/logs", tags=["Logs"])
async def get_logs(limit: int = 100, min_level: Optional[str] = None):
    """Retrieves recent in-memory logs for ProdPilot and UI live viewer (single lookup)."""
    logs = get_recent_logs(limit=limit, min_level=min_level)
    return {
        "count": len(logs),
        "logs": logs,
    }


@app.get("/api/debug/simulation", tags=["Simulation"], dependencies=[Depends(verify_admin_or_sim_allowed)])
async def get_simulation_state():
    """Inspects detailed failure simulator configuration."""
    return simulator.get_status()


# --- Transfer Endpoints ---


@app.post("/api/transfers", tags=["Transfer"])
async def init_transfer(payload: CreateTransferRequest):
    """Initializes a new transfer session and persists metadata in SQLite."""
    transfer_id = payload.transfer_id or generate_transfer_id()

    # Verify if transfer already exists
    existing = get_transfer(transfer_id)
    if existing:
        return existing

    record = create_transfer(
        transfer_id=transfer_id,
        filename=payload.filename,
        filesize=payload.filesize,
        chunk_size=payload.chunk_size,
        total_chunks=payload.total_chunks,
        mime_type=payload.mime_type,
        checksum=payload.checksum,
    )
    return record


@app.get("/api/transfers", tags=["Transfer"])
async def list_all_transfers(limit: int = 30):
    """Lists recent transfers with progress summaries."""
    return list_transfers(limit=limit)


@app.get("/api/transfers/{transfer_id}", tags=["Transfer"])
async def get_transfer_details(transfer_id: str):
    """Fetches details of a transfer, including acknowledged and missing chunk sets."""
    record = get_transfer(transfer_id)
    if not record:
        raise HTTPException(status_code=404, detail=f"Transfer {transfer_id} not found")
    return record


@app.get("/api/transfers/{transfer_id}/metrics", tags=["Transfer"])
async def get_transfer_metrics(transfer_id: str):
    """Fetches real-time transfer metrics and telemetry summary."""
    record = get_transfer(transfer_id)
    if not record:
        raise HTTPException(status_code=404, detail=f"Transfer {transfer_id} not found")
    summary = get_transfer_telemetry_summary(transfer_id)
    return {"transfer_id": transfer_id, "metrics": summary}


@app.post("/api/transfers/{transfer_id}/chunks/{chunk_index}", tags=["Transfer"])
async def upload_chunk_http(
    transfer_id: str,
    chunk_index: int,
    request: Request,
    checksum: Optional[str] = Query(None),
):
    """
    HTTP chunk upload endpoint.
    Includes chunk upload size guards and failure injection.
    """
    if simulator.simulate_storage_error:
        logger.error(
            "HTTP chunk upload failed: Disk I/O simulated error on transfer %s chunk %d",
            transfer_id,
            chunk_index,
        )
        raise HTTPException(status_code=500, detail="Disk I/O error (Simulated)")

    if simulator.should_fail_chunk(chunk_index):
        logger.warning(
            "HTTP chunk %d dropped for transfer %s (Simulated error rate)",
            chunk_index,
            transfer_id,
        )
        raise HTTPException(status_code=503, detail="Simulated chunk transmission failure")

    data = await request.body()
    if len(data) > MAX_CHUNK_UPLOAD_BYTES:
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail=f"Chunk exceeds max upload limit of {MAX_CHUNK_UPLOAD_BYTES} bytes",
        )

    success, msg = save_chunk(transfer_id, chunk_index, data, expected_checksum=checksum)
    if not success:
        raise HTTPException(status_code=400, detail=msg)

    metrics.record_chunk("success")
    return {
        "status": "saved",
        "transfer_id": transfer_id,
        "chunk_index": chunk_index,
        "bytes_received": len(data),
    }


@app.post("/api/transfers/{transfer_id}/assemble", tags=["Transfer"])
async def trigger_assembly(transfer_id: str):
    """Triggers file reassembly in background thread pool once all chunks are present."""
    success, out_path, err = await assemble_file_async(transfer_id)
    if not success:
        logger.error("Assembly failed for %s: %s", transfer_id, err)
        raise HTTPException(status_code=400, detail=err)

    return {
        "status": "completed",
        "transfer_id": transfer_id,
        "download_url": f"/api/transfers/{transfer_id}/download",
    }


@app.get("/api/transfers/{transfer_id}/download", tags=["Transfer"])
async def download_file(transfer_id: str):
    """Downloads the assembled file with path traversal validation."""
    record = get_transfer(transfer_id)
    if not record:
        raise HTTPException(status_code=404, detail="Transfer not found")

    safe_name = sanitize_filename(record["filename"])
    downloads_base = os.path.join(os.environ.get("CHUNKSTREAM_DATA_DIR", "data"), "downloads")
    out_dir = os.path.join(downloads_base, transfer_id)
    out_path = os.path.join(out_dir, safe_name)

    # Path traversal validation
    abs_base = os.path.abspath(downloads_base)
    abs_path = os.path.abspath(out_path)
    if not abs_path.startswith(abs_base + os.sep):
        raise HTTPException(status_code=400, detail="Invalid transfer path detected")

    if not os.path.isfile(out_path):
        success, assembled_path, err = await assemble_file_async(transfer_id)
        if not success or not assembled_path:
            raise HTTPException(
                status_code=400, detail=f"File not ready for download: {err or 'Missing chunks'}"
            )
        out_path = assembled_path

    return FileResponse(
        out_path,
        media_type=record.get("mime_type") or "application/octet-stream",
        filename=safe_name,
    )


# --- Failure Simulation & Control Panel Endpoints ---


@app.post("/api/test/fail", tags=["Simulation"], dependencies=[Depends(verify_admin_or_sim_allowed)])
async def trigger_failure_scenario(payload: FailureScenarioRequest):
    """Configures simulated failure mode (F1, F3, F4, F6, etc.)."""
    result = simulator.set_scenario(payload.scenario)
    return {"scenario": payload.scenario, "status": result}


@app.post("/api/test/reset", tags=["Simulation"], dependencies=[Depends(verify_admin_or_sim_allowed)])
async def reset_failure_scenarios():
    """Resets simulator to normal operation."""
    result = simulator.set_scenario("none")
    return {"scenario": "none", "status": result}


@app.post("/api/test/disconnect-receiver", tags=["Simulation"], dependencies=[Depends(verify_admin_or_sim_allowed)])
async def trigger_receiver_disconnect(transfer_id: str = Query(...)):
    """Simulates receiver disconnection (F5)."""
    record = get_transfer(transfer_id)
    if not record:
        raise HTTPException(status_code=404, detail=f"Transfer {transfer_id} not found")
    count = await manager.force_disconnect_role(transfer_id, "receiver")
    return {"transfer_id": transfer_id, "disconnected_receivers": count}


@app.api_route("/api/test/crash", methods=["GET", "POST"], tags=["Simulation"], dependencies=[Depends(verify_crash_allowed)])
async def crash_server():
    """
    F2: Intentional Server Crash.
    Emits critical log with traceback and terminates process after a brief delay so HTTP response flushes.
    """
    logger.critical(
        "[FATAL CRASH TRIGGERED - F2] Simulated SIGSEGV / unhandled fatal exception in transfer engine",
        exc_info=True,
    )

    def _delayed_exit():
        time.sleep(0.3)
        os._exit(1)

    threading.Thread(target=_delayed_exit, daemon=True).start()
    return {"status": "crashing", "message": "Server exiting immediately with exit code 1 (F2)"}


@app.api_route("/api/test/crash/immediate", methods=["GET", "POST"], tags=["Simulation"], dependencies=[Depends(verify_crash_allowed)])
async def crash_server_immediate():
    """Immediately terminates the process with exit code 1."""
    logger.critical("FATAL: Immediate server crash requested via /api/test/crash/immediate")
    os._exit(1)


@app.api_route("/api/test/crash/deadlock", methods=["GET", "POST"], tags=["Simulation"], dependencies=[Depends(verify_crash_allowed)])
async def trigger_deadlock():
    """
    Simulates a thread deadlock between worker threads.
    Thread is daemonized and capped so it does not hang server processes permanently.
    """
    logger.critical(
        "[DEADLOCK TRIGGERED] Simulating circular mutex deadlock between storage worker and network relay"
    )

    lock1 = threading.Lock()
    lock2 = threading.Lock()

    def _worker_1():
        with lock1:
            time.sleep(0.1)
            with lock2:
                pass

    def _worker_2():
        with lock2:
            time.sleep(0.1)
            with lock1:
                pass

    t1 = threading.Thread(target=_worker_1, daemon=True, name="deadlock_worker_1")
    t2 = threading.Thread(target=_worker_2, daemon=True, name="deadlock_worker_2")
    t1.start()
    t2.start()

    def _freeze_background():
        # Capped sleep loop to prevent immortal leak
        for _ in range(30):
            time.sleep(10)

    threading.Thread(target=_freeze_background, daemon=True).start()

    return {
        "status": "deadlocked",
        "message": "Deadlock created between lock1 and lock2. Worker threads are frozen.",
    }


@app.api_route("/api/test/crash/oom", methods=["GET", "POST"], tags=["Simulation"], dependencies=[Depends(verify_crash_allowed)])
async def trigger_oom():
    """
    Simulates Out-Of-Memory (OOM) by rapidly allocating heap memory in a background thread,
    triggering MemoryError or container OOM-killer (SIGKILL 137).
    """
    logger.critical("[OOM TRIGGERED] Rapid heap memory allocation loop started to trigger OOM/SIGKILL")

    def _alloc_loop():
        time.sleep(0.2)
        blobs = []
        try:
            while True:
                blobs.append(bytearray(64 * 1024 * 1024))
        except MemoryError:
            logger.critical("Process MemoryError: Heap allocation limit exceeded, exiting process")
            os._exit(137)

    threading.Thread(target=_alloc_loop, daemon=True).start()
    return {"status": "oom_initiated", "message": "Rapid memory allocation started; container will crash shortly"}


@app.api_route("/api/test/error", methods=["GET", "POST"], tags=["Simulation"], dependencies=[Depends(verify_admin_or_sim_allowed)])
async def trigger_runtime_error(error_type: str = Query("key_error", alias="type")):
    """
    Triggers realistic unhandled program errors/exceptions that log full tracebacks to stdout/Render/ProdPilot
    while the server remains running. Let errors flow unhandled!
    """
    if error_type == "key_error":
        data = {"transfer_id": "9999"}
        _ = data["non_existent_chunk_hash_table"]
    elif error_type == "type_error":
        offset = None
        _ = offset + 262144
    elif error_type == "db_lock":
        import sqlite3
        raise sqlite3.OperationalError("database is locked (Simulated concurrent lock contention on chunks.db)")
    elif error_type == "ack_timeout":
        raise TimeoutError("ReceiverAckTimeoutError: Transfer 9999 halted: 10 consecutive chunks sent without ACK from receiver")
    elif error_type == "storage_full":
        raise OSError(28, "No space left on device: '/data/downloads/9999/chunk_0042.part'")
    elif error_type == "broken_pipe":
        raise BrokenPipeError("ReceiverMissingError: Cannot relay chunk 42 for transfer 9999 - receiver connection broken")
    else:
        raise ValueError(f"Unknown simulated error type '{error_type}'")


# --- WebSocket Endpoint ---


@app.websocket("/ws/transfer/{transfer_id}")
async def transfer_websocket(
    websocket: WebSocket,
    transfer_id: str,
    role: str = Query("observer"),
    crash_on_disconnect: bool = Query(False),
):
    """
    Live WebSocket relay for sender and receiver.
    Relays chunks, ACKs, progress updates, and resume handshakes.
    """
    await manager.connect(websocket, transfer_id, role)
    try:
        while True:
            text = await websocket.receive_text()
            await handle_websocket_message(websocket, text)
    except WebSocketDisconnect as exc:
        manager.disconnect(websocket)
        record = get_transfer(transfer_id)
        is_transfer_incomplete = record and record.get("status") != "completed"
        if role in ("sender", "receiver") and is_transfer_incomplete:
            done_chunks = record.get("received_chunks_count", 0)
            total_chunks = record.get("total_chunks", 0)
            pct = record.get("progress_percentage", 0.0)
            update_transfer_status(transfer_id, "interrupted")

            if (
                crash_on_disconnect
                or simulator.crash_on_disconnect
                or os.environ.get("CRASH_ON_DISCONNECT", "").lower() in ("1", "true", "yes")
            ):
                logger.critical(
                    "FATAL CLIENT DISCONNECT [CRASH TRIGGERED]: %s disconnected at chunk %d/%d (%.1f%%) for transfer %s (code=%s) -> TERMINATING SERVER PROCESS",
                    role.upper(),
                    done_chunks,
                    total_chunks,
                    pct,
                    transfer_id,
                    exc.code,
                    exc_info=True,
                )
                os._exit(1)

            # Browser refresh sends code 1001 (Going Away); network drop sends code 1006 (Abnormal Closure)
            if exc.code != 1000:
                logger.critical(
                    "ClientAbruptDisconnectError: %s unexpectedly disconnected / refreshed mid-transfer for %s at chunk %d/%d (%.1f%%, code=%s)",
                    role.capitalize(),
                    transfer_id,
                    done_chunks,
                    total_chunks,
                    pct,
                    exc.code,
                    exc_info=True,
                )
                raise ConnectionResetError(
                    f"ClientAbruptDisconnectError: {role.capitalize()} abruptly disconnected / refreshed mid-transfer for {transfer_id} "
                    f"at chunk {done_chunks}/{total_chunks} ({pct}%, code={exc.code})"
                ) from exc
            else:
                logger.warning(
                    "Transfer %s interrupted: %s closed connection cleanly (code=1000) before completion (%d/%d chunks)",
                    transfer_id,
                    role.capitalize(),
                    done_chunks,
                    total_chunks,
                )
    except ZeroDivisionError as exc:
        logger.critical(
            "FATAL APPLICATION CRASH: Unhandled ZeroDivisionError in transfer engine throughput calculation for transfer %s: %s",
            transfer_id,
            exc,
            exc_info=True,
        )
        os._exit(1)
    except Exception as exc:
        logger.exception(
            "CRITICAL Unhandled Exception in WebSocket transfer=%s role=%s: %s",
            transfer_id,
            role,
            exc,
        )
        raise


# Mount static web interface files
static_dir = os.path.join(os.path.dirname(__file__), "static")
if os.path.isdir(static_dir):
    app.mount("/", StaticFiles(directory=static_dir, html=True), name="static")


def run():
    """CLI entrypoint for running the server."""
    import uvicorn

    port = int(os.environ.get("PORT", 8000))
    host = os.environ.get("HOST", "0.0.0.0")
    uvicorn.run("app.main:app", host=host, port=port, reload=False)


if __name__ == "__main__":
    run()
