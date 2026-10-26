import asyncio
import os
import secrets
import string
import time
import uuid
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional

from fastapi import (
    FastAPI,
    HTTPException,
    Query,
    Request,
    Response,
    WebSocket,
    WebSocketDisconnect,
    status,
)
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field, field_validator

from app.logging_config import request_id_ctx, setup_logging
from app.metrics import metrics
from app.storage import (
    check_storage_health,
    cleanup_expired_transfers,
    create_transfer,
    delete_transfer,
    get_transfer,
    init_db,
    list_transfers,
    perform_full_cleanup,
    sanitize_filename,
    update_transfer_status,
)
from app.transfer import (
    clear_transfer_telemetry,
    get_transfer_telemetry_summary,
    handle_websocket_message,
    manager,
)

APP_VERSION = os.environ.get("CHUNKSTREAM_VERSION", "2.1.0")
APP_START_TIME = time.time()
ENVIRONMENT = os.environ.get("ENVIRONMENT", "production").lower()

logger = setup_logging(os.environ.get("LOG_LEVEL", "INFO"))


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting ChunkStream Zero-Storage Relay v%s (env: %s)", APP_VERSION, ENVIRONMENT)
    init_db()

    # Periodic maintenance worker: clean up expired metadata records older than 15 mins & checkpoint DB
    stop_event = asyncio.Event()

    async def _cleanup_worker():
        while not stop_event.is_set():
            try:
                await asyncio.sleep(300)
                await asyncio.to_thread(perform_full_cleanup, 15)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.warning("Error during periodic transfer cleanup: %s", exc)

    cleanup_task = asyncio.create_task(_cleanup_worker())

    yield

    stop_event.set()
    cleanup_task.cancel()
    await manager.close_all(reason="Server is shutting down")
    logger.info("ChunkStream shutdown complete")


app = FastAPI(
    title="ChunkStream",
    description="Ultra-fast zero-server-storage peer file streaming relay",
    version=APP_VERSION,
    lifespan=lifespan,
)

# CORS
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


@app.middleware("http")
async def security_and_metrics_middleware(request: Request, call_next):
    req_id = request.headers.get("X-Request-ID") or uuid.uuid4().hex[:10]
    token = request_id_ctx.set(req_id)
    start_time = time.time()
    path = request.url.path

    try:
        response: Response = await call_next(request)
    except Exception as exc:
        latency_ms = (time.time() - start_time) * 1000
        logger.exception("%s %s ERROR (%.1fms): %s", request.method, path, latency_ms, exc)
        metrics.record_request(request.method, path, 500)
        request_id_ctx.reset(token)
        raise

    latency_ms = (time.time() - start_time) * 1000
    metrics.record_request(request.method, path, response.status_code)

    response.headers["X-Request-ID"] = req_id
    response.headers["X-Response-Time-Ms"] = f"{latency_ms:.2f}"
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "SAMEORIGIN"
    response.headers["Content-Security-Policy"] = (
        "default-src 'self'; script-src 'self' 'unsafe-inline'; "
        "style-src 'self' 'unsafe-inline'; font-src 'self' data:; "
        "connect-src 'self' ws: wss:;"
    )

    request_id_ctx.reset(token)
    return response


def generate_transfer_id() -> str:
    """Generates a clean 4-digit transfer code."""
    for _ in range(50):
        code = f"{secrets.randbelow(9000) + 1000}"
        if not get_transfer(code):
            return code
    alphabet = string.ascii_uppercase + string.digits
    for _ in range(50):
        code = "".join(secrets.choice(alphabet) for _ in range(6))
        if not get_transfer(code):
            return code
    raise RuntimeError("Unable to generate unique transfer ID")


class CreateTransferRequest(BaseModel):
    filename: str
    filesize: int = Field(gt=0, le=100 * 1024 * 1024 * 1024, description="File size up to 100 GB")
    chunk_size: int = Field(default=262144, ge=1, le=10 * 1024 * 1024)
    total_chunks: int = Field(gt=0, le=1_000_000)
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


# --- Core Endpoints ---


@app.get("/health", tags=["Health"])
@app.get("/api/health", tags=["Health"])
async def health_check():
    """Authentic liveness and database accessibility check."""
    if not check_storage_health():
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Storage backend unavailable",
        )

    active_ws = manager.get_total_active_connections()
    metrics.set_active_websockets(active_ws)

    return {
        "status": "healthy",
        "service": "chunkstream",
        "version": APP_VERSION,
        "uptime_seconds": int(time.time() - APP_START_TIME),
        "active_connections": active_ws,
        "zero_server_storage": True,
    }


@app.get("/metrics", tags=["Observability"])
async def prometheus_metrics():
    """Standard Prometheus exposition metrics."""
    metrics.set_active_websockets(manager.get_total_active_connections())
    return Response(
        content=metrics.generate_prometheus_text(),
        media_type="text/plain; version=0.0.4; charset=utf-8",
    )


@app.get("/api/version", tags=["Health"])
async def get_version():
    return {
        "version": APP_VERSION,
        "service": "chunkstream",
        "zero_server_storage": True,
        "uptime": round(time.time() - APP_START_TIME, 2),
    }


@app.get("/api/network-info", tags=["Observability"])
async def get_network_info(request: Request):
    """Provides client IP estimation, network classification, and relay server region."""
    forwarded = request.headers.get("x-forwarded-for") or request.headers.get("cf-connecting-ip")
    client_ip = forwarded.split(",")[0].strip() if forwarded else (request.client.host if request.client else "127.0.0.1")

    is_local = client_ip in ("127.0.0.1", "localhost", "testclient") or client_ip.startswith(("192.168.", "10.", "172.16."))

    masked_ip = client_ip
    if not is_local and "." in client_ip:
        parts = client_ip.split(".")
        if len(parts) == 4:
            masked_ip = f"{parts[0]}.{parts[1]}.{parts[2]}.xxx"

    server_region = os.environ.get("SERVER_REGION") or os.environ.get("RENDER_REGION") or "Frankfurt, EU (Cloud Relay)"

    return {
        "client_ip": masked_ip,
        "is_local": is_local,
        "server_region": server_region,
        "timestamp": time.time(),
    }



@app.post("/api/transfers", tags=["Transfer"])
async def init_transfer(payload: CreateTransferRequest):
    """Initializes a new transfer session metadata."""
    transfer_id = payload.transfer_id or generate_transfer_id()
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
    logger.info(
        "[%s] Registered transfer session: \"%s\" (%d bytes, %d chunks @ %d bytes/chunk)",
        transfer_id,
        payload.filename,
        payload.filesize,
        payload.total_chunks,
        payload.chunk_size,
    )
    return record


@app.get("/api/transfers", tags=["Transfer"])
async def list_all_transfers(limit: int = 30):
    return list_transfers(limit=limit)


@app.get("/api/transfers/{transfer_id}", tags=["Transfer"])
async def get_transfer_details(transfer_id: str):
    record = get_transfer(transfer_id)
    if not record:
        raise HTTPException(status_code=404, detail=f"Transfer {transfer_id} not found")
    return record


@app.get("/api/transfers/{transfer_id}/metrics", tags=["Transfer"])
async def get_transfer_metrics(transfer_id: str):
    record = get_transfer(transfer_id)
    if not record:
        raise HTTPException(status_code=404, detail=f"Transfer {transfer_id} not found")
    return {"transfer_id": transfer_id, "metrics": get_transfer_telemetry_summary(transfer_id)}


@app.delete("/api/transfers/{transfer_id}", tags=["Transfer"])
async def remove_transfer(transfer_id: str):
    """Deletes transfer session metadata and clears server cache idempotently."""
    delete_transfer(transfer_id)
    clear_transfer_telemetry(transfer_id)
    logger.info("[%s] Transfer session metadata and telemetry cleared", transfer_id)
    return {"status": "deleted", "transfer_id": transfer_id}


@app.post("/api/cleanup", tags=["Maintenance"])
async def trigger_cleanup():
    """Triggers server session cache prune and SQLite WAL checkpoint."""
    result = await asyncio.to_thread(perform_full_cleanup, 15)
    active_peers = manager.get_total_active_connections()
    logger.info(
        "Server maintenance cleanup executed: %d records purged, active peers: %d",
        result.get("cleaned_transfers", 0),
        active_peers,
    )
    return {
        "status": "ok",
        "cleaned_transfers": result.get("cleaned_transfers", 0),
        "active_connections": active_peers,
        "maintenance": result.get("maintenance"),
        "message": "Server cache cleared and database checkpointed",
    }


# --- Real-Time Streaming WebSocket ---


@app.websocket("/ws/transfer/{transfer_id}")
async def transfer_websocket(
    websocket: WebSocket,
    transfer_id: str,
    role: str = Query("observer"),
):
    """Zero-server-storage real-time streaming WebSocket relay."""
    client_ip = getattr(websocket.client, "host", "unknown") if websocket.client else "unknown"
    logger.info("[%s] WebSocket incoming: role=%s from %s", transfer_id, role.upper(), client_ip)
    await manager.connect(websocket, transfer_id, role)
    try:
        while True:
            text = await websocket.receive_text()
            await handle_websocket_message(websocket, text)
    except WebSocketDisconnect:
        logger.info("[%s] WebSocket disconnect: role=%s from %s", transfer_id, role.upper(), client_ip)
        manager.disconnect(websocket)
        record = get_transfer(transfer_id)
        if record and record.get("status") not in ("completed", "paused", "cancelled"):
            update_transfer_status(transfer_id, "interrupted")
    except Exception as exc:
        logger.error("[%s] WebSocket error for role=%s: %s", transfer_id, role.upper(), exc)
        manager.disconnect(websocket)


# Mount static frontend
static_dir = os.path.join(os.path.dirname(__file__), "static")
if os.path.isdir(static_dir):
    app.mount("/", StaticFiles(directory=static_dir, html=True), name="static")


def run():
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    host = os.environ.get("HOST", "0.0.0.0")
    uvicorn.run("app.main:app", host=host, port=port, reload=False)


if __name__ == "__main__":
    run()
