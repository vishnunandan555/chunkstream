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
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field, field_validator

from app.logging_config import request_id_ctx, setup_logging
from app.metrics import metrics
from app.storage import (
    assemble_file_async,
    check_storage_health,
    cleanup_expired_transfers,
    create_transfer,
    delete_transfer,
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
)

APP_VERSION = os.environ.get("CHUNKSTREAM_VERSION", "2.0.0")
APP_START_TIME = time.time()
ENVIRONMENT = os.environ.get("ENVIRONMENT", "production").lower()
MAX_CHUNK_UPLOAD_BYTES = int(os.environ.get("MAX_CHUNK_UPLOAD_BYTES", 10 * 1024 * 1024))

logger = setup_logging(os.environ.get("LOG_LEVEL", "INFO"))


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting ChunkStream v%s (env: %s)", APP_VERSION, ENVIRONMENT)
    init_db()

    # Background worker: prune stale transfers older than 1 hour every 30 mins
    stop_event = asyncio.Event()

    async def _cleanup_worker():
        while not stop_event.is_set():
            try:
                await asyncio.sleep(1800)
                await asyncio.to_thread(cleanup_expired_transfers, 1)
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
    description="Minimal, ultra-fast resumable file transfer service",
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
    # Fallback to 6 alphanumeric characters
    alphabet = string.ascii_uppercase + string.digits
    for _ in range(50):
        code = "".join(secrets.choice(alphabet) for _ in range(6))
        if not get_transfer(code):
            return code
    raise RuntimeError("Unable to generate unique transfer ID")


class CreateTransferRequest(BaseModel):
    filename: str
    filesize: int = Field(gt=0, le=10 * 1024 * 1024 * 1024, description="File size up to 10 GB")
    chunk_size: int = Field(default=262144, ge=1, le=10 * 1024 * 1024)
    total_chunks: int = Field(gt=0, le=500_000)
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
        "uptime": round(time.time() - APP_START_TIME, 2),
    }


@app.post("/api/transfers", tags=["Transfer"])
async def init_transfer(payload: CreateTransferRequest):
    """Initializes a new transfer session."""
    transfer_id = payload.transfer_id or generate_transfer_id()
    existing = get_transfer(transfer_id)
    if existing:
        return existing

    return create_transfer(
        transfer_id=transfer_id,
        filename=payload.filename,
        filesize=payload.filesize,
        chunk_size=payload.chunk_size,
        total_chunks=payload.total_chunks,
        mime_type=payload.mime_type,
        checksum=payload.checksum,
    )


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


@app.post("/api/transfers/{transfer_id}/chunks/{chunk_index}", tags=["Transfer"])
async def upload_chunk_http(
    transfer_id: str,
    chunk_index: int,
    request: Request,
    checksum: Optional[str] = Query(None),
):
    """HTTP chunk upload endpoint."""
    data = await request.body()
    if len(data) > MAX_CHUNK_UPLOAD_BYTES:
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail=f"Chunk exceeds limit of {MAX_CHUNK_UPLOAD_BYTES} bytes",
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
    """Triggers file reassembly."""
    success, out_path, err = await assemble_file_async(transfer_id)
    if not success:
        raise HTTPException(status_code=400, detail=err)

    return {
        "status": "completed",
        "transfer_id": transfer_id,
        "download_url": f"/api/transfers/{transfer_id}/download",
    }


@app.get("/api/transfers/{transfer_id}/download", tags=["Transfer"])
async def download_file(transfer_id: str):
    """Downloads the assembled file with path validation."""
    record = get_transfer(transfer_id)
    if not record:
        raise HTTPException(status_code=404, detail="Transfer not found")

    safe_name = sanitize_filename(record["filename"])
    downloads_base = os.path.join(os.environ.get("CHUNKSTREAM_DATA_DIR", "data"), "downloads")
    out_dir = os.path.join(downloads_base, transfer_id)
    out_path = os.path.join(out_dir, safe_name)

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


@app.delete("/api/transfers/{transfer_id}", tags=["Transfer"])
async def remove_transfer(transfer_id: str):
    """Deletes transfer and immediately frees server disk space."""
    deleted = delete_transfer(transfer_id)
    if not deleted:
        raise HTTPException(status_code=404, detail=f"Transfer {transfer_id} not found")
    return {"status": "deleted", "transfer_id": transfer_id}


# --- WebSocket Endpoint ---


@app.websocket("/ws/transfer/{transfer_id}")
async def transfer_websocket(
    websocket: WebSocket,
    transfer_id: str,
    role: str = Query("observer"),
):
    """Live WebSocket relay for real-time chunk streaming & peer progress."""
    await manager.connect(websocket, transfer_id, role)
    try:
        while True:
            text = await websocket.receive_text()
            await handle_websocket_message(websocket, text)
    except WebSocketDisconnect:
        manager.disconnect(websocket)
        record = get_transfer(transfer_id)
        if record and record.get("status") not in ("completed", "paused", "cancelled"):
            update_transfer_status(transfer_id, "interrupted")
    except Exception as exc:
        logger.error("WebSocket error for transfer=%s role=%s: %s", transfer_id, role, exc)
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
