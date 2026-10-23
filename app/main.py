import os
import secrets
import string
import sys
import threading
import time
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
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from app.logging_config import clear_logs, get_recent_logs, setup_logging
from app.storage import (
    assemble_file,
    create_transfer,
    get_chunk,
    get_transfer,
    init_db,
    list_transfers,
    save_chunk,
    update_transfer_status,
)
from app.transfer import (
    handle_websocket_message,
    manager,
    simulator,
)

# Initialize logger
logger = setup_logging(
    log_level=os.environ.get("LOG_LEVEL", "INFO"),
    log_dir=os.environ.get("LOG_DIR", "logs"),
)

APP_VERSION = os.environ.get("CHUNKSTREAM_VERSION", "1.0.0")
APP_START_TIME = time.time()


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    logger.info("Starting ChunkStream v%s", APP_VERSION)
    init_db()
    yield
    # Shutdown
    logger.info("ChunkStream shutting down gracefully")


app = FastAPI(
    title="ChunkStream",
    description="Resumable chunked file-transfer service designed for testing ProdPilot",
    version=APP_VERSION,
    lifespan=lifespan,
)

# CORS configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def generate_transfer_id() -> str:
    """Generates a memorable transfer ID like CS-7A2B."""
    suffix = "".join(secrets.choice(string.ascii_uppercase + string.digits) for _ in range(4))
    return f"CS-{suffix}"


# Pydantic Models
class CreateTransferRequest(BaseModel):
    filename: str
    filesize: int
    chunk_size: int = Field(default=262144)  # 256 KB default
    total_chunks: int
    mime_type: Optional[str] = "application/octet-stream"
    checksum: Optional[str] = None
    transfer_id: Optional[str] = None


class FailureScenarioRequest(BaseModel):
    scenario: str = Field(
        ...,
        description="Scenario key: 'none', 'low_error', 'high_error', 'resume_failure', 'storage_error', 'unhealthy'",
    )


# --- Health & Diagnostics (Phase 5, Phase 6) ---


@app.get("/health", tags=["Health"])
@app.get("/api/health", tags=["Health"])
async def health_check():
    """Health check endpoint used by Render and ProdPilot."""
    if simulator.force_health_unhealthy:
        logger.critical(
            "[HEALTH CHECK FAILURE] Simulated 503 Unhealthy status returned to monitor"
        )
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Simulated Service Degradation: Storage cluster or internal worker unreachable",
        )

    return {
        "status": "healthy",
        "service": "chunkstream",
        "version": APP_VERSION,
        "uptime_seconds": int(time.time() - APP_START_TIME),
        "simulation_state": simulator.get_status(),
    }


@app.get("/api/version", tags=["Health"])
async def get_version():
    """Returns application version and environment information."""
    return {
        "version": APP_VERSION,
        "service": "chunkstream",
        "environment": os.environ.get("ENVIRONMENT", "development"),
        "uptime": round(time.time() - APP_START_TIME, 2),
    }


@app.get("/api/logs", tags=["Logs"])
async def get_logs(limit: int = 100, min_level: Optional[str] = None):
    """Retrieves recent in-memory logs for ProdPilot and UI live viewer."""
    return {
        "count": len(get_recent_logs(limit=limit, min_level=min_level)),
        "logs": get_recent_logs(limit=limit, min_level=min_level),
    }


# --- Transfer Endpoints (Phase 1, Phase 2, Phase 3) ---


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
    """Lists recent transfers and current progress."""
    return list_transfers(limit=limit)


@app.get("/api/transfers/{transfer_id}", tags=["Transfer"])
async def get_transfer_details(transfer_id: str):
    """Fetches details of a transfer, including acknowledged and missing chunk sets."""
    record = get_transfer(transfer_id)
    if not record:
        raise HTTPException(status_code=404, detail=f"Transfer {transfer_id} not found")
    return record


@app.post("/api/transfers/{transfer_id}/chunks/{chunk_index}", tags=["Transfer"])
async def upload_chunk_http(
    transfer_id: str,
    chunk_index: int,
    request: Request,
    checksum: Optional[str] = Query(None),
):
    """
    HTTP chunk upload endpoint.
    Supports resumable chunk uploads and failure injection for F3/F4.
    """
    if simulator.simulate_storage_error:
        logger.error(
            "HTTP chunk upload failed: Disk I/O simulated error on transfer %s chunk %d",
            transfer_id,
            chunk_index,
        )
        raise HTTPException(
            status_code=status.HTTP_507_INSUFFICIENT_STORAGE,
            detail="Simulated Disk I/O storage failure",
        )

    if simulator.should_fail_chunk(chunk_index):
        logger.warning(
            "HTTP chunk upload simulated drop: 503 Service Unavailable on transfer %s chunk %d",
            transfer_id,
            chunk_index,
        )
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Simulated transient chunk upload failure",
        )

    data = await request.body()
    if not data:
        raise HTTPException(status_code=400, detail="Empty chunk payload")

    success, msg = save_chunk(transfer_id, chunk_index, data, expected_checksum=checksum)
    if not success:
        logger.warning("Chunk %d save failed for %s: %s", chunk_index, transfer_id, msg)
        raise HTTPException(status_code=400, detail=msg)

    # Notify WebSocket peers of progress
    transfer_info = get_transfer(transfer_id)
    if transfer_info:
        await manager.broadcast(
            transfer_id,
            {
                "type": "progress",
                "transfer_id": transfer_id,
                "received": transfer_info["received_chunks_count"],
                "total": transfer_info["total_chunks"],
                "percentage": transfer_info["progress_percentage"],
            },
        )

    return {"status": "ok", "chunk_index": chunk_index, "size": len(data)}


@app.get("/api/transfers/{transfer_id}/chunks/{chunk_index}", tags=["Transfer"])
async def download_chunk_http(transfer_id: str, chunk_index: int):
    """Downloads a single raw chunk."""
    chunk_bytes = get_chunk(transfer_id, chunk_index)
    if chunk_bytes is None:
        raise HTTPException(status_code=404, detail="Chunk not found")
    return Response(content=chunk_bytes, media_type="application/octet-stream")


@app.post("/api/transfers/{transfer_id}/assemble", tags=["Transfer"])
async def trigger_assembly(transfer_id: str):
    """Triggers file reassembly once all chunks are present."""
    success, out_path, err = assemble_file(transfer_id)
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
    """Downloads the assembled file."""
    record = get_transfer(transfer_id)
    if not record:
        raise HTTPException(status_code=404, detail="Transfer not found")

    out_path = os.path.join(
        os.environ.get("CHUNKSTREAM_DATA_DIR", "data"),
        "downloads",
        transfer_id,
        record["filename"],
    )
    if not os.path.isfile(out_path):
        # Attempt assembly if chunks are all present
        success, assembled_path, err = assemble_file(transfer_id)
        if not success or not assembled_path:
            raise HTTPException(
                status_code=400, detail=f"File not ready for download: {err or 'Missing chunks'}"
            )
        out_path = assembled_path

    return FileResponse(
        out_path,
        media_type=record.get("mime_type") or "application/octet-stream",
        filename=record["filename"],
    )


# --- Failure Simulation & Control Panel Endpoints (Phase 4) ---


@app.post("/api/test/fail", tags=["Simulation"])
async def trigger_failure_scenario(payload: FailureScenarioRequest):
    """Configures simulated failure mode (F1, F3, F4, F6, etc.)."""
    result = simulator.set_scenario(payload.scenario)
    return {"scenario": payload.scenario, "status": result}


@app.post("/api/test/reset", tags=["Simulation"])
async def reset_failure_scenarios():
    """Resets simulator to normal operation."""
    result = simulator.set_scenario("none")
    return {"scenario": "none", "status": result}


@app.post("/api/test/disconnect-receiver", tags=["Simulation"])
async def trigger_receiver_disconnect(transfer_id: str = Query(...)):
    """Simulates receiver disconnection (F5)."""
    count = await manager.force_disconnect_role(transfer_id, "receiver")
    return {"transfer_id": transfer_id, "disconnected_receivers": count}


@app.post("/api/test/crash", tags=["Simulation"])
async def crash_server():
    """
    F2: Intentional Server Crash.
    Emits critical log and terminates process after a brief delay so HTTP response flushes.
    """
    logger.critical(
        "[FATAL CRASH TRIGGERED - F2] Simulated SIGSEGV / unhandled MemoryError in transfer engine"
    )

    def _delayed_exit():
        time.sleep(0.3)
        os._exit(1)

    threading.Thread(target=_delayed_exit, daemon=True).start()
    return {"status": "crashing", "message": "Server exiting immediately with exit code 1 (F2)"}


# --- WebSocket Endpoint (Phase 1, Phase 2, Phase 3) ---


@app.websocket("/ws/transfer/{transfer_id}")
async def transfer_websocket(
    websocket: WebSocket, transfer_id: str, role: str = Query("observer")
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
    except WebSocketDisconnect:
        manager.disconnect(websocket)
    except Exception as e:
        logger.warning("WebSocket exception for %s role=%s: %s", transfer_id, role, e)
        manager.disconnect(websocket)


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
