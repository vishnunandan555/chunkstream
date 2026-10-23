import asyncio
import base64
import json
import logging
import random
import sys
from typing import Any, Dict, List, Optional, Set
from fastapi import WebSocket, WebSocketDisconnect

from app.storage import (
    assemble_file,
    get_transfer,
    save_chunk,
    update_transfer_status,
)

logger = logging.getLogger("chunkstream.transfer")


class FailureSimulator:
    """Controls simulated failure scenarios for ProdPilot testing."""

    def __init__(self) -> None:
        self.error_rate: float = 0.0  # 0.0 to 1.0
        self.error_mode: str = "none"  # "none", "low", "high"
        self.force_resume_failure: bool = False
        self.force_health_unhealthy: bool = False
        self.simulate_storage_error: bool = False
        self.delay_ms: int = 0

    def set_scenario(self, scenario: str) -> Dict[str, Any]:
        if scenario == "none" or scenario == "reset":
            self.error_rate = 0.0
            self.error_mode = "none"
            self.force_resume_failure = False
            self.force_health_unhealthy = False
            self.simulate_storage_error = False
            self.delay_ms = 0
            logger.info("Simulator reset to normal operation (all failure triggers cleared)")
        elif scenario == "low_error":  # F3
            self.error_rate = 0.20
            self.error_mode = "low"
            logger.warning("[SIMULATION F3] Low error rate activated (20% chunk drop/retry)")
        elif scenario == "high_error":  # F4
            self.error_rate = 0.75
            self.error_mode = "high"
            logger.error(
                "[SIMULATION F4] High error rate activated (75% chunk failure - repeated errors)"
            )
        elif scenario == "resume_failure":  # F6
            self.force_resume_failure = True
            logger.error("[SIMULATION F6] Resume failure simulation activated (corrupt resume state)")
        elif scenario == "storage_error":
            self.simulate_storage_error = True
            logger.error("[SIMULATION] Storage I/O failure simulation activated")
        elif scenario == "unhealthy":  # F1
            self.force_health_unhealthy = True
            logger.critical("[SIMULATION F1] Service marked UNHEALTHY (health checks failing 503)")
        return self.get_status()

    def get_status(self) -> Dict[str, Any]:
        return {
            "error_mode": self.error_mode,
            "error_rate": self.error_rate,
            "force_resume_failure": self.force_resume_failure,
            "force_health_unhealthy": self.force_health_unhealthy,
            "simulate_storage_error": self.simulate_storage_error,
            "delay_ms": self.delay_ms,
        }

    def should_fail_chunk(self, chunk_index: int) -> bool:
        if self.error_rate > 0.0:
            if random.random() < self.error_rate:
                logger.warning(
                    "Simulated network/chunk fault injected on chunk %d (rate=%.2f, mode=%s)",
                    chunk_index,
                    self.error_rate,
                    self.error_mode,
                )
                return True
        return False


# Global simulator instance
simulator = FailureSimulator()


class ConnectionManager:
    """Manages active WebSockets for file transfer relay and progress reporting."""

    def __init__(self) -> None:
        # transfer_id -> list of active WebSockets
        self.active_connections: Dict[str, Set[WebSocket]] = {}
        # websocket -> metadata dict {"transfer_id": str, "role": str}
        self.connection_meta: Dict[WebSocket, Dict[str, str]] = {}

    async def connect(self, websocket: WebSocket, transfer_id: str, role: str) -> None:
        await websocket.accept()
        if transfer_id not in self.active_connections:
            self.active_connections[transfer_id] = set()
        self.active_connections[transfer_id].add(websocket)
        self.connection_meta[websocket] = {"transfer_id": transfer_id, "role": role}

        logger.info(
            "WebSocket connected: transfer=%s, role=%s, active_peers=%d",
            transfer_id,
            role,
            len(self.active_connections[transfer_id]),
        )

        # Notify peers of peer status
        await self.broadcast(
            transfer_id,
            {
                "type": "peer_connected",
                "role": role,
                "peers_count": len(self.active_connections[transfer_id]),
            },
        )

    def disconnect(self, websocket: WebSocket) -> None:
        meta = self.connection_meta.pop(websocket, None)
        if meta:
            transfer_id = meta["transfer_id"]
            role = meta["role"]
            if transfer_id in self.active_connections:
                self.active_connections[transfer_id].discard(websocket)
                peer_count = len(self.active_connections[transfer_id])
                if peer_count == 0:
                    del self.active_connections[transfer_id]
                logger.info(
                    "WebSocket disconnected: transfer=%s, role=%s, remaining_peers=%d",
                    transfer_id,
                    role,
                    peer_count,
                )
                # Broadcast disconnect notification
                asyncio.create_task(
                    self.broadcast(
                        transfer_id,
                        {
                            "type": "peer_disconnected",
                            "role": role,
                            "peers_count": peer_count,
                        },
                    )
                )

    async def broadcast(self, transfer_id: str, message: Dict[str, Any]) -> None:
        if transfer_id not in self.active_connections:
            return
        payload = json.dumps(message)
        dead_sockets = []
        for ws in self.active_connections[transfer_id]:
            try:
                await ws.send_text(payload)
            except Exception:
                dead_sockets.append(ws)

        for ws in dead_sockets:
            self.disconnect(ws)

    async def send_to_role(
        self, transfer_id: str, target_role: str, message: Dict[str, Any]
    ) -> None:
        if transfer_id not in self.active_connections:
            return
        payload = json.dumps(message)
        dead_sockets = []
        for ws in self.active_connections[transfer_id]:
            meta = self.connection_meta.get(ws, {})
            if meta.get("role") == target_role:
                try:
                    await ws.send_text(payload)
                except Exception:
                    dead_sockets.append(ws)

        for ws in dead_sockets:
            self.disconnect(ws)

    async def force_disconnect_role(self, transfer_id: str, target_role: str) -> int:
        """Forces disconnect on all sockets with given role (for F5 simulation)."""
        count = 0
        if transfer_id in self.active_connections:
            targets = [
                ws
                for ws in self.active_connections[transfer_id]
                if self.connection_meta.get(ws, {}).get("role") == target_role
            ]
            for ws in targets:
                try:
                    await ws.close(code=1001, reason="Simulated network disconnect (F5)")
                    self.disconnect(ws)
                    count += 1
                except Exception:
                    pass
        logger.warning(
            "[SIMULATION F5] Force disconnected %d connection(s) for role=%s in transfer=%s",
            count,
            target_role,
            transfer_id,
        )
        return count


manager = ConnectionManager()


async def handle_websocket_message(websocket: WebSocket, raw_message: str) -> None:
    """Processes incoming messages from sender or receiver."""
    meta = manager.connection_meta.get(websocket)
    if not meta:
        return

    transfer_id = meta["transfer_id"]
    role = meta["role"]

    try:
        data = json.loads(raw_message)
    except json.JSONDecodeError:
        logger.error("Invalid JSON message received from %s", role)
        await websocket.send_text(json.dumps({"type": "error", "message": "Invalid JSON payload"}))
        return

    msg_type = data.get("type")

    # 1. Chunk received from sender
    if msg_type == "chunk":
        chunk_index = data.get("index")
        total_chunks = data.get("total")
        chunk_data_b64 = data.get("data")
        chunk_hash = data.get("hash")

        if chunk_index is None or chunk_data_b64 is None:
            await websocket.send_text(
                json.dumps({"type": "error", "message": "Missing chunk index or data"})
            )
            return

        # Check simulated failures
        if simulator.simulate_storage_error:
            logger.error("Storage write failed (Simulated disk I/O error on chunk %d)", chunk_index)
            await websocket.send_text(
                json.dumps(
                    {
                        "type": "chunk_error",
                        "index": chunk_index,
                        "error": "StorageWriteError: Disk full or I/O failure (Simulated)",
                        "retryable": False,
                    }
                )
            )
            return

        if simulator.should_fail_chunk(chunk_index):
            await websocket.send_text(
                json.dumps(
                    {
                        "type": "chunk_error",
                        "index": chunk_index,
                        "error": "NetworkTransmissionError: Chunk dropped or corrupted in transit (Simulated)",
                        "retryable": True,
                    }
                )
            )
            return

        # Decode and save chunk
        try:
            chunk_bytes = base64.b64decode(chunk_data_b64)
        except Exception as e:
            await websocket.send_text(
                json.dumps(
                    {
                        "type": "chunk_error",
                        "index": chunk_index,
                        "error": f"Base64 decode error: {e}",
                        "retryable": False,
                    }
                )
            )
            return

        success, msg = save_chunk(transfer_id, chunk_index, chunk_bytes, chunk_hash)
        if not success:
            await websocket.send_text(
                json.dumps(
                    {
                        "type": "chunk_error",
                        "index": chunk_index,
                        "error": msg,
                        "retryable": True,
                    }
                )
            )
            return

        # Send ACK back to sender
        await websocket.send_text(
            json.dumps({"type": "chunk_ack", "index": chunk_index, "transfer_id": transfer_id})
        )

        # Forward chunk to receiver(s) in real time
        await manager.send_to_role(
            transfer_id,
            "receiver",
            {
                "type": "chunk_data",
                "transfer_id": transfer_id,
                "index": chunk_index,
                "total": total_chunks,
                "size": len(chunk_bytes),
                "data": chunk_data_b64,
                "hash": chunk_hash,
            },
        )

        # Broadcast progress update to all peers/observers
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
                    "status": transfer_info["status"],
                },
            )

    # 2. Resume Request from receiver or sender (Phase 3)
    elif msg_type == "resume_request":
        logger.info("Resume request initiated for transfer %s by %s", transfer_id, role)
        if simulator.force_resume_failure:
            err_msg = "ResumeConflictError: Transfer metadata mismatch or state checksum corrupted (Simulated F6)"
            logger.error("[SIMULATION F6] %s", err_msg)
            await websocket.send_text(
                json.dumps({"type": "resume_error", "message": err_msg, "transfer_id": transfer_id})
            )
            return

        transfer_info = get_transfer(transfer_id)
        if not transfer_info:
            await websocket.send_text(
                json.dumps({"type": "error", "message": f"Transfer {transfer_id} not found"})
            )
            return

        # Return acknowledged chunks and missing chunks list
        update_transfer_status(transfer_id, "transferring")
        await websocket.send_text(
            json.dumps(
                {
                    "type": "resume_state",
                    "transfer_id": transfer_id,
                    "filename": transfer_info["filename"],
                    "filesize": transfer_info["filesize"],
                    "total_chunks": transfer_info["total_chunks"],
                    "chunk_size": transfer_info["chunk_size"],
                    "acknowledged_chunks": transfer_info["acknowledged_chunks"],
                    "missing_chunks": transfer_info["missing_chunks"],
                    "received_count": transfer_info["received_chunks_count"],
                }
            )
        )

        # Tell sender what chunks are missing so it can stream only missing chunks
        await manager.send_to_role(
            transfer_id,
            "sender",
            {
                "type": "request_missing",
                "transfer_id": transfer_id,
                "missing_chunks": transfer_info["missing_chunks"],
            },
        )

    # 3. Assemble complete file
    elif msg_type == "complete":
        logger.info("Transfer completion signal received for %s", transfer_id)
        success, out_path, err = assemble_file(transfer_id)
        if not success:
            logger.error("Assembly failed for %s: %s", transfer_id, err)
            await manager.broadcast(
                transfer_id,
                {"type": "assembly_failed", "transfer_id": transfer_id, "error": err},
            )
        else:
            logger.info("Assembly successful for %s -> %s", transfer_id, out_path)
            await manager.broadcast(
                transfer_id,
                {
                    "type": "completed",
                    "transfer_id": transfer_id,
                    "download_url": f"/api/transfers/{transfer_id}/download",
                },
            )
