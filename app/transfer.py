import asyncio
import json
import logging
import time
from typing import Any, Dict, List, Optional, Set
from fastapi import WebSocket

from app.storage import (
    delete_transfer,
    get_transfer,
    update_transfer_status,
)

logger = logging.getLogger("chunkstream.transfer")

_transfer_telemetry: Dict[str, Dict[str, Any]] = {}


def record_chunk_throughput(transfer_id: str, chunk_bytes_len: int) -> None:
    """Updates rolling bandwidth telemetry safely with high-resolution timer."""
    now = time.perf_counter()
    telemetry = _transfer_telemetry.setdefault(
        transfer_id,
        {
            "start_time": now,
            "last_ts": now,
            "total_bytes": 0,
        },
    )
    telemetry["total_bytes"] += chunk_bytes_len
    telemetry["last_ts"] = now


def get_transfer_telemetry_summary(transfer_id: str) -> Dict[str, Any]:
    """Retrieves rolling bandwidth and telemetry summary."""
    telemetry = _transfer_telemetry.get(transfer_id)
    if not telemetry:
        return {
            "transfer_id": transfer_id,
            "total_bytes": 0,
            "duration_sec": 0.0,
            "speed_mbps": 0.0,
        }
    now = time.perf_counter()
    elapsed = max(now - telemetry.get("start_time", now), 0.001)
    total_bytes = telemetry.get("total_bytes", 0)
    return {
        "transfer_id": transfer_id,
        "total_bytes": total_bytes,
        "duration_sec": round(elapsed, 2),
        "speed_mbps": round((total_bytes / (1024 * 1024)) / elapsed, 2),
    }


def clear_transfer_telemetry(transfer_id: str) -> None:
    """Frees in-memory telemetry records to prevent RAM leaks."""
    _transfer_telemetry.pop(transfer_id, None)


class ConnectionManager:
    """Manages active WebSockets for pure real-time in-memory relay."""

    def __init__(self) -> None:
        self.active_connections: Dict[str, Set[WebSocket]] = {}
        self.connection_meta: Dict[WebSocket, Dict[str, str]] = {}

    def get_total_active_connections(self) -> int:
        return sum(len(ws_set) for ws_set in self.active_connections.values())

    async def close_all(self, reason: str = "server_shutdown") -> None:
        payload = json.dumps({"type": "server_shutdown", "reason": reason})
        all_sockets = [
            ws
            for ws_set in list(self.active_connections.values())
            for ws in list(ws_set)
        ]
        for ws in all_sockets:
            try:
                await ws.send_text(payload)
                await ws.close(code=1001, reason=reason)
            except Exception:
                pass
        self.active_connections.clear()
        self.connection_meta.clear()

    def has_role(self, transfer_id: str, role: str) -> bool:
        if transfer_id not in self.active_connections:
            return False
        return any(
            self.connection_meta.get(ws, {}).get("role") == role
            for ws in self.active_connections[transfer_id]
        )

    async def connect(self, websocket: WebSocket, transfer_id: str, role: str) -> None:
        await websocket.accept()
        if transfer_id not in self.active_connections:
            self.active_connections[transfer_id] = set()
        self.active_connections[transfer_id].add(websocket)
        self.connection_meta[websocket] = {"transfer_id": transfer_id, "role": role}

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
                    clear_transfer_telemetry(transfer_id)

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
    ) -> bool:
        if transfer_id not in self.active_connections:
            return False
        target_sockets = [
            ws
            for ws in self.active_connections[transfer_id]
            if self.connection_meta.get(ws, {}).get("role") == target_role
        ]
        if not target_sockets:
            return False
        payload = json.dumps(message)
        for ws in target_sockets:
            try:
                await ws.send_text(payload)
            except Exception:
                self.disconnect(ws)
        return True


manager = ConnectionManager()


async def handle_websocket_message(websocket: WebSocket, raw_message: str) -> None:
    """Processes incoming messages from sender or receiver (Zero Server Storage)."""
    meta = manager.connection_meta.get(websocket)
    if not meta:
        return

    transfer_id = meta["transfer_id"]
    role = meta["role"]

    try:
        data = json.loads(raw_message)
    except json.JSONDecodeError:
        await websocket.send_text(json.dumps({"type": "error", "message": "Invalid JSON"}))
        return

    msg_type = data.get("type")

    # 1. Ping / Heartbeat (includes RTT client timestamp echo)
    if msg_type == "ping":
        await websocket.send_text(
            json.dumps({
                "type": "pong",
                "transfer_id": transfer_id,
                "client_ts": data.get("client_ts"),
                "server_ts": time.time(),
            })
        )
        return

    # 2. Peer Device & Network Info Exchange
    if msg_type == "peer_info":
        await manager.broadcast(
            transfer_id,
            {
                "type": "peer_info",
                "role": role,
                "transfer_id": transfer_id,
                "device": data.get("device"),
                "os": data.get("os"),
                "browser": data.get("browser"),
                "network": data.get("network"),
                "device_id": data.get("device_id"),
                "location": data.get("location"),
            },
        )
        return

    # 3. Receiver ACK: forward to sender
    if msg_type == "rx_ack":
        await manager.send_to_role(transfer_id, "sender", data)
        return

    # 3. Pause Transfer
    if msg_type == "pause":
        update_transfer_status(transfer_id, "paused")
        await manager.broadcast(
            transfer_id,
            {"type": "peer_paused", "role": role, "transfer_id": transfer_id},
        )
        return

    # 4. Resume Transfer
    if msg_type == "resume":
        update_transfer_status(transfer_id, "transferring")
        await manager.broadcast(
            transfer_id,
            {"type": "peer_resumed", "role": role, "transfer_id": transfer_id},
        )
        return

    # 5. Cancel Transfer
    if msg_type == "cancel":
        update_transfer_status(transfer_id, "cancelled")
        clear_transfer_telemetry(transfer_id)
        await manager.broadcast(
            transfer_id,
            {"type": "peer_cancelled", "role": role, "transfer_id": transfer_id},
        )
        delete_transfer(transfer_id)
        return

    # 6. Real-Time Chunk Relay (ZERO SERVER DISK USAGE)
    if msg_type == "chunk":
        chunk_index = data.get("index")
        total_chunks = data.get("total")
        chunk_data_b64 = data.get("data")
        chunk_hash = data.get("hash")
        chunk_size = data.get("size", len(chunk_data_b64) if chunk_data_b64 else 0)

        if chunk_index is None or chunk_data_b64 is None:
            await websocket.send_text(
                json.dumps({"type": "error", "message": "Missing chunk index or data"})
            )
            return

        record_chunk_throughput(transfer_id, chunk_size)

        # ACK to sender immediately so flow control advances
        await websocket.send_text(
            json.dumps({"type": "chunk_ack", "index": chunk_index, "transfer_id": transfer_id})
        )

        # Forward chunk directly to receiver in real time (streamed straight to receiver disk/RAM)
        await manager.send_to_role(
            transfer_id,
            "receiver",
            {
                "type": "chunk_data",
                "transfer_id": transfer_id,
                "index": chunk_index,
                "total": total_chunks,
                "size": chunk_size,
                "data": chunk_data_b64,
                "hash": chunk_hash,
            },
        )
        return

    # 7. Resume Request: receiver announces missing chunks to sender
    elif msg_type == "resume_request":
        update_transfer_status(transfer_id, "transferring")
        # Forward missing chunk request directly to sender
        missing_chunks = data.get("missing_chunks", [])
        await manager.send_to_role(
            transfer_id,
            "sender",
            {
                "type": "request_missing",
                "transfer_id": transfer_id,
                "missing_chunks": missing_chunks,
            },
        )
        return

    # 8. Transfer Completed
    elif msg_type == "complete":
        update_transfer_status(transfer_id, "completed")
        await manager.broadcast(
            transfer_id,
            {
                "type": "completed",
                "transfer_id": transfer_id,
            },
        )
        return
