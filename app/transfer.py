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
        self.connection_locks: Dict[WebSocket, asyncio.Lock] = {}

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
        self.connection_locks[websocket] = asyncio.Lock()

        client_host = getattr(websocket.client, "host", "unknown") if websocket.client else "unknown"
        logger.info(
            "[%s] Peer connected: role=%s from %s (total peers in session: %d)",
            transfer_id,
            role.upper(),
            client_host,
            len(self.active_connections[transfer_id]),
        )

        await self.broadcast(
            transfer_id,
            {
                "type": "peer_connected",
                "role": role,
                "peers_count": len(self.active_connections[transfer_id]),
            },
        )

    def disconnect(self, websocket: WebSocket) -> None:
        self.connection_locks.pop(websocket, None)
        meta = self.connection_meta.pop(websocket, None)
        if meta:
            transfer_id = meta["transfer_id"]
            role = meta["role"]
            if transfer_id in self.active_connections:
                self.active_connections[transfer_id].discard(websocket)
                peer_count = len(self.active_connections[transfer_id])
                logger.info(
                    "[%s] Peer disconnected: role=%s (remaining peers in session: %d)",
                    transfer_id,
                    role.upper(),
                    peer_count,
                )
                if peer_count == 0:
                    del self.active_connections[transfer_id]
                    clear_transfer_telemetry(transfer_id)
                    logger.info("[%s] All peers disconnected. In-memory telemetry cleared.", transfer_id)

                    # Auto-purge finished transfers from SQLite, or schedule cleanup for abandoned sessions
                    record = get_transfer(transfer_id)
                    status = record.get("status") if record else None
                    if status in ("completed", "cancelled"):
                        delete_transfer(transfer_id)
                        logger.info("[%s] Finished transfer (%s) removed from server storage.", transfer_id, status)
                    else:
                        async def _delayed_abandonment_cleanup(tid: str, delay_sec: int = 300) -> None:
                            try:
                                await asyncio.sleep(delay_sec)
                                if tid not in self.active_connections or len(self.active_connections[tid]) == 0:
                                    if delete_transfer(tid):
                                        clear_transfer_telemetry(tid)
                                        logger.info(
                                            "[%s] Abandoned session automatically purged from database (no reconnect in %ds).",
                                            tid,
                                            delay_sec,
                                        )
                            except asyncio.CancelledError:
                                pass
                            except Exception as exc:
                                logger.warning("[%s] Error in delayed session cleanup: %s", tid, exc)

                        asyncio.create_task(_delayed_abandonment_cleanup(transfer_id))

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

    async def safe_send(self, websocket: WebSocket, payload: str) -> bool:
        """Sends a WebSocket text payload safely under a per-connection lock to avoid concurrency errors."""
        lock = self.connection_locks.get(websocket)
        if lock is None:
            return False
        async with lock:
            try:
                await websocket.send_text(payload)
                return True
            except Exception as exc:
                logger.warning("Failed to deliver WebSocket text message: %s", exc)
                return False

    async def broadcast(self, transfer_id: str, message: Dict[str, Any]) -> None:
        if transfer_id not in self.active_connections:
            return
        payload = json.dumps(message)
        sockets = list(self.active_connections.get(transfer_id, ()))
        for ws in sockets:
            ok = await self.safe_send(ws, payload)
            if not ok:
                self.disconnect(ws)

    async def send_to_role(
        self, transfer_id: str, target_role: str, message: Dict[str, Any]
    ) -> bool:
        if transfer_id not in self.active_connections:
            return False
        target_sockets = [
            ws
            for ws in list(self.active_connections.get(transfer_id, ()))
            if self.connection_meta.get(ws, {}).get("role") == target_role
        ]
        if not target_sockets:
            return False
        payload = json.dumps(message)
        for ws in target_sockets:
            ok = await self.safe_send(ws, payload)
            if not ok:
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
        device_label = data.get("display_name") or f"{data.get('device_model') or data.get('device') or 'Device'} ({data.get('browser') or 'Browser'})"
        logger.info(
            "[%s] Peer Identified: %s (Role: %s, OS: %s, Browser: %s)",
            transfer_id,
            device_label,
            role.upper(),
            data.get("os", "unknown"),
            data.get("browser", "unknown"),
        )
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
    if msg_type in ("rx_ack", "chunk_ack"):
        await manager.send_to_role(transfer_id, "sender", data)
        return

    # 4. WebRTC Direct LAN Signaling (Offer, Answer, ICE Candidates)
    if msg_type in ("signal_offer", "signal_answer", "signal_candidate"):
        target_role = "receiver" if role == "sender" else "sender"
        await manager.send_to_role(transfer_id, target_role, data)
        return

    # 3. Pause Transfer
    if msg_type == "pause":
        logger.info("[%s] Transfer paused by %s", transfer_id, role.upper())
        update_transfer_status(transfer_id, "paused")
        await manager.broadcast(
            transfer_id,
            {"type": "peer_paused", "role": role, "transfer_id": transfer_id},
        )
        return

    # 4. Resume Transfer
    if msg_type == "resume":
        logger.info("[%s] Transfer resumed by %s", transfer_id, role.upper())
        update_transfer_status(transfer_id, "transferring")
        await manager.broadcast(
            transfer_id,
            {"type": "peer_resumed", "role": role, "transfer_id": transfer_id},
        )
        return

    # 5. Cancel Transfer
    if msg_type == "cancel":
        logger.info("[%s] Transfer cancelled by %s", transfer_id, role.upper())
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

        # If receiver is not connected, notify sender and do not ACK
        if not manager.has_role(transfer_id, "receiver"):
            logger.warning("[%s] Chunk %s dropped: receiver is NOT connected", transfer_id, chunk_index)
            await websocket.send_text(
                json.dumps({
                    "type": "peer_disconnected",
                    "role": "receiver",
                    "message": "Receiver not connected",
                })
            )
            return

        record_chunk_throughput(transfer_id, chunk_size)

        # Periodic log for Render live tail visibility (first 5 chunks, every 20, and last)
        if total_chunks and (chunk_index < 5 or (chunk_index + 1) % 20 == 0 or (chunk_index + 1) == total_chunks):
            telem = _transfer_telemetry.get(transfer_id, {})
            total_b = telem.get("total_bytes", chunk_size)
            elapsed = max(0.001, time.perf_counter() - telem.get("start_time", time.perf_counter()))
            speed_mb = (total_b / elapsed) / (1024 * 1024)
            pct = round(((chunk_index + 1) / total_chunks) * 100)
            logger.info(
                "[%s] Relaying chunk %d/%d (%d%%) - %d KB - Live Speed: %.1f MB/s",
                transfer_id,
                chunk_index + 1,
                total_chunks,
                pct,
                round(chunk_size / 1024),
                speed_mb,
            )

        # Forward chunk directly to receiver with flow-control (synchronous zero-buffering relay)
        delivered = await manager.send_to_role(
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

        if delivered:
            # Acknowledge to sender that chunk was delivered directly to receiver
            await manager.safe_send(
                websocket,
                json.dumps({"type": "chunk_ack", "index": chunk_index, "transfer_id": transfer_id}),
            )
        else:
            logger.warning("[%s] Chunk %d delivery failed: receiver disconnected", transfer_id, chunk_index)
        return

    # 7. Resume Request / Missing Chunk Pull
    elif msg_type in ("resume_request", "request_missing"):
        update_transfer_status(transfer_id, "transferring")
        # Forward missing chunk request directly to sender
        missing_chunks = data.get("missing_chunks", [])
        logger.info("[%s] Resume requested by receiver: %d missing chunks", transfer_id, len(missing_chunks))
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
        logger.info("[%s] Transfer complete! All chunks relayed live to receiver.", transfer_id)
        update_transfer_status(transfer_id, "completed")
        await manager.broadcast(
            transfer_id,
            {
                "type": "completed",
                "transfer_id": transfer_id,
            },
        )

        async def _delayed_completed_cleanup(tid: str, delay_sec: int = 60) -> None:
            try:
                await asyncio.sleep(delay_sec)
                # Purge completed transfer record and telemetry after grace period
                delete_transfer(tid)
                clear_transfer_telemetry(tid)
                logger.info("[%s] Completed transfer pruned from database after %ds grace period.", tid, delay_sec)
            except asyncio.CancelledError:
                pass
            except Exception as exc:
                logger.warning("[%s] Error in completed session cleanup: %s", tid, exc)

        asyncio.create_task(_delayed_completed_cleanup(transfer_id))
        return
