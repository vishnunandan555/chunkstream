import hashlib
import logging
import os
import sqlite3
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional

logger = logging.getLogger("chunkstream.storage")

DATA_DIR = os.environ.get("CHUNKSTREAM_DATA_DIR", "data")
DB_PATH = os.path.join(DATA_DIR, "chunkstream.db")


def sanitize_filename(filename: str) -> str:
    """Sanitizes filename safely against directory traversal."""
    if not filename:
        return "unnamed_transfer.bin"
    base = os.path.basename(filename.strip())
    base = "".join(c for c in base if ord(c) >= 32 and c != 127)
    for bad in ("/", "\\", ":", "*", "?", '"', "<", ">", "|"):
        base = base.replace(bad, "_")
    clean = base.strip(" .")
    return clean or "unnamed_transfer.bin"


def get_db_connection() -> sqlite3.Connection:
    """Configures SQLite connection with WAL mode and fast busy timeout."""
    conn = sqlite3.connect(DB_PATH, timeout=15.0, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode = WAL;")
    conn.execute("PRAGMA synchronous = NORMAL;")
    conn.execute("PRAGMA busy_timeout = 15000;")
    return conn


def init_db() -> None:
    """Initializes sqlite database and transfers metadata table. Zero file storage on disk."""
    os.makedirs(DATA_DIR, exist_ok=True)

    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS transfers (
                id TEXT PRIMARY KEY,
                filename TEXT NOT NULL,
                filesize INTEGER NOT NULL,
                chunk_size INTEGER NOT NULL,
                total_chunks INTEGER NOT NULL,
                mime_type TEXT,
                checksum TEXT,
                status TEXT NOT NULL DEFAULT 'pending',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_transfers_created ON transfers (created_at);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_transfers_status ON transfers (status);")
        conn.commit()


def check_storage_health() -> bool:
    """Authenticates database connectivity."""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
        return True
    except Exception as exc:
        logger.error("Storage health check failure: %s", exc)
        return False


def create_transfer(
    transfer_id: str,
    filename: str,
    filesize: int,
    chunk_size: int,
    total_chunks: int,
    mime_type: Optional[str] = None,
    checksum: Optional[str] = None,
) -> Dict[str, Any]:
    """Creates a new transfer metadata record. Zero chunk files are created."""
    safe_filename = sanitize_filename(filename)
    now = datetime.now(timezone.utc).isoformat()
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO transfers (
                id, filename, filesize, chunk_size, total_chunks, mime_type, checksum, status, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?)
            """,
            (
                transfer_id,
                safe_filename,
                filesize,
                chunk_size,
                total_chunks,
                mime_type or "application/octet-stream",
                checksum or "",
                now,
                now,
            ),
        )
        conn.commit()

    return get_transfer(transfer_id)  # type: ignore


def get_transfer(transfer_id: str) -> Optional[Dict[str, Any]]:
    """Retrieves transfer session metadata by ID."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM transfers WHERE id = ?", (transfer_id,))
        row = cursor.fetchone()
        if not row:
            return None
        return dict(row)


def list_transfers(limit: int = 30) -> List[Dict[str, Any]]:
    """Lists recent transfer sessions."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT id, filename, filesize, chunk_size, total_chunks, mime_type, checksum, status, created_at, updated_at
            FROM transfers
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (limit,),
        )
        return [dict(row) for row in cursor.fetchall()]


def update_transfer_status(transfer_id: str, status: str) -> None:
    """Updates transfer status."""
    now = datetime.now(timezone.utc).isoformat()
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "UPDATE transfers SET status = ?, updated_at = ? WHERE id = ?",
            (status, now, transfer_id),
        )
        conn.commit()


def delete_transfer(transfer_id: str) -> bool:
    """Removes a transfer session record from SQLite."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM transfers WHERE id = ?", (transfer_id,))
        deleted = cursor.rowcount > 0
        conn.commit()
    return deleted


def cleanup_expired_transfers(
    max_age_minutes: int = 15,
    max_age_hours: Optional[int] = None,
) -> int:
    """Deletes stale/expired transfer metadata records (default: 15 minutes retention, 2 mins for completed/cancelled)."""
    if max_age_hours is not None:
        delta = timedelta(hours=max_age_hours)
    else:
        delta = timedelta(minutes=max_age_minutes)

    cutoff = (datetime.now(timezone.utc) - delta).isoformat()
    # Completed or cancelled transfers only need 2 minutes grace period
    completed_cutoff = (datetime.now(timezone.utc) - timedelta(minutes=2)).isoformat()

    cleaned = 0
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT id FROM transfers
            WHERE updated_at < ?
               OR (status IN ('completed', 'cancelled') AND updated_at < ?)
            """,
            (cutoff, completed_cutoff),
        )
        expired_ids = [r["id"] for r in cursor.fetchall()]

    for tid in expired_ids:
        if delete_transfer(tid):
            cleaned += 1

    if cleaned > 0:
        logger.info("Cleaned up %d expired/finished transfer record(s) from server database", cleaned)
    return cleaned


def prune_transfers_db() -> Dict[str, Any]:
    """Truncates SQLite WAL log and optimizes query plans to reclaim memory and disk space."""
    wal_status: List[Any] = []
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("PRAGMA wal_checkpoint(TRUNCATE);")
            res = cursor.fetchone()
            if res:
                wal_status = list(res)
            cursor.execute("PRAGMA optimize;")
            conn.commit()
    except Exception as exc:
        logger.warning("Error during SQLite WAL checkpoint prune: %s", exc)

    return {"wal_checkpoint": wal_status}


def perform_full_cleanup(max_age_minutes: int = 15) -> Dict[str, Any]:
    """Runs automated server cleanup: purges stale records and checkpoints the SQLite database."""
    cleaned = cleanup_expired_transfers(max_age_minutes=max_age_minutes)
    maintenance = prune_transfers_db()
    return {
        "cleaned_transfers": cleaned,
        "maintenance": maintenance,
    }

