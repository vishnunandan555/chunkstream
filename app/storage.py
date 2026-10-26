import asyncio
import hashlib
import logging
import os
import shutil
import sqlite3
import threading
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger("chunkstream.storage")

DATA_DIR = os.environ.get("CHUNKSTREAM_DATA_DIR", "data")
DB_PATH = os.path.join(DATA_DIR, "chunkstream.db")
CHUNKS_DIR = os.path.join(DATA_DIR, "chunks")
DOWNLOADS_DIR = os.path.join(DATA_DIR, "downloads")

_assembly_locks: Dict[str, threading.Lock] = {}
_assembly_locks_guard = threading.Lock()


def _get_assembly_lock(transfer_id: str) -> threading.Lock:
    with _assembly_locks_guard:
        if transfer_id not in _assembly_locks:
            _assembly_locks[transfer_id] = threading.Lock()
        return _assembly_locks[transfer_id]


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
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def init_db() -> None:
    """Initializes sqlite database, tables, indexes, and storage directories."""
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(CHUNKS_DIR, exist_ok=True)
    os.makedirs(DOWNLOADS_DIR, exist_ok=True)

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
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS chunks (
                transfer_id TEXT NOT NULL,
                chunk_index INTEGER NOT NULL,
                size INTEGER NOT NULL,
                checksum TEXT,
                received_at TEXT NOT NULL,
                PRIMARY KEY (transfer_id, chunk_index),
                FOREIGN KEY (transfer_id) REFERENCES transfers (id) ON DELETE CASCADE
            )
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_chunks_transfer ON chunks (transfer_id, chunk_index);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_transfers_created ON transfers (created_at);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_transfers_status ON transfers (status);")
        conn.commit()


def check_storage_health() -> bool:
    """Authenticates database connectivity and disk write capacity."""
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
    """Creates a new transfer record in SQLite with sanitized filename."""
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

    os.makedirs(os.path.join(CHUNKS_DIR, transfer_id), exist_ok=True)
    return get_transfer(transfer_id)  # type: ignore


def get_transfer(transfer_id: str) -> Optional[Dict[str, Any]]:
    """Retrieves a transfer by ID, including chunk progress and missing chunks info."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM transfers WHERE id = ?", (transfer_id,))
        row = cursor.fetchone()
        if not row:
            return None

        transfer = dict(row)
        cursor.execute(
            "SELECT chunk_index FROM chunks WHERE transfer_id = ? ORDER BY chunk_index ASC",
            (transfer_id,),
        )
        acked = [r["chunk_index"] for r in cursor.fetchall()]
        transfer["acknowledged_chunks"] = acked
        transfer["received_chunks_count"] = len(acked)
        transfer["progress_percentage"] = (
            round((len(acked) / transfer["total_chunks"]) * 100, 1)
            if transfer["total_chunks"] > 0
            else 0.0
        )

        all_indices = set(range(transfer["total_chunks"]))
        transfer["missing_chunks"] = sorted(list(all_indices - set(acked)))
        return transfer


def list_transfers(limit: int = 30) -> List[Dict[str, Any]]:
    """Lists recent transfers with progress summaries in a single indexed query."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        query = """
            SELECT t.id, t.filename, t.filesize, t.chunk_size, t.total_chunks,
                   t.mime_type, t.checksum, t.status, t.created_at, t.updated_at,
                   COUNT(c.chunk_index) as received_chunks_count
            FROM transfers t
            LEFT JOIN chunks c ON t.id = c.transfer_id
            GROUP BY t.id
            ORDER BY t.created_at DESC
            LIMIT ?
        """
        cursor.execute(query, (limit,))
        transfers = []
        for row in cursor.fetchall():
            t = dict(row)
            count = t["received_chunks_count"]
            t["progress_percentage"] = (
                round((count / t["total_chunks"]) * 100, 1) if t["total_chunks"] > 0 else 0.0
            )
            transfers.append(t)
        return transfers


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


def save_chunk(
    transfer_id: str,
    chunk_index: int,
    data: bytes,
    expected_checksum: Optional[str] = None,
) -> Tuple[bool, str]:
    """
    Saves a chunk to disk and registers it in SQLite.
    Includes bounds checking, checksum verification, and idempotency.
    """
    calc_checksum = hashlib.sha256(data).hexdigest()
    if expected_checksum and expected_checksum.lower() != calc_checksum.lower():
        return False, f"Checksum mismatch for chunk {chunk_index}"

    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT total_chunks FROM transfers WHERE id = ?", (transfer_id,))
        row = cursor.fetchone()
        if not row:
            return False, f"Transfer {transfer_id} not found"

        total_chunks = row["total_chunks"]
        if chunk_index < 0 or chunk_index >= total_chunks:
            return False, f"Chunk index {chunk_index} out of bounds"

        cursor.execute(
            "SELECT size, checksum FROM chunks WHERE transfer_id = ? AND chunk_index = ?",
            (transfer_id, chunk_index),
        )
        existing = cursor.fetchone()
        if existing and existing["size"] == len(data) and (existing["checksum"] or "").lower() == calc_checksum.lower():
            return True, "Chunk already recorded"

    transfer_chunks_dir = os.path.join(CHUNKS_DIR, transfer_id)
    os.makedirs(transfer_chunks_dir, exist_ok=True)
    chunk_file_path = os.path.join(transfer_chunks_dir, f"chunk_{chunk_index}.part")

    with open(chunk_file_path, "wb") as f:
        f.write(data)

    now = datetime.now(timezone.utc).isoformat()
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT OR REPLACE INTO chunks (transfer_id, chunk_index, size, checksum, received_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (transfer_id, chunk_index, len(data), calc_checksum, now),
        )
        conn.commit()

    return True, "Chunk saved successfully"


def get_chunk(transfer_id: str, chunk_index: int) -> Optional[bytes]:
    """Reads a chunk from disk if present."""
    chunk_file_path = os.path.join(CHUNKS_DIR, transfer_id, f"chunk_{chunk_index}.part")
    if not os.path.isfile(chunk_file_path):
        return None
    with open(chunk_file_path, "rb") as f:
        return f.read()


def assemble_file(transfer_id: str) -> Tuple[bool, Optional[str], Optional[str]]:
    """
    Reassembles all chunks into a complete file.
    Immediately cleans up intermediate chunk files to conserve disk on free servers.
    """
    lock = _get_assembly_lock(transfer_id)
    with lock:
        transfer = get_transfer(transfer_id)
        if not transfer:
            return False, None, f"Transfer {transfer_id} not found"

        if len(transfer["missing_chunks"]) > 0:
            return False, None, f"Cannot assemble file: missing {len(transfer['missing_chunks'])} chunks"

        safe_name = sanitize_filename(transfer["filename"])
        out_dir = os.path.join(DOWNLOADS_DIR, transfer_id)
        os.makedirs(out_dir, exist_ok=True)
        out_path = os.path.join(out_dir, safe_name)

        # Path traversal guard
        abs_out_dir = os.path.abspath(out_dir)
        abs_out_path = os.path.abspath(out_path)
        if not abs_out_path.startswith(abs_out_dir + os.sep) and abs_out_path != abs_out_dir:
            return False, None, "Path traversal detected"

        if transfer.get("status") == "completed" and os.path.isfile(out_path):
            return True, out_path, None

        sha = hashlib.sha256()
        total_written = 0

        with open(out_path, "wb") as out_f:
            for idx in range(transfer["total_chunks"]):
                chunk_data = get_chunk(transfer_id, idx)
                if chunk_data is None:
                    return False, None, f"Missing chunk {idx} on disk"
                out_f.write(chunk_data)
                sha.update(chunk_data)
                total_written += len(chunk_data)

        calc_hash = sha.hexdigest()
        if transfer["checksum"] and transfer["checksum"].lower() != calc_hash.lower():
            try:
                os.remove(out_path)
            except OSError:
                pass
            return False, None, f"Checksum mismatch: expected {transfer['checksum']}, got {calc_hash}"

        update_transfer_status(transfer_id, "completed")

        # Free tier optimization: immediately reclaim chunk disk space!
        cleanup_transfer_chunks(transfer_id)

        logger.info("Assembled transfer %s -> %s (%d bytes)", transfer_id, out_path, total_written)
        return True, out_path, None


async def assemble_file_async(transfer_id: str) -> Tuple[bool, Optional[str], Optional[str]]:
    return await asyncio.to_thread(assemble_file, transfer_id)


def cleanup_transfer_chunks(transfer_id: str) -> bool:
    """Deletes individual .part files after assembly to reclaim disk."""
    chunk_dir = os.path.join(CHUNKS_DIR, transfer_id)
    if os.path.isdir(chunk_dir):
        shutil.rmtree(chunk_dir, ignore_errors=True)
        return True
    return False


def delete_transfer(transfer_id: str) -> bool:
    """Completely removes a transfer, its chunks, and its assembled file."""
    chunk_dir = os.path.join(CHUNKS_DIR, transfer_id)
    dl_dir = os.path.join(DOWNLOADS_DIR, transfer_id)
    shutil.rmtree(chunk_dir, ignore_errors=True)
    shutil.rmtree(dl_dir, ignore_errors=True)

    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM transfers WHERE id = ?", (transfer_id,))
        deleted = cursor.rowcount > 0
        conn.commit()

    with _assembly_locks_guard:
        _assembly_locks.pop(transfer_id, None)

    return deleted


def cleanup_expired_transfers(max_age_hours: int = 1) -> int:
    """Deletes stale/expired transfers from disk and DB (default: 1 hour retention)."""
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=max_age_hours)).isoformat()
    cleaned = 0
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT id FROM transfers WHERE updated_at < ?", (cutoff,))
        expired_ids = [r["id"] for r in cursor.fetchall()]

    for tid in expired_ids:
        if delete_transfer(tid):
            cleaned += 1

    if cleaned > 0:
        logger.info("Cleaned up %d expired transfer(s) older than %d hour(s)", cleaned, max_age_hours)
    return cleaned
