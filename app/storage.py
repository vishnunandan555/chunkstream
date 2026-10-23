import asyncio
import hashlib
import json
import logging
import os
import re
import shutil
import sqlite3
import threading
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Set, Tuple

logger = logging.getLogger("chunkstream.storage")

DATA_DIR = os.environ.get("CHUNKSTREAM_DATA_DIR", "data")
DB_PATH = os.path.join(DATA_DIR, "chunkstream.db")
CHUNKS_DIR = os.path.join(DATA_DIR, "chunks")
DOWNLOADS_DIR = os.path.join(DATA_DIR, "downloads")

# Mutex dictionary to serialize concurrent assembly calls per transfer_id
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
    for bad in ("/", "\\", ":", "*", "?", '"' , "<", ">", "|"):
        base = base.replace(bad, "_")
    clean = base.strip(" .")
    return clean or "unnamed_transfer.bin"

def get_db_connection() -> sqlite3.Connection:
    """
    Returns a configured SQLite connection with WAL mode, busy timeout,
    and foreign keys enabled to maximize concurrency and reliability.
    """
    conn = sqlite3.connect(DB_PATH, timeout=30.0, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode = WAL;")
    conn.execute("PRAGMA synchronous = NORMAL;")
    conn.execute("PRAGMA busy_timeout = 30000;")
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
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_chunks_transfer ON chunks (transfer_id, chunk_index);
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_transfers_created ON transfers (created_at);
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_transfers_status ON transfers (status);
        """)
        conn.commit()
    logger.info("Storage initialized (DB: %s, Chunks: %s)", DB_PATH, CHUNKS_DIR)


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

    transfer_chunks_dir = os.path.join(CHUNKS_DIR, transfer_id)
    os.makedirs(transfer_chunks_dir, exist_ok=True)

    logger.info(
        "Created transfer %s: file='%s' (%d bytes, %d chunks @ %d bytes)",
        transfer_id,
        safe_filename,
        filesize,
        total_chunks,
        chunk_size,
    )
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
        missing = sorted(list(all_indices - set(acked)))
        transfer["missing_chunks"] = missing

        return transfer


def list_transfers(limit: int = 50, summary_only: bool = True) -> List[Dict[str, Any]]:
    """
    Lists recent transfers.
    Uses an optimized single SQL query with count subquery rather than N+1 queries.
    """
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
    logger.info("Transfer %s status updated to: %s", transfer_id, status)


def save_chunk(
    transfer_id: str,
    chunk_index: int,
    data: bytes,
    expected_checksum: Optional[str] = None,
) -> Tuple[bool, str]:
    """
    Saves a chunk to disk and registers it in SQLite.
    Optimized: Avoids N+1 full get_transfer query and provides idempotency checks.
    Returns (success, message).
    """
    calc_checksum = hashlib.sha256(data).hexdigest()
    if expected_checksum and expected_checksum.lower() != calc_checksum.lower():
        msg = f"Checksum mismatch for chunk {chunk_index}: got {calc_checksum[:8]}, expected {expected_checksum[:8]}"
        logger.warning(msg)
        return False, msg

    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT total_chunks FROM transfers WHERE id = ?", (transfer_id,))
        row = cursor.fetchone()
        if not row:
            return False, f"Transfer {transfer_id} not found"

        total_chunks = row["total_chunks"]
        if chunk_index < 0 or chunk_index >= total_chunks:
            return False, f"Chunk index {chunk_index} out of bounds (0-{total_chunks - 1})"

        # Idempotency check: if chunk is already stored with identical checksum and size
        cursor.execute(
            "SELECT size, checksum FROM chunks WHERE transfer_id = ? AND chunk_index = ?",
            (transfer_id, chunk_index),
        )
        existing = cursor.fetchone()
        if existing and existing["size"] == len(data) and (existing["checksum"] or "").lower() == calc_checksum.lower():
            return True, "Chunk already recorded (idempotent)"

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

    logger.debug(
        "Saved chunk %d for transfer %s (%d bytes, sha256=%s)",
        chunk_index,
        transfer_id,
        len(data),
        calc_checksum[:8],
    )
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
    Reassembles all chunks in sequential order into a complete file.
    Guarded with per-transfer mutex to avoid concurrent race conditions.
    Validates destination path against path traversal attacks.
    Returns (success, assembled_path, error_message).
    """
    lock = _get_assembly_lock(transfer_id)
    with lock:
        transfer = get_transfer(transfer_id)
        if not transfer:
            return False, None, f"Transfer {transfer_id} not found"

        if len(transfer["missing_chunks"]) > 0:
            return (
                False,
                None,
                f"Cannot assemble file: missing chunks {transfer['missing_chunks'][:10]} (total missing: {len(transfer['missing_chunks'])})",
            )

        safe_name = sanitize_filename(transfer["filename"])
        out_dir = os.path.join(DOWNLOADS_DIR, transfer_id)
        os.makedirs(out_dir, exist_ok=True)
        out_path = os.path.join(out_dir, safe_name)

        # Ensure path does not escape destination directory
        abs_out_dir = os.path.abspath(out_dir)
        abs_out_path = os.path.abspath(out_path)
        if not abs_out_path.startswith(abs_out_dir + os.sep) and abs_out_path != abs_out_dir:
            return False, None, "Path traversal detected in transfer filename"

        # If already assembled successfully and matches checksum, return early
        if transfer.get("status") == "completed" and os.path.isfile(out_path):
            return True, out_path, None

        sha = hashlib.sha256()
        total_written = 0

        with open(out_path, "wb") as out_f:
            for idx in range(transfer["total_chunks"]):
                chunk_data = get_chunk(transfer_id, idx)
                if chunk_data is None:
                    return False, None, f"Missing chunk file on disk for index {idx}"
                out_f.write(chunk_data)
                sha.update(chunk_data)
                total_written += len(chunk_data)

        calc_hash = sha.hexdigest()
        if transfer["checksum"] and transfer["checksum"].lower() != calc_hash.lower():
            msg = f"Assembled file checksum mismatch: got {calc_hash}, expected {transfer['checksum']}"
            logger.error(msg)
            try:
                os.remove(out_path)
            except OSError:
                pass
            return False, None, msg

        update_transfer_status(transfer_id, "completed")
        logger.info(
            "Successfully assembled transfer %s -> %s (%d bytes, sha256=%s)",
            transfer_id,
            out_path,
            total_written,
            calc_hash[:8],
        )
        return True, out_path, None


async def assemble_file_async(transfer_id: str) -> Tuple[bool, Optional[str], Optional[str]]:
    """Asynchronously reassembles file in a thread pool without blocking the event loop."""
    return await asyncio.to_thread(assemble_file, transfer_id)


def cleanup_expired_transfers(max_age_hours: int = 24) -> int:
    """
    Deletes stale/expired transfers and associated chunk files from disk and SQLite.
    Returns the count of cleaned up transfers.
    """
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=max_age_hours)).isoformat()
    cleaned_count = 0
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT id FROM transfers WHERE updated_at < ?",
            (cutoff,),
        )
        expired_ids = [r["id"] for r in cursor.fetchall()]

        for tid in expired_ids:
            # Remove chunk files
            chunk_dir = os.path.join(CHUNKS_DIR, tid)
            if os.path.isdir(chunk_dir):
                shutil.rmtree(chunk_dir, ignore_errors=True)
            # Remove assembled files
            dl_dir = os.path.join(DOWNLOADS_DIR, tid)
            if os.path.isdir(dl_dir):
                shutil.rmtree(dl_dir, ignore_errors=True)
            # Remove from DB
            cursor.execute("DELETE FROM transfers WHERE id = ?", (tid,))
            cleaned_count += 1

        conn.commit()

    if cleaned_count > 0:
        logger.info("Cleaned up %d expired transfers older than %d hours", cleaned_count, max_age_hours)
    return cleaned_count


def cleanup_transfer_chunks(transfer_id: str) -> bool:
    """Cleans up individual .part chunk files after successful assembly to reclaim disk."""
    chunk_dir = os.path.join(CHUNKS_DIR, transfer_id)
    if os.path.isdir(chunk_dir):
        shutil.rmtree(chunk_dir, ignore_errors=True)
        logger.info("Reclaimed chunk disk space for completed transfer %s", transfer_id)
        return True
    return False
