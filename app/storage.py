import hashlib
import json
import logging
import os
import sqlite3
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

logger = logging.getLogger("chunkstream.storage")

DATA_DIR = os.environ.get("CHUNKSTREAM_DATA_DIR", "data")
DB_PATH = os.path.join(DATA_DIR, "chunkstream.db")
CHUNKS_DIR = os.path.join(DATA_DIR, "chunks")
DOWNLOADS_DIR = os.path.join(DATA_DIR, "downloads")


def get_db_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    """Initializes sqlite database and storage directories."""
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
    """Creates a new transfer record in SQLite."""
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
                filename,
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

    # Create transfer chunks directory
    transfer_chunks_dir = os.path.join(CHUNKS_DIR, transfer_id)
    os.makedirs(transfer_chunks_dir, exist_ok=True)

    logger.info(
        "Created transfer %s: file='%s' (%d bytes, %d chunks @ %d bytes)",
        transfer_id,
        filename,
        filesize,
        total_chunks,
        chunk_size,
    )
    return get_transfer(transfer_id)  # type: ignore


def get_transfer(transfer_id: str) -> Optional[Dict[str, Any]]:
    """Retrieves a transfer by ID, including chunk progress info."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM transfers WHERE id = ?", (transfer_id,))
        row = cursor.fetchone()
        if not row:
            return None

        transfer = dict(row)

        # Get acknowledged chunks
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


def list_transfers(limit: int = 50) -> List[Dict[str, Any]]:
    """Lists recent transfers."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM transfers ORDER BY created_at DESC LIMIT ?", (limit,))
        transfers = []
        for row in cursor.fetchall():
            t = dict(row)
            cursor.execute(
                "SELECT COUNT(*) as count FROM chunks WHERE transfer_id = ?",
                (t["id"],),
            )
            count = cursor.fetchone()["count"]
            t["received_chunks_count"] = count
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
    Returns (success, message).
    """
    transfer = get_transfer(transfer_id)
    if not transfer:
        return False, f"Transfer {transfer_id} not found"

    if chunk_index < 0 or chunk_index >= transfer["total_chunks"]:
        return False, f"Chunk index {chunk_index} out of bounds (0-{transfer['total_chunks'] - 1})"

    calc_checksum = hashlib.sha256(data).hexdigest()
    if expected_checksum and expected_checksum.lower() != calc_checksum.lower():
        msg = f"Checksum mismatch for chunk {chunk_index}: got {calc_checksum[:8]}, expected {expected_checksum[:8]}"
        logger.warning(msg)
        return False, msg

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
    Returns (success, assembled_path, error_message).
    """
    transfer = get_transfer(transfer_id)
    if not transfer:
        return False, None, f"Transfer {transfer_id} not found"

    if len(transfer["missing_chunks"]) > 0:
        return (
            False,
            None,
            f"Cannot assemble file: missing chunks {transfer['missing_chunks'][:10]} (total missing: {len(transfer['missing_chunks'])})",
        )

    out_dir = os.path.join(DOWNLOADS_DIR, transfer_id)
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, transfer["filename"])

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
