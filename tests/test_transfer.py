import base64
import hashlib
import os
import shutil
import tempfile
import pytest
from fastapi.testclient import TestClient

# Temporary directories for isolation during tests
os.environ["CHUNKSTREAM_DATA_DIR"] = tempfile.mkdtemp(prefix="test_chunkstream_")
os.environ["LOG_LEVEL"] = "DEBUG"

from app.main import app
from app.storage import (
    DATA_DIR,
    cleanup_expired_transfers,
    create_transfer,
    delete_transfer,
    get_transfer,
    init_db,
)


@pytest.fixture(autouse=True)
def setup_teardown():
    init_db()
    yield


client = TestClient(app)


def test_health_check_healthy():
    response = client.get("/api/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "healthy"
    assert data["service"] == "chunkstream"
    assert data["zero_server_storage"] is True


def test_version_endpoint():
    response = client.get("/api/version")
    assert response.status_code == 200
    data = response.json()
    assert "version" in data
    assert data["service"] == "chunkstream"
    assert data["zero_server_storage"] is True


def test_transfer_metadata_and_zero_server_disk():
    transfer_id = "PROD-META-01"
    record = create_transfer(
        transfer_id=transfer_id,
        filename="big_archive.zip",
        filesize=5 * 1024 * 1024 * 1024,  # 5 GB virtual file
        chunk_size=524288,
        total_chunks=10000,
    )

    assert record["id"] == transfer_id
    assert record["filename"] == "big_archive.zip"
    assert record["filesize"] == 5 * 1024 * 1024 * 1024

    # Verify: ZERO chunk files and ZERO download directories exist on server disk!
    assert not os.path.exists(os.path.join(DATA_DIR, "chunks"))
    assert not os.path.exists(os.path.join(DATA_DIR, "downloads"))


def test_path_traversal_sanitization():
    transfer_id = "PROD-TRAVERSAL"
    init_res = client.post(
        "/api/transfers",
        json={
            "transfer_id": transfer_id,
            "filename": "../../../../etc/passwd",
            "filesize": 12,
            "chunk_size": 6,
            "total_chunks": 2,
        },
    )
    assert init_res.status_code == 200
    data = init_res.json()
    assert data["filename"] == "passwd"
    assert ".." not in data["filename"]


def test_prometheus_metrics_endpoint():
    res = client.get("/metrics")
    assert res.status_code == 200
    assert "text/plain" in res.headers["content-type"]
    assert "http_requests_total" in res.text


def test_delete_transfer_endpoint():
    transfer_id = "PROD-DELETE-01"
    create_transfer(
        transfer_id=transfer_id,
        filename="delete_me.txt",
        filesize=5,
        chunk_size=5,
        total_chunks=1,
    )

    # DELETE endpoint removes metadata record
    del_res = client.delete(f"/api/transfers/{transfer_id}")
    assert del_res.status_code == 200
    assert del_res.json()["status"] == "deleted"

    assert get_transfer(transfer_id) is None


def test_zero_storage_websocket_live_relay():
    """
    Validates live chunk relay: sender sends chunk -> server routes directly to receiver.
    Zero chunk files are written to server disk.
    """
    transfer_id = "PROD-ZERO-STORE-WS"
    chunk_data = b"STREAMING_CHUNK_PAYLOAD_DIRECT_TO_RECEIVER"
    b64_data = base64.b64encode(chunk_data).decode("utf-8")

    create_transfer(
        transfer_id=transfer_id,
        filename="live_stream.dat",
        filesize=len(chunk_data),
        chunk_size=len(chunk_data),
        total_chunks=1,
    )

    # 1. Receiver connects
    with client.websocket_connect(f"/ws/transfer/{transfer_id}?role=receiver") as rx_ws:
        _ = rx_ws.receive_json()  # peer_connected

        # 2. Sender connects
        with client.websocket_connect(f"/ws/transfer/{transfer_id}?role=sender") as sender_ws:
            _ = sender_ws.receive_json()  # peer_connected on sender
            _ = rx_ws.receive_json()      # peer_connected on receiver

            # Sender streams chunk live
            sender_ws.send_json({
                "type": "chunk",
                "index": 0,
                "total": 1,
                "size": len(chunk_data),
                "data": b64_data,
                "hash": hashlib.sha256(chunk_data).hexdigest(),
            })

            # Sender gets immediate ACK
            sender_ack = sender_ws.receive_json()
            assert sender_ack["type"] == "chunk_ack"
            assert sender_ack["index"] == 0

            # Receiver receives chunk in real time directly from WebSocket
            rx_chunk = rx_ws.receive_json()
            assert rx_chunk["type"] == "chunk_data"
            assert rx_chunk["index"] == 0
            assert rx_chunk["data"] == b64_data

            # Receiver decodes and matches exact payload
            decoded = base64.b64decode(rx_chunk["data"])
            assert decoded == chunk_data

            # Confirm ZERO chunk files on server disk
            assert not os.path.exists(os.path.join(DATA_DIR, "chunks"))
            assert not os.path.exists(os.path.join(DATA_DIR, "downloads"))

            # Receiver ACKs
            rx_ws.send_json({"type": "rx_ack", "index": 0})
            sender_rx_ack = sender_ws.receive_json()
            assert sender_rx_ack["type"] == "rx_ack"

            # Sender completes
            sender_ws.send_json({"type": "complete"})
            sender_completed = sender_ws.receive_json()
            assert sender_completed["type"] == "completed"
            rx_completed = rx_ws.receive_json()
            assert rx_completed["type"] == "completed"


def test_pause_and_resume_websocket():
    transfer_id = "PROD-PAUSE-RESUME"
    create_transfer(
        transfer_id=transfer_id,
        filename="pause_test.txt",
        filesize=20,
        chunk_size=10,
        total_chunks=2,
    )

    with client.websocket_connect(f"/ws/transfer/{transfer_id}?role=receiver") as rx_ws:
        _ = rx_ws.receive_json()

        with client.websocket_connect(f"/ws/transfer/{transfer_id}?role=sender") as sender_ws:
            _ = sender_ws.receive_json()
            _ = rx_ws.receive_json()

            # Sender pauses
            sender_ws.send_json({"type": "pause"})
            assert sender_ws.receive_json()["type"] == "peer_paused"
            assert rx_ws.receive_json()["type"] == "peer_paused"

            t = get_transfer(transfer_id)
            assert t["status"] == "paused"

            # Sender resumes
            sender_ws.send_json({"type": "resume"})
            assert sender_ws.receive_json()["type"] == "peer_resumed"
            assert rx_ws.receive_json()["type"] == "peer_resumed"

            t = get_transfer(transfer_id)
            assert t["status"] == "transferring"


def test_cancel_transfer_websocket():
    transfer_id = "PROD-CANCEL-TEST"
    create_transfer(
        transfer_id=transfer_id,
        filename="cancel_test.txt",
        filesize=10,
        chunk_size=5,
        total_chunks=2,
    )

    with client.websocket_connect(f"/ws/transfer/{transfer_id}?role=receiver") as rx_ws:
        _ = rx_ws.receive_json()

        with client.websocket_connect(f"/ws/transfer/{transfer_id}?role=sender") as sender_ws:
            _ = sender_ws.receive_json()
            _ = rx_ws.receive_json()

            # Sender cancels
            sender_ws.send_json({"type": "cancel"})
            assert sender_ws.receive_json()["type"] == "peer_cancelled"
            assert rx_ws.receive_json()["type"] == "peer_cancelled"

            # Session is deleted
            assert get_transfer(transfer_id) is None
