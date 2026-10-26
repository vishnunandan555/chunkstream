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
    CHUNKS_DIR,
    assemble_file,
    cleanup_expired_transfers,
    create_transfer,
    delete_transfer,
    get_chunk,
    get_transfer,
    init_db,
    save_chunk,
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


def test_version_endpoint():
    response = client.get("/api/version")
    assert response.status_code == 200
    data = response.json()
    assert "version" in data
    assert data["service"] == "chunkstream"


def test_transfer_lifecycle_and_chunk_cleanup():
    # 100 KB payload
    payload = b"A" * 32768 + b"B" * 32768 + b"C" * 34464
    chunk_size = 32768
    total_chunks = 3
    file_hash = hashlib.sha256(payload).hexdigest()

    transfer_id = "PROD-FLOW-01"
    record = create_transfer(
        transfer_id=transfer_id,
        filename="test_payload.bin",
        filesize=len(payload),
        chunk_size=chunk_size,
        total_chunks=total_chunks,
        checksum=file_hash,
    )

    assert record["id"] == transfer_id
    assert record["total_chunks"] == 3
    assert record["missing_chunks"] == [0, 1, 2]

    # Save chunk 0
    ok, _ = save_chunk(transfer_id, 0, payload[0:32768])
    assert ok is True

    # Missing should be [1, 2]
    t_state = get_transfer(transfer_id)
    assert t_state["acknowledged_chunks"] == [0]
    assert t_state["missing_chunks"] == [1, 2]

    # Assembly must fail while chunks missing
    ok, path, err = assemble_file(transfer_id)
    assert ok is False

    # Save remaining chunks
    assert save_chunk(transfer_id, 1, payload[32768:65536])[0] is True
    assert save_chunk(transfer_id, 2, payload[65536:])[0] is True

    # Assembly should now succeed
    ok, path, err = assemble_file(transfer_id)
    assert ok is True
    assert path is not None
    assert err is None

    with open(path, "rb") as f:
        assembled_data = f.read()
    assert hashlib.sha256(assembled_data).hexdigest() == file_hash

    # Crucial free-server check: intermediate chunk .part directory must be deleted!
    chunk_dir = os.path.join(CHUNKS_DIR, transfer_id)
    assert not os.path.exists(chunk_dir)


def test_http_chunk_upload_api():
    transfer_id = "PROD-API-02"
    init_res = client.post(
        "/api/transfers",
        json={
            "transfer_id": transfer_id,
            "filename": "api_test.txt",
            "filesize": 10,
            "chunk_size": 5,
            "total_chunks": 2,
        },
    )
    assert init_res.status_code == 200

    # Upload chunks via HTTP
    c0 = client.post(f"/api/transfers/{transfer_id}/chunks/0", content=b"hello")
    assert c0.status_code == 200
    c1 = client.post(f"/api/transfers/{transfer_id}/chunks/1", content=b"world")
    assert c1.status_code == 200

    # Trigger assembly
    asm_res = client.post(f"/api/transfers/{transfer_id}/assemble")
    assert asm_res.status_code == 200

    # Download assembled file
    dl_res = client.get(f"/api/transfers/{transfer_id}/download")
    assert dl_res.status_code == 200
    assert dl_res.content == b"helloworld"


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

    client.post(f"/api/transfers/{transfer_id}/chunks/0", content=b"root:x")
    client.post(f"/api/transfers/{transfer_id}/chunks/1", content=b":0:0::")
    client.post(f"/api/transfers/{transfer_id}/assemble")

    dl_res = client.get(f"/api/transfers/{transfer_id}/download")
    assert dl_res.status_code == 200
    assert dl_res.content == b"root:x:0:0::"


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
    save_chunk(transfer_id, 0, b"trash")

    # DELETE endpoint
    del_res = client.delete(f"/api/transfers/{transfer_id}")
    assert del_res.status_code == 200
    assert del_res.json()["status"] == "deleted"

    # Transfer should no longer exist
    assert get_transfer(transfer_id) is None


def test_websocket_transfer_and_resume():
    transfer_id = "PROD-WS-RESUME"
    chunk_payloads = [b"AAA_", b"BBB_", b"CCC_", b"DDD_"]
    full_payload = b"".join(chunk_payloads)
    file_hash = hashlib.sha256(full_payload).hexdigest()

    create_transfer(
        transfer_id=transfer_id,
        filename="resume_test.txt",
        filesize=len(full_payload),
        chunk_size=4,
        total_chunks=4,
        checksum=file_hash,
    )

    # 1. Receiver connects first
    with client.websocket_connect(f"/ws/transfer/{transfer_id}?role=receiver") as rx_ws:
        _ = rx_ws.receive_json()  # peer_connected

        # 2. Sender connects
        with client.websocket_connect(f"/ws/transfer/{transfer_id}?role=sender") as sender_ws:
            _ = sender_ws.receive_json()  # peer_connected on sender
            _ = rx_ws.receive_json()      # peer_connected on receiver

            # Stream chunk 0
            sender_ws.send_json({
                "type": "chunk",
                "index": 0,
                "total": 4,
                "data": base64.b64encode(chunk_payloads[0]).decode("utf-8"),
                "hash": hashlib.sha256(chunk_payloads[0]).hexdigest(),
            })

            _ = sender_ws.receive_json()  # chunk_ack
            _ = sender_ws.receive_json()  # progress
            rx_chunk = rx_ws.receive_json()
            assert rx_chunk["type"] == "chunk_data"
            assert rx_chunk["index"] == 0
            _ = rx_ws.receive_json()      # progress

            # Receiver ACKs
            rx_ws.send_json({"type": "rx_ack", "index": 0})

            # 3. Simulate resume request (receiver asks for missing chunks)
            rx_ws.send_json({"type": "resume_request"})
            resume_state = rx_ws.receive_json()
            assert resume_state["type"] == "resume_state"
            assert resume_state["acknowledged_chunks"] == [0]
            assert resume_state["missing_chunks"] == [1, 2, 3]

            missing_req = sender_ws.receive_json()
            assert missing_req["type"] == "request_missing"
            assert missing_req["missing_chunks"] == [1, 2, 3]

            # 4. Sender streams remaining chunks 1, 2, 3
            for idx in (1, 2, 3):
                sender_ws.send_json({
                    "type": "chunk",
                    "index": idx,
                    "total": 4,
                    "data": base64.b64encode(chunk_payloads[idx]).decode("utf-8"),
                    "hash": hashlib.sha256(chunk_payloads[idx]).hexdigest(),
                })
                _ = sender_ws.receive_json()
                _ = sender_ws.receive_json()
                rx_c = rx_ws.receive_json()
                assert rx_c["index"] == idx
                _ = rx_ws.receive_json()
                rx_ws.send_json({"type": "rx_ack", "index": idx})

            # Complete transfer
            sender_ws.send_json({"type": "complete"})
            comp_sender = sender_ws.receive_json()
            assert comp_sender["type"] == "completed"

            comp_rx = rx_ws.receive_json()
            assert comp_rx["type"] == "completed"

    # Download verified file
    dl_res = client.get(f"/api/transfers/{transfer_id}/download")
    assert dl_res.status_code == 200
    assert dl_res.content == full_payload
