import hashlib
import os
import shutil
import tempfile
import pytest
from fastapi.testclient import TestClient

# Use temporary data directory for tests
os.environ["CHUNKSTREAM_DATA_DIR"] = tempfile.mkdtemp(prefix="test_chunkstream_")
os.environ["LOG_DIR"] = tempfile.mkdtemp(prefix="test_logs_")

from app.main import app
from app.storage import (
    assemble_file,
    create_transfer,
    get_chunk,
    get_transfer,
    init_db,
    save_chunk,
)
from app.transfer import simulator


@pytest.fixture(autouse=True)
def setup_teardown():
    init_db()
    simulator.set_scenario("none")
    yield
    simulator.set_scenario("none")


client = TestClient(app)


def test_health_check_healthy():
    response = client.get("/api/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "healthy"
    assert data["service"] == "chunkstream"


def test_health_check_unhealthy_simulation():
    # Simulate F1
    simulator.set_scenario("unhealthy")
    response = client.get("/api/health")
    assert response.status_code == 503
    simulator.set_scenario("none")


def test_transfer_lifecycle_and_assembly():
    # Create 100 KB payload
    payload = b"A" * 32768 + b"B" * 32768 + b"C" * 34464
    chunk_size = 32768
    total_chunks = 3
    file_hash = hashlib.sha256(payload).hexdigest()

    transfer_id = "TEST-FLOW-01"
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
    c0 = payload[0:32768]
    ok, msg = save_chunk(transfer_id, 0, c0)
    assert ok is True

    # Check state: missing should be [1, 2]
    t_state = get_transfer(transfer_id)
    assert t_state["acknowledged_chunks"] == [0]
    assert t_state["missing_chunks"] == [1, 2]

    # Try assembly prematurely -> must fail
    ok, path, err = assemble_file(transfer_id)
    assert ok is False
    assert "missing chunks" in err.lower()

    # Save remaining chunks
    c1 = payload[32768:65536]
    c2 = payload[65536:]
    assert save_chunk(transfer_id, 1, c1)[0] is True
    assert save_chunk(transfer_id, 2, c2)[0] is True

    # Assembly should now succeed and match hash
    ok, path, err = assemble_file(transfer_id)
    assert ok is True
    assert path is not None
    assert err is None

    with open(path, "rb") as f:
        assembled_data = f.read()
    assert hashlib.sha256(assembled_data).hexdigest() == file_hash


def test_http_chunk_upload_api():
    transfer_id = "TEST-API-02"
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

    # Upload chunk 0 via HTTP endpoint
    c0_res = client.post(
        f"/api/transfers/{transfer_id}/chunks/0",
        content=b"hello",
    )
    assert c0_res.status_code == 200
    assert c0_res.json()["chunk_index"] == 0

    # Upload chunk 1
    c1_res = client.post(
        f"/api/transfers/{transfer_id}/chunks/1",
        content=b"world",
    )
    assert c1_res.status_code == 200

    # Trigger assembly
    assemble_res = client.post(f"/api/transfers/{transfer_id}/assemble")
    assert assemble_res.status_code == 200

    # Download assembled file
    download_res = client.get(f"/api/transfers/{transfer_id}/download")
    assert download_res.status_code == 200
    assert download_res.content == b"helloworld"


def test_log_buffer_retrieval():
    res = client.get("/api/logs?limit=10")
    assert res.status_code == 200
    data = res.json()
    assert "logs" in data
    assert isinstance(data["logs"], list)
    assert len(data["logs"]) > 0


def test_failure_scenarios_api():
    # Low error
    r = client.post("/api/test/fail", json={"scenario": "low_error"})
    assert r.status_code == 200
    assert simulator.error_mode == "low"

    # Reset
    r_reset = client.post("/api/test/reset")
    assert r_reset.status_code == 200
    assert simulator.error_mode == "none"


def test_simulated_runtime_errors():
    error_types = ["key_error", "type_error", "db_lock", "ack_timeout", "storage_full", "broken_pipe"]
    for err in error_types:
        r = client.post(f"/api/test/error?type={err}")
        assert r.status_code == 500
        data = r.json()
        assert data["status"] == "error_logged"
        assert data["error_type"] == err
        assert data["server_running"] is True

    # Unknown error type
    r_unknown = client.post("/api/test/error?type=unknown_error_kind")
    assert r_unknown.status_code == 500
    assert "Unknown simulated error" in r_unknown.json()["message"]


def test_websocket_missing_receiver_and_ack_timeout():
    import base64
    import json

    transfer_id = "TEST-WS-ACK-01"
    create_transfer(
        transfer_id=transfer_id,
        filename="ws_test.txt",
        filesize=2000,
        chunk_size=100,
        total_chunks=20,
    )

    # 1. Sender connects without receiver -> sends chunk -> receives NO_RECEIVER error
    with client.websocket_connect(f"/ws/transfer/{transfer_id}?role=sender") as sender_ws:
        # Initial peer_connected message
        _ = sender_ws.receive_json()

        sender_ws.send_json({
            "type": "chunk",
            "index": 0,
            "total": 20,
            "data": base64.b64encode(b"A" * 100).decode("utf-8")
        })

        err_msg = sender_ws.receive_json()
        assert err_msg["type"] == "error"
        assert err_msg["error_code"] == "NO_RECEIVER"

    # 2. Both sender and receiver connect.
    with client.websocket_connect(f"/ws/transfer/{transfer_id}?role=receiver") as rx_ws:
        # receiver receives peer_connected
        _ = rx_ws.receive_json()

        with client.websocket_connect(f"/ws/transfer/{transfer_id}?role=sender") as sender_ws:
            # sender and receiver receive peer_connected
            _ = sender_ws.receive_json()
            _ = rx_ws.receive_json()

            # Receiver fails to ACK for 10 chunks
            for idx in range(10):
                sender_ws.send_json({
                    "type": "chunk",
                    "index": idx,
                    "total": 20,
                    "data": base64.b64encode(b"A" * 100).decode("utf-8")
                })
                # sender receives chunk_ack
                c_ack = sender_ws.receive_json()
                assert c_ack["type"] == "chunk_ack"
                # progress broadcast
                _ = sender_ws.receive_json()
                # rx receives chunk_data and progress
                _ = rx_ws.receive_json()
                _ = rx_ws.receive_json()
                # DO NOT send rx_ack from receiver!

            # 11th chunk should trigger ACK_TIMEOUT!
            sender_ws.send_json({
                "type": "chunk",
                "index": 10,
                "total": 20,
                "data": base64.b64encode(b"A" * 100).decode("utf-8")
            })
            timeout_err = sender_ws.receive_json()
            assert timeout_err["type"] == "error"
            assert timeout_err["error_code"] == "ACK_TIMEOUT"
            assert "10 chunks unacknowledged" in timeout_err["message"]
