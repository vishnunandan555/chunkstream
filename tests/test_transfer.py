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
    client_no_raise = TestClient(app, raise_server_exceptions=False)
    error_types = ["key_error", "type_error", "db_lock", "ack_timeout", "storage_full", "broken_pipe", "connection_reset"]
    for err in error_types:
        r = client_no_raise.post(f"/api/test/error?type={err}")
        assert r.status_code == 500

    r_unknown = client_no_raise.post("/api/test/error?type=unknown_error_kind")
    assert r_unknown.status_code == 500


def test_websocket_missing_receiver_and_ack_timeout():
    import base64

    transfer_id = "TEST-WS-ACK-01"
    create_transfer(
        transfer_id=transfer_id,
        filename="ws_test.txt",
        filesize=2000,
        chunk_size=100,
        total_chunks=20,
    )

    # 1. Sender connects without receiver -> sends chunk -> BrokenPipeError flows unhandled!
    with pytest.raises(BrokenPipeError) as exc_info:
        with client.websocket_connect(f"/ws/transfer/{transfer_id}?role=sender") as sender_ws:
            _ = sender_ws.receive_json()
            sender_ws.send_json({
                "type": "chunk",
                "index": 0,
                "total": 20,
                "data": base64.b64encode(b"A" * 100).decode("utf-8")
            })
            _ = sender_ws.receive_json()
    assert "ReceiverMissingError" in str(exc_info.value)

    # 2. Both sender and receiver connect.
    with client.websocket_connect(f"/ws/transfer/{transfer_id}?role=receiver") as rx_ws:
        _ = rx_ws.receive_json()

        with pytest.raises(TimeoutError) as exc_timeout:
            with client.websocket_connect(f"/ws/transfer/{transfer_id}?role=sender") as sender_ws:
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
                    _ = sender_ws.receive_json()
                    _ = sender_ws.receive_json()
                    _ = rx_ws.receive_json()
                    _ = rx_ws.receive_json()
                    # DO NOT send rx_ack from receiver!

                # 11th chunk triggers TimeoutError unhandled!
                sender_ws.send_json({
                    "type": "chunk",
                    "index": 10,
                    "total": 20,
                    "data": base64.b64encode(b"A" * 100).decode("utf-8")
                })
                _ = sender_ws.receive_json()

        assert "ReceiverAckTimeoutError" in str(exc_timeout.value)


def test_path_traversal_sanitization():
    transfer_id = "TEST-SEC-TRAVERSAL"
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

    # Upload chunks
    c0 = client.post(f"/api/transfers/{transfer_id}/chunks/0", content=b"root:x")
    c1 = client.post(f"/api/transfers/{transfer_id}/chunks/1", content=b":0:0::")
    assert c0.status_code == 200
    assert c1.status_code == 200

    # Assemble and download
    asm_res = client.post(f"/api/transfers/{transfer_id}/assemble")
    assert asm_res.status_code == 200

    dl_res = client.get(f"/api/transfers/{transfer_id}/download")
    assert dl_res.status_code == 200
    assert dl_res.content == b"root:x:0:0::"


def test_crash_endpoints_gated_by_default():
    # Destructive crash endpoint must return 403 Forbidden without ALLOW_SIM_CRASH or X-Admin-Key
    res = client.post("/api/test/crash")
    assert res.status_code == 403
    assert "Destructive crash endpoint disabled" in res.json()["detail"]

    res_oom = client.post("/api/test/crash/oom")
    assert res_oom.status_code == 403

    res_deadlock = client.post("/api/test/crash/deadlock")
    assert res_deadlock.status_code == 403


def test_prometheus_metrics_endpoint():
    res = client.get("/metrics")
    assert res.status_code == 200
    assert "text/plain" in res.headers["content-type"]
    text = res.text
    assert "http_requests_total" in text
    assert "chunkstream_ws_active_connections" in text


def test_websocket_resume_flow():
    import base64
    import json

    transfer_id = "TEST-RESUME-E2E"
    chunk_payloads = [b"PART_0_", b"PART_1_", b"PART_2_", b"PART_3_"]
    full_payload = b"".join(chunk_payloads)
    file_hash = hashlib.sha256(full_payload).hexdigest()

    create_transfer(
        transfer_id=transfer_id,
        filename="resume_test.txt",
        filesize=len(full_payload),
        chunk_size=7,
        total_chunks=4,
        checksum=file_hash,
    )

    # 1. Receiver connects first
    with client.websocket_connect(f"/ws/transfer/{transfer_id}?role=receiver") as rx_ws:
        _ = rx_ws.receive_json()  # peer_connected

        # 2. Sender connects
        with client.websocket_connect(f"/ws/transfer/{transfer_id}?role=sender") as sender_ws:
            _ = sender_ws.receive_json()  # peer_connected on sender
            _ = rx_ws.receive_json()      # peer_connected on rx

            # Stream chunks 0 and 1
            for idx in (0, 1):
                sender_ws.send_json({
                    "type": "chunk",
                    "index": idx,
                    "total": 4,
                    "data": base64.b64encode(chunk_payloads[idx]).decode("utf-8"),
                    "hash": hashlib.sha256(chunk_payloads[idx]).hexdigest(),
                })
                # Sender gets ack and progress
                _ = sender_ws.receive_json()
                _ = sender_ws.receive_json()
                # Rx gets chunk_data and progress
                rx_chunk = rx_ws.receive_json()
                assert rx_chunk["type"] == "chunk_data"
                assert rx_chunk["index"] == idx
                _ = rx_ws.receive_json()

                # Rx ACKs chunk
                rx_ws.send_json({"type": "rx_ack", "index": idx})

            # 3. Simulate receiver initiating resume handshake
            rx_ws.send_json({"type": "resume_request", "peer_name": "TestReceiver"})
            resume_state = rx_ws.receive_json()
            assert resume_state["type"] == "resume_state"
            assert resume_state["acknowledged_chunks"] == [0, 1]
            assert resume_state["missing_chunks"] == [2, 3]

            # Sender receives missing chunk request
            missing_req = sender_ws.receive_json()
            assert missing_req["type"] == "request_missing"
            assert missing_req["missing_chunks"] == [2, 3]

            # 4. Sender streams missing chunks 2 and 3
            for idx in (2, 3):
                sender_ws.send_json({
                    "type": "chunk",
                    "index": idx,
                    "total": 4,
                    "data": base64.b64encode(chunk_payloads[idx]).decode("utf-8"),
                    "hash": hashlib.sha256(chunk_payloads[idx]).hexdigest(),
                })
                _ = sender_ws.receive_json()
                _ = sender_ws.receive_json()
                rx_chunk = rx_ws.receive_json()
                assert rx_chunk["index"] == idx
                _ = rx_ws.receive_json()
                rx_ws.send_json({"type": "rx_ack", "index": idx})

            # 5. Sender signals complete
            sender_ws.send_json({"type": "complete"})
            comp_msg_sender = sender_ws.receive_json()
            assert comp_msg_sender["type"] == "completed"

            comp_msg_rx = rx_ws.receive_json()
            assert comp_msg_rx["type"] == "completed"

    # 6. Verify assembled file download
    dl_res = client.get(f"/api/transfers/{transfer_id}/download")
    assert dl_res.status_code == 200
    assert dl_res.content == full_payload


def test_sender_abrupt_disconnect_raises_connection_reset_error():
    transfer_id = "TEST-ABRUPT-DISCONNECT"
    create_transfer(
        transfer_id=transfer_id,
        filename="abrupt.txt",
        filesize=1000,
        chunk_size=100,
        total_chunks=10,
    )

    # Simulates browser refresh mid-transfer (code=1001 Going Away)
    with pytest.raises(ConnectionResetError) as exc_info:
        with client.websocket_connect(f"/ws/transfer/{transfer_id}?role=sender") as ws:
            _ = ws.receive_json()
            ws.close(code=1001)

    assert "ClientAbruptDisconnectError" in str(exc_info.value)
    assert "Sender abruptly disconnected / refreshed" in str(exc_info.value)

    # Check that transfer was marked interrupted
    t = get_transfer(transfer_id)
    assert t["status"] == "interrupted"


def test_crash_endpoints_with_confirm_flag():
    # Calling crash endpoint with confirm=true bypasses 403
    res = client.post("/api/test/crash?confirm=true")
    assert res.status_code == 200
    assert res.json()["status"] == "crashing"

    res_dl = client.get("/api/test/crash/deadlock?confirm=true")
    assert res_dl.status_code == 200
    assert res_dl.json()["status"] == "deadlocked"


def test_calculate_chunk_throughput_triggers_zero_division_error_at_chunk_25():
    from app.transfer import calculate_chunk_throughput
    # Chunks below 25 calculate without error
    assert calculate_chunk_throughput("TRANSFER-ZERO-TEST", 0, 262144) == 0.0
    assert calculate_chunk_throughput("TRANSFER-ZERO-TEST", 24, 262144) == 0.0

    # Chunk 25 triggers ZeroDivisionError due to integer truncation of millisecond delta
    with pytest.raises(ZeroDivisionError):
        calculate_chunk_throughput("TRANSFER-ZERO-TEST", 25, 262144)


