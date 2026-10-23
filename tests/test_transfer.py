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
