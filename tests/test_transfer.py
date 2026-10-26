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


def test_network_info_endpoint():
    """Validates the /api/network-info endpoint returns masked IP, locality, and server region."""
    res = client.get("/api/network-info")
    assert res.status_code == 200
    data = res.json()
    assert "client_ip" in data
    assert "is_local" in data
    assert "server_region" in data
    assert "timestamp" in data
    assert isinstance(data["is_local"], bool)


def test_peer_info_and_rtt_ping_websocket():
    """Validates peer_info broadcasting and RTT ping/pong echoing with client timestamps."""
    transfer_id = "PROD-TELEMETRY-WS"
    create_transfer(
        transfer_id=transfer_id,
        filename="telemetry_test.dat",
        filesize=100,
        chunk_size=50,
        total_chunks=2,
    )

    with client.websocket_connect(f"/ws/transfer/{transfer_id}?role=receiver") as rx_ws:
        _ = rx_ws.receive_json()

        with client.websocket_connect(f"/ws/transfer/{transfer_id}?role=sender") as sender_ws:
            _ = sender_ws.receive_json()
            _ = rx_ws.receive_json()

            # 1. Test RTT ping/pong echo
            sender_ws.send_json({"type": "ping", "client_ts": 123456.78})
            pong = sender_ws.receive_json()
            assert pong["type"] == "pong"
            assert pong["transfer_id"] == transfer_id
            assert pong["client_ts"] == 123456.78
            assert "server_ts" in pong

            # 2. Test peer_info exchange
            sender_ws.send_json({
                "type": "peer_info",
                "device": "MacBook Pro",
                "os": "macOS",
                "browser": "Chrome",
                "network": "Wi-Fi (5G)",
                "device_id": "ABC123",
                "location": "127.0.0.1 (Local)",
            })

            # Both sender and receiver receive the broadcast peer_info
            sender_peer_msg = sender_ws.receive_json()
            rx_peer_msg = rx_ws.receive_json()

            assert sender_peer_msg["type"] == "peer_info"
            assert sender_peer_msg["role"] == "sender"
            assert sender_peer_msg["device_id"] == "ABC123"
            assert sender_peer_msg["network"] == "Wi-Fi (5G)"

            assert rx_peer_msg["type"] == "peer_info"
            assert rx_peer_msg["role"] == "sender"
            assert rx_peer_msg["device_id"] == "ABC123"


def test_webrtc_signaling_relay():
    """Validates WebRTC signal_offer, signal_answer, and signal_candidate forwarding."""
    transfer_id = "PROD-WEBRTC-SIGNAL"
    create_transfer(
        transfer_id=transfer_id,
        filename="p2p_signal.dat",
        filesize=1000,
        chunk_size=500,
        total_chunks=2,
    )

    with client.websocket_connect(f"/ws/transfer/{transfer_id}?role=receiver") as rx_ws:
        _ = rx_ws.receive_json()

        with client.websocket_connect(f"/ws/transfer/{transfer_id}?role=sender") as sender_ws:
            _ = sender_ws.receive_json()
            _ = rx_ws.receive_json()

            # 1. Sender sends signal_offer
            sender_ws.send_json({
                "type": "signal_offer",
                "sdp": "v=0\r\no=sender 123 456 IN IP4 127.0.0.1\r\ns=ChunkStream\r\n",
            })
            offer_msg = rx_ws.receive_json()
            assert offer_msg["type"] == "signal_offer"
            assert "sender" in offer_msg["sdp"]

            # 2. Receiver sends signal_answer
            rx_ws.send_json({
                "type": "signal_answer",
                "sdp": "v=0\r\no=receiver 789 012 IN IP4 127.0.0.1\r\ns=ChunkStream\r\n",
            })
            answer_msg = sender_ws.receive_json()
            assert answer_msg["type"] == "signal_answer"
            assert "receiver" in answer_msg["sdp"]

            # 3. Candidate relay from sender to receiver
            sender_ws.send_json({
                "type": "signal_candidate",
                "candidate": {"candidate": "candidate:1 1 UDP 2122260223 192.168.1.100 50000 typ host"},
            })
            cand_msg = rx_ws.receive_json()
            assert cand_msg["type"] == "signal_candidate"
            assert "192.168.1.100" in cand_msg["candidate"]["candidate"]


