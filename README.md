# ChunkStream

**Ultra-fast, zero-server-storage peer file streaming relay.**

ChunkStream is an ultra-lightweight, resilient file streaming relay built with FastAPI, WebSockets, WebRTC, and SQLite. Files are sliced on the sender's device, relayed directly peer-to-peer or through an in-memory WebSocket pipeline in real time, and assembled straight into receiver memory without writing a single byte of file data to the server disk.

Optimized specifically for free-tier hosting (Render, Railway, Fly.io, etc.) with minimal memory footprint (<64MB RAM), zero disk usage, automatic memory purging, and seamless dark and light modes.

---

## Highlights

- **Zero Server Storage (0 Bytes on Disk)**: Chunks stream directly from sender to receiver in real time. The server never writes chunk files or assembled downloads to disk, allowing transfers of virtually any size (100MB, 500MB, 5GB+) on free hosting tiers with tiny disk limits.
- **Dynamic LAN & Internet Relay**:
  - **Direct LAN Mode**: Uses WebSockets for zero-configuration WebRTC signaling and transfers files directly peer-to-peer over local Wi-Fi / LAN when available.
  - **Cloud In-Memory Relay**: Seamlessly relays chunks in real time over secure WebSockets when peers are on different networks.
- **Engine & Route Diagnostics Drawer**: Expandable settings drawer in the top navigation allowing users to force transmission routes (Auto, Force Direct LAN P2P, Force Cloud Relay) and choose custom slice sizes (Auto 512KB Cloud / 256KB P2P, 256KB, 512KB, 1024KB).
- **Per-Chunk SHA-256 Data Integrity**: Hardware-accelerated SubtleCrypto SHA-256 checksums calculated for each chunk, ensuring 100% bit-exact assembly and proactive rejection of corrupt packets.
- **Missing Chunk Recovery (NACK Pull)**: Client-side receiver watchdog detects dropped chunk indices and requests targeted re-transmission (`request_missing`) without restarting the transfer.
- **Resilient Flow-Controlled Pipeline**: A dynamic in-flight window with high-precision timestamp tracking and a watchdog timer prevents packet loss or pipeline stalls.
- **Automatic Server & Client Cleanup**:
  - **Immediate Session Purge**: Once all peers disconnect from a completed or cancelled transfer, metadata and in-memory telemetry are immediately removed from SQLite and RAM.
  - **SQLite WAL Maintenance**: Periodic truncate checkpoints (`PRAGMA wal_checkpoint(TRUNCATE)`) keep the database footprint virtually at zero bytes.
  - **Client RAM Purging**: Receiver chunks are cleared from browser memory immediately upon Blob creation, and Object URLs are revoked within 8 seconds to prevent browser memory bloat.
- **Live Monospace Activity Feed**: Real-time event log in both sender and receiver interfaces tracking peer discovery, slicing progress, relay speed, and completion milestones.
- **Refined Minimalist UI**: Calm, monochromatic interface with smooth light/dark mode toggling, zero external CSS frameworks, zero emojis, and pure SVG iconography.

---

## Architecture

| Component | Technology | Description |
|---|---|---|
| **Backend** | Python 3.10+ / FastAPI / Uvicorn | Asynchronous, non-blocking real-time streaming relay |
| **Direct LAN Streaming** | WebRTC DataChannels | Peer-to-peer local streaming with zero server bandwidth |
| **Cloud Relay** | Pure WebSockets | Zero-server-disk streaming relay with bidirectional flow control |
| **Session Metadata** | SQLite (WAL mode) | Ephemeral session state, auto-purged on disconnect / completion |
| **Frontend** | Vanilla HTML5 / Modern CSS / ES6 | Fast, buildless UI with dynamic dark/light theme and live telemetry |
| **Observability** | Prometheus (`/metrics`) | Real-time request metrics, latency tracking, and throughput counters |

---

## Quickstart

### 1. Install Dependencies
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Run Test Suite
```bash
pytest -v
```

### 3. Start Development Server
```bash
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```
Open **`http://localhost:8000`** in your browser.

---

## Deployment (Free Tier)

### Render
1. Connect your repository to [Render](https://render.com).
2. Choose **Web Service** with Python environment:
   - **Build Command**: `pip install -r requirements.txt`
   - **Start Command**: `uvicorn app.main:app --host 0.0.0.0 --port $PORT --workers 1 --timeout-keep-alive 65`
   - **Health Check Path**: `/api/health`

### Railway / Fly.io / Heroku
ChunkStream includes a standard `Procfile` ready for one-click deployment on any PaaS or container provider.

---

## API Reference

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/api/health` | Service liveness, uptime, and database connectivity check |
| `GET` | `/api/version` | Active version and zero-server-storage capability status |
| `GET` | `/api/network-info` | Client network locality, masked IP, and server region |
| `GET` | `/metrics` | Prometheus metrics exposition |
| `POST` | `/api/transfers` | Initialize transfer session metadata with 4-digit code |
| `GET` | `/api/transfers` | List active transfer sessions |
| `GET` | `/api/transfers/{id}` | Retrieve transfer session status and metadata |
| `GET` | `/api/transfers/{id}/metrics` | Live bandwidth, duration, and transfer throughput stats |
| `DELETE` | `/api/transfers/{id}` | Idempotently cancel transfer, free telemetry, and purge metadata |
| `POST` | `/api/cleanup` | Maintenance endpoint to truncate SQLite WAL and prune stale sessions |
| `WS` | `/ws/transfer/{id}?role=sender|receiver` | Real-time bidirectional WebSocket relay & WebRTC signaling |

---

## License

MIT
