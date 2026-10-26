# ChunkStream

**Fast, minimal, resumable peer-to-peer file transfers.**

ChunkStream is an ultra-lightweight, resilient file transfer service built with FastAPI, WebSockets, and SQLite. It slices files into chunks, streams them across peers in real time, and automatically reassembles and verifies the final file with SHA-256 checksums.

Optimized specifically for free-tier hosting (Render, Railway, Fly.io, etc.) with minimal memory (<256MB RAM), automatic disk space reclamation, and zero external database dependencies.

---

## Highlights

- **Direct Chunked Streaming**: Files are sliced into 256 KB chunks and streamed over low-latency WebSockets.
- **Resumable Transfers**: If a sender or receiver disconnects, simply reconnect with the 4-digit code. ChunkStream synchronizes missing chunks and resumes right where it left off.
- **SHA-256 Integrity Verification**: Individual chunks and final assembled files are verified against SHA-256 hashes.
- **Hyper-Optimized for Free Hosting**:
  - Automatically deletes intermediate `.part` chunk files upon reassembly (halving disk usage).
  - Background worker purges expired transfers after 1 hour.
  - Zero heavy database or caching dependencies; uses SQLite in WAL mode.
- **Flat, Minimal UI**: Clean, responsive, high-contrast flat design with real-time transfer stats (MB/s, ETA, chunks, bytes).

---

## Architecture

| Component | Technology | Description |
|---|---|---|
| **Backend** | Python 3 + FastAPI | Fast asynchronous REST API |
| **Real-time Relay** | WebSockets | Real-time chunk streaming & peer progress sync |
| **Storage & State** | SQLite (WAL mode) | Persistent transfer tracking & missing chunk indexing |
| **Frontend** | Vanilla HTML5 / Flat CSS / JS | Zero-build minimal web UI with system typography |
| **Observability** | Prometheus (`/metrics`) | Request counts, active WebSocket gauges, and chunks |

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
2. Use the included `render.yaml` blueprint or choose **Web Service**:
   - **Environment**: Python
   - **Build Command**: `pip install -r requirements.txt`
   - **Start Command**: `uvicorn app.main:app --host 0.0.0.0 --port $PORT --workers 1 --timeout-keep-alive 65`
   - **Health Check Path**: `/api/health`

### Railway / Fly.io / Heroku
ChunkStream includes a standard `Procfile` ready for one-click deployment on any container/PaaS provider.

---

## API Reference

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/api/health` | Service liveness & storage health check |
| `GET` | `/metrics` | Prometheus metrics exposition |
| `GET` | `/api/version` | Version and uptime |
| `POST` | `/api/transfers` | Initialize transfer session and metadata |
| `GET` | `/api/transfers` | List recent transfers |
| `GET` | `/api/transfers/{id}` | Inspect transfer progress and missing chunk indices |
| `POST` | `/api/transfers/{id}/chunks/{index}` | Upload chunk via HTTP fallback |
| `POST` | `/api/transfers/{id}/assemble` | Trigger file reassembly |
| `GET` | `/api/transfers/{id}/download` | Download assembled file |
| `DELETE` | `/api/transfers/{id}` | Delete transfer and immediately reclaim disk space |
| `WS` | `/ws/transfer/{id}?role=sender\|receiver` | Bidirectional real-time streaming WebSocket |

---

## License

MIT
