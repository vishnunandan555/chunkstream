# ChunkStream

### Resumable Chunked File-Transfer Application for Testing ProdPilot

ChunkStream is a live file-transfer prototype built to reliably demonstrate and test the **ProdPilot** autonomous incident lifecycle: error detection, stack-trace fingerprinting, crash loops, degraded health checks, and recovery verification.

---

## Architecture & Technology Stack

| Component | Technology | Description |
|---|---|---|
| **Backend** | Python 3 + FastAPI | Fast asynchronous REST & WebSocket server |
| **Live Relay** | WebSockets | Real-time chunk streaming & peer progress synchronization |
| **Metadata** | SQLite | Persistent tracking of transfers, acknowledged chunks, & status |
| **Chunk Storage** | Filesystem | Segmented `.part` chunk files and reassembly pipeline |
| **Frontend** | Vanilla HTML5 / Modern CSS / JS | Sender, Receiver, Failure Lab, and Live Log Streamer |
| **Monitoring** | ProdPilot Integration | Formatted logs (`/api/logs`), health checks (`/api/health`) |
| **Hosting** | Render Web Service | `render.yaml` & `Procfile` ready |

---

## Key Features

1. **Chunked Streaming & Assembly**
   - Sender splits any file into configurable chunks (64 KB to 1 MB).
   - Real-time chunk relay with SHA-256 verification per chunk and on final assembled file.
   - Interactive Chunk Matrix visualizer: displays in-flight, acknowledged, and retried chunks.

2. **Resumable Transfers**
   - Receiver or sender can disconnect and reconnect anytime using the transfer session code (e.g. `CS-7892`).
   - Bidirectional resume handshake: queries SQLite for missing chunk indices and streams only unacknowledged chunks.

3. **ProdPilot Failure Lab (Test Control Panel)**
   - **F1 (Deployment / Health Failure)**: Degrades `/health` endpoint to return `503 Service Unavailable`.
   - **F2 (Fatal Server Crash)**: Emits critical crash trace and exits process (`os._exit(1)`) to test auto-restart.
   - **F3 (Low Error Rate)**: Injects intermittent 20% chunk packet drop; client auto-retries and completes.
   - **F4 (High Error Rate)**: Generates 75% chunk failure storm with error logs for ProdPilot alert triage.
   - **F5 (Receiver Disconnect)**: Forces connection termination on receiver socket to trigger resume flow.
   - **F6 (Resume Conflict)**: Simulates corrupted resume handshake state.
   - **F7 & F8 (Restart & Rollback)**: State persists across server reboots via SQLite database.

4. **Live Log Streamer for ProdPilot Ingestion**
   - High-performance bounded in-memory log buffer accessible via `GET /api/logs`.
   - Structured console (stdout) and file (`logs/chunkstream.log`) logging with level filtering.

---

## Directory Structure

```text
chunkstream/
├── app/
│   ├── __init__.py
│   ├── main.py              # FastAPI app, REST & WebSocket endpoints, health checks
│   ├── transfer.py          # WebSocket connection manager, chunk relay & failure simulator
│   ├── storage.py           # SQLite DB schema, chunk file I/O & file reassembly
│   ├── logging_config.py    # Structured logging with bounded in-memory buffer
│   └── static/
│       ├── index.html       # Web UI with Sender, Receiver, and Failure Lab tabs
│       ├── app.js           # Client-side chunk slicing, WS relay, and log polling
│       └── style.css        # Modern dark-mode UI with glassmorphism & glowing status
├── tests/
│   ├── __init__.py
│   └── test_transfer.py     # Pytest test suite (health, storage, chunking, resume, API)
├── Procfile                 # Render & PaaS process launcher
├── render.yaml              # Render blueprint deployment definition
├── requirements.txt         # Production dependencies
├── pytest.ini               # Test configuration
└── README.md
```

---

## Quickstart (Local Development)

### 1. Install Dependencies
```bash
# Using uv (recommended)
uv venv .venv
source .venv/bin/activate
uv pip install -r requirements.txt

# Or using standard python venv
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Run Test Suite
```bash
.venv/bin/pytest -v
```

### 3. Launch ChunkStream Server
```bash
.venv/bin/uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```
Open **`http://localhost:8000`** in your browser.

---

## Testing File Transfer & ProdPilot Scenarios

### Basic Transfer Demo (Sender & Receiver)
1. Open `http://localhost:8000` in Browser Window A (Sender tab).
2. Choose a file (e.g. an image or PDF) and note the generated Session Code (e.g. `CS-7892`).
3. Open `http://localhost:8000` in Browser Window B (Receiver tab).
4. Enter `CS-7892` and click **Connect**.
5. In Window A, click **Start Transfer**.
6. Observe the Chunk Grid Matrix illuminating chunks as they stream and assemble in real time!
7. When complete, click **Download Assembled File** on Window B.

### ProdPilot Incident Simulation
Switch to the **Failure Lab & Logs** tab:
- Click **Inject Low Errors (F3)**: Watch intermittent amber retry chunks appear, while the transfer survives.
- Click **Inject High Errors (F4)**: Produces error bursts in the live log streamer, ideal for ProdPilot log intake.
- Click **Degrade /health (F1)**: `/api/health` immediately reports `503`, triggering ProdPilot uptime alerts.
- Click **Crash Backend (F2)**: Backend exits with status 1.
- Click **Reset All to Normal**: Clears all failure flags.

---

## API Reference for ProdPilot

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/health` / `/api/health` | Service health status (returns 200 or 503) |
| `GET` | `/api/version` | Current service version & uptime |
| `GET` | `/api/logs?limit=100&min_level=ERROR` | Retrieves recent structured application logs |
| `POST` | `/api/test/fail` | Injects failure scenario (`low_error`, `high_error`, `unhealthy`, etc.) |
| `POST` | `/api/test/reset` | Resets simulator to healthy state |
| `POST` | `/api/test/crash` | Kills the backend process (F2 simulation) |
| `GET` | `/api/transfers` | List active transfers and progress |
| `GET` | `/api/transfers/{id}` | Inspect transfer details, acknowledged & missing chunks |
| `POST` | `/api/transfers/{id}/assemble` | Triggers chunk reassembly |
| `GET` | `/api/transfers/{id}/download` | Downloads assembled file |
| `WS` | `/ws/transfer/{id}?role=sender\|receiver` | Live bidirectional transfer socket |
