import threading
from collections import defaultdict
from typing import Dict, List, Tuple

class MetricsCollector:
    """Lightweight in-memory Prometheus metrics registry for ChunkStream."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.http_requests_total: Dict[Tuple[str, str, int], int] = defaultdict(int)
        self.chunk_transfers_total: Dict[str, int] = defaultdict(int)
        self.ws_connections_active: int = 0
        self.assembly_duration_seconds: List[float] = []

    def record_request(self, method: str, endpoint: str, status_code: int) -> None:
        with self._lock:
            self.http_requests_total[(method, endpoint, status_code)] += 1

    def record_chunk(self, status: str = "success") -> None:
        with self._lock:
            self.chunk_transfers_total[status] += 1

    def set_active_websockets(self, count: int) -> None:
        with self._lock:
            self.ws_connections_active = count

    def generate_prometheus_text(self) -> str:
        lines: List[str] = [
            "# HELP http_requests_total Total number of HTTP requests processed",
            "# TYPE http_requests_total counter",
        ]
        with self._lock:
            for (method, endpoint, status), count in self.http_requests_total.items():
                lines.append(f'http_requests_total{{method="{method}",endpoint="{endpoint}",status="{status}"}} {count}')

            lines.append("# HELP chunkstream_ws_active_connections Current active WebSocket connections")
            lines.append("# TYPE chunkstream_ws_active_connections gauge")
            lines.append(f"chunkstream_ws_active_connections {self.ws_connections_active}")

            lines.append("# HELP chunkstream_chunks_total Total number of chunks processed")
            lines.append("# TYPE chunkstream_chunks_total counter")
            for status, count in self.chunk_transfers_total.items():
                lines.append(f'chunkstream_chunks_total{{status="{status}"}} {count}')

        return "\n".join(lines) + "\n"

metrics = MetricsCollector()
