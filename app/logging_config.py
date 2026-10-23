import json
import logging
import os
import sys
from collections import deque
from contextvars import ContextVar
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from typing import Any, Deque, Dict, List, Optional

# ContextVar to propagate request IDs across coroutines & logging
request_id_ctx: ContextVar[str] = ContextVar("request_id", default="")

# In-memory buffer to store recent logs for UI and ProdPilot retrieval
LOG_BUFFER_MAX_SIZE = 500
_log_buffer: Deque[Dict[str, Any]] = deque(maxlen=LOG_BUFFER_MAX_SIZE)


class JSONFormatter(logging.Formatter):
    """Structured JSON log formatter for machine consumption (Loki, Datadog, ProdPilot)."""

    def format(self, record: logging.LogRecord) -> str:
        req_id = request_id_ctx.get("")
        payload: Dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "pathname": record.pathname,
            "lineno": record.lineno,
        }
        if req_id:
            payload["request_id"] = req_id
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload)


class BufferLogHandler(logging.Handler):
    """Logging handler that appends log entries to a bounded in-memory deque with UTC timestamps."""

    def emit(self, record: logging.LogRecord) -> None:
        try:
            req_id = request_id_ctx.get("")
            entry = {
                "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
                "level": record.levelname,
                "logger": record.name,
                "message": self.format(record),
                "raw_message": record.getMessage(),
                "pathname": record.pathname,
                "lineno": record.lineno,
                "request_id": req_id or None,
            }
            if record.exc_info:
                entry["exc_text"] = self.formatException(record.exc_info)
            _log_buffer.append(entry)
        except Exception:
            self.handleError(record)


def setup_logging(log_level: str = "INFO", log_dir: str = "logs") -> logging.Logger:
    """Configures structured console, rotating file, and in-memory logging."""
    os.makedirs(log_dir, exist_ok=True)
    log_file_path = os.path.join(log_dir, "chunkstream.log")

    level = getattr(logging, log_level.upper(), logging.INFO)

    root_logger = logging.getLogger()
    root_logger.setLevel(level)

    use_json = os.environ.get("LOG_FORMAT", "").lower() == "json"
    if use_json:
        formatter: logging.Formatter = JSONFormatter()
    else:
        formatter = logging.Formatter(
            "[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

    # Console Handler (stdout)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)

    # Rotating File Handler (10MB max, keep 5 backups)
    file_handler = RotatingFileHandler(
        log_file_path,
        maxBytes=10 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )
    file_handler.setLevel(level)
    file_handler.setFormatter(formatter)

    # In-memory Buffer Handler
    buffer_handler = BufferLogHandler()
    buffer_handler.setLevel(level)
    buffer_handler.setFormatter(formatter)

    # Clear existing handlers to prevent duplicates on reload
    root_logger.handlers.clear()
    root_logger.addHandler(console_handler)
    root_logger.addHandler(file_handler)
    root_logger.addHandler(buffer_handler)

    app_logger = logging.getLogger("chunkstream")
    app_logger.setLevel(level)
    app_logger.handlers.clear()
    app_logger.addHandler(console_handler)
    app_logger.addHandler(file_handler)
    app_logger.addHandler(buffer_handler)
    app_logger.propagate = False

    app_logger.info("Logging configured successfully (file: %s, rotating=10MB, json=%s)", log_file_path, use_json)
    return app_logger


def get_recent_logs(limit: int = 100, min_level: Optional[str] = None) -> List[Dict[str, Any]]:
    """Returns the most recent log entries matching optional min_level."""
    logs = list(_log_buffer)
    if min_level:
        level_order = {"DEBUG": 10, "INFO": 20, "WARNING": 30, "ERROR": 40, "CRITICAL": 50}
        min_val = level_order.get(min_level.upper(), 0)
        logs = [entry for entry in logs if level_order.get(entry["level"].upper(), 0) >= min_val]
    return logs[-limit:]


def clear_logs() -> None:
    """Clears the in-memory log buffer."""
    _log_buffer.clear()
