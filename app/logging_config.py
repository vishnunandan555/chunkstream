import logging
import os
import sys
from collections import deque
from datetime import datetime
from typing import Any, Deque, Dict, List, Optional

# In-memory buffer to store recent logs for UI and ProdPilot retrieval
LOG_BUFFER_MAX_SIZE = 500
_log_buffer: Deque[Dict[str, Any]] = deque(maxlen=LOG_BUFFER_MAX_SIZE)


class BufferLogHandler(logging.Handler):
    """Logging handler that appends log entries to a bounded in-memory deque."""

    def emit(self, record: logging.LogRecord) -> None:
        try:
            entry = {
                "timestamp": datetime.fromtimestamp(record.created).isoformat(),
                "level": record.levelname,
                "logger": record.name,
                "message": self.format(record),
                "raw_message": record.getMessage(),
                "pathname": record.pathname,
                "lineno": record.lineno,
            }
            if record.exc_info:
                entry["exc_text"] = self.formatException(record.exc_info)
            _log_buffer.append(entry)
        except Exception:
            self.handleError(record)


def setup_logging(log_level: str = "INFO", log_dir: str = "logs") -> logging.Logger:
    """Configures structured console, file, and in-memory logging."""
    os.makedirs(log_dir, exist_ok=True)
    log_file_path = os.path.join(log_dir, "chunkstream.log")

    level = getattr(logging, log_level.upper(), logging.INFO)

    root_logger = logging.getLogger()
    root_logger.setLevel(level)

    # Format
    formatter = logging.Formatter(
        "[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Console Handler (stdout for Render / Docker / local)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)

    # File Handler
    file_handler = logging.FileHandler(log_file_path, encoding="utf-8")
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
    app_logger.info("Logging configured successfully (file: %s)", log_file_path)
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
