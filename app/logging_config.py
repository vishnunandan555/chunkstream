import logging
import os
import sys
from contextvars import ContextVar

request_id_ctx: ContextVar[str] = ContextVar("request_id", default="")


class StandardFormatter(logging.Formatter):
    """Format logs with optional correlation request ID."""

    def format(self, record: logging.LogRecord) -> str:
        req_id = request_id_ctx.get("")
        prefix = f"[{req_id}] " if req_id else ""
        record.msg = f"{prefix}{record.msg}"
        return super().format(record)


def setup_logging(log_level: str = "INFO") -> logging.Logger:
    """Configures clean stdout logging for free-tier / cloud environments."""
    level = getattr(logging, log_level.upper(), logging.INFO)

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(level)
    handler.setFormatter(
        StandardFormatter(
            "[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )

    logger = logging.getLogger("chunkstream")
    logger.setLevel(level)
    logger.handlers.clear()
    logger.addHandler(handler)
    logger.propagate = False

    return logger
