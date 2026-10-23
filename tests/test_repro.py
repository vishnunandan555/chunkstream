import logging
import pytest
from app.transfer import calculate_chunk_throughput
from app.logging_config import BufferLogHandler, _log_buffer


def test_reproduce_throughput_zero_division():
    """
    Reproduces ZeroDivisionError in transfer engine throughput calculation.
    When chunk_index >= 25 is reached during transfer 5280, sub-second delta
    integer truncation causes division by zero.
    """
    throughput = calculate_chunk_throughput(transfer_id="5280", chunk_index=25, chunk_bytes_len=1024)
    assert throughput >= 0.0


def test_reproduce_logging_buffer_handler_format_exception():
    """
    Reproduces AttributeError in BufferLogHandler.emit when logging exceptions with exc_info.
    BufferLogHandler attempts to call self.formatException() which does not exist on Handler,
    triggering handleError() and dropping the log record from _log_buffer.
    """
    handler = BufferLogHandler()
    formatter = logging.Formatter("%(message)s")
    handler.setFormatter(formatter)

    logger = logging.getLogger("chunkstream.test_repro")
    logger.handlers.clear()
    logger.addHandler(handler)
    logger.propagate = False

    _log_buffer.clear()

    try:
        calculate_chunk_throughput("5280", 25, 1024)
    except ZeroDivisionError as exc:
        logger.critical(
            "FATAL APPLICATION CRASH: Unhandled ZeroDivisionError in transfer engine throughput calculation for transfer %s: %s",
            "5280",
            exc,
            exc_info=True,
        )

    # In baseline code, emit failed with AttributeError and handled error without appending to buffer
    assert len(_log_buffer) == 1, "Log record should have been successfully appended to _log_buffer"
    assert "exc_text" in _log_buffer[0]
