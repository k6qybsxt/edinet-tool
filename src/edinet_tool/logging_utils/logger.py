import logging
import os
from logging.handlers import RotatingFileHandler

from edinet_tool.config.settings import (
    LOG_LEVEL,
    LOG_DIR,
    LOG_MAX_BYTES,
    LOG_BACKUP_COUNT,
)

logger = None


def setup_logger(
    log_level: str | None = None,
    log_dir: str | None = None,
    emit_initialized_log: bool = True,
    enable_file_handler: bool = True,
    enable_stream_handler: bool = True,
) -> logging.Logger:
    logger = logging.getLogger("edinet")
    logger.setLevel(logging.DEBUG)
    logger.propagate = False

    state_key = (
        f"{emit_initialized_log}:"
        f"{enable_file_handler}:"
        f"{enable_stream_handler}:"
        f"{log_dir or ''}:"
        f"{log_level or ''}"
    )
    if getattr(logger, "_edinet_initialized_key", None) == state_key:
        return logger

    if logger.handlers:
        for handler in list(logger.handlers):
            logger.removeHandler(handler)
            try:
                handler.close()
            except Exception:
                pass

    effective_level_name = (log_level or LOG_LEVEL or "INFO").upper()
    effective_console_level = getattr(logging, effective_level_name, logging.INFO)

    datefmt = "%Y-%m-%d %H:%M:%S"
    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s",
        datefmt=datefmt
    )

    if enable_stream_handler:
        ch = logging.StreamHandler()
        ch.setFormatter(fmt)
        ch.setLevel(effective_console_level)
        logger.addHandler(ch)

    log_path = None
    if enable_file_handler:
        effective_log_dir = log_dir or LOG_DIR or "logs"
        os.makedirs(effective_log_dir, exist_ok=True)

        log_path = os.path.join(effective_log_dir, "run.log")
        fh = RotatingFileHandler(
            log_path,
            maxBytes=LOG_MAX_BYTES,
            backupCount=LOG_BACKUP_COUNT,
            encoding="utf-8"
        )
        fh.setFormatter(fmt)
        fh.setLevel(logging.DEBUG)
        logger.addHandler(fh)

    if not logger.handlers:
        logger.addHandler(logging.NullHandler())

    logger._edinet_initialized = True
    logger._edinet_initialized_key = state_key
    logger._edinet_log_path = log_path

    if emit_initialized_log:
        logger.debug(
            "logger initialized: console_level=%s, log_path=%s, file_handler=%s, stream_handler=%s",
            effective_level_name,
            log_path,
            enable_file_handler,
            enable_stream_handler,
        )

    return logger


def log(*args, **kwargs):
    global logger
    if logger is not None:
        logger.debug(" ".join(map(str, args)))