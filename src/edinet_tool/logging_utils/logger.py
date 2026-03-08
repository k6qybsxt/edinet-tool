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
) -> logging.Logger:
    logger = logging.getLogger("edinet")
    logger.setLevel(logging.DEBUG)
    logger.propagate = False

    if logger.handlers:
        for h in list(logger.handlers):
            logger.removeHandler(h)

    effective_level_name = (log_level or LOG_LEVEL or "INFO").upper()
    effective_console_level = getattr(logging, effective_level_name, logging.INFO)

    datefmt = "%Y-%m-%d %H:%M:%S"
    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s",
        datefmt=datefmt
    )

    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    ch.setLevel(effective_console_level)
    logger.addHandler(ch)

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

    logger.debug(
        "logger initialized: console_level=%s, log_path=%s",
        effective_level_name,
        log_path,
    )
    return logger

def log(*args, **kwargs):
    global logger
    if logger is not None:
        logger.debug(" ".join(map(str, args)))