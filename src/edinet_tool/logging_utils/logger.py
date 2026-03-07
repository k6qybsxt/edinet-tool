import logging
import os
from logging.handlers import RotatingFileHandler


logger = None
DEBUG = False


def setup_logger(debug: bool = False, log_dir: str | None = None) -> logging.Logger:
    logger = logging.getLogger("edinet")
    logger.setLevel(logging.DEBUG)
    logger.propagate = False

    if logger.handlers:
        for h in list(logger.handlers):
            logger.removeHandler(h)

    datefmt = "%Y-%m-%d %H:%M:%S"
    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s",
        datefmt=datefmt
    )

    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    ch.setLevel(logging.DEBUG if debug else logging.INFO)
    logger.addHandler(ch)

    if log_dir is None:
        log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)

    log_path = os.path.join(log_dir, "run.log")
    fh = RotatingFileHandler(
        log_path,
        maxBytes=2_000_000,
        backupCount=5,
        encoding="utf-8"
    )
    fh.setFormatter(fmt)
    fh.setLevel(logging.DEBUG)
    logger.addHandler(fh)

    logger.debug(f"logger initialized: debug={debug}, log_path={log_path}")
    return logger


def log(*args, **kwargs):
    global logger
    if DEBUG and logger is not None:
        logger.debug(" ".join(map(str, args)))