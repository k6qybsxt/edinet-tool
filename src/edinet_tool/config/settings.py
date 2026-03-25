import json
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parents[3]


def load_config(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        return json.load(f)


# =========================================================
# logging mode
#   NORMAL = 通常運用
#   DEBUG  = 不具合調査
# =========================================================
LOG_MODE = "NORMAL"   # "NORMAL" or "DEBUG"


if LOG_MODE == "DEBUG":
    LOG_LEVEL = "DEBUG"
    WORKER_LOG_LEVEL = "DEBUG"
    WORKER_ENABLE_STREAM_HANDLER = True
    WORKER_ENABLE_FILE_HANDLER = True
    WORKER_EMIT_INITIALIZED_LOG = True
else:
    LOG_LEVEL = "INFO"
    WORKER_LOG_LEVEL = "INFO"
    WORKER_ENABLE_STREAM_HANDLER = False
    WORKER_ENABLE_FILE_HANDLER = False
    WORKER_EMIT_INITIALIZED_LOG = False


# === logging settings ===
LOG_DIR = "logs"
LOG_MAX_BYTES = 2_000_000
LOG_BACKUP_COUNT = 5