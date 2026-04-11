from __future__ import annotations

import os
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[3]


def _path_from_env(env_name: str, default_path: str) -> Path:
    value = os.getenv(env_name, "").strip()
    if value:
        return Path(value)
    return Path(default_path)

# =========================================================
# edinet_monitor 保存先
#   - DB は C ドライブ側に置く
#   - ZIP / XBRL / logs は D ドライブ側に置く
# =========================================================
MONITOR_DB_ROOT = _path_from_env(
    "EDINET_MONITOR_DB_ROOT",
    r"E:\EDINET_Data\edinet_monitor\db",
)
DB_PATH = MONITOR_DB_ROOT / "edinet_monitor.db"

MONITOR_STORAGE_ROOT = Path(r"D:\EDINET_Data") / "edinet_monitor"
TSE_LISTING_MASTER_CSV_PATH = Path(r"D:\EDINET_Data\master\tse_issuer_master_latest.csv")

RAW_ROOT = MONITOR_STORAGE_ROOT / "raw"
ZIP_ROOT = RAW_ROOT / "zip"
XBRL_ROOT = RAW_ROOT / "xbrl"
MANIFEST_ROOT = RAW_ROOT / "manifests"

LOG_ROOT = MONITOR_STORAGE_ROOT / "logs"
ZIP_BACKFILL_RUN_LOG_PATH = LOG_ROOT / "zip_backfill_runs.jsonl"
ZIP_BACKFILL_CHUNK_LOG_PATH = LOG_ROOT / "zip_backfill_chunk_runs.jsonl"

# =========================================================
# logging mode
#   NORMAL = 通常運用
#   DEBUG  = 不具合調査
# =========================================================
LOG_MODE = "NORMAL"   # "NORMAL" or "DEBUG"

if LOG_MODE == "DEBUG":
    LOG_LEVEL = "DEBUG"
else:
    LOG_LEVEL = "INFO"

TARGET_FORM_CODES = [
    "030000",  # 有価証券報告書
]

EDINET_API_BASE_URL = "https://api.edinet-fsa.go.jp/api/v2"

DOCUMENT_TYPE_ZIP = 1
RAW_SAVE_YEARS = 10
XBRL_RETENTION_ENABLED = False
XBRL_RETENTION_MONTHS = 3
DOWNLOAD_PROFILE_DEFAULT = "normal"
DOWNLOAD_CONNECT_TIMEOUT_SEC = 10
DOWNLOAD_READ_TIMEOUT_SEC = 30
DOWNLOAD_MAX_RETRIES = 1
DOWNLOAD_RETRY_WAIT_SEC = 1.0
DOWNLOAD_PROGRESS_EVERY = 10
DOWNLOAD_COOLDOWN_FAILURE_STREAK = 5
DOWNLOAD_COOLDOWN_SEC = 180.0

DOWNLOAD_PEAK_BATCH_SIZE = 10
DOWNLOAD_PEAK_CONNECT_TIMEOUT_SEC = 15
DOWNLOAD_PEAK_READ_TIMEOUT_SEC = 60
DOWNLOAD_PEAK_MAX_RETRIES = 2
DOWNLOAD_PEAK_RETRY_WAIT_SEC = 3.0
DOWNLOAD_PEAK_PROGRESS_EVERY = 5
DOWNLOAD_PEAK_COOLDOWN_FAILURE_STREAK = 3
DOWNLOAD_PEAK_COOLDOWN_SEC = 180.0
DOWNLOAD_PEAK_MANIFEST_GRANULARITY = "day"

DEFAULT_RULE_VERSION = "v1"
DEFAULT_DERIVED_METRICS_RULE_VERSION = "2026-04-04-v1"


def ensure_data_dirs() -> None:
    MONITOR_DB_ROOT.mkdir(parents=True, exist_ok=True)
    RAW_ROOT.mkdir(parents=True, exist_ok=True)
    ZIP_ROOT.mkdir(parents=True, exist_ok=True)
    XBRL_ROOT.mkdir(parents=True, exist_ok=True)
    MANIFEST_ROOT.mkdir(parents=True, exist_ok=True)
    LOG_ROOT.mkdir(parents=True, exist_ok=True)
