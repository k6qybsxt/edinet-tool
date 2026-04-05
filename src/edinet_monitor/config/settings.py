from __future__ import annotations

from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[3]

# =========================================================
# edinet_monitor 保存先
#   - DB は C ドライブ側に置く
#   - ZIP / XBRL / logs は D ドライブ側に置く
# =========================================================
MONITOR_DB_ROOT = PROJECT_ROOT / "data" / "edinet_monitor"
DB_PATH = MONITOR_DB_ROOT / "edinet_monitor.db"

MONITOR_STORAGE_ROOT = Path(r"D:\EDINET_Data") / "edinet_monitor"
TSE_LISTING_MASTER_CSV_PATH = Path(r"D:\EDINET_Data\master\tse_issuer_master_latest.csv")

RAW_ROOT = MONITOR_STORAGE_ROOT / "raw"
ZIP_ROOT = RAW_ROOT / "zip"
XBRL_ROOT = RAW_ROOT / "xbrl"
MANIFEST_ROOT = RAW_ROOT / "manifests"

LOG_ROOT = MONITOR_STORAGE_ROOT / "logs"

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

DEFAULT_RULE_VERSION = "v1"
DEFAULT_DERIVED_METRICS_RULE_VERSION = "2026-04-04-v1"


def ensure_data_dirs() -> None:
    MONITOR_DB_ROOT.mkdir(parents=True, exist_ok=True)
    RAW_ROOT.mkdir(parents=True, exist_ok=True)
    ZIP_ROOT.mkdir(parents=True, exist_ok=True)
    XBRL_ROOT.mkdir(parents=True, exist_ok=True)
    MANIFEST_ROOT.mkdir(parents=True, exist_ok=True)
    LOG_ROOT.mkdir(parents=True, exist_ok=True)
