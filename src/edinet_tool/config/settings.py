import json
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parents[3]


def load_config(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        return json.load(f)
    
# === logging settings ===
LOG_LEVEL = "INFO"          # 普段: "INFO" / 調査時: "DEBUG"
LOG_DIR = "logs"
LOG_MAX_BYTES = 2_000_000
LOG_BACKUP_COUNT = 5