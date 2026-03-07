import json
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parents[3]


def load_config(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        return json.load(f)