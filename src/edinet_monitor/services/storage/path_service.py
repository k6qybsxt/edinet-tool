from __future__ import annotations

from pathlib import Path

from edinet_monitor.config.settings import ZIP_ROOT


def build_zip_save_path(submit_date: str, doc_id: str) -> Path:
    yyyy_mm_dd = str(submit_date or "")[:10]
    if not yyyy_mm_dd:
        yyyy_mm_dd = "unknown_date"

    return ZIP_ROOT / yyyy_mm_dd / f"{doc_id}.zip"