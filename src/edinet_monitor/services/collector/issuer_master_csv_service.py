from __future__ import annotations

import csv
from pathlib import Path

from edinet_monitor.config.settings import TSE_LISTING_MASTER_CSV_PATH


def load_listing_master_rows(csv_path: Path | None = None) -> list[dict[str, str]]:
    target_path = Path(csv_path or TSE_LISTING_MASTER_CSV_PATH)

    with target_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        return [{str(key): str(value or "") for key, value in row.items()} for row in reader]


def load_allowed_edinet_codes(csv_path: Path | None = None) -> set[str]:
    rows = load_listing_master_rows(csv_path)
    allowed_codes: set[str] = set()

    for row in rows:
        edinet_code = str(row.get("edinet_code") or "").strip()
        exchange = str(row.get("exchange") or "TSE").strip()

        if not edinet_code:
            continue

        if exchange and exchange != "TSE":
            continue

        allowed_codes.add(edinet_code)

    return allowed_codes
