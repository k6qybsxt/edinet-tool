from __future__ import annotations

import csv
from datetime import datetime

from edinet_monitor.db.schema import create_tables, get_connection
from edinet_monitor.services.collector.issuer_store_service import upsert_issuers


CSV_PATH = r"D:\EDINET_Data\master\tse_listed_companies.csv"


def now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def load_csv_rows(csv_path: str) -> list[dict]:
    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        return list(reader)


def row_to_issuer_record(row: dict) -> dict:
    security_code = str(row.get("security_code") or "").strip()
    if len(security_code) == 4 and security_code.isdigit():
        security_code = f"{security_code}0"

    return {
        "edinet_code": str(row.get("edinet_code") or "").strip(),
        "security_code": security_code,
        "company_name": str(row.get("company_name") or "").strip(),
        "market": str(row.get("market") or "").strip(),
        "industry": str(row.get("industry") or "").strip(),
        "is_listed": 1,
        "exchange": str(row.get("exchange") or "TSE").strip(),
        "listing_source": "tse_master_csv",
        "updated_at": now_text(),
    }


def main() -> None:
    create_tables()

    rows = load_csv_rows(CSV_PATH)
    issuers = [row_to_issuer_record(row) for row in rows if str(row.get("edinet_code") or "").strip()]

    conn = get_connection()
    try:
        saved_count = upsert_issuers(conn, issuers)
    finally:
        conn.close()

    print(f"csv_path={CSV_PATH}")
    print(f"csv_rows={len(rows)}")
    print(f"saved_count={saved_count}")


if __name__ == "__main__":
    main()