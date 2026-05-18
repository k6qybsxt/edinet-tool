from __future__ import annotations

import argparse
import sqlite3
from typing import Any

from edinet_monitor.config.settings import DB_PATH
from edinet_monitor.db.schema import get_connection


OBSOLETE_HALF_METRIC_KEYS = [
    "CashBalanceGrowthRateCurrent",
    "NetSalesGrowthRate5YearCurrent",
    "NetSalesGrowthRate10YearCurrent",
    "OrdinaryIncomeGrowthRate5YearCurrent",
    "OrdinaryIncomeGrowthRate10YearCurrent",
    "CashBalanceGrowthRate5YearCurrent",
    "CashBalanceGrowthRate10YearCurrent",
    "OutstandingSharesGrowthRateCurrent",
    "OutstandingSharesGrowthRate5YearCurrent",
    "OutstandingSharesGrowthRate10YearCurrent",
    "TheoreticalSharePriceGrowthRate5YearCurrent",
    "TheoreticalSharePriceGrowthRate10YearCurrent",
    "AssetsPerShareCurrent",
    "LiabilitiesPerShareCurrent",
]


def count_obsolete_half_metrics(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    placeholders = ",".join("?" for _ in OBSOLETE_HALF_METRIC_KEYS)
    rows = conn.execute(
        f"""
        SELECT
          dm.metric_key,
          COUNT(*) AS row_count
        FROM derived_metrics dm
        JOIN filings f
          ON f.doc_id = dm.doc_id
        WHERE f.form_type = '043A00'
          AND dm.metric_key IN ({placeholders})
        GROUP BY dm.metric_key
        ORDER BY dm.metric_key
        """,
        OBSOLETE_HALF_METRIC_KEYS,
    ).fetchall()
    return [dict(row) for row in rows]


def delete_obsolete_half_metrics(conn: sqlite3.Connection) -> int:
    placeholders = ",".join("?" for _ in OBSOLETE_HALF_METRIC_KEYS)
    before = sum(int(row["row_count"]) for row in count_obsolete_half_metrics(conn))
    conn.execute(
        f"""
        DELETE FROM derived_metrics
        WHERE metric_key IN ({placeholders})
          AND doc_id IN (
            SELECT doc_id
            FROM filings
            WHERE form_type = '043A00'
          )
        """,
        OBSOLETE_HALF_METRIC_KEYS,
    )
    conn.commit()
    return before


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="半期前年比5年/10年系の不要derived_metricsを確認・削除します。"
    )
    parser.add_argument("--apply", action="store_true", help="指定した場合だけDBから削除します。")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    conn = get_connection()
    conn.row_factory = sqlite3.Row
    try:
        rows = count_obsolete_half_metrics(conn)
        total = sum(int(row["row_count"]) for row in rows)
        print(f"db_path={DB_PATH}")
        print(f"mode={'apply' if args.apply else 'dry_run'}")
        print(f"target_rows={total}")
        for row in rows:
            print(f"{row['metric_key']}: {row['row_count']}")
        if args.apply:
            deleted = delete_obsolete_half_metrics(conn)
            print(f"deleted_rows={deleted}")
            remaining = sum(int(row["row_count"]) for row in count_obsolete_half_metrics(conn))
            print(f"remaining_rows={remaining}")
        else:
            print("dry_run_only=1")
            print("hint=削除する場合は --apply を付けてください。")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
