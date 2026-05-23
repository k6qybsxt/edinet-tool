from __future__ import annotations

import argparse
import sqlite3
from typing import Any

from edinet_monitor.config.settings import DB_PATH
from edinet_monitor.db.schema import get_connection
from edinet_monitor.services.collector.document_filter_service import HALF_REPORT_FORM_CODES


HALF_FORM_TYPES = tuple(sorted(HALF_REPORT_FORM_CODES))
NORMALIZED_SUFFIXES = ("Current", "Prior1", "Prior2", "Prior3", "Prior4")

OBSOLETE_HALF_DERIVED_METRIC_KEYS = [
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
    "StockPriceGrowthRate5YearCurrent",
    "StockPriceGrowthRate10YearCurrent",
    "TheoreticalSharePriceGrowthRate5YearCurrent",
    "TheoreticalSharePriceGrowthRate10YearCurrent",
    "AssetsPerShareCurrent",
    "LiabilitiesPerShareCurrent",
]

OBSOLETE_HALF_NORMALIZED_BASES = (
    "NumberOfEmployees",
    "AverageAge",
    "AverageAnnualSalary",
)
OBSOLETE_HALF_NORMALIZED_METRIC_KEYS = [
    f"{base}{suffix}"
    for base in OBSOLETE_HALF_NORMALIZED_BASES
    for suffix in NORMALIZED_SUFFIXES
]

OBSOLETE_HALF_MARKET_METRIC_KEYS = [
    "StockPriceGrowthRate5YearCurrent",
    "StockPriceGrowthRate10YearCurrent",
]


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table_name,),
    ).fetchone()
    return row is not None


def _placeholders(values: list[str] | tuple[str, ...]) -> str:
    return ",".join("?" for _ in values)


def _count_filing_metric_rows(
    conn: sqlite3.Connection,
    *,
    table_name: str,
    metric_keys: list[str],
) -> list[dict[str, Any]]:
    if not metric_keys or not _table_exists(conn, table_name) or not _table_exists(conn, "filings"):
        return []
    rows = conn.execute(
        f"""
        SELECT
          ? AS metric_source,
          m.metric_key,
          COUNT(*) AS row_count
        FROM {table_name} m
        JOIN filings f
          ON f.doc_id = m.doc_id
        WHERE f.form_type IN ({_placeholders(HALF_FORM_TYPES)})
          AND m.metric_key IN ({_placeholders(metric_keys)})
        GROUP BY m.metric_key
        ORDER BY m.metric_key
        """,
        [table_name, *HALF_FORM_TYPES, *metric_keys],
    ).fetchall()
    return [dict(row) for row in rows]


def _count_market_metric_rows(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    if not _table_exists(conn, "market_derived_metrics"):
        return []
    rows = conn.execute(
        f"""
        SELECT
          'market_derived_metrics' AS metric_source,
          metric_key,
          COUNT(*) AS row_count
        FROM market_derived_metrics
        WHERE metric_key IN ({_placeholders(OBSOLETE_HALF_MARKET_METRIC_KEYS)})
          AND period_scope = 'quarter'
          AND (quarter_type = '2Q' OR period_key = 'actual:2Q')
        GROUP BY metric_key
        ORDER BY metric_key
        """,
        OBSOLETE_HALF_MARKET_METRIC_KEYS,
    ).fetchall()
    return [dict(row) for row in rows]


def count_obsolete_half_metrics(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    rows.extend(
        _count_filing_metric_rows(
            conn,
            table_name="derived_metrics",
            metric_keys=OBSOLETE_HALF_DERIVED_METRIC_KEYS,
        )
    )
    rows.extend(
        _count_filing_metric_rows(
            conn,
            table_name="normalized_metrics",
            metric_keys=OBSOLETE_HALF_NORMALIZED_METRIC_KEYS,
        )
    )
    rows.extend(_count_market_metric_rows(conn))
    rows.sort(key=lambda row: (str(row["metric_source"]), str(row["metric_key"])))
    return rows


def _delete_filing_metric_rows(
    conn: sqlite3.Connection,
    *,
    table_name: str,
    metric_keys: list[str],
) -> None:
    if not metric_keys or not _table_exists(conn, table_name) or not _table_exists(conn, "filings"):
        return
    conn.execute(
        f"""
        DELETE FROM {table_name}
        WHERE metric_key IN ({_placeholders(metric_keys)})
          AND doc_id IN (
            SELECT doc_id
            FROM filings
            WHERE form_type IN ({_placeholders(HALF_FORM_TYPES)})
          )
        """,
        [*metric_keys, *HALF_FORM_TYPES],
    )


def _delete_market_metric_rows(conn: sqlite3.Connection) -> None:
    if not _table_exists(conn, "market_derived_metrics"):
        return
    conn.execute(
        f"""
        DELETE FROM market_derived_metrics
        WHERE metric_key IN ({_placeholders(OBSOLETE_HALF_MARKET_METRIC_KEYS)})
          AND period_scope = 'quarter'
          AND (quarter_type = '2Q' OR period_key = 'actual:2Q')
        """,
        OBSOLETE_HALF_MARKET_METRIC_KEYS,
    )


def delete_obsolete_half_metrics(conn: sqlite3.Connection) -> int:
    before = sum(int(row["row_count"]) for row in count_obsolete_half_metrics(conn))
    _delete_filing_metric_rows(
        conn,
        table_name="derived_metrics",
        metric_keys=OBSOLETE_HALF_DERIVED_METRIC_KEYS,
    )
    _delete_filing_metric_rows(
        conn,
        table_name="normalized_metrics",
        metric_keys=OBSOLETE_HALF_NORMALIZED_METRIC_KEYS,
    )
    _delete_market_metric_rows(conn)
    conn.commit()
    return before


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Count or delete obsolete 2Q metrics from normalized, derived, and market tables."
    )
    parser.add_argument("--apply", action="store_true", help="Delete rows. Omit for dry-run.")
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
            print(f"{row['metric_source']}:{row['metric_key']}: {row['row_count']}")
        if args.apply:
            deleted = delete_obsolete_half_metrics(conn)
            print(f"deleted_rows={deleted}")
            remaining = sum(int(row["row_count"]) for row in count_obsolete_half_metrics(conn))
            print(f"remaining_rows={remaining}")
        else:
            print("dry_run_only=1")
            print("hint=run with --apply to delete rows")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
