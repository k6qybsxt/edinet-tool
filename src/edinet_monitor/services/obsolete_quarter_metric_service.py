from __future__ import annotations

import sqlite3
from typing import Any


STANDALONE_QUARTER_TYPES = ("1Q", "2Q", "3Q", "4Q")

OBSOLETE_QUARTER_STANDALONE_BASES = frozenset()

FCF_GROWTH_BASE = "FCFGrowthRate"
FCF_GROWTH_METRIC_KEY = "FCFGrowthRateCurrent"
FCF_GROWTH_METRIC_TABLES = (
    "derived_metrics",
    "jquants_financial_metrics",
    "market_derived_metrics",
    "quarter_standalone_metrics",
    "industry_aggregate_metrics",
    "segment_metrics",
)


def table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table_name,),
    ).fetchone()
    return row is not None


def table_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    if not table_exists(conn, table_name):
        return set()
    return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table_name})").fetchall()}


def _placeholders(values: tuple[str, ...] | list[str] | frozenset[str]) -> str:
    return ",".join("?" for _ in values)


def is_obsolete_quarter_standalone_metric(metric_base: str, quarter_type: str) -> bool:
    if metric_base == FCF_GROWTH_BASE:
        return True
    return quarter_type in STANDALONE_QUARTER_TYPES and metric_base in OBSOLETE_QUARTER_STANDALONE_BASES


def _count_quarter_standalone_rows(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    if not table_exists(conn, "quarter_standalone_metrics"):
        return []
    bases = tuple(sorted(OBSOLETE_QUARTER_STANDALONE_BASES))
    if not bases:
        return []
    rows = conn.execute(
        f"""
        SELECT
          'quarter_standalone_metrics' AS table_name,
          'standalone_1q_4q' AS target_scope,
          metric_base,
          COUNT(*) AS row_count
        FROM quarter_standalone_metrics
        WHERE quarter_type IN ({_placeholders(STANDALONE_QUARTER_TYPES)})
          AND metric_base IN ({_placeholders(bases)})
        GROUP BY metric_base
        ORDER BY metric_base
        """,
        [*STANDALONE_QUARTER_TYPES, *bases],
    ).fetchall()
    return [dict(row) for row in rows]


def _count_fcf_growth_rows(conn: sqlite3.Connection, table_name: str) -> list[dict[str, Any]]:
    columns = table_columns(conn, table_name)
    if not columns:
        return []
    if "metric_base" in columns:
        rows = conn.execute(
            f"""
            SELECT
              ? AS table_name,
              'all_scopes' AS target_scope,
              metric_base,
              COUNT(*) AS row_count
            FROM {table_name}
            WHERE metric_base = ?
            GROUP BY metric_base
            """,
            (table_name, FCF_GROWTH_BASE),
        ).fetchall()
        return [dict(row) for row in rows]
    if "metric_key" in columns:
        row = conn.execute(
            f"""
            SELECT
              ? AS table_name,
              'all_scopes' AS target_scope,
              ? AS metric_base,
              COUNT(*) AS row_count
            FROM {table_name}
            WHERE metric_key = ?
            """,
            (table_name, FCF_GROWTH_BASE, FCF_GROWTH_METRIC_KEY),
        ).fetchone()
        if row is not None and int(row["row_count"]) > 0:
            return [dict(row)]
    return []


def count_obsolete_quarter_metrics(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    rows.extend(_count_quarter_standalone_rows(conn))
    if table_exists(conn, "normalized_metrics"):
        rows.extend(_count_fcf_growth_rows(conn, "normalized_metrics"))
    for table_name in FCF_GROWTH_METRIC_TABLES:
        rows.extend(_count_fcf_growth_rows(conn, table_name))
    rows = [row for row in rows if int(row.get("row_count") or 0) > 0]
    rows.sort(
        key=lambda row: (
            str(row["table_name"]),
            str(row["target_scope"]),
            str(row["metric_base"]),
        )
    )
    return rows


def _delete_quarter_standalone_rows(conn: sqlite3.Connection) -> None:
    if not table_exists(conn, "quarter_standalone_metrics"):
        return
    bases = tuple(sorted(OBSOLETE_QUARTER_STANDALONE_BASES))
    if not bases:
        return
    conn.execute(
        f"""
        DELETE FROM quarter_standalone_metrics
        WHERE quarter_type IN ({_placeholders(STANDALONE_QUARTER_TYPES)})
          AND metric_base IN ({_placeholders(bases)})
        """,
        [*STANDALONE_QUARTER_TYPES, *bases],
    )


def _delete_fcf_growth_rows(conn: sqlite3.Connection, table_name: str) -> None:
    columns = table_columns(conn, table_name)
    if not columns:
        return
    if "metric_base" in columns:
        conn.execute(f"DELETE FROM {table_name} WHERE metric_base = ?", (FCF_GROWTH_BASE,))
    elif "metric_key" in columns:
        conn.execute(f"DELETE FROM {table_name} WHERE metric_key = ?", (FCF_GROWTH_METRIC_KEY,))


def delete_obsolete_quarter_metrics(conn: sqlite3.Connection) -> int:
    before = sum(int(row["row_count"]) for row in count_obsolete_quarter_metrics(conn))
    _delete_quarter_standalone_rows(conn)
    if table_exists(conn, "normalized_metrics"):
        _delete_fcf_growth_rows(conn, "normalized_metrics")
    for table_name in FCF_GROWTH_METRIC_TABLES:
        _delete_fcf_growth_rows(conn, table_name)
    conn.commit()
    return before
