from __future__ import annotations

import sqlite3
from typing import Any


def fetch_table_counts(conn: sqlite3.Connection) -> dict[str, int]:
    counts: dict[str, int] = {}
    for table_name in [
        "issuer_master",
        "filings",
        "raw_facts",
        "normalized_metrics",
        "derived_metrics",
        "screening_runs",
        "screening_results",
        "notifications",
    ]:
        row = conn.execute(f"SELECT COUNT(*) AS row_count FROM {table_name}").fetchone()
        counts[table_name] = int(row["row_count"]) if row else 0
    return counts


def fetch_latest_filing_status_rows(
    conn: sqlite3.Connection,
    *,
    limit: int = 10,
    listed_only: bool = True,
) -> list[sqlite3.Row]:
    where_clauses = []
    params: list[Any] = []
    if listed_only:
        where_clauses.append("is_listed = 1")
    where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
    return conn.execute(
        f"""
        SELECT *
        FROM issuer_latest_filing_status
        {where_sql}
        ORDER BY COALESCE(submit_date, '') DESC, edinet_code
        LIMIT ?
        """,
        (*params, limit),
    ).fetchall()


def fetch_monthly_collection_status_rows(
    conn: sqlite3.Connection,
    *,
    limit: int = 12,
) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT *
        FROM monthly_collection_status
        ORDER BY submit_month DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()


def fetch_metric_coverage_rows(
    conn: sqlite3.Connection,
    *,
    metric_source: str | None = None,
    metric_key_like: str | None = None,
    limit: int = 20,
) -> list[sqlite3.Row]:
    where_clauses: list[str] = []
    params: list[Any] = []

    if metric_source:
        where_clauses.append("metric_source = ?")
        params.append(metric_source)

    if metric_key_like:
        where_clauses.append("metric_key LIKE ?")
        params.append(metric_key_like)

    where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
    return conn.execute(
        f"""
        SELECT *
        FROM metric_coverage_summary
        {where_sql}
        ORDER BY doc_count DESC, metric_source, metric_key
        LIMIT ?
        """,
        (*params, limit),
    ).fetchall()


def fetch_screening_hit_summary_rows(
    conn: sqlite3.Connection,
    *,
    limit: int = 10,
) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT *
        FROM screening_hit_summary
        ORDER BY screening_date DESC, rule_name
        LIMIT ?
        """,
        (limit,),
    ).fetchall()

