from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from typing import Sequence


DERIVED_METRIC_INSERT_SQL = """
INSERT OR REPLACE INTO derived_metrics (
    doc_id,
    edinet_code,
    security_code,
    metric_key,
    metric_base,
    metric_group,
    fiscal_year,
    period_end,
    period_scope,
    period_key,
    quarter_type,
    period_offset,
    consolidation,
    accounting_standard,
    document_display_unit,
    value_num,
    value_unit,
    calc_status,
    formula_name,
    source_detail_json,
    rule_version,
    created_at,
    updated_at
)
VALUES (
    :doc_id,
    :edinet_code,
    :security_code,
    :metric_key,
    :metric_base,
    :metric_group,
    :fiscal_year,
    :period_end,
    :period_scope,
    :period_key,
    :quarter_type,
    :period_offset,
    :consolidation,
    :accounting_standard,
    :document_display_unit,
    :value_num,
    :value_unit,
    :calc_status,
    :formula_name,
    :source_detail_json,
    :rule_version,
    :created_at,
    :updated_at
)
"""


def now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def delete_derived_metrics_by_doc_id(
    conn: sqlite3.Connection,
    doc_id: str,
    *,
    commit: bool = True,
) -> None:
    conn.execute("DELETE FROM derived_metrics WHERE doc_id = ?", (doc_id,))
    if commit:
        conn.commit()


def _chunked_values(values: Sequence[str], chunk_size: int) -> list[list[str]]:
    size = max(int(chunk_size or 1), 1)
    return [list(values[index:index + size]) for index in range(0, len(values), size)]


def delete_derived_metrics_by_doc_ids(
    conn: sqlite3.Connection,
    doc_ids: Sequence[str],
    *,
    chunk_size: int = 500,
    commit: bool = True,
) -> int:
    clean_doc_ids = [str(doc_id) for doc_id in doc_ids if str(doc_id or "")]
    if not clean_doc_ids:
        return 0

    deleted_total = 0
    for chunk in _chunked_values(clean_doc_ids, chunk_size):
        placeholders = ",".join("?" for _ in chunk)
        cursor = conn.execute(
            f"DELETE FROM derived_metrics WHERE doc_id IN ({placeholders})",
            chunk,
        )
        deleted_total += int(cursor.rowcount if cursor.rowcount is not None else 0)
    if commit:
        conn.commit()
    return deleted_total


def _prepare_row(row: dict, created_at: str) -> dict:
    return {
        "doc_id": row["doc_id"],
        "edinet_code": row["edinet_code"],
        "security_code": row["security_code"],
        "metric_key": row["metric_key"],
        "metric_base": row["metric_base"],
        "metric_group": row["metric_group"],
        "fiscal_year": row["fiscal_year"],
        "period_end": row["period_end"],
        "period_scope": row["period_scope"],
        "period_key": row.get("period_key"),
        "quarter_type": row.get("quarter_type"),
        "period_offset": row["period_offset"],
        "consolidation": row["consolidation"],
        "accounting_standard": row["accounting_standard"],
        "document_display_unit": row["document_display_unit"],
        "value_num": row["value_num"],
        "value_unit": row["value_unit"],
        "calc_status": row["calc_status"],
        "formula_name": row["formula_name"],
        "source_detail_json": json.dumps(row["source_detail_json"], ensure_ascii=False),
        "rule_version": row["rule_version"],
        "created_at": created_at,
        "updated_at": created_at,
    }


class DerivedMetricInserter:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn
        self.insert_sql = DERIVED_METRIC_INSERT_SQL

    def insert_many(self, rows: Sequence[dict], *, chunk_size: int = 50000) -> int:
        if not rows:
            return 0

        size = max(int(chunk_size or 1), 1)
        created_at = now_text()
        saved_total = 0
        for index in range(0, len(rows), size):
            row_chunk = rows[index:index + size]
            prepared = [_prepare_row(row, created_at) for row in row_chunk]
            self.conn.executemany(self.insert_sql, prepared)
            saved_total += len(prepared)
        return saved_total


def insert_derived_metrics(
    conn: sqlite3.Connection,
    rows: list[dict],
    *,
    commit: bool = True,
) -> int:
    if not rows:
        return 0

    saved_count = DerivedMetricInserter(conn).insert_many(rows, chunk_size=len(rows))
    if commit:
        conn.commit()
    return saved_count
