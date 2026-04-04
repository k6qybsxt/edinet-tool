from __future__ import annotations

import json
import sqlite3
from datetime import datetime


def now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def delete_derived_metrics_by_doc_id(conn: sqlite3.Connection, doc_id: str) -> None:
    conn.execute("DELETE FROM derived_metrics WHERE doc_id = ?", (doc_id,))
    conn.commit()


def insert_derived_metrics(conn: sqlite3.Connection, rows: list[dict]) -> int:
    if not rows:
        return 0

    created_at = now_text()
    prepared: list[dict] = []

    for row in rows:
        prepared.append(
            {
                "doc_id": row["doc_id"],
                "edinet_code": row["edinet_code"],
                "security_code": row["security_code"],
                "metric_key": row["metric_key"],
                "metric_base": row["metric_base"],
                "metric_group": row["metric_group"],
                "fiscal_year": row["fiscal_year"],
                "period_end": row["period_end"],
                "period_scope": row["period_scope"],
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
        )

    conn.executemany(
        """
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
        """,
        prepared,
    )
    conn.commit()
    return len(prepared)
