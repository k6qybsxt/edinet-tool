from __future__ import annotations

import sqlite3
from datetime import datetime

from edinet_monitor.services.normalizer.metric_normalize_service import dedupe_normalized_metrics


def now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def delete_normalized_metrics_by_doc_id(conn: sqlite3.Connection, doc_id: str) -> None:
    conn.execute("DELETE FROM normalized_metrics WHERE doc_id = ?", (doc_id,))
    conn.commit()


def insert_normalized_metrics(conn: sqlite3.Connection, rows: list[dict]) -> int:
    if not rows:
        return 0

    rows = dedupe_normalized_metrics(rows)

    created_at = now_text()
    prepared = []

    for row in rows:
        prepared.append(
            {
                "doc_id": row["doc_id"],
                "edinet_code": row["edinet_code"],
                "security_code": row["security_code"],
                "metric_key": row["metric_key"],
                "fiscal_year": row["fiscal_year"],
                "period_end": row["period_end"],
                "value_num": row["value_num"],
                "source_tag": row["source_tag"],
                "consolidation": row["consolidation"],
                "rule_version": row["rule_version"],
                "created_at": created_at,
                "updated_at": created_at,
            }
        )

    conn.executemany(
        """
        INSERT OR REPLACE INTO normalized_metrics (
            doc_id,
            edinet_code,
            security_code,
            metric_key,
            fiscal_year,
            period_end,
            value_num,
            source_tag,
            consolidation,
            rule_version,
            created_at,
            updated_at
        )
        VALUES (
            :doc_id,
            :edinet_code,
            :security_code,
            :metric_key,
            :fiscal_year,
            :period_end,
            :value_num,
            :source_tag,
            :consolidation,
            :rule_version,
            :created_at,
            :updated_at
        )
        """,
        prepared,
    )
    conn.commit()
    return len(prepared)