from __future__ import annotations

import sqlite3


def delete_raw_facts_by_doc_id(conn: sqlite3.Connection, doc_id: str) -> None:
    conn.execute("DELETE FROM raw_facts WHERE doc_id = ?", (doc_id,))
    conn.commit()


def insert_raw_facts(conn: sqlite3.Connection, rows: list[dict]) -> int:
    if not rows:
        return 0

    conn.executemany(
        """
        INSERT INTO raw_facts (
            doc_id,
            tag_name,
            context_ref,
            unit_ref,
            period_type,
            period_start,
            period_end,
            instant_date,
            consolidation,
            value_text,
            created_at
        )
        VALUES (
            :doc_id,
            :tag_name,
            :context_ref,
            :unit_ref,
            :period_type,
            :period_start,
            :period_end,
            :instant_date,
            :consolidation,
            :value_text,
            :created_at
        )
        """,
        rows,
    )
    conn.commit()
    return len(rows)