from __future__ import annotations

import sqlite3


RAW_FACT_COLUMNS = [
    "doc_id",
    "tag_name",
    "tag_qname",
    "namespace_uri",
    "namespace_prefix",
    "taxonomy_kind",
    "context_ref",
    "unit_ref",
    "decimals",
    "period_type",
    "period_start",
    "period_end",
    "instant_date",
    "consolidation",
    "is_nil",
    "context_dimensions_json",
    "unit_measures_json",
    "xbrl_member_name",
    "value_text",
    "created_at",
]


def delete_raw_facts_by_doc_id(conn: sqlite3.Connection, doc_id: str) -> None:
    conn.execute("DELETE FROM raw_facts WHERE doc_id = ?", (doc_id,))
    conn.commit()


def _get_table_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return {str(row[1]) for row in rows}


def insert_raw_facts(conn: sqlite3.Connection, rows: list[dict]) -> int:
    if not rows:
        return 0

    available_columns = _get_table_columns(conn, "raw_facts")
    insert_columns = [column for column in RAW_FACT_COLUMNS if column in available_columns]
    placeholders = ", ".join(f":{column}" for column in insert_columns)
    column_sql = ",\n            ".join(insert_columns)
    payloads = [
        {column: row.get(column) for column in insert_columns}
        for row in rows
    ]

    conn.executemany(
        f"""
        INSERT INTO raw_facts (
            {column_sql}
        )
        VALUES (
            {placeholders}
        )
        """,
        payloads,
    )
    conn.commit()
    return len(rows)
