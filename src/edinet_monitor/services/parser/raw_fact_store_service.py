from __future__ import annotations

import sqlite3
from collections.abc import Sequence


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


def delete_raw_facts_by_doc_id(conn: sqlite3.Connection, doc_id: str, *, commit: bool = True) -> None:
    conn.execute("DELETE FROM raw_facts WHERE doc_id = ?", (doc_id,))
    if commit:
        conn.commit()


def _chunked_values(values: Sequence[str], chunk_size: int) -> list[list[str]]:
    size = max(int(chunk_size or 1), 1)
    return [list(values[index:index + size]) for index in range(0, len(values), size)]


def delete_raw_facts_by_doc_ids(
    conn: sqlite3.Connection,
    doc_ids: Sequence[str],
    *,
    chunk_size: int = 500,
    commit: bool = True,
) -> int:
    clean_doc_ids = [str(doc_id) for doc_id in doc_ids if str(doc_id)]
    if not clean_doc_ids:
        return 0

    deleted_total = 0
    for chunk in _chunked_values(clean_doc_ids, chunk_size):
        placeholders = ",".join("?" for _ in chunk)
        cursor = conn.execute(
            f"DELETE FROM raw_facts WHERE doc_id IN ({placeholders})",
            chunk,
        )
        deleted_total += int(cursor.rowcount if cursor.rowcount is not None else 0)
    if commit:
        conn.commit()
    return deleted_total


def _get_table_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return {str(row[1]) for row in rows}


class RawFactInserter:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn
        available_columns = _get_table_columns(conn, "raw_facts")
        self.insert_columns = [column for column in RAW_FACT_COLUMNS if column in available_columns]
        placeholders = ", ".join(f":{column}" for column in self.insert_columns)
        column_sql = ",\n            ".join(self.insert_columns)
        self.insert_sql = f"""
        INSERT INTO raw_facts (
            {column_sql}
        )
        VALUES (
            {placeholders}
        )
        """

    def insert_many(self, rows: Sequence[dict], *, chunk_size: int = 50000) -> int:
        if not rows:
            return 0

        size = max(int(chunk_size or 1), 1)
        saved_total = 0
        for index in range(0, len(rows), size):
            row_chunk = rows[index:index + size]
            payloads = [
                {column: row.get(column) for column in self.insert_columns}
                for row in row_chunk
            ]
            self.conn.executemany(self.insert_sql, payloads)
            saved_total += len(row_chunk)
        return saved_total


def insert_raw_facts(conn: sqlite3.Connection, rows: list[dict], *, commit: bool = True) -> int:
    if not rows:
        return 0

    saved_count = RawFactInserter(conn).insert_many(rows, chunk_size=len(rows))
    if commit:
        conn.commit()
    return saved_count
