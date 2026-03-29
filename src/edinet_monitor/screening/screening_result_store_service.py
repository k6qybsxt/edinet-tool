from __future__ import annotations

import json
import sqlite3
from datetime import datetime


def now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def insert_screening_run(
    conn: sqlite3.Connection,
    *,
    screening_date: str,
    rule_name: str,
    rule_version: str,
    target_count: int,
    hit_count: int,
) -> int:
    created_at = now_text()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO screening_runs (
            screening_date,
            rule_name,
            rule_version,
            target_count,
            hit_count,
            created_at
        )
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            screening_date,
            rule_name,
            rule_version,
            target_count,
            hit_count,
            created_at,
        ),
    )
    conn.commit()
    return int(cur.lastrowid)


def insert_screening_result(
    conn: sqlite3.Connection,
    *,
    screening_run_id: int,
    screening_date: str,
    rule_name: str,
    rule_version: str,
    edinet_code: str,
    security_code: str | None,
    company_name: str | None,
    period_end: str | None,
    result_flag: int,
    score: float | None,
    detail: dict,
) -> int:
    created_at = now_text()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO screening_results (
            screening_run_id,
            screening_date,
            rule_name,
            rule_version,
            edinet_code,
            security_code,
            company_name,
            period_end,
            result_flag,
            score,
            detail_json,
            created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            screening_run_id,
            screening_date,
            rule_name,
            rule_version,
            edinet_code,
            security_code,
            company_name,
            period_end,
            result_flag,
            score,
            json.dumps(detail, ensure_ascii=False),
            created_at,
        ),
    )
    conn.commit()
    return int(cur.lastrowid)