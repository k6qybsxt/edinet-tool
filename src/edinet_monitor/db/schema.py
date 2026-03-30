from __future__ import annotations

import sqlite3

from edinet_monitor.config.settings import DB_PATH, ensure_data_dirs


def get_connection() -> sqlite3.Connection:
    ensure_data_dirs()
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _get_table_columns(cur: sqlite3.Cursor, table_name: str) -> set[str]:
    rows = cur.execute(f"PRAGMA table_info({table_name})").fetchall()
    return {str(row[1]) for row in rows}


def _rebuild_screening_results_if_needed(cur: sqlite3.Cursor) -> None:
    table_exists = cur.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type = 'table' AND name = 'screening_results'
        """
    ).fetchone()

    if not table_exists:
        return

    columns = _get_table_columns(cur, "screening_results")
    required_columns = {
        "screening_run_id",
        "screening_date",
        "rule_name",
        "rule_version",
        "edinet_code",
        "security_code",
        "company_name",
        "period_end",
        "result_flag",
        "score",
        "detail_json",
        "created_at",
    }

    if required_columns.issubset(columns):
        return

    cur.execute("DROP TABLE IF EXISTS notifications")
    cur.execute("DROP TABLE IF EXISTS screening_results")
    cur.execute("DROP TABLE IF EXISTS screening_runs")

def _rebuild_filings_if_needed(cur: sqlite3.Cursor) -> None:
    table_exists = cur.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type = 'table' AND name = 'filings'
        """
    ).fetchone()

    if not table_exists:
        return

    columns = _get_table_columns(cur, "filings")
    required_columns = {
        "doc_info_edit_status",
        "legal_status",
    }

    if required_columns.issubset(columns):
        return

    cur.execute("DROP TABLE IF EXISTS filings")

def _rebuild_issuer_master_if_needed(cur: sqlite3.Cursor) -> None:
    table_exists = cur.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type = 'table' AND name = 'issuer_master'
        """
    ).fetchone()

    if not table_exists:
        return

    columns = _get_table_columns(cur, "issuer_master")
    required_columns = {
        "exchange",
        "listing_source",
    }

    if required_columns.issubset(columns):
        return

    cur.execute("DROP TABLE IF EXISTS issuer_master")

def create_tables() -> None:
    conn = get_connection()
    cur = conn.cursor()

    _rebuild_screening_results_if_needed(cur)
    _rebuild_filings_if_needed(cur)
    _rebuild_issuer_master_if_needed(cur)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS issuer_master (
        edinet_code TEXT PRIMARY KEY,
        security_code TEXT,
        company_name TEXT NOT NULL,
        market TEXT,
        industry TEXT,
        is_listed INTEGER NOT NULL DEFAULT 1,
        exchange TEXT,
        listing_source TEXT,
        updated_at TEXT NOT NULL
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS filings (
        doc_id TEXT PRIMARY KEY,
        edinet_code TEXT NOT NULL,
        security_code TEXT,
        form_type TEXT NOT NULL,
        period_end TEXT,
        submit_date TEXT,
        amendment_flag INTEGER NOT NULL DEFAULT 0,
        doc_info_edit_status TEXT,
        legal_status TEXT,
        zip_path TEXT,
        xbrl_path TEXT,
        download_status TEXT NOT NULL,
        parse_status TEXT NOT NULL,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS raw_facts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        doc_id TEXT NOT NULL,
        tag_name TEXT NOT NULL,
        context_ref TEXT,
        unit_ref TEXT,
        period_type TEXT,
        period_start TEXT,
        period_end TEXT,
        instant_date TEXT,
        consolidation TEXT,
        value_text TEXT,
        created_at TEXT NOT NULL
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS normalized_metrics (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        doc_id TEXT NOT NULL,
        edinet_code TEXT NOT NULL,
        security_code TEXT,
        metric_key TEXT NOT NULL,
        fiscal_year INTEGER,
        period_end TEXT,
        value_num REAL,
        source_tag TEXT,
        consolidation TEXT,
        rule_version TEXT NOT NULL,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS screening_runs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        screening_date TEXT NOT NULL,
        rule_name TEXT NOT NULL,
        rule_version TEXT NOT NULL,
        target_count INTEGER NOT NULL DEFAULT 0,
        hit_count INTEGER NOT NULL DEFAULT 0,
        created_at TEXT NOT NULL
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS screening_results (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        screening_run_id INTEGER NOT NULL,
        screening_date TEXT NOT NULL,
        rule_name TEXT NOT NULL,
        rule_version TEXT NOT NULL,
        edinet_code TEXT NOT NULL,
        security_code TEXT,
        company_name TEXT,
        period_end TEXT,
        result_flag INTEGER NOT NULL,
        score REAL,
        detail_json TEXT,
        created_at TEXT NOT NULL
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS notifications (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        screening_result_id INTEGER NOT NULL,
        notify_type TEXT NOT NULL,
        notify_status TEXT NOT NULL,
        sent_at TEXT,
        created_at TEXT NOT NULL
    )
    """)

    cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_filings_edinet_code
    ON filings(edinet_code)
    """)

    cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_filings_submit_date
    ON filings(submit_date)
    """)

    cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_filings_parse_status
    ON filings(parse_status)
    """)

    cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_raw_facts_doc_id
    ON raw_facts(doc_id)
    """)

    cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_raw_facts_tag_name
    ON raw_facts(tag_name)
    """)

    cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_raw_facts_doc_tag
    ON raw_facts(doc_id, tag_name)
    """)

    cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_normalized_metrics_doc_id
    ON normalized_metrics(doc_id)
    """)

    cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_normalized_metrics_code_metric
    ON normalized_metrics(security_code, metric_key, period_end)
    """)

    cur.execute("""
    CREATE UNIQUE INDEX IF NOT EXISTS uq_normalized_metrics_doc_metric_period
    ON normalized_metrics(doc_id, metric_key, period_end)
    """)

    cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_normalized_metrics_code_period
    ON normalized_metrics(edinet_code, period_end)
    """)

    cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_screening_runs_date_rule
    ON screening_runs(screening_date, rule_name)
    """)

    cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_screening_results_run_id
    ON screening_results(screening_run_id)
    """)

    cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_screening_results_date_rule
    ON screening_results(screening_date, rule_name)
    """)

    cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_screening_results_code
    ON screening_results(edinet_code, security_code)
    """)

    conn.commit()
    conn.close()


if __name__ == "__main__":
    create_tables()
    print(f"DB created: {DB_PATH}")