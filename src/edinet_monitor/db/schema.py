from __future__ import annotations

import sqlite3

from edinet_monitor.config.settings import DB_PATH, ensure_data_dirs


def get_connection() -> sqlite3.Connection:
    ensure_data_dirs()
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def create_tables() -> None:
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS issuer_master (
        edinet_code TEXT PRIMARY KEY,
        security_code TEXT,
        company_name TEXT NOT NULL,
        market TEXT,
        industry TEXT,
        is_listed INTEGER NOT NULL DEFAULT 1,
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
    CREATE TABLE IF NOT EXISTS screening_results (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        screening_date TEXT NOT NULL,
        rule_name TEXT NOT NULL,
        edinet_code TEXT NOT NULL,
        security_code TEXT,
        company_name TEXT NOT NULL,
        result_flag INTEGER NOT NULL,
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
    CREATE INDEX IF NOT EXISTS idx_raw_facts_doc_id
    ON raw_facts(doc_id)
    """)

    cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_raw_facts_tag_name
    ON raw_facts(tag_name)
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
    CREATE INDEX IF NOT EXISTS idx_screening_results_date_rule
    ON screening_results(screening_date, rule_name)
    """)

    conn.commit()
    conn.close()


if __name__ == "__main__":
    create_tables()
    print(f"DB created: {DB_PATH}")