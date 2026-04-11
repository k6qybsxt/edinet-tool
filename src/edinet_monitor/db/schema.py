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


def _ensure_table_column(cur: sqlite3.Cursor, table_name: str, column_def: str) -> None:
    column_name = column_def.split()[0]
    columns = _get_table_columns(cur, table_name)
    if column_name in columns:
        return
    cur.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_def}")


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

def _ensure_filings_columns(cur: sqlite3.Cursor) -> None:
    table_exists = cur.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type = 'table' AND name = 'filings'
        """
    ).fetchone()

    if not table_exists:
        return

    _ensure_table_column(cur, "filings", "doc_info_edit_status TEXT")
    _ensure_table_column(cur, "filings", "legal_status TEXT")
    _ensure_table_column(cur, "filings", "accounting_standard TEXT")
    _ensure_table_column(cur, "filings", "document_display_unit TEXT")

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
        "industry_33",
        "industry_17",
        "listing_category_raw",
    }

    if required_columns.issubset(columns):
        return

    cur.execute("DROP TABLE IF EXISTS issuer_master")


def ensure_summary_views(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.executescript(
        """
        DROP VIEW IF EXISTS issuer_latest_filing_status;
        CREATE VIEW issuer_latest_filing_status AS
        WITH latest_filing AS (
            SELECT
                f.*,
                ROW_NUMBER() OVER (
                    PARTITION BY f.edinet_code
                    ORDER BY COALESCE(f.submit_date, '') DESC,
                             COALESCE(f.period_end, '') DESC,
                             f.doc_id DESC
                ) AS row_num
            FROM filings f
        ),
        normalized_counts AS (
            SELECT
                doc_id,
                COUNT(*) AS normalized_metric_count
            FROM normalized_metrics
            GROUP BY doc_id
        ),
        derived_counts AS (
            SELECT
                doc_id,
                COUNT(*) AS derived_metric_count,
                SUM(CASE WHEN calc_status = 'ok' THEN 1 ELSE 0 END) AS derived_metric_ok_count
            FROM derived_metrics
            GROUP BY doc_id
        )
        SELECT
            im.edinet_code,
            COALESCE(lf.security_code, im.security_code) AS security_code,
            im.company_name,
            im.market,
            im.exchange,
            im.industry_33,
            im.industry_17,
            im.is_listed,
            lf.doc_id,
            lf.form_type,
            lf.period_end,
            lf.submit_date,
            lf.download_status,
            lf.parse_status,
            lf.amendment_flag,
            lf.doc_info_edit_status,
            lf.legal_status,
            lf.accounting_standard,
            lf.document_display_unit,
            CASE
                WHEN lf.xbrl_path IS NOT NULL AND lf.xbrl_path <> '' THEN 1
                ELSE 0
            END AS has_xbrl_path,
            COALESCE(nc.normalized_metric_count, 0) AS normalized_metric_count,
            COALESCE(dc.derived_metric_count, 0) AS derived_metric_count,
            COALESCE(dc.derived_metric_ok_count, 0) AS derived_metric_ok_count
        FROM issuer_master im
        LEFT JOIN latest_filing lf
            ON lf.edinet_code = im.edinet_code
           AND lf.row_num = 1
        LEFT JOIN normalized_counts nc
            ON nc.doc_id = lf.doc_id
        LEFT JOIN derived_counts dc
            ON dc.doc_id = lf.doc_id;

        DROP VIEW IF EXISTS monthly_collection_status;
        CREATE VIEW monthly_collection_status AS
        SELECT
            substr(submit_date, 1, 7) AS submit_month,
            COUNT(*) AS filing_count,
            COUNT(DISTINCT edinet_code) AS issuer_count,
            SUM(CASE WHEN download_status = 'downloaded' THEN 1 ELSE 0 END) AS downloaded_count,
            SUM(CASE WHEN parse_status = 'xbrl_ready' THEN 1 ELSE 0 END) AS xbrl_ready_count,
            SUM(CASE WHEN parse_status = 'raw_facts_saved' THEN 1 ELSE 0 END) AS raw_facts_saved_count,
            SUM(CASE WHEN parse_status = 'normalized_metrics_saved' THEN 1 ELSE 0 END) AS normalized_metrics_saved_count,
            SUM(CASE WHEN parse_status = 'derived_metrics_saved' THEN 1 ELSE 0 END) AS derived_metrics_saved_count,
            SUM(CASE WHEN parse_status = 'raw_facts_error' THEN 1 ELSE 0 END) AS raw_facts_error_count,
            SUM(CASE WHEN parse_status = 'normalized_metrics_error' THEN 1 ELSE 0 END) AS normalized_metrics_error_count,
            SUM(CASE WHEN parse_status = 'derived_metrics_error' THEN 1 ELSE 0 END) AS derived_metrics_error_count
        FROM filings
        WHERE submit_date IS NOT NULL
          AND submit_date <> ''
        GROUP BY substr(submit_date, 1, 7);

        DROP VIEW IF EXISTS metric_coverage_summary;
        CREATE VIEW metric_coverage_summary AS
        SELECT
            'normalized_metrics' AS metric_source,
            NULL AS metric_group,
            metric_key,
            COUNT(*) AS row_count,
            COUNT(DISTINCT doc_id) AS doc_count,
            COUNT(DISTINCT edinet_code) AS issuer_count,
            COUNT(*) AS ok_row_count,
            MIN(period_end) AS min_period_end,
            MAX(period_end) AS max_period_end
        FROM normalized_metrics
        GROUP BY metric_key

        UNION ALL

        SELECT
            'derived_metrics' AS metric_source,
            metric_group,
            metric_key,
            COUNT(*) AS row_count,
            COUNT(DISTINCT doc_id) AS doc_count,
            COUNT(DISTINCT edinet_code) AS issuer_count,
            SUM(CASE WHEN calc_status = 'ok' THEN 1 ELSE 0 END) AS ok_row_count,
            MIN(period_end) AS min_period_end,
            MAX(period_end) AS max_period_end
        FROM derived_metrics
        GROUP BY metric_group, metric_key;

        DROP VIEW IF EXISTS screening_hit_summary;
        CREATE VIEW screening_hit_summary AS
        WITH screening_result_agg AS (
            SELECT
                screening_run_id,
                COUNT(*) AS result_count,
                SUM(CASE WHEN result_flag = 1 THEN 1 ELSE 0 END) AS hit_result_count,
                AVG(CASE WHEN result_flag = 1 THEN score END) AS avg_hit_score,
                MAX(period_end) AS latest_period_end
            FROM screening_results
            GROUP BY screening_run_id
        )
        SELECT
            sr.id AS screening_run_id,
            sr.screening_date,
            sr.rule_name,
            sr.rule_version,
            sr.target_count,
            sr.hit_count,
            CASE
                WHEN sr.target_count > 0 THEN CAST(sr.hit_count AS REAL) / sr.target_count
                ELSE 0.0
            END AS hit_ratio,
            COALESCE(sra.result_count, 0) AS stored_result_count,
            COALESCE(sra.hit_result_count, 0) AS stored_hit_result_count,
            sra.avg_hit_score,
            sra.latest_period_end,
            sr.created_at
        FROM screening_runs sr
        LEFT JOIN screening_result_agg sra
            ON sra.screening_run_id = sr.id;
        """
    )

def create_tables() -> None:
    conn = get_connection()
    cur = conn.cursor()

    _rebuild_screening_results_if_needed(cur)
    _rebuild_issuer_master_if_needed(cur)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS issuer_master (
        edinet_code TEXT PRIMARY KEY,
        security_code TEXT,
        company_name TEXT NOT NULL,
        market TEXT,
        industry_33 TEXT,
        industry_17 TEXT,
        is_listed INTEGER NOT NULL DEFAULT 1,
        exchange TEXT,
        listing_category_raw TEXT,
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
        accounting_standard TEXT,
        document_display_unit TEXT,
        zip_path TEXT,
        xbrl_path TEXT,
        download_status TEXT NOT NULL,
        parse_status TEXT NOT NULL,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """)
    _ensure_filings_columns(cur)

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
    CREATE TABLE IF NOT EXISTS derived_metrics (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        doc_id TEXT NOT NULL,
        edinet_code TEXT NOT NULL,
        security_code TEXT,
        metric_key TEXT NOT NULL,
        metric_base TEXT NOT NULL,
        metric_group TEXT NOT NULL,
        fiscal_year INTEGER,
        period_end TEXT,
        period_scope TEXT NOT NULL,
        period_offset INTEGER NOT NULL DEFAULT 0,
        consolidation TEXT,
        accounting_standard TEXT,
        document_display_unit TEXT,
        value_num REAL,
        value_unit TEXT NOT NULL,
        calc_status TEXT NOT NULL,
        formula_name TEXT NOT NULL,
        source_detail_json TEXT,
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
    CREATE UNIQUE INDEX IF NOT EXISTS uq_derived_metrics_doc_metric_period
    ON derived_metrics(doc_id, metric_key, period_end, consolidation)
    """)

    cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_derived_metrics_code_base_period
    ON derived_metrics(edinet_code, metric_base, period_scope, period_end)
    """)

    cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_derived_metrics_doc_id
    ON derived_metrics(doc_id)
    """)

    cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_derived_metrics_status
    ON derived_metrics(calc_status)
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

    ensure_summary_views(conn)

    conn.commit()
    conn.close()


if __name__ == "__main__":
    create_tables()
    print(f"DB created: {DB_PATH}")
