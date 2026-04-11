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

        DROP VIEW IF EXISTS pipeline_status_summary;
        CREATE VIEW pipeline_status_summary AS
        WITH filing_counts AS (
            SELECT
                COUNT(*) AS filing_count,
                SUM(CASE WHEN download_status = 'downloaded' THEN 1 ELSE 0 END) AS downloaded_count,
                SUM(CASE WHEN xbrl_path IS NOT NULL AND xbrl_path <> '' THEN 1 ELSE 0 END) AS xbrl_path_count,
                SUM(CASE WHEN parse_status = 'raw_facts_saved' THEN 1 ELSE 0 END) AS raw_facts_saved_count,
                SUM(CASE WHEN parse_status = 'normalized_metrics_saved' THEN 1 ELSE 0 END) AS normalized_metrics_saved_count,
                SUM(CASE WHEN parse_status = 'derived_metrics_saved' THEN 1 ELSE 0 END) AS derived_metrics_saved_count,
                SUM(CASE WHEN parse_status = 'raw_facts_error' THEN 1 ELSE 0 END) AS raw_facts_error_count,
                SUM(CASE WHEN parse_status = 'normalized_metrics_error' THEN 1 ELSE 0 END) AS normalized_metrics_error_count,
                SUM(CASE WHEN parse_status = 'derived_metrics_error' THEN 1 ELSE 0 END) AS derived_metrics_error_count,
                MAX(submit_date) AS latest_submit_date,
                MAX(CASE WHEN parse_status = 'derived_metrics_saved' THEN submit_date END) AS latest_derived_saved_submit_date
            FROM filings
        ),
        issuer_counts AS (
            SELECT
                COUNT(*) AS issuer_count,
                SUM(CASE WHEN is_listed = 1 THEN 1 ELSE 0 END) AS listed_issuer_count
            FROM issuer_master
        )
        SELECT
            ic.issuer_count,
            ic.listed_issuer_count,
            fc.filing_count,
            fc.downloaded_count,
            fc.xbrl_path_count,
            fc.raw_facts_saved_count,
            fc.normalized_metrics_saved_count,
            fc.derived_metrics_saved_count,
            fc.raw_facts_error_count,
            fc.normalized_metrics_error_count,
            fc.derived_metrics_error_count,
            fc.latest_submit_date,
            fc.latest_derived_saved_submit_date
        FROM issuer_counts ic
        CROSS JOIN filing_counts fc;

        DROP VIEW IF EXISTS data_quality_summary;
        CREATE VIEW data_quality_summary AS
        WITH raw_fact_doc_counts AS (
            SELECT doc_id, COUNT(*) AS row_count
            FROM raw_facts
            GROUP BY doc_id
        ),
        normalized_doc_counts AS (
            SELECT doc_id, COUNT(*) AS row_count
            FROM normalized_metrics
            GROUP BY doc_id
        ),
        derived_doc_counts AS (
            SELECT
                doc_id,
                COUNT(*) AS row_count,
                SUM(CASE WHEN calc_status = 'ok' THEN 1 ELSE 0 END) AS ok_row_count
            FROM derived_metrics
            GROUP BY doc_id
        )
        SELECT
            'filings_missing_accounting_standard' AS check_name,
            COUNT(*) AS affected_count
        FROM filings
        WHERE accounting_standard IS NULL OR accounting_standard = ''

        UNION ALL

        SELECT
            'filings_missing_document_display_unit' AS check_name,
            COUNT(*) AS affected_count
        FROM filings
        WHERE document_display_unit IS NULL OR document_display_unit = ''

        UNION ALL

        SELECT
            'filings_missing_zip_path' AS check_name,
            COUNT(*) AS affected_count
        FROM filings
        WHERE zip_path IS NULL OR zip_path = ''

        UNION ALL

        SELECT
            'raw_facts_saved_without_raw_rows' AS check_name,
            COUNT(*) AS affected_count
        FROM filings f
        LEFT JOIN raw_fact_doc_counts rfd
            ON rfd.doc_id = f.doc_id
        WHERE f.parse_status = 'raw_facts_saved'
          AND COALESCE(rfd.row_count, 0) = 0

        UNION ALL

        SELECT
            'normalized_saved_without_normalized_rows' AS check_name,
            COUNT(*) AS affected_count
        FROM filings f
        LEFT JOIN normalized_doc_counts ndc
            ON ndc.doc_id = f.doc_id
        WHERE f.parse_status IN ('normalized_metrics_saved', 'derived_metrics_saved')
          AND COALESCE(ndc.row_count, 0) = 0

        UNION ALL

        SELECT
            'derived_saved_without_derived_rows' AS check_name,
            COUNT(*) AS affected_count
        FROM filings f
        LEFT JOIN derived_doc_counts ddc
            ON ddc.doc_id = f.doc_id
        WHERE f.parse_status = 'derived_metrics_saved'
          AND COALESCE(ddc.row_count, 0) = 0

        UNION ALL

        SELECT
            'derived_saved_without_derived_ok_rows' AS check_name,
            COUNT(*) AS affected_count
        FROM filings f
        LEFT JOIN derived_doc_counts ddc
            ON ddc.doc_id = f.doc_id
        WHERE f.parse_status = 'derived_metrics_saved'
          AND COALESCE(ddc.ok_row_count, 0) = 0;
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
    CREATE TABLE IF NOT EXISTS pipeline_runs (
        run_id TEXT PRIMARY KEY,
        run_type TEXT NOT NULL,
        started_at TEXT,
        finished_at TEXT,
        elapsed_seconds REAL,
        run_status TEXT,
        run_error TEXT,
        target_date TEXT,
        date_from TEXT,
        date_to TEXT,
        manifest_prefix TEXT,
        manifest_granularity TEXT,
        requested_download_profile TEXT,
        download_auto_peak_threshold INTEGER,
        prepare_only INTEGER,
        overwrite_manifests INTEGER,
        chunks INTEGER,
        manifest_rows_total INTEGER,
        downloaded_total INTEGER,
        existing_total INTEGER,
        error_total INTEGER,
        cooldown_total INTEGER,
        download_elapsed_seconds REAL,
        retry_wait_elapsed_seconds REAL,
        cooldown_elapsed_seconds REAL,
        effective_profile_totals_json TEXT,
        error_type_totals_json TEXT,
        raw_retention_summary_json TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS pipeline_run_chunks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        run_id TEXT NOT NULL,
        run_type TEXT NOT NULL,
        chunk_key TEXT NOT NULL,
        chunk_granularity TEXT,
        chunk_date_from TEXT,
        chunk_date_to TEXT,
        manifest_name TEXT,
        manifest_path TEXT,
        started_at TEXT,
        finished_at TEXT,
        elapsed_seconds REAL,
        chunk_status TEXT,
        chunk_error TEXT,
        manifest_rows INTEGER,
        effective_download_profile TEXT,
        downloaded_total INTEGER,
        existing_total INTEGER,
        error_total INTEGER,
        cooldown_count INTEGER,
        download_elapsed_seconds REAL,
        retry_wait_elapsed_seconds REAL,
        cooldown_elapsed_seconds REAL,
        error_type_totals_json TEXT,
        collect_summary_json TEXT,
        manifest_summary_json TEXT,
        download_summary_json TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
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

    cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_pipeline_runs_type_started_at
    ON pipeline_runs(run_type, started_at)
    """)

    cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_pipeline_run_chunks_run_id
    ON pipeline_run_chunks(run_id)
    """)

    cur.execute("""
    CREATE UNIQUE INDEX IF NOT EXISTS uq_pipeline_run_chunks_identity
    ON pipeline_run_chunks(run_id, chunk_key, manifest_name)
    """)

    ensure_summary_views(conn)

    conn.commit()
    conn.close()


if __name__ == "__main__":
    create_tables()
    print(f"DB created: {DB_PATH}")
