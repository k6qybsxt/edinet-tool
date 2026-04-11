from __future__ import annotations

import shutil
import sqlite3
import sys
import unittest
import uuid
from datetime import date
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from edinet_monitor.cli.run_daily_pipeline import run_daily_pipeline  # noqa: E402


def create_pipeline_log_tables(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE pipeline_runs (
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
            summary_json TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE pipeline_run_chunks (
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
            summary_json TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE UNIQUE INDEX uq_pipeline_run_chunks_identity
        ON pipeline_run_chunks(run_id, chunk_key, manifest_name);
        """
    )
    conn.commit()


class RunDailyPipelineTest(unittest.TestCase):
    def test_run_daily_pipeline_writes_db_logs(self) -> None:
        temp_parent = ROOT_DIR / "tests" / "_tmp_run_daily_pipeline"
        temp_parent.mkdir(parents=True, exist_ok=True)
        db_path = temp_parent / f"case_{uuid.uuid4().hex}.db"
        self.addCleanup(lambda: db_path.unlink(missing_ok=True))

        def connection_factory() -> sqlite3.Connection:
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            return conn

        setup_conn = connection_factory()
        try:
            create_pipeline_log_tables(setup_conn)
        finally:
            setup_conn.close()

        result = run_daily_pipeline(
            target_date_text="2026-04-09",
            api_key="dummy",
            resolve_target_dates_func=lambda **_: [date(2026, 4, 9)],
            collect_func=lambda target_dates, api_key: {
                "target_dates": [target.isoformat() for target in target_dates],
                "totals": {"filing_saved_count": 1},
            },
            download_func=lambda **_: {
                "target_total": 0,
                "downloaded_total": 0,
                "error_total": 0,
            },
            extract_func=lambda **_: {
                "target_total": 0,
                "extracted_total": 0,
                "error_total": 0,
            },
            raw_func=lambda **_: {
                "target_total": 0,
                "saved_docs_total": 0,
                "saved_rows_total": 0,
                "error_total": 0,
            },
            normalized_func=lambda **_: {
                "target_total": 0,
                "saved_docs_total": 0,
                "saved_rows_total": 0,
                "error_total": 0,
            },
            derived_func=lambda **_: {
                "target_total": 0,
                "saved_docs_total": 0,
                "saved_rows_total": 0,
                "error_total": 0,
            },
            screening_func=lambda **_: {
                "screening_date": "2026-04-11",
                "rule_name": "annual_growth_quality_check",
                "rule_version": "2026-04-04-v1",
                "period_scope": "annual",
                "target_count": 10,
                "hit_count": 3,
                "screening_run_id": 1,
            },
            xbrl_retention_func=lambda conn, enabled, keep_months: {
                "status": "completed",
                "reason": "",
                "reference_month": "2026-04",
                "keep_from_month": "2026-02",
                "target_total": 0,
                "deleted_total": 0,
                "missing_file_total": 0,
                "error_total": 0,
            },
            create_tables_func=lambda: None,
            connection_factory=connection_factory,
        )

        self.assertEqual(result["run_status"], "completed")

        verify_conn = connection_factory()
        try:
            run_row = verify_conn.execute(
                "SELECT run_type, target_date, date_from, date_to, chunks FROM pipeline_runs"
            ).fetchone()
            self.assertIsNotNone(run_row)
            assert run_row is not None
            self.assertEqual(run_row["run_type"], "daily_pipeline")
            self.assertEqual(run_row["target_date"], "2026-04-09")
            self.assertEqual(run_row["date_from"], "2026-04-09")
            self.assertEqual(run_row["date_to"], "2026-04-09")
            self.assertEqual(run_row["chunks"], 8)

            chunk_rows = verify_conn.execute(
                "SELECT chunk_key, chunk_status FROM pipeline_run_chunks ORDER BY id"
            ).fetchall()
            self.assertEqual(
                [row["chunk_key"] for row in chunk_rows],
                [
                    "collect",
                    "download",
                    "extract_xbrl",
                    "save_raw_facts",
                    "save_normalized_metrics",
                    "save_derived_metrics",
                    "screening",
                    "xbrl_retention",
                ],
            )
            self.assertTrue(all(row["chunk_status"] == "completed" for row in chunk_rows))
        finally:
            verify_conn.close()


if __name__ == "__main__":
    unittest.main()
