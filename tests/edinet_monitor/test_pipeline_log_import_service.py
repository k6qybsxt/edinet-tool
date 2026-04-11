from __future__ import annotations

import json
import sqlite3
import shutil
import sys
import unittest
import uuid
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from edinet_monitor.services.storage.pipeline_log_import_service import (  # noqa: E402
    import_zip_backfill_run_logs,
)


def create_tables(conn: sqlite3.Connection) -> None:
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
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE UNIQUE INDEX uq_pipeline_run_chunks_identity
        ON pipeline_run_chunks(run_id, chunk_key, manifest_name);
        """
    )
    conn.commit()


class PipelineLogImportServiceTest(unittest.TestCase):
    def test_import_zip_backfill_run_logs_upserts_rows(self) -> None:
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        self.addCleanup(conn.close)
        create_tables(conn)

        temp_parent = ROOT_DIR / "tests" / "_tmp_pipeline_log_import"
        temp_parent.mkdir(parents=True, exist_ok=True)
        temp_root = temp_parent / f"case_{uuid.uuid4().hex}"
        temp_root.mkdir(parents=True, exist_ok=False)
        self.addCleanup(lambda: shutil.rmtree(temp_root, ignore_errors=True))

        try:
            run_log_path = temp_root / "runs.jsonl"
            chunk_log_path = temp_root / "chunks.jsonl"

            run_log_path.write_text(
                "\n".join(
                    [
                        json.dumps(
                            {
                                "run_id": "run_001",
                                "started_at": "2026-04-11 10:00:00",
                                "finished_at": "2026-04-11 10:05:00",
                                "elapsed_seconds": 300.0,
                                "run_status": "completed",
                                "date_from": "2026-04-01",
                                "date_to": "2026-04-30",
                                "manifest_prefix": "document_manifest",
                                "manifest_granularity": "month",
                                "requested_download_profile": "auto",
                                "download_auto_peak_threshold": 100,
                                "prepare_only": False,
                                "overwrite_manifests": False,
                                "chunks": 1,
                                "manifest_rows_total": 12,
                                "downloaded_total": 12,
                                "existing_total": 0,
                                "error_total": 0,
                                "cooldown_total": 0,
                                "download_elapsed_seconds": 280.0,
                                "retry_wait_elapsed_seconds": 10.0,
                                "cooldown_elapsed_seconds": 10.0,
                                "effective_profile_totals": {"normal": 1},
                                "error_type_totals": {},
                                "raw_retention_deleted_zip_dirs": 0,
                            },
                            ensure_ascii=False,
                        )
                    ]
                ),
                encoding="utf-8",
            )
            chunk_log_path.write_text(
                "\n".join(
                    [
                        json.dumps(
                            {
                                "run_id": "run_001",
                                "chunk_key": "2026-04",
                                "chunk_granularity": "month",
                                "chunk_date_from": "2026-04-01",
                                "chunk_date_to": "2026-04-30",
                                "manifest_name": "document_manifest_2026-04",
                                "manifest_path": "D:\\EDINET_Data\\edinet_monitor\\raw\\manifests\\document_manifest_2026-04.jsonl",
                                "started_at": "2026-04-11 10:00:00",
                                "finished_at": "2026-04-11 10:05:00",
                                "elapsed_seconds": 300.0,
                                "chunk_status": "completed",
                                "manifest_rows": 12,
                                "effective_download_profile": "normal",
                                "downloaded_total": 12,
                                "existing_total": 0,
                                "error_total": 0,
                                "cooldown_count": 0,
                                "download_elapsed_seconds": 280.0,
                                "retry_wait_elapsed_seconds": 10.0,
                                "cooldown_elapsed_seconds": 10.0,
                                "error_type_totals": {},
                                "collect_summary": {"saved_count": 12},
                                "manifest_summary": {"target_total": 12},
                                "download_summary": {"downloaded_total": 12},
                            },
                            ensure_ascii=False,
                        )
                    ]
                ),
                encoding="utf-8",
            )

            summary = import_zip_backfill_run_logs(
                conn,
                run_log_path=run_log_path,
                chunk_log_path=chunk_log_path,
            )

            self.assertEqual(summary["run_rows"], 1)
            self.assertEqual(summary["chunk_rows"], 1)
            self.assertEqual(
                conn.execute("SELECT COUNT(*) FROM pipeline_runs").fetchone()[0],
                1,
            )
            self.assertEqual(
                conn.execute("SELECT COUNT(*) FROM pipeline_run_chunks").fetchone()[0],
                1,
            )

            run_log_path.write_text(
                json.dumps(
                    {
                        "run_id": "run_001",
                        "started_at": "2026-04-11 10:00:00",
                        "finished_at": "2026-04-11 10:06:00",
                        "elapsed_seconds": 360.0,
                        "run_status": "completed",
                        "date_from": "2026-04-01",
                        "date_to": "2026-04-30",
                        "manifest_prefix": "document_manifest",
                        "manifest_granularity": "month",
                        "requested_download_profile": "auto",
                        "download_auto_peak_threshold": 100,
                        "prepare_only": False,
                        "overwrite_manifests": False,
                        "chunks": 1,
                        "manifest_rows_total": 12,
                        "downloaded_total": 12,
                        "existing_total": 0,
                        "error_total": 0,
                        "cooldown_total": 0,
                        "download_elapsed_seconds": 300.0,
                        "retry_wait_elapsed_seconds": 20.0,
                        "cooldown_elapsed_seconds": 40.0,
                        "effective_profile_totals": {"peak": 1},
                        "error_type_totals": {"timeout": 1},
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )

            import_zip_backfill_run_logs(
                conn,
                run_log_path=run_log_path,
                chunk_log_path=chunk_log_path,
            )

            run_row = conn.execute(
                "SELECT elapsed_seconds, requested_download_profile, error_type_totals_json FROM pipeline_runs WHERE run_id = 'run_001'"
            ).fetchone()
            self.assertIsNotNone(run_row)
            assert run_row is not None
            self.assertEqual(run_row["elapsed_seconds"], 360.0)
            self.assertEqual(run_row["requested_download_profile"], "auto")
            self.assertIn("timeout", str(run_row["error_type_totals_json"]))
        finally:
            shutil.rmtree(temp_root, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
