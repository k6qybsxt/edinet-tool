from __future__ import annotations

import json
import shutil
import sqlite3
import sys
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch


ROOT_DIR = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT_DIR / "src"
TMP_ROOT = ROOT_DIR / "tests" / "_tmp_save_raw_facts_parallel"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from edinet_monitor.cli import save_raw_facts as cli  # noqa: E402
from edinet_monitor.db.schema import create_tables  # noqa: E402


def _raw_row(doc_id: str, tag_name: str = "NetSales") -> dict:
    return {
        "doc_id": doc_id,
        "tag_name": tag_name,
        "is_nil": 0,
        "created_at": "2026-05-30 10:00:00",
        "value_text": "100",
    }


class SaveRawFactsParallelTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp_path = TMP_ROOT / uuid.uuid4().hex
        self.tmp_path.mkdir(parents=True, exist_ok=True)
        self.db_path = self.tmp_path / "monitor.db"
        create_tables(self.db_path)

    def tearDown(self) -> None:
        shutil.rmtree(self.tmp_path, ignore_errors=True)

    def _insert_target(self, conn: sqlite3.Connection, doc_id: str, xbrl_path: str) -> None:
        conn.execute(
            """
            INSERT OR IGNORE INTO issuer_master (
                edinet_code, security_code, company_name, industry_33,
                is_listed, exchange, updated_at
            )
            VALUES ('E00001', '12340', 'A Corp', 'Chemicals', 1, 'TSE',
                    '2026-05-30 10:00:00')
            """
        )
        conn.execute(
            """
            INSERT INTO filings (
                doc_id, edinet_code, security_code, form_type, period_end,
                submit_date, download_status, parse_status, xbrl_path,
                created_at, updated_at
            )
            VALUES (?, 'E00001', '12340', '030000', '2026-03-31',
                    '2026-05-30 10:00:00', 'downloaded', 'xbrl_ready', ?,
                    '2026-05-30 10:00:00', '2026-05-30 10:00:00')
            """,
            (doc_id, xbrl_path),
        )

    def test_parse_raw_fact_job_returns_rows_and_metadata(self) -> None:
        parsed = {
            "meta": {"accounting_standard": "IFRS", "document_display_unit": "JPY_million"},
            "facts": [],
        }
        with (
            patch("edinet_monitor.cli.save_raw_facts.parse_xbrl_to_raw", return_value=parsed),
            patch(
                "edinet_monitor.cli.save_raw_facts.to_raw_fact_rows",
                return_value=[_raw_row("DOC1")],
            ),
        ):
            result = cli._parse_raw_fact_job(
                {
                    "order_index": 2,
                    "doc_id": "DOC1",
                    "form_type": "030000",
                    "xbrl_path": "C:/tmp/doc1.xbrl",
                    "xbrl_member_name": "",
                }
            )

        self.assertTrue(result["ok"])
        self.assertEqual(result["order_index"], 2)
        self.assertEqual(result["raw_rows"], [_raw_row("DOC1")])
        self.assertEqual(result["accounting_standard"], "IFRS")
        self.assertEqual(result["document_display_unit"], "JPY_million")

    def test_parse_raw_fact_job_returns_error_without_raising(self) -> None:
        with patch(
            "edinet_monitor.cli.save_raw_facts.parse_xbrl_to_raw",
            side_effect=RuntimeError("parse failed"),
        ):
            result = cli._parse_raw_fact_job(
                {
                    "order_index": 0,
                    "doc_id": "DOC1",
                    "form_type": "030000",
                    "xbrl_path": "C:/tmp/doc1.xbrl",
                    "xbrl_member_name": "",
                }
            )

        self.assertFalse(result["ok"])
        self.assertEqual(result["doc_id"], "DOC1")
        self.assertIn("parse failed", result["error"])

    def test_run_parse_jobs_chunks_and_preserves_order_with_single_worker(self) -> None:
        jobs = [
            {"order_index": index, "doc_id": f"DOC{index}", "form_type": "030000", "xbrl_path": ""}
            for index in range(6)
        ]

        def fake_parse_chunk(chunk: list[dict]) -> list[dict]:
            return [
                {
                    "ok": True,
                    "order_index": int(job["order_index"]),
                    "doc_id": str(job["doc_id"]),
                    "raw_rows": [],
                    "accounting_standard": "",
                    "document_display_unit": "",
                    "elapsed_seconds": 0.1,
                    "error": "",
                }
                for job in reversed(chunk)
            ]

        with patch("edinet_monitor.cli.save_raw_facts._parse_raw_fact_chunk", side_effect=fake_parse_chunk):
            results, chunk_count, elapsed = cli._run_parse_jobs(
                jobs,
                workers=1,
                parse_chunk_size=5,
            )

        self.assertEqual(chunk_count, 2)
        self.assertEqual([row["doc_id"] for row in results], [f"DOC{index}" for index in range(6)])
        self.assertAlmostEqual(elapsed, 0.6)

    def test_run_save_raw_facts_parallel_parent_writes_success_and_error_results(self) -> None:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            self._insert_target(conn, "DOC1", "C:/tmp/doc1.xbrl")
            self._insert_target(conn, "DOC2", "C:/tmp/doc2.xbrl")
            conn.commit()

            def fake_run_parse_jobs(jobs: list[dict], *, workers: int, parse_chunk_size: int):
                self.assertEqual(workers, 4)
                self.assertEqual(parse_chunk_size, 5)
                self.assertEqual([job["doc_id"] for job in jobs], ["DOC1", "DOC2"])
                return (
                    [
                        {
                            "ok": True,
                            "order_index": 0,
                            "doc_id": "DOC1",
                            "raw_rows": [_raw_row("DOC1")],
                            "accounting_standard": "IFRS",
                            "document_display_unit": "JPY_million",
                            "elapsed_seconds": 1.25,
                            "error": "",
                        },
                        {
                            "ok": False,
                            "order_index": 1,
                            "doc_id": "DOC2",
                            "raw_rows": [],
                            "accounting_standard": "",
                            "document_display_unit": "",
                            "elapsed_seconds": 0.5,
                            "error": "RuntimeError('broken xbrl')",
                        },
                    ],
                    1,
                    1.75,
                )

            with (
                patch("edinet_monitor.cli.save_raw_facts.create_tables"),
                patch("edinet_monitor.cli.save_raw_facts.get_connection", return_value=conn),
                patch("edinet_monitor.cli.save_raw_facts._run_parse_jobs", side_effect=fake_run_parse_jobs),
            ):
                result = cli.run_save_raw_facts(
                    batch_size=10,
                    run_all=True,
                    workers=4,
                    parse_chunk_size=5,
                )

            checker = sqlite3.connect(self.db_path)
            checker.row_factory = sqlite3.Row
            try:
                raw_count = checker.execute(
                    "SELECT COUNT(*) FROM raw_facts WHERE doc_id = 'DOC1'"
                ).fetchone()[0]
                statuses = {
                    str(row["doc_id"]): str(row["parse_status"])
                    for row in checker.execute(
                        "SELECT doc_id, parse_status FROM filings ORDER BY doc_id"
                    ).fetchall()
                }
                metadata = checker.execute(
                    """
                    SELECT accounting_standard, document_display_unit
                    FROM filings
                    WHERE doc_id = 'DOC1'
                    """
                ).fetchone()
                perf_row = checker.execute(
                    """
                    SELECT run_id, workers, summary_json
                    FROM pipeline_performance_runs
                    WHERE command_name = 'save_raw_facts'
                    ORDER BY started_at DESC
                    LIMIT 1
                    """
                ).fetchone()
                span_names = [
                    str(row["span_name"])
                    for row in checker.execute(
                        """
                        SELECT span_name
                        FROM pipeline_performance_spans
                        WHERE run_id = ?
                        ORDER BY id
                        """,
                        (perf_row["run_id"],),
                    ).fetchall()
                ] if perf_row else []
            finally:
                checker.close()

            self.assertEqual(result["target_total"], 2)
            self.assertEqual(result["saved_docs_total"], 1)
            self.assertEqual(result["saved_rows_total"], 1)
            self.assertEqual(result["error_total"], 1)
            self.assertEqual(result["workers"], 4)
            self.assertEqual(result["parse_chunk_count"], 1)
            self.assertEqual(result["worker_parse_elapsed_seconds_total"], 1.75)
            self.assertEqual(raw_count, 1)
            self.assertEqual(statuses, {"DOC1": "raw_facts_saved", "DOC2": "raw_facts_error"})
            self.assertEqual(metadata["accounting_standard"], "IFRS")
            self.assertEqual(metadata["document_display_unit"], "JPY_million")
            self.assertIsNotNone(perf_row)
            self.assertEqual(perf_row["workers"], 4)
            summary = json.loads(str(perf_row["summary_json"]))
            self.assertEqual(summary["parse_chunk_count"], 1)
            self.assertEqual(summary["worker_parse_elapsed_seconds_total"], 1.75)
            self.assertEqual(summary["fallback_doc_count"], 0)
            self.assertEqual(summary["fallback_error_count"], 0)
            self.assertIn("raw_facts_delete", span_names)
            self.assertIn("raw_facts_insert", span_names)
            self.assertIn("filing_metadata_update", span_names)
            self.assertIn("status_update", span_names)
            self.assertIn("commit", span_names)
        finally:
            try:
                conn.close()
            except sqlite3.ProgrammingError:
                pass

    def test_run_save_raw_facts_batch_writes_multiple_successes(self) -> None:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            self._insert_target(conn, "DOC1", "C:/tmp/doc1.xbrl")
            self._insert_target(conn, "DOC2", "C:/tmp/doc2.xbrl")
            conn.commit()

            def fake_run_parse_jobs(jobs: list[dict], *, workers: int, parse_chunk_size: int):
                return (
                    [
                        {
                            "ok": True,
                            "order_index": 0,
                            "doc_id": "DOC1",
                            "raw_rows": [_raw_row("DOC1", "NetSales")],
                            "accounting_standard": "Japan GAAP",
                            "document_display_unit": "JPY",
                            "elapsed_seconds": 0.1,
                            "error": "",
                        },
                        {
                            "ok": True,
                            "order_index": 1,
                            "doc_id": "DOC2",
                            "raw_rows": [_raw_row("DOC2", "OperatingIncome")],
                            "accounting_standard": "IFRS",
                            "document_display_unit": "JPY_million",
                            "elapsed_seconds": 0.2,
                            "error": "",
                        },
                    ],
                    1,
                    0.3,
                )

            with (
                patch("edinet_monitor.cli.save_raw_facts.create_tables"),
                patch("edinet_monitor.cli.save_raw_facts.get_connection", return_value=conn),
                patch("edinet_monitor.cli.save_raw_facts._run_parse_jobs", side_effect=fake_run_parse_jobs),
            ):
                result = cli.run_save_raw_facts(
                    batch_size=10,
                    run_all=True,
                    db_insert_chunk_size=1,
                    db_doc_id_chunk_size=1,
                )

            checker = sqlite3.connect(self.db_path)
            checker.row_factory = sqlite3.Row
            try:
                raw_counts = {
                    str(row["doc_id"]): int(row["count"])
                    for row in checker.execute(
                        """
                        SELECT doc_id, COUNT(*) AS count
                        FROM raw_facts
                        GROUP BY doc_id
                        ORDER BY doc_id
                        """
                    ).fetchall()
                }
                statuses = {
                    str(row["doc_id"]): str(row["parse_status"])
                    for row in checker.execute(
                        "SELECT doc_id, parse_status FROM filings ORDER BY doc_id"
                    ).fetchall()
                }
                metadata = {
                    str(row["doc_id"]): (
                        str(row["accounting_standard"] or ""),
                        str(row["document_display_unit"] or ""),
                    )
                    for row in checker.execute(
                        """
                        SELECT doc_id, accounting_standard, document_display_unit
                        FROM filings
                        ORDER BY doc_id
                        """
                    ).fetchall()
                }
            finally:
                checker.close()

            self.assertEqual(result["saved_docs_total"], 2)
            self.assertEqual(result["saved_rows_total"], 2)
            self.assertEqual(result["error_total"], 0)
            self.assertEqual(result["db_insert_chunk_size"], 1)
            self.assertEqual(result["db_doc_id_chunk_size"], 1)
            self.assertEqual(raw_counts, {"DOC1": 1, "DOC2": 1})
            self.assertEqual(statuses, {"DOC1": "raw_facts_saved", "DOC2": "raw_facts_saved"})
            self.assertEqual(metadata["DOC1"], ("Japan GAAP", "JPY"))
            self.assertEqual(metadata["DOC2"], ("IFRS", "JPY_million"))
        finally:
            try:
                conn.close()
            except sqlite3.ProgrammingError:
                pass

    def test_run_save_raw_facts_fallback_separates_success_and_error_docs(self) -> None:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            self._insert_target(conn, "DOC1", "C:/tmp/doc1.xbrl")
            self._insert_target(conn, "DOC2", "C:/tmp/doc2.xbrl")
            conn.commit()

            def fake_run_parse_jobs(jobs: list[dict], *, workers: int, parse_chunk_size: int):
                return (
                    [
                        {
                            "ok": True,
                            "order_index": 0,
                            "doc_id": "DOC1",
                            "raw_rows": [_raw_row("DOC1")],
                            "accounting_standard": "Japan GAAP",
                            "document_display_unit": "JPY",
                            "elapsed_seconds": 0.1,
                            "error": "",
                        },
                        {
                            "ok": True,
                            "order_index": 1,
                            "doc_id": "DOC2",
                            "raw_rows": [
                                {
                                    "doc_id": "DOC2",
                                    "created_at": "2026-05-30 10:00:00",
                                }
                            ],
                            "accounting_standard": "IFRS",
                            "document_display_unit": "JPY_million",
                            "elapsed_seconds": 0.2,
                            "error": "",
                        },
                    ],
                    1,
                    0.3,
                )

            with (
                patch("edinet_monitor.cli.save_raw_facts.create_tables"),
                patch("edinet_monitor.cli.save_raw_facts.get_connection", return_value=conn),
                patch("edinet_monitor.cli.save_raw_facts._run_parse_jobs", side_effect=fake_run_parse_jobs),
                patch(
                    "edinet_monitor.cli.save_raw_facts._save_raw_facts_batch",
                    side_effect=RuntimeError("batch insert failed"),
                ),
            ):
                result = cli.run_save_raw_facts(batch_size=10, run_all=True)

            checker = sqlite3.connect(self.db_path)
            checker.row_factory = sqlite3.Row
            try:
                statuses = {
                    str(row["doc_id"]): str(row["parse_status"])
                    for row in checker.execute(
                        "SELECT doc_id, parse_status FROM filings ORDER BY doc_id"
                    ).fetchall()
                }
                raw_counts = {
                    str(row["doc_id"]): int(row["count"])
                    for row in checker.execute(
                        """
                        SELECT doc_id, COUNT(*) AS count
                        FROM raw_facts
                        GROUP BY doc_id
                        ORDER BY doc_id
                        """
                    ).fetchall()
                }
                perf_row = checker.execute(
                    """
                    SELECT summary_json
                    FROM pipeline_performance_runs
                    WHERE command_name = 'save_raw_facts'
                    ORDER BY started_at DESC
                    LIMIT 1
                    """
                ).fetchone()
            finally:
                checker.close()

            self.assertEqual(result["saved_docs_total"], 1)
            self.assertEqual(result["saved_rows_total"], 1)
            self.assertEqual(result["error_total"], 1)
            self.assertEqual(result["fallback_doc_count"], 2)
            self.assertEqual(result["fallback_error_count"], 1)
            self.assertEqual(statuses, {"DOC1": "raw_facts_saved", "DOC2": "raw_facts_error"})
            self.assertEqual(raw_counts, {"DOC1": 1})
            summary = json.loads(str(perf_row["summary_json"]))
            self.assertEqual(summary["fallback_doc_count"], 2)
            self.assertEqual(summary["fallback_error_count"], 1)
        finally:
            try:
                conn.close()
            except sqlite3.ProgrammingError:
                pass


if __name__ == "__main__":
    unittest.main()
