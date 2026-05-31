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
TMP_ROOT = ROOT_DIR / "tests" / "_tmp_save_normalized_metrics_parallel"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from edinet_monitor.cli import save_normalized_metrics as cli  # noqa: E402
from edinet_monitor.db.schema import create_tables  # noqa: E402


def _raw_row(doc_id: str, tag_name: str = "NetSales") -> dict:
    return {
        "doc_id": doc_id,
        "tag_name": tag_name,
        "context_ref": "CurrentYearDuration",
        "unit_ref": "JPY",
        "period_type": "duration",
        "period_start": "2025-04-01",
        "period_end": "2026-03-31",
        "instant_date": "",
        "consolidation": "Consolidated",
        "decimals": "0",
        "is_nil": 0,
        "context_dimensions_json": "{}",
        "unit_measures_json": "{}",
        "value_text": "100",
    }


def _normalized_row(doc_id: str) -> dict:
    return {
        "doc_id": doc_id,
        "edinet_code": "E00001",
        "security_code": "12340",
        "metric_key": "NetSalesCurrent",
        "fiscal_year": 2026,
        "period_end": "2026-03-31",
        "value_num": 100.0,
        "source_tag": "NetSales",
        "consolidation": "Consolidated",
        "rule_version": "test",
    }


class _ImmediateFuture:
    def __init__(self, result):
        self._result = result

    def result(self):
        return self._result


class _ImmediateExecutor:
    def __init__(self, *, max_workers: int):
        self.max_workers = max_workers

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def submit(self, func, *args):
        return _ImmediateFuture(func(*args))

    def shutdown(self):
        return None


class SaveNormalizedMetricsParallelTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp_path = TMP_ROOT / uuid.uuid4().hex
        self.tmp_path.mkdir(parents=True, exist_ok=True)
        self.db_path = self.tmp_path / "monitor.db"
        create_tables(self.db_path)

    def tearDown(self) -> None:
        shutil.rmtree(self.tmp_path, ignore_errors=True)

    def _insert_target(self, conn: sqlite3.Connection, doc_id: str) -> None:
        conn.execute(
            """
            INSERT OR IGNORE INTO issuer_master (
                edinet_code, security_code, company_name, industry_33,
                is_listed, exchange, updated_at
            )
            VALUES ('E00001', '12340', 'A Corp', 'Chemicals', 1, 'TSE',
                    '2026-05-31 10:00:00')
            """
        )
        conn.execute(
            """
            INSERT INTO filings (
                doc_id, edinet_code, security_code, form_type, period_end,
                submit_date, download_status, parse_status, xbrl_path, zip_path,
                created_at, updated_at
            )
            VALUES (?, 'E00001', '12340', '030000', '2026-03-31',
                    '2026-05-31 10:00:00', 'downloaded', 'raw_facts_saved',
                    'C:/tmp/test.xbrl', 'C:/tmp/test.zip',
                    '2026-05-31 10:00:00', '2026-05-31 10:00:00')
            """,
            (doc_id,),
        )

    def test_fetch_raw_fact_rows_by_doc_ids_groups_rows(self) -> None:
        conn = sqlite3.connect(self.db_path)
        try:
            conn.executemany(
                """
                INSERT INTO raw_facts (
                    doc_id, tag_name, context_ref, is_nil, created_at
                )
                VALUES (?, ?, 'CurrentYearDuration', 0, '2026-05-31 10:00:00')
                """,
                [
                    ("DOC1", "NetSales"),
                    ("DOC1", "OperatingIncome"),
                    ("DOC2", "ProfitLoss"),
                ],
            )
            conn.commit()

            grouped = cli.fetch_raw_fact_rows_by_doc_ids(conn, ["DOC2", "DOC1", "DOC3"])

            self.assertEqual(set(grouped), {"DOC1", "DOC2", "DOC3"})
            self.assertEqual([row["tag_name"] for row in grouped["DOC1"]], ["NetSales", "OperatingIncome"])
            self.assertEqual([row["tag_name"] for row in grouped["DOC2"]], ["ProfitLoss"])
            self.assertEqual(grouped["DOC3"], [])
            self.assertEqual(cli.fetch_raw_fact_rows(conn, "DOC2"), grouped["DOC2"])
        finally:
            conn.close()

    def test_normalize_metric_job_returns_rows_without_db_access(self) -> None:
        job = {
            "order_index": 2,
            "filing": {
                "doc_id": "DOC1",
                "edinet_code": "E00001",
                "security_code": "12340",
                "industry_33": "Chemicals",
                "period_end": "2026-03-31",
                "form_type": "030000",
                "xbrl_path": "C:/tmp/test.xbrl",
                "zip_path": "C:/tmp/test.zip",
            },
            "raw_rows": [_raw_row("DOC1")],
            "enable_period_fallback": True,
            "enforce_candidate_validation": True,
        }
        with (
            patch("edinet_monitor.cli.save_normalized_metrics.get_connection") as get_connection,
            patch(
                "edinet_monitor.cli.save_normalized_metrics.normalize_raw_fact_rows",
                return_value=[_normalized_row("DOC1")],
            ) as normalize,
        ):
            result = cli._normalize_metric_job(job)

        self.assertTrue(result["ok"])
        self.assertEqual(result["order_index"], 2)
        self.assertEqual(result["normalized_rows"], [_normalized_row("DOC1")])
        get_connection.assert_not_called()
        normalize.assert_called_once_with(
            [_raw_row("DOC1")],
            edinet_code="E00001",
            security_code="12340",
            industry_33="Chemicals",
            xbrl_path="C:/tmp/test.xbrl",
            zip_path="C:/tmp/test.zip",
            filing_period_end="2026-03-31",
            form_type="030000",
            enable_period_fallback=True,
            enforce_candidate_validation=True,
        )

    def test_normalize_metric_job_returns_error_without_raising(self) -> None:
        with patch(
            "edinet_monitor.cli.save_normalized_metrics.normalize_raw_fact_rows",
            side_effect=RuntimeError("normalization failed"),
        ):
            result = cli._normalize_metric_job(
                {
                    "order_index": 0,
                    "filing": {"doc_id": "DOC1"},
                    "raw_rows": [_raw_row("DOC1")],
                }
            )

        self.assertFalse(result["ok"])
        self.assertEqual(result["doc_id"], "DOC1")
        self.assertIn("normalization failed", result["error"])

    def test_run_normalize_jobs_parallel_matches_serial(self) -> None:
        jobs = [
            {
                "order_index": index,
                "filing": {"doc_id": f"DOC{index}"},
                "raw_rows": [],
            }
            for index in range(6)
        ]

        def fake_normalize_chunk(chunk: list[dict]) -> list[dict]:
            return [
                {
                    "ok": True,
                    "order_index": int(job["order_index"]),
                    "doc_id": str(job["filing"]["doc_id"]),
                    "raw_row_count": 0,
                    "normalized_rows": [],
                    "elapsed_seconds": 0.1,
                    "error": "",
                }
                for job in reversed(chunk)
            ]

        with patch(
            "edinet_monitor.cli.save_normalized_metrics._normalize_metric_chunk",
            side_effect=fake_normalize_chunk,
        ):
            serial, serial_chunks, serial_elapsed = cli._run_normalize_jobs(
                jobs,
                workers=1,
                normalize_chunk_size=2,
            )

        with (
            patch(
                "edinet_monitor.cli.save_normalized_metrics._normalize_metric_chunk",
                side_effect=fake_normalize_chunk,
            ),
            patch(
                "edinet_monitor.cli.save_normalized_metrics.ProcessPoolExecutor",
                _ImmediateExecutor,
            ),
            patch(
                "edinet_monitor.cli.save_normalized_metrics.as_completed",
                side_effect=lambda futures: list(futures),
            ),
        ):
            parallel, parallel_chunks, parallel_elapsed = cli._run_normalize_jobs(
                jobs,
                workers=4,
                normalize_chunk_size=2,
            )

        self.assertEqual(serial, parallel)
        self.assertEqual(serial_chunks, 3)
        self.assertEqual(parallel_chunks, 3)
        self.assertAlmostEqual(serial_elapsed, 0.6)
        self.assertAlmostEqual(parallel_elapsed, 0.6)
        self.assertEqual([row["doc_id"] for row in parallel], [f"DOC{index}" for index in range(6)])

    def test_run_save_normalized_metrics_parallel_parent_writes_results_and_performance_log(self) -> None:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            self._insert_target(conn, "DOC1")
            self._insert_target(conn, "DOC2")
            conn.commit()

            def fake_fetch_bulk(_: sqlite3.Connection, doc_ids: list[str]):
                self.assertEqual(doc_ids, ["DOC1", "DOC2"])
                return {
                    "DOC1": [_raw_row("DOC1")],
                    "DOC2": [_raw_row("DOC2")],
                }

            def fake_run_jobs(
                jobs: list[dict],
                *,
                workers: int,
                normalize_chunk_size: int,
                executor,
            ):
                self.assertEqual(workers, 4)
                self.assertEqual(normalize_chunk_size, 5)
                self.assertIsNotNone(executor)
                self.assertEqual([job["filing"]["doc_id"] for job in jobs], ["DOC1", "DOC2"])
                return (
                    [
                        {
                            "ok": True,
                            "order_index": 0,
                            "doc_id": "DOC1",
                            "raw_row_count": 1,
                            "normalized_rows": [_normalized_row("DOC1")],
                            "elapsed_seconds": 1.25,
                            "error": "",
                        },
                        {
                            "ok": False,
                            "order_index": 1,
                            "doc_id": "DOC2",
                            "raw_row_count": 1,
                            "normalized_rows": [],
                            "elapsed_seconds": 0.5,
                            "error": "RuntimeError('broken normalization')",
                        },
                    ],
                    1,
                    1.75,
                )

            with (
                patch("edinet_monitor.cli.save_normalized_metrics.create_tables"),
                patch("edinet_monitor.cli.save_normalized_metrics.get_connection", return_value=conn),
                patch(
                    "edinet_monitor.cli.save_normalized_metrics.fetch_raw_fact_rows_by_doc_ids",
                    side_effect=fake_fetch_bulk,
                ),
                patch(
                    "edinet_monitor.cli.save_normalized_metrics._run_normalize_jobs",
                    side_effect=fake_run_jobs,
                ),
                patch(
                    "edinet_monitor.cli.save_normalized_metrics.ProcessPoolExecutor",
                    _ImmediateExecutor,
                ),
            ):
                result = cli.run_save_normalized_metrics(
                    batch_size=10,
                    workers=4,
                    normalize_chunk_size=5,
                )

            checker = sqlite3.connect(self.db_path)
            checker.row_factory = sqlite3.Row
            try:
                metric_count = checker.execute(
                    "SELECT COUNT(*) FROM normalized_metrics WHERE doc_id = 'DOC1'"
                ).fetchone()[0]
                statuses = {
                    str(row["doc_id"]): str(row["parse_status"])
                    for row in checker.execute(
                        "SELECT doc_id, parse_status FROM filings ORDER BY doc_id"
                    ).fetchall()
                }
                perf_row = checker.execute(
                    """
                    SELECT workers, summary_json
                    FROM pipeline_performance_runs
                    WHERE command_name = 'save_normalized_metrics'
                    ORDER BY started_at DESC
                    LIMIT 1
                    """
                ).fetchone()
            finally:
                checker.close()

            self.assertEqual(result["target_total"], 2)
            self.assertEqual(result["saved_docs_total"], 1)
            self.assertEqual(result["saved_rows_total"], 1)
            self.assertEqual(result["error_total"], 1)
            self.assertEqual(result["workers"], 4)
            self.assertEqual(result["normalize_window_count"], 1)
            self.assertEqual(result["normalize_chunk_count"], 1)
            self.assertEqual(result["raw_facts_row_total"], 2)
            self.assertEqual(result["worker_normalize_elapsed_seconds_total"], 1.75)
            self.assertEqual(metric_count, 1)
            self.assertEqual(
                statuses,
                {"DOC1": "normalized_metrics_saved", "DOC2": "normalized_metrics_error"},
            )
            self.assertIsNotNone(perf_row)
            self.assertEqual(perf_row["workers"], 4)
            summary = json.loads(str(perf_row["summary_json"]))
            self.assertEqual(summary["normalize_window_count"], 1)
            self.assertEqual(summary["normalize_chunk_count"], 1)
            self.assertEqual(summary["raw_facts_row_total"], 2)
            self.assertEqual(summary["worker_normalize_elapsed_seconds_total"], 1.75)
        finally:
            try:
                conn.close()
            except sqlite3.ProgrammingError:
                pass

    def test_run_save_normalized_metrics_reuses_parallel_executor_across_windows(self) -> None:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            for doc_id in ("DOC1", "DOC2", "DOC3"):
                self._insert_target(conn, doc_id)
            conn.commit()
            executors = []

            def fake_fetch_bulk(_: sqlite3.Connection, doc_ids: list[str]):
                return {doc_id: [_raw_row(doc_id)] for doc_id in doc_ids}

            def fake_run_jobs(
                jobs: list[dict],
                *,
                workers: int,
                normalize_chunk_size: int,
                executor,
            ):
                self.assertEqual(workers, 2)
                self.assertEqual(normalize_chunk_size, 1)
                executors.append(executor)
                return (
                    [
                        {
                            "ok": True,
                            "order_index": int(job["order_index"]),
                            "doc_id": str(job["filing"]["doc_id"]),
                            "raw_row_count": 1,
                            "normalized_rows": [_normalized_row(str(job["filing"]["doc_id"]))],
                            "elapsed_seconds": 0.1,
                            "error": "",
                        }
                        for job in jobs
                    ],
                    len(jobs),
                    round(len(jobs) * 0.1, 6),
                )

            with (
                patch("edinet_monitor.cli.save_normalized_metrics.create_tables"),
                patch("edinet_monitor.cli.save_normalized_metrics.get_connection", return_value=conn),
                patch(
                    "edinet_monitor.cli.save_normalized_metrics.fetch_raw_fact_rows_by_doc_ids",
                    side_effect=fake_fetch_bulk,
                ),
                patch(
                    "edinet_monitor.cli.save_normalized_metrics._run_normalize_jobs",
                    side_effect=fake_run_jobs,
                ),
                patch(
                    "edinet_monitor.cli.save_normalized_metrics.ProcessPoolExecutor",
                    side_effect=_ImmediateExecutor,
                ) as pool_factory,
            ):
                result = cli.run_save_normalized_metrics(
                    batch_size=10,
                    workers=2,
                    normalize_chunk_size=1,
                )

            self.assertEqual(result["saved_docs_total"], 3)
            self.assertEqual(result["normalize_window_count"], 2)
            pool_factory.assert_called_once_with(max_workers=2)
            self.assertEqual(len(executors), 2)
            self.assertIs(executors[0], executors[1])
        finally:
            try:
                conn.close()
            except sqlite3.ProgrammingError:
                pass

    def test_cli_defaults_to_serial(self) -> None:
        args = cli.build_arg_parser().parse_args([])
        self.assertEqual(args.workers, 1)
        self.assertEqual(args.normalize_chunk_size, 5)


if __name__ == "__main__":
    unittest.main()
