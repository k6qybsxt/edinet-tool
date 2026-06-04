from __future__ import annotations

import shutil
import sqlite3
import sys
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch


ROOT_DIR = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT_DIR / "src"
TMP_ROOT = ROOT_DIR / "tests" / "_tmp_db_write_transaction_service"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from edinet_monitor.cli.save_normalized_metrics import run_save_normalized_metrics  # noqa: E402
from edinet_monitor.cli.save_derived_metrics import run_save_derived_metrics  # noqa: E402
from edinet_monitor.db.schema import create_tables  # noqa: E402
from edinet_monitor.services.collector.download_queue_service import mark_raw_facts_saved  # noqa: E402
from edinet_monitor.services.parser.raw_fact_store_service import insert_raw_facts  # noqa: E402


class DbWriteTransactionServiceTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp_path = TMP_ROOT / uuid.uuid4().hex
        self.tmp_path.mkdir(parents=True, exist_ok=True)
        self.db_path = self.tmp_path / "monitor.db"
        create_tables(self.db_path)

    def tearDown(self) -> None:
        shutil.rmtree(self.tmp_path, ignore_errors=True)

    def test_store_commit_false_waits_for_caller_commit(self) -> None:
        writer = sqlite3.connect(self.db_path)
        reader = sqlite3.connect(self.db_path)
        try:
            insert_raw_facts(
                writer,
                [
                    {
                        "doc_id": "DOC1",
                        "tag_name": "NetSales",
                        "is_nil": 0,
                        "created_at": "2026-05-30 10:00:00",
                    }
                ],
                commit=False,
            )

            before_commit = reader.execute(
                "SELECT COUNT(*) FROM raw_facts WHERE doc_id = 'DOC1'"
            ).fetchone()[0]
            writer.commit()
            after_commit = reader.execute(
                "SELECT COUNT(*) FROM raw_facts WHERE doc_id = 'DOC1'"
            ).fetchone()[0]

            self.assertEqual(before_commit, 0)
            self.assertEqual(after_commit, 1)
        finally:
            writer.close()
            reader.close()

    def test_status_commit_false_waits_for_caller_commit(self) -> None:
        writer = sqlite3.connect(self.db_path)
        reader = sqlite3.connect(self.db_path)
        try:
            writer.execute(
                """
                INSERT INTO filings (
                    doc_id, edinet_code, form_type, download_status,
                    parse_status, created_at, updated_at
                )
                VALUES ('DOC1', 'E00001', '030000', 'downloaded', 'xbrl_ready',
                        '2026-05-30 10:00:00', '2026-05-30 10:00:00')
                """
            )
            writer.commit()

            mark_raw_facts_saved(writer, "DOC1", commit=False)
            before_commit = reader.execute(
                "SELECT parse_status FROM filings WHERE doc_id = 'DOC1'"
            ).fetchone()[0]
            writer.commit()
            after_commit = reader.execute(
                "SELECT parse_status FROM filings WHERE doc_id = 'DOC1'"
            ).fetchone()[0]

            self.assertEqual(before_commit, "xbrl_ready")
            self.assertEqual(after_commit, "raw_facts_saved")
        finally:
            writer.close()
            reader.close()

    def test_save_normalized_metrics_rolls_back_doc_save_when_status_update_fails(self) -> None:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            conn.execute(
                """
                INSERT INTO issuer_master (
                    edinet_code, security_code, company_name, industry_33,
                    is_listed, exchange, updated_at
                )
                VALUES ('E00001', '12340', 'A社', '化学', 1, 'TSE',
                        '2026-05-30 10:00:00')
                """
            )
            conn.execute(
                """
                INSERT INTO filings (
                    doc_id, edinet_code, security_code, form_type, period_end,
                    submit_date, download_status, parse_status, created_at, updated_at
                )
                VALUES ('DOC1', 'E00001', '12340', '030000', '2026-03-31',
                        '2026-05-30 10:00:00', 'downloaded', 'raw_facts_saved',
                        '2026-05-30 10:00:00', '2026-05-30 10:00:00')
                """
            )
            conn.commit()
            filing_row = conn.execute(
                """
                SELECT
                    'DOC1' AS doc_id,
                    'E00001' AS edinet_code,
                    '12340' AS security_code,
                    '化学' AS industry_33,
                    '2026-03-31' AS period_end,
                    '030000' AS form_type,
                    '' AS xbrl_path,
                    '' AS zip_path
                """
            ).fetchone()
            assert filing_row is not None

            with (
                patch("edinet_monitor.cli.save_normalized_metrics.create_tables"),
                patch("edinet_monitor.cli.save_normalized_metrics.get_connection", return_value=conn),
                patch(
                    "edinet_monitor.cli.save_normalized_metrics.fetch_raw_facts_saved_filings",
                    side_effect=[[filing_row], []],
                ),
                patch(
                    "edinet_monitor.cli.save_normalized_metrics.fetch_raw_fact_rows_by_doc_ids",
                    return_value={"DOC1": [{"doc_id": "DOC1"}]},
                ),
                patch(
                    "edinet_monitor.cli.save_normalized_metrics.normalize_raw_fact_rows",
                    return_value=[
                        {
                            "doc_id": "DOC1",
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
                    ],
                ),
                patch(
                    "edinet_monitor.cli.save_normalized_metrics.mark_normalized_metrics_saved",
                    side_effect=RuntimeError("status update failed"),
                ),
            ):
                result = run_save_normalized_metrics(batch_size=10)

            checker = sqlite3.connect(self.db_path)
            try:
                metric_count = checker.execute(
                    "SELECT COUNT(*) FROM normalized_metrics WHERE doc_id = 'DOC1'"
                ).fetchone()[0]
                parse_status = checker.execute(
                    "SELECT parse_status FROM filings WHERE doc_id = 'DOC1'"
                ).fetchone()[0]
            finally:
                checker.close()

            self.assertEqual(result["saved_docs_total"], 0)
            self.assertEqual(result["error_total"], 1)
            self.assertEqual(metric_count, 0)
            self.assertEqual(parse_status, "normalized_metrics_error")
        finally:
            conn.close()

    def test_save_derived_metrics_uses_bulk_inputs_and_falls_back_when_batch_status_update_fails(self) -> None:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            conn.execute(
                """
                INSERT INTO issuer_master (
                    edinet_code, security_code, company_name, industry_33,
                    is_listed, exchange, updated_at
                )
                VALUES ('E00001', '12340', 'A遉ｾ', '蛹門ｭｦ', 1, 'TSE',
                        '2026-05-30 10:00:00')
                """
            )
            conn.execute(
                """
                INSERT INTO filings (
                    doc_id, edinet_code, security_code, form_type, period_end,
                    submit_date, accounting_standard, document_display_unit,
                    download_status, parse_status, created_at, updated_at
                )
                VALUES ('DOC1', 'E00001', '12340', '030000', '2026-03-31',
                        '2026-05-30 10:00:00', 'Japan GAAP', '千円',
                        'downloaded', 'normalized_metrics_saved',
                        '2026-05-30 10:00:00', '2026-05-30 10:00:00')
                """
            )
            conn.commit()
            filing_row = conn.execute(
                """
                SELECT
                    'DOC1' AS doc_id,
                    'E00001' AS edinet_code,
                    '12340' AS security_code,
                    '蛹門ｭｦ' AS industry_33,
                    '2026-03-31' AS period_end,
                    '030000' AS form_type,
                    'Japan GAAP' AS accounting_standard,
                    '千円' AS document_display_unit,
                    '' AS xbrl_path,
                    '' AS zip_path
                """
            ).fetchone()
            assert filing_row is not None
            normalized_row = {
                "doc_id": "DOC1",
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
            derived_row = {
                "doc_id": "DOC1",
                "edinet_code": "E00001",
                "security_code": "12340",
                "metric_key": "NetSalesGrowthRateCurrent",
                "metric_base": "NetSalesGrowthRate",
                "metric_group": "growth",
                "fiscal_year": 2026,
                "period_end": "2026-03-31",
                "period_scope": "current",
                "period_offset": 0,
                "consolidation": "Consolidated",
                "accounting_standard": "Japan GAAP",
                "document_display_unit": "千円",
                "value_num": 0.1,
                "value_unit": "ratio",
                "calc_status": "ok",
                "formula_name": "test",
                "source_detail_json": {},
                "rule_version": "test",
            }

            with (
                patch("edinet_monitor.cli.save_derived_metrics.create_tables"),
                patch("edinet_monitor.cli.save_derived_metrics.get_connection", return_value=conn),
                patch(
                    "edinet_monitor.cli.save_derived_metrics.fetch_derived_metrics_target_filings",
                    side_effect=[[filing_row], []],
                ),
                patch(
                    "edinet_monitor.cli.save_derived_metrics.fetch_normalized_metric_rows_by_doc_ids",
                    return_value={"DOC1": [normalized_row]},
                ) as fetch_normalized_bulk,
                patch(
                    "edinet_monitor.cli.save_derived_metrics.fetch_historical_growth_values_bulk",
                    return_value={"DOC1": {"NetSales": {1: {"value_num": 90.0}}}},
                ) as fetch_historical_bulk,
                patch(
                    "edinet_monitor.cli.save_derived_metrics.fetch_half_progress_annual_values_bulk",
                    return_value={},
                ) as fetch_half_bulk,
                patch(
                    "edinet_monitor.cli.save_derived_metrics.calculate_derived_metrics",
                    return_value=[derived_row],
                ),
                patch(
                    "edinet_monitor.cli.save_derived_metrics.mark_derived_metrics_saved_many",
                    side_effect=RuntimeError("status update failed"),
                ),
            ):
                result = run_save_derived_metrics(batch_size=10, rule_version="test")

            checker = sqlite3.connect(self.db_path)
            try:
                metric_count = checker.execute(
                    "SELECT COUNT(*) FROM derived_metrics WHERE doc_id = 'DOC1'"
                ).fetchone()[0]
                parse_status = checker.execute(
                    "SELECT parse_status FROM filings WHERE doc_id = 'DOC1'"
                ).fetchone()[0]
            finally:
                checker.close()

            fetch_normalized_bulk.assert_called_once()
            fetch_historical_bulk.assert_called_once()
            fetch_half_bulk.assert_called_once()
            self.assertEqual(result["saved_docs_total"], 1)
            self.assertEqual(result["error_total"], 0)
            self.assertEqual(result["normalized_input_rows"], 1)
            self.assertEqual(result["fallback_doc_count"], 1)
            self.assertEqual(metric_count, 1)
            self.assertEqual(parse_status, "derived_metrics_saved")
        finally:
            conn.close()

    def test_save_derived_metrics_batch_write_records_performance_spans(self) -> None:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            conn.execute(
                """
                INSERT INTO issuer_master (
                    edinet_code, security_code, company_name, industry_33,
                    is_listed, exchange, updated_at
                )
                VALUES ('E00001', '12340', 'Company A', 'Chemicals', 1, 'TSE',
                        '2026-05-30 10:00:00')
                """
            )
            conn.execute(
                """
                INSERT INTO filings (
                    doc_id, edinet_code, security_code, form_type, period_end,
                    submit_date, accounting_standard, document_display_unit,
                    download_status, parse_status, created_at, updated_at
                )
                VALUES ('DOC1', 'E00001', '12340', '030000', '2026-03-31',
                        '2026-05-30 10:00:00', 'Japan GAAP', 'unit',
                        'downloaded', 'normalized_metrics_saved',
                        '2026-05-30 10:00:00', '2026-05-30 10:00:00')
                """
            )
            conn.commit()
            filing_row = conn.execute(
                """
                SELECT
                    'DOC1' AS doc_id,
                    'E00001' AS edinet_code,
                    '12340' AS security_code,
                    'Chemicals' AS industry_33,
                    '2026-03-31' AS period_end,
                    '030000' AS form_type,
                    'Japan GAAP' AS accounting_standard,
                    'unit' AS document_display_unit,
                    '' AS xbrl_path,
                    '' AS zip_path
                """
            ).fetchone()
            assert filing_row is not None
            normalized_row = {
                "doc_id": "DOC1",
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
            derived_row = {
                "doc_id": "DOC1",
                "edinet_code": "E00001",
                "security_code": "12340",
                "metric_key": "NetSalesGrowthRateCurrent",
                "metric_base": "NetSalesGrowthRate",
                "metric_group": "growth",
                "fiscal_year": 2026,
                "period_end": "2026-03-31",
                "period_scope": "current",
                "period_offset": 0,
                "consolidation": "Consolidated",
                "accounting_standard": "Japan GAAP",
                "document_display_unit": "unit",
                "value_num": 0.1,
                "value_unit": "ratio",
                "calc_status": "ok",
                "formula_name": "test",
                "source_detail_json": {},
                "rule_version": "test",
            }

            with (
                patch("edinet_monitor.cli.save_derived_metrics.create_tables"),
                patch("edinet_monitor.cli.save_derived_metrics.get_connection", return_value=conn),
                patch(
                    "edinet_monitor.cli.save_derived_metrics.fetch_derived_metrics_target_filings",
                    return_value=[filing_row],
                ),
                patch(
                    "edinet_monitor.cli.save_derived_metrics.fetch_normalized_metric_rows_by_doc_ids",
                    return_value={"DOC1": [normalized_row]},
                ),
                patch(
                    "edinet_monitor.cli.save_derived_metrics.fetch_historical_growth_values_bulk",
                    return_value={},
                ),
                patch(
                    "edinet_monitor.cli.save_derived_metrics.fetch_half_progress_annual_values_bulk",
                    return_value={},
                ),
                patch(
                    "edinet_monitor.cli.save_derived_metrics.calculate_derived_metrics",
                    return_value=[derived_row],
                ),
            ):
                result = run_save_derived_metrics(
                    batch_size=10,
                    rule_version="test",
                    db_insert_chunk_size=1,
                    db_doc_id_chunk_size=1,
                )

            checker = sqlite3.connect(self.db_path)
            checker.row_factory = sqlite3.Row
            try:
                metric_count = checker.execute(
                    "SELECT COUNT(*) FROM derived_metrics WHERE doc_id = 'DOC1'"
                ).fetchone()[0]
                parse_status = checker.execute(
                    "SELECT parse_status FROM filings WHERE doc_id = 'DOC1'"
                ).fetchone()[0]
                span_names = [
                    str(row["span_name"])
                    for row in checker.execute(
                        """
                        SELECT span_name
                        FROM pipeline_performance_spans
                        WHERE run_id = (
                            SELECT run_id
                            FROM pipeline_performance_runs
                            WHERE command_name = 'save_derived_metrics'
                            ORDER BY started_at DESC, id DESC
                            LIMIT 1
                        )
                        """
                    ).fetchall()
                ]
            finally:
                checker.close()

            self.assertEqual(result["saved_docs_total"], 1)
            self.assertEqual(result["error_total"], 0)
            self.assertEqual(metric_count, 1)
            self.assertEqual(parse_status, "derived_metrics_saved")
            self.assertIn("derived_metrics_delete", span_names)
            self.assertIn("derived_metrics_insert", span_names)
            self.assertIn("filing_metadata_update", span_names)
            self.assertIn("status_update", span_names)
            self.assertIn("commit", span_names)
        finally:
            conn.close()


if __name__ == "__main__":
    unittest.main()
