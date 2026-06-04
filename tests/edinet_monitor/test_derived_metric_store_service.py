from __future__ import annotations

import json
import shutil
import sqlite3
import sys
import unittest
import uuid
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT_DIR / "src"
TMP_ROOT = ROOT_DIR / "tests" / "_tmp_derived_metric_store_service"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from edinet_monitor.db.schema import create_tables  # noqa: E402
from edinet_monitor.services.derived_metrics.derived_metric_store_service import (  # noqa: E402
    DerivedMetricInserter,
    delete_derived_metrics_by_doc_ids,
)


def _derived_row(doc_id: str, metric_key: str, value_num: float) -> dict:
    return {
        "doc_id": doc_id,
        "edinet_code": "E00001",
        "security_code": "12340",
        "metric_key": metric_key,
        "metric_base": metric_key.replace("Current", ""),
        "metric_group": "growth",
        "fiscal_year": 2026,
        "period_end": "2026-03-31",
        "period_scope": "current",
        "period_key": "actual:annual",
        "quarter_type": "",
        "period_offset": 0,
        "consolidation": "Consolidated",
        "accounting_standard": "Japan GAAP",
        "document_display_unit": "unit",
        "value_num": value_num,
        "value_unit": "ratio",
        "calc_status": "ok",
        "formula_name": "test_formula",
        "source_detail_json": {"inputs": {metric_key: value_num}},
        "rule_version": "test",
    }


class DerivedMetricStoreServiceTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp_path = TMP_ROOT / uuid.uuid4().hex
        self.tmp_path.mkdir(parents=True, exist_ok=True)
        self.db_path = self.tmp_path / "monitor.db"
        create_tables(self.db_path)

    def tearDown(self) -> None:
        shutil.rmtree(self.tmp_path, ignore_errors=True)

    def test_derived_metric_inserter_chunks_rows_and_preserves_metadata(self) -> None:
        conn = sqlite3.connect(self.db_path)
        try:
            inserter = DerivedMetricInserter(conn)
            rows = [
                _derived_row("DOC1", "MetricACurrent", 1.0),
                _derived_row("DOC1", "MetricBCurrent", 2.0),
                _derived_row("DOC2", "MetricACurrent", 3.0),
            ]

            saved_count = inserter.insert_many(rows, chunk_size=2)
            conn.commit()

            stored_rows = conn.execute(
                """
                SELECT doc_id, metric_key, period_key, quarter_type, source_detail_json
                FROM derived_metrics
                ORDER BY doc_id, metric_key
                """
            ).fetchall()

            self.assertEqual(saved_count, 3)
            self.assertEqual(len(stored_rows), 3)
            self.assertEqual(stored_rows[0][2], "actual:annual")
            self.assertEqual(stored_rows[0][3], "")
            self.assertEqual(json.loads(stored_rows[0][4])["inputs"]["MetricACurrent"], 1.0)
        finally:
            conn.close()

    def test_delete_derived_metrics_by_doc_ids_chunks_targets_only(self) -> None:
        conn = sqlite3.connect(self.db_path)
        try:
            inserter = DerivedMetricInserter(conn)
            inserter.insert_many(
                [
                    _derived_row("DOC1", "MetricACurrent", 1.0),
                    _derived_row("DOC2", "MetricACurrent", 2.0),
                    _derived_row("DOC3", "MetricACurrent", 3.0),
                    _derived_row("DOC4", "MetricACurrent", 4.0),
                ],
                chunk_size=10,
            )
            conn.commit()

            deleted_count = delete_derived_metrics_by_doc_ids(
                conn,
                ["DOC1", "DOC2", "DOC3"],
                chunk_size=2,
            )
            remaining_doc_ids = [
                str(row[0])
                for row in conn.execute(
                    "SELECT doc_id FROM derived_metrics ORDER BY doc_id"
                ).fetchall()
            ]

            self.assertEqual(deleted_count, 3)
            self.assertEqual(remaining_doc_ids, ["DOC4"])
        finally:
            conn.close()


if __name__ == "__main__":
    unittest.main()
