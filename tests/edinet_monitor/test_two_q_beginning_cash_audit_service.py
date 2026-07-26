from __future__ import annotations

import sqlite3
import shutil
import unittest
import uuid
from pathlib import Path

from edinet_monitor.services.two_q_beginning_cash_audit_service import (
    build_two_q_beginning_cash_audit,
    write_two_q_beginning_cash_audit,
)


ROOT_DIR = Path(__file__).resolve().parents[2]
TMP_ROOT = ROOT_DIR / "tests" / "_tmp_two_q_beginning_cash_audit_service"


class TwoQBeginningCashAuditServiceTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp_path = TMP_ROOT / uuid.uuid4().hex
        self.tmp_path.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        self.conn.executescript(
            """
            CREATE TABLE filings (
                doc_id TEXT PRIMARY KEY,
                edinet_code TEXT,
                security_code TEXT,
                form_type TEXT,
                period_end TEXT
            );
            CREATE TABLE raw_facts (
                doc_id TEXT,
                tag_name TEXT,
                context_ref TEXT,
                unit_ref TEXT,
                period_type TEXT,
                period_start TEXT,
                period_end TEXT,
                instant_date TEXT,
                consolidation TEXT,
                context_dimensions_json TEXT,
                unit_measures_json TEXT,
                value_text TEXT
            );
            CREATE TABLE derived_metrics (
                doc_id TEXT,
                metric_key TEXT,
                value_num REAL,
                calc_status TEXT
            );
            """
        )

    def tearDown(self) -> None:
        self.conn.close()
        shutil.rmtree(self.tmp_path, ignore_errors=True)

    def test_builds_manifest_with_mismatch_and_writes_artifacts(self) -> None:
        self.conn.execute(
            "INSERT INTO filings VALUES ('S100TEST', 'E00001', '12340', '043A00', '2025-01-31')"
        )
        self.conn.executemany(
            "INSERT INTO raw_facts VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    "S100TEST", "NetSales", "CurrentYTDDuration", "JPY", "duration",
                    "2024-08-01", "2025-01-31", None, "Consolidated", "", "", "100",
                ),
                (
                    "S100TEST", "CashAndCashEquivalents", "Prior1YearInstant", "JPY", "instant",
                    None, None, "2024-07-31", "Consolidated", "", "", "300",
                ),
            ],
        )
        self.conn.execute(
            "INSERT INTO derived_metrics VALUES ('S100TEST', 'BeginningCashBalanceCurrent', 200, 'ok')"
        )

        result = build_two_q_beginning_cash_audit(self.conn)

        self.assertEqual(result.summary, {
            "target_count": 1,
            "match_count": 0,
            "mismatch_count": 1,
            "missing_count": 0,
        })
        self.assertEqual(result.rows[0].prior1year_value, 300.0)
        paths = write_two_q_beginning_cash_audit(result, output_dir=self.tmp_path)
        self.assertEqual(paths.doc_ids_path.read_text(encoding="utf-8"), "S100TEST\n")
        self.assertTrue(paths.json_path.exists())
        self.assertTrue(paths.tsv_path.exists())
