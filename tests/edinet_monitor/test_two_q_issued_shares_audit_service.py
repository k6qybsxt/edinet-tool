from __future__ import annotations

import shutil
import sqlite3
import unittest
import uuid
from pathlib import Path

from edinet_monitor.services.two_q_issued_shares_audit_service import (
    build_two_q_issued_shares_audit,
    write_two_q_issued_shares_audit,
)


ROOT_DIR = Path(__file__).resolve().parents[2]
TMP_ROOT = ROOT_DIR / "tests" / "_tmp_two_q_issued_shares_audit_service"


class TwoQIssuedSharesAuditServiceTest(unittest.TestCase):
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
            CREATE TABLE issuer_master (
                edinet_code TEXT PRIMARY KEY,
                security_code TEXT,
                is_listed INTEGER,
                exchange TEXT
            );
            CREATE TABLE raw_facts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
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
            CREATE TABLE normalized_metrics (
                doc_id TEXT,
                metric_key TEXT,
                value_num REAL,
                source_tag TEXT
            );
            CREATE TABLE derived_metrics (
                doc_id TEXT,
                metric_key TEXT,
                value_num REAL,
                calc_status TEXT
            );
            """
        )
        self.conn.executemany(
            "INSERT INTO issuer_master VALUES (?, ?, 1, 'TSE')",
            [("E00001", "11110"), ("E00002", "22220")],
        )
        self.conn.executemany(
            "INSERT INTO filings VALUES (?, ?, ?, ?, ?)",
            [
                ("S100OLD", "E00001", "11110", "043000", "2020-07-31"),
                ("S100NEW", "E00002", "22220", "043A00", "2025-09-30"),
            ],
        )
        self.conn.executemany(
            "INSERT INTO raw_facts (doc_id, tag_name, context_ref, unit_ref, period_type, period_start, period_end, instant_date, consolidation, context_dimensions_json, unit_measures_json, value_text) VALUES (?, ?, 'FilingDateInstant', 'shares', 'instant', NULL, NULL, ?, 'NonConsolidated', '', '', '1000000')",
            [
                ("S100OLD", "NumberOfIssuedSharesAsOfFiscalYearEndIssuedSharesTotalNumberOfSharesEtc", "2020-10-20"),
                ("S100NEW", "NumberOfIssuedSharesAsOfFilingDateIssuedSharesTotalNumberOfSharesEtc", "2025-11-12"),
            ],
        )
        self.conn.execute(
            "INSERT INTO normalized_metrics VALUES ('S100NEW', 'IssuedSharesCurrent', 1000000, 'NumberOfSharesIssuedSharesVotingRights')"
        )
        self.conn.execute(
            "INSERT INTO derived_metrics VALUES ('S100NEW', 'OutstandingSharesCurrent', NULL, 'missing_input')"
        )

    def tearDown(self) -> None:
        self.conn.close()
        shutil.rmtree(self.tmp_path, ignore_errors=True)

    def test_builds_actionable_manifest_for_both_half_forms(self) -> None:
        result = build_two_q_issued_shares_audit(self.conn)

        self.assertEqual(result.summary, {
            "eligible_count": 2,
            "actionable_count": 2,
            "match_count": 0,
            "missing_count": 1,
            "unsafe_source_count": 1,
        })
        self.assertEqual([row.status for row in result.rows], ["unsafe_source", "missing"])
        self.assertEqual(result.rows[1].period_end, "2020-07-31")
        self.assertEqual(result.rows[1].source_instant_date, "2020-10-20")
        paths = write_two_q_issued_shares_audit(result, output_dir=self.tmp_path)
        self.assertEqual(paths.doc_ids_path.read_text(encoding="utf-8"), "S100NEW\nS100OLD\n")
        self.assertTrue(paths.json_path.exists())
        self.assertTrue(paths.tsv_path.exists())

    def test_ignores_nonlisted_and_nonshare_candidates(self) -> None:
        self.conn.execute("INSERT INTO issuer_master VALUES ('E00003', '33330', 0, 'TSE')")
        self.conn.execute("INSERT INTO filings VALUES ('S100SKIP', 'E00003', '33330', '043A00', '2025-09-30')")
        self.conn.execute(
            "INSERT INTO raw_facts (doc_id, tag_name, context_ref, unit_ref, period_type, period_start, period_end, instant_date, consolidation, context_dimensions_json, unit_measures_json, value_text) VALUES ('S100SKIP', 'NumberOfIssuedSharesAsOfFilingDateIssuedSharesTotalNumberOfSharesEtc', 'FilingDateInstant', 'JPY', 'instant', NULL, NULL, '2025-11-12', 'NonConsolidated', '', '', '1000000')"
        )

        result = build_two_q_issued_shares_audit(self.conn)

        self.assertEqual(result.summary["eligible_count"], 2)
