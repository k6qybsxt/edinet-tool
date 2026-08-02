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
TMP_ROOT = ROOT_DIR / "tests" / "_tmp_segment_note_semantics_audit"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from edinet_monitor.cli import audit_segment_note_semantics as audit_cli  # noqa: E402
from edinet_monitor.db.schema import _create_segment_metrics_table  # noqa: E402
from edinet_monitor.services.segment_metric_service import replace_segment_metrics  # noqa: E402
from edinet_monitor.services.segment_note_semantics_audit_service import (  # noqa: E402
    build_segment_note_semantics_audit,
    write_segment_note_semantics_audit,
)


def _create_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE issuer_master (
            edinet_code TEXT PRIMARY KEY,
            security_code TEXT,
            company_name TEXT,
            industry_33 TEXT,
            market TEXT,
            is_listed INTEGER NOT NULL DEFAULT 1,
            exchange TEXT
        );
        CREATE TABLE filings (
            doc_id TEXT PRIMARY KEY,
            edinet_code TEXT NOT NULL,
            security_code TEXT,
            form_type TEXT NOT NULL,
            period_end TEXT,
            zip_path TEXT,
            xbrl_path TEXT
        );
        CREATE TABLE raw_facts (
            doc_id TEXT NOT NULL,
            tag_name TEXT,
            tag_qname TEXT,
            context_ref TEXT,
            unit_ref TEXT,
            decimals TEXT,
            period_type TEXT,
            period_start TEXT,
            period_end TEXT,
            instant_date TEXT,
            is_nil INTEGER,
            context_dimensions_json TEXT,
            unit_measures_json TEXT,
            value_text TEXT
        );
        CREATE INDEX idx_raw_facts_tag_name ON raw_facts(tag_name);
        """
    )
    _create_segment_metrics_table(conn.cursor())


class SegmentNoteSemanticsAuditServiceTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp_path = TMP_ROOT / uuid.uuid4().hex
        self.tmp_path.mkdir(parents=True, exist_ok=True)
        self.db_path = self.tmp_path / "audit.db"
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row
        _create_schema(self.conn)
        self.conn.execute(
            """
            INSERT INTO issuer_master (
                edinet_code, security_code, company_name, industry_33, market, is_listed, exchange
            ) VALUES ('E00001', '11110', 'Example', 'Services', 'Prime', 1, 'TSE')
            """
        )
        self.conn.execute(
            """
            INSERT INTO filings (doc_id, edinet_code, security_code, form_type, period_end, zip_path, xbrl_path)
            VALUES ('doc1', 'E00001', '11110', '030000', '2025-12-31', '', '')
            """
        )
        self.conn.execute(
            """
            INSERT INTO raw_facts (
                doc_id, tag_name, tag_qname, context_ref, unit_ref, decimals,
                period_type, period_start, period_end, instant_date, is_nil,
                context_dimensions_json, unit_measures_json, value_text
            ) VALUES (
                'doc1', 'FootnotesRegardingSegmentInformationTableTextBlock',
                'jpcrp_cor:FootnotesRegardingSegmentInformationTableTextBlock',
                'CurrentYearDuration', '', '', 'duration', '2025-01-01', '2025-12-31', NULL, 0,
                '{}', '{}', ?
            )
            """,
            (
                """
                <p>地域別に関する情報</p>
                <table><tr><td></td><td>Japan</td><td>Overseas</td></tr>
                <tr><td>非流動資産</td><td>10</td><td>20</td></tr></table>
                <p>クラスター別</p>
                <table><tr><td></td><td>Asia</td><td>EMA</td></tr>
                <tr><td>売上収益</td><td>100</td><td>200</td></tr>
                <tr><td>営業利益</td><td>10</td><td>20</td></tr></table>
                """,
            ),
        )
        self.conn.commit()

    def tearDown(self) -> None:
        self.conn.close()
        shutil.rmtree(self.tmp_path, ignore_errors=True)

    def test_audit_builds_manifest_then_converges_after_replacement(self) -> None:
        before = build_segment_note_semantics_audit(self.conn)

        self.assertEqual(before.rebuild_doc_ids, ("doc1",))
        self.assertEqual(before.summary["candidate_doc_count"], 1)
        self.assertGreater(before.summary["excluded_candidate_count"], 0)
        paths = write_segment_note_semantics_audit(before, output_dir=self.tmp_path)
        self.assertEqual(paths.rebuild_doc_ids_path.read_text(encoding="utf-8"), "doc1\n")
        self.assertTrue(paths.rows_json_path.exists())
        self.assertTrue(paths.rows_tsv_path.exists())

        from edinet_monitor.services.segment_metric_service import build_segment_metric_rows

        build = build_segment_metric_rows(self.conn, doc_ids=["doc1"], form_codes=["030000"])
        replace_segment_metrics(self.conn, build.rows, replace_doc_ids=["doc1"])
        self.conn.commit()

        after = build_segment_note_semantics_audit(self.conn)
        self.assertEqual(after.rebuild_doc_ids, ())
        self.assertEqual(after.summary["match_doc_count"], 1)

        exact_after = build_segment_note_semantics_audit(self.conn, doc_ids=("doc1",), exact=True)
        self.assertEqual(exact_after.rebuild_doc_ids, ())
        self.assertEqual(exact_after.summary["match_doc_count"], 1)

    def test_cli_writes_read_only_audit_outputs(self) -> None:
        output_dir = self.tmp_path / "cli_output"
        self.conn.close()
        with patch.object(
            sys,
            "argv",
            [
                "audit_segment_note_semantics",
                "--db-path",
                str(self.db_path),
                "--output-dir",
                str(output_dir),
            ],
        ):
            audit_cli.main()

        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row
        manifests = list(output_dir.rglob("rebuild_doc_ids.txt"))
        self.assertEqual(len(manifests), 1)
        self.assertEqual(manifests[0].read_text(encoding="utf-8"), "doc1\n")
