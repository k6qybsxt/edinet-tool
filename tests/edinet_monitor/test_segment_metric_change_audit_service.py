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
TMP_ROOT = ROOT_DIR / "tests" / "_tmp_segment_metric_change_audit"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from edinet_monitor.db.schema import _create_segment_metrics_table  # noqa: E402
from edinet_monitor.services.segment_metric_change_audit_service import (  # noqa: E402
    build_segment_metric_change_audit,
    write_segment_metric_change_audit,
)
from edinet_monitor.services.segment_metric_service import (  # noqa: E402
    build_segment_metric_rows,
    replace_segment_metrics,
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
        """
    )
    _create_segment_metrics_table(conn.cursor())


def _insert_existing_metric(
    conn: sqlite3.Connection,
    *,
    metric_base: str,
    metric_key: str,
    value_kind: str,
    calc_status: str,
    source_detail_json: str,
) -> None:
    conn.execute(
        """
        INSERT INTO segment_metrics (
            doc_id, edinet_code, security_code, form_type, period_scope, quarter_type,
            fiscal_year, period_start, period_end, segment_kind, segment_name,
            axis_qname, member_qname, metric_base, metric_key, value_kind,
            value_num, value_unit, source_tag, tag_qname, context_ref, decimals,
            calc_status, source_detail_json, rule_version, created_at, updated_at
        ) VALUES (
            'doc1', 'E00001', '1111', '043A00', 'quarter', '2Q',
            2024, '2024-02-01', '2024-07-31', 'business', 'Housing',
            'jpcrp_cor:OperatingSegmentsAxis', 'example:HousingMember', ?, ?, ?,
            10.0, 'yen', 'OperatingIncome', 'jpcrp_cor:OperatingIncome',
            'InterimDuration', '-6', ?, ?, 'old', 'now', 'now'
        )
        """,
        (metric_base, metric_key, value_kind, calc_status, source_detail_json),
    )


class SegmentMetricChangeAuditServiceTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp_path = TMP_ROOT / uuid.uuid4().hex
        self.tmp_path.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(":memory:")
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
            INSERT INTO filings (
                doc_id, edinet_code, security_code, form_type, period_end, zip_path, xbrl_path
            ) VALUES ('doc1', 'E00001', '11110', '043A00', '2024-07-31', '', '')
            """
        )
        dimensions = json.dumps(
            {
                "axis_members": {
                    "jpcrp_cor:OperatingSegmentsAxis": ["example:HousingMember"],
                }
            }
        )
        self.conn.executemany(
            """
            INSERT INTO raw_facts (
                doc_id, tag_name, tag_qname, context_ref, unit_ref, decimals,
                period_type, period_start, period_end, instant_date, is_nil,
                context_dimensions_json, unit_measures_json, value_text
            ) VALUES (
                'doc1', ?, ?, 'InterimDuration', 'JPY', '-6',
                'duration', '2024-02-01', '2024-07-31', NULL, 0, ?, '{}', ?
            )
            """,
            [
                ("NetSales", "jpcrp_cor:NetSales", dimensions, "100"),
                ("OperatingIncome", "jpcrp_cor:OperatingIncome", dimensions, "10"),
            ],
        )
        self.conn.execute(
            """
            INSERT INTO raw_facts (
                doc_id, tag_name, tag_qname, context_ref, unit_ref, decimals,
                period_type, period_start, period_end, instant_date, is_nil,
                context_dimensions_json, unit_measures_json, value_text
            ) VALUES (
                'doc1', 'CurrentFiscalYearEndDateDEI', 'jpdei_cor:CurrentFiscalYearEndDateDEI',
                'FilingDateInstant', '', '', 'instant', NULL, NULL, '2024-09-12', 0,
                '{}', '{}', '2025-01-31'
            )
            """
        )
        _insert_existing_metric(
            self.conn,
            metric_base="OperatingIncome",
            metric_key="SegmentOperatingIncomeCurrent",
            value_kind="operating_profit",
            calc_status="ok",
            source_detail_json="{}",
        )
        _insert_existing_metric(
            self.conn,
            metric_base="SegmentProfit",
            metric_key="SegmentProfitCurrent",
            value_kind="segment_profit",
            calc_status="ok",
            source_detail_json='{"source":"operating_income_segment_profit_fallback"}',
        )
        self.conn.commit()

    def tearDown(self) -> None:
        self.conn.close()
        shutil.rmtree(self.tmp_path, ignore_errors=True)

    def test_audit_rebuild_manifest_converges_after_replacement(self) -> None:
        before = build_segment_metric_change_audit(self.conn)

        self.assertEqual(before.rebuild_doc_ids, ("doc1",))
        self.assertEqual(before.summary["fiscal_anchor_mismatch_count"], 1)
        self.assertEqual(before.summary["profit_classification_changed_count"], 1)
        paths = write_segment_metric_change_audit(before, output_dir=self.tmp_path)
        self.assertEqual(paths.rebuild_doc_ids_path.read_text(encoding="utf-8"), "doc1\n")
        self.assertTrue(paths.fiscal_anchor_tsv_path.exists())
        self.assertTrue(paths.profit_classification_tsv_path.exists())

        build = build_segment_metric_rows(self.conn, doc_ids=["doc1"], form_codes=["043A00"])
        replace_segment_metrics(self.conn, build.rows, replace_doc_ids=["doc1"])
        self.conn.commit()

        after = build_segment_metric_change_audit(self.conn)
        self.assertEqual(after.rebuild_doc_ids, ())
        self.assertEqual(after.summary["fiscal_anchor_mismatch_count"], 0)
        self.assertEqual(after.summary["profit_classification_changed_count"], 0)
