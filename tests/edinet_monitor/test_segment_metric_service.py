from __future__ import annotations

import json
import sqlite3
import sys
import unittest
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from edinet_monitor.db.schema import _create_segment_metrics_table  # noqa: E402
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
    conn.execute(
        """
        CREATE UNIQUE INDEX uq_segment_metrics_scope
        ON segment_metrics(
            doc_id, segment_kind, member_qname, metric_key, value_kind, period_start, period_end
        )
        """
    )


def _dimensions(member_qname: str) -> str:
    return json.dumps(
        {
            "axis_members": {
                "jpcrp_cor:OperatingSegmentsAxis": [member_qname],
            }
        },
        ensure_ascii=False,
    )


def _insert_raw_fact(
    conn: sqlite3.Connection,
    *,
    tag_name: str,
    member_qname: str,
    value_text: str,
    period_end: str = "2025-09-30",
) -> None:
    conn.execute(
        """
        INSERT INTO raw_facts (
            doc_id, tag_name, tag_qname, context_ref, unit_ref, decimals,
            period_type, period_start, period_end, instant_date, is_nil,
            context_dimensions_json, unit_measures_json, value_text
        ) VALUES (
            'doc1', ?, ?, 'InterimDuration', 'JPY', '-6',
            'duration', '2025-04-01', ?, NULL, 0,
            ?, '{}', ?
        )
        """,
        (tag_name, f"jpcrp_cor:{tag_name}", period_end, _dimensions(member_qname), value_text),
    )


class SegmentMetricServiceTest(unittest.TestCase):
    def setUp(self) -> None:
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        _create_schema(self.conn)
        self.conn.execute(
            """
            INSERT INTO issuer_master (
                edinet_code, security_code, company_name, industry_33, market,
                is_listed, exchange
            ) VALUES ('E00893', '46130', '関西ペイント', '化学', 'Prime', 1, 'TSE')
            """
        )
        self.conn.execute(
            """
            INSERT INTO filings (
                doc_id, edinet_code, security_code, form_type, period_end, zip_path, xbrl_path
            ) VALUES ('doc1', 'E00893', '46130', '043A00', '2025-09-30', '', '')
            """
        )

    def tearDown(self) -> None:
        self.conn.close()

    def test_build_segment_metric_rows_selects_region_and_total(self) -> None:
        _insert_raw_fact(
            self.conn,
            tag_name="NetSales",
            member_qname="jpcrp_cor:JAPANReportableSegmentMember",
            value_text="100000000",
        )
        _insert_raw_fact(
            self.conn,
            tag_name="ProfitLossBeforeTaxIFRS",
            member_qname="jpcrp_cor:JAPANReportableSegmentMember",
            value_text="30000000",
        )
        _insert_raw_fact(
            self.conn,
            tag_name="NetSales",
            member_qname="jpcrp_cor:TotalOfReportableSegmentsAndOthersMember",
            value_text="500000000",
        )
        _insert_raw_fact(
            self.conn,
            tag_name="NetSales",
            member_qname="jpcrp_cor:ReconcilingItemsMember",
            value_text="999999999",
        )
        self.conn.commit()

        result = build_segment_metric_rows(self.conn, codes=["4613"], form_codes=["043A00"])

        self.assertEqual(len(result.rows), 3)
        row_by_base_kind = {(row.metric_base, row.segment_kind): row for row in result.rows}
        self.assertIn(("NetSales", "region"), row_by_base_kind)
        self.assertIn(("ProfitBeforeTax", "region"), row_by_base_kind)
        self.assertIn(("NetSales", "total"), row_by_base_kind)
        profit_row = row_by_base_kind[("ProfitBeforeTax", "region")]
        self.assertEqual(profit_row.period_scope, "quarter")
        self.assertEqual(profit_row.quarter_type, "2Q")
        self.assertEqual(profit_row.segment_name, "日本")
        self.assertEqual(profit_row.value_num, 30000000.0)
        excluded_reasons = {candidate.reason for candidate in result.candidates if candidate.status == "excluded"}
        self.assertIn("excluded_adjustment_or_elimination_member", excluded_reasons)

    def test_replace_segment_metrics_is_idempotent(self) -> None:
        _insert_raw_fact(
            self.conn,
            tag_name="NetSales",
            member_qname="jpcrp_cor:JAPANReportableSegmentMember",
            value_text="100000000",
        )
        self.conn.commit()
        result = build_segment_metric_rows(self.conn, codes=["4613"], form_codes=["043A00"])

        first_count = replace_segment_metrics(self.conn, result.rows)
        second_count = replace_segment_metrics(self.conn, result.rows)
        saved_count = self.conn.execute("SELECT count(*) FROM segment_metrics").fetchone()[0]

        self.assertEqual(first_count, 1)
        self.assertEqual(second_count, 1)
        self.assertEqual(saved_count, 1)

    def test_build_segment_metric_rows_excludes_prior_period_segment_fact(self) -> None:
        _insert_raw_fact(
            self.conn,
            tag_name="NetSales",
            member_qname="jpcrp_cor:JAPANReportableSegmentMember",
            value_text="100000000",
            period_end="2024-09-30",
        )
        self.conn.commit()

        result = build_segment_metric_rows(self.conn, codes=["4613"], form_codes=["043A00"])

        self.assertEqual(result.rows, [])
        excluded_reasons = {candidate.reason for candidate in result.candidates if candidate.status == "excluded"}
        self.assertIn("period_end_mismatch", excluded_reasons)


if __name__ == "__main__":
    unittest.main()
