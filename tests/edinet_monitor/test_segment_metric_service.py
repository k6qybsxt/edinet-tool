from __future__ import annotations

import json
import shutil
import sqlite3
import sys
import unittest
from pathlib import Path
from unittest.mock import patch
from zipfile import ZipFile


ROOT_DIR = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from edinet_monitor.db.schema import _create_segment_metrics_table  # noqa: E402
from edinet_monitor.services.segment_metric_service import (  # noqa: E402
    SegmentMetricBuildResult,
    SegmentMetricRow,
    build_segment_metric_rows,
    replace_segment_metrics,
    resolve_segment_fiscal_year_anchor,
    save_segment_metrics,
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


def _dimensions(member_qname: str, axis_qname: str = "jpcrp_cor:OperatingSegmentsAxis") -> str:
    return json.dumps(
        {
            "axis_members": {
                axis_qname: [member_qname],
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
    axis_qname: str = "jpcrp_cor:OperatingSegmentsAxis",
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
        (tag_name, f"jpcrp_cor:{tag_name}", period_end, _dimensions(member_qname, axis_qname), value_text),
    )


def _insert_raw_fact_context_ref(
    conn: sqlite3.Connection,
    *,
    tag_name: str,
    context_ref: str,
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
            'doc1', ?, ?, ?, 'JPY', '-6',
            'duration', '2025-04-01', ?, NULL, 0,
            NULL, '{}', ?
        )
        """,
        (tag_name, f"jpcrp_cor:{tag_name}", context_ref, period_end, value_text),
    )


def _insert_geographical_textblock(
    conn: sqlite3.Connection,
    value_text: str,
    period_end: str = "2025-09-30",
    tag_name: str = "InformationAboutGeographicalAreasIFRSTextBlock",
) -> None:
    conn.execute(
        """
        INSERT INTO raw_facts (
            doc_id, tag_name, tag_qname, context_ref, unit_ref, decimals,
            period_type, period_start, period_end, instant_date, is_nil,
            context_dimensions_json, unit_measures_json, value_text
        ) VALUES (
            'doc1', ?, ?,
            'InterimDuration', '', '',
            'duration', '2025-04-01', ?, NULL, 0,
            '{}', '{}', ?
        )
        """,
        (tag_name, f"jpcrp_cor:{tag_name}", period_end, value_text),
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

    def test_plural_reportable_segments_member_is_not_total(self) -> None:
        _insert_raw_fact(
            self.conn,
            tag_name="NetSales",
            member_qname="jpcrp_cor:JAPANReportableSegmentsMember",
            value_text="100000000",
        )
        _insert_raw_fact(
            self.conn,
            tag_name="NetSales",
            member_qname="jpcrp_cor:ReportableSegmentsMember",
            value_text="500000000",
        )
        self.conn.commit()

        result = build_segment_metric_rows(self.conn, codes=["4613"], form_codes=["043A00"])

        row_by_member = {row.member_qname: row for row in result.rows}
        self.assertEqual(row_by_member["jpcrp_cor:JAPANReportableSegmentsMember"].segment_kind, "region")
        self.assertEqual(row_by_member["jpcrp_cor:JAPANReportableSegmentsMember"].segment_name, "日本")
        self.assertEqual(row_by_member["jpcrp_cor:ReportableSegmentsMember"].segment_kind, "total")

    def test_business_member_containing_region_name_is_not_region(self) -> None:
        _insert_raw_fact(
            self.conn,
            tag_name="NetSales",
            member_qname="jpcrp040300-ssr_E03217-000:UNIQLOJapanMember",
            value_text="100000000",
        )
        _insert_raw_fact(
            self.conn,
            tag_name="OperatingIncome",
            member_qname="jpcrp040300-ssr_E03217-000:UNIQLOJAPANReportableSegmentMember",
            value_text="120000000",
            axis_qname="jpcrp_cor:GeographicalAreasAxis",
        )
        _insert_raw_fact(
            self.conn,
            tag_name="NetSales",
            member_qname="jpcrp_cor:JAPANMember",
            value_text="200000000",
            axis_qname="jpcrp_cor:GeographicalAreasAxis",
        )
        self.conn.commit()

        result = build_segment_metric_rows(self.conn, codes=["4613"], form_codes=["043A00"])

        row_by_member = {row.member_qname: row for row in result.rows}
        self.assertEqual(
            row_by_member["jpcrp040300-ssr_E03217-000:UNIQLOJapanMember"].segment_kind,
            "business",
        )
        self.assertEqual(
            row_by_member["jpcrp040300-ssr_E03217-000:UNIQLOJAPANReportableSegmentMember"].segment_kind,
            "business",
        )
        self.assertEqual(row_by_member["jpcrp_cor:JAPANMember"].segment_kind, "region")

    def test_context_ref_segment_member_is_used_when_dimensions_are_missing(self) -> None:
        _insert_raw_fact_context_ref(
            self.conn,
            tag_name="NetSales",
            context_ref="InterimDuration_jpcrp040300-ssr_E00893-000JAPANReportableSegmentsMember",
            value_text="100000000",
        )
        _insert_raw_fact_context_ref(
            self.conn,
            tag_name="OrdinaryIncome",
            context_ref="InterimDuration_jpcrp040300-ssr_E00893-000JAPANReportableSegmentsMember",
            value_text="30000000",
        )
        self.conn.commit()

        result = build_segment_metric_rows(self.conn, codes=["4613"], form_codes=["043A00"])

        row_by_base = {row.metric_base: row for row in result.rows}
        self.assertEqual(row_by_base["NetSales"].segment_kind, "region")
        self.assertEqual(row_by_base["NetSales"].value_num, 100000000.0)
        self.assertEqual(row_by_base["SegmentProfit"].calc_status, "review")
        self.assertEqual(row_by_base["SegmentProfit"].source_tag, "OrdinaryIncome")

    def test_operating_income_does_not_create_segment_profit_fallback(self) -> None:
        _insert_raw_fact(
            self.conn,
            tag_name="OperatingIncome",
            member_qname="jpcrp_cor:JAPANReportableSegmentMember",
            value_text="30000000",
        )
        self.conn.commit()

        result = build_segment_metric_rows(self.conn, codes=["4613"], form_codes=["043A00"])

        row_by_base = {row.metric_base: row for row in result.rows}
        self.assertEqual(row_by_base["OperatingIncome"].value_num, 30000000.0)
        self.assertNotIn("SegmentProfit", row_by_base)

    def test_linkbase_label_overrides_profit_tag(self) -> None:
        _insert_raw_fact(
            self.conn,
            tag_name="OperatingIncome",
            member_qname="jpcrp_cor:JAPANReportableSegmentMember",
            value_text="30000000",
        )
        self.conn.commit()

        structure = {
            "jpcrp_cor:OperatingIncome": {"label": "\u30bb\u30b0\u30e1\u30f3\u30c8\u5229\u76ca\u53c8\u306f\u640d\u5931"},
        }
        with patch(
            "edinet_monitor.services.segment_metric_service._linkbase_structure_for_filing",
            return_value=structure,
        ):
            result = build_segment_metric_rows(self.conn, codes=["4613"], form_codes=["043A00"])

        row_by_base = {row.metric_base: row for row in result.rows}
        self.assertNotIn("OperatingIncome", row_by_base)
        self.assertEqual(row_by_base["SegmentProfit"].calc_status, "review")
        detail = json.loads(row_by_base["SegmentProfit"].source_detail_json)
        self.assertEqual(detail["profit_metric_classification_source"], "linkbase_label")
        self.assertEqual(detail["profit_metric_classification_status"], "confirmed")

    def test_conflicting_profit_metrics_are_marked_review(self) -> None:
        _insert_raw_fact(
            self.conn,
            tag_name="OperatingIncome",
            member_qname="jpcrp_cor:JAPANReportableSegmentMember",
            value_text="30000000",
        )
        _insert_raw_fact(
            self.conn,
            tag_name="SegmentProfit",
            member_qname="jpcrp_cor:JAPANReportableSegmentMember",
            value_text="30000000",
        )
        self.conn.commit()

        result = build_segment_metric_rows(self.conn, codes=["4613"], form_codes=["043A00"])

        profit_rows = [row for row in result.rows if row.metric_base in {"OperatingIncome", "SegmentProfit"}]
        self.assertEqual({row.calc_status for row in profit_rows}, {"review"})

    def test_two_q_fiscal_year_uses_current_fiscal_year_end_dei(self) -> None:
        self.conn.execute("UPDATE filings SET period_end = '2020-07-31' WHERE doc_id = 'doc1'")
        _insert_raw_fact(
            self.conn,
            tag_name="NetSales",
            member_qname="jpcrp_cor:JAPANReportableSegmentMember",
            value_text="100000000",
            period_end="2020-07-31",
        )
        self.conn.execute(
            """
            INSERT INTO raw_facts (
                doc_id, tag_name, tag_qname, context_ref, unit_ref, decimals,
                period_type, period_start, period_end, instant_date, is_nil,
                context_dimensions_json, unit_measures_json, value_text
            ) VALUES (
                'doc1', 'CurrentFiscalYearEndDateDEI', 'jpdei_cor:CurrentFiscalYearEndDateDEI',
                'FilingDateInstant', '', '', 'instant', NULL, NULL, '2020-09-11', 0,
                '{}', '{}', '2021-01-31'
            )
            """
        )
        self.conn.commit()

        result = build_segment_metric_rows(self.conn, codes=["4613"], form_codes=["043A00"])

        row = next(item for item in result.rows if item.metric_base == "NetSales")
        self.assertEqual(row.fiscal_year, 2021)
        detail = json.loads(row.source_detail_json)
        self.assertEqual(detail["fiscal_year_anchor_period_end"], "2021-01-31")
        self.assertEqual(detail["fiscal_year_anchor_source"], "current_fiscal_year_end_dei")

    def test_two_q_fiscal_year_falls_back_to_next_annual_period(self) -> None:
        self.conn.execute("UPDATE filings SET period_end = '2020-09-30' WHERE doc_id = 'doc1'")
        self.conn.execute(
            """
            INSERT INTO filings (
                doc_id, edinet_code, security_code, form_type, period_end, zip_path, xbrl_path
            ) VALUES ('annual1', 'E00893', '46130', '030000', '2021-03-31', '', '')
            """
        )
        _insert_raw_fact(
            self.conn,
            tag_name="NetSales",
            member_qname="jpcrp_cor:JAPANReportableSegmentMember",
            value_text="100000000",
            period_end="2020-09-30",
        )
        self.conn.commit()

        result = build_segment_metric_rows(self.conn, codes=["4613"], form_codes=["043A00"])

        row = next(item for item in result.rows if item.metric_base == "NetSales")
        self.assertEqual(row.fiscal_year, 2021)
        detail = json.loads(row.source_detail_json)
        self.assertEqual(detail["fiscal_year_anchor_source"], "next_annual_period_end")

    def test_two_q_fiscal_year_direct_dei_covers_january_march_and_december_year_ends(self) -> None:
        filing = {"doc_id": "doc1", "edinet_code": "E00893", "form_type": "043A00"}
        cases = [
            ("2020-07-31", "2021-01-31", 2021),
            ("2020-09-30", "2021-03-31", 2021),
            ("2020-06-30", "2020-12-31", 2020),
        ]

        for period_end, fiscal_year_end, expected_year in cases:
            with self.subTest(period_end=period_end):
                anchor = resolve_segment_fiscal_year_anchor(
                    filing=filing,
                    fact_period_end=period_end,
                    current_fiscal_year_ends={"doc1": fiscal_year_end},
                    annual_period_ends_by_edinet_code={},
                )
                self.assertEqual(anchor.fiscal_year, expected_year)
                self.assertEqual(anchor.fiscal_year_end, fiscal_year_end)
                self.assertEqual(anchor.source, "current_fiscal_year_end_dei")
                self.assertEqual(anchor.status, "ok")

    def test_geographical_area_textblock_matrix_table_extracts_current_region_values(self) -> None:
        _insert_geographical_textblock(
            self.conn,
            """
            <p>（単位：百万円）</p>
            <table>
              <tr><td></td><td>日本</td><td>北米</td><td>連結</td></tr>
              <tr><td>営業収益</td><td></td><td></td><td></td></tr>
              <tr><td>外部顧客への営業収益</td><td>10</td><td>20</td><td>30</td></tr>
              <tr><td>営業利益</td><td>1</td><td>2</td><td>3</td></tr>
            </table>
            """,
        )
        self.conn.commit()

        result = build_segment_metric_rows(self.conn, codes=["4613"], form_codes=["043A00"])

        row_by_name_base = {(row.segment_name, row.metric_base): row for row in result.rows}
        self.assertEqual(row_by_name_base[("日本", "NetSales")].segment_kind, "region")
        self.assertEqual(row_by_name_base[("日本", "NetSales")].value_num, 10_000_000.0)
        self.assertEqual(row_by_name_base[("北米", "OperatingIncome")].value_num, 2_000_000.0)
        self.assertEqual(row_by_name_base[("合計", "NetSales")].segment_kind, "total")

    def test_geographical_area_textblock_row_table_extracts_current_sales_column(self) -> None:
        _insert_geographical_textblock(
            self.conn,
            """
            <table>
              <tr><td>項目</td><td>2024年度</td><td>2025年度</td></tr>
              <tr><td>金額（百万円）</td><td>金額（百万円）</td><td>金額（百万円）</td></tr>
              <tr><td>売上高：</td><td></td><td></td></tr>
              <tr><td>日本</td><td>10</td><td>11</td></tr>
              <tr><td>米国</td><td>20</td><td>22</td></tr>
              <tr><td>その他地域</td><td>5</td><td>6</td></tr>
            </table>
            """,
        )
        self.conn.commit()

        result = build_segment_metric_rows(self.conn, codes=["4613"], form_codes=["043A00"])

        row_by_name = {row.segment_name: row for row in result.rows if row.metric_base == "NetSales"}
        self.assertEqual(row_by_name["日本"].value_num, 11_000_000.0)
        self.assertEqual(row_by_name["米国"].value_num, 22_000_000.0)
        self.assertEqual(row_by_name["その他地域"].value_num, 6_000_000.0)

    def test_segment_note_textblock_rejects_assets_and_extracts_cluster_metrics(self) -> None:
        _insert_geographical_textblock(
            self.conn,
            """
            <p>地域別に関する情報</p>
            <table>
              <tr><td></td><td>日本</td><td>海外</td></tr>
              <tr><td>非流動資産</td><td>100</td><td>200</td></tr>
            </table>
            <p>クラスター別</p>
            <table>
              <tr><td></td><td>Asia</td><td>Western Europe</td><td>EMA</td></tr>
              <tr><td>自社たばこ製品売上収益</td><td>10</td><td>20</td><td>30</td></tr>
              <tr><td>調整後営業利益</td><td>1</td><td>2</td><td>3</td></tr>
            </table>
            """,
            tag_name="NotesSegmentInformationConsolidatedFinancialStatementsIFRSTextBlock",
        )
        self.conn.commit()

        result = build_segment_metric_rows(self.conn, codes=["4613"], form_codes=["043A00"])

        rows_by_name_base = {(row.segment_name, row.metric_base): row for row in result.rows}
        self.assertNotIn(("日本", "NetSales"), rows_by_name_base)
        self.assertEqual(rows_by_name_base[("Asia", "NetSales")].value_num, 10.0)
        self.assertEqual(rows_by_name_base[("Western Europe", "NetSales")].segment_kind, "region")
        self.assertEqual(rows_by_name_base[("EMA", "SegmentProfit")].value_num, 3.0)
        detail = json.loads(rows_by_name_base[("Asia", "NetSales")].source_detail_json)
        self.assertEqual(detail["table_kind"], "region")
        self.assertEqual(detail["source"], "segment_note_textblock")
        self.assertTrue(any(item.reason.endswith("excluded_asset_table") for item in result.candidates))

    def test_segment_note_textblock_uses_current_cluster_table_and_ignores_unit_headers(self) -> None:
        _insert_geographical_textblock(
            self.conn,
            """
            <p>前年度 (自 2024年4月1日 至 2025年3月31日)</p>
            <table><tr><td></td><td>報告セグメント</td></tr>
              <tr><td></td><td>たばこ</td></tr>
              <tr><td></td><td>百万円</td></tr>
              <tr><td>売上収益</td><td>100</td></tr></table>
            <p>クラスター別</p>
            <table><tr><td></td><td>Asia</td><td>EMA</td></tr>
              <tr><td></td><td>百万円</td><td>百万円</td></tr>
              <tr><td>売上収益</td><td>10</td><td>20</td></tr></table>
            <p>当年度 (自 2025年4月1日 至 2025年9月30日)</p>
            <table><tr><td></td><td>報告セグメント</td></tr>
              <tr><td></td><td>たばこ</td></tr>
              <tr><td></td><td>百万円</td></tr>
              <tr><td>売上収益</td><td>200</td></tr></table>
            <p>クラスター別</p>
            <table><tr><td></td><td>Asia</td><td>EMA</td></tr>
              <tr><td></td><td>百万円</td><td>百万円</td></tr>
              <tr><td>売上収益</td><td>11</td><td>22</td></tr></table>
            """,
            tag_name="NotesSegmentInformationConsolidatedFinancialStatementsIFRSTextBlock",
        )
        self.conn.commit()

        result = build_segment_metric_rows(self.conn, codes=["4613"], form_codes=["043A00"])

        rows_by_name = {
            row.segment_name: row
            for row in result.rows
            if row.segment_kind == "region" and row.metric_base == "NetSales"
        }
        self.assertEqual(rows_by_name["Asia"].value_num, 11_000_000.0)
        self.assertEqual(rows_by_name["EMA"].value_num, 22_000_000.0)
        self.assertNotIn("百万円", rows_by_name)

    def test_segment_note_textblock_expands_reportable_segment_header_spans(self) -> None:
        _insert_geographical_textblock(
            self.conn,
            """
            <p>当年度 (自 2025年4月1日 至 2025年9月30日)</p>
            <table>
              <tr><td></td><td colspan="2">報告セグメント</td><td>その他</td><td>連結</td></tr>
              <tr><td></td><td>事業A</td><td>事業B</td><td>その他事業</td><td>連結財務諸表計上額</td></tr>
              <tr><td>売上高</td><td></td><td></td><td></td><td></td></tr>
              <tr><td>外部顧客への売上高</td><td>10</td><td>20</td><td>3</td><td>33</td></tr>
              <tr><td>セグメント利益又は損失</td><td>1</td><td>2</td><td>0</td><td>3</td></tr>
            </table>
            """,
            tag_name="NotesSegmentInformationConsolidatedFinancialStatementsIFRSTextBlock",
        )
        self.conn.commit()

        result = build_segment_metric_rows(self.conn, codes=["4613"], form_codes=["043A00"])

        rows_by_name_base = {(row.segment_name, row.metric_base): row for row in result.rows}
        self.assertEqual(rows_by_name_base[("事業A", "NetSales")].value_num, 10.0)
        self.assertEqual(rows_by_name_base[("事業B", "SegmentProfit")].value_num, 2.0)
        self.assertNotIn(("連結財務諸表計上額", "NetSales"), rows_by_name_base)

    def test_segment_note_textblock_conflicting_dimensioned_value_is_review(self) -> None:
        _insert_raw_fact(
            self.conn,
            tag_name="NetSales",
            member_qname="jpcrp_cor:AsiaMember",
            value_text="999",
            axis_qname="jpcrp_cor:GeographicalAreasAxis",
        )
        _insert_geographical_textblock(
            self.conn,
            """
            <p>クラスター別</p>
            <table><tr><td></td><td>アジア</td></tr>
            <tr><td>売上高</td><td>10</td></tr></table>
            """,
            tag_name="NotesSegmentInformationConsolidatedFinancialStatementsIFRSTextBlock",
        )
        self.conn.commit()

        result = build_segment_metric_rows(self.conn, codes=["4613"], form_codes=["043A00"])

        raw_row = next(row for row in result.rows if row.source_tag == "NetSales")
        text_row = next(row for row in result.rows if row.source_tag.startswith("NotesSegmentInformation"))
        self.assertEqual(raw_row.value_num, 999.0)
        self.assertEqual(text_row.calc_status, "review")
        detail = json.loads(text_row.source_detail_json)
        self.assertEqual(detail["semantic_conflict_reason"], "conflicting_dimensioned_xbrl_value")

    def test_operating_segments_not_included_label_is_other(self) -> None:
        _insert_raw_fact(
            self.conn,
            tag_name="NetSales",
            member_qname=(
                "jpcrp_cor:"
                "OperatingSegmentsNotIncludedInReportableSegmentsAndOtherRevenueGeneratingBusinessActivitiesMember"
            ),
            value_text="100000000",
        )
        self.conn.commit()

        result = build_segment_metric_rows(self.conn, codes=["4613"], form_codes=["043A00"])

        self.assertEqual(result.rows[0].segment_kind, "business")
        self.assertEqual(result.rows[0].segment_name, "その他")

    def test_segment_member_label_and_order_come_from_public_doc_linkbase(self) -> None:
        tmp_dir = Path("tests") / "_tmp_segment_linkbase"
        shutil.rmtree(tmp_dir, ignore_errors=True)
        tmp_dir.mkdir(parents=True, exist_ok=True)
        zip_path = tmp_dir / "sample.zip"
        lab_xml = """<?xml version="1.0" encoding="utf-8"?>
<link:linkbase xmlns:link="http://www.xbrl.org/2003/linkbase" xmlns:xlink="http://www.w3.org/1999/xlink">
  <link:labelLink xlink:type="extended" xlink:role="http://www.xbrl.org/2003/role/link">
    <link:loc xlink:type="locator" xlink:href="sample.xsd#jpcrp030000-asr_E00001-000_PaintBusinessMember" xlink:label="paint" />
    <link:label xlink:type="resource" xlink:label="label_paint" xlink:role="http://www.xbrl.org/2003/role/label" xml:lang="ja">塗料事業</link:label>
    <link:labelArc xlink:type="arc" xlink:from="paint" xlink:to="label_paint" />
  </link:labelLink>
</link:linkbase>
"""
        pre_xml = """<?xml version="1.0" encoding="utf-8"?>
<link:linkbase xmlns:link="http://www.xbrl.org/2003/linkbase" xmlns:xlink="http://www.w3.org/1999/xlink">
  <link:presentationLink xlink:type="extended" xlink:role="http://example.com/role/Segments">
    <link:loc xlink:type="locator" xlink:href="sample.xsd#jpcrp_cor_OperatingSegmentsAxis" xlink:label="axis" />
    <link:loc xlink:type="locator" xlink:href="sample.xsd#jpcrp030000-asr_E00001-000_PaintBusinessMember" xlink:label="paint" />
    <link:presentationArc xlink:type="arc" xlink:from="axis" xlink:to="paint" order="3" />
  </link:presentationLink>
</link:linkbase>
"""
        try:
            with ZipFile(zip_path, "w") as zf:
                zf.writestr("XBRL/PublicDoc/sample.xbrl", "<xbrli:xbrl/>")
                zf.writestr("XBRL/PublicDoc/sample_lab.xml", lab_xml)
                zf.writestr("XBRL/PublicDoc/sample_pre.xml", pre_xml)
            self.conn.execute("UPDATE filings SET zip_path = ?, xbrl_path = 'sample.xbrl' WHERE doc_id = 'doc1'", (str(zip_path),))
            _insert_raw_fact(
                self.conn,
                tag_name="NetSales",
                member_qname="jpcrp030000-asr_E00001-000:PaintBusinessMember",
                value_text="100000000",
            )
            self.conn.commit()

            result = build_segment_metric_rows(self.conn, codes=["4613"], form_codes=["043A00"])
        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)

        self.assertEqual(result.rows[0].segment_name, "塗料事業")
        detail = json.loads(result.rows[0].source_detail_json)
        self.assertIsInstance(detail.get("segment_order"), int)

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

    def test_replace_segment_metrics_deletes_existing_doc_when_new_rows_are_empty(self) -> None:
        _insert_raw_fact(
            self.conn,
            tag_name="NetSales",
            member_qname="jpcrp_cor:JAPANReportableSegmentMember",
            value_text="100000000",
        )
        self.conn.commit()
        result = build_segment_metric_rows(self.conn, codes=["4613"], form_codes=["043A00"])
        replace_segment_metrics(self.conn, result.rows)

        saved_count_before = self.conn.execute("SELECT count(*) FROM segment_metrics").fetchone()[0]
        deleted_insert_count = replace_segment_metrics(self.conn, [], replace_doc_ids=["doc1"])
        saved_count_after = self.conn.execute("SELECT count(*) FROM segment_metrics").fetchone()[0]

        self.assertEqual(saved_count_before, 1)
        self.assertEqual(deleted_insert_count, 0)
        self.assertEqual(saved_count_after, 0)

    def test_save_segment_metrics_batches_large_doc_id_manifests(self) -> None:
        doc_ids = [f"doc{index:03d}" for index in range(251)]

        def build_for_batch(_conn, *, doc_ids, **_kwargs):
            doc_id = doc_ids[0]
            row = SegmentMetricRow(
                doc_id=doc_id,
                edinet_code="E00893",
                security_code="4613",
                form_type="043A00",
                period_scope="quarter",
                quarter_type="2Q",
                fiscal_year=2026,
                period_start="2025-04-01",
                period_end="2025-09-30",
                segment_kind="business",
                segment_name="Paint",
                axis_qname="axis",
                member_qname="member",
                metric_base="NetSales",
                metric_key="SegmentNetSalesCurrent",
                value_kind="total",
                value_num=1.0,
                value_unit="yen",
                source_tag="NetSales",
                tag_qname="tag",
                context_ref="context",
                decimals="-6",
                calc_status="ok",
                source_detail_json="{}",
            )
            return SegmentMetricBuildResult(rows=[row], candidates=[], warnings=[])

        with (
            patch(
                "edinet_monitor.services.segment_metric_service.build_segment_metric_rows",
                side_effect=build_for_batch,
            ) as build,
            patch(
                "edinet_monitor.services.segment_metric_service.replace_segment_metrics",
                side_effect=lambda _conn, rows, **_kwargs: len(rows),
            ) as replace_rows,
            patch(
                "edinet_monitor.services.segment_metric_service.write_segment_metric_report",
                return_value=Path("report.txt"),
            ),
        ):
            result = save_segment_metrics(self.conn, doc_ids=doc_ids, apply=True)

        self.assertEqual(build.call_count, 2)
        self.assertEqual(replace_rows.call_count, 3)
        self.assertEqual(result.built_row_count, 2)
        self.assertEqual(result.saved_rows, 2)
        self.assertEqual(result.replaced_doc_count, 251)

    def test_save_segment_metrics_stops_without_deleting_when_all_requested_docs_are_empty(self) -> None:
        empty_build = SegmentMetricBuildResult(rows=[], candidates=[], warnings=[])
        with (
            patch(
                "edinet_monitor.services.segment_metric_service.build_segment_metric_rows",
                return_value=empty_build,
            ),
            patch(
                "edinet_monitor.services.segment_metric_service.replace_segment_metrics"
            ) as replace_rows,
        ):
            with self.assertRaisesRegex(RuntimeError, "No segment metrics were built"):
                save_segment_metrics(self.conn, doc_ids=["S100EMPTY"], apply=True)

        replace_rows.assert_not_called()

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
