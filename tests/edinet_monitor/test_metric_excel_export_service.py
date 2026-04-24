from __future__ import annotations

import sqlite3
import shutil
import sys
import unittest
import uuid
from pathlib import Path

from openpyxl import Workbook, load_workbook


ROOT_DIR = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT_DIR / "src"
TMP_ROOT = ROOT_DIR / "tests" / "_tmp_metric_excel_export"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from edinet_monitor.services.metric_excel_export_service import (  # noqa: E402
    GENERAL_SHEET,
    build_metric_excel_rows,
    export_metric_excel,
    read_metric_excel_condition,
)


def _create_condition_workbook(path: Path, rows: list[tuple[str, str]]) -> None:
    wb = Workbook()
    ws = wb.active
    ws.title = "条件"
    for key, value in rows:
        ws.append([key, value])
    wb.save(path)


def _create_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE issuer_master (
            edinet_code TEXT PRIMARY KEY,
            security_code TEXT,
            company_name TEXT NOT NULL,
            market TEXT,
            industry_33 TEXT,
            industry_17 TEXT,
            is_listed INTEGER NOT NULL DEFAULT 1,
            exchange TEXT,
            listing_category_raw TEXT,
            listing_source TEXT,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE filings (
            doc_id TEXT PRIMARY KEY,
            edinet_code TEXT NOT NULL,
            security_code TEXT,
            form_type TEXT NOT NULL,
            period_end TEXT,
            submit_date TEXT,
            amendment_flag INTEGER NOT NULL DEFAULT 0,
            doc_info_edit_status TEXT,
            legal_status TEXT,
            accounting_standard TEXT,
            document_display_unit TEXT,
            zip_path TEXT,
            xbrl_path TEXT,
            download_status TEXT NOT NULL,
            parse_status TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE normalized_metrics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            doc_id TEXT NOT NULL,
            edinet_code TEXT NOT NULL,
            security_code TEXT,
            metric_key TEXT NOT NULL,
            fiscal_year INTEGER,
            period_end TEXT,
            value_num REAL,
            source_tag TEXT,
            consolidation TEXT,
            rule_version TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE derived_metrics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            doc_id TEXT NOT NULL,
            edinet_code TEXT NOT NULL,
            security_code TEXT,
            metric_key TEXT NOT NULL,
            metric_base TEXT NOT NULL,
            metric_group TEXT NOT NULL,
            fiscal_year INTEGER,
            period_end TEXT,
            period_scope TEXT NOT NULL,
            period_offset INTEGER NOT NULL DEFAULT 0,
            consolidation TEXT,
            accounting_standard TEXT,
            document_display_unit TEXT,
            value_num REAL,
            value_unit TEXT NOT NULL,
            calc_status TEXT NOT NULL,
            formula_name TEXT NOT NULL,
            source_detail_json TEXT,
            rule_version TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        """
    )


def _insert_company(
    conn: sqlite3.Connection,
    *,
    edinet_code: str,
    security_code: str,
    company_name: str,
    industry_33: str,
    net_sales_values: list[float],
) -> None:
    conn.execute(
        """
        INSERT INTO issuer_master (
            edinet_code, security_code, company_name, market, industry_33, industry_17,
            is_listed, exchange, listing_category_raw, listing_source, updated_at
        ) VALUES (?, ?, ?, 'Prime', ?, ?, 1, 'TSE', 'Prime', 'csv', '2026-04-24')
        """,
        (edinet_code, f"{security_code}0", company_name, industry_33, industry_33),
    )
    period_ends = ["2026-03-31", "2025-03-31", "2024-03-31"]
    for offset, period_end in enumerate(period_ends):
        doc_id = f"{edinet_code}_{offset}"
        conn.execute(
            """
            INSERT INTO filings (
                doc_id, edinet_code, security_code, form_type, period_end, submit_date,
                amendment_flag, doc_info_edit_status, legal_status, accounting_standard,
                document_display_unit, zip_path, xbrl_path, download_status, parse_status,
                created_at, updated_at
            ) VALUES (?, ?, ?, '030000', ?, ?, 0, '0', '1', 'Japan GAAP', '百万円',
                      'zip', 'xbrl', 'downloaded', 'derived_metrics_saved',
                      '2026-04-24', '2026-04-24')
            """,
            (doc_id, edinet_code, f"{security_code}0", period_end, f"{period_end} 12:00"),
        )
        conn.executemany(
            """
            INSERT INTO normalized_metrics (
                doc_id, edinet_code, security_code, metric_key, fiscal_year, period_end,
                value_num, source_tag, consolidation, rule_version, created_at, updated_at
            ) VALUES (?, ?, ?, ?, 2025, ?, ?, 'tag', 'consolidated', 'v1', '2026-04-24', '2026-04-24')
            """,
            [
                (doc_id, edinet_code, f"{security_code}0", "NetSalesCurrent", period_end, net_sales_values[offset]),
                (doc_id, edinet_code, f"{security_code}0", "CostOfSalesCurrent", period_end, 60_000_000.0),
                (doc_id, edinet_code, f"{security_code}0", "NetAssetsCurrent", period_end, 50_000_000.0),
            ],
        )
        conn.executemany(
            """
            INSERT INTO derived_metrics (
                doc_id, edinet_code, security_code, metric_key, metric_base, metric_group,
                fiscal_year, period_end, period_scope, period_offset, consolidation,
                accounting_standard, document_display_unit, value_num, value_unit, calc_status,
                formula_name, source_detail_json, rule_version, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, 'profitability', 2025, ?, 'annual', 0, 'consolidated',
                      'Japan GAAP', '百万円', ?, 'ratio', 'ok',
                      'test', '{}', 'v1', '2026-04-24', '2026-04-24')
            """,
            [
                (doc_id, edinet_code, f"{security_code}0", "CostOfSalesRatioCurrent", "CostOfSalesRatio", period_end, 0.6),
                (doc_id, edinet_code, f"{security_code}0", "EquityRatioCurrent", "EquityRatio", period_end, 0.5),
            ],
        )


class MetricExcelExportServiceTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp_path = TMP_ROOT / uuid.uuid4().hex
        self.tmp_path.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        _create_schema(self.conn)
        _insert_company(
            self.conn,
            edinet_code="E00001",
            security_code="1111",
            company_name="A社",
            industry_33="化学",
            net_sales_values=[100_000_000.0, 80_000_000.0, 70_000_000.0],
        )
        _insert_company(
            self.conn,
            edinet_code="E00002",
            security_code="2222",
            company_name="B社",
            industry_33="化学",
            net_sales_values=[70_000_000.0, 80_000_000.0, 100_000_000.0],
        )
        _insert_company(
            self.conn,
            edinet_code="E00003",
            security_code="3333",
            company_name="C銀行",
            industry_33="銀行業",
            net_sales_values=[100_000_000.0, 80_000_000.0, 70_000_000.0],
        )
        _insert_company(
            self.conn,
            edinet_code="E00004",
            security_code="4444",
            company_name="D証券",
            industry_33="証券、商品先物取引業",
            net_sales_values=[100_000_000.0, 80_000_000.0, 70_000_000.0],
        )
        _insert_company(
            self.conn,
            edinet_code="E00005",
            security_code="5555",
            company_name="E保険",
            industry_33="保険業",
            net_sales_values=[100_000_000.0, 80_000_000.0, 70_000_000.0],
        )
        self.conn.commit()

    def tearDown(self) -> None:
        self.conn.close()
        shutil.rmtree(self.tmp_path, ignore_errors=True)

    def test_read_metric_excel_condition_parses_all_and_period_range(self) -> None:
        path = self.tmp_path / "condition.xlsx"
        _create_condition_workbook(
            path,
            [
                ("業種", ""),
                ("証券コード", "1111,2222"),
                ("指標", "売上高,売上原価率"),
                ("期間", "2年前-当期"),
                ("増減判定", "increase"),
                ("増減判定指標", "売上高"),
            ],
        )

        condition = read_metric_excel_condition(path)

        self.assertEqual(condition.industries, [])
        self.assertEqual(condition.security_codes, ["1111", "2222"])
        self.assertEqual(condition.metric_labels, ["売上高", "売上原価率"])
        self.assertEqual(condition.period_offsets, [2, 1, 0])
        self.assertEqual(condition.trend, "increase")

    def test_read_metric_excel_condition_accepts_securities_industry_alias(self) -> None:
        path = self.tmp_path / "condition.xlsx"
        _create_condition_workbook(
            path,
            [
                ("業種", "証券・商品先物取引業"),
                ("指標", "売上高"),
            ],
        )

        condition = read_metric_excel_condition(path)

        self.assertEqual(condition.industries, ["証券、商品先物取引業"])

    def test_read_metric_excel_condition_keeps_official_securities_industry_name(self) -> None:
        path = self.tmp_path / "condition.xlsx"
        _create_condition_workbook(
            path,
            [
                ("業種", "証券、商品先物取引業"),
                ("指標", "売上高"),
            ],
        )

        condition = read_metric_excel_condition(path)

        self.assertEqual(condition.industries, ["証券、商品先物取引業"])

    def test_build_rows_absorbs_ratio_metrics_into_ratio_columns(self) -> None:
        path = self.tmp_path / "condition.xlsx"
        _create_condition_workbook(
            path,
            [
                ("証券コード", "1111"),
                ("指標", "売上高,売上原価率,自己資本比率"),
                ("期間", "2年前-当期"),
            ],
        )
        condition = read_metric_excel_condition(path)

        rows, errors, _warnings, _preview, target_companies = build_metric_excel_rows(
            self.conn,
            condition,
        )

        self.assertEqual(errors, [])
        self.assertEqual(target_companies, 1)
        by_metric = {row.metric_base: row for row in rows}
        self.assertEqual(set(by_metric), {"NetSales", "CostOfSales", "NetAssets"})
        self.assertEqual(by_metric["NetSales"].values_by_offset[0], 100.0)
        self.assertEqual(by_metric["NetSales"].ratios_by_offset[0], 1.0)
        self.assertEqual(by_metric["CostOfSales"].ratios_by_offset[0], 0.6)
        self.assertEqual(by_metric["NetAssets"].ratios_by_offset[0], 0.5)

    def test_trend_filter_keeps_only_continuous_increase_companies(self) -> None:
        path = self.tmp_path / "condition.xlsx"
        _create_condition_workbook(
            path,
            [
                ("業種", "化学"),
                ("指標", "売上高"),
                ("期間", "2年前-当期"),
                ("増減判定", "increase"),
                ("増減判定指標", "売上高"),
                ("増減判定期間", "2年前-当期"),
            ],
        )
        condition = read_metric_excel_condition(path)

        rows, errors, _warnings, _preview, target_companies = build_metric_excel_rows(
            self.conn,
            condition,
        )

        self.assertEqual(errors, [])
        self.assertEqual(target_companies, 1)
        self.assertEqual({row.security_code for row in rows}, {"1111"})

    def test_build_rows_uses_fixed_metric_order(self) -> None:
        path = self.tmp_path / "condition.xlsx"
        _create_condition_workbook(
            path,
            [
                ("証券コード", "1111"),
                ("指標", "純資産,売上原価率,売上高"),
                ("期間", "当期"),
            ],
        )
        condition = read_metric_excel_condition(path)

        rows, errors, _warnings, _preview, _target_companies = build_metric_excel_rows(
            self.conn,
            condition,
        )

        self.assertEqual(errors, [])
        self.assertEqual([row.metric_base for row in rows], ["NetSales", "CostOfSales", "NetAssets"])

    def test_build_rows_uses_industry_specific_labels_and_order(self) -> None:
        cases = [
            (
                "銀行業",
                "3333",
                ["売上高", "費用合計", "├資金調達費用", "└営業経費", "役務取引等収益"],
            ),
            (
                "証券、商品先物取引業",
                "4444",
                ["売上高", "純収益", "費用合計", "├金融費用", "└販管費", "　├ 一般管理費", "　└ 販売費"],
            ),
            (
                "保険業",
                "5555",
                ["売上高", "売上総利益", "費用合計", "├保険金等支払金", "├責任準備金等繰入額", "├資産運用費用", "└ 事業費"],
            ),
        ]
        for industry, security_code, expected_labels in cases:
            with self.subTest(industry=industry):
                path = self.tmp_path / f"{security_code}.xlsx"
                _create_condition_workbook(
                    path,
                    [
                        ("業種", industry),
                        ("証券コード", security_code),
                        ("指標", "ALL"),
                        ("期間", "当期"),
                    ],
                )
                condition = read_metric_excel_condition(path)

                rows, errors, _warnings, _preview, _target_companies = build_metric_excel_rows(
                    self.conn,
                    condition,
                )

                self.assertEqual(errors, [])
                self.assertEqual(
                    [row.metric_label for row in rows[: len(expected_labels)]],
                    expected_labels,
                )

    def test_cash_balance_metric_label_is_registered(self) -> None:
        path = self.tmp_path / "condition.xlsx"
        _create_condition_workbook(
            path,
            [
                ("証券コード", "1111"),
                ("指標", "期末残"),
                ("期間", "当期"),
            ],
        )
        condition = read_metric_excel_condition(path)

        rows, errors, _warnings, _preview, _target_companies = build_metric_excel_rows(
            self.conn,
            condition,
        )

        self.assertEqual(errors, [])
        self.assertEqual([row.metric_base for row in rows], ["CashAndCashEquivalents"])
        self.assertEqual(rows[0].metric_label, "期末残")

    def test_eps_growth_metric_label_uses_prior_period_suffix(self) -> None:
        path = self.tmp_path / "condition.xlsx"
        _create_condition_workbook(
            path,
            [
                ("証券コード", "1111"),
                ("指標", "EPS増加率"),
                ("期間", "当期"),
            ],
        )
        condition = read_metric_excel_condition(path)

        rows, errors, _warnings, _preview, _target_companies = build_metric_excel_rows(
            self.conn,
            condition,
        )

        self.assertEqual(errors, [])
        self.assertEqual([row.metric_base for row in rows], ["EPSGrowthRate"])
        self.assertEqual(rows[0].metric_label, "EPS増加率（前期比）")

    def test_export_metric_excel_writes_percent_format(self) -> None:
        condition_path = self.tmp_path / "condition.xlsx"
        output_path = self.tmp_path / "export.xlsx"
        _create_condition_workbook(
            condition_path,
            [
                ("証券コード", "1111"),
                ("指標", "売上原価率"),
                ("期間", "当期"),
            ],
        )

        result = export_metric_excel(
            self.conn,
            condition_xlsx=condition_path,
            output_path=output_path,
            db_path=":memory:",
        )

        self.assertEqual(result.errors, [])
        workbook = load_workbook(output_path)
        ws = workbook[GENERAL_SHEET]
        self.assertEqual(ws["E2"].value, "├売上原価")
        self.assertEqual(ws["F2"].value, 60.0)
        self.assertEqual(ws["G2"].value, 0.6)
        self.assertEqual(ws["G2"].number_format, "0.0%")

    def test_export_metric_excel_formats_growth_rates_as_percent(self) -> None:
        self.conn.execute(
            """
            INSERT INTO derived_metrics (
                doc_id, edinet_code, security_code, metric_key, metric_base, metric_group,
                fiscal_year, period_end, period_scope, period_offset, consolidation,
                accounting_standard, document_display_unit, value_num, value_unit, calc_status,
                formula_name, source_detail_json, rule_version, created_at, updated_at
            ) VALUES (
                'E00001_0', 'E00001', '11110', 'NetSalesGrowthRateCurrent',
                'NetSalesGrowthRate', 'growth', 2025, '2026-03-31', 'annual',
                0, 'consolidated', 'Japan GAAP', '百万円', 1.25, 'ratio',
                'ok', 'test', '{}', 'v1', '2026-04-24', '2026-04-24'
            )
            """
        )
        self.conn.commit()
        condition_path = self.tmp_path / "condition.xlsx"
        output_path = self.tmp_path / "growth.xlsx"
        _create_condition_workbook(
            condition_path,
            [
                ("証券コード", "1111"),
                ("指標", "売上高増収率"),
                ("期間", "当期"),
            ],
        )

        result = export_metric_excel(
            self.conn,
            condition_xlsx=condition_path,
            output_path=output_path,
            db_path=":memory:",
        )

        self.assertEqual(result.errors, [])
        workbook = load_workbook(output_path)
        ws = workbook[GENERAL_SHEET]
        self.assertEqual(ws["E2"].value, "売上高増収率")
        self.assertEqual(ws["F2"].value, 1.25)
        self.assertEqual(ws["F2"].number_format, "0.0%")

    def test_export_metric_excel_formats_per_share_and_theoretical_values(self) -> None:
        self.conn.executemany(
            """
            INSERT INTO derived_metrics (
                doc_id, edinet_code, security_code, metric_key, metric_base, metric_group,
                fiscal_year, period_end, period_scope, period_offset, consolidation,
                accounting_standard, document_display_unit, value_num, value_unit, calc_status,
                formula_name, source_detail_json, rule_version, created_at, updated_at
            ) VALUES (
                'E00001_0', 'E00001', '11110', ?, ?, 'valuation', 2025,
                '2026-03-31', 'annual', 0, 'consolidated', 'Japan GAAP',
                '百万円', ?, 'number', 'ok', 'test', '{}', 'v1',
                '2026-04-24', '2026-04-24'
            )
            """,
            [
                ("EPSCurrent", "EPS", 123.456),
                ("BPSCurrent", "BPS", 234.567),
                ("TheoreticalPBRCurrent", "TheoreticalPBR", 1.234),
                ("AssetsPerShareCurrent", "AssetsPerShare", 3456.789),
                ("TheoreticalSharePriceCurrent", "TheoreticalSharePrice", 4567.89),
            ],
        )
        self.conn.commit()
        condition_path = self.tmp_path / "condition.xlsx"
        output_path = self.tmp_path / "formats.xlsx"
        _create_condition_workbook(
            condition_path,
            [
                ("証券コード", "1111"),
                ("指標", "EPS,BPS,理論PBR,1株資産,理論株価"),
                ("期間", "当期"),
            ],
        )

        result = export_metric_excel(
            self.conn,
            condition_xlsx=condition_path,
            output_path=output_path,
            db_path=":memory:",
        )

        self.assertEqual(result.errors, [])
        workbook = load_workbook(output_path)
        ws = workbook[GENERAL_SHEET]
        formats_by_label = {
            ws.cell(row=row_index, column=5).value: ws.cell(row=row_index, column=6).number_format
            for row_index in range(2, ws.max_row + 1)
        }
        self.assertEqual(formats_by_label["EPS"], "#,##0.0")
        self.assertEqual(formats_by_label["BPS"], "#,##0.0")
        self.assertEqual(formats_by_label["理論PBR"], "#,##0.0")
        self.assertEqual(formats_by_label["1株資産"], "#,##0")
        self.assertEqual(formats_by_label["理論株価"], "#,##0")


if __name__ == "__main__":
    unittest.main()
