from __future__ import annotations

import sys
import unittest
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from edinet_monitor.services.derived_metrics.derived_metric_service import (  # noqa: E402
    calculate_derived_metrics,
    scale_value_for_display,
)


def build_normalized_row(metric_key: str, value_num: float, *, source_tag: str = "DummyTag") -> dict:
    return {
        "doc_id": "S100TEST",
        "edinet_code": "E00001",
        "security_code": "12340",
        "metric_key": metric_key,
        "fiscal_year": 2026,
        "period_end": "2026-03-31",
        "value_num": value_num,
        "source_tag": source_tag,
        "consolidation": "Consolidated",
        "rule_version": "v1",
    }


class DerivedMetricServiceTest(unittest.TestCase):
    def test_scale_value_for_display_uses_document_unit(self) -> None:
        self.assertEqual(
            scale_value_for_display(1_000_000, value_unit="yen", document_display_unit="百万円"),
            1,
        )
        self.assertEqual(
            scale_value_for_display(1_000_000, value_unit="yen", document_display_unit="千円"),
            1000,
        )

    def test_calculate_derived_metrics_builds_expected_rows(self) -> None:
        normalized_rows = [
            build_normalized_row("NetSalesCurrent", 1_200_000),
            build_normalized_row("NetSalesPrior1", 1_000_000),
            build_normalized_row("NetSalesPrior2", 800_000),
            build_normalized_row("NetSalesPrior3", 700_000),
            build_normalized_row("NetSalesPrior4", 600_000),
            build_normalized_row("OrdinaryIncomeCurrent", 240_000),
            build_normalized_row("OrdinaryIncomePrior1", 200_000),
            build_normalized_row("OrdinaryIncomePrior2", 160_000),
            build_normalized_row("OrdinaryIncomePrior3", 140_000),
            build_normalized_row("OrdinaryIncomePrior4", 120_000),
            build_normalized_row("CostOfSalesCurrent", 500_000, source_tag="CostOfSales"),
            build_normalized_row("CostOfSalesPrior1", 420_000, source_tag="CostOfSales"),
            build_normalized_row("CostOfSalesPrior2", 350_000, source_tag="CostOfSales"),
            build_normalized_row("CostOfSalesPrior3", 300_000, source_tag="CostOfSales"),
            build_normalized_row("CostOfSalesPrior4", 260_000, source_tag="CostOfSales"),
            build_normalized_row("SellingExpensesCurrent", 300_000, source_tag="SellingGeneralAndAdministrativeExpenses"),
            build_normalized_row("SellingExpensesPrior1", 260_000, source_tag="SellingGeneralAndAdministrativeExpenses"),
            build_normalized_row("SellingExpensesPrior2", 220_000, source_tag="SellingGeneralAndAdministrativeExpenses"),
            build_normalized_row("SellingExpensesPrior3", 200_000, source_tag="SellingGeneralAndAdministrativeExpenses"),
            build_normalized_row("SellingExpensesPrior4", 180_000, source_tag="SellingGeneralAndAdministrativeExpenses"),
            build_normalized_row("OperatingIncomeCurrent", 180_000),
            build_normalized_row("OperatingIncomePrior1", 150_000),
            build_normalized_row("OperatingIncomePrior2", 120_000),
            build_normalized_row("OperatingIncomePrior3", 100_000),
            build_normalized_row("OperatingIncomePrior4", 90_000),
            build_normalized_row("CashAndCashEquivalentsCurrent", 300_000),
            build_normalized_row("CashAndCashEquivalentsPrior1", 250_000),
            build_normalized_row("CashAndCashEquivalentsPrior2", 200_000),
            build_normalized_row("CashAndCashEquivalentsPrior3", 160_000),
            build_normalized_row("CashAndCashEquivalentsPrior4", 140_000),
            build_normalized_row("IssuedSharesCurrent", 1_000_000),
            build_normalized_row("IssuedSharesPrior1", 1_000_000),
            build_normalized_row("IssuedSharesPrior2", 1_000_000),
            build_normalized_row("IssuedSharesPrior3", 980_000),
            build_normalized_row("IssuedSharesPrior4", 960_000),
            build_normalized_row("TreasurySharesCurrent", 50_000),
            build_normalized_row("TreasurySharesPrior1", 52_000),
            build_normalized_row("TreasurySharesPrior2", 53_000),
            build_normalized_row("TreasurySharesPrior3", 48_000),
            build_normalized_row("TreasurySharesPrior4", 40_000),
            build_normalized_row("OperatingCashCurrent", 90_000),
            build_normalized_row("OperatingCashPrior1", 80_000),
            build_normalized_row("OperatingCashPrior2", 70_000),
            build_normalized_row("OperatingCashPrior3", 60_000),
            build_normalized_row("OperatingCashPrior4", 50_000),
            build_normalized_row("InvestmentCashCurrent", -20_000),
            build_normalized_row("InvestmentCashPrior1", -18_000),
            build_normalized_row("InvestmentCashPrior2", -16_000),
            build_normalized_row("InvestmentCashPrior3", -14_000),
            build_normalized_row("InvestmentCashPrior4", -12_000),
            build_normalized_row("TotalAssetsCurrent", 2_000_000),
            build_normalized_row("TotalAssetsPrior1", 1_800_000),
            build_normalized_row("TotalAssetsPrior2", 1_600_000),
            build_normalized_row("TotalAssetsPrior3", 1_500_000),
            build_normalized_row("TotalAssetsPrior4", 1_400_000),
            build_normalized_row("NetAssetsCurrent", 1_000_000),
            build_normalized_row("NetAssetsPrior1", 900_000),
            build_normalized_row("NetAssetsPrior2", 820_000),
            build_normalized_row("NetAssetsPrior3", 760_000),
            build_normalized_row("NetAssetsPrior4", 700_000),
        ]

        rows = calculate_derived_metrics(
            normalized_rows,
            form_type="030000",
            accounting_standard="ifrs",
            document_display_unit="百万円",
        )
        by_key = {row["metric_key"]: row for row in rows}

        self.assertEqual(by_key["NetSalesGrowthRateCurrent"]["value_num"], 1.2)
        self.assertEqual(by_key["GrossProfitCurrent"]["value_num"], 700_000)
        self.assertEqual(
            by_key["GrossProfitCurrent"]["source_detail_json"]["selected_source"],
            "net_sales_minus_cost_of_sales",
        )
        self.assertEqual(
            by_key["GrossProfitCurrent"]["source_detail_json"]["calculated_value"],
            700_000,
        )
        self.assertEqual(by_key["CostOfSalesRatioCurrent"]["value_num"], 500_000 / 1_200_000)
        self.assertEqual(by_key["EstimatedNetIncomeCurrent"]["value_num"], 168_000)
        self.assertEqual(by_key["EquityRatioCurrent"]["value_num"], 0.5)
        self.assertEqual(by_key["FCFCurrent"]["value_num"], 70_000)
        self.assertEqual(by_key["OutstandingSharesCurrent"]["value_num"], 950_000)
        self.assertEqual(by_key["OutstandingSharesCurrent"]["metric_group"], "share")
        self.assertEqual(by_key["OutstandingSharesCurrent"]["value_unit"], "shares")
        self.assertEqual(by_key["GrossProfitCurrent"]["document_display_unit"], "百万円")

    def test_combined_cost_and_sga_sums_cost_and_selling_expenses(self) -> None:
        normalized_rows = [
            build_normalized_row("NetSalesCurrent", 1_200_000),
            build_normalized_row("CostOfSalesCurrent", 500_000, source_tag="CostOfSales"),
            build_normalized_row("SellingExpensesCurrent", 300_000, source_tag="SellingGeneralAndAdministrativeExpenses"),
            build_normalized_row("OrdinaryIncomeCurrent", 240_000),
            build_normalized_row("CashAndCashEquivalentsCurrent", 300_000),
            build_normalized_row("IssuedSharesCurrent", 1_000_000),
            build_normalized_row("OperatingCashCurrent", 90_000),
            build_normalized_row("InvestmentCashCurrent", -20_000),
            build_normalized_row("TotalAssetsCurrent", 2_000_000),
            build_normalized_row("NetAssetsCurrent", 1_000_000),
        ]

        rows = calculate_derived_metrics(
            normalized_rows,
            form_type="030000",
            accounting_standard="ifrs",
            document_display_unit="逋ｾ荳・・",
        )
        by_key = {row["metric_key"]: row for row in rows}

        self.assertEqual(
            by_key["CostOfSalesAndSellingGeneralAndAdministrativeExpensesCurrent"]["value_num"],
            800_000,
        )
        self.assertEqual(
            by_key["CostOfSalesAndSellingGeneralAndAdministrativeExpensesCurrent"]["source_detail_json"]["selected_source"],
            "cost_of_sales_plus_selling_expenses",
        )

    def test_gross_profit_prefers_tag_and_records_difference(self) -> None:
        normalized_rows = [
            build_normalized_row("NetSalesCurrent", 1_200_000),
            build_normalized_row("CostOfSalesCurrent", 500_000, source_tag="CostOfSales"),
            build_normalized_row("GrossProfitCurrent", 710_000, source_tag="GrossProfit"),
            build_normalized_row("OrdinaryIncomeCurrent", 240_000),
            build_normalized_row("CashAndCashEquivalentsCurrent", 300_000),
            build_normalized_row("IssuedSharesCurrent", 1_000_000),
            build_normalized_row("OperatingCashCurrent", 90_000),
            build_normalized_row("InvestmentCashCurrent", -20_000),
            build_normalized_row("TotalAssetsCurrent", 2_000_000),
            build_normalized_row("NetAssetsCurrent", 1_000_000),
        ]

        rows = calculate_derived_metrics(
            normalized_rows,
            form_type="030000",
            accounting_standard="ifrs",
            document_display_unit="逋ｾ荳・・",
        )
        by_key = {row["metric_key"]: row for row in rows}

        self.assertEqual(by_key["GrossProfitCurrent"]["value_num"], 710_000)
        self.assertEqual(
            by_key["GrossProfitCurrent"]["source_detail_json"]["selected_source"],
            "gross_profit_tag",
        )
        self.assertEqual(
            by_key["GrossProfitCurrent"]["source_detail_json"]["tag_value"],
            710_000,
        )
        self.assertEqual(
            by_key["GrossProfitCurrent"]["source_detail_json"]["calculated_value"],
            700_000,
        )
        self.assertEqual(
            by_key["GrossProfitCurrent"]["source_detail_json"]["difference_tag_minus_calculated"],
            10_000,
        )
        self.assertEqual(
            by_key["GrossProfitMarginCurrent"]["value_num"],
            710_000 / 1_200_000,
        )
        self.assertEqual(
            by_key["GrossProfitMarginCurrent"]["source_detail_json"]["numerator_detail"]["selected_source"],
            "gross_profit_tag",
        )

    def test_gross_profit_does_not_calculate_with_unsafe_cost_source(self) -> None:
        normalized_rows = [
            build_normalized_row("NetSalesCurrent", 1_200_000),
            build_normalized_row("CostOfSalesCurrent", 500_000, source_tag="OrdinaryExpensesBNK"),
            build_normalized_row("OrdinaryIncomeCurrent", 240_000),
            build_normalized_row("CashAndCashEquivalentsCurrent", 300_000),
            build_normalized_row("IssuedSharesCurrent", 1_000_000),
            build_normalized_row("OperatingCashCurrent", 90_000),
            build_normalized_row("InvestmentCashCurrent", -20_000),
            build_normalized_row("TotalAssetsCurrent", 2_000_000),
            build_normalized_row("NetAssetsCurrent", 1_000_000),
        ]

        rows = calculate_derived_metrics(
            normalized_rows,
            form_type="030000",
            accounting_standard="jpgaap",
            document_display_unit="逋ｾ荳・・",
        )
        by_key = {row["metric_key"]: row for row in rows}

        self.assertIsNone(by_key["GrossProfitCurrent"]["value_num"])
        self.assertEqual(by_key["GrossProfitCurrent"]["calc_status"], "missing_input")
        self.assertEqual(
            by_key["GrossProfitCurrent"]["source_detail_json"]["missing_reason"],
            "unsafe_cost_of_sales_source_tag",
        )

    def test_gross_profit_calculates_with_operating_cost_source(self) -> None:
        normalized_rows = [
            build_normalized_row("NetSalesCurrent", 1_200_000, source_tag="RevenueIFRSSummaryOfBusinessResults"),
            build_normalized_row("CostOfSalesCurrent", 500_000, source_tag="OperatingCost"),
            build_normalized_row("OrdinaryIncomeCurrent", 240_000),
            build_normalized_row("CashAndCashEquivalentsCurrent", 300_000),
            build_normalized_row("IssuedSharesCurrent", 1_000_000),
            build_normalized_row("OperatingCashCurrent", 90_000),
            build_normalized_row("InvestmentCashCurrent", -20_000),
            build_normalized_row("TotalAssetsCurrent", 2_000_000),
            build_normalized_row("NetAssetsCurrent", 1_000_000),
        ]

        rows = calculate_derived_metrics(
            normalized_rows,
            form_type="030000",
            accounting_standard="ifrs",
            document_display_unit="百万円",
        )
        by_key = {row["metric_key"]: row for row in rows}

        self.assertEqual(by_key["GrossProfitCurrent"]["value_num"], 700_000)
        self.assertEqual(by_key["GrossProfitCurrent"]["calc_status"], "ok")
        self.assertEqual(
            by_key["GrossProfitCurrent"]["source_detail_json"]["selected_source"],
            "net_sales_minus_cost_of_sales",
        )
        self.assertEqual(
            by_key["GrossProfitCurrent"]["source_detail_json"]["cost_of_sales_source_tag"],
            "OperatingCost",
        )

    def test_gross_profit_calculates_with_completed_work_cost_source(self) -> None:
        normalized_rows = [
            build_normalized_row("NetSalesCurrent", 1_200_000, source_tag="BusinessRevenueSummaryOfBusinessResults"),
            build_normalized_row("CostOfSalesCurrent", 500_000, source_tag="CostOfCompletedWorkCOSExpOA"),
            build_normalized_row("OrdinaryIncomeCurrent", 240_000),
            build_normalized_row("CashAndCashEquivalentsCurrent", 300_000),
            build_normalized_row("IssuedSharesCurrent", 1_000_000),
            build_normalized_row("OperatingCashCurrent", 90_000),
            build_normalized_row("InvestmentCashCurrent", -20_000),
            build_normalized_row("TotalAssetsCurrent", 2_000_000),
            build_normalized_row("NetAssetsCurrent", 1_000_000),
        ]

        rows = calculate_derived_metrics(
            normalized_rows,
            form_type="030000",
            accounting_standard="jpgaap",
            document_display_unit="逋ｾ荳・・",
        )
        by_key = {row["metric_key"]: row for row in rows}

        self.assertEqual(by_key["GrossProfitCurrent"]["value_num"], 700_000)
        self.assertEqual(by_key["GrossProfitCurrent"]["calc_status"], "ok")
        self.assertEqual(
            by_key["GrossProfitCurrent"]["source_detail_json"]["cost_of_sales_source_tag"],
            "CostOfCompletedWorkCOSExpOA",
        )

    def test_combined_cost_and_sga_prefers_tag_and_records_difference(self) -> None:
        normalized_rows = [
            build_normalized_row("CostOfSalesCurrent", 500_000, source_tag="CostOfSales"),
            build_normalized_row("SellingExpensesCurrent", 300_000, source_tag="SellingGeneralAndAdministrativeExpenses"),
            build_normalized_row(
                "CostOfSalesAndSellingGeneralAndAdministrativeExpensesCurrent",
                820_000,
                source_tag="CostOfSalesAndSellingGeneralAndAdministrativeExpensesIFRS",
            ),
            build_normalized_row("NetSalesCurrent", 1_200_000),
            build_normalized_row("OrdinaryIncomeCurrent", 240_000),
            build_normalized_row("CashAndCashEquivalentsCurrent", 300_000),
            build_normalized_row("IssuedSharesCurrent", 1_000_000),
            build_normalized_row("OperatingCashCurrent", 90_000),
            build_normalized_row("InvestmentCashCurrent", -20_000),
            build_normalized_row("TotalAssetsCurrent", 2_000_000),
            build_normalized_row("NetAssetsCurrent", 1_000_000),
        ]

        rows = calculate_derived_metrics(
            normalized_rows,
            form_type="030000",
            accounting_standard="ifrs",
            document_display_unit="逋ｾ荳・・",
        )
        by_key = {row["metric_key"]: row for row in rows}

        self.assertEqual(
            by_key["CostOfSalesAndSellingGeneralAndAdministrativeExpensesCurrent"]["value_num"],
            820_000,
        )
        self.assertEqual(
            by_key["CostOfSalesAndSellingGeneralAndAdministrativeExpensesCurrent"]["source_detail_json"]["selected_source"],
            "combined_expense_tag",
        )
        self.assertEqual(
            by_key["CostOfSalesAndSellingGeneralAndAdministrativeExpensesCurrent"]["source_detail_json"]["difference_tag_minus_calculated"],
            20_000,
        )

    def test_combined_cost_and_sga_accepts_operating_expenses_ifrs_tag(self) -> None:
        normalized_rows = [
            build_normalized_row(
                "CostOfSalesAndSellingGeneralAndAdministrativeExpensesCurrent",
                820_000,
                source_tag="OperatingExpensesIFRS",
            ),
            build_normalized_row("CostOfSalesCurrent", 500_000, source_tag="CostOfSales"),
            build_normalized_row("SellingExpensesCurrent", 300_000, source_tag="SellingGeneralAndAdministrativeExpenses"),
            build_normalized_row("NetSalesCurrent", 1_200_000),
            build_normalized_row("OrdinaryIncomeCurrent", 240_000),
            build_normalized_row("CashAndCashEquivalentsCurrent", 300_000),
            build_normalized_row("IssuedSharesCurrent", 1_000_000),
            build_normalized_row("OperatingCashCurrent", 90_000),
            build_normalized_row("InvestmentCashCurrent", -20_000),
            build_normalized_row("TotalAssetsCurrent", 2_000_000),
            build_normalized_row("NetAssetsCurrent", 1_000_000),
        ]

        rows = calculate_derived_metrics(
            normalized_rows,
            form_type="030000",
            accounting_standard="ifrs",
            document_display_unit="逋ｾ荳・・",
        )
        by_key = {row["metric_key"]: row for row in rows}

        self.assertEqual(
            by_key["CostOfSalesAndSellingGeneralAndAdministrativeExpensesCurrent"]["value_num"],
            820_000,
        )
        self.assertEqual(
            by_key["CostOfSalesAndSellingGeneralAndAdministrativeExpensesCurrent"]["source_detail_json"]["selected_source"],
            "combined_expense_tag",
        )

    def test_outstanding_shares_treats_missing_treasury_as_zero(self) -> None:
        normalized_rows = [
            build_normalized_row("IssuedSharesCurrent", 10_000),
            build_normalized_row("NetSalesCurrent", 1_200_000),
            build_normalized_row("OrdinaryIncomeCurrent", 100_000),
            build_normalized_row("CostOfSalesCurrent", 400_000),
            build_normalized_row("SellingExpensesCurrent", 200_000),
            build_normalized_row("OperatingIncomeCurrent", 150_000),
            build_normalized_row("CashAndCashEquivalentsCurrent", 100_000),
            build_normalized_row("OperatingCashCurrent", 70_000),
            build_normalized_row("InvestmentCashCurrent", -10_000),
            build_normalized_row("TotalAssetsCurrent", 1_500_000),
            build_normalized_row("NetAssetsCurrent", 700_000),
        ]

        rows = calculate_derived_metrics(
            normalized_rows,
            form_type="030000",
            accounting_standard="jpgaap",
            document_display_unit="千円",
        )
        by_key = {row["metric_key"]: row for row in rows}

        self.assertEqual(by_key["OutstandingSharesCurrent"]["value_num"], 10_000)
        self.assertEqual(by_key["OutstandingSharesCurrent"]["calc_status"], "ok")

    def test_outstanding_shares_treats_small_treasury_as_zero(self) -> None:
        normalized_rows = [
            build_normalized_row("IssuedSharesCurrent", 10_000),
            build_normalized_row("TreasurySharesCurrent", 100),
            build_normalized_row("NetSalesCurrent", 1_200_000),
            build_normalized_row("OrdinaryIncomeCurrent", 100_000),
            build_normalized_row("CostOfSalesCurrent", 400_000),
            build_normalized_row("SellingExpensesCurrent", 200_000),
            build_normalized_row("OperatingIncomeCurrent", 150_000),
            build_normalized_row("CashAndCashEquivalentsCurrent", 100_000),
            build_normalized_row("OperatingCashCurrent", 70_000),
            build_normalized_row("InvestmentCashCurrent", -10_000),
            build_normalized_row("TotalAssetsCurrent", 1_500_000),
            build_normalized_row("NetAssetsCurrent", 700_000),
        ]

        rows = calculate_derived_metrics(
            normalized_rows,
            form_type="030000",
            accounting_standard="jpgaap",
            document_display_unit="千円",
        )
        by_key = {row["metric_key"]: row for row in rows}

        self.assertEqual(by_key["OutstandingSharesCurrent"]["value_num"], 10_000)
        self.assertEqual(by_key["OutstandingSharesCurrent"]["calc_status"], "ok")

    def test_growth_rate_uses_null_when_prior_is_zero_or_negative(self) -> None:
        normalized_rows = [
            build_normalized_row("NetSalesCurrent", 1_200_000),
            build_normalized_row("NetSalesPrior1", 0),
            build_normalized_row("NetSalesPrior2", 800_000),
            build_normalized_row("NetSalesPrior3", 700_000),
            build_normalized_row("NetSalesPrior4", 600_000),
            build_normalized_row("OrdinaryIncomeCurrent", 100_000),
            build_normalized_row("OrdinaryIncomePrior1", 90_000),
            build_normalized_row("OrdinaryIncomePrior2", 80_000),
            build_normalized_row("OrdinaryIncomePrior3", 70_000),
            build_normalized_row("OrdinaryIncomePrior4", 60_000),
            build_normalized_row("CashAndCashEquivalentsCurrent", 100_000),
            build_normalized_row("CashAndCashEquivalentsPrior1", 80_000),
            build_normalized_row("CashAndCashEquivalentsPrior2", 60_000),
            build_normalized_row("CashAndCashEquivalentsPrior3", 50_000),
            build_normalized_row("CashAndCashEquivalentsPrior4", 40_000),
            build_normalized_row("IssuedSharesCurrent", 1_000_000),
            build_normalized_row("CostOfSalesCurrent", 400_000),
            build_normalized_row("SellingExpensesCurrent", 200_000),
            build_normalized_row("OperatingIncomeCurrent", 150_000),
            build_normalized_row("OperatingCashCurrent", 70_000),
            build_normalized_row("InvestmentCashCurrent", -10_000),
            build_normalized_row("TotalAssetsCurrent", 1_500_000),
            build_normalized_row("NetAssetsCurrent", 700_000),
        ]

        rows = calculate_derived_metrics(
            normalized_rows,
            form_type="030000",
            accounting_standard="jpgaap",
            document_display_unit="千円",
        )
        by_key = {row["metric_key"]: row for row in rows}

        self.assertIsNone(by_key["NetSalesGrowthRateCurrent"]["value_num"])
        self.assertEqual(
            by_key["NetSalesGrowthRateCurrent"]["calc_status"],
            "zero_or_negative_base",
        )
        self.assertEqual(by_key["OutstandingSharesCurrent"]["value_num"], 1_000_000)
        self.assertEqual(by_key["OutstandingSharesCurrent"]["calc_status"], "ok")

    def test_outstanding_shares_requires_issued_shares(self) -> None:
        normalized_rows = [
            build_normalized_row("TreasurySharesCurrent", 100),
            build_normalized_row("NetSalesCurrent", 1_200_000),
            build_normalized_row("OrdinaryIncomeCurrent", 100_000),
            build_normalized_row("CostOfSalesCurrent", 400_000),
            build_normalized_row("SellingExpensesCurrent", 200_000),
            build_normalized_row("OperatingIncomeCurrent", 150_000),
            build_normalized_row("CashAndCashEquivalentsCurrent", 100_000),
            build_normalized_row("OperatingCashCurrent", 70_000),
            build_normalized_row("InvestmentCashCurrent", -10_000),
            build_normalized_row("TotalAssetsCurrent", 1_500_000),
            build_normalized_row("NetAssetsCurrent", 700_000),
        ]

        rows = calculate_derived_metrics(
            normalized_rows,
            form_type="030000",
            accounting_standard="jpgaap",
            document_display_unit="千円",
        )
        by_key = {row["metric_key"]: row for row in rows}

        self.assertIsNone(by_key["OutstandingSharesCurrent"]["value_num"])
        self.assertEqual(by_key["OutstandingSharesCurrent"]["calc_status"], "missing_input")
