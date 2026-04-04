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


def build_normalized_row(metric_key: str, value_num: float) -> dict:
    return {
        "doc_id": "S100TEST",
        "edinet_code": "E00001",
        "security_code": "12340",
        "metric_key": metric_key,
        "fiscal_year": 2026,
        "period_end": "2026-03-31",
        "value_num": value_num,
        "source_tag": "DummyTag",
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
            build_normalized_row("CostOfSalesCurrent", 500_000),
            build_normalized_row("CostOfSalesPrior1", 420_000),
            build_normalized_row("CostOfSalesPrior2", 350_000),
            build_normalized_row("CostOfSalesPrior3", 300_000),
            build_normalized_row("CostOfSalesPrior4", 260_000),
            build_normalized_row("SellingExpensesCurrent", 300_000),
            build_normalized_row("SellingExpensesPrior1", 260_000),
            build_normalized_row("SellingExpensesPrior2", 220_000),
            build_normalized_row("SellingExpensesPrior3", 200_000),
            build_normalized_row("SellingExpensesPrior4", 180_000),
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
        self.assertEqual(by_key["CostOfSalesRatioCurrent"]["value_num"], 500_000 / 1_200_000)
        self.assertEqual(by_key["EstimatedNetIncomeCurrent"]["value_num"], 168_000)
        self.assertEqual(by_key["EquityRatioCurrent"]["value_num"], 0.5)
        self.assertEqual(by_key["FCFCurrent"]["value_num"], 70_000)
        self.assertEqual(by_key["GrossProfitCurrent"]["document_display_unit"], "百万円")

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
