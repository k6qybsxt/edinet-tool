from __future__ import annotations

import sys
import unittest
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from edinet_pipeline.domain.metric_labels import (  # noqa: E402
    metric_base_to_display_name,
    metric_group_to_display_name,
    metric_key_to_display_name,
    split_metric_key,
    tag_name_to_display_name,
)


class MetricLabelsTest(unittest.TestCase):
    def test_metric_base_to_display_name_returns_japanese_label(self) -> None:
        self.assertEqual(metric_base_to_display_name("NetSales"), "\u58f2\u4e0a\u9ad8")
        self.assertEqual(metric_base_to_display_name("CostOfSales"), "\u58f2\u4e0a\u539f\u4fa1")
        self.assertEqual(
            metric_base_to_display_name("CostOfSalesAndSellingGeneralAndAdministrativeExpenses"),
            "\u8cbb\u7528\u5408\u8a08",
        )
        self.assertEqual(metric_base_to_display_name("CashAndCashEquivalents"), "\u671f\u672b\u6b8b")
        self.assertEqual(metric_base_to_display_name("OutstandingShares"), "\u767a\u884c\u682a\u6570")
        self.assertEqual(metric_base_to_display_name("EPS"), "EPS")
        self.assertEqual(metric_base_to_display_name("EPSGrowthRate"), "\u0045\u0050\u0053\u5897\u52a0\u7387")
        self.assertEqual(metric_base_to_display_name("BPS"), "BPS")
        self.assertEqual(metric_base_to_display_name("AssetsPerShare"), "1\u682a\u8cc7\u7523")
        self.assertEqual(metric_base_to_display_name("LiabilitiesPerShare"), "1\u682a\u8ca0\u50b5")
        self.assertEqual(metric_base_to_display_name("OperatingCashPerShare"), "1\u682a\u55b6\u696dCF")
        self.assertEqual(metric_base_to_display_name("InvestmentCashPerShare"), "1\u682a\u6295\u8cc7CF")
        self.assertEqual(metric_base_to_display_name("FinancingCashPerShare"), "1\u682a\u8ca1\u52d9CF")
        self.assertEqual(metric_base_to_display_name("FCFPerShare"), "1\u682aFCF")
        self.assertEqual(
            metric_base_to_display_name("FinancialLeverageAdjustment"),
            "\u8ca1\u52d9\u30ec\u30d0\u30ec\u30c3\u30b8\u88dc\u6b63",
        )
        self.assertEqual(metric_base_to_display_name("AssetValue"), "\u8cc7\u7523\u4fa1\u5024")
        self.assertEqual(metric_base_to_display_name("BusinessValue"), "\u4e8b\u696d\u4fa1\u5024")
        self.assertEqual(metric_base_to_display_name("TheoreticalSharePrice"), "\u7406\u8ad6\u682a\u4fa1")
        self.assertEqual(
            metric_base_to_display_name("UpperBoundTheoreticalSharePrice"),
            "\u4e0a\u9650\u7406\u8ad6\u682a\u4fa1",
        )
        self.assertEqual(metric_base_to_display_name("TheoreticalPBR"), "\u7406\u8ad6PBR")
        self.assertEqual(metric_base_to_display_name("TheoreticalPER"), "\u7406\u8ad6PER")
        self.assertEqual(metric_base_to_display_name("TheoreticalPCFR"), "\u7406\u8ad6PCFR")
        self.assertEqual(
            metric_base_to_display_name("EstimatedNetIncome"),
            "\u63a8\u5b9a\u7d14\u5229\u76ca(\u7d4c\u5e38\u5229\u76ca*0.7)",
        )
        self.assertEqual(metric_base_to_display_name("EstimatedNetMargin"), "\u7d14\u5229\u76ca\u7387")
        self.assertEqual(metric_base_to_display_name("FundingIncome"), "\u8cc7\u91d1\u904b\u7528\u53ce\u76ca")
        self.assertEqual(
            metric_base_to_display_name("FeesAndCommissionsIncome"),
            "\u5f79\u52d9\u53d6\u5f15\u7b49\u53ce\u76ca",
        )
        self.assertEqual(
            metric_base_to_display_name("InsuranceClaimsPayments"),
            "\u4fdd\u967a\u91d1\u7b49\u652f\u6255\u91d1",
        )
        self.assertEqual(
            metric_base_to_display_name("PolicyReserveProvision"),
            "\u8cac\u4efb\u6e96\u5099\u91d1\u7b49\u7e70\u5165\u984d",
        )
        self.assertEqual(
            metric_base_to_display_name("InvestmentExpenses"),
            "\u8cc7\u7523\u904b\u7528\u8cbb\u7528",
        )
        self.assertEqual(
            metric_base_to_display_name("ProjectExpenses"),
            "\u4e8b\u696d\u8cbb",
        )

    def test_metric_base_to_display_name_uses_bank_labels_for_bank_industry(self) -> None:
        self.assertEqual(
            metric_base_to_display_name("CostOfSales", "\u9280\u884c\u696d"),
            "\u8cc7\u91d1\u8abf\u9054\u8cbb\u7528",
        )
        self.assertEqual(
            metric_base_to_display_name("SellingExpenses", "\u9280\u884c\u696d"),
            "\u55b6\u696d\u7d4c\u8cbb",
        )
        self.assertEqual(
            metric_base_to_display_name(
                "CostOfSalesAndSellingGeneralAndAdministrativeExpenses",
                "\u9280\u884c\u696d",
            ),
            "\u8cbb\u7528\u5408\u8a08",
        )
        self.assertEqual(
            metric_base_to_display_name("GrossProfit", "\u9280\u884c\u696d"),
            "\u8cc7\u91d1\u5229\u76ca",
        )

    def test_metric_base_to_display_name_uses_securities_labels_for_securities_industry(self) -> None:
        self.assertEqual(
            metric_base_to_display_name("CostOfSales", "\u8a3c\u5238\u3001\u5546\u54c1\u5148\u7269\u53d6\u5f15\u696d"),
            "\u91d1\u878d\u8cbb\u7528",
        )
        self.assertEqual(
            metric_base_to_display_name(
                "CostOfSalesAndSellingGeneralAndAdministrativeExpenses",
                "\u8a3c\u5238\u3001\u5546\u54c1\u5148\u7269\u53d6\u5f15\u696d",
            ),
            "\u8cbb\u7528\u5408\u8a08",
        )
        self.assertEqual(
            metric_base_to_display_name("GrossProfit", "\u8a3c\u5238\u3001\u5546\u54c1\u5148\u7269\u53d6\u5f15\u696d"),
            "\u7d14\u53ce\u76ca",
        )
        self.assertEqual(
            metric_base_to_display_name(
                "CostOfSalesAndSellingGeneralAndAdministrativeExpenses",
                "\u4fdd\u967a\u696d",
            ),
            "\u8cbb\u7528\u5408\u8a08",
        )

    def test_metric_key_to_display_name_appends_period_suffix(self) -> None:
        self.assertEqual(
            metric_key_to_display_name("NetSalesCurrent"),
            "\u58f2\u4e0a\u9ad8\uff08\u5f53\u671f\uff09",
        )
        self.assertEqual(
            metric_key_to_display_name("OperatingMarginCurrent"),
            "\u55b6\u696d\u5229\u76ca\u7387\uff08\u5f53\u671f\uff09",
        )
        self.assertEqual(
            metric_key_to_display_name("CashBalanceGrowthRatePrior2"),
            "\u73fe\u91d1\u6b8b\u9ad8\u6210\u9577\u7387\uff08\u524d\u3005\u671f\uff09",
        )

    def test_split_metric_key_handles_unknown_key(self) -> None:
        self.assertEqual(split_metric_key("CustomMetric"), ("CustomMetric", None))

    def test_tag_name_to_display_name_normalizes_original_tag(self) -> None:
        self.assertEqual(tag_name_to_display_name("NetSalesIFRS"), "\u58f2\u4e0a\u9ad8")
        self.assertEqual(tag_name_to_display_name("CostOfSales"), "\u58f2\u4e0a\u539f\u4fa1")
        self.assertEqual(
            tag_name_to_display_name("CostOfSalesAndSellingGeneralAndAdministrativeExpensesIFRS"),
            "\u8cbb\u7528\u5408\u8a08",
        )
        self.assertEqual(
            tag_name_to_display_name("FinancingExpensesOpeCFBNK", "\u9280\u884c\u696d"),
            "\u8cc7\u91d1\u8abf\u9054\u8cbb\u7528",
        )
        self.assertEqual(
            tag_name_to_display_name("FinancialExpensesSEC", "\u8a3c\u5238\u3001\u5546\u54c1\u5148\u7269\u53d6\u5f15\u696d"),
            "\u91d1\u878d\u8cbb\u7528",
        )
        self.assertEqual(
            tag_name_to_display_name("ExpenseIFRS", "\u8a3c\u5238\u3001\u5546\u54c1\u5148\u7269\u53d6\u5f15\u696d"),
            "\u8cbb\u7528\u5408\u8a08",
        )
        self.assertEqual(
            tag_name_to_display_name("OperatingExpensesINS", "\u4fdd\u967a\u696d"),
            "\u8cbb\u7528\u5408\u8a08",
        )

    def test_metric_group_to_display_name_returns_japanese_label(self) -> None:
        self.assertEqual(metric_group_to_display_name("growth"), "\u6210\u9577")
        self.assertEqual(metric_group_to_display_name("cashflow"), "\u30ad\u30e3\u30c3\u30b7\u30e5\u30d5\u30ed\u30fc")
        self.assertEqual(metric_group_to_display_name("share"), "\u682a\u5f0f")
        self.assertEqual(metric_group_to_display_name("valuation"), "\u8a55\u4fa1")


if __name__ == "__main__":
    unittest.main()
