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
            "\u58f2\u4e0a\u539f\u4fa1\u4e26\u3073\u306b\u8ca9\u58f2\u8cbb\u53ca\u3073\u4e00\u822c\u7ba1\u7406\u8cbb",
        )
        self.assertEqual(metric_base_to_display_name("CashAndCashEquivalents"), "\u671f\u672b\u6b8b")
        self.assertEqual(metric_base_to_display_name("OutstandingShares"), "\u767a\u884c\u682a\u6570")
        self.assertEqual(
            metric_base_to_display_name("EstimatedNetIncome"),
            "\u63a8\u5b9a\u7d14\u5229\u76ca(\u7d4c\u5e38\u5229\u76ca*0.7)",
        )
        self.assertEqual(metric_base_to_display_name("EstimatedNetMargin"), "\u7d14\u5229\u76ca\u7387")

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
            "\u58f2\u4e0a\u539f\u4fa1\u4e26\u3073\u306b\u8ca9\u58f2\u8cbb\u53ca\u3073\u4e00\u822c\u7ba1\u7406\u8cbb",
        )

    def test_metric_group_to_display_name_returns_japanese_label(self) -> None:
        self.assertEqual(metric_group_to_display_name("growth"), "\u6210\u9577")
        self.assertEqual(metric_group_to_display_name("cashflow"), "\u30ad\u30e3\u30c3\u30b7\u30e5\u30d5\u30ed\u30fc")
        self.assertEqual(metric_group_to_display_name("share"), "\u682a\u5f0f")


if __name__ == "__main__":
    unittest.main()
