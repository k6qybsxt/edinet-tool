from __future__ import annotations

import unittest

from edinet_monitor.services.normalizer.structure_classifier import classify_structure


class StructureClassifierTest(unittest.TestCase):
    def test_classify_cost_role(self) -> None:
        result = classify_structure(
            metric_base="CostOfSales",
            tag_name="FinancialExpensesSEC",
            structure_info={
                "label": "金融費用",
                "presentation_parent_labels": ["費用"],
                "calculation_children_count": 0,
                "is_total": False,
            },
        )

        self.assertEqual(result["role"], "cost")
        self.assertEqual(result["confidence"], "high")

    def test_classify_combined_expense_role(self) -> None:
        result = classify_structure(
            metric_base="CostOfSalesAndSellingGeneralAndAdministrativeExpenses",
            tag_name="ExpenseIFRS",
            structure_info={
                "label": "費用合計",
                "presentation_parent_labels": ["収益"],
                "calculation_children_count": 3,
                "is_total": True,
            },
        )

        self.assertEqual(result["role"], "combined_expense")
        self.assertEqual(result["confidence"], "high")

    def test_classify_profit_role(self) -> None:
        result = classify_structure(
            metric_base="GrossProfit",
            tag_name="NetRevenueSummaryOfBusinessResults",
            structure_info={
                "label": "収益合計（金融費用控除後）",
                "presentation_parent_labels": ["損益計算書"],
                "calculation_children_count": 0,
                "is_total": False,
            },
        )

        self.assertEqual(result["role"], "profit")
        self.assertEqual(result["confidence"], "high")


if __name__ == "__main__":
    unittest.main()
