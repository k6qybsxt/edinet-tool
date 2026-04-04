from __future__ import annotations

import sys
import unittest
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from edinet_monitor.screening.screening_rule_service import (  # noqa: E402
    DEFAULT_RULE_NAME,
    MetricCheckDefinition,
    ScreeningRuleDefinition,
    evaluate_minimum_viable_value_check,
    evaluate_screening_rule,
    evaluate_rule_definition,
    get_rule_definition,
    list_rule_names,
)


def build_metric_row(value_num: float | None, **overrides: object) -> dict:
    row = {
        "value_num": value_num,
        "period_end": "2026-03-31",
        "metric_source": "derived_metrics",
        "metric_base": "DummyMetric",
        "metric_group": "dummy",
        "period_scope": "annual",
        "value_unit": "ratio",
        "calc_status": "ok" if value_num is not None else "missing_input",
        "document_display_unit": "million_yen",
        "accounting_standard": "ifrs",
        "source_tag": "DummyTag",
        "consolidation": "Consolidated",
    }
    row.update(overrides)
    return row


class ScreeningRuleServiceTest(unittest.TestCase):
    def test_default_rule_is_registered(self) -> None:
        self.assertIn(DEFAULT_RULE_NAME, list_rule_names())
        self.assertIn("annual_growth_quality_check", list_rule_names())
        self.assertIn("annual_profitability_safety_check", list_rule_names())
        self.assertIn("annual_sga_efficiency_check", list_rule_names())

        rule_definition = get_rule_definition(DEFAULT_RULE_NAME)

        self.assertEqual(rule_definition.rule_name, DEFAULT_RULE_NAME)
        self.assertEqual(rule_definition.period_scope, "annual")
        self.assertEqual(len(rule_definition.checks), 4)

    def test_minimum_viable_rule_passes_when_required_metrics_exist(self) -> None:
        metrics = {
            "NetSalesCurrent": build_metric_row(100.0, metric_source="normalized_metrics"),
            "OperatingIncomeCurrent": build_metric_row(10.0, metric_source="normalized_metrics"),
            "NetAssetsCurrent": build_metric_row(50.0, metric_source="normalized_metrics"),
            "CashAndCashEquivalentsCurrent": build_metric_row(20.0, metric_source="normalized_metrics"),
        }

        result = evaluate_minimum_viable_value_check(metrics)

        self.assertEqual(result["result_flag"], 1)
        self.assertEqual(result["score"], 100.0)
        self.assertEqual(result["detail"]["missing_keys"], [])
        self.assertEqual(result["detail"]["failed_required_checks"], [])
        self.assertTrue(all(row["passed"] for row in result["detail"]["check_results"]))

    def test_generic_rule_definition_supports_threshold_checks(self) -> None:
        rule_definition = ScreeningRuleDefinition(
            rule_name="profitability_check",
            rule_version="test-v1",
            period_scope="annual",
            checks=(
                MetricCheckDefinition(
                    check_name="net_sales_positive",
                    metric_key="NetSalesCurrent",
                    operator="gt",
                    threshold=0.0,
                    weight=60.0,
                ),
                MetricCheckDefinition(
                    check_name="operating_income_positive",
                    metric_key="OperatingIncomeCurrent",
                    operator="gt",
                    threshold=0.0,
                    weight=40.0,
                ),
            ),
        )
        metrics = {
            "NetSalesCurrent": build_metric_row(100.0),
            "OperatingIncomeCurrent": build_metric_row(-5.0),
        }

        result = evaluate_rule_definition(metrics, rule_definition)

        self.assertEqual(result["rule_name"], "profitability_check")
        self.assertEqual(result["result_flag"], 0)
        self.assertEqual(result["score"], 60.0)
        self.assertEqual(
            result["detail"]["failed_required_checks"],
            ["operating_income_positive"],
        )
        self.assertEqual(
            result["detail"]["check_results"][1]["failure_reason"],
            "comparison_failed",
        )

    def test_annual_growth_quality_rule_passes_with_expected_metrics(self) -> None:
        metrics = {
            "NetSalesGrowthRateCurrent": build_metric_row(1.2),
            "OrdinaryIncomeGrowthRateCurrent": build_metric_row(1.1),
            "EquityRatioCurrent": build_metric_row(0.45),
            "FCFCurrent": build_metric_row(100.0, value_unit="yen", metric_group="cashflow"),
        }

        result = evaluate_screening_rule(
            metrics,
            rule_name="annual_growth_quality_check",
        )

        self.assertEqual(result["result_flag"], 1)
        self.assertEqual(result["score"], 100.0)
        self.assertEqual(result["period_scope"], "annual")

    def test_annual_growth_quality_rule_keeps_calc_status_in_failure_detail(self) -> None:
        metrics = {
            "NetSalesGrowthRateCurrent": build_metric_row(1.2),
            "OrdinaryIncomeGrowthRateCurrent": build_metric_row(
                None,
                calc_status="zero_or_negative_base",
            ),
            "EquityRatioCurrent": build_metric_row(0.45),
            "FCFCurrent": build_metric_row(100.0, value_unit="yen", metric_group="cashflow"),
        }

        result = evaluate_screening_rule(
            metrics,
            rule_name="annual_growth_quality_check",
        )

        self.assertEqual(result["result_flag"], 0)
        self.assertEqual(result["score"], 75.0)
        self.assertEqual(result["detail"]["missing_keys"], [])
        self.assertEqual(
            result["detail"]["failed_required_checks"],
            ["ordinary_income_growth_current_gt_100pct"],
        )
        self.assertEqual(
            result["detail"]["check_results"][1]["failure_reason"],
            "zero_or_negative_base",
        )

    def test_annual_profitability_safety_rule_allows_optional_roe_to_affect_score_only(self) -> None:
        metrics = {
            "OperatingMarginCurrent": build_metric_row(0.08),
            "EquityRatioCurrent": build_metric_row(0.45),
            "FCFCurrent": build_metric_row(100.0, value_unit="yen", metric_group="cashflow"),
            "ROECurrent": build_metric_row(0.05),
        }

        result = evaluate_screening_rule(
            metrics,
            rule_name="annual_profitability_safety_check",
        )

        self.assertEqual(result["result_flag"], 1)
        self.assertEqual(result["score"], 90.0)
        self.assertEqual(result["detail"]["failed_required_checks"], [])
        self.assertEqual(
            result["detail"]["check_results"][3]["failure_reason"],
            "comparison_failed",
        )

    def test_annual_sga_efficiency_rule_fails_when_required_ratio_is_missing(self) -> None:
        metrics = {
            "OrdinaryIncomeMarginCurrent": build_metric_row(0.06),
            "EquityRatioCurrent": build_metric_row(0.40),
            "FCFCurrent": build_metric_row(100.0, value_unit="yen", metric_group="cashflow"),
        }

        result = evaluate_screening_rule(
            metrics,
            rule_name="annual_sga_efficiency_check",
        )

        self.assertEqual(result["result_flag"], 0)
        self.assertEqual(
            result["detail"]["missing_keys"],
            ["SellingExpensesRatioCurrent"],
        )
        self.assertEqual(
            result["detail"]["failed_required_checks"],
            ["selling_expenses_ratio_current_lte_30pct"],
        )


if __name__ == "__main__":
    unittest.main()
