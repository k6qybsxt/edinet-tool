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
    evaluate_rule_definition,
    get_rule_definition,
    list_rule_names,
)


def build_metric_row(value_num: float) -> dict:
    return {
        "value_num": value_num,
        "period_end": "2026-03-31",
        "source_tag": "DummyTag",
        "consolidation": "Consolidated",
    }


class ScreeningRuleServiceTest(unittest.TestCase):
    def test_default_rule_is_registered(self) -> None:
        self.assertIn(DEFAULT_RULE_NAME, list_rule_names())

        rule_definition = get_rule_definition(DEFAULT_RULE_NAME)

        self.assertEqual(rule_definition.rule_name, DEFAULT_RULE_NAME)
        self.assertEqual(len(rule_definition.checks), 4)

    def test_minimum_viable_rule_passes_when_required_metrics_exist(self) -> None:
        metrics = {
            "NetSalesCurrent": build_metric_row(100.0),
            "OperatingIncomeCurrent": build_metric_row(10.0),
            "NetAssetsCurrent": build_metric_row(50.0),
            "CashAndCashEquivalentsCurrent": build_metric_row(20.0),
        }

        result = evaluate_minimum_viable_value_check(metrics)

        self.assertEqual(result["result_flag"], 1)
        self.assertEqual(result["score"], 100.0)
        self.assertEqual(result["detail"]["missing_keys"], [])
        self.assertEqual(result["detail"]["failed_required_checks"], [])
        self.assertTrue(all(row["passed"] for row in result["detail"]["check_results"]))

    def test_minimum_viable_rule_fails_when_metric_is_missing(self) -> None:
        metrics = {
            "NetSalesCurrent": build_metric_row(100.0),
            "OperatingIncomeCurrent": build_metric_row(10.0),
            "NetAssetsCurrent": build_metric_row(50.0),
        }

        result = evaluate_minimum_viable_value_check(metrics)

        self.assertEqual(result["result_flag"], 0)
        self.assertEqual(result["score"], 75.0)
        self.assertEqual(
            result["detail"]["missing_keys"],
            ["CashAndCashEquivalentsCurrent"],
        )
        self.assertEqual(
            result["detail"]["failed_required_checks"],
            ["cash_current_exists"],
        )

    def test_generic_rule_definition_supports_threshold_checks(self) -> None:
        rule_definition = ScreeningRuleDefinition(
            rule_name="profitability_check",
            rule_version="test-v1",
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
