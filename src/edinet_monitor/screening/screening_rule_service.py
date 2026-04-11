from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from edinet_pipeline.domain.metric_labels import (
    metric_base_to_display_name,
    metric_group_to_display_name,
    metric_key_to_display_name,
    tag_name_to_display_name,
)


ComparisonOperator = Literal["exists", "gt", "gte", "lt", "lte"]
PeriodScope = Literal["annual", "half"]


@dataclass(frozen=True)
class MetricCheckDefinition:
    check_name: str
    metric_key: str
    operator: ComparisonOperator = "exists"
    threshold: float | None = None
    weight: float = 1.0
    required_for_pass: bool = True


@dataclass(frozen=True)
class ScreeningRuleDefinition:
    rule_name: str
    rule_version: str
    period_scope: PeriodScope
    checks: tuple[MetricCheckDefinition, ...]


def _build_minimum_viable_rule() -> ScreeningRuleDefinition:
    required_keys = [
        ("net_sales_current_exists", "NetSalesCurrent"),
        ("operating_income_current_exists", "OperatingIncomeCurrent"),
        ("net_assets_current_exists", "NetAssetsCurrent"),
        ("cash_current_exists", "CashAndCashEquivalentsCurrent"),
    ]
    weight = 100.0 / len(required_keys)

    return ScreeningRuleDefinition(
        rule_name="minimum_viable_value_check",
        rule_version="2026-04-04-v2",
        period_scope="annual",
        checks=tuple(
            MetricCheckDefinition(
                check_name=check_name,
                metric_key=metric_key,
                operator="exists",
                threshold=None,
                weight=weight,
                required_for_pass=True,
            )
            for check_name, metric_key in required_keys
        ),
    )


def _build_annual_growth_quality_rule() -> ScreeningRuleDefinition:
    checks = (
        MetricCheckDefinition(
            check_name="net_sales_growth_current_gt_100pct",
            metric_key="NetSalesGrowthRateCurrent",
            operator="gt",
            threshold=1.0,
            weight=25.0,
        ),
        MetricCheckDefinition(
            check_name="ordinary_income_growth_current_gt_100pct",
            metric_key="OrdinaryIncomeGrowthRateCurrent",
            operator="gt",
            threshold=1.0,
            weight=25.0,
        ),
        MetricCheckDefinition(
            check_name="equity_ratio_current_gte_30pct",
            metric_key="EquityRatioCurrent",
            operator="gte",
            threshold=0.3,
            weight=25.0,
        ),
        MetricCheckDefinition(
            check_name="fcf_current_gt_zero",
            metric_key="FCFCurrent",
            operator="gt",
            threshold=0.0,
            weight=25.0,
        ),
    )

    return ScreeningRuleDefinition(
        rule_name="annual_growth_quality_check",
        rule_version="2026-04-04-v1",
        period_scope="annual",
        checks=checks,
    )


def _build_annual_profitability_safety_rule() -> ScreeningRuleDefinition:
    checks = (
        MetricCheckDefinition(
            check_name="operating_margin_current_gte_5pct",
            metric_key="OperatingMarginCurrent",
            operator="gte",
            threshold=0.05,
            weight=30.0,
        ),
        MetricCheckDefinition(
            check_name="equity_ratio_current_gte_30pct",
            metric_key="EquityRatioCurrent",
            operator="gte",
            threshold=0.3,
            weight=30.0,
        ),
        MetricCheckDefinition(
            check_name="fcf_current_gt_zero",
            metric_key="FCFCurrent",
            operator="gt",
            threshold=0.0,
            weight=30.0,
        ),
        MetricCheckDefinition(
            check_name="roe_current_gte_8pct",
            metric_key="ROECurrent",
            operator="gte",
            threshold=0.08,
            weight=10.0,
            required_for_pass=False,
        ),
    )

    return ScreeningRuleDefinition(
        rule_name="annual_profitability_safety_check",
        rule_version="2026-04-04-v1",
        period_scope="annual",
        checks=checks,
    )


def _build_annual_sga_efficiency_rule() -> ScreeningRuleDefinition:
    checks = (
        MetricCheckDefinition(
            check_name="selling_expenses_ratio_current_lte_30pct",
            metric_key="SellingExpensesRatioCurrent",
            operator="lte",
            threshold=0.3,
            weight=30.0,
        ),
        MetricCheckDefinition(
            check_name="ordinary_income_margin_current_gte_5pct",
            metric_key="OrdinaryIncomeMarginCurrent",
            operator="gte",
            threshold=0.05,
            weight=30.0,
        ),
        MetricCheckDefinition(
            check_name="equity_ratio_current_gte_30pct",
            metric_key="EquityRatioCurrent",
            operator="gte",
            threshold=0.3,
            weight=30.0,
        ),
        MetricCheckDefinition(
            check_name="fcf_current_gt_zero",
            metric_key="FCFCurrent",
            operator="gt",
            threshold=0.0,
            weight=10.0,
            required_for_pass=False,
        ),
    )

    return ScreeningRuleDefinition(
        rule_name="annual_sga_efficiency_check",
        rule_version="2026-04-04-v1",
        period_scope="annual",
        checks=checks,
    )


RULE_DEFINITIONS = {
    "minimum_viable_value_check": _build_minimum_viable_rule(),
    "annual_growth_quality_check": _build_annual_growth_quality_rule(),
    "annual_profitability_safety_check": _build_annual_profitability_safety_rule(),
    "annual_sga_efficiency_check": _build_annual_sga_efficiency_rule(),
}
DEFAULT_RULE_NAME = "minimum_viable_value_check"
RULE_NAME = DEFAULT_RULE_NAME
RULE_VERSION = RULE_DEFINITIONS[DEFAULT_RULE_NAME].rule_version


def list_rule_names() -> list[str]:
    return sorted(RULE_DEFINITIONS.keys())


def get_rule_definition(rule_name: str | None = None) -> ScreeningRuleDefinition:
    resolved_rule_name = rule_name or DEFAULT_RULE_NAME
    rule_definition = RULE_DEFINITIONS.get(resolved_rule_name)

    if rule_definition is not None:
        return rule_definition

    allowed_values = ", ".join(list_rule_names())
    raise ValueError(
        f"Unknown screening rule: {resolved_rule_name}. Allowed values: {allowed_values}"
    )


def _compare_value(
    *,
    value_num: float,
    operator: ComparisonOperator,
    threshold: float | None,
) -> bool:
    if operator == "exists":
        return True

    if threshold is None:
        raise ValueError(f"threshold is required for operator={operator}")

    if operator == "gt":
        return value_num > threshold
    if operator == "gte":
        return value_num >= threshold
    if operator == "lt":
        return value_num < threshold
    if operator == "lte":
        return value_num <= threshold

    raise ValueError(f"Unsupported operator: {operator}")


def _evaluate_metric_check(
    metrics: dict[str, dict],
    check_definition: MetricCheckDefinition,
) -> dict:
    metric_row = dict(metrics.get(check_definition.metric_key) or {})
    value_num = metric_row.get("value_num")

    if not metric_row:
        passed = False
        failure_reason = "missing_metric"
    elif value_num is None:
        passed = False
        failure_reason = str(metric_row.get("calc_status") or "missing_value")
    else:
        passed = _compare_value(
            value_num=float(value_num),
            operator=check_definition.operator,
            threshold=check_definition.threshold,
        )
        failure_reason = "" if passed else "comparison_failed"

    return {
        "check_name": check_definition.check_name,
        "metric_key": check_definition.metric_key,
        "metric_label": metric_key_to_display_name(check_definition.metric_key),
        "operator": check_definition.operator,
        "threshold": check_definition.threshold,
        "weight": float(check_definition.weight),
        "required_for_pass": bool(check_definition.required_for_pass),
        "passed": passed,
        "failure_reason": failure_reason,
        "value_num": value_num,
        "period_end": metric_row.get("period_end"),
        "metric_source": metric_row.get("metric_source"),
        "metric_base": metric_row.get("metric_base"),
        "metric_base_label": metric_base_to_display_name(metric_row.get("metric_base")),
        "metric_group": metric_row.get("metric_group"),
        "metric_group_label": metric_group_to_display_name(metric_row.get("metric_group")),
        "period_scope": metric_row.get("period_scope"),
        "value_unit": metric_row.get("value_unit"),
        "calc_status": metric_row.get("calc_status"),
        "document_display_unit": metric_row.get("document_display_unit"),
        "accounting_standard": metric_row.get("accounting_standard"),
        "source_tag": metric_row.get("source_tag"),
        "source_tag_label": tag_name_to_display_name(metric_row.get("source_tag")),
        "consolidation": metric_row.get("consolidation"),
    }


def evaluate_rule_definition(
    metrics: dict[str, dict],
    rule_definition: ScreeningRuleDefinition,
) -> dict:
    check_results = [
        _evaluate_metric_check(metrics, check_definition)
        for check_definition in rule_definition.checks
    ]

    checked_keys = list(
        dict.fromkeys(check_definition.metric_key for check_definition in rule_definition.checks)
    )
    missing_keys = [
        str(check_result["metric_key"])
        for check_result in check_results
        if check_result["failure_reason"] == "missing_metric"
    ]
    failed_required_checks = [
        str(check_result["check_name"])
        for check_result in check_results
        if bool(check_result["required_for_pass"]) and not bool(check_result["passed"])
    ]

    total_weight = sum(float(check_result["weight"]) for check_result in check_results)
    passed_weight = sum(
        float(check_result["weight"])
        for check_result in check_results
        if bool(check_result["passed"])
    )
    score = round((passed_weight / total_weight) * 100, 2) if total_weight > 0 else 0.0

    detail = {
        "missing_keys": missing_keys,
        "checked_keys": checked_keys,
        "values": {
            str(check_result["metric_key"]): check_result["value_num"]
            for check_result in check_results
        },
        "check_results": check_results,
        "failed_required_checks": failed_required_checks,
    }

    return {
        "rule_name": rule_definition.rule_name,
        "rule_version": rule_definition.rule_version,
        "period_scope": rule_definition.period_scope,
        "result_flag": 1 if len(failed_required_checks) == 0 else 0,
        "score": score,
        "detail": detail,
    }


def evaluate_screening_rule(
    metrics: dict[str, dict],
    *,
    rule_name: str | None = None,
) -> dict:
    rule_definition = get_rule_definition(rule_name)
    return evaluate_rule_definition(metrics, rule_definition)


def evaluate_minimum_viable_value_check(metrics: dict[str, dict]) -> dict:
    return evaluate_screening_rule(metrics, rule_name=RULE_NAME)
