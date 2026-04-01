from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


ComparisonOperator = Literal["exists", "gt", "gte", "lt", "lte"]


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
        rule_version="2026-04-01-v2",
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


RULE_DEFINITIONS = {
    "minimum_viable_value_check": _build_minimum_viable_rule(),
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

    if value_num is None:
        passed = False
        failure_reason = "missing_metric"
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
        "operator": check_definition.operator,
        "threshold": check_definition.threshold,
        "weight": float(check_definition.weight),
        "required_for_pass": bool(check_definition.required_for_pass),
        "passed": passed,
        "failure_reason": failure_reason,
        "value_num": value_num,
        "period_end": metric_row.get("period_end"),
        "source_tag": metric_row.get("source_tag"),
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
