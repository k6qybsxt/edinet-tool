from __future__ import annotations

from typing import Any

from edinet_monitor.config.settings import DEFAULT_RULE_VERSION
from edinet_pipeline.domain.tag_alias import normalize_tag_to_metric
from edinet_pipeline.services.xbrl_parser import METRICS


TARGET_CONTEXT_SUFFIXES = {
    "CurrentYearDuration": ("Current", "duration"),
    "Prior1YearDuration": ("Prior1", "duration"),
    "Prior2YearDuration": ("Prior2", "duration"),
    "Prior3YearDuration": ("Prior3", "duration"),
    "Prior4YearDuration": ("Prior4", "duration"),
    "CurrentYearInstant": ("Current", "instant"),
    "Prior1YearInstant": ("Prior1", "instant"),
    "Prior2YearInstant": ("Prior2", "instant"),
    "Prior3YearInstant": ("Prior3", "instant"),
    "Prior4YearInstant": ("Prior4", "instant"),
}


def _to_number(value_text: str | None) -> float | None:
    if value_text in (None, ""):
        return None
    try:
        return float(str(value_text).replace(",", ""))
    except Exception:
        return None


def _get_suffix_and_period_kind(context_ref: str) -> tuple[str, str] | None:
    text = str(context_ref or "")

    for suffix_key in sorted(TARGET_CONTEXT_SUFFIXES.keys(), key=len, reverse=True):
        if suffix_key in text:
            return TARGET_CONTEXT_SUFFIXES[suffix_key]

    return None

def _build_metric_key(base_metric: str, suffix: str) -> str:
    return f"{base_metric}{suffix}"


def _extract_fiscal_year(period_end: str | None) -> int | None:
    if not period_end:
        return None
    try:
        return int(str(period_end)[:4])
    except Exception:
        return None


def _build_source_tag_priority_map() -> dict[str, dict[str, int]]:
    priority_map: dict[str, dict[str, int]] = {}

    for metric_base, meta in METRICS.items():
        metric_map: dict[str, int] = {}
        for idx, full_tag in enumerate(meta.get("tags", [])):
            local_tag = str(full_tag).split(":", 1)[1] if ":" in str(full_tag) else str(full_tag)
            if local_tag not in metric_map:
                metric_map[local_tag] = idx
        priority_map[metric_base] = metric_map

    return priority_map


SOURCE_TAG_PRIORITY_MAP = _build_source_tag_priority_map()

SOURCE_TAG_PRIORITY_OVERRIDES = {
    "CostOfSales": {
        "FinancialExpensesSEC": 0,
    },
    "CostOfSalesAndSellingGeneralAndAdministrativeExpenses": {
        "ExpenseIFRS": 0,
        "OperatingExpensesIFRS": 0,
        "ElectricUtilityOperatingExpensesELE": 0,
        "ElectricUtilityOperatingExpenses": 0,
        "OperatingExpensesOILTelecommunications": 0,
        "BusinessExpenses": 0,
        "OperatingExpensesOE": 0,
        "OperatingCostsAndExpensesCOSExpOA": 0,
        "OperatingExpenses": 1,
    },
    "SellingExpenses": {
        "SellingGeneralAndAdministrativeExpensesIFRS": 0,
        "SellingGeneralAndAdministrativeExpenses": 0,
        "SellingExpensesAndGeneralAdministrativeExpensesIFRS": 0,
        "SellingExpensesAndGeneralAdministrativeExpenses": 0,
        "GeneralAndAdministrativeExpensesSGA": 1,
        "GeneralAndAdministrativeExpensesIFRS": 1,
        "GeneralAndAdministrativeExpenses": 1,
    },
}


def _get_source_tag_priority(metric_base: str, tag_name: str) -> int:
    override_map = SOURCE_TAG_PRIORITY_OVERRIDES.get(metric_base, {})
    if tag_name in override_map:
        return override_map[tag_name]
    metric_map = SOURCE_TAG_PRIORITY_MAP.get(metric_base, {})
    return metric_map.get(tag_name, 9999)


def _consolidation_rank(consolidation: str | None) -> int:
    text = str(consolidation or "").strip()
    if text in ("Consolidated", "C", "consolidated"):
        return 0
    if text in ("NonConsolidated", "N", "nonconsolidated"):
        return 1
    return 2


def _is_forbidden_candidate(metric_base: str, tag_name: str, consolidation: str | None) -> bool:
    return False

def normalize_raw_fact_row(
    row: dict[str, Any],
    *,
    edinet_code: str,
    security_code: str,
) -> dict[str, Any] | None:
    tag_name = str(row.get("tag_name") or "")
    metric_base = normalize_tag_to_metric(tag_name)
    if not metric_base:
        return None

    context_ref = str(row.get("context_ref") or "")
    suffix_info = _get_suffix_and_period_kind(context_ref)
    if not suffix_info:
        return None

    suffix, expected_period_type = suffix_info
    period_type = str(row.get("period_type") or "")
    if period_type != expected_period_type:
        return None

    value_num = _to_number(row.get("value_text"))
    if value_num is None:
        return None

    consolidation = row.get("consolidation")
    if _is_forbidden_candidate(metric_base, tag_name, consolidation):
        return None

    period_end = row.get("period_end") or row.get("instant_date")
    fiscal_year = _extract_fiscal_year(period_end)

    return {
        "doc_id": row["doc_id"],
        "edinet_code": edinet_code,
        "security_code": security_code,
        "metric_key": _build_metric_key(metric_base, suffix),
        "fiscal_year": fiscal_year,
        "period_end": period_end,
        "value_num": value_num,
        "source_tag": tag_name,
        "consolidation": consolidation,
        "rule_version": DEFAULT_RULE_VERSION,
        "_metric_base": metric_base,
        "_tag_priority": _get_source_tag_priority(metric_base, tag_name),
        "_consolidation_rank": _consolidation_rank(consolidation),
    }


def _dedupe_group_key(row: dict[str, Any]) -> tuple:
    return (
        row["doc_id"],
        row["metric_key"],
        row["period_end"],
    )


def _dedupe_sort_key(row: dict[str, Any]) -> tuple:
    return (
        row.get("_consolidation_rank", 9999),
        row.get("_tag_priority", 9999),
        str(row.get("source_tag") or ""),
    )


def _rewrite_service_operating_expenses_as_cost_of_sales(
    rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    selling_groups = {
        _dedupe_group_key(row)
        for row in rows
        if str(row.get("_metric_base") or "") == "SellingExpenses"
    }

    rewritten: list[dict[str, Any]] = []
    for row in rows:
        candidate = dict(row)
        if (
            str(candidate.get("_metric_base") or "") == "CostOfSalesAndSellingGeneralAndAdministrativeExpenses"
            and str(candidate.get("source_tag") or "") == "OperatingExpenses"
            and _dedupe_group_key(candidate) in selling_groups
        ):
            metric_key = str(candidate.get("metric_key") or "")
            if metric_key.endswith("Current"):
                suffix = "Current"
            elif metric_key.endswith("Prior1"):
                suffix = "Prior1"
            elif metric_key.endswith("Prior2"):
                suffix = "Prior2"
            elif metric_key.endswith("Prior3"):
                suffix = "Prior3"
            elif metric_key.endswith("Prior4"):
                suffix = "Prior4"
            else:
                suffix = ""
            candidate["metric_key"] = _build_metric_key("CostOfSales", suffix)
            candidate["_metric_base"] = "CostOfSales"
            candidate["_tag_priority"] = _get_source_tag_priority("CostOfSales", "OperatingExpenses")
        rewritten.append(candidate)
    return rewritten


def dedupe_normalized_metrics(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    best_by_key: dict[tuple, dict[str, Any]] = {}

    for row in rows:
        key = _dedupe_group_key(row)
        current = best_by_key.get(key)

        if current is None:
            best_by_key[key] = row
            continue

        if _dedupe_sort_key(row) < _dedupe_sort_key(current):
            best_by_key[key] = row

    out: list[dict[str, Any]] = []

    for row in best_by_key.values():
        cleaned = dict(row)
        cleaned.pop("_metric_base", None)
        cleaned.pop("_tag_priority", None)
        cleaned.pop("_consolidation_rank", None)
        out.append(cleaned)

    out.sort(
        key=lambda x: (
            str(x.get("doc_id") or ""),
            str(x.get("metric_key") or ""),
            str(x.get("period_end") or ""),
        )
    )
    return out


def normalize_raw_fact_rows(
    raw_rows: list[dict[str, Any]],
    *,
    edinet_code: str,
    security_code: str,
) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []

    for row in raw_rows:
        normalized = normalize_raw_fact_row(
            row,
            edinet_code=edinet_code,
            security_code=security_code,
        )
        if normalized is not None:
            candidates.append(normalized)

    candidates = _rewrite_service_operating_expenses_as_cost_of_sales(candidates)
    return dedupe_normalized_metrics(candidates)
