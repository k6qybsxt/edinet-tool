from __future__ import annotations

import json
from datetime import date, timedelta
from typing import Any

from edinet_monitor.config.settings import DEFAULT_RULE_VERSION
from edinet_monitor.services.collector.document_filter_service import is_half_form_type
from edinet_monitor.services.normalizer.structure_classifier import classify_structure
from edinet_pipeline.domain.tag_alias import normalize_tag_to_metric
from edinet_pipeline.services.linkbase_analyzer import analyze_linkbase_structure
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

MAX_PERIOD_FALLBACK_OFFSET = 4
CANDIDATE_VALIDATION_STATUS_OK = "OK"
CANDIDATE_VALIDATION_STATUS_EXCLUDE = "EXCLUDE"
HALF_DISABLED_NORMALIZED_BASES = {
    "NumberOfEmployees",
    "AverageAge",
    "AverageLengthOfService",
    "AverageAnnualSalary",
}
SHARE_COUNT_METRIC_BASES = {
    "IssuedShares",
    "TreasuryShares",
}
ISSUED_FILING_DATE_TAGS = {
    "NumberOfIssuedSharesAsOfFiscalYearEndIssuedSharesTotalNumberOfSharesEtc",
    "NumberOfIssuedSharesAsOfFilingDateIssuedSharesTotalNumberOfSharesEtc",
}
BEGINNING_CASH_EXPLICIT_TAGS = (
    "CashAndCashEquivalentsAtBeginningOfPeriod",
    "CashAndCashEquivalentsAtBeginningOfYear",
    "CashAndCashEquivalentsAtBeginningOfFiscalYear",
    "CashAndCashEquivalentsAtBeginningOfInterimPeriod",
)
BEGINNING_CASH_PRIMARY_TAGS = (
    "CashAndCashEquivalents",
    "CashAndCashEquivalentsIFRS",
)
BEGINNING_CASH_SUMMARY_TAGS = (
    "CashAndCashEquivalentsSummaryOfBusinessResults",
    "CashAndCashEquivalentsIFRSSummaryOfBusinessResults",
    "CashAndCashEquivalentsUSGAAPSummaryOfBusinessResults",
)


def _period_scope_from_form_type(form_type: str | None) -> str:
    if is_half_form_type(form_type):
        return "half"
    return "annual"


def _to_number(value_text: str | None) -> float | None:
    if value_text in (None, ""):
        return None
    try:
        value = float(str(value_text).replace(",", ""))
    except Exception:
        return None
    if value == 0:
        return 0.0
    return value


def _get_suffix_and_period_kind(context_ref: str) -> tuple[str, str] | None:
    text = str(context_ref or "")

    for suffix_key in sorted(TARGET_CONTEXT_SUFFIXES.keys(), key=len, reverse=True):
        if suffix_key in text:
            return TARGET_CONTEXT_SUFFIXES[suffix_key]

    return None


def _issued_filing_date_suffix_info(
    *,
    metric_base: str,
    tag_name: str,
    context_ref: str,
) -> tuple[str, str] | None:
    if metric_base != "IssuedShares":
        return None
    if tag_name not in ISSUED_FILING_DATE_TAGS:
        return None
    if not str(context_ref or "").startswith("FilingDateInstant"):
        return None
    return "Current", "instant"


def _parse_iso_date(value: Any) -> date | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return date.fromisoformat(text[:10])
    except Exception:
        return None


def _period_offset_from_filing_end(filing_period_end: Any, target_date: Any) -> int | None:
    filing_end = _parse_iso_date(filing_period_end)
    target = _parse_iso_date(target_date)
    if not filing_end or not target:
        return None
    if (filing_end.month, filing_end.day) != (target.month, target.day):
        return None
    offset = filing_end.year - target.year
    if offset < 0 or offset > MAX_PERIOD_FALLBACK_OFFSET:
        return None
    return offset


def _suffix_from_period_offset(offset: int) -> str | None:
    if offset == 0:
        return "Current"
    if 1 <= offset <= MAX_PERIOD_FALLBACK_OFFSET:
        return f"Prior{offset}"
    return None


def _is_expected_duration(period_start: Any, period_end: Any, *, period_scope: str) -> bool:
    start = _parse_iso_date(period_start)
    end = _parse_iso_date(period_end)
    if not start or not end or start >= end:
        return False
    days = (end - start).days + 1
    if period_scope == "half":
        return 120 <= days <= 220
    return 300 <= days <= 400


def _infer_suffix_and_period_kind_from_dates(
    row: dict[str, Any],
    *,
    filing_period_end: str | None,
    period_scope: str = "annual",
) -> tuple[str, str] | None:
    period_type = str(row.get("period_type") or "").strip().lower()
    if period_type == "duration":
        period_end = row.get("period_end")
        if not _is_expected_duration(row.get("period_start"), period_end, period_scope=period_scope):
            return None
        offset = _period_offset_from_filing_end(filing_period_end, period_end)
    elif period_type == "instant":
        offset = _period_offset_from_filing_end(
            filing_period_end,
            row.get("instant_date") or row.get("period_end"),
        )
    else:
        return None

    if offset is None:
        return None
    suffix = _suffix_from_period_offset(offset)
    if not suffix:
        return None
    return suffix, period_type


def _half_suffix_from_dates(
    row: dict[str, Any],
    *,
    filing_period_end: str | None,
    expected_period_type: str,
) -> str | None:
    period_type = str(row.get("period_type") or "").strip().lower()
    if period_type != expected_period_type:
        return None
    if period_type == "duration":
        if not _is_expected_duration(row.get("period_start"), row.get("period_end"), period_scope="half"):
            return None
        target_date = row.get("period_end")
    elif period_type == "instant":
        target_date = row.get("instant_date") or row.get("period_end")
    else:
        return None
    offset = _period_offset_from_filing_end(filing_period_end, target_date)
    if offset is None:
        return None
    return _suffix_from_period_offset(offset)


def _infer_filing_period_end(raw_rows: list[dict[str, Any]]) -> str | None:
    dates = [
        parsed
        for row in raw_rows
        for parsed in [_parse_iso_date(row.get("period_end") or row.get("instant_date"))]
        if parsed is not None
    ]
    if not dates:
        return None
    return max(dates).isoformat()

def _build_metric_key(base_metric: str, suffix: str) -> str:
    return f"{base_metric}{suffix}"


def _extract_fiscal_year(period_end: str | None) -> int | None:
    if not period_end:
        return None
    try:
        return int(str(period_end)[:4])
    except Exception:
        return None


def _safe_json_loads(value: Any) -> Any:
    if not value:
        return {}
    try:
        return json.loads(str(value))
    except Exception:
        return {}


def _unit_measures(row: dict[str, Any]) -> list[str]:
    value = _safe_json_loads(row.get("unit_measures_json"))
    measures = value.get("measures") if isinstance(value, dict) else None
    if not measures:
        return []
    return [str(item) for item in measures if str(item or "").strip()]


def _has_jpy_unit(row: dict[str, Any]) -> bool:
    unit_ref = str(row.get("unit_ref") or "").lower()
    measures = [item.lower() for item in _unit_measures(row)]
    return unit_ref == "jpy" or "iso4217:jpy" in measures


def _has_shares_unit(row: dict[str, Any]) -> bool:
    unit_ref = str(row.get("unit_ref") or "").lower()
    measures = [item.lower() for item in _unit_measures(row)]
    return "shares" in unit_ref or any("shares" in item for item in measures)


def _schema_type_kind(schema_type: Any) -> str:
    text = str(schema_type or "").lower()
    if "monetary" in text:
        return "monetary"
    if "shares" in text:
        return "shares"
    if "percent" in text:
        return "percent"
    if "pure" in text:
        return "pure"
    if "string" in text or "textblock" in text:
        return "text"
    return ""


def _expected_unit_kind(metric_base: str) -> str:
    if metric_base in {"NumberOfEmployees", "AverageAge", "AverageLengthOfService"}:
        return ""
    unit = str((METRICS.get(metric_base) or {}).get("unit") or "").lower()
    if unit == "ones":
        return "shares"
    if unit in {"millions", "thousands", "yen"}:
        return "monetary"
    return ""


def _expected_schema_period_type(metric_base: str) -> str:
    kind = str((METRICS.get(metric_base) or {}).get("kind") or "").lower()
    if kind == "duration":
        return "duration"
    if kind.startswith("instant"):
        return "instant"
    return ""


def _is_true_text(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes"}


def _is_taxonomy_structure_concept(tag_name: str, schema: dict[str, Any]) -> bool:
    text = " ".join(
        [
            str(tag_name or ""),
            str(schema.get("type") or ""),
            str(schema.get("substitution_group") or ""),
        ]
    ).lower()
    suffix = str(tag_name or "")
    if any(suffix.endswith(item) for item in ("TextBlock", "Table", "Axis", "Member", "LineItems")):
        return True
    return any(token in text for token in ("textblock", "hypercubeitem", "dimensionitem"))


def _is_consolidation_member(member: Any) -> bool:
    text = str(member or "")
    return "ConsolidatedMember" in text or "NonConsolidatedMember" in text


def _has_detail_dimension(row: dict[str, Any]) -> bool:
    dimensions = _safe_json_loads(row.get("context_dimensions_json"))
    if not isinstance(dimensions, dict):
        return False
    typed_members = dimensions.get("typed_members") or []
    if typed_members:
        return True
    axis_members = dimensions.get("axis_members") or {}
    if isinstance(axis_members, dict):
        for members in axis_members.values():
            if not isinstance(members, list):
                members = [members]
            for member in members:
                if member and not _is_consolidation_member(member):
                    return True
    explicit_members = dimensions.get("explicit_members") or []
    if isinstance(explicit_members, list):
        for item in explicit_members:
            if not isinstance(item, dict):
                continue
            member = item.get("member")
            if member and not _is_consolidation_member(member):
                return True
    return False


def validate_candidate_for_enforcement(
    *,
    metric_base: str,
    row: dict[str, Any],
    schema: dict[str, Any] | None,
    expected_period_type: str,
) -> dict[str, Any]:
    schema = schema or {}
    issues: list[str] = []
    schema_period_type = str(schema.get("period_type") or "").strip()
    schema_kind = _schema_type_kind(schema.get("type"))
    expected_unit_kind = _expected_unit_kind(metric_base)
    expected_schema_period_type = _expected_schema_period_type(metric_base) or expected_period_type

    if _is_true_text(schema.get("abstract")):
        issues.append("schema_abstract")
    if _is_taxonomy_structure_concept(str(row.get("tag_name") or ""), schema):
        issues.append("taxonomy_structure_concept")
    if schema_period_type and expected_schema_period_type and schema_period_type != expected_schema_period_type:
        issues.append(f"schema_period_type_mismatch:{schema_period_type}!={expected_schema_period_type}")

    if expected_unit_kind == "monetary":
        if schema_kind in {"shares", "percent", "pure", "text"}:
            issues.append(f"schema_type_mismatch:expected_monetary:{schema_kind}")
        if row.get("unit_ref") and not _has_jpy_unit(row):
            issues.append("unit_mismatch:expected_jpy")
    elif expected_unit_kind == "shares":
        if schema_kind in {"monetary", "percent", "pure", "text"}:
            issues.append(f"schema_type_mismatch:expected_shares:{schema_kind}")
        if row.get("unit_ref") and not _has_shares_unit(row):
            issues.append("unit_mismatch:expected_shares")

    if _has_detail_dimension(row):
        issues.append("detail_dimension_candidate")

    return {
        "status": CANDIDATE_VALIDATION_STATUS_EXCLUDE if issues else CANDIDATE_VALIDATION_STATUS_OK,
        "issues": ",".join(issues) if issues else "OK",
    }


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

MANUAL_SOURCE_TAG_PRIORITY_OVERRIDES = {
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
        "SellingGeneralAndAdministrativeExpensesGAS": 0,
        "SellingExpensesAndGeneralAdministrativeExpensesIFRS": 0,
        "SellingExpensesAndGeneralAdministrativeExpenses": 0,
    },
    "GeneralAndAdministrativeExpenses": {
        "GeneralAndAdministrativeExpensesSGA": 0,
        "GeneralAndAdministrativeExpensesIFRS": 0,
        "GeneralAndAdministrativeExpenses": 0,
        "AdministrativeExpensesIFRS": 0,
        "AdministrativeExpenses": 0,
    },
    "SellingExpensesOnly": {
        "SupplyAndSalesExpensesGAS": 0,
    },
}

PROFIT_BEFORE_TAX_SOURCE_TAGS = {
    "ProfitLossBeforeTaxIFRSSummaryOfBusinessResults",
    "ProfitBeforeIncomeTaxesFromContinuingIFRSKeyFinancialData",
    "ProfitBeforeTax",
    "ProfitBeforeTaxIFRS",
    "ProfitLossBeforeTax",
    "ProfitLossBeforeTaxIFRS",
    "ProfitLossBeforeIncomeTaxes",
    "ProfitLossBeforeIncomeTaxesIFRS",
    "IncomeBeforeIncomeTaxes",
    "IncomeBeforeIncomeTaxesIFRS",
    "ProfitBeforeIncomeTaxes",
    "ProfitBeforeIncomeTaxesIFRS",
    "ProfitBeforeTaxFromContinuingOperationsIFRS",
    "ProfitBeforeTaxFromContinuingOperations",
}

SECONDARY_METRIC_BASES_BY_TAG = {
    tag_name: ("ProfitBeforeTax",)
    for tag_name in PROFIT_BEFORE_TAX_SOURCE_TAGS
}

MONTH_TO_YEAR_TAGS = {
    "AverageAgeMonthsInformationAboutReportingCompanyInformationAboutEmployees",
    "AverageLengthOfServiceMonthsInformationAboutReportingCompanyInformationAboutEmployees",
}


def _get_source_tag_priority(metric_base: str, tag_name: str) -> int:
    metric_map = SOURCE_TAG_PRIORITY_MAP.get(metric_base, {})
    return metric_map.get(tag_name, 9999)


def _get_manual_source_tag_priority(metric_base: str, tag_name: str) -> int:
    override_map = MANUAL_SOURCE_TAG_PRIORITY_OVERRIDES.get(metric_base, {})
    return override_map.get(tag_name, 9999)


def _structure_priority(metric_base: str, tag_name: str, structure_info: dict[str, Any] | None) -> int:
    classification = classify_structure(
        metric_base=metric_base,
        tag_name=tag_name,
        structure_info=structure_info,
    )
    text = str(classification["text"])
    is_total = bool(classification["is_total"])
    confidence = str(classification["confidence"])
    role = str(classification["role"])

    if metric_base == "CostOfSales":
        if role == "cost":
            return 0 if is_total else 1
        if confidence == "medium" and any(keyword in text for keyword in ("売上原価", "原価", "金融費用", "資金調達費用")):
            return 2
        return 9999

    if metric_base == "SellingExpenses":
        if "販売費及び一般管理費合計" in text:
            return 0
        if role == "expense" and ("販売費及び一般管理費" in text or "販管費" in text):
            return 1
        if role == "expense" and ("一般管理費" in text or "営業経費" in text):
            return 2
        return 9999

    if metric_base == "GeneralAndAdministrativeExpenses":
        if tag_name == "GeneralAndAdministrativeExpensesSGA":
            return 0
        if tag_name in {"GeneralAndAdministrativeExpenses", "GeneralAndAdministrativeExpensesIFRS"}:
            return 1
        if tag_name in {"AdministrativeExpenses", "AdministrativeExpensesIFRS"}:
            return 2
        return 9999

    if metric_base == "SellingExpensesOnly":
        if tag_name == "SupplyAndSalesExpensesGAS":
            return 0
        return 9999

    if metric_base == "CostOfSalesAndSellingGeneralAndAdministrativeExpenses":
        if role == "combined_expense" and any(keyword in text for keyword in ("費用合計", "経常費用", "営業費用合計", "事業費用合計")):
            return 0
        if role in {"combined_expense", "expense"} and any(keyword in text for keyword in ("営業費用", "事業費用", "電気事業営業費用", "業務費")):
            return 1 if is_total else 2
        if is_total and "費用" in text:
            return 2
        return 9999

    if metric_base == "GrossProfit":
        if role == "profit" and any(keyword in text for keyword in ("売上総利益", "粗利益", "資金利益", "純収益")):
            return 0
        if role == "profit" and "控除後" in text and "収益" in text:
            return 1
        if role == "profit" and "利益" in text:
            return 2
        return 9999

    return 9999


def _consolidation_rank(consolidation: str | None) -> int:
    text = str(consolidation or "").strip()
    if text in ("Consolidated", "C", "consolidated"):
        return 0
    if text in ("NonConsolidated", "N", "nonconsolidated"):
        return 1
    return 2


GAS_INDUSTRY_33 = "電気・ガス業"
GAS_ONLY_SOURCE_TAGS = {
    "SellingGeneralAndAdministrativeExpensesGAS",
    "SupplyAndSalesExpensesGAS",
}


def _is_forbidden_candidate(
    metric_base: str,
    tag_name: str,
    consolidation: str | None,
    *,
    industry_33: str | None = None,
) -> bool:
    if tag_name in GAS_ONLY_SOURCE_TAGS and str(industry_33 or "").strip() != GAS_INDUSTRY_33:
        return True
    return False


def _is_six_month_ytd_duration(row: dict[str, Any], filing_period_end: str | None) -> bool:
    if not str(row.get("context_ref") or "").startswith("CurrentYTDDuration"):
        return False
    if str(row.get("period_type") or "").strip().lower() != "duration":
        return False
    period_start = _parse_iso_date(row.get("period_start"))
    period_end = _parse_iso_date(row.get("period_end"))
    filing_end = _parse_iso_date(filing_period_end)
    if not period_start or not period_end or period_end != filing_end:
        return False
    next_day = period_end + timedelta(days=1)
    months = (next_day.year - period_start.year) * 12 + (next_day.month - period_start.month)
    return months == 6 and next_day.day == period_start.day


def _is_six_month_from_prior_year_instant(row: dict[str, Any], filing_period_end: str | None) -> bool:
    prior_year_end = _parse_iso_date(row.get("instant_date"))
    filing_end = _parse_iso_date(filing_period_end)
    if not prior_year_end or not filing_end:
        return False
    period_start = prior_year_end + timedelta(days=1)
    next_day = filing_end + timedelta(days=1)
    months = (next_day.year - period_start.year) * 12 + (next_day.month - period_start.month)
    return months == 6 and next_day.day == period_start.day


def _is_current_2q_filing(
    raw_rows: list[dict[str, Any]],
    *,
    filing_period_end: str | None,
    form_type: str | None,
) -> bool:
    if not is_half_form_type(form_type):
        return False
    if any(_is_six_month_ytd_duration(row, filing_period_end) for row in raw_rows):
        return True
    return any(
        source_group != "explicit"
        and _is_six_month_from_prior_year_instant(row, filing_period_end)
        for _, source_group, row in _beginning_cash_source_rows(raw_rows)
    )


def _beginning_cash_source_rows(
    raw_rows: list[dict[str, Any]],
) -> list[tuple[int, str, dict[str, Any]]]:
    source_groups = (
        ("explicit", BEGINNING_CASH_EXPLICIT_TAGS),
        ("primary", BEGINNING_CASH_PRIMARY_TAGS),
        ("summary", BEGINNING_CASH_SUMMARY_TAGS),
    )
    ranked: list[tuple[int, str, dict[str, Any]]] = []
    for group_index, (source_group, tag_names) in enumerate(source_groups):
        for tag_index, tag_name in enumerate(tag_names):
            source_priority = group_index * 100 + tag_index
            for row in raw_rows:
                if str(row.get("tag_name") or "") != tag_name:
                    continue
                context_ref = str(row.get("context_ref") or "")
                if source_group == "explicit":
                    if not context_ref.startswith("Current"):
                        continue
                elif (
                    "Prior1YearInstant" not in context_ref
                    or str(row.get("period_type") or "").strip().lower() != "instant"
                ):
                    continue
                if _to_number(row.get("value_text")) is None:
                    continue
                if row.get("unit_ref") and not _has_jpy_unit(row):
                    continue
                if _has_detail_dimension(row):
                    continue
                ranked.append((source_priority, source_group, row))
    return ranked


def select_beginning_cash_source(
    raw_rows: list[dict[str, Any]],
) -> tuple[int, str, dict[str, Any]] | None:
    ranked_rows = _beginning_cash_source_rows(raw_rows)
    if not ranked_rows:
        return None
    return min(
        ranked_rows,
        key=lambda item: (
            _consolidation_rank(item[2].get("consolidation")),
            item[0],
            str(item[2].get("source_tag") or item[2].get("tag_name") or ""),
        ),
    )


def select_2q_beginning_cash_source(
    raw_rows: list[dict[str, Any]],
    *,
    filing_period_end: str | None,
    form_type: str | None,
) -> tuple[int, str, dict[str, Any]] | None:
    if not _is_current_2q_filing(
        raw_rows,
        filing_period_end=filing_period_end,
        form_type=form_type,
    ):
        return None
    return select_beginning_cash_source(raw_rows)


def _build_2q_beginning_cash_candidate(
    raw_rows: list[dict[str, Any]],
    *,
    filing_period_end: str | None,
    form_type: str | None,
) -> dict[str, Any] | None:
    selected = select_2q_beginning_cash_source(
        raw_rows,
        filing_period_end=filing_period_end,
        form_type=form_type,
    )
    if selected is None:
        return None
    source_priority, source_group, row = selected
    value_num = _to_number(row.get("value_text"))
    if value_num is None:
        return None
    period_end = filing_period_end or row.get("period_end") or row.get("instant_date")
    tag_name = str(row.get("tag_name") or "")
    return {
        "doc_id": row["doc_id"],
        "edinet_code": row.get("edinet_code"),
        "security_code": row.get("security_code"),
        "metric_key": _build_metric_key("BeginningCashBalance", "Current"),
        "fiscal_year": _extract_fiscal_year(period_end),
        "period_end": period_end,
        "value_num": value_num,
        "source_tag": tag_name,
        "consolidation": row.get("consolidation"),
        "rule_version": DEFAULT_RULE_VERSION,
        "_metric_base": "BeginningCashBalance",
        "_tag_priority": source_priority,
        "_structure_priority": 0,
        "_manual_override_priority": 0,
        "_consolidation_rank": _consolidation_rank(row.get("consolidation")),
        "_period_source": f"2q_beginning_cash_{source_group}",
        "_period_fallback_used": 0,
        "_candidate_validation_status": CANDIDATE_VALIDATION_STATUS_OK,
        "_candidate_validation_issues": "OK",
    }


CONSOLIDATED_REPORTING_GUARD_METRIC_BASES = {
    "NetSales",
    "CostOfSales",
    "GrossProfit",
    "SellingExpenses",
    "GeneralAndAdministrativeExpenses",
    "SellingExpensesOnly",
    "CostOfSalesAndSellingGeneralAndAdministrativeExpenses",
    "OperatingIncome",
    "OrdinaryIncome",
    "ProfitLoss",
    "OperatingCash",
    "InvestmentCash",
    "FinancingCash",
    "TotalAssets",
    "NetAssets",
    "CashAndCashEquivalents",
    "FundingIncome",
    "FeesAndCommissionsIncome",
    "InsuranceClaimsPayments",
    "PolicyReserveProvision",
    "InvestmentExpenses",
    "ProjectExpenses",
    "ProfitBeforeTax",
    "IncomeTaxes",
    "IncomeTaxesCurrentExpense",
    "IncomeTaxesDeferredExpense",
    "InterestBearingLiabilitiesCurrent",
    "InterestBearingLiabilitiesNonCurrent",
    "BondsAndBorrowingsCurrent",
    "BondsAndBorrowingsNonCurrent",
    "BorrowingsCurrent",
    "BorrowingsNonCurrent",
    "LeaseLiabilitiesCurrent",
    "LeaseLiabilitiesNonCurrent",
    "ShortTermLoansPayable",
    "ShortTermLoansPayableToSubsidiariesAndAffiliates",
    "CurrentPortionOfLongTermLoansPayable",
    "LongTermLoansPayable",
    "BondsPayable",
    "CurrentPortionOfBonds",
    "ShortTermBondsPayable",
    "CommercialPapersLiabilities",
    "NumberOfEmployees",
}


def _is_consolidated_reporting_guard_metric(row: dict[str, Any]) -> bool:
    return str(row.get("_metric_base") or "") in CONSOLIDATED_REPORTING_GUARD_METRIC_BASES


def _has_wrong_currency_consolidated_candidate(row: dict[str, Any]) -> bool:
    return (
        row.get("_consolidation_rank", 9999) == 0
        and "unit_mismatch:expected_jpy" in str(row.get("_candidate_validation_issues") or "")
    )


def _filter_enforced_candidates_with_consolidated_currency_guard(
    rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    has_consolidated_reporting = any(
        row.get("_consolidation_rank", 9999) == 0
        and _is_consolidated_reporting_guard_metric(row)
        for row in rows
    )
    blocked_keys = {
        _dedupe_group_key(row)
        for row in rows
        if _has_wrong_currency_consolidated_candidate(row)
    }

    filtered: list[dict[str, Any]] = []
    for row in rows:
        if row.get("_candidate_validation_status") == CANDIDATE_VALIDATION_STATUS_EXCLUDE:
            continue
        if (
            has_consolidated_reporting
            and _is_consolidated_reporting_guard_metric(row)
            and row.get("_consolidation_rank", 9999) > 0
        ):
            continue
        if (
            _dedupe_group_key(row) in blocked_keys
            and row.get("_consolidation_rank", 9999) > 0
        ):
            continue
        filtered.append(row)
    return filtered

def normalize_raw_fact_row(
    row: dict[str, Any],
    *,
    edinet_code: str,
    security_code: str,
    industry_33: str | None = None,
    structure_map: dict[str, dict[str, Any]] | None = None,
    filing_period_end: str | None = None,
    form_type: str | None = None,
    enable_period_fallback: bool = False,
    enforce_candidate_validation: bool = False,
) -> dict[str, Any] | None:
    tag_name = str(row.get("tag_name") or "")
    metric_base = normalize_tag_to_metric(tag_name)
    if not metric_base:
        return None

    context_ref = str(row.get("context_ref") or "")
    suffix_info = _get_suffix_and_period_kind(context_ref)
    period_scope = _period_scope_from_form_type(form_type)
    if period_scope == "half" and metric_base in HALF_DISABLED_NORMALIZED_BASES:
        return None
    period_source = "context_ref"
    if not suffix_info:
        suffix_info = _issued_filing_date_suffix_info(
            metric_base=metric_base,
            tag_name=tag_name,
            context_ref=context_ref,
        )
        period_source = "filing_date_context" if suffix_info else period_source
    if not suffix_info and enable_period_fallback:
        suffix_info = _infer_suffix_and_period_kind_from_dates(
            row,
            filing_period_end=filing_period_end,
            period_scope=period_scope,
        )
        period_source = "period_fallback" if suffix_info else ""
    if not suffix_info:
        return None

    suffix, expected_period_type = suffix_info
    # Issued-share facts in FilingDateInstant contexts are current-period facts
    # whose raw instant is the filing date, not the half-year end date.
    if period_scope == "half" and period_source != "filing_date_context":
        half_suffix = _half_suffix_from_dates(
            row,
            filing_period_end=filing_period_end,
            expected_period_type=expected_period_type,
        )
        if not half_suffix:
            return None
        if half_suffix != suffix:
            suffix = half_suffix
            period_source = "half_period_dates"
    period_type = str(row.get("period_type") or "")
    if period_type != expected_period_type:
        return None

    value_num = _to_number(row.get("value_text"))
    if value_num is None:
        return None
    if tag_name in MONTH_TO_YEAR_TAGS:
        value_num = value_num / 12.0
    if metric_base == "EquityRatio" and abs(value_num) > 1:
        value_num = value_num / 100.0
        if abs(value_num) > 1:
            return None

    consolidation = row.get("consolidation")
    if _is_forbidden_candidate(metric_base, tag_name, consolidation, industry_33=industry_33):
        return None

    period_end = row.get("period_end") or row.get("instant_date")
    if period_source == "filing_date_context" and filing_period_end:
        period_end = filing_period_end
    fiscal_year = _extract_fiscal_year(period_end)
    structure_info = (structure_map or {}).get(tag_name)
    schema = (structure_info or {}).get("schema") or {}
    validation = validate_candidate_for_enforcement(
        metric_base=metric_base,
        row=row,
        schema=schema,
        expected_period_type=expected_period_type,
    )
    if enforce_candidate_validation and validation["status"] == CANDIDATE_VALIDATION_STATUS_EXCLUDE:
        return None

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
        "_structure_priority": _structure_priority(metric_base, tag_name, structure_info),
        "_manual_override_priority": _get_manual_source_tag_priority(metric_base, tag_name),
        "_consolidation_rank": _consolidation_rank(consolidation),
        "_period_source": period_source,
        "_period_fallback_used": int(period_source == "period_fallback"),
        "_candidate_validation_status": validation["status"],
        "_candidate_validation_issues": validation["issues"],
    }


def _clone_normalized_candidate_for_metric_base(
    row: dict[str, Any],
    metric_base: str,
    *,
    structure_info: dict[str, Any] | None,
) -> dict[str, Any]:
    metric_key = str(row.get("metric_key") or "")
    suffix = ""
    for candidate_suffix in sorted(TARGET_CONTEXT_SUFFIXES.values(), key=lambda x: len(x[0]), reverse=True):
        suffix_text = candidate_suffix[0]
        if metric_key.endswith(suffix_text):
            suffix = suffix_text
            break
    cloned = dict(row)
    cloned["metric_key"] = _build_metric_key(metric_base, suffix)
    cloned["_metric_base"] = metric_base
    cloned["_tag_priority"] = _get_source_tag_priority(metric_base, str(row.get("source_tag") or ""))
    cloned["_structure_priority"] = _structure_priority(
        metric_base,
        str(row.get("source_tag") or ""),
        structure_info,
    )
    cloned["_manual_override_priority"] = _get_manual_source_tag_priority(
        metric_base,
        str(row.get("source_tag") or ""),
    )
    return cloned


def _dedupe_group_key(row: dict[str, Any]) -> tuple:
    return (
        row["doc_id"],
        row["metric_key"],
        row["period_end"],
    )


def _dedupe_sort_key(row: dict[str, Any]) -> tuple:
    if str(row.get("_metric_base") or "") in SHARE_COUNT_METRIC_BASES:
        return (
            row.get("_tag_priority", 9999),
            row.get("_structure_priority", 9999),
            row.get("_manual_override_priority", 9999),
            row.get("_consolidation_rank", 9999),
            str(row.get("source_tag") or ""),
        )
    return (
        row.get("_consolidation_rank", 9999),
        row.get("_tag_priority", 9999),
        row.get("_structure_priority", 9999),
        row.get("_manual_override_priority", 9999),
        str(row.get("source_tag") or ""),
    )


def _rewrite_service_operating_expenses_as_cost_of_sales(
    rows: list[dict[str, Any]],
    *,
    structure_map: dict[str, dict[str, Any]] | None = None,
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
            candidate["_structure_priority"] = _structure_priority(
                "CostOfSales",
                "OperatingExpenses",
                (structure_map or {}).get("OperatingExpenses"),
            )
            candidate["_manual_override_priority"] = _get_manual_source_tag_priority(
                "CostOfSales",
                "OperatingExpenses",
            )
        rewritten.append(candidate)
    return rewritten


def build_normalization_candidates(
    raw_rows: list[dict[str, Any]],
    *,
    edinet_code: str,
    security_code: str,
    industry_33: str | None = None,
    xbrl_path: str | None = None,
    zip_path: str | None = None,
    filing_period_end: str | None = None,
    form_type: str | None = None,
    enable_period_fallback: bool = False,
    enforce_candidate_validation: bool = False,
) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    structure_map = analyze_linkbase_structure(
        xbrl_path=xbrl_path,
        zip_path=zip_path,
    )

    effective_filing_period_end = filing_period_end
    if enable_period_fallback and not effective_filing_period_end:
        effective_filing_period_end = _infer_filing_period_end(raw_rows)

    for row in raw_rows:
        normalized = normalize_raw_fact_row(
            row,
            edinet_code=edinet_code,
            security_code=security_code,
            industry_33=industry_33,
            structure_map=structure_map,
            filing_period_end=effective_filing_period_end,
            form_type=form_type,
            enable_period_fallback=enable_period_fallback,
            enforce_candidate_validation=False,
        )
        if normalized is not None:
            candidates.append(normalized)
            tag_name = str(normalized.get("source_tag") or "")
            structure_info = structure_map.get(tag_name)
            for secondary_metric_base in SECONDARY_METRIC_BASES_BY_TAG.get(tag_name, ()):
                if secondary_metric_base == str(normalized.get("_metric_base") or ""):
                    continue
                candidates.append(
                    _clone_normalized_candidate_for_metric_base(
                        normalized,
                        secondary_metric_base,
                        structure_info=structure_info,
                    )
                )

    if enforce_candidate_validation:
        candidates = _filter_enforced_candidates_with_consolidated_currency_guard(candidates)

    candidates = _rewrite_service_operating_expenses_as_cost_of_sales(
        candidates,
        structure_map=structure_map,
    )
    beginning_cash_candidate = _build_2q_beginning_cash_candidate(
        raw_rows,
        filing_period_end=effective_filing_period_end,
        form_type=form_type,
    )
    if beginning_cash_candidate is not None:
        beginning_cash_candidate["edinet_code"] = edinet_code
        beginning_cash_candidate["security_code"] = security_code
        candidates.append(beginning_cash_candidate)

    enriched: list[dict[str, Any]] = []
    for candidate in candidates:
        structure_info = structure_map.get(str(candidate.get("source_tag") or ""))
        classification = classify_structure(
            metric_base=str(candidate.get("_metric_base") or ""),
            tag_name=str(candidate.get("source_tag") or ""),
            structure_info=structure_info,
        )
        enriched_candidate = dict(candidate)
        enriched_candidate["_structure_role"] = classification["role"]
        enriched_candidate["_structure_confidence"] = classification["confidence"]
        enriched_candidate["_structure_is_total"] = classification["is_total"]
        enriched_candidate["_structure_parent_labels"] = classification["presentation_parent_labels"]
        enriched_candidate["_structure_text"] = classification["text"]
        enriched.append(enriched_candidate)

    return enriched


def dedupe_normalized_metrics(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    best_rows = select_best_normalization_candidates(rows)
    out: list[dict[str, Any]] = []

    for row in best_rows:
        cleaned = dict(row)
        cleaned.pop("_metric_base", None)
        cleaned.pop("_tag_priority", None)
        cleaned.pop("_structure_priority", None)
        cleaned.pop("_manual_override_priority", None)
        cleaned.pop("_consolidation_rank", None)
        cleaned.pop("_structure_role", None)
        cleaned.pop("_structure_confidence", None)
        cleaned.pop("_structure_is_total", None)
        cleaned.pop("_structure_parent_labels", None)
        cleaned.pop("_structure_text", None)
        cleaned.pop("_period_source", None)
        cleaned.pop("_period_fallback_used", None)
        cleaned.pop("_candidate_validation_status", None)
        cleaned.pop("_candidate_validation_issues", None)
        out.append(cleaned)

    out.sort(
        key=lambda x: (
            str(x.get("doc_id") or ""),
            str(x.get("metric_key") or ""),
            str(x.get("period_end") or ""),
        )
    )
    return out


def select_best_normalization_candidates(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    best_by_key: dict[tuple, dict[str, Any]] = {}

    for row in rows:
        key = _dedupe_group_key(row)
        current = best_by_key.get(key)

        if current is None:
            best_by_key[key] = row
            continue

        if _dedupe_sort_key(row) < _dedupe_sort_key(current):
            best_by_key[key] = row

    out = list(best_by_key.values())
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
    industry_33: str | None = None,
    xbrl_path: str | None = None,
    zip_path: str | None = None,
    filing_period_end: str | None = None,
    form_type: str | None = None,
    enable_period_fallback: bool = False,
    enforce_candidate_validation: bool = False,
) -> list[dict[str, Any]]:
    candidates = build_normalization_candidates(
        raw_rows,
        edinet_code=edinet_code,
        security_code=security_code,
        industry_33=industry_33,
        xbrl_path=xbrl_path,
        zip_path=zip_path,
        filing_period_end=filing_period_end,
        form_type=form_type,
        enable_period_fallback=enable_period_fallback,
        enforce_candidate_validation=enforce_candidate_validation,
    )
    return dedupe_normalized_metrics(candidates)
