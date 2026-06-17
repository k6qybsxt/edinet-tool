from __future__ import annotations


JQUANTS_ACTUAL_FINANCIAL_QUARTERS = ("1Q", "3Q")
EDINET_ACTUAL_FINANCIAL_QUARTERS = ("2Q", "4Q")
EDINET_SEGMENT_QUARTERS = ("2Q", "4Q")
JQUANTS_FORECAST_STAGES = ("initial", "1Q", "2Q", "3Q")


def normalize_quarter_label(value: str | None) -> str:
    return str(value or "").strip().upper()


def actual_financial_source_for_quarter(quarter: str | None) -> str | None:
    normalized = normalize_quarter_label(quarter)
    if normalized in JQUANTS_ACTUAL_FINANCIAL_QUARTERS:
        return "jquants"
    if normalized in EDINET_ACTUAL_FINANCIAL_QUARTERS:
        return "edinet"
    return None


def uses_jquants_actual_financial(quarter: str | None) -> bool:
    return actual_financial_source_for_quarter(quarter) == "jquants"


def uses_edinet_actual_financial(quarter: str | None) -> bool:
    return actual_financial_source_for_quarter(quarter) == "edinet"


def uses_edinet_segment(quarter: str | None) -> bool:
    return normalize_quarter_label(quarter) in EDINET_SEGMENT_QUARTERS


def uses_jquants_forecast(stage: str | None) -> bool:
    return str(stage or "").strip() in JQUANTS_FORECAST_STAGES
