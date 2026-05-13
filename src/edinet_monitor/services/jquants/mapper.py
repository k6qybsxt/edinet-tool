from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
import json
from typing import Any


JQUANTS_RULE_VERSION = "jquants-2026-05-06-v2"
ACTUAL_PERIODS = {"1Q", "3Q"}
FORECAST_TARGETS = {"FY"}
FORECAST_STAGE_BY_PERIOD = {
    "FY": "initial",
    "4Q": "initial",
    "1Q": "1Q",
    "2Q": "2Q",
    "3Q": "3Q",
}


@dataclass(frozen=True)
class JQuantsStatementMetric:
    disclosure_number: str
    local_code: str
    security_code: str
    metric_kind: str
    period_scope: str
    period_key: str
    quarter_type: str | None
    forecast_target: str | None
    forecast_stage: str | None
    fiscal_year: int | None
    period_start: str
    period_end: str
    disclosed_date: str
    disclosed_time: str
    metric_key: str
    metric_base: str
    metric_group: str
    value_num: float | None
    value_unit: str
    calc_status: str
    source_field: str
    source_detail_json: str
    rule_version: str = JQUANTS_RULE_VERSION


@dataclass(frozen=True)
class JQuantsStatementRaw:
    disclosure_number: str
    disclosed_date: str
    disclosed_time: str
    local_code: str
    security_code: str
    type_of_document: str
    type_of_current_period: str
    current_period_start_date: str
    current_period_end_date: str
    current_fiscal_year_start_date: str
    current_fiscal_year_end_date: str
    fiscal_year: int | None
    raw_json: str


@dataclass(frozen=True)
class JQuantsQuote:
    local_code: str
    security_code: str
    trade_date: str
    open: float | None
    high: float | None
    low: float | None
    close: float | None
    volume: float | None
    turnover_value: float | None
    adjustment_factor: float | None
    adjustment_open: float | None
    adjustment_high: float | None
    adjustment_low: float | None
    adjustment_close: float | None
    adjustment_close_rounded: float | None
    adjustment_volume: float | None
    raw_json: str


ACTUAL_FIELD_MAP = [
    ("Sales", "NetSales", "sales", "yen"),
    ("OP", "OperatingIncome", "profit", "yen"),
    ("OdP", "OrdinaryIncome", "profit", "yen"),
    ("NP", "ProfitLoss", "profit", "yen"),
    ("EPS", "EPS", "per_share", "yen_per_share"),
    ("TA", "TotalAssets", "balance", "yen"),
    ("Eq", "NetAssets", "balance", "yen"),
    ("EqAR", "EquityRatio", "ratio", "ratio"),
    ("BPS", "BPS", "per_share", "yen_per_share"),
    ("CFO", "OperatingCash", "cashflow", "yen"),
    ("CFI", "InvestmentCash", "cashflow", "yen"),
    ("CFF", "FinancingCash", "cashflow", "yen"),
    ("CashEq", "CashAndCashEquivalents", "cashflow", "yen"),
    ("ShOutFY", "IssuedShares", "shares", "shares"),
    ("TrShFY", "TreasuryShares", "shares", "shares"),
]

FORECAST_FIELD_MAP = {
    "FY": [
        ("FSales", "NetSales", "sales", "yen"),
        ("FOP", "OperatingIncome", "profit", "yen"),
        ("FOdP", "OrdinaryIncome", "profit", "yen"),
        ("FNP", "ProfitLoss", "profit", "yen"),
    ],
}


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def normalize_security_code(local_code: str | None) -> str:
    text = str(local_code or "").strip()
    if len(text) == 5 and text.endswith("0"):
        return text[:-1]
    return text


def _parse_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return Decimal(text.replace(",", ""))
    except InvalidOperation:
        return None


def _to_float(value: Decimal | None) -> float | None:
    return float(value) if value is not None else None


def _date_year(date_text: str | None) -> int | None:
    text = str(date_text or "").strip()
    if len(text) < 4:
        return None
    try:
        return int(text[:4])
    except ValueError:
        return None


def _normalize_date(value: Any) -> str:
    text = str(value or "").strip().replace("/", "-")
    if len(text) == 8 and text.isdigit():
        return f"{text[:4]}-{text[4:6]}-{text[6:8]}"
    return text


def _stable_disclosure_number(row: dict[str, Any]) -> str:
    disclosure_number = str(row.get("DiscNo") or "").strip()
    if disclosure_number:
        return disclosure_number
    parts = [
        str(row.get("Code") or ""),
        _normalize_date(row.get("DiscDate")),
        str(row.get("CurPerType") or ""),
        _normalize_date(row.get("CurPerEn")),
        str(row.get("DocType") or ""),
    ]
    return "generated:" + "|".join(parts)


def build_statement_raw(row: dict[str, Any]) -> JQuantsStatementRaw:
    local_code = str(row.get("Code") or "")
    fiscal_year = _date_year(_normalize_date(row.get("CurFYEn")))
    disclosure_number = _stable_disclosure_number(row)
    return JQuantsStatementRaw(
        disclosure_number=disclosure_number,
        disclosed_date=_normalize_date(row.get("DiscDate")),
        disclosed_time=str(row.get("DiscTime") or ""),
        local_code=local_code,
        security_code=normalize_security_code(local_code),
        type_of_document=str(row.get("DocType") or ""),
        type_of_current_period=str(row.get("CurPerType") or ""),
        current_period_start_date=_normalize_date(row.get("CurPerSt")),
        current_period_end_date=_normalize_date(row.get("CurPerEn")),
        current_fiscal_year_start_date=_normalize_date(row.get("CurFYSt")),
        current_fiscal_year_end_date=_normalize_date(row.get("CurFYEn")),
        fiscal_year=fiscal_year,
        raw_json=_json_dumps(row),
    )


def _metric_from_field(
    row: dict[str, Any],
    *,
    field_name: str,
    metric_base: str,
    metric_group: str,
    value_unit: str,
    metric_kind: str,
    period_scope: str,
    period_key: str,
    quarter_type: str | None,
    forecast_target: str | None,
    forecast_stage: str | None,
) -> JQuantsStatementMetric:
    raw = build_statement_raw(row)
    decimal_value = _parse_decimal(row.get(field_name))
    calc_status = "ok" if decimal_value is not None else "missing"
    return JQuantsStatementMetric(
        disclosure_number=raw.disclosure_number,
        local_code=raw.local_code,
        security_code=raw.security_code,
        metric_kind=metric_kind,
        period_scope=period_scope,
        period_key=period_key,
        quarter_type=quarter_type,
        forecast_target=forecast_target,
        forecast_stage=forecast_stage,
        fiscal_year=raw.fiscal_year,
        period_start=(
            raw.current_fiscal_year_start_date
            if metric_kind == "forecast"
            else raw.current_period_start_date
        ),
        period_end=(
            raw.current_fiscal_year_end_date
            if metric_kind == "forecast"
            else raw.current_period_end_date or raw.current_fiscal_year_end_date
        ),
        disclosed_date=raw.disclosed_date,
        disclosed_time=raw.disclosed_time,
        metric_key=f"{metric_base}Current",
        metric_base=metric_base,
        metric_group=metric_group,
        value_num=_to_float(decimal_value),
        value_unit=value_unit,
        calc_status=calc_status,
        source_field=field_name,
        source_detail_json=_json_dumps(
            {
                "source": "jquants",
                "api_version": "v2",
                "field": field_name,
                "forecast_stage": forecast_stage,
            }
        ),
    )


def statement_metrics_from_row(
    row: dict[str, Any],
    *,
    periods: set[str] | None = None,
    include_forecasts: bool = True,
) -> list[JQuantsStatementMetric]:
    period = str(row.get("CurPerType") or "")
    allowed_periods = periods or ACTUAL_PERIODS
    metrics: list[JQuantsStatementMetric] = []

    if period in allowed_periods:
        for field_name, metric_base, metric_group, value_unit in ACTUAL_FIELD_MAP:
            metrics.append(
                _metric_from_field(
                    row,
                    field_name=field_name,
                    metric_base=metric_base,
                    metric_group=metric_group,
                    value_unit=value_unit,
                    metric_kind="actual",
                    period_scope="quarter",
                    period_key=f"actual:{period}",
                    quarter_type=period,
                    forecast_target=None,
                    forecast_stage=None,
                )
            )
        metrics.append(_outstanding_shares_metric(row, period))

    if include_forecasts:
        forecast_stage = FORECAST_STAGE_BY_PERIOD.get(period)
        if forecast_stage is not None:
            for forecast_target, fields in FORECAST_FIELD_MAP.items():
                for field_name, metric_base, metric_group, value_unit in fields:
                    metrics.append(
                        _metric_from_field(
                            row,
                            field_name=field_name,
                            metric_base=metric_base,
                            metric_group=metric_group,
                            value_unit=value_unit,
                            metric_kind="forecast",
                            period_scope="forecast",
                            period_key=f"forecast:{forecast_target}",
                            quarter_type=None,
                            forecast_target=forecast_target,
                            forecast_stage=forecast_stage,
                        )
                    )

    return metrics


def _outstanding_shares_metric(row: dict[str, Any], period: str) -> JQuantsStatementMetric:
    issued_field = "ShOutFY"
    treasury_field = "TrShFY"
    issued = _parse_decimal(row.get(issued_field))
    treasury = _parse_decimal(row.get(treasury_field))
    calc_status = "ok"
    value: Decimal | None
    if issued is None:
        value = None
        calc_status = "missing"
    else:
        treasury_value = treasury or Decimal("0")
        if Decimal("0") <= treasury_value < Decimal("1000"):
            treasury_value = Decimal("0")
        value = issued - treasury_value
    raw = build_statement_raw(row)
    return JQuantsStatementMetric(
        disclosure_number=raw.disclosure_number,
        local_code=raw.local_code,
        security_code=raw.security_code,
        metric_kind="actual",
        period_scope="quarter",
        period_key=f"actual:{period}",
        quarter_type=period,
        forecast_target=None,
        forecast_stage=None,
        fiscal_year=raw.fiscal_year,
        period_start=raw.current_period_start_date,
        period_end=raw.current_period_end_date or raw.current_fiscal_year_end_date,
        disclosed_date=raw.disclosed_date,
        disclosed_time=raw.disclosed_time,
        metric_key="OutstandingSharesCurrent",
        metric_base="OutstandingShares",
        metric_group="shares",
        value_num=_to_float(value),
        value_unit="shares",
        calc_status=calc_status,
        source_field=f"{issued_field}-{treasury_field}",
        source_detail_json=_json_dumps(
            {
                "source": "jquants",
                "api_version": "v2",
                "issued_field": issued_field,
                "treasury_field": treasury_field,
            }
        ),
    )


def quote_from_row(row: dict[str, Any]) -> JQuantsQuote:
    local_code = str(row.get("Code") or "")
    adjustment_close = _parse_decimal(row.get("AdjC"))
    rounded = (
        adjustment_close.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        if adjustment_close is not None
        else None
    )
    return JQuantsQuote(
        local_code=local_code,
        security_code=normalize_security_code(local_code),
        trade_date=_normalize_date(row.get("Date")),
        open=_to_float(_parse_decimal(row.get("O"))),
        high=_to_float(_parse_decimal(row.get("H"))),
        low=_to_float(_parse_decimal(row.get("L"))),
        close=_to_float(_parse_decimal(row.get("C"))),
        volume=_to_float(_parse_decimal(row.get("Vo"))),
        turnover_value=_to_float(_parse_decimal(row.get("Va"))),
        adjustment_factor=_to_float(_parse_decimal(row.get("AdjFactor"))),
        adjustment_open=_to_float(_parse_decimal(row.get("AdjO"))),
        adjustment_high=_to_float(_parse_decimal(row.get("AdjH"))),
        adjustment_low=_to_float(_parse_decimal(row.get("AdjL"))),
        adjustment_close=_to_float(adjustment_close),
        adjustment_close_rounded=_to_float(rounded),
        adjustment_volume=_to_float(_parse_decimal(row.get("AdjVo"))),
        raw_json=_json_dumps(row),
    )
