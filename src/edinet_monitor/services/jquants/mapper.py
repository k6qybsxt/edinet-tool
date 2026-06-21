from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
import json
from typing import Any


JQUANTS_RULE_VERSION = "jquants-2026-06-17-v2"
ACTUAL_PERIODS = {"FY", "1Q", "2Q", "3Q"}
FORECAST_TARGETS = ("FY", "2Q")
_UNSET = object()
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
    ("NP", "ProfitLoss", "profit", "yen"),
    ("TA", "TotalAssets", "balance", "yen"),
    ("Eq", "NetAssets", "balance", "yen"),
    ("CFO", "OperatingCash", "cashflow", "yen"),
    ("CFI", "InvestmentCash", "cashflow", "yen"),
    ("CFF", "FinancingCash", "cashflow", "yen"),
    ("CashEq", "CashAndCashEquivalents", "cashflow", "yen"),
    ("ShOutFY", "IssuedShares", "shares", "shares"),
    ("TrShFY", "TreasuryShares", "shares", "shares"),
]
ACTUAL_OFFICIAL_FIELD_MAP = [
    ("EPS", "OfficialEPS", "per_share", "yen_per_share"),
    ("BPS", "OfficialBPS", "per_share", "yen_per_share"),
    ("DEPS", "OfficialDilutedEPS", "per_share", "yen_per_share"),
    ("AvgSh", "AverageShares", "shares", "shares"),
]
ORDINARY_INCOME_ACTUAL_FIELDS = ("OdP",)
PROFIT_BEFORE_TAX_ACTUAL_FIELDS = (
    "ProfitBeforeTax",
    "ProfitLossBeforeTax",
    "IncomeBeforeIncomeTaxes",
    "ProfitBeforeIncomeTaxes",
    "PBT",
)

FORECAST_FIELD_MAP = {
    "FY": [
        (("FSales", "FNCSales"), "NetSales", "sales", "yen"),
        (("FOP", "FNCOP"), "OperatingIncome", "profit", "yen"),
        (("FOdP", "FNCOdP"), "OrdinaryIncome", "profit", "yen"),
        (("FNP", "FNCNP"), "ProfitLoss", "profit", "yen"),
    ],
    "2Q": [
        (("FSales2Q", "FNCSales2Q"), "NetSales", "sales", "yen"),
        (("FOP2Q", "FNCOP2Q"), "OperatingIncome", "profit", "yen"),
        (("FOdP2Q", "FNCOdP2Q"), "OrdinaryIncome", "profit", "yen"),
        (("FNP2Q", "FNCNP2Q"), "ProfitLoss", "profit", "yen"),
    ],
}

INITIAL_FORECAST_FIELD_MAP = {
    "FY": [
        (("NxFSales", "NxFNCSales"), "NetSales", "sales", "yen"),
        (("NxFOP", "NxFNCOP"), "OperatingIncome", "profit", "yen"),
        (("NxFOdP", "NxFNCOdP"), "OrdinaryIncome", "profit", "yen"),
        (("NxFNp", "NxFNCNP"), "ProfitLoss", "profit", "yen"),
    ],
    "2Q": [
        (("NxFSales2Q", "NxFNCSales2Q"), "NetSales", "sales", "yen"),
        (("NxFOP2Q", "NxFNCOP2Q"), "OperatingIncome", "profit", "yen"),
        (("NxFOdP2Q", "NxFNCOdP2Q"), "OrdinaryIncome", "profit", "yen"),
        (("NxFNp2Q", "NxFNCNP2Q"), "ProfitLoss", "profit", "yen"),
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


def _first_decimal(row: dict[str, Any], fields: tuple[str, ...]) -> tuple[str | None, Decimal | None]:
    for field in fields:
        value = _parse_decimal(row.get(field))
        if value is not None:
            return field, value
    return None, None


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
    fiscal_year: int | None | object = _UNSET,
    period_start: str | None = None,
    period_end: str | None = None,
) -> JQuantsStatementMetric:
    raw = build_statement_raw(row)
    decimal_value = _parse_decimal(row.get(field_name))
    calc_status = "ok" if decimal_value is not None else "missing"
    metric_period_start = (
        period_start
        if period_start is not None
        else (
            raw.current_fiscal_year_start_date
            if metric_kind == "forecast"
            else raw.current_period_start_date
        )
    )
    metric_period_end = (
        period_end
        if period_end is not None
        else (
            raw.current_fiscal_year_end_date
            if metric_kind == "forecast"
            else raw.current_period_end_date or raw.current_fiscal_year_end_date
        )
    )
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
        fiscal_year=raw.fiscal_year if fiscal_year is _UNSET else fiscal_year,
        period_start=metric_period_start,
        period_end=metric_period_end,
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
                "period_start": metric_period_start,
                "period_end": metric_period_end,
            }
        ),
    )


def _metric_from_first_available_field(
    row: dict[str, Any],
    *,
    field_names: tuple[str, ...],
    metric_base: str,
    metric_group: str,
    value_unit: str,
    metric_kind: str,
    period_scope: str,
    period_key: str,
    quarter_type: str | None,
    forecast_target: str | None,
    forecast_stage: str | None,
    fiscal_year: int | None | object = _UNSET,
    period_start: str | None = None,
    period_end: str | None = None,
    rule: str = "first available field",
    direct_fields: set[str] | None = None,
) -> JQuantsStatementMetric:
    selected_field = field_names[0]
    selected_value: Decimal | None = None
    for field_name in field_names:
        decimal_value = _parse_decimal(row.get(field_name))
        if decimal_value is not None:
            selected_field = field_name
            selected_value = decimal_value
            break

    raw = build_statement_raw(row)
    calc_status = "ok" if selected_value is not None else "missing"
    metric_period_start = (
        period_start
        if period_start is not None
        else (
            raw.current_fiscal_year_start_date
            if metric_kind == "forecast"
            else raw.current_period_start_date
        )
    )
    metric_period_end = (
        period_end
        if period_end is not None
        else (
            raw.current_fiscal_year_end_date
            if metric_kind == "forecast"
            else raw.current_period_end_date or raw.current_fiscal_year_end_date
        )
    )
    selected_field_text = selected_field if selected_value is not None else None
    semantic_status = None
    if selected_field_text is not None:
        semantic_status = (
            "direct"
            if direct_fields is None or selected_field_text in direct_fields
            else "proxy"
        )
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
        fiscal_year=raw.fiscal_year if fiscal_year is _UNSET else fiscal_year,
        period_start=metric_period_start,
        period_end=metric_period_end,
        disclosed_date=raw.disclosed_date,
        disclosed_time=raw.disclosed_time,
        metric_key=f"{metric_base}Current",
        metric_base=metric_base,
        metric_group=metric_group,
        value_num=_to_float(selected_value),
        value_unit=value_unit,
        calc_status=calc_status,
        source_field=selected_field_text if selected_field_text is not None else "|".join(field_names),
        source_detail_json=_json_dumps(
            {
                "source": "jquants",
                "api_version": "v2",
                "field": selected_field_text,
                "candidate_fields": list(field_names),
                "candidate_raw_values": {
                    field_name: row.get(field_name) for field_name in field_names
                },
                "semantic_status": semantic_status,
                "forecast_stage": forecast_stage,
                "forecast_target": forecast_target,
                "period_start": metric_period_start,
                "period_end": metric_period_end,
                "rule": rule,
            }
        ),
    )


def _forecast_fields_for_stage(
    row: dict[str, Any],
    forecast_stage: str,
    forecast_target: str,
) -> tuple[list[tuple[str, str, str, str]], int | None, str | None, str | None]:
    if forecast_stage == "initial":
        period_start = _normalize_date(row.get("NxtFYSt"))
        period_end = _normalize_date(row.get("NxtFYEn"))
        fiscal_year = _date_year(period_end)
        return (
            INITIAL_FORECAST_FIELD_MAP.get(forecast_target, []),
            fiscal_year,
            period_start,
            period_end,
        )
    raw = build_statement_raw(row)
    return (
        FORECAST_FIELD_MAP.get(forecast_target, []),
        raw.fiscal_year,
        raw.current_fiscal_year_start_date,
        raw.current_fiscal_year_end_date,
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
        for field_name, metric_base, metric_group, value_unit in ACTUAL_OFFICIAL_FIELD_MAP:
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
        metrics.append(
            _metric_from_first_available_field(
                row,
                field_names=ORDINARY_INCOME_ACTUAL_FIELDS,
                metric_base="OrdinaryIncome",
                metric_group="profit",
                value_unit="yen",
                metric_kind="actual",
                period_scope="quarter",
                period_key=f"actual:{period}",
                quarter_type=period,
                forecast_target=None,
                forecast_stage=None,
                rule="ordinary income field only",
                direct_fields={"OdP"},
            )
        )
        metrics.append(
            _metric_from_first_available_field(
                row,
                field_names=PROFIT_BEFORE_TAX_ACTUAL_FIELDS,
                metric_base="ProfitBeforeTax",
                metric_group="profit",
                value_unit="yen",
                metric_kind="actual",
                period_scope="quarter",
                period_key=f"actual:{period}",
                quarter_type=period,
                forecast_target=None,
                forecast_stage=None,
                rule="first available profit before tax field",
            )
        )
        metrics.append(_calculated_combined_expense_metric(row, period))
        metrics.append(_calculated_equity_ratio_metric(row, period))
        metrics.append(_outstanding_shares_metric(row, period))
        metrics.append(_calculated_eps_metric(row, period))
        metrics.append(_calculated_bps_metric(row, period))

    if include_forecasts:
        forecast_stage = FORECAST_STAGE_BY_PERIOD.get(period)
        if forecast_stage is not None:
            for forecast_target in FORECAST_TARGETS:
                fields, fiscal_year, period_start, period_end = _forecast_fields_for_stage(
                    row,
                    forecast_stage,
                    forecast_target,
                )
                for field_names, metric_base, metric_group, value_unit in fields:
                    metrics.append(
                        _metric_from_first_available_field(
                            row,
                            field_names=field_names,
                            metric_base=metric_base,
                            metric_group=metric_group,
                            value_unit=value_unit,
                            metric_kind="forecast",
                            period_scope="forecast",
                            period_key=f"forecast:{forecast_target}",
                            quarter_type=None,
                            forecast_target=forecast_target,
                            forecast_stage=forecast_stage,
                            fiscal_year=fiscal_year,
                            period_start=period_start,
                            period_end=period_end,
                            rule="prefer consolidated forecast field; fall back to non-consolidated forecast field when consolidated is blank",
                            direct_fields={field_names[0]},
                        )
                    )

    return metrics


def _outstanding_shares_value(row: dict[str, Any]) -> tuple[Decimal | None, str]:
    issued = _parse_decimal(row.get("ShOutFY"))
    treasury = _parse_decimal(row.get("TrShFY"))
    if issued is None:
        return None, "missing"
    treasury_value = treasury or Decimal("0")
    if Decimal("0") <= treasury_value < Decimal("1000"):
        treasury_value = Decimal("0")
    outstanding = issued - treasury_value
    if outstanding <= 0:
        return None, "invalid_input"
    return outstanding, "ok"


def _outstanding_shares_metric(row: dict[str, Any], period: str) -> JQuantsStatementMetric:
    issued_field = "ShOutFY"
    treasury_field = "TrShFY"
    value, calc_status = _outstanding_shares_value(row)
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
                "issued_raw_value": row.get(issued_field),
                "treasury_raw_value": row.get(treasury_field),
                "rule": "OutstandingShares = ShOutFY - TrShFY; blank or less-than-1000 treasury shares are treated as zero",
            }
        ),
    )


def _calculated_equity_ratio_metric(row: dict[str, Any], period: str) -> JQuantsStatementMetric:
    net_assets = _parse_decimal(row.get("Eq"))
    total_assets = _parse_decimal(row.get("TA"))
    value: Decimal | None = None
    calc_status = "missing_input"
    if net_assets is not None and total_assets is not None:
        if total_assets == 0:
            calc_status = "division_by_zero"
        else:
            value = net_assets / total_assets
            calc_status = "ok"
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
        metric_key="EquityRatioCurrent",
        metric_base="EquityRatio",
        metric_group="ratio",
        value_num=_to_float(value),
        value_unit="ratio",
        calc_status=calc_status,
        source_field="calculated:Eq/TA",
        source_detail_json=_json_dumps(
            {
                "source": "jquants",
                "api_version": "v2",
                "net_assets_field": "Eq",
                "total_assets_field": "TA",
                "net_assets_raw_value": row.get("Eq"),
                "total_assets_raw_value": row.get("TA"),
                "raw_equity_ratio_field": "EqAR",
                "raw_equity_ratio_value": row.get("EqAR"),
                "rule": "EquityRatio = NetAssets / TotalAssets",
            }
        ),
    )


def _calculated_combined_expense_metric(row: dict[str, Any], period: str) -> JQuantsStatementMetric:
    net_sales = _parse_decimal(row.get("Sales"))
    operating_income = _parse_decimal(row.get("OP"))
    value: Decimal | None = None
    calc_status = "missing_input"
    if net_sales is not None and operating_income is not None:
        value = net_sales - operating_income
        calc_status = "ok"
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
        metric_key="CostOfSalesAndSellingGeneralAndAdministrativeExpensesCurrent",
        metric_base="CostOfSalesAndSellingGeneralAndAdministrativeExpenses",
        metric_group="profitability",
        value_num=_to_float(value),
        value_unit="yen",
        calc_status=calc_status,
        source_field="calculated:Sales-OP",
        source_detail_json=_json_dumps(
            {
                "source": "jquants",
                "api_version": "v2",
                "net_sales_field": "Sales",
                "operating_income_field": "OP",
                "net_sales_raw_value": row.get("Sales"),
                "operating_income_raw_value": row.get("OP"),
                "rule": "CostOfSalesAndSellingGeneralAndAdministrativeExpenses = NetSales - OperatingIncome",
            }
        ),
    )


def _estimated_profit_for_eps(
    row: dict[str, Any],
) -> tuple[Decimal | None, str | None, str | None, str | None]:
    field, profit_before_tax = _first_decimal(row, PROFIT_BEFORE_TAX_ACTUAL_FIELDS)
    if profit_before_tax is not None:
        return (
            profit_before_tax * Decimal("0.7"),
            "ProfitBeforeTax",
            field,
            "estimated_profit_before_tax",
        )
    ordinary_income = _parse_decimal(row.get("OdP"))
    if ordinary_income is not None:
        return (
            ordinary_income * Decimal("0.7"),
            "OrdinaryIncome",
            "OdP",
            "estimated_net_income",
        )
    return None, None, None, None


def _calculated_eps_metric(row: dict[str, Any], period: str) -> JQuantsStatementMetric:
    estimated_profit, profit_base, profit_field, estimated_profit_label = _estimated_profit_for_eps(row)
    outstanding_shares, shares_status = _outstanding_shares_value(row)
    value: Decimal | None = None
    calc_status = "missing"
    if estimated_profit is not None and outstanding_shares is not None and outstanding_shares > 0:
        value = estimated_profit / outstanding_shares
        calc_status = "ok"
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
        metric_key="EPSCurrent",
        metric_base="EPS",
        metric_group="per_share",
        value_num=_to_float(value),
        value_unit="yen_per_share",
        calc_status=calc_status,
        source_field=(
            f"calculated:{profit_field}*0.7/OutstandingShares"
            if profit_field
            else "calculated:estimated_profit/OutstandingShares"
        ),
        source_detail_json=_json_dumps(
            {
                "source": "jquants",
                "api_version": "v2",
                "profit_base": profit_base,
                "profit_field": profit_field,
                "profit_raw_value": row.get(profit_field) if profit_field else None,
                "estimated_profit_value": _to_float(estimated_profit),
                "estimated_profit_label": estimated_profit_label,
                "shares_status": shares_status,
                "rule": "EPS = estimated_profit / OutstandingShares; estimated_profit = ProfitBeforeTax * 0.7 when available, otherwise OrdinaryIncome * 0.7",
            }
        ),
    )


def _calculated_bps_metric(row: dict[str, Any], period: str) -> JQuantsStatementMetric:
    net_assets = _parse_decimal(row.get("Eq"))
    outstanding_shares, shares_status = _outstanding_shares_value(row)
    value: Decimal | None = None
    calc_status = "missing"
    if net_assets is not None and outstanding_shares is not None and outstanding_shares > 0:
        value = net_assets / outstanding_shares
        calc_status = "ok"
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
        metric_key="BPSCurrent",
        metric_base="BPS",
        metric_group="per_share",
        value_num=_to_float(value),
        value_unit="yen_per_share",
        calc_status=calc_status,
        source_field="calculated:Eq/OutstandingShares",
        source_detail_json=_json_dumps(
            {
                "source": "jquants",
                "api_version": "v2",
                "net_assets_field": "Eq",
                "shares_status": shares_status,
                "rule": "BPS = NetAssets / OutstandingShares",
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
