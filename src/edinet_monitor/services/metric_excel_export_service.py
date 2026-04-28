from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
import re
import sqlite3
from typing import Any

from openpyxl import Workbook, load_workbook
from openpyxl.styles import Font, PatternFill
from openpyxl.utils import get_column_letter

from edinet_pipeline.domain.metric_labels import (
    BANK_INDUSTRY_LABEL,
    INSURANCE_INDUSTRY_LABEL,
    METRIC_BASE_LABELS,
    SECURITIES_INDUSTRY_LABEL,
    metric_base_to_display_name,
)


GENERAL_SHEET = "一般企業"
SUMMARY_SHEET = "summary"
VERTICAL_DATA_SHEET = "\u30c7\u30fc\u30bf\u7528_\u7e26"
CONDITION_SHEET = "条件"

SHEET_ORDER = [
    GENERAL_SHEET,
    BANK_INDUSTRY_LABEL,
    SECURITIES_INDUSTRY_LABEL,
    INSURANCE_INDUSTRY_LABEL,
]

PERIOD_LABEL_BY_OFFSET = {
    0: "当期",
    1: "前期",
    2: "2期前",
    3: "3期前",
    4: "4期前",
    5: "5期前",
    6: "6期前",
    7: "7期前",
    8: "8期前",
    9: "9期前",
    10: "10期前",
}
OFFSET_BY_PERIOD_LABEL = {label: offset for offset, label in PERIOD_LABEL_BY_OFFSET.items()}

ABSORBED_RATIO_BASE_BY_ROW_BASE = {
    "NetSales": None,
    "GrossProfit": "GrossProfitMargin",
    "CostOfSales": "CostOfSalesRatio",
    "SellingExpenses": "SellingExpensesRatio",
    "OperatingIncome": "OperatingMargin",
    "OrdinaryIncome": "OrdinaryIncomeMargin",
    "ProfitLoss": "EstimatedNetMargin",
    "TotalAssets": None,
    "NetAssets": "EquityRatio",
}

CONSTANT_RATIO_BY_ROW_BASE = {
    "NetSales": 1.0,
    "TotalAssets": 1.0,
}

ROW_BASE_BY_RATIO_BASE = {
    ratio_base: row_base
    for row_base, ratio_base in ABSORBED_RATIO_BASE_BY_ROW_BASE.items()
    if ratio_base
}

ABSORBED_RATIO_BASES = set(ROW_BASE_BY_RATIO_BASE)

PERIOD_SCOPE_BY_FORM_TYPE = {
    "030000": "annual",
    "043A00": "half",
}
FORM_TYPES_BY_PERIOD_SCOPE = {
    "annual": ("030000",),
    "half": ("043A00",),
}
PERIOD_SCOPE_LABEL_BY_FORM_TYPE = {
    "030000": "\u901a\u671f",
    "043A00": "\u534a\u671f",
}
HALF_ONLY_BASES = {
    "HalfNetSalesProgressRate",
    "HalfOrdinaryIncomeProgressRate",
    "HalfProfitProgressRate",
}
HALF_PREFIX = "\u534a\u671f "
HALF_YOY_PREFIX = "\u534a\u671f\u524d\u5e74\u6bd4 "

MONETARY_BASES = {
    "NetSales",
    "CostOfSales",
    "CostOfSalesAndSellingGeneralAndAdministrativeExpenses",
    "SellingExpenses",
    "GeneralAndAdministrativeExpenses",
    "SellingExpensesOnly",
    "OperatingIncome",
    "OrdinaryIncome",
    "ProfitLoss",
    "OperatingCash",
    "InvestmentCash",
    "FinancingCash",
    "TotalAssets",
    "NetAssets",
    "BeginningCashBalance",
    "CashAndCashEquivalents",
    "GrossProfit",
    "EstimatedNetIncome",
    "FCF",
    "FundingIncome",
    "FeesAndCommissionsIncome",
    "InsuranceClaimsPayments",
    "PolicyReserveProvision",
    "InvestmentExpenses",
    "ProjectExpenses",
}

PERCENT_VALUE_BASES = {"ROA", "ROE", "EquityRatio"} | ABSORBED_RATIO_BASES
RATIO_VALUE_BASES = {
    "TheoreticalPBR",
    "TheoreticalPER",
    "TheoreticalPCFR",
}

PER_SHARE_BASES = {
    "AssetsPerShare",
    "LiabilitiesPerShare",
    "OperatingCashPerShare",
    "InvestmentCashPerShare",
    "FinancingCashPerShare",
    "FCFPerShare",
    "AssetValue",
    "BusinessValue",
    "TheoreticalSharePrice",
    "UpperBoundTheoreticalSharePrice",
}

ONE_DECIMAL_VALUE_BASES = {
    "EPS",
    "BPS",
    "TheoreticalPBR",
    "TheoreticalPER",
    "TheoreticalPCFR",
}

INTEGER_VALUE_BASES = {
    "OutstandingShares",
    "AssetsPerShare",
    "LiabilitiesPerShare",
    "AssetValue",
    "BusinessValue",
    "TheoreticalSharePrice",
    "OperatingCashPerShare",
    "InvestmentCashPerShare",
    "FinancingCashPerShare",
    "FCFPerShare",
}

GROWTH_RATIO_BASES = {
    "NetSalesGrowthRate",
    "NetSalesGrowthRate5Year",
    "NetSalesGrowthRate10Year",
    "OrdinaryIncomeGrowthRate",
    "OrdinaryIncomeGrowthRate5Year",
    "OrdinaryIncomeGrowthRate10Year",
    "CashBalanceGrowthRate",
    "CashBalanceGrowthRate5Year",
    "CashBalanceGrowthRate10Year",
    "OutstandingSharesGrowthRate",
    "OutstandingSharesGrowthRate5Year",
    "OutstandingSharesGrowthRate10Year",
    "EPSGrowthRate",
    "TheoreticalSharePriceGrowthRate",
    "TheoreticalSharePriceGrowthRate5Year",
    "TheoreticalSharePriceGrowthRate10Year",
    "HalfNetSalesProgressRate",
    "HalfOrdinaryIncomeProgressRate",
    "HalfProfitProgressRate",
}

SPARSE_PERIOD_OFFSETS_BY_BASE = {
    "NetSalesGrowthRate5Year": {5, 0},
    "OrdinaryIncomeGrowthRate5Year": {5, 0},
    "CashBalanceGrowthRate5Year": {5, 0},
    "OutstandingSharesGrowthRate5Year": {5, 0},
    "TheoreticalSharePriceGrowthRate5Year": {5, 0},
    "NetSalesGrowthRate10Year": {0},
    "OrdinaryIncomeGrowthRate10Year": {0},
    "CashBalanceGrowthRate10Year": {0},
    "OutstandingSharesGrowthRate10Year": {0},
    "TheoreticalSharePriceGrowthRate10Year": {0},
}

FIXED_ROW_BASE_ORDER = [
    "NetSales",
    "GrossProfit",
    "CostOfSalesAndSellingGeneralAndAdministrativeExpenses",
    "CostOfSales",
    "SellingExpenses",
    "GeneralAndAdministrativeExpenses",
    "SellingExpensesOnly",
    "OperatingIncome",
    "OrdinaryIncome",
    "ProfitLoss",
    "EstimatedNetIncome",
    "TotalAssets",
    "NetAssets",
    "BeginningCashBalance",
    "CashAndCashEquivalents",
    "OperatingCash",
    "InvestmentCash",
    "FinancingCash",
    "FCF",
    "OutstandingShares",
    "EPS",
    "EPSGrowthRate",
    "BPS",
    "HalfNetSalesProgressRate",
    "HalfOrdinaryIncomeProgressRate",
    "HalfProfitProgressRate",
    "NetSalesGrowthRate",
    "NetSalesGrowthRate5Year",
    "NetSalesGrowthRate10Year",
    "OrdinaryIncomeGrowthRate",
    "OrdinaryIncomeGrowthRate5Year",
    "OrdinaryIncomeGrowthRate10Year",
    "CashBalanceGrowthRate",
    "CashBalanceGrowthRate5Year",
    "CashBalanceGrowthRate10Year",
    "OutstandingSharesGrowthRate",
    "OutstandingSharesGrowthRate5Year",
    "OutstandingSharesGrowthRate10Year",
    "AssetsPerShare",
    "LiabilitiesPerShare",
    "ROA",
    "ROE",
    "EquityRatio",
    "AssetValue",
    "BusinessValue",
    "TheoreticalSharePrice",
    "TheoreticalSharePriceGrowthRate",
    "TheoreticalSharePriceGrowthRate5Year",
    "TheoreticalSharePriceGrowthRate10Year",
    "TheoreticalPBR",
    "TheoreticalPER",
    "OperatingCashPerShare",
    "InvestmentCashPerShare",
    "FinancingCashPerShare",
    "FCFPerShare",
    "TheoreticalPCFR",
]

ROW_BASE_ORDER_INDEX = {
    metric_base: index for index, metric_base in enumerate(FIXED_ROW_BASE_ORDER)
}

EXCEL_METRIC_LABEL_OVERRIDES = {
    "CostOfSales": "├売上原価",
    "SellingExpenses": "└販管費",
    "GeneralAndAdministrativeExpenses": "　├ 一般管理費",
    "SellingExpensesOnly": "　└ 販売費",
    "OperatingCash": "営業cf",
    "InvestmentCash": "投資cf",
    "FinancingCash": "財務cf",
    "EPSGrowthRate": "EPS増加率（前期比）",
    "TheoreticalSharePriceGrowthRate": "理論株価上昇率",
    "TheoreticalSharePriceGrowthRate5Year": "理論株価上昇率(５年)",
    "TheoreticalSharePriceGrowthRate10Year": "理論株価上昇率(10年)",
}

INDUSTRY_EXCEL_METRIC_LABEL_OVERRIDES = {
    BANK_INDUSTRY_LABEL: {
        "CostOfSales": "├資金調達費用",
        "SellingExpenses": "└営業経費",
        "FeesAndCommissionsIncome": "役務取引等収益",
    },
    SECURITIES_INDUSTRY_LABEL: {
        "GrossProfit": "純収益",
        "CostOfSales": "├金融費用",
        "SellingExpenses": "└販管費",
        "GeneralAndAdministrativeExpenses": "　├ 一般管理費",
        "SellingExpensesOnly": "　└ 販売費",
    },
    INSURANCE_INDUSTRY_LABEL: {
        "InsuranceClaimsPayments": "├保険金等支払金",
        "PolicyReserveProvision": "├責任準備金等繰入額",
        "InvestmentExpenses": "├資産運用費用",
        "ProjectExpenses": "└ 事業費",
    },
}

COMMON_TAIL_BASES = [
    base
    for base in FIXED_ROW_BASE_ORDER
    if base
    not in {
        "NetSales",
        "GrossProfit",
        "CostOfSalesAndSellingGeneralAndAdministrativeExpenses",
        "CostOfSales",
        "SellingExpenses",
        "GeneralAndAdministrativeExpenses",
        "SellingExpensesOnly",
        "OperatingIncome",
        "OrdinaryIncome",
        "ProfitLoss",
        "TotalAssets",
        "NetAssets",
        "BeginningCashBalance",
        "CashAndCashEquivalents",
        "OperatingCash",
        "InvestmentCash",
        "FinancingCash",
        "FCF",
    }
]

POST_BPS_TAIL_BASES = FIXED_ROW_BASE_ORDER[FIXED_ROW_BASE_ORDER.index("BPS") + 1 :]

DEFAULT_BASES_BY_SHEET = {
    GENERAL_SHEET: list(FIXED_ROW_BASE_ORDER),
    BANK_INDUSTRY_LABEL: [
        "NetSales",
        "CostOfSalesAndSellingGeneralAndAdministrativeExpenses",
        "CostOfSales",
        "SellingExpenses",
        "FeesAndCommissionsIncome",
        "OperatingIncome",
        "OrdinaryIncome",
        "ProfitLoss",
        "EstimatedNetIncome",
        "TotalAssets",
        "NetAssets",
        "BeginningCashBalance",
        "CashAndCashEquivalents",
        "OperatingCash",
        "InvestmentCash",
        "FinancingCash",
        "FCF",
        "OutstandingShares",
        "EPS",
        "EPSGrowthRate",
        "BPS",
    ]
    + POST_BPS_TAIL_BASES,
    SECURITIES_INDUSTRY_LABEL: [
        "NetSales",
        "GrossProfit",
        "CostOfSalesAndSellingGeneralAndAdministrativeExpenses",
        "CostOfSales",
        "SellingExpenses",
        "GeneralAndAdministrativeExpenses",
        "SellingExpensesOnly",
        "OperatingIncome",
        "OrdinaryIncome",
        "ProfitLoss",
        "EstimatedNetIncome",
        "TotalAssets",
        "NetAssets",
        "BeginningCashBalance",
        "CashAndCashEquivalents",
        "OperatingCash",
        "InvestmentCash",
        "FinancingCash",
        "FCF",
        "OutstandingShares",
        "EPS",
        "EPSGrowthRate",
        "BPS",
    ]
    + POST_BPS_TAIL_BASES,
    INSURANCE_INDUSTRY_LABEL: [
        "NetSales",
        "GrossProfit",
        "CostOfSalesAndSellingGeneralAndAdministrativeExpenses",
        "InsuranceClaimsPayments",
        "PolicyReserveProvision",
        "InvestmentExpenses",
        "ProjectExpenses",
        "OperatingIncome",
        "OrdinaryIncome",
        "ProfitLoss",
        "EstimatedNetIncome",
        "TotalAssets",
        "NetAssets",
        "BeginningCashBalance",
        "CashAndCashEquivalents",
        "OperatingCash",
        "InvestmentCash",
        "FinancingCash",
        "FCF",
        "OutstandingShares",
        "EPS",
        "EPSGrowthRate",
        "BPS",
    ]
    + POST_BPS_TAIL_BASES,
}

ROW_BASE_ORDER_INDEX_BY_SHEET = {
    sheet_name: {metric_base: index for index, metric_base in enumerate(metric_bases)}
    for sheet_name, metric_bases in DEFAULT_BASES_BY_SHEET.items()
}

CONDITION_KEYS = {
    "業種": "industries",
    "証券コード": "security_codes",
    "企業名": "company_names",
    "指標": "metrics",
    "決算種別": "period_scopes",
    "期間": "periods",
    "期間Start": "period_start",
    "期間End": "period_end",
    "連続増減": "trend",
    "連続増減判定": "trend",
    "連続増減指標": "trend_metrics",
    "連続増減期間": "trend_periods",
    "連続増減期間Start": "trend_period_start",
    "連続増減期間End": "trend_period_end",
    "連続増減下限": "trend_min",
    "連続増減以上": "trend_min",
    "連続増減上限": "trend_max",
    "連続増減以下": "trend_max",
    "増減判定": "trend",
    "増減判定指標": "trend_metrics",
    "増減判定期間": "trend_periods",
    "増減判定下限": "trend_min",
    "増減判定以上": "trend_min",
    "増減判定上限": "trend_max",
    "増減判定以下": "trend_max",
    "％条件指標": "percent_filter_metrics",
    "%条件指標": "percent_filter_metrics",
    "比率条件指標": "percent_filter_metrics",
    "比率条件": "percent_filter_metrics",
    "比率条件期間": "percent_filter_periods",
    "比率条件期間Start": "percent_filter_period_start",
    "比率条件期間End": "percent_filter_period_end",
    "％条件期間": "percent_filter_periods",
    "%条件期間": "percent_filter_periods",
    "比率条件下限": "percent_filter_min",
    "比率条件以上": "percent_filter_min",
    "比率条件上限": "percent_filter_max",
    "比率条件以下": "percent_filter_max",
    "％条件下限": "percent_filter_min",
    "%条件下限": "percent_filter_min",
    "％条件以上": "percent_filter_min",
    "%条件以上": "percent_filter_min",
    "％条件上限": "percent_filter_max",
    "%条件上限": "percent_filter_max",
    "％条件以下": "percent_filter_max",
    "%条件以下": "percent_filter_max",
    "％下限": "percent_filter_min",
    "%下限": "percent_filter_min",
    "比率下限": "percent_filter_min",
    "下限": "percent_filter_min",
    "以上": "percent_filter_min",
    "％上限": "percent_filter_max",
    "%上限": "percent_filter_max",
    "比率上限": "percent_filter_max",
    "上限": "percent_filter_max",
    "以下": "percent_filter_max",
}

EXCEL_PERCENT_RATIO_PREFIX = "__excel_percent_ratio__:"

INDUSTRY_ALIASES = {
    "証券・商品先物取引業": SECURITIES_INDUSTRY_LABEL,
    "証券商品先物取引業": SECURITIES_INDUSTRY_LABEL,
}


@dataclass(frozen=True)
class MetricExcelCondition:
    industries: list[str] = field(default_factory=list)
    security_codes: list[str] = field(default_factory=list)
    company_names: list[str] = field(default_factory=list)
    metric_labels: list[str] = field(default_factory=list)
    period_scopes: list[str] = field(default_factory=lambda: ["annual", "half"])
    period_offsets: list[int] = field(default_factory=lambda: list(range(9, -1, -1)))
    trend: str = "none"
    trend_metric_labels: list[str] = field(default_factory=list)
    trend_period_offsets: list[int] = field(default_factory=list)
    trend_min: float | None = None
    trend_max: float | None = None
    percent_filter_metric_labels: list[str] = field(default_factory=list)
    percent_filter_period_offsets: list[int] = field(default_factory=lambda: [0])
    percent_filter_min: float | None = None
    percent_filter_max: float | None = None
    raw_values: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class MetricExcelRow:
    sheet_name: str
    security_code: str
    company_name: str
    industry_33: str
    period_scope: str
    current_period_end: str
    metric_base: str
    metric_label: str
    periods_by_offset: dict[int, str]
    values_by_offset: dict[int, float | None]
    units_by_offset: dict[int, str]
    ratios_by_offset: dict[int, float | None]


@dataclass(frozen=True)
class MetricExcelExportResult:
    output_path: Path
    target_companies: int
    output_rows: int
    errors: list[str]
    warnings: list[str]
    preview_rows: list[dict[str, Any]]


def _normalize_text(value: Any) -> str:
    text = str(value or "").replace("\u3000", " ").replace("\xa0", " ").strip()
    return re.sub(r"\s+", "", text)


def _split_multi(value: str | None) -> list[str]:
    text = str(value or "").strip()
    if not text or text.upper() == "ALL":
        return []
    parts = re.split(r"[,、\n\r]+", text)
    return [part.strip() for part in parts if part.strip()]


def _normalize_industry(value: str) -> str:
    text = str(value or "").strip()
    return INDUSTRY_ALIASES.get(text, text)


def _split_industries(value: str | None) -> list[str]:
    text = str(value or "").strip()
    if not text or text.upper() == "ALL":
        return []

    # The official securities industry name contains "、", so protect it before comma splitting.
    protected = text
    for alias in [SECURITIES_INDUSTRY_LABEL, *INDUSTRY_ALIASES]:
        protected = protected.replace(alias, "__SECURITIES_INDUSTRY__")

    parts = re.split(r"[,、\n\r]+", protected)
    industries: list[str] = []
    for part in parts:
        item = part.strip()
        if not item:
            continue
        if item == "__SECURITIES_INDUSTRY__":
            item = SECURITIES_INDUSTRY_LABEL
        industries.append(_normalize_industry(item))
    return industries


def _normalize_security_code(value: str) -> str:
    text = str(value or "").strip().replace("-", "")
    if len(text) == 5 and text.endswith("0"):
        return text[:-1]
    return text


def _parse_period_token(token: str) -> int:
    text = _normalize_text(token)
    if text == "当期":
        return 0
    if text in {"前期", "1期前", "1年前"}:
        return 1
    match = re.fullmatch(r"(\d+)(?:期前|年前)", text)
    if match:
        offset = int(match.group(1))
        if 0 <= offset <= 10:
            return offset
    raise ValueError(f"Unsupported period: {token}")


def _parse_periods(value: str | None) -> list[int]:
    text = str(value or "").strip()
    if not text or text.upper() == "ALL":
        return list(range(9, -1, -1))

    normalized = text.replace("～", "-").replace("－", "-").replace("〜", "-")
    if "-" in normalized and "," not in normalized and "、" not in normalized:
        left, right = normalized.split("-", 1)
        start = _parse_period_token(left)
        end = _parse_period_token(right)
        hi = max(start, end)
        lo = min(start, end)
        return list(range(hi, lo - 1, -1))

    offsets = sorted({_parse_period_token(part) for part in _split_multi(normalized)}, reverse=True)
    if not offsets:
        return list(range(9, -1, -1))
    return offsets


def _period_range_text(
    raw: dict[str, str],
    *,
    combined_key: str,
    start_key: str,
    end_key: str,
) -> str | None:
    combined = str(raw.get(combined_key) or "").strip()
    if combined:
        return combined

    start = str(raw.get(start_key) or "").strip()
    end = str(raw.get(end_key) or "").strip()
    if not start and not end:
        return None
    if not start or not end:
        return start or end
    if start.upper() == "ALL" or end.upper() == "ALL":
        return "all"
    return f"{start}-{end}"


def _condition_cell_value(cell: Any, key: str) -> str:
    value = getattr(cell, "value", None)
    if value is None:
        return ""
    if (
        key in {"percent_filter_min", "percent_filter_max", "trend_min", "trend_max"}
        and isinstance(value, (int, float))
        and "%" in str(getattr(cell, "number_format", ""))
    ):
        # Excel stores 500% as numeric 5.0 when the cell uses percent formatting.
        return f"{EXCEL_PERCENT_RATIO_PREFIX}{value}"
    return str(value).strip()


def _parse_percent_threshold(value: str | None) -> float | None:
    text = str(value or "").strip()
    if not text:
        return None
    if text.startswith(EXCEL_PERCENT_RATIO_PREFIX):
        return float(text.removeprefix(EXCEL_PERCENT_RATIO_PREFIX))
    has_percent_sign = "%" in text or "％" in text
    normalized = text.replace("%", "").replace("％", "").replace(",", "").strip()
    if not normalized:
        return None
    threshold = float(normalized)
    if has_percent_sign or abs(threshold) > 1:
        threshold /= 100
    return threshold


def _parse_period_scopes(value: str | None) -> list[str]:
    text = str(value or "").strip()
    if not text:
        return ["annual", "half"]
    if text.upper() == "ALL":
        return ["annual", "half"]
    mapping = {
        "\u901a\u671f": "annual",
        "annual": "annual",
        "030000": "annual",
        "\u534a\u671f": "half",
        "half": "half",
        "043000": "half",
        "043a00": "half",
    }
    scopes: list[str] = []
    seen: set[str] = set()
    for item in _split_multi(text):
        scope = mapping.get(_normalize_text(item).lower()) or mapping.get(str(item).strip())
        if not scope:
            raise ValueError(f"Unsupported period scope: {item}")
        if scope not in seen:
            scopes.append(scope)
            seen.add(scope)
    return scopes or ["annual"]


def read_metric_excel_condition(condition_xlsx: str | Path) -> MetricExcelCondition:
    path = Path(condition_xlsx)
    workbook = load_workbook(path, data_only=True)
    if CONDITION_SHEET not in workbook.sheetnames:
        raise ValueError("条件シートがありません。条件シートを追加してください。")

    ws = workbook[CONDITION_SHEET]
    raw: dict[str, str] = {}
    for row in ws.iter_rows():
        cell_texts = ["" if cell.value is None else str(cell.value).strip() for cell in row]
        for idx, cell_text in enumerate(cell_texts):
            key = CONDITION_KEYS.get(_normalize_text(cell_text))
            if key is None:
                continue
            value = ""
            for candidate in row[idx + 1 :]:
                candidate_value = _condition_cell_value(candidate, key)
                if candidate_value:
                    value = candidate_value
                    break
            raw[key] = value

    trend = str(raw.get("trend") or "none").strip().lower()
    if trend in {"", "all"}:
        trend = "none"
    if trend not in {"none", "increase", "decrease"}:
        raise ValueError("連続増減は none / increase / decrease のいずれかで指定してください。")

    period_text = _period_range_text(
        raw,
        combined_key="periods",
        start_key="period_start",
        end_key="period_end",
    )
    trend_period_text = _period_range_text(
        raw,
        combined_key="trend_periods",
        start_key="trend_period_start",
        end_key="trend_period_end",
    )
    percent_filter_period_text = _period_range_text(
        raw,
        combined_key="percent_filter_periods",
        start_key="percent_filter_period_start",
        end_key="percent_filter_period_end",
    )

    period_offsets = _parse_periods(period_text)
    trend_period_offsets = _parse_periods(trend_period_text) if trend_period_text else period_offsets
    trend_min = _parse_percent_threshold(raw.get("trend_min"))
    trend_max = _parse_percent_threshold(raw.get("trend_max"))
    percent_filter_period_offsets = _parse_periods(percent_filter_period_text) if percent_filter_period_text else [0]
    percent_filter_min = _parse_percent_threshold(raw.get("percent_filter_min"))
    percent_filter_max = _parse_percent_threshold(raw.get("percent_filter_max"))

    return MetricExcelCondition(
        industries=_split_industries(raw.get("industries")),
        security_codes=[_normalize_security_code(code) for code in _split_multi(raw.get("security_codes"))],
        company_names=_split_multi(raw.get("company_names")),
        metric_labels=_split_multi(raw.get("metrics")),
        period_scopes=_parse_period_scopes(raw.get("period_scopes")),
        period_offsets=period_offsets,
        trend=trend,
        trend_metric_labels=_split_multi(raw.get("trend_metrics")),
        trend_period_offsets=trend_period_offsets,
        trend_min=trend_min,
        trend_max=trend_max,
        percent_filter_metric_labels=_split_multi(raw.get("percent_filter_metrics")),
        percent_filter_period_offsets=percent_filter_period_offsets,
        percent_filter_min=percent_filter_min,
        percent_filter_max=percent_filter_max,
        raw_values=raw,
    )


def _sheet_name_for_industry(industry_33: str | None) -> str:
    industry = str(industry_33 or "").strip()
    if industry in {BANK_INDUSTRY_LABEL, SECURITIES_INDUSTRY_LABEL, INSURANCE_INDUSTRY_LABEL}:
        return industry
    return GENERAL_SHEET


def _base_metric_label_for_excel(metric_base: str, industry_33: str | None = None) -> str:
    industry = str(industry_33 or "").strip()
    industry_overrides = INDUSTRY_EXCEL_METRIC_LABEL_OVERRIDES.get(industry, {})
    if metric_base in industry_overrides:
        return industry_overrides[metric_base]
    return EXCEL_METRIC_LABEL_OVERRIDES.get(
        metric_base,
        metric_base_to_display_name(metric_base, industry_33),
    )


def _metric_label_for_excel(
    metric_base: str,
    industry_33: str | None = None,
    *,
    period_scope: str = "annual",
) -> str:
    label = _base_metric_label_for_excel(metric_base, industry_33)
    if period_scope != "half":
        return label
    if metric_base in HALF_ONLY_BASES or label.startswith(HALF_PREFIX):
        return label
    prefix = HALF_YOY_PREFIX if metric_base in GROWTH_RATIO_BASES else HALF_PREFIX
    return f"{prefix}{label}"


def _add_half_label_aliases(mapping: dict[str, str], label: str, base: str) -> None:
    mapping[_normalize_text(f"{HALF_PREFIX}{label}")] = base
    mapping[_normalize_text(f"{HALF_YOY_PREFIX}{label}")] = base


def _build_label_to_base_map(sheet_name: str) -> dict[str, str]:
    mapping: dict[str, str] = {}
    candidate_bases = set(METRIC_BASE_LABELS) | set(DEFAULT_BASES_BY_SHEET[sheet_name]) | ABSORBED_RATIO_BASES
    for base in candidate_bases:
        mapping[_normalize_text(metric_base_to_display_name(base))] = base
        mapping[_normalize_text(metric_base_to_display_name(base, sheet_name))] = base
        mapping[_normalize_text(_metric_label_for_excel(base, sheet_name))] = base
        _add_half_label_aliases(mapping, metric_base_to_display_name(base), base)
        _add_half_label_aliases(mapping, metric_base_to_display_name(base, sheet_name), base)
        _add_half_label_aliases(mapping, _base_metric_label_for_excel(base, sheet_name), base)
        for industry in SHEET_ORDER:
            mapping[_normalize_text(metric_base_to_display_name(base, industry))] = base
            mapping[_normalize_text(_metric_label_for_excel(base, industry))] = base
            _add_half_label_aliases(mapping, metric_base_to_display_name(base, industry), base)
            _add_half_label_aliases(mapping, _base_metric_label_for_excel(base, industry), base)
        mapping[_normalize_text(base)] = base

    for ratio_base, row_base in ROW_BASE_BY_RATIO_BASE.items():
        mapping[_normalize_text(metric_base_to_display_name(ratio_base))] = row_base
        mapping[_normalize_text(ratio_base)] = row_base
    return mapping


def _build_label_to_value_base_map(sheet_name: str) -> dict[str, str]:
    mapping = _build_label_to_base_map(sheet_name)
    for ratio_base in ABSORBED_RATIO_BASES:
        mapping[_normalize_text(metric_base_to_display_name(ratio_base))] = ratio_base
        mapping[_normalize_text(ratio_base)] = ratio_base
    return mapping


def _resolve_row_bases(sheet_name: str, metric_labels: list[str], errors: list[str]) -> list[str]:
    if not metric_labels:
        return list(DEFAULT_BASES_BY_SHEET[sheet_name])

    mapping = _build_label_to_base_map(sheet_name)
    bases: list[str] = []
    seen: set[str] = set()
    for label in metric_labels:
        base = mapping.get(_normalize_text(label))
        if not base:
            errors.append(f"unknown_metric sheet={sheet_name} label={label}")
            continue
        if base in ABSORBED_RATIO_BASES:
            base = ROW_BASE_BY_RATIO_BASE[base]
        if base not in seen:
            bases.append(base)
            seen.add(base)
    return bases


def _resolve_value_bases(sheet_name: str, metric_labels: list[str], errors: list[str]) -> list[str]:
    mapping = _build_label_to_value_base_map(sheet_name)
    bases: list[str] = []
    seen: set[str] = set()
    for label in metric_labels:
        base = mapping.get(_normalize_text(label))
        if not base:
            errors.append(f"unknown_trend_metric sheet={sheet_name} label={label}")
            continue
        if base not in seen:
            bases.append(base)
            seen.add(base)
    return bases


def _fetch_ranked_filings(
    conn: sqlite3.Connection,
    condition: MetricExcelCondition,
) -> list[sqlite3.Row]:
    params: list[Any] = []
    form_types: list[str] = []
    for scope in condition.period_scopes:
        form_types.extend(FORM_TYPES_BY_PERIOD_SCOPE.get(scope, ()))
    if not form_types:
        form_types = ["030000"]
    form_type_placeholders = ",".join("?" for _ in form_types)
    where = [
        f"f.form_type IN ({form_type_placeholders})",
        "f.parse_status = 'derived_metrics_saved'",
        "coalesce(im.is_listed, 0) = 1",
        "coalesce(im.exchange, '') = 'TSE'",
    ]
    params.extend(form_types)

    if condition.industries:
        placeholders = ",".join("?" for _ in condition.industries)
        where.append(f"im.industry_33 IN ({placeholders})")
        params.extend(condition.industries)

    if condition.company_names:
        placeholders = ",".join("?" for _ in condition.company_names)
        where.append(f"im.company_name IN ({placeholders})")
        params.extend(condition.company_names)

    sql = f"""
    WITH base AS (
      SELECT
        f.doc_id,
        f.edinet_code,
        f.security_code,
        f.form_type,
        f.period_end,
        f.submit_date,
        f.document_display_unit,
        im.company_name,
        im.industry_33,
        im.security_code AS issuer_security_code,
        CASE
          WHEN f.form_type = '043A00'
            THEN COALESCE(date(f.period_end, 'start of month', '+6 months', '+1 month', '-1 day'), f.period_end)
          ELSE f.period_end
        END AS period_bucket_end
      FROM filings f
      JOIN issuer_master im
        ON im.edinet_code = f.edinet_code
      WHERE {" AND ".join(where)}
    ),
    ranked AS (
      SELECT
        *,
        -- Half filings are bucketed with their corresponding annual fiscal year.
        DENSE_RANK() OVER (
          PARTITION BY f.edinet_code
          ORDER BY period_bucket_end DESC
        ) - 1 AS period_offset
      FROM base f
    )
    SELECT *
    FROM ranked
    WHERE period_offset BETWEEN 0 AND 10
    ORDER BY edinet_code, period_offset, form_type
    """

    rows = conn.execute(sql, params).fetchall()
    if not condition.security_codes:
        return rows

    allowed = set(condition.security_codes)
    filtered = []
    for row in rows:
        code = _normalize_security_code(row["issuer_security_code"] or row["security_code"] or "")
        if code in allowed:
            filtered.append(row)
    return filtered


def _metric_key(base: str) -> str:
    return f"{base}Current"


def _chunked(items: list[str], size: int) -> list[list[str]]:
    return [items[index : index + size] for index in range(0, len(items), size)]


def _fetch_metric_values(
    conn: sqlite3.Connection,
    *,
    doc_ids: list[str],
    metric_bases: list[str],
) -> dict[tuple[str, str], float | None]:
    if not doc_ids or not metric_bases:
        return {}

    metric_keys = [_metric_key(base) for base in metric_bases]
    key_placeholders = ",".join("?" for _ in metric_keys)

    values: dict[tuple[str, str], float | None] = {}
    unique_doc_ids = sorted(set(doc_ids))

    # SQLite builds often cap bound parameters, so large all-company exports read doc_ids in chunks.
    for doc_chunk in _chunked(unique_doc_ids, 800):
        doc_placeholders = ",".join("?" for _ in doc_chunk)
        normalized_rows = conn.execute(
            f"""
            SELECT doc_id, metric_key, value_num
            FROM normalized_metrics
            WHERE doc_id IN ({doc_placeholders})
              AND metric_key IN ({key_placeholders})
            """,
            [*doc_chunk, *metric_keys],
        ).fetchall()
        for row in normalized_rows:
            values[(str(row["doc_id"]), str(row["metric_key"]))] = row["value_num"]

    for doc_chunk in _chunked(unique_doc_ids, 800):
        doc_placeholders = ",".join("?" for _ in doc_chunk)
        derived_rows = conn.execute(
            f"""
            SELECT doc_id, metric_key, value_num, calc_status
            FROM derived_metrics
            WHERE doc_id IN ({doc_placeholders})
              AND metric_key IN ({key_placeholders})
            """,
            [*doc_chunk, *metric_keys],
        ).fetchall()
        for row in derived_rows:
            key = (str(row["doc_id"]), str(row["metric_key"]))
            if key in values and values[key] is not None:
                continue
            if str(row["calc_status"] or "") == "missing_input":
                values[key] = None
            else:
                values[key] = row["value_num"]
    return values


def _scale_value(metric_base: str, value: float | None) -> float | None:
    return _scale_value_for_document_unit(metric_base, value, "")


def _scale_value_for_document_unit(
    metric_base: str,
    value: float | None,
    document_display_unit: str | None,
) -> float | None:
    if value is None:
        return None
    if metric_base in MONETARY_BASES:
        display_unit = str(document_display_unit or "").strip()
        if display_unit == "千円":
            return value / 1_000
        if display_unit == "百万円":
            return value / 1_000_000
    return value


def _display_unit_for_metric(metric_base: str, document_display_unit: str | None) -> str:
    if metric_base in MONETARY_BASES:
        display_unit = str(document_display_unit or "").strip()
        if display_unit in {"百万円", "千円"}:
            return display_unit
        return "円"
    if metric_base == "OutstandingShares":
        return "株"
    if metric_base in GROWTH_RATIO_BASES or metric_base in PERCENT_VALUE_BASES:
        return "%"
    if metric_base in RATIO_VALUE_BASES:
        return "倍"
    if metric_base in PER_SHARE_BASES or metric_base in ONE_DECIMAL_VALUE_BASES or metric_base in INTEGER_VALUE_BASES:
        return "円"
    return ""


def _period_display_for_filing(filing: sqlite3.Row | dict[str, Any] | None) -> str:
    if filing is None:
        return ""
    form_type = str(filing["form_type"] if isinstance(filing, sqlite3.Row) else filing.get("form_type") or "")
    scope_label = PERIOD_SCOPE_LABEL_BY_FORM_TYPE.get(form_type, form_type)
    period_end = str(filing["period_end"] if isinstance(filing, sqlite3.Row) else filing.get("period_end") or "")
    period_month = period_end[:7] if len(period_end) >= 7 else period_end
    if scope_label and period_month:
        return f"{scope_label} {period_month}"
    return period_month


def _period_scope_label(period_scope: str) -> str:
    return "\u534a\u671f" if period_scope == "half" else "\u901a\u671f"


def _passes_trend(values: list[float | None], direction: str) -> bool:
    if direction == "none":
        return True
    if len(values) < 2 or any(value is None for value in values):
        return False
    pairs = zip(values, values[1:])
    if direction == "increase":
        return all(float(left) < float(right) for left, right in pairs)
    return all(float(left) > float(right) for left, right in pairs)


def _passes_value_thresholds(
    values: list[float | None],
    *,
    min_value: float | None,
    max_value: float | None,
) -> bool:
    if min_value is None and max_value is None:
        return True
    if not values or any(value is None for value in values):
        return False
    for value in values:
        numeric_value = float(value)
        if min_value is not None and numeric_value < min_value:
            return False
        if max_value is not None and numeric_value > max_value:
            return False
    return True


def _passes_percent_filters(
    *,
    filter_bases: list[str],
    filter_offsets: list[int],
    by_offset: dict[int, sqlite3.Row],
    metric_values: dict[tuple[str, str], float | None],
    min_value: float | None,
    max_value: float | None,
) -> bool:
    if not filter_bases or (min_value is None and max_value is None):
        return True
    offsets = filter_offsets or [0]
    for base in filter_bases:
        for offset in offsets:
            filing = by_offset.get(offset)
            if filing is None:
                return False
            doc_id = str(filing["doc_id"])
            value = metric_values.get((doc_id, _metric_key(base)))
            if value is None:
                return False
            numeric_value = float(value)
            if min_value is not None and numeric_value < min_value:
                return False
            if max_value is not None and numeric_value > max_value:
                return False
    return True


def _build_preview_rows(rows: list[MetricExcelRow], periods: list[int], limit: int) -> list[dict[str, Any]]:
    preview = []
    for row in rows[:limit]:
        item: dict[str, Any] = {
            "security_code": row.security_code,
            "company_name": row.company_name,
            "industry_33": row.industry_33,
            "period_scope": _period_scope_label(row.period_scope),
            "metric": row.metric_label,
        }
        for offset in periods:
            item[PERIOD_LABEL_BY_OFFSET[offset]] = row.values_by_offset.get(offset)
        preview.append(item)
    return preview


def _row_sort_key(row: MetricExcelRow) -> tuple[int, int, str, int]:
    sheet_order = SHEET_ORDER.index(row.sheet_name)
    sheet_metric_order = ROW_BASE_ORDER_INDEX_BY_SHEET.get(row.sheet_name, ROW_BASE_ORDER_INDEX)
    metric_order = sheet_metric_order.get(row.metric_base, len(sheet_metric_order))
    scope_order = 0 if row.period_scope == "annual" else 1
    return (sheet_order, metric_order, row.security_code, scope_order)


def build_metric_excel_rows(
    conn: sqlite3.Connection,
    condition: MetricExcelCondition,
    *,
    preview_limit: int = 10,
) -> tuple[list[MetricExcelRow], list[str], list[str], list[dict[str, Any]], int]:
    errors: list[str] = []
    warnings: list[str] = []
    filings = _fetch_ranked_filings(conn, condition)

    filings_by_company_scope: dict[tuple[str, str], list[sqlite3.Row]] = {}
    for row in filings:
        period_scope = PERIOD_SCOPE_BY_FORM_TYPE.get(str(row["form_type"] or ""), "annual")
        filings_by_company_scope.setdefault((str(row["edinet_code"]), period_scope), []).append(row)

    selected_row_bases_by_sheet = {
        sheet: _resolve_row_bases(sheet, condition.metric_labels, errors)
        for sheet in SHEET_ORDER
    }

    selected_value_bases: set[str] = set()
    for bases in selected_row_bases_by_sheet.values():
        for base in bases:
            selected_value_bases.add(base)
            ratio_base = ABSORBED_RATIO_BASE_BY_ROW_BASE.get(base)
            if ratio_base:
                selected_value_bases.add(ratio_base)

    has_trend_threshold = condition.trend_min is not None or condition.trend_max is not None
    if condition.trend != "none" or has_trend_threshold:
        trend_labels = condition.trend_metric_labels or condition.metric_labels
        if trend_labels:
            for sheet in SHEET_ORDER:
                for base in _resolve_value_bases(sheet, trend_labels, errors):
                    selected_value_bases.add(base)
        else:
            for bases in selected_row_bases_by_sheet.values():
                selected_value_bases.update(bases)

    percent_filter_bases_by_sheet: dict[str, list[str]] = {sheet: [] for sheet in SHEET_ORDER}
    if condition.percent_filter_metric_labels:
        for sheet in SHEET_ORDER:
            bases = _resolve_value_bases(sheet, condition.percent_filter_metric_labels, errors)
            percent_filter_bases_by_sheet[sheet] = bases
            selected_value_bases.update(bases)

    doc_ids = [str(row["doc_id"]) for row in filings]
    metric_values = _fetch_metric_values(
        conn,
        doc_ids=doc_ids,
        metric_bases=sorted(selected_value_bases),
    )

    rows: list[MetricExcelRow] = []
    for (_edinet_code, current_period_scope), company_filings in filings_by_company_scope.items():
        by_offset = {int(row["period_offset"]): row for row in company_filings}
        current = by_offset.get(0) or min(
            company_filings,
            key=lambda row: int(row["period_offset"]),
            default=None,
        )
        if current is None:
            continue

        sheet_name = _sheet_name_for_industry(current["industry_33"])
        row_bases = selected_row_bases_by_sheet[sheet_name]
        if current_period_scope != "half":
            row_bases = [base for base in row_bases if base not in HALF_ONLY_BASES]

        if not _passes_percent_filters(
            filter_bases=percent_filter_bases_by_sheet.get(sheet_name, []),
            filter_offsets=condition.percent_filter_period_offsets,
            by_offset=by_offset,
            metric_values=metric_values,
            min_value=condition.percent_filter_min,
            max_value=condition.percent_filter_max,
        ):
            continue

        if condition.trend != "none" or has_trend_threshold:
            trend_labels = condition.trend_metric_labels or condition.metric_labels
            trend_bases = (
                _resolve_value_bases(sheet_name, trend_labels, errors)
                if trend_labels
                else row_bases
            )
            trend_ok = True
            for trend_base in trend_bases:
                trend_values = []
                for offset in sorted(condition.trend_period_offsets, reverse=True):
                    filing = by_offset.get(offset)
                    value = None
                    if filing is not None:
                        value = metric_values.get((str(filing["doc_id"]), _metric_key(trend_base)))
                    trend_values.append(value)
                if not _passes_trend(trend_values, condition.trend):
                    trend_ok = False
                    break
                if not _passes_value_thresholds(
                    trend_values,
                    min_value=condition.trend_min,
                    max_value=condition.trend_max,
                ):
                    trend_ok = False
                    break
            if not trend_ok:
                continue

        security_code = _normalize_security_code(current["issuer_security_code"] or current["security_code"] or "")
        for base in row_bases:
            periods_by_offset: dict[int, str] = {}
            values_by_offset: dict[int, float | None] = {}
            units_by_offset: dict[int, str] = {}
            ratios_by_offset: dict[int, float | None] = {}
            ratio_base = ABSORBED_RATIO_BASE_BY_ROW_BASE.get(base)
            allowed_offsets = SPARSE_PERIOD_OFFSETS_BY_BASE.get(base)
            for offset in condition.period_offsets:
                if allowed_offsets is not None and offset not in allowed_offsets:
                    periods_by_offset[offset] = ""
                    values_by_offset[offset] = None
                    units_by_offset[offset] = ""
                    ratios_by_offset[offset] = None
                    continue

                filing = by_offset.get(offset)
                if filing is None:
                    periods_by_offset[offset] = ""
                    values_by_offset[offset] = None
                    units_by_offset[offset] = ""
                    ratios_by_offset[offset] = None
                    continue

                doc_id = str(filing["doc_id"])
                periods_by_offset[offset] = _period_display_for_filing(filing)
                raw_value = metric_values.get((doc_id, _metric_key(base)))
                values_by_offset[offset] = _scale_value_for_document_unit(
                    base,
                    raw_value,
                    str(filing["document_display_unit"] or ""),
                )
                units_by_offset[offset] = (
                    _display_unit_for_metric(base, str(filing["document_display_unit"] or ""))
                    if raw_value is not None
                    else ""
                )

                if base in CONSTANT_RATIO_BY_ROW_BASE:
                    ratios_by_offset[offset] = CONSTANT_RATIO_BY_ROW_BASE[base]
                elif ratio_base:
                    ratios_by_offset[offset] = metric_values.get((doc_id, _metric_key(ratio_base)))
                else:
                    ratios_by_offset[offset] = None

            rows.append(
                MetricExcelRow(
                    sheet_name=sheet_name,
                    security_code=security_code,
                    company_name=str(current["company_name"] or ""),
                    industry_33=str(current["industry_33"] or ""),
                    period_scope=current_period_scope,
                    current_period_end=str(current["period_end"] or ""),
                    metric_base=base,
                    metric_label=_metric_label_for_excel(
                        base,
                        current["industry_33"],
                        period_scope=current_period_scope,
                    ),
                    periods_by_offset=periods_by_offset,
                    values_by_offset=values_by_offset,
                    units_by_offset=units_by_offset,
                    ratios_by_offset=ratios_by_offset,
                )
            )

    rows.sort(key=_row_sort_key)
    preview_rows = _build_preview_rows(rows, condition.period_offsets, preview_limit)
    return rows, errors, warnings, preview_rows, len({row.security_code for row in rows})


def _write_summary_sheet(
    workbook: Workbook,
    *,
    condition: MetricExcelCondition,
    db_path: str | Path,
    output_rows: int,
    target_companies: int,
    errors: list[str],
    warnings: list[str],
) -> None:
    ws = workbook.create_sheet(SUMMARY_SHEET)
    rows = [
        ("generated_at", datetime.now().isoformat(timespec="seconds")),
        ("db_path", str(db_path)),
        ("target_companies", target_companies),
        ("output_rows", output_rows),
        ("errors", len(errors)),
        ("warnings", len(warnings)),
        ("industries", ", ".join(condition.industries) if condition.industries else "ALL"),
        ("security_codes", ", ".join(condition.security_codes) if condition.security_codes else "ALL"),
        ("company_names", ", ".join(condition.company_names) if condition.company_names else "ALL"),
        ("metrics", ", ".join(condition.metric_labels) if condition.metric_labels else "ALL"),
        ("period_scopes", ", ".join(condition.period_scopes)),
        (
            "periods",
            ", ".join(PERIOD_LABEL_BY_OFFSET[offset] for offset in condition.period_offsets),
        ),
        ("連続増減", condition.trend),
        (
            "連続増減指標",
            ", ".join(condition.trend_metric_labels) if condition.trend_metric_labels else "",
        ),
        (
            "連続増減期間",
            ", ".join(PERIOD_LABEL_BY_OFFSET[offset] for offset in condition.trend_period_offsets),
        ),
        (
            "連続増減下限",
            "" if condition.trend_min is None else condition.trend_min,
        ),
        (
            "連続増減上限",
            "" if condition.trend_max is None else condition.trend_max,
        ),
        (
            "比率条件指標",
            ", ".join(condition.percent_filter_metric_labels)
            if condition.percent_filter_metric_labels
            else "",
        ),
        (
            "比率条件期間",
            ", ".join(
                PERIOD_LABEL_BY_OFFSET[offset]
                for offset in condition.percent_filter_period_offsets
            ),
        ),
        (
            "比率条件下限",
            "" if condition.percent_filter_min is None else condition.percent_filter_min,
        ),
        (
            "比率条件上限",
            "" if condition.percent_filter_max is None else condition.percent_filter_max,
        ),
    ]
    for row in rows:
        ws.append(row)
    if errors:
        ws.append(("", ""))
        ws.append(("error_detail", ""))
        for error in errors:
            ws.append(("", error))
    if warnings:
        ws.append(("", ""))
        ws.append(("warning_detail", ""))
        for warning in warnings:
            ws.append(("", warning))
    ws.column_dimensions["A"].width = 24
    ws.column_dimensions["B"].width = 80


def _format_value_cell(cell: Any, metric_base: str) -> None:
    if metric_base in PERCENT_VALUE_BASES:
        cell.number_format = "0.0%"
    elif metric_base in GROWTH_RATIO_BASES:
        cell.number_format = "0.0%"
    elif metric_base in ONE_DECIMAL_VALUE_BASES:
        cell.number_format = "#,##0.0"
    elif metric_base in INTEGER_VALUE_BASES:
        cell.number_format = "#,##0"
    elif metric_base in PER_SHARE_BASES:
        cell.number_format = "#,##0.00"
    else:
        cell.number_format = "#,##0"


def _write_metric_sheet(
    workbook: Workbook,
    sheet_name: str,
    rows: list[MetricExcelRow],
    period_offsets: list[int],
) -> None:
    ws = workbook.create_sheet(sheet_name)
    headers = ["証券コード", "企業名", "業種", "決算種別", "期末年月日_当期", "指標"]
    for offset in period_offsets:
        label = PERIOD_LABEL_BY_OFFSET[offset]
        headers.extend([f"{label}_期間", f"{label}_数値", f"{label}_単位", f"{label}_比率"])
    ws.append(headers)

    header_fill = PatternFill("solid", fgColor="D9EAF7")
    for cell in ws[1]:
        cell.font = Font(bold=True)
        cell.fill = header_fill

    for row in rows:
        values: list[Any] = [
            row.security_code,
            row.company_name,
            row.industry_33,
            _period_scope_label(row.period_scope),
            row.current_period_end,
            row.metric_label,
        ]
        for offset in period_offsets:
            values.extend(
                [
                    row.periods_by_offset.get(offset, ""),
                    row.values_by_offset.get(offset),
                    row.units_by_offset.get(offset, ""),
                    row.ratios_by_offset.get(offset),
                ]
            )
        ws.append(values)
        current_row = ws.max_row
        for idx, offset in enumerate(period_offsets):
            value_col = 8 + idx * 4
            ratio_col = value_col + 2
            _format_value_cell(ws.cell(current_row, value_col), row.metric_base)
            ws.cell(current_row, ratio_col).number_format = "0.0%"

    ws.freeze_panes = "G2"
    ws.auto_filter.ref = ws.dimensions
    widths = {
        "A": 12,
        "B": 28,
        "C": 18,
        "D": 12,
        "E": 16,
        "F": 24,
    }
    for column, width in widths.items():
        ws.column_dimensions[column].width = width
    for col_idx in range(7, ws.max_column + 1):
        ws.column_dimensions[get_column_letter(col_idx)].width = 14


def _write_vertical_data_sheet(
    workbook: Workbook,
    rows: list[MetricExcelRow],
    period_offsets: list[int],
) -> None:
    ws = workbook.create_sheet(VERTICAL_DATA_SHEET)
    headers = [
        "証券コード",
        "企業名",
        "業種",
        "期間",
        "指標",
        "数値",
        "単位",
        "比率",
    ]
    headers = [
        "\u8a3c\u5238\u30b3\u30fc\u30c9",
        "\u4f01\u696d\u540d",
        "\u696d\u7a2e",
        "\u6c7a\u7b97\u7a2e\u5225",
        "\u671f\u9593",
        "\u6307\u6a19",
        "\u6570\u5024",
        "\u5358\u4f4d",
        "\u6bd4\u7387",
    ]
    ws.append(headers)

    header_fill = PatternFill("solid", fgColor="D9EAF7")
    for cell in ws[1]:
        cell.font = Font(bold=True)
        cell.fill = header_fill

    for row in rows:
        for offset in period_offsets:
            period_text = row.periods_by_offset.get(offset, "")
            if not period_text:
                continue
            ws.append(
                [
                    row.security_code,
                    row.company_name,
                    row.industry_33,
                    _period_scope_label(row.period_scope),
                    period_text,
                    row.metric_label,
                    row.values_by_offset.get(offset),
                    row.units_by_offset.get(offset, ""),
                    row.ratios_by_offset.get(offset),
                ]
            )
            current_row = ws.max_row
            _format_value_cell(ws.cell(current_row, 7), row.metric_base)
            ws.cell(current_row, 9).number_format = "0.0%"

    ws.freeze_panes = "A2"
    ws.auto_filter.ref = ws.dimensions
    widths = {
        "A": 12,
        "B": 28,
        "C": 18,
        "D": 12,
        "E": 14,
        "F": 28,
        "G": 16,
        "H": 10,
        "I": 12,
    }
    for column, width in widths.items():
        ws.column_dimensions[column].width = width


def write_metric_excel(
    *,
    rows: list[MetricExcelRow],
    condition: MetricExcelCondition,
    output_path: str | Path,
    db_path: str | Path,
    errors: list[str],
    warnings: list[str],
) -> Path:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    workbook = Workbook()
    default_sheet = workbook.active
    workbook.remove(default_sheet)

    _write_summary_sheet(
        workbook,
        condition=condition,
        db_path=db_path,
        output_rows=len(rows),
        target_companies=len({row.security_code for row in rows}),
        errors=errors,
        warnings=warnings,
    )

    rows_by_sheet = {sheet: [] for sheet in SHEET_ORDER}
    for row in rows:
        rows_by_sheet.setdefault(row.sheet_name, []).append(row)

    for sheet_name in SHEET_ORDER:
        _write_metric_sheet(
            workbook,
            sheet_name,
            rows_by_sheet.get(sheet_name, []),
            condition.period_offsets,
        )

    _write_vertical_data_sheet(workbook, rows, condition.period_offsets)

    workbook.save(path)
    return path


def export_metric_excel(
    conn: sqlite3.Connection,
    *,
    condition_xlsx: str | Path,
    output_path: str | Path,
    db_path: str | Path,
    preview_limit: int = 10,
) -> MetricExcelExportResult:
    condition = read_metric_excel_condition(condition_xlsx)
    rows, errors, warnings, preview_rows, target_companies = build_metric_excel_rows(
        conn,
        condition,
        preview_limit=preview_limit,
    )
    path = write_metric_excel(
        rows=rows,
        condition=condition,
        output_path=output_path,
        db_path=db_path,
        errors=errors,
        warnings=warnings,
    )
    return MetricExcelExportResult(
        output_path=path,
        target_companies=target_companies,
        output_rows=len(rows),
        errors=errors,
        warnings=warnings,
        preview_rows=preview_rows,
    )
