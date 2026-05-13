from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
import re
import sqlite3
from typing import Any

from openpyxl import Workbook, load_workbook
from openpyxl.comments import Comment
from openpyxl.styles import Border, Font, PatternFill, Side
from openpyxl.utils import get_column_letter

from edinet_pipeline.domain.metric_labels import (
    BANK_INDUSTRY_LABEL,
    INSURANCE_INDUSTRY_LABEL,
    METRIC_BASE_LABELS,
    SECURITIES_INDUSTRY_LABEL,
    metric_base_to_display_name,
)
from edinet_monitor.domain.issuer_flags import tenbagger_learning_mark
from edinet_monitor.services.industry_aggregate_metric_service import (
    INDUSTRY_AGGREGATE_PERIOD_SCOPE,
    INDUSTRY_AGGREGATE_ROW_BASES,
    industry_aggregate_table_exists,
)
from edinet_monitor.services.market_derived_metric_service import (
    MARKET_METRIC_BASES,
    market_derived_table_exists,
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
    "043A00": "quarter:2Q",
}
FORM_TYPES_BY_PERIOD_SCOPE = {
    "annual": ("030000",),
    "quarter": ("043A00",),
}
PERIOD_SCOPE_LABEL_BY_FORM_TYPE = {
    "030000": "\u901a\u671f",
    "043A00": "2Q",
}
ALL_PERIOD_SCOPES = ["annual", "quarter", "forecast"]
QUARTER_SUPPORTED_BASES = {
    "NetSales",
    "OperatingIncome",
    "OrdinaryIncome",
    "ProfitLoss",
    "EPS",
    "TotalAssets",
    "NetAssets",
    "BPS",
    "OperatingCash",
    "InvestmentCash",
    "FinancingCash",
    "CashAndCashEquivalents",
    "IssuedShares",
    "TreasuryShares",
    "OutstandingShares",
    "EquityRatio",
    "MarketCapitalization",
    "StockPrice",
    "PBR",
    "PER",
    "TheoreticalSharePrice",
    "TheoreticalSharePriceGrowthRate",
    "TheoreticalPBR",
    "TheoreticalPER",
}
FORECAST_SUPPORTED_BASES = {
    "NetSales",
    "OperatingIncome",
    "OrdinaryIncome",
    "ProfitLoss",
}
FORECAST_PROGRESS_BASES = {"NetSales", "OperatingIncome", "OrdinaryIncome", "ProfitLoss"}
JQUANTS_QUARTER_TYPES = ("1Q", "3Q")
JQUANTS_FORECAST_STAGES = ("initial", "1Q", "2Q", "3Q")
JQUANTS_FORECAST_STAGE_LABELS = {
    "initial": "\u5f53\u671f",
    "1Q": "1Q",
    "2Q": "2Q",
    "3Q": "3Q",
}
FORECAST_PROGRESS_RATIO_KIND = "forecast_progress"
DATE_POINT_PERIOD_BASES = {
    "MarketCapitalization",
    "StockPrice",
    "PBR",
    "PER",
    "PCFR",
    "TheoreticalSharePrice",
    "TheoreticalSharePriceGrowthRate",
    "TheoreticalPBR",
    "TheoreticalPER",
    "TheoreticalPCFR",
}
HALF_ONLY_BASES = {
    "HalfNetSalesProgressRate",
    "HalfOrdinaryIncomeProgressRate",
    "HalfProfitProgressRate",
}
HALF_DISABLED_BASES = {
    "NetSalesGrowthRate5Year",
    "NetSalesGrowthRate10Year",
    "OrdinaryIncomeGrowthRate5Year",
    "OrdinaryIncomeGrowthRate10Year",
    "CashBalanceGrowthRate5Year",
    "CashBalanceGrowthRate10Year",
    "EPSGrowthRate5Year",
    "EPSGrowthRate10Year",
    "BPSGrowthRate5Year",
    "BPSGrowthRate10Year",
    "OutstandingSharesGrowthRate5Year",
    "OutstandingSharesGrowthRate10Year",
    "TheoreticalSharePriceGrowthRate5Year",
    "TheoreticalSharePriceGrowthRate10Year",
}
HALF_PREFIX = "\u534a\u671f "
HALF_YOY_PREFIX = "\u534a\u671f\u524d\u5e74\u6bd4 "
INDUSTRY_ONLY_TOKEN = "\u696d\u7a2e\u306e\u307f"
ROW_KIND_DETAIL = "\u660e\u7d30"
ROW_KIND_AVERAGE = "\u5e73\u5747\u5024"
ROW_KIND_MEDIAN = "\u4e2d\u592e\u5024"
SUPPRESSED_EXCEL_BASES = set(HALF_ONLY_BASES)
PERIOD_BLOCK_FILL_COLORS = ("EAF4FF", "FFFFFF")
CURRENT_PERIOD_BLOCK_FILL_COLOR = "D9EAF7"
PERIOD_BLOCK_BORDER_COLOR = "9FBAD0"
FORECAST_PROGRESS_FILL_COLOR = "FFF2CC"

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
    "ProfitBeforeTax",
    "IncomeTaxes",
    "InterestBearingDebt",
    "MarketCapitalization",
}

PERCENT_VALUE_BASES = {
    "ROA",
    "ROE",
    "ROIC",
    "EquityRatio",
    "InvestmentCashToNetSalesRatio",
    "InvestmentCashToOperatingCashRatio",
    "StockPriceGrowthRate",
    "StockPriceGrowthRate5Year",
    "StockPriceGrowthRate10Year",
} | ABSORBED_RATIO_BASES
RATIO_VALUE_BASES = {
    "PBR",
    "PER",
    "PCFR",
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
    "StockPrice",
    "TheoreticalSharePrice",
    "UpperBoundTheoreticalSharePrice",
}

ONE_DECIMAL_VALUE_BASES = {
    "EPS",
    "BPS",
    "TheoreticalPBR",
    "TheoreticalPER",
    "PBR",
    "PER",
    "PCFR",
    "TheoreticalPCFR",
    "AverageAge",
    "AverageLengthOfService",
}

INTEGER_VALUE_BASES = {
    "OutstandingShares",
    "AssetsPerShare",
    "LiabilitiesPerShare",
    "AssetValue",
    "BusinessValue",
    "TheoreticalSharePrice",
    "UpperBoundTheoreticalSharePrice",
    "MarketCapitalization",
    "OperatingCashPerShare",
    "InvestmentCashPerShare",
    "FinancingCashPerShare",
    "FCFPerShare",
    "NumberOfEmployees",
    "AverageAnnualSalary",
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
    "EPSGrowthRate5Year",
    "EPSGrowthRate10Year",
    "BPSGrowthRate",
    "BPSGrowthRate5Year",
    "BPSGrowthRate10Year",
    "TheoreticalSharePriceGrowthRate",
    "TheoreticalSharePriceGrowthRate5Year",
    "TheoreticalSharePriceGrowthRate10Year",
    "StockPriceGrowthRate",
    "StockPriceGrowthRate5Year",
    "StockPriceGrowthRate10Year",
    "HalfNetSalesProgressRate",
    "HalfOrdinaryIncomeProgressRate",
    "HalfProfitProgressRate",
    "InvestmentCashToNetSalesRatio",
    "InvestmentCashToOperatingCashRatio",
}

INDUSTRY_AGGREGATE_VALUE_BASES = set(INDUSTRY_AGGREGATE_ROW_BASES)

SPARSE_PERIOD_OFFSETS_BY_BASE = {
    "NetSalesGrowthRate5Year": {5, 0},
    "OrdinaryIncomeGrowthRate5Year": {5, 0},
    "CashBalanceGrowthRate5Year": {5, 0},
    "EPSGrowthRate5Year": {5, 0},
    "BPSGrowthRate5Year": {5, 0},
    "OutstandingSharesGrowthRate5Year": {5, 0},
    "TheoreticalSharePriceGrowthRate5Year": {5, 0},
    "StockPriceGrowthRate5Year": {5, 0},
    "NetSalesGrowthRate10Year": {0},
    "OrdinaryIncomeGrowthRate10Year": {0},
    "CashBalanceGrowthRate10Year": {0},
    "EPSGrowthRate10Year": {0},
    "BPSGrowthRate10Year": {0},
    "OutstandingSharesGrowthRate10Year": {0},
    "TheoreticalSharePriceGrowthRate10Year": {0},
    "StockPriceGrowthRate10Year": {0},
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
    "InvestmentCashToNetSalesRatio",
    "InvestmentCashToOperatingCashRatio",
    "InterestBearingDebt",
    "OutstandingShares",
    "EPS",
    "EPSGrowthRate",
    "EPSGrowthRate5Year",
    "EPSGrowthRate10Year",
    "BPS",
    "BPSGrowthRate",
    "BPSGrowthRate5Year",
    "BPSGrowthRate10Year",
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
    "ROIC",
    "EquityRatio",
    "MarketCapitalization",
    "StockPrice",
    "StockPriceGrowthRate",
    "StockPriceGrowthRate5Year",
    "StockPriceGrowthRate10Year",
    "PBR",
    "PER",
    "AssetValue",
    "BusinessValue",
    "TheoreticalSharePrice",
    "UpperBoundTheoreticalSharePrice",
    "TheoreticalSharePriceGrowthRate",
    "TheoreticalSharePriceGrowthRate5Year",
    "TheoreticalSharePriceGrowthRate10Year",
    "TheoreticalPBR",
    "TheoreticalPER",
    "OperatingCashPerShare",
    "InvestmentCashPerShare",
    "FinancingCashPerShare",
    "FCFPerShare",
    "PCFR",
    "TheoreticalPCFR",
    "NumberOfEmployees",
    "AverageAge",
    "AverageLengthOfService",
    "AverageAnnualSalary",
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
    "EPSGrowthRate": "EPS増加率",
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
POST_BPS_TAIL_BASES_FOR_FINANCIAL = [base for base in POST_BPS_TAIL_BASES if base != "ROIC"]

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
    + POST_BPS_TAIL_BASES_FOR_FINANCIAL,
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
    + POST_BPS_TAIL_BASES_FOR_FINANCIAL,
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
    + POST_BPS_TAIL_BASES_FOR_FINANCIAL,
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
    industry_only: bool = False
    security_codes: list[str] = field(default_factory=list)
    company_names: list[str] = field(default_factory=list)
    metric_labels: list[str] = field(default_factory=list)
    period_scopes: list[str] = field(default_factory=lambda: list(ALL_PERIOD_SCOPES))
    period_offsets: list[int] = field(default_factory=lambda: list(range(10, -1, -1)))
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


@dataclass
class MetricExcelRow:
    sheet_name: str
    security_code: str
    company_name: str
    industry_33: str
    market: str
    period_scope: str
    current_period_end: str
    metric_base: str
    metric_label: str
    periods_by_offset: dict[int, str]
    values_by_offset: dict[int, float | None]
    units_by_offset: dict[int, str]
    ratios_by_offset: dict[int, float | None]
    row_kind: str = ROW_KIND_DETAIL
    raw_values_by_offset: dict[int, float | None] = field(default_factory=dict)
    ranks_by_offset: dict[int, str] = field(default_factory=dict)
    ratio_kinds_by_offset: dict[int, str] = field(default_factory=dict)


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
    if text in {"当期", "最新"}:
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
        return list(range(10, -1, -1))

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
        return list(range(10, -1, -1))
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
        return list(ALL_PERIOD_SCOPES)
    if text.upper() == "ALL":
        return list(ALL_PERIOD_SCOPES)
    mapping = {
        "\u901a\u671f": "annual",
        "annual": "annual",
        "030000": "annual",
        "\u534a\u671f": "quarter",
        "half": "quarter",
        "2q": "quarter",
        "043000": "quarter",
        "043a00": "quarter",
        "\u56db\u534a\u671f": "quarter",
        "quarter": "quarter",
        "1q": "quarter",
        "3q": "quarter",
        "\u4e88\u60f3": "forecast",
        "forecast": "forecast",
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
    industries = _split_industries(raw.get("industries"))
    industry_only = any(_normalize_text(item) == _normalize_text(INDUSTRY_ONLY_TOKEN) for item in industries)
    industries = [
        item
        for item in industries
        if _normalize_text(item) != _normalize_text(INDUSTRY_ONLY_TOKEN)
    ]

    return MetricExcelCondition(
        industries=industries,
        industry_only=industry_only,
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
    if period_scope.startswith("quarter:"):
        quarter = period_scope.split(":", 1)[1]
        return f"{quarter} {label}"
    if period_scope.startswith("forecast:"):
        stage = period_scope.split(":", 1)[1]
        stage_label = JQUANTS_FORECAST_STAGE_LABELS.get(stage, stage)
        return f"{stage_label} {label} \u4e88\u60f3"
    if period_scope == "half":
        return f"2Q {label}"
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
        for quarter in JQUANTS_QUARTER_TYPES:
            mapping[_normalize_text(f"{quarter} {metric_base_to_display_name(base)}")] = base
            mapping[_normalize_text(f"{quarter} {_base_metric_label_for_excel(base, sheet_name)}")] = base
        mapping[_normalize_text(f"2Q {metric_base_to_display_name(base)}")] = base
        mapping[_normalize_text(f"2Q {_base_metric_label_for_excel(base, sheet_name)}")] = base
        for forecast_stage, stage_label in JQUANTS_FORECAST_STAGE_LABELS.items():
            mapping[_normalize_text(f"{stage_label} {metric_base_to_display_name(base)} \u4e88\u60f3")] = base
            mapping[_normalize_text(f"{stage_label} {_base_metric_label_for_excel(base, sheet_name)} \u4e88\u60f3")] = base
            mapping[_normalize_text(f"{stage_label} {metric_base_to_display_name(base)}(\u4e88\u60f3)")] = base
            mapping[_normalize_text(f"{stage_label} {_base_metric_label_for_excel(base, sheet_name)}(\u4e88\u60f3)")] = base
            if forecast_stage == "initial":
                mapping[_normalize_text(f"\u5f53\u521d {metric_base_to_display_name(base)} \u4e88\u60f3")] = base
                mapping[_normalize_text(f"\u5f53\u521d {_base_metric_label_for_excel(base, sheet_name)} \u4e88\u60f3")] = base
                mapping[_normalize_text(f"\u5f53\u521d {metric_base_to_display_name(base)}(\u4e88\u60f3)")] = base
                mapping[_normalize_text(f"\u5f53\u521d {_base_metric_label_for_excel(base, sheet_name)}(\u4e88\u60f3)")] = base
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
        return []
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
        im.market,
        im.security_code AS issuer_security_code,
        CASE
          WHEN f.form_type = '043A00' THEN 'quarter:2Q'
          ELSE 'annual'
        END AS period_scope_key,
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
          PARTITION BY f.edinet_code, f.period_scope_key
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
    if market_derived_table_exists(conn):
        for doc_chunk in _chunked(unique_doc_ids, 800):
            doc_placeholders = ",".join("?" for _ in doc_chunk)
            market_rows = conn.execute(
                f"""
                SELECT source_id AS doc_id, metric_key, value_num, calc_status
                FROM market_derived_metrics
                WHERE source_type = 'edinet'
                  AND source_id IN ({doc_placeholders})
                  AND metric_key IN ({key_placeholders})
                """,
                [*doc_chunk, *metric_keys],
            ).fetchall()
            for row in market_rows:
                key = (str(row["doc_id"]), str(row["metric_key"]))
                if str(row["calc_status"] or "") == "missing_input":
                    values[key] = None
                else:
                    values[key] = row["value_num"]
    return values


def _fetch_industry_aggregate_max_year(
    conn: sqlite3.Connection,
    industries: list[str],
) -> int | None:
    where = ["period_scope = ?"]
    params: list[Any] = [INDUSTRY_AGGREGATE_PERIOD_SCOPE]
    if industries:
        placeholders = ",".join("?" for _ in industries)
        where.append(f"industry_33 IN ({placeholders})")
        params.extend(industries)
    row = conn.execute(
        f"""
        SELECT MAX(fiscal_year) AS max_fiscal_year
        FROM industry_aggregate_metrics
        WHERE {" AND ".join(where)}
        """,
        params,
    ).fetchone()
    if row is None or row["max_fiscal_year"] is None:
        return None
    return int(row["max_fiscal_year"])


def _fetch_industry_aggregate_rows(
    conn: sqlite3.Connection,
    *,
    industries: list[str],
    fiscal_years: list[int],
    metric_bases: list[str],
) -> list[sqlite3.Row]:
    if not fiscal_years or not metric_bases:
        return []
    where = [
        "period_scope = ?",
        f"fiscal_year IN ({','.join('?' for _ in fiscal_years)})",
        f"metric_base IN ({','.join('?' for _ in metric_bases)})",
    ]
    params: list[Any] = [INDUSTRY_AGGREGATE_PERIOD_SCOPE, *fiscal_years, *metric_bases]
    if industries:
        where.append(f"industry_33 IN ({','.join('?' for _ in industries)})")
        params.extend(industries)
    rows = conn.execute(
        f"""
        SELECT
          industry_33,
          fiscal_year,
          period_bucket_end,
          metric_key,
          metric_base,
          value_num,
          value_unit,
          calc_status,
          source_company_count
        FROM industry_aggregate_metrics
        WHERE {" AND ".join(where)}
        ORDER BY industry_33, fiscal_year, metric_base
        """,
        params,
    ).fetchall()
    return rows


def _scale_value(metric_base: str, value: float | None) -> float | None:
    return _scale_value_for_document_unit(metric_base, value, "")


def _scale_value_for_document_unit(
    metric_base: str,
    value: float | None,
    document_display_unit: str | None,
) -> float | None:
    if value is None:
        return None
    if metric_base == "MarketCapitalization":
        return value / 1_000_000
    if metric_base in MONETARY_BASES:
        display_unit = str(document_display_unit or "").strip()
        if display_unit == "千円":
            return value / 1_000
        if display_unit == "百万円":
            return value / 1_000_000
    return value


def _display_unit_for_metric(metric_base: str, document_display_unit: str | None) -> str:
    if metric_base == "NumberOfEmployees":
        return "\u4eba"
    if metric_base in {"AverageAge", "AverageLengthOfService"}:
        return "\u5e74"
    if metric_base == "AverageAnnualSalary":
        return "\u5186"
    if metric_base == "MarketCapitalization":
        return "\u767e\u4e07\u5186"
    if metric_base == "StockPrice":
        return "\u5186"
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


def _period_point_display_for_filing(
    filing: sqlite3.Row | dict[str, Any] | None,
    period_scope: str,
) -> str:
    if filing is None:
        return ""
    period_end = str(filing["period_end"] if isinstance(filing, sqlite3.Row) else filing.get("period_end") or "")
    if not period_end:
        return ""
    if period_scope == "quarter:2Q":
        return f"2Q {period_end}\u6642\u70b9"
    form_type = str(filing["form_type"] if isinstance(filing, sqlite3.Row) else filing.get("form_type") or "")
    scope_label = PERIOD_SCOPE_LABEL_BY_FORM_TYPE.get(form_type, form_type)
    return f"{scope_label} {period_end}\u6642\u70b9" if scope_label else f"{period_end}\u6642\u70b9"


def _period_scope_label(period_scope: str) -> str:
    if period_scope == "half":
        return "\u56db\u534a\u671f"
    if period_scope.startswith("quarter"):
        return "\u56db\u534a\u671f"
    if period_scope.startswith("forecast"):
        return "\u4e88\u60f3"
    return "\u901a\u671f"


def _source_offset_for_display(period_scope: str, display_offset: int) -> int | None:
    if period_scope == "annual":
        if display_offset == 0:
            return None
        return display_offset - 1
    return display_offset


def _calendar_year_bucket(period_bucket_end: str | None) -> int | None:
    text = str(period_bucket_end or "").strip()
    if len(text) < 4:
        return None
    try:
        year = int(text[:4])
    except ValueError:
        return None
    return year


def _aggregate_period_display(period_scope: str, calendar_year: int | None) -> str:
    if calendar_year is None:
        return ""
    return f"{_period_scope_label(period_scope)} {calendar_year}\u5e74\u6c7a\u7b97"


def _scale_industry_aggregate_value(metric_base: str, value: float | None) -> tuple[float | None, str]:
    if value is None:
        return None, ""
    if metric_base in MONETARY_BASES:
        return value / 100_000_000, "\u5104\u5186"
    if metric_base == "NumberOfEmployees":
        return value, "\u4eba"
    return value, _display_unit_for_metric(metric_base, "")


def _aggregate_ratio_for_row_base(metric_base: str, sums: dict[str, float]) -> float | None:
    def ratio(numerator_base: str, denominator_base: str, *, positive_denominator: bool = True) -> float | None:
        numerator = sums.get(numerator_base)
        denominator = sums.get(denominator_base)
        if numerator is None or denominator is None:
            return None
        if positive_denominator and denominator <= 0:
            return None
        if denominator == 0:
            return None
        return numerator / denominator

    if metric_base in CONSTANT_RATIO_BY_ROW_BASE:
        return 1.0 if sums.get(metric_base) is not None else None
    if metric_base == "GrossProfit":
        return ratio("GrossProfit", "NetSales")
    if metric_base == "CostOfSales":
        return ratio("CostOfSales", "NetSales")
    if metric_base == "SellingExpenses":
        return ratio("SellingExpenses", "NetSales")
    if metric_base == "OperatingIncome":
        return ratio("OperatingIncome", "NetSales")
    if metric_base == "OrdinaryIncome":
        return ratio("OrdinaryIncome", "NetSales")
    if metric_base == "ProfitLoss":
        return ratio("ProfitLoss", "NetSales")
    if metric_base == "NetAssets":
        return ratio("NetAssets", "TotalAssets", positive_denominator=False)
    return None


def _append_warning_once(warnings: list[str], warning: str) -> None:
    if warning not in warnings:
        warnings.append(warning)


def _filter_excel_visible_bases(
    bases: list[str],
    *,
    warnings: list[str],
    explicit_metric_request: bool,
) -> list[str]:
    suppressed = [base for base in bases if base in SUPPRESSED_EXCEL_BASES]
    if suppressed and explicit_metric_request:
        labels = ", ".join(_base_metric_label_for_excel(base) for base in suppressed)
        _append_warning_once(warnings, f"excel_suppressed_metrics={labels}")
    return [base for base in bases if base not in SUPPRESSED_EXCEL_BASES]


def _mean(values: list[float]) -> float | None:
    if not values:
        return None
    return sum(values) / len(values)


def _median(values: list[float]) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    midpoint = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[midpoint]
    return (ordered[midpoint - 1] + ordered[midpoint]) / 2


def _stat_value_and_unit(
    metric_base: str,
    value: float | None,
    *,
    industry_only: bool,
) -> tuple[float | None, str]:
    if value is None:
        return None, ""
    if metric_base in MONETARY_BASES:
        if industry_only:
            return value / 100_000_000, "\u5104\u5186"
        return value / 1_000_000, "\u767e\u4e07\u5186"
    return value, _display_unit_for_metric(metric_base, "")


def _period_text_for_stats(rows: list[MetricExcelRow], offset: int) -> str:
    for row in rows:
        text = row.periods_by_offset.get(offset, "")
        if text:
            return text
    return ""


def _raw_value_for_export_stats(row: MetricExcelRow, offset: int) -> float | None:
    value = row.raw_values_by_offset.get(offset)
    if value is not None:
        return value
    return row.values_by_offset.get(offset)


def _assign_ranks(rows: list[MetricExcelRow], period_offsets: list[int]) -> None:
    groups: dict[tuple[str, str, str, int], list[tuple[int, float]]] = {}
    group_sizes: dict[tuple[str, str, str, int], int] = {}
    for index, row in enumerate(rows):
        if row.row_kind != ROW_KIND_DETAIL:
            continue
        for offset in period_offsets:
            key = (row.sheet_name, row.period_scope, row.metric_base, offset)
            group_sizes[key] = group_sizes.get(key, 0) + 1
            value = _raw_value_for_export_stats(row, offset)
            if value is None:
                continue
            groups.setdefault(key, []).append((index, float(value)))

    for key, values in groups.items():
        denominator = group_sizes.get(key, len(values))
        previous_value: float | None = None
        previous_rank = 0
        for position, (row_index, value) in enumerate(
            sorted(values, key=lambda item: item[1], reverse=True),
            start=1,
        ):
            if previous_value is None or value != previous_value:
                previous_rank = position
                previous_value = value
            rows[row_index].ranks_by_offset[key[3]] = f"{previous_rank}/{denominator}"


def _append_stat_rows(
    rows: list[MetricExcelRow],
    period_offsets: list[int],
    *,
    industry_only: bool,
) -> list[MetricExcelRow]:
    detail_rows = [row for row in rows if row.row_kind == ROW_KIND_DETAIL]
    grouped: dict[tuple[str, str, str], list[MetricExcelRow]] = {}
    for row in detail_rows:
        grouped.setdefault((row.sheet_name, row.period_scope, row.metric_base), []).append(row)

    stat_rows: list[MetricExcelRow] = []
    for (_sheet, _scope, metric_base), group_rows in grouped.items():
        for row_kind, aggregator in (
            (ROW_KIND_AVERAGE, _mean),
            (ROW_KIND_MEDIAN, _median),
        ):
            periods_by_offset: dict[int, str] = {}
            values_by_offset: dict[int, float | None] = {}
            units_by_offset: dict[int, str] = {}
            ratios_by_offset: dict[int, float | None] = {}
            raw_values_by_offset: dict[int, float | None] = {}

            for offset in period_offsets:
                raw_values = [
                    float(raw_value)
                    for row in group_rows
                    if (raw_value := _raw_value_for_export_stats(row, offset)) is not None
                ]
                ratio_values = [
                    float(row.ratios_by_offset[offset])
                    for row in group_rows
                    if row.ratios_by_offset.get(offset) is not None
                ]
                raw_stat = aggregator(raw_values)
                display_stat, unit = _stat_value_and_unit(
                    metric_base,
                    raw_stat,
                    industry_only=industry_only,
                )
                periods_by_offset[offset] = _period_text_for_stats(group_rows, offset)
                values_by_offset[offset] = display_stat
                units_by_offset[offset] = unit if raw_stat is not None else ""
                ratios_by_offset[offset] = aggregator(ratio_values)
                raw_values_by_offset[offset] = raw_stat

            first = group_rows[0]
            stat_rows.append(
                MetricExcelRow(
                    sheet_name=first.sheet_name,
                    security_code="",
                    company_name="",
                    industry_33="",
                    market="",
                    period_scope=first.period_scope,
                    row_kind=row_kind,
                    current_period_end=first.current_period_end,
                    metric_base=metric_base,
                    metric_label=row_kind,
                    periods_by_offset=periods_by_offset,
                    values_by_offset=values_by_offset,
                    units_by_offset=units_by_offset,
                    ratios_by_offset=ratios_by_offset,
                    raw_values_by_offset=raw_values_by_offset,
                )
            )

    return [*rows, *stat_rows]


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
    period_scope: str,
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
            source_offset = _source_offset_for_display(period_scope, offset)
            if source_offset is None:
                return False
            filing = by_offset.get(source_offset)
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
            "market": row.market,
            "period_scope": _period_scope_label(row.period_scope),
            "metric": row.metric_label,
        }
        for offset in periods:
            item[PERIOD_LABEL_BY_OFFSET[offset]] = row.values_by_offset.get(offset)
        preview.append(item)
    return preview


def _row_sort_key(row: MetricExcelRow) -> tuple[int, int, int, int, str]:
    sheet_order = SHEET_ORDER.index(row.sheet_name)
    sheet_metric_order = ROW_BASE_ORDER_INDEX_BY_SHEET.get(row.sheet_name, ROW_BASE_ORDER_INDEX)
    metric_order = sheet_metric_order.get(row.metric_base, len(sheet_metric_order))
    scope_order = {
        "annual": 0,
        "half": 2,
        "quarter:1Q": 2,
        "quarter:2Q": 3,
        "quarter:3Q": 4,
        "forecast:initial": 5,
        "forecast:1Q": 6,
        "forecast:2Q": 7,
        "forecast:3Q": 8,
    }.get(row.period_scope, 9)
    row_kind_order = {
        ROW_KIND_DETAIL: 0,
        ROW_KIND_AVERAGE: 1,
        ROW_KIND_MEDIAN: 2,
    }.get(row.row_kind, 9)
    return (sheet_order, metric_order, scope_order, row_kind_order, row.security_code)


def _aggregate_required_bases(row_bases: list[str]) -> list[str]:
    required = set(row_bases)
    for base in row_bases:
        ratio_base = ABSORBED_RATIO_BASE_BY_ROW_BASE.get(base)
        if ratio_base:
            required.add(ratio_base)
        if base in {
            "GrossProfit",
            "CostOfSales",
            "SellingExpenses",
            "OperatingIncome",
            "OrdinaryIncome",
            "ProfitLoss",
        }:
            required.add("NetSales")
        if base == "NetAssets":
            required.add("TotalAssets")
    return sorted(required)


def _build_industry_only_metric_excel_rows(
    conn: sqlite3.Connection,
    condition: MetricExcelCondition,
    *,
    preview_limit: int,
) -> tuple[list[MetricExcelRow], list[str], list[str], list[dict[str, Any]], int]:
    errors: list[str] = []
    warnings: list[str] = []
    if condition.period_scopes != ["annual"]:
        warnings.append("industry_only_mode_forced_to_annual")
    if condition.security_codes:
        warnings.append("industry_only_mode_security_codes_filter_applied")
    if condition.company_names:
        warnings.append("industry_only_mode_company_names_filter_applied")
    if not industry_aggregate_table_exists(conn):
        warnings.append("industry_aggregate_metrics_not_ready")
        return [], errors, warnings, [], 0

    selected_row_bases = _resolve_row_bases(GENERAL_SHEET, condition.metric_labels, errors)
    unsupported_bases = [
        base
        for base in selected_row_bases
        if base not in INDUSTRY_AGGREGATE_VALUE_BASES
    ]
    if unsupported_bases and condition.metric_labels:
        labels = ", ".join(_base_metric_label_for_excel(base) for base in unsupported_bases)
        _append_warning_once(warnings, f"industry_only_suppressed_metrics={labels}")
    selected_row_bases = [
        base
        for base in selected_row_bases
        if base in INDUSTRY_AGGREGATE_VALUE_BASES
    ]
    selected_row_bases = _filter_excel_visible_bases(
        selected_row_bases,
        warnings=warnings,
        explicit_metric_request=bool(condition.metric_labels),
    )
    if not selected_row_bases:
        return [], errors, warnings, [], 0

    ratio_bases = [
        ratio_base
        for base in selected_row_bases
        if (ratio_base := ABSORBED_RATIO_BASE_BY_ROW_BASE.get(base))
    ]
    metric_bases_to_fetch = sorted(set(selected_row_bases + ratio_bases))
    max_fiscal_year = _fetch_industry_aggregate_max_year(conn, condition.industries)
    if max_fiscal_year is None:
        warnings.append("industry_aggregate_metrics_empty")
        return [], errors, warnings, [], 0
    fiscal_years = sorted(
        {
            max_fiscal_year - source_offset
            for offset in condition.period_offsets
            if (source_offset := _source_offset_for_display("annual", offset)) is not None
        }
    )
    aggregate_rows = _fetch_industry_aggregate_rows(
        conn,
        industries=condition.industries,
        fiscal_years=fiscal_years,
        metric_bases=metric_bases_to_fetch,
    )
    aggregate_by_key = {
        (str(row["industry_33"] or ""), int(row["fiscal_year"]), str(row["metric_base"] or "")): row
        for row in aggregate_rows
    }
    industries = sorted({str(row["industry_33"] or "") for row in aggregate_rows})
    rows: list[MetricExcelRow] = []
    for industry in industries:
        for base in selected_row_bases:
            periods_by_offset: dict[int, str] = {}
            values_by_offset: dict[int, float | None] = {}
            units_by_offset: dict[int, str] = {}
            ratios_by_offset: dict[int, float | None] = {}
            raw_values_by_offset: dict[int, float | None] = {}
            for offset in condition.period_offsets:
                source_offset = _source_offset_for_display("annual", offset)
                fiscal_year = max_fiscal_year - source_offset if source_offset is not None else None
                row = (
                    aggregate_by_key.get((industry, fiscal_year, base))
                    if fiscal_year is not None
                    else None
                )
                raw_value = (
                    float(row["value_num"])
                    if row is not None
                    and str(row["calc_status"] or "") == "ok"
                    and row["value_num"] is not None
                    else None
                )
                periods_by_offset[offset] = _aggregate_period_display(
                    INDUSTRY_AGGREGATE_PERIOD_SCOPE,
                    fiscal_year,
                )
                scaled_value, unit = _scale_industry_aggregate_value(base, raw_value)
                values_by_offset[offset] = scaled_value
                raw_values_by_offset[offset] = raw_value
                units_by_offset[offset] = unit if raw_value is not None else ""
                ratio_base = ABSORBED_RATIO_BASE_BY_ROW_BASE.get(base)
                if base in CONSTANT_RATIO_BY_ROW_BASE:
                    ratios_by_offset[offset] = 1.0 if raw_value is not None else None
                elif ratio_base:
                    ratio_row = aggregate_by_key.get((industry, fiscal_year, ratio_base))
                    ratios_by_offset[offset] = (
                        float(ratio_row["value_num"])
                        if ratio_row is not None
                        and str(ratio_row["calc_status"] or "") == "ok"
                        and ratio_row["value_num"] is not None
                        else None
                    )
                else:
                    ratios_by_offset[offset] = None
            rows.append(
                MetricExcelRow(
                    sheet_name=GENERAL_SHEET,
                    security_code="",
                    company_name="",
                    industry_33=industry,
                    market="",
                    period_scope=INDUSTRY_AGGREGATE_PERIOD_SCOPE,
                    row_kind=ROW_KIND_DETAIL,
                    current_period_end=_aggregate_period_display(
                        INDUSTRY_AGGREGATE_PERIOD_SCOPE,
                        max_fiscal_year,
                    ),
                    metric_base=base,
                    metric_label=_metric_label_for_excel(
                        base,
                        industry,
                        period_scope=INDUSTRY_AGGREGATE_PERIOD_SCOPE,
                    ),
                    periods_by_offset=periods_by_offset,
                    values_by_offset=values_by_offset,
                    units_by_offset=units_by_offset,
                    ratios_by_offset=ratios_by_offset,
                    raw_values_by_offset=raw_values_by_offset,
                )
            )

    _assign_ranks(rows, condition.period_offsets)
    rows = _append_stat_rows(rows, condition.period_offsets, industry_only=True)
    rows.sort(key=_row_sort_key)
    preview_rows = _build_preview_rows(rows, condition.period_offsets, preview_limit)
    return rows, errors, warnings, preview_rows, len(industries)


def _jquants_table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type = 'table' AND name = ?
        """,
        (table_name,),
    ).fetchone()
    return row is not None


def _fetch_jquants_companies(
    conn: sqlite3.Connection,
    condition: MetricExcelCondition,
) -> list[sqlite3.Row]:
    where = [
        "coalesce(im.is_listed, 0) = 1",
        "coalesce(im.exchange, '') = 'TSE'",
    ]
    params: list[Any] = []
    if condition.industries:
        placeholders = ",".join("?" for _ in condition.industries)
        where.append(f"im.industry_33 IN ({placeholders})")
        params.extend(condition.industries)
    if condition.company_names:
        placeholders = ",".join("?" for _ in condition.company_names)
        where.append(f"im.company_name IN ({placeholders})")
        params.extend(condition.company_names)
    if condition.security_codes:
        codes = [_normalize_security_code(code) for code in condition.security_codes]
        placeholders = ",".join("?" for _ in codes)
        where.append(
            f"(substr(coalesce(im.security_code, ''), 1, 4) IN ({placeholders}) "
            f"OR coalesce(im.security_code, '') IN ({placeholders}))"
        )
        params.extend([*codes, *codes])
    rows = conn.execute(
        f"""
        SELECT
          im.edinet_code,
          im.security_code,
          im.company_name,
          im.industry_33,
          im.market
        FROM issuer_master im
        WHERE {" AND ".join(where)}
        ORDER BY im.security_code, im.edinet_code
        """,
        params,
    ).fetchall()
    return rows


def _security_code_for_jquants(row: sqlite3.Row) -> str:
    return _normalize_security_code(str(row["security_code"] or ""))


def _latest_jquants_metric_rows(
    rows: list[sqlite3.Row],
) -> dict[tuple[str, str, str, int, str, str], sqlite3.Row]:
    latest: dict[tuple[str, str, str, int, str, str], sqlite3.Row] = {}
    for row in rows:
        fiscal_year = row["fiscal_year"]
        if fiscal_year is None:
            continue
        key = (
            _normalize_security_code(row["security_code"] or row["local_code"] or ""),
            str(row["period_scope"] or ""),
            str(row["metric_base"] or ""),
            int(fiscal_year),
            str(row["period_key"] or ""),
            str(row["forecast_stage"] or ""),
        )
        existing = latest.get(key)
        if existing is None:
            latest[key] = row
            continue
        existing_order = (str(existing["disclosed_date"] or ""), str(existing["disclosed_time"] or ""))
        row_order = (str(row["disclosed_date"] or ""), str(row["disclosed_time"] or ""))
        if row_order >= existing_order:
            latest[key] = row
    return latest


def _fetch_jquants_metric_rows(
    conn: sqlite3.Connection,
    *,
    security_codes: list[str],
    metric_bases: list[str],
    min_fiscal_year: int | None = None,
) -> list[sqlite3.Row]:
    if not security_codes or not metric_bases:
        return []
    columns = {
        str(row[1])
        for row in conn.execute("PRAGMA table_info(jquants_financial_metrics)").fetchall()
    }
    forecast_stage_expr = "forecast_stage" if "forecast_stage" in columns else "NULL AS forecast_stage"
    code_placeholders = ",".join("?" for _ in security_codes)
    base_placeholders = ",".join("?" for _ in metric_bases)
    where = [
        f"(security_code IN ({code_placeholders}) OR local_code IN ({code_placeholders}))",
        f"metric_base IN ({base_placeholders})",
    ]
    params: list[Any] = [*security_codes, *security_codes, *metric_bases]
    if min_fiscal_year is not None:
        where.append("fiscal_year >= ?")
        params.append(min_fiscal_year)
    rows = conn.execute(
        f"""
        SELECT
          disclosure_number,
          local_code,
          security_code,
          metric_kind,
          period_scope,
          period_key,
          quarter_type,
          forecast_target,
          {forecast_stage_expr},
          fiscal_year,
          period_start,
          period_end,
          disclosed_date,
          disclosed_time,
          metric_key,
          metric_base,
          metric_group,
          value_num,
          value_unit,
          calc_status,
          source_field
        FROM jquants_financial_metrics
        WHERE {" AND ".join(where)}
        ORDER BY security_code, fiscal_year, period_key, forecast_stage, metric_base, disclosed_date, disclosed_time
        """,
        params,
    ).fetchall()
    if market_derived_table_exists(conn):
        market_metric_bases = [base for base in metric_bases if base in MARKET_METRIC_BASES]
        if market_metric_bases:
            market_base_placeholders = ",".join("?" for _ in market_metric_bases)
            market_where = [
                "source_type = 'jquants'",
                f"security_code IN ({code_placeholders})",
                f"metric_base IN ({market_base_placeholders})",
            ]
            market_params: list[Any] = [*security_codes, *market_metric_bases]
            if min_fiscal_year is not None:
                market_where.append("fiscal_year >= ?")
                market_params.append(min_fiscal_year)
            rows.extend(
                conn.execute(
                    f"""
                    SELECT
                      source_id AS disclosure_number,
                      '' AS local_code,
                      security_code,
                      'market_derived' AS metric_kind,
                      period_scope,
                      period_key,
                      quarter_type,
                      NULL AS forecast_target,
                      NULL AS forecast_stage,
                      fiscal_year,
                      NULL AS period_start,
                      period_end,
                      period_end AS disclosed_date,
                      '' AS disclosed_time,
                      metric_key,
                      metric_base,
                      metric_group,
                      value_num,
                      value_unit,
                      calc_status,
                      formula_name AS source_field
                    FROM market_derived_metrics
                    WHERE {" AND ".join(market_where)}
                    ORDER BY security_code, fiscal_year, period_key, metric_base
                    """,
                    market_params,
                ).fetchall()
            )
    return rows


def _max_jquants_fiscal_year(
    rows: list[sqlite3.Row],
    security_code: str,
    period_key: str,
    forecast_stage: str | None = None,
) -> int | None:
    years = [
        int(row["fiscal_year"])
        for row in rows
        if _normalize_security_code(row["security_code"] or row["local_code"] or "") == security_code
        and str(row["period_key"] or "") == period_key
        and (forecast_stage is None or str(row["forecast_stage"] or "") == forecast_stage)
        and row["fiscal_year"] is not None
    ]
    return max(years) if years else None


def _jquants_period_display(row: sqlite3.Row | None, period_scope: str, metric_base: str | None = None) -> str:
    if row is None:
        return ""
    period_end = str(row["period_end"] or "")
    if metric_base in DATE_POINT_PERIOD_BASES and period_scope.startswith("quarter:"):
        quarter = period_scope.split(":", 1)[1]
        return f"{quarter} {period_end}\u6642\u70b9" if period_end else quarter
    period_month = period_end[:7] if len(period_end) >= 7 else period_end
    if period_scope.startswith("quarter:"):
        quarter = period_scope.split(":", 1)[1]
        return f"{quarter} {period_month}" if period_month else quarter
    if period_scope.startswith("forecast:"):
        stage = period_scope.split(":", 1)[1]
        stage_label = f"{JQUANTS_FORECAST_STAGE_LABELS.get(stage, stage)}\u4e88\u60f3"
        return f"{stage_label} {period_month}" if period_month else stage_label
    return period_month


def _scale_jquants_value(metric_base: str, value: float | None) -> tuple[float | None, str]:
    if value is None:
        return None, ""
    if metric_base in MONETARY_BASES:
        return value / 1_000_000, "\u767e\u4e07\u5186"
    if metric_base == "OutstandingShares" or metric_base in {"IssuedShares", "TreasuryShares"}:
        return value, "\u682a"
    if metric_base in GROWTH_RATIO_BASES or metric_base in PERCENT_VALUE_BASES:
        return value, "%"
    if metric_base in RATIO_VALUE_BASES:
        return value, "\u500d"
    if metric_base in PER_SHARE_BASES or metric_base in ONE_DECIMAL_VALUE_BASES:
        return value, "\u5186"
    return value, ""


def _append_jquants_rows(
    conn: sqlite3.Connection,
    condition: MetricExcelCondition,
    rows: list[MetricExcelRow],
    selected_row_bases_by_sheet: dict[str, list[str]],
    warnings: list[str],
) -> None:
    needs_quarter = "quarter" in condition.period_scopes
    needs_forecast = "forecast" in condition.period_scopes
    if not needs_quarter and not needs_forecast:
        return
    if not _jquants_table_exists(conn, "jquants_financial_metrics"):
        warnings.append("jquants_financial_metrics_not_ready")
        return

    companies = _fetch_jquants_companies(conn, condition)
    if not companies:
        return
    security_codes = sorted({_security_code_for_jquants(company) for company in companies if _security_code_for_jquants(company)})
    requested_bases = sorted(
        {
            base
            for sheet, bases in selected_row_bases_by_sheet.items()
            for base in bases
            if base in (QUARTER_SUPPORTED_BASES | FORECAST_SUPPORTED_BASES)
        }
    )
    if not requested_bases:
        return
    max_offset = max(condition.period_offsets or [0])
    all_metric_rows = _fetch_jquants_metric_rows(
        conn,
        security_codes=security_codes,
        metric_bases=sorted(set(requested_bases) | FORECAST_PROGRESS_BASES),
        min_fiscal_year=None,
    )
    if not all_metric_rows:
        warnings.append("jquants_metrics_not_found")
        return
    latest = _latest_jquants_metric_rows(all_metric_rows)

    for company in companies:
        security_code = _security_code_for_jquants(company)
        if not security_code:
            continue
        sheet_name = _sheet_name_for_industry(company["industry_33"])
        base_candidates = selected_row_bases_by_sheet[sheet_name]
        if needs_quarter:
            for quarter in JQUANTS_QUARTER_TYPES:
                _append_jquants_period_rows(
                    rows,
                    company=company,
                    security_code=security_code,
                    base_candidates=[base for base in base_candidates if base in QUARTER_SUPPORTED_BASES],
                    period_scope=f"quarter:{quarter}",
                    period_key=f"actual:{quarter}",
                    latest=latest,
                    all_rows=all_metric_rows,
                    period_offsets=condition.period_offsets,
                    max_offset=max_offset,
                    with_progress_ratio=True,
                )
        if needs_forecast:
            for forecast_stage in JQUANTS_FORECAST_STAGES:
                _append_jquants_period_rows(
                    rows,
                    company=company,
                    security_code=security_code,
                    base_candidates=[base for base in base_candidates if base in FORECAST_SUPPORTED_BASES],
                    period_scope=f"forecast:{forecast_stage}",
                    period_key="forecast:FY",
                    forecast_stage=forecast_stage,
                    latest=latest,
                    all_rows=all_metric_rows,
                    period_offsets=condition.period_offsets,
                    max_offset=max_offset,
                    with_progress_ratio=False,
                )


def _append_jquants_period_rows(
    rows: list[MetricExcelRow],
    *,
    company: sqlite3.Row,
    security_code: str,
    base_candidates: list[str],
    period_scope: str,
    period_key: str,
    latest: dict[tuple[str, str, str, int, str, str], sqlite3.Row],
    all_rows: list[sqlite3.Row],
    period_offsets: list[int],
    max_offset: int,
    with_progress_ratio: bool,
    forecast_stage: str | None = None,
) -> None:
    if not base_candidates:
        return
    max_fiscal_year = _max_jquants_fiscal_year(
        all_rows,
        security_code,
        period_key,
        forecast_stage=forecast_stage,
    )
    if max_fiscal_year is None:
        return
    min_year = max_fiscal_year - max_offset
    for base in base_candidates:
        periods_by_offset: dict[int, str] = {}
        values_by_offset: dict[int, float | None] = {}
        units_by_offset: dict[int, str] = {}
        ratios_by_offset: dict[int, float | None] = {}
        raw_values_by_offset: dict[int, float | None] = {}
        ratio_kinds_by_offset: dict[int, str] = {}
        for offset in period_offsets:
            fiscal_year = max_fiscal_year - offset
            if fiscal_year < min_year:
                continue
            row = latest.get(
                (
                    security_code,
                    "quarter" if period_scope.startswith("quarter") else "forecast",
                    base,
                    fiscal_year,
                    period_key,
                    forecast_stage or "",
                )
            )
            raw_value = (
                float(row["value_num"])
                if row is not None
                and str(row["calc_status"] or "") == "ok"
                and row["value_num"] is not None
                else None
            )
            display_value, display_unit = _scale_jquants_value(base, raw_value)
            periods_by_offset[offset] = _jquants_period_display(row, period_scope, base)
            values_by_offset[offset] = display_value
            units_by_offset[offset] = display_unit if raw_value is not None else ""
            raw_values_by_offset[offset] = raw_value
            ratios_by_offset[offset] = None
            if with_progress_ratio and base in FORECAST_PROGRESS_BASES and row is not None:
                forecast_value = _latest_forecast_value_as_of(
                    all_rows,
                    security_code=security_code,
                    fiscal_year=fiscal_year,
                    metric_base=base,
                    disclosed_date=str(row["disclosed_date"] or ""),
                )
                if forecast_value is not None and forecast_value > 0 and raw_value is not None:
                    ratios_by_offset[offset] = raw_value / forecast_value
                    ratio_kinds_by_offset[offset] = FORECAST_PROGRESS_RATIO_KIND
        if not any(value is not None for value in raw_values_by_offset.values()):
            continue
        rows.append(
            MetricExcelRow(
                sheet_name=_sheet_name_for_industry(company["industry_33"]),
                security_code=security_code,
                company_name=str(company["company_name"] or ""),
                industry_33=str(company["industry_33"] or ""),
                market=str(company["market"] or ""),
                period_scope=period_scope,
                row_kind=ROW_KIND_DETAIL,
                current_period_end=periods_by_offset.get(0, ""),
                metric_base=base,
                metric_label=_metric_label_for_excel(
                    base,
                    company["industry_33"],
                    period_scope=period_scope,
                ),
                periods_by_offset=periods_by_offset,
                values_by_offset=values_by_offset,
                units_by_offset=units_by_offset,
                ratios_by_offset=ratios_by_offset,
                raw_values_by_offset=raw_values_by_offset,
                ratio_kinds_by_offset=ratio_kinds_by_offset,
            )
        )


def _latest_forecast_value_as_of(
    rows: list[sqlite3.Row],
    *,
    security_code: str,
    fiscal_year: int,
    metric_base: str,
    disclosed_date: str,
) -> float | None:
    candidates = [
        row
        for row in rows
        if _normalize_security_code(row["security_code"] or row["local_code"] or "") == security_code
        and str(row["period_scope"] or "") == "forecast"
        and str(row["metric_base"] or "") == metric_base
        and row["fiscal_year"] is not None
        and int(row["fiscal_year"]) == fiscal_year
        and str(row["period_key"] or "") == "forecast:FY"
        and str(row["calc_status"] or "") == "ok"
        and row["value_num"] is not None
        and str(row["disclosed_date"] or "") <= disclosed_date
    ]
    if not candidates:
        return None
    candidates.sort(key=lambda row: (str(row["disclosed_date"] or ""), str(row["disclosed_time"] or "")))
    return float(candidates[-1]["value_num"])


def build_metric_excel_rows(
    conn: sqlite3.Connection,
    condition: MetricExcelCondition,
    *,
    preview_limit: int = 10,
) -> tuple[list[MetricExcelRow], list[str], list[str], list[dict[str, Any]], int]:
    if condition.industry_only:
        return _build_industry_only_metric_excel_rows(
            conn,
            condition,
            preview_limit=preview_limit,
        )

    errors: list[str] = []
    warnings: list[str] = []
    filings = _fetch_ranked_filings(conn, condition)

    filings_by_company_scope: dict[tuple[str, str], list[sqlite3.Row]] = {}
    for row in filings:
        period_scope = PERIOD_SCOPE_BY_FORM_TYPE.get(str(row["form_type"] or ""), "annual")
        filings_by_company_scope.setdefault((str(row["edinet_code"]), period_scope), []).append(row)

    selected_row_bases_by_sheet = {
        sheet: _filter_excel_visible_bases(
            _resolve_row_bases(sheet, condition.metric_labels, errors),
            warnings=warnings,
            explicit_metric_request=bool(condition.metric_labels),
        )
        for sheet in SHEET_ORDER
    }

    selected_value_bases: set[str] = set()
    for bases in selected_row_bases_by_sheet.values():
        for base in bases:
            selected_value_bases.add(base)
            ratio_base = ABSORBED_RATIO_BASE_BY_ROW_BASE.get(base)
            if ratio_base:
                selected_value_bases.add(ratio_base)
    if selected_value_bases & MARKET_METRIC_BASES and not market_derived_table_exists(conn):
        warnings.append("market_derived_metrics_not_ready")

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
        if current_period_scope != "quarter:2Q":
            row_bases = [base for base in row_bases if base not in HALF_ONLY_BASES]
        else:
            row_bases = [base for base in row_bases if base not in HALF_DISABLED_BASES]

        if not _passes_percent_filters(
            filter_bases=percent_filter_bases_by_sheet.get(sheet_name, []),
            filter_offsets=condition.percent_filter_period_offsets,
            period_scope=current_period_scope,
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
                    source_offset = _source_offset_for_display(current_period_scope, offset)
                    filing = by_offset.get(source_offset) if source_offset is not None else None
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
            raw_values_by_offset: dict[int, float | None] = {}
            ratio_base = ABSORBED_RATIO_BASE_BY_ROW_BASE.get(base)
            allowed_offsets = SPARSE_PERIOD_OFFSETS_BY_BASE.get(base)
            for offset in condition.period_offsets:
                if allowed_offsets is not None and offset not in allowed_offsets:
                    periods_by_offset[offset] = ""
                    values_by_offset[offset] = None
                    units_by_offset[offset] = ""
                    ratios_by_offset[offset] = None
                    raw_values_by_offset[offset] = None
                    continue

                source_offset = _source_offset_for_display(current_period_scope, offset)
                filing = by_offset.get(source_offset) if source_offset is not None else None
                if filing is None:
                    periods_by_offset[offset] = ""
                    values_by_offset[offset] = None
                    units_by_offset[offset] = ""
                    ratios_by_offset[offset] = None
                    raw_values_by_offset[offset] = None
                    continue

                doc_id = str(filing["doc_id"])
                periods_by_offset[offset] = (
                    _period_point_display_for_filing(filing, current_period_scope)
                    if base in DATE_POINT_PERIOD_BASES
                    else _period_display_for_filing(filing)
                )
                raw_value = metric_values.get((doc_id, _metric_key(base)))
                raw_values_by_offset[offset] = raw_value
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
                    market=str(current["market"] or ""),
                    period_scope=current_period_scope,
                    row_kind=ROW_KIND_DETAIL,
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
                    raw_values_by_offset=raw_values_by_offset,
                )
            )

    _append_jquants_rows(conn, condition, rows, selected_row_bases_by_sheet, warnings)
    if any(
        FORECAST_PROGRESS_RATIO_KIND in row.ratio_kinds_by_offset.values()
        for row in rows
        if row.row_kind == ROW_KIND_DETAIL
    ):
        warnings.append("quarter_ratio_cells_show_latest_forecast_progress")
    target_companies = len({row.security_code for row in rows if row.row_kind == ROW_KIND_DETAIL})
    _assign_ranks(rows, condition.period_offsets)
    rows = _append_stat_rows(rows, condition.period_offsets, industry_only=False)
    rows.sort(key=_row_sort_key)
    preview_rows = _build_preview_rows(rows, condition.period_offsets, preview_limit)
    return rows, errors, warnings, preview_rows, target_companies


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
        ("industry_only", "1" if condition.industry_only else "0"),
        (
            "aggregation_basis_note",
            (
                "\u96c6\u8a08\u57fa\u6e96: "
                f"{datetime.now().isoformat(timespec='minutes')}"
                "\u6642\u70b9\u3067DB\u306b\u53cd\u6620\u6e08\u307f\u306e\u6c7a\u7b97\u671f\u672b\u65e5\u30d9\u30fc\u30b9"
            )
            if condition.industry_only
            else "",
        ),
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


def _with_left_border(cell: Any, side: Side) -> Border:
    current = cell.border
    return Border(
        left=side,
        right=current.right,
        top=current.top,
        bottom=current.bottom,
        diagonal=current.diagonal,
        diagonal_direction=current.diagonal_direction,
        diagonalUp=current.diagonalUp,
        diagonalDown=current.diagonalDown,
        outline=current.outline,
        vertical=current.vertical,
        horizontal=current.horizontal,
    )


def _apply_period_block_styles(
    ws: Any,
    *,
    period_offsets: list[int],
    start_col: int,
    block_width: int,
) -> None:
    border_side = Side(style="thin", color=PERIOD_BLOCK_BORDER_COLOR)
    for block_index, offset in enumerate(period_offsets):
        first_col = start_col + block_index * block_width
        last_col = first_col + block_width - 1
        fill_color = (
            CURRENT_PERIOD_BLOCK_FILL_COLOR
            if offset == 0
            else PERIOD_BLOCK_FILL_COLORS[block_index % len(PERIOD_BLOCK_FILL_COLORS)]
        )
        fill = PatternFill("solid", fgColor=fill_color)
        for row_idx in range(1, ws.max_row + 1):
            for col_idx in range(first_col, last_col + 1):
                cell = ws.cell(row_idx, col_idx)
                cell.fill = fill
                if col_idx == first_col:
                    cell.border = _with_left_border(cell, border_side)


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
    headers = [
        "\u8a3c\u5238\u30b3\u30fc\u30c9",
        "\u4f01\u696d\u540d",
        "\u30c6\u30f3\u30d0\u30ac\u30fc",
        "\u696d\u7a2e",
        "\u5e02\u5834\u533a\u5206",
        "\u6c7a\u7b97\u7a2e\u5225",
        "\u884c\u7a2e\u5225",
        "\u671f\u672b\u5e74\u6708\u65e5_\u5f53\u671f",
        "\u6307\u6a19",
    ]
    for offset in period_offsets:
        label = PERIOD_LABEL_BY_OFFSET[offset]
        headers.extend(
            [
                f"{label}_\u671f\u9593",
                f"{label}_\u6570\u5024",
                f"{label}_\u5358\u4f4d",
                f"{label}_\u6bd4\u7387",
                f"{label}_\u9806\u4f4d",
            ]
        )
    ws.append(headers)

    header_fill = PatternFill("solid", fgColor="D9EAF7")
    for cell in ws[1]:
        cell.font = Font(bold=True)
        cell.fill = header_fill

    forecast_progress_cells: list[Any] = []
    for row in rows:
        values: list[Any] = [
            row.security_code,
            row.company_name,
            tenbagger_learning_mark(row.security_code),
            row.industry_33,
            row.market,
            _period_scope_label(row.period_scope),
            row.row_kind,
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
                    row.ranks_by_offset.get(offset, ""),
                ]
            )
        ws.append(values)
        current_row = ws.max_row
        for idx, offset in enumerate(period_offsets):
            value_col = 11 + idx * 5
            ratio_col = value_col + 2
            _format_value_cell(ws.cell(current_row, value_col), row.metric_base)
            ratio_cell = ws.cell(current_row, ratio_col)
            ratio_cell.number_format = "0.0%"
            if row.ratio_kinds_by_offset.get(offset) == FORECAST_PROGRESS_RATIO_KIND:
                forecast_progress_cells.append(ratio_cell)

    _apply_period_block_styles(
        ws,
        period_offsets=period_offsets,
        start_col=10,
        block_width=5,
    )
    progress_fill = PatternFill("solid", fgColor=FORECAST_PROGRESS_FILL_COLOR)
    for cell in forecast_progress_cells:
        cell.fill = progress_fill
        cell.comment = Comment(
            "\u3053\u306e\u6bd4\u7387\u306f\u3001\u56db\u534a\u671f\u5b9f\u7e3e \u00f7 \u540c\u4e00\u5e74\u5ea6\u306e\u6700\u65b0\u901a\u671f\u4e88\u60f3\u3067\u8a08\u7b97\u3057\u305f\u6700\u65b0\u4e88\u60f3\u9032\u6357\u7387\u3067\u3059\u3002",
            "EDINET_MONITOR",
        )
    ws.freeze_panes = "J2"
    ws.auto_filter.ref = ws.dimensions
    widths = {
        "A": 12,
        "B": 28,
        "C": 12,
        "D": 18,
        "E": 12,
        "F": 12,
        "G": 16,
        "H": 16,
        "I": 24,
    }
    for column, width in widths.items():
        ws.column_dimensions[column].width = width
    for col_idx in range(10, ws.max_column + 1):
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
        "\u30c6\u30f3\u30d0\u30ac\u30fc",
        "\u696d\u7a2e",
        "\u5e02\u5834\u533a\u5206",
        "\u6c7a\u7b97\u7a2e\u5225",
        "\u884c\u7a2e\u5225",
        "\u671f\u9593",
        "\u6307\u6a19",
        "\u6570\u5024",
        "\u5358\u4f4d",
        "\u6bd4\u7387",
        "\u9806\u4f4d",
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
                    tenbagger_learning_mark(row.security_code),
                    row.industry_33,
                    row.market,
                    _period_scope_label(row.period_scope),
                    row.row_kind,
                    period_text,
                    row.metric_label,
                    row.values_by_offset.get(offset),
                    row.units_by_offset.get(offset, ""),
                    row.ratios_by_offset.get(offset),
                    row.ranks_by_offset.get(offset, ""),
                ]
            )
            current_row = ws.max_row
            _format_value_cell(ws.cell(current_row, 10), row.metric_base)
            ws.cell(current_row, 12).number_format = "0.0%"

    ws.freeze_panes = "A2"
    ws.auto_filter.ref = ws.dimensions
    widths = {
        "A": 12,
        "B": 28,
        "C": 12,
        "D": 18,
        "E": 12,
        "F": 12,
        "G": 14,
        "H": 28,
        "I": 16,
        "J": 10,
        "K": 12,
        "L": 12,
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
    target_companies: int | None = None,
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
        target_companies=target_companies
        if target_companies is not None
        else len({row.security_code for row in rows}),
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
        target_companies=target_companies,
    )
    return MetricExcelExportResult(
        output_path=path,
        target_companies=target_companies,
        output_rows=len(rows),
        errors=errors,
        warnings=warnings,
        preview_rows=preview_rows,
    )
