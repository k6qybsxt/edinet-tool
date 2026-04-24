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
CONDITION_SHEET = "条件"

SHEET_ORDER = [
    GENERAL_SHEET,
    BANK_INDUSTRY_LABEL,
    SECURITIES_INDUSTRY_LABEL,
    INSURANCE_INDUSTRY_LABEL,
]

PERIOD_LABEL_BY_OFFSET = {
    0: "当期",
    1: "1年前",
    2: "2年前",
    3: "3年前",
    4: "4年前",
    5: "5年前",
    6: "6年前",
    7: "7年前",
    8: "8年前",
    9: "9年前",
    10: "10年前",
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
    "CashAndCashEquivalents",
    "OperatingCash",
    "InvestmentCash",
    "FinancingCash",
    "FCF",
    "EPS",
    "EPSGrowthRate",
    "BPS",
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
        "CashAndCashEquivalents",
        "OperatingCash",
        "InvestmentCash",
        "FinancingCash",
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
        "CashAndCashEquivalents",
        "OperatingCash",
        "InvestmentCash",
        "FinancingCash",
        "FCF",
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
        "CashAndCashEquivalents",
        "OperatingCash",
        "InvestmentCash",
        "FinancingCash",
        "FCF",
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
        "CashAndCashEquivalents",
        "OperatingCash",
        "InvestmentCash",
        "FinancingCash",
        "FCF",
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
    "期間": "periods",
    "増減判定": "trend",
    "増減判定指標": "trend_metrics",
    "増減判定期間": "trend_periods",
}

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
    period_offsets: list[int] = field(default_factory=lambda: list(range(10, -1, -1)))
    trend: str = "none"
    trend_metric_labels: list[str] = field(default_factory=list)
    trend_period_offsets: list[int] = field(default_factory=list)
    raw_values: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class MetricExcelRow:
    sheet_name: str
    security_code: str
    company_name: str
    industry_33: str
    current_period_end: str
    metric_base: str
    metric_label: str
    values_by_offset: dict[int, float | None]
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
    match = re.fullmatch(r"(\d+)年前", text)
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


def read_metric_excel_condition(condition_xlsx: str | Path) -> MetricExcelCondition:
    path = Path(condition_xlsx)
    workbook = load_workbook(path, data_only=True)
    if CONDITION_SHEET not in workbook.sheetnames:
        raise ValueError("条件シートがありません。条件シートを追加してください。")

    ws = workbook[CONDITION_SHEET]
    raw: dict[str, str] = {}
    for row in ws.iter_rows(values_only=True):
        cells = ["" if cell is None else str(cell).strip() for cell in row]
        for idx, cell in enumerate(cells):
            key = CONDITION_KEYS.get(_normalize_text(cell))
            if key is None:
                continue
            value = ""
            for candidate in cells[idx + 1 :]:
                if candidate:
                    value = candidate
                    break
            raw[key] = value

    trend = str(raw.get("trend") or "none").strip().lower()
    if trend in {"", "all"}:
        trend = "none"
    if trend not in {"none", "increase", "decrease"}:
        raise ValueError("増減判定は none / increase / decrease のいずれかで指定してください。")

    period_offsets = _parse_periods(raw.get("periods"))
    trend_period_offsets = _parse_periods(raw.get("trend_periods")) if raw.get("trend_periods") else period_offsets

    return MetricExcelCondition(
        industries=_split_industries(raw.get("industries")),
        security_codes=[_normalize_security_code(code) for code in _split_multi(raw.get("security_codes"))],
        company_names=_split_multi(raw.get("company_names")),
        metric_labels=_split_multi(raw.get("metrics")),
        period_offsets=period_offsets,
        trend=trend,
        trend_metric_labels=_split_multi(raw.get("trend_metrics")),
        trend_period_offsets=trend_period_offsets,
        raw_values=raw,
    )


def _sheet_name_for_industry(industry_33: str | None) -> str:
    industry = str(industry_33 or "").strip()
    if industry in {BANK_INDUSTRY_LABEL, SECURITIES_INDUSTRY_LABEL, INSURANCE_INDUSTRY_LABEL}:
        return industry
    return GENERAL_SHEET


def _metric_label_for_excel(metric_base: str, industry_33: str | None = None) -> str:
    industry = str(industry_33 or "").strip()
    industry_overrides = INDUSTRY_EXCEL_METRIC_LABEL_OVERRIDES.get(industry, {})
    if metric_base in industry_overrides:
        return industry_overrides[metric_base]
    return EXCEL_METRIC_LABEL_OVERRIDES.get(
        metric_base,
        metric_base_to_display_name(metric_base, industry_33),
    )


def _build_label_to_base_map(sheet_name: str) -> dict[str, str]:
    mapping: dict[str, str] = {}
    candidate_bases = set(METRIC_BASE_LABELS) | set(DEFAULT_BASES_BY_SHEET[sheet_name]) | ABSORBED_RATIO_BASES
    for base in candidate_bases:
        mapping[_normalize_text(metric_base_to_display_name(base))] = base
        mapping[_normalize_text(metric_base_to_display_name(base, sheet_name))] = base
        mapping[_normalize_text(_metric_label_for_excel(base, sheet_name))] = base
        for industry in SHEET_ORDER:
            mapping[_normalize_text(metric_base_to_display_name(base, industry))] = base
            mapping[_normalize_text(_metric_label_for_excel(base, industry))] = base
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
    where = [
        "f.form_type = '030000'",
        "f.parse_status = 'derived_metrics_saved'",
        "coalesce(im.is_listed, 0) = 1",
        "coalesce(im.exchange, '') = 'TSE'",
    ]

    if condition.industries:
        placeholders = ",".join("?" for _ in condition.industries)
        where.append(f"im.industry_33 IN ({placeholders})")
        params.extend(condition.industries)

    if condition.company_names:
        placeholders = ",".join("?" for _ in condition.company_names)
        where.append(f"im.company_name IN ({placeholders})")
        params.extend(condition.company_names)

    sql = f"""
    WITH ranked AS (
      SELECT
        f.doc_id,
        f.edinet_code,
        f.security_code,
        f.period_end,
        f.submit_date,
        f.document_display_unit,
        im.company_name,
        im.industry_33,
        im.security_code AS issuer_security_code,
        ROW_NUMBER() OVER (
          PARTITION BY f.edinet_code
          ORDER BY f.period_end DESC, coalesce(f.submit_date, '') DESC, f.doc_id DESC
        ) - 1 AS period_offset
      FROM filings f
      JOIN issuer_master im
        ON im.edinet_code = f.edinet_code
      WHERE {" AND ".join(where)}
    )
    SELECT *
    FROM ranked
    WHERE period_offset BETWEEN 0 AND 10
    ORDER BY edinet_code, period_offset
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
    if value is None:
        return None
    if metric_base in MONETARY_BASES:
        return value / 1_000_000
    return value


def _passes_trend(values: list[float | None], direction: str) -> bool:
    if direction == "none":
        return True
    if len(values) < 2 or any(value is None for value in values):
        return False
    pairs = zip(values, values[1:])
    if direction == "increase":
        return all(float(left) < float(right) for left, right in pairs)
    return all(float(left) > float(right) for left, right in pairs)


def _build_preview_rows(rows: list[MetricExcelRow], periods: list[int], limit: int) -> list[dict[str, Any]]:
    preview = []
    for row in rows[:limit]:
        item: dict[str, Any] = {
            "security_code": row.security_code,
            "company_name": row.company_name,
            "industry_33": row.industry_33,
            "metric": row.metric_label,
        }
        for offset in periods:
            item[PERIOD_LABEL_BY_OFFSET[offset]] = row.values_by_offset.get(offset)
        preview.append(item)
    return preview


def _row_sort_key(row: MetricExcelRow) -> tuple[int, int, str]:
    sheet_order = SHEET_ORDER.index(row.sheet_name)
    sheet_metric_order = ROW_BASE_ORDER_INDEX_BY_SHEET.get(row.sheet_name, ROW_BASE_ORDER_INDEX)
    metric_order = sheet_metric_order.get(row.metric_base, len(sheet_metric_order))
    return (sheet_order, metric_order, row.security_code)


def build_metric_excel_rows(
    conn: sqlite3.Connection,
    condition: MetricExcelCondition,
    *,
    preview_limit: int = 10,
) -> tuple[list[MetricExcelRow], list[str], list[str], list[dict[str, Any]], int]:
    errors: list[str] = []
    warnings: list[str] = []
    filings = _fetch_ranked_filings(conn, condition)

    filings_by_company: dict[str, list[sqlite3.Row]] = {}
    for row in filings:
        filings_by_company.setdefault(str(row["edinet_code"]), []).append(row)

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

    if condition.trend != "none":
        trend_labels = condition.trend_metric_labels or condition.metric_labels
        if trend_labels:
            for sheet in SHEET_ORDER:
                for base in _resolve_value_bases(sheet, trend_labels, errors):
                    selected_value_bases.add(base)
        else:
            for bases in selected_row_bases_by_sheet.values():
                selected_value_bases.update(bases)

    doc_ids = [str(row["doc_id"]) for row in filings]
    metric_values = _fetch_metric_values(
        conn,
        doc_ids=doc_ids,
        metric_bases=sorted(selected_value_bases),
    )

    rows: list[MetricExcelRow] = []
    for edinet_code, company_filings in filings_by_company.items():
        by_offset = {int(row["period_offset"]): row for row in company_filings}
        current = by_offset.get(0)
        if current is None:
            continue

        sheet_name = _sheet_name_for_industry(current["industry_33"])
        row_bases = selected_row_bases_by_sheet[sheet_name]

        if condition.trend != "none":
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
            if not trend_ok:
                continue

        security_code = _normalize_security_code(current["issuer_security_code"] or current["security_code"] or "")
        for base in row_bases:
            values_by_offset: dict[int, float | None] = {}
            ratios_by_offset: dict[int, float | None] = {}
            ratio_base = ABSORBED_RATIO_BASE_BY_ROW_BASE.get(base)
            for offset in condition.period_offsets:
                filing = by_offset.get(offset)
                if filing is None:
                    values_by_offset[offset] = None
                    ratios_by_offset[offset] = None
                    continue

                doc_id = str(filing["doc_id"])
                raw_value = metric_values.get((doc_id, _metric_key(base)))
                values_by_offset[offset] = _scale_value(base, raw_value)

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
                    current_period_end=str(current["period_end"] or ""),
                    metric_base=base,
                    metric_label=_metric_label_for_excel(base, current["industry_33"]),
                    values_by_offset=values_by_offset,
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
        (
            "periods",
            ", ".join(PERIOD_LABEL_BY_OFFSET[offset] for offset in condition.period_offsets),
        ),
        ("trend", condition.trend),
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
    headers = ["証券コード", "企業名", "業種", "期末年月日_当期", "指標"]
    for offset in period_offsets:
        label = PERIOD_LABEL_BY_OFFSET[offset]
        headers.extend([f"{label}_数値", f"{label}_比率"])
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
            row.current_period_end,
            row.metric_label,
        ]
        for offset in period_offsets:
            values.extend([row.values_by_offset.get(offset), row.ratios_by_offset.get(offset)])
        ws.append(values)
        current_row = ws.max_row
        for idx, offset in enumerate(period_offsets):
            value_col = 6 + idx * 2
            ratio_col = value_col + 1
            _format_value_cell(ws.cell(current_row, value_col), row.metric_base)
            ws.cell(current_row, ratio_col).number_format = "0.0%"

    ws.freeze_panes = "F2"
    ws.auto_filter.ref = ws.dimensions
    widths = {
        "A": 12,
        "B": 28,
        "C": 18,
        "D": 16,
        "E": 24,
    }
    for column, width in widths.items():
        ws.column_dimensions[column].width = width
    for col_idx in range(6, ws.max_column + 1):
        ws.column_dimensions[get_column_letter(col_idx)].width = 14


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
