from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime
import gc
from pathlib import Path
import json
import re
import sqlite3
from typing import Any

from lxml import html as lxml_html

from edinet_pipeline.services.linkbase_analyzer import analyze_linkbase_structure
from edinet_monitor.services.segment_name_normalize_service import (
    SegmentNameCandidate,
    canonical_segment_key,
    preferred_segment_name_map,
)


SEGMENT_RULE_VERSION = "segment-metrics-2026-08-02-v3"
SEGMENT_SAVE_DOC_ID_BATCH_SIZE = 250
CURRENT_FISCAL_YEAR_END_DEI_TAG = "CurrentFiscalYearEndDateDEI"
SEGMENT_FORM_CODES = ("030000", "043A00", "043000")
SEGMENT_PERIOD_SCOPE_BY_FORM_TYPE = {
    "030000": ("annual", ""),
    "043A00": ("quarter", "2Q"),
    "043000": ("quarter", "2Q"),
}

SEGMENT_AXIS_MARKERS = (
    "OperatingSegmentsAxis",
    "ReportableSegments",
    "Segment",
    "Geographical",
    "Geographic",
    "Region",
    "Area",
    "Business",
    "Product",
    "Service",
)
EXCLUDED_MEMBER_MARKERS = (
    "ReconcilingItems",
    "CorporateShared",
    "CorporateExpensesAndElimination",
    "Elimination",
    "Adjustment",
)
TOTAL_MEMBER_MARKERS = (
    "TotalOfReportableSegmentsAndOthersMember",
    "TotalOfReportableSegmentsMember",
    "ReportableSegmentsMember",
)
SEGMENT_MEMBER_SUFFIXES = (
    "ReportableSegmentsMember",
    "ReportableSegmentMember",
    "OperatingSegmentsMember",
    "OperatingSegmentMember",
    "BusinessUnitReportableSegmentsMember",
    "BusinessUnitReportableSegmentMember",
    "BusinessMember",
    "Member",
)
REGION_MEMBER_LABELS = {
    "JAPAN": "日本",
    "INDIA": "インド",
    "ASIA": "アジア",
    "AFRICA": "アフリカ",
    "EUROPE": "欧州",
    "NORTHAMERICA": "北米",
    "AMERICAS": "米州",
    "AMERICA": "米州",
    "CHINA": "中国",
    "OCEANIA": "オセアニア",
}

REVENUE_TOTAL_TAGS = {
    "NetSales",
    "NetSalesIFRS",
    "RevenueIFRS",
    "Revenue2IFRS",
    "SalesRevenuesIFRS",
    "SalesAndFinancialServicesRevenueIFRS",
    "OperatingRevenueIFRS",
}
REVENUE_EXTERNAL_TAGS = {
    "RevenuesFromExternalCustomers",
    "RevenueFromExternalCustomersIFRS",
    "RevenueFromExternalCustomers2IFRS",
    "SalesToExternalCustomersIFRS",
    "OperatingRevenueFromExternalCustomersIFRS",
    "SalesAndFinancialServicesRevenueToCustomersIFRS",
}
OPERATING_INCOME_TAGS = {
    "OperatingIncome",
    "OperatingIncomeIFRS",
    "OperatingProfitLossIFRS",
}
PROFIT_BEFORE_TAX_TAGS = {
    "ProfitLossBeforeTaxIFRS",
    "ProfitBeforeTaxIFRS",
    "IncomeBeforeIncomeTaxesIFRS",
    "IncomeBeforeIncomeTaxes",
}
PROFIT_LOSS_TAGS = {
    "ProfitLoss",
    "ProfitLossIFRS",
    "ProfitLossAttributableToOwnersOfParent",
    "ProfitLossAttributableToOwnersOfParentIFRS",
}
SEGMENT_PROFIT_TAGS = {
    "OrdinaryIncome",
    "SegmentProfit",
    "SegmentProfitLoss",
    "SegmentProfitLossIFRS",
    "SegmentProfitSegmentInformation",
}

METRIC_INFO_BY_TAG: dict[str, tuple[str, str, str, int]] = {
    **{tag: ("NetSales", "SegmentNetSalesCurrent", "total", 10) for tag in REVENUE_TOTAL_TAGS},
    **{tag: ("NetSales", "SegmentExternalNetSalesCurrent", "external", 30) for tag in REVENUE_EXTERNAL_TAGS},
    **{tag: ("OperatingIncome", "SegmentOperatingIncomeCurrent", "operating_profit", 10) for tag in OPERATING_INCOME_TAGS},
    **{tag: ("ProfitBeforeTax", "SegmentProfitBeforeTaxCurrent", "profit_before_tax", 10) for tag in PROFIT_BEFORE_TAX_TAGS},
    **{tag: ("ProfitLoss", "SegmentProfitLossCurrent", "profit_loss", 10) for tag in PROFIT_LOSS_TAGS},
    **{tag: ("SegmentProfit", "SegmentProfitCurrent", "segment_profit", 10) for tag in SEGMENT_PROFIT_TAGS},
}
TEXTBLOCK_METRIC_INFO_BY_BASE: dict[str, tuple[str, str, str, int]] = {
    "NetSales": ("NetSales", "SegmentNetSalesCurrent", "total", 80),
    "OperatingIncome": ("OperatingIncome", "SegmentOperatingIncomeCurrent", "operating_profit", 80),
    "ProfitBeforeTax": ("ProfitBeforeTax", "SegmentProfitBeforeTaxCurrent", "profit_before_tax", 80),
    "ProfitLoss": ("ProfitLoss", "SegmentProfitLossCurrent", "profit_loss", 80),
    "SegmentProfit": ("SegmentProfit", "SegmentProfitCurrent", "segment_profit", 80),
}
PROFIT_METRIC_INFO_BY_BASE: dict[str, tuple[str, str, str, int]] = {
    "OperatingIncome": ("OperatingIncome", "SegmentOperatingIncomeCurrent", "operating_profit", 10),
    "SegmentProfit": ("SegmentProfit", "SegmentProfitCurrent", "segment_profit", 10),
}

SEGMENT_EXCEL_METRIC_LABELS = {
    "NetSales": "売上高",
    "OperatingIncome": "営業利益",
    "ProfitBeforeTax": "経常利益",
    "ProfitLoss": "純利益",
    "SegmentProfit": "セグメント利益",
}

GEOGRAPHICAL_AREA_TEXTBLOCK_TAGS = {
    "InformationAboutGeographicalAreasIFRSTextBlock",
    "InformationAboutGeographicalAreasTextBlock",
}
TEXTBLOCK_METRIC_BASES = {"NetSales", "OperatingIncome", "SegmentProfit"}


@dataclass(frozen=True)
class SegmentMetricRow:
    doc_id: str
    edinet_code: str
    security_code: str
    form_type: str
    period_scope: str
    quarter_type: str
    fiscal_year: int | None
    period_start: str
    period_end: str
    segment_kind: str
    segment_name: str
    axis_qname: str
    member_qname: str
    metric_base: str
    metric_key: str
    value_kind: str
    value_num: float | None
    value_unit: str
    source_tag: str
    tag_qname: str
    context_ref: str
    decimals: str
    calc_status: str
    source_detail_json: str
    rule_version: str = SEGMENT_RULE_VERSION


@dataclass(frozen=True)
class SegmentCandidate:
    doc_id: str
    security_code: str
    company_name: str
    period_end: str
    segment_kind: str
    segment_name: str
    member_qname: str
    source_tag: str
    metric_base: str
    value_kind: str
    value_num: float | None
    status: str
    reason: str
    context_ref: str


@dataclass(frozen=True)
class TextblockSegmentEntry:
    segment_kind: str
    segment_name: str
    metric_base: str
    metric_key: str
    value_kind: str
    value_num: float
    unit_multiplier: int
    table_kind: str
    source_heading: str
    metric_label: str
    table_index: int


@dataclass(frozen=True)
class SegmentMetricBuildResult:
    rows: list[SegmentMetricRow]
    candidates: list[SegmentCandidate]
    warnings: list[str]


@dataclass(frozen=True)
class SegmentMetricSaveResult:
    rows: list[SegmentMetricRow]
    candidates: list[SegmentCandidate]
    saved_rows: int
    warnings: list[str]
    output_path: Path
    built_row_count: int = 0
    candidate_count: int = 0
    replaced_doc_count: int = 0


@dataclass(frozen=True)
class SegmentFiscalYearAnchor:
    fiscal_year: int | None
    fiscal_year_end: str
    source: str
    status: str


def segment_metrics_table_exists(conn: sqlite3.Connection) -> bool:
    row = conn.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type = 'table' AND name = 'segment_metrics'
        """
    ).fetchone()
    return row is not None


def _now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _normalize_security_code(value: Any) -> str:
    text = str(value or "").strip().replace("-", "")
    if len(text) == 5 and text.endswith("0"):
        return text[:-1]
    return text


def _normalize_form_codes(form_codes: list[str] | None) -> list[str]:
    requested = [str(item or "").strip() for item in (form_codes or []) if str(item or "").strip()]
    if not requested:
        requested = ["030000", "043A00"]
    out: list[str] = []
    for code in requested:
        normalized = "043A00" if code.lower() == "043a00" else code
        if normalized == "043000":
            candidates = ["043000", "043A00"]
        elif normalized == "043A00":
            candidates = ["043A00", "043000"]
        else:
            candidates = [normalized]
        for candidate in candidates:
            if candidate not in out:
                out.append(candidate)
    return out


def _parse_year(value: Any) -> int | None:
    text = str(value or "").strip()
    if len(text) < 4:
        return None
    try:
        return int(text[:4])
    except ValueError:
        return None


def _valid_date_text(value: Any) -> str:
    text = str(value or "").strip()[:10]
    try:
        return datetime.strptime(text, "%Y-%m-%d").strftime("%Y-%m-%d")
    except ValueError:
        return ""


def _days_between(start: str, end: str) -> int | None:
    try:
        return (datetime.strptime(end, "%Y-%m-%d") - datetime.strptime(start, "%Y-%m-%d")).days
    except ValueError:
        return None


def _shift_year(date_text: str) -> str:
    text = _valid_date_text(date_text)
    if not text:
        return ""
    year = int(text[:4]) + 1
    suffix = text[4:]
    if suffix == "-02-29":
        suffix = "-02-28"
    return f"{year}{suffix}"


def _fetch_current_fiscal_year_ends(
    conn: sqlite3.Connection,
    doc_ids: list[str],
) -> dict[str, str]:
    values_by_doc: dict[str, set[str]] = {}
    for chunk in _chunked(doc_ids):
        placeholders = ",".join("?" for _ in chunk)
        rows = conn.execute(
            f"""
            SELECT doc_id, value_text
            FROM raw_facts
            WHERE doc_id IN ({placeholders})
              AND tag_name = ?
            """,
            [*chunk, CURRENT_FISCAL_YEAR_END_DEI_TAG],
        ).fetchall()
        for row in rows:
            value = _valid_date_text(row["value_text"])
            if value:
                values_by_doc.setdefault(str(row["doc_id"]), set()).add(value)
    return {
        doc_id: next(iter(values))
        for doc_id, values in values_by_doc.items()
        if len(values) == 1
    }


def _annual_period_ends_by_edinet_code(conn: sqlite3.Connection) -> dict[str, list[str]]:
    rows = conn.execute(
        """
        SELECT edinet_code, period_end
        FROM filings
        WHERE form_type = '030000'
          AND period_end IS NOT NULL
        ORDER BY edinet_code, period_end
        """
    ).fetchall()
    result: dict[str, list[str]] = {}
    for row in rows:
        edinet_code = str(row["edinet_code"] or "")
        period_end = _valid_date_text(row["period_end"])
        if edinet_code and period_end:
            result.setdefault(edinet_code, []).append(period_end)
    return result


def resolve_segment_fiscal_year_anchor(
    *,
    filing: sqlite3.Row,
    fact_period_end: str,
    current_fiscal_year_ends: dict[str, str],
    annual_period_ends_by_edinet_code: dict[str, list[str]],
) -> SegmentFiscalYearAnchor:
    period_end = _valid_date_text(fact_period_end)
    form_type = str(filing["form_type"] or "")
    if form_type not in {"043000", "043A00"}:
        return SegmentFiscalYearAnchor(
            fiscal_year=_parse_year(period_end),
            fiscal_year_end=period_end,
            source="period_end",
            status="ok" if period_end else "unresolved",
        )

    direct = current_fiscal_year_ends.get(str(filing["doc_id"] or ""), "")
    direct_gap = _days_between(period_end, direct)
    if direct and direct_gap is not None and 0 < direct_gap <= 260:
        return SegmentFiscalYearAnchor(
            fiscal_year=_parse_year(direct),
            fiscal_year_end=direct,
            source="current_fiscal_year_end_dei",
            status="ok",
        )

    annual_periods = annual_period_ends_by_edinet_code.get(str(filing["edinet_code"] or ""), [])
    future = [item for item in annual_periods if item > period_end]
    if future:
        candidate = min(future)
        gap = _days_between(period_end, candidate)
        if gap is not None and 0 < gap <= 260:
            return SegmentFiscalYearAnchor(
                fiscal_year=_parse_year(candidate),
                fiscal_year_end=candidate,
                source="next_annual_period_end",
                status="ok",
            )

    past = [item for item in annual_periods if item <= period_end]
    if past:
        candidate = _shift_year(max(past))
        gap = _days_between(period_end, candidate)
        if candidate and gap is not None and 0 < gap <= 260:
            return SegmentFiscalYearAnchor(
                fiscal_year=_parse_year(candidate),
                fiscal_year_end=candidate,
                source="prior_annual_period_end_shifted",
                status="ok",
            )

    return SegmentFiscalYearAnchor(
        fiscal_year=None,
        fiscal_year_end="",
        source="",
        status="unresolved",
    )


def _to_float(value: Any) -> float | None:
    text = str(value or "").strip().replace(",", "")
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _safe_json(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    text = str(value or "").strip()
    if not text:
        return {}
    try:
        parsed = json.loads(text)
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _local_name(qname: str) -> str:
    text = str(qname or "").strip()
    if ":" in text:
        return text.rsplit(":", 1)[-1]
    if "}" in text:
        return text.rsplit("}", 1)[-1]
    return text


def _concept_lookup_keys(qname: str) -> list[str]:
    text = str(qname or "").strip()
    local = _local_name(text)
    keys = [text, local]
    if "_" in local:
        keys.append(local.rsplit("_", 1)[-1])
    result: list[str] = []
    for key in keys:
        if key and key not in result:
            result.append(key)
    return result


def _linkbase_label_for_qname(qname: str, labels_by_concept: dict[str, str]) -> str:
    for key in _concept_lookup_keys(qname):
        label = str(labels_by_concept.get(key) or "").strip()
        if label:
            return label
    return ""


def _linkbase_presentation_sequence(qname: str, structure_by_concept: dict[str, dict[str, Any]]) -> int | None:
    for key in _concept_lookup_keys(qname):
        item = structure_by_concept.get(key)
        if not isinstance(item, dict):
            continue
        sequence = item.get("presentation_sequence")
        try:
            return int(sequence) if sequence is not None else None
        except (TypeError, ValueError):
            continue
    return None


def _segment_order_from_detail(source_detail_json: str) -> int:
    detail = _safe_json(source_detail_json)
    try:
        value = detail.get("segment_order")
        return int(value) if value is not None else 999999
    except (TypeError, ValueError):
        return 999999


def _contains_any(text: str, markers: tuple[str, ...]) -> bool:
    return any(marker.lower() in text.lower() for marker in markers)


def _segment_member_from_context_ref(context_ref: Any) -> str | None:
    text = str(context_ref or "")
    if not text:
        return None
    markers = (
        "ReportableSegmentsMember",
        "ReportableSegmentMember",
        "OperatingSegmentMember",
        "OperatingSegmentsNotIncludedInReportableSegmentsAndOtherRevenueGeneratingBusinessActivitiesMember",
        "TotalOfReportableSegmentsAndOthersMember",
        "TotalOfReportableSegmentsMember",
        "ReconcilingItemsMember",
    )
    if not any(marker in text for marker in markers):
        return None
    for part in text.split("_")[1:]:
        if any(marker in part for marker in markers):
            return part
    return None


def _segment_axis_member(dimensions_json: Any, context_ref: Any = None) -> tuple[str, str] | None:
    dimensions = _safe_json(dimensions_json)
    axis_members = dimensions.get("axis_members") or {}
    if isinstance(axis_members, dict):
        for axis, members in axis_members.items():
            for member in members if isinstance(members, list) else [members]:
                axis_text = str(axis or "")
                member_text = str(member or "")
                if _contains_any(axis_text, SEGMENT_AXIS_MARKERS) or _contains_any(member_text, SEGMENT_AXIS_MARKERS):
                    return axis_text, member_text

    explicit_members = dimensions.get("explicit_members") or []
    if isinstance(explicit_members, list):
        for item in explicit_members:
            if not isinstance(item, dict):
                continue
            axis_text = str(item.get("dimension") or "")
            member_text = str(item.get("member") or "")
            if _contains_any(axis_text, SEGMENT_AXIS_MARKERS) or _contains_any(member_text, SEGMENT_AXIS_MARKERS):
                return axis_text, member_text
    fallback_member = _segment_member_from_context_ref(context_ref)
    if fallback_member:
        return "context_ref:OperatingSegmentsAxis", fallback_member
    return None


def _member_priority(member_qname: str) -> int:
    local = _local_name(member_qname)
    if "TotalOfReportableSegmentsAndOthersMember" in local:
        return 0
    if "TotalOfReportableSegmentsMember" in local:
        return 1
    if local == "ReportableSegmentsMember":
        return 2
    return 10


def _is_total_member(member_qname: str) -> bool:
    local = _local_name(member_qname)
    return (
        local == "ReportableSegmentsMember"
        or local == "TotalOfReportableSegmentsMember"
        or local == "TotalOfReportableSegmentsAndOthersMember"
    )


def _member_core(member_qname: str) -> str:
    local = _local_name(member_qname)
    local = re.sub(r"^[A-Z]\d{5}-\d{3}", "", local)
    for suffix in SEGMENT_MEMBER_SUFFIXES:
        if local.endswith(suffix):
            return local[: -len(suffix)]
    return local


def _is_region_member(member_qname: str) -> bool:
    core_upper = _member_core(member_qname).upper()
    return any(core_upper == marker or core_upper.startswith(marker) for marker in REGION_MEMBER_LABELS)


def _is_known_business_member_with_region_name(member_qname: str) -> bool:
    core_upper = _member_core(member_qname).upper()
    return "UNIQLO" in core_upper and "JAPAN" in core_upper


def _segment_kind(member_qname: str, axis_qname: str) -> str | None:
    text = f"{member_qname} {axis_qname}"
    if _is_total_member(member_qname):
        return "total"
    if _contains_any(text, EXCLUDED_MEMBER_MARKERS):
        return None
    if _is_known_business_member_with_region_name(member_qname):
        return "business"
    if _contains_any(axis_qname, ("Geographical", "Geographic", "Region", "Area")):
        return "region"
    if _is_region_member(member_qname):
        return "region"
    return "business"


def _humanize_member_name(member_qname: str, labels_by_concept: dict[str, str]) -> str:
    local = _local_name(member_qname)
    if _is_total_member(member_qname):
        return "合計"
    if (
        "OperatingSegmentsNotIncludedInReportableSegmentsAndOtherRevenueGeneratingBusinessActivities" in local
        or local == "OtherReportableSegmentsMember"
    ):
        return "その他"

    label = _linkbase_label_for_qname(member_qname, labels_by_concept)
    if label:
        return label

    upper = local.upper()
    for marker, label_text in REGION_MEMBER_LABELS.items():
        if marker in upper:
            return label_text

    cleaned = local
    for suffix in (
        "ReportableSegmentsMember",
        "ReportableSegmentMember",
        "OperatingSegmentMember",
        "BusinessMember",
        "Member",
    ):
        cleaned = cleaned.replace(suffix, "")
    cleaned = re.sub(r"([a-z])([A-Z])", r"\1 \2", cleaned).strip()
    return cleaned or local


def _normalize_cell_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "").replace("\u3000", " ")).strip()


def _textblock_number(value: str) -> float | None:
    text = _normalize_cell_text(value)
    if not text or text in {"-", "－", "―"}:
        return None
    negative = "△" in text or text.startswith("(") and text.endswith(")")
    cleaned = (
        text.replace(",", "")
        .replace("△", "")
        .replace("▲", "")
        .replace("－", "")
        .replace("百万円", "")
        .replace("千円", "")
        .replace("億円", "")
        .replace("円", "")
        .replace("(", "")
        .replace(")", "")
        .strip()
    )
    if not cleaned:
        return None
    try:
        number = float(cleaned)
    except ValueError:
        return None
    return -number if negative else number


def _textblock_unit_multiplier(text: str) -> int:
    normalized = _normalize_cell_text(text)
    if "億円" in normalized:
        return 100_000_000
    if "百万円" in normalized:
        return 1_000_000
    if "千円" in normalized:
        return 1_000
    return 1


def _textblock_metric_info(label: str, section_metric: str | None = None) -> tuple[str, str, str, int] | None:
    text = _normalize_cell_text(label).lower()
    if not text:
        return None
    if any(marker in text for marker in ("非流動資産", "資産合計", "資産", "営業費用")):
        return None
    if "調整後営業利益" in text or "adjusted operating" in text:
        return ("SegmentProfit", "SegmentProfitCurrent", "segment_profit", 20)
    if "営業利益" in text or "operating profit" in text or "operating income" in text:
        return ("OperatingIncome", "SegmentOperatingIncomeCurrent", "operating_profit", 10)
    if "税引前" in text or "税金等調整前" in text or "before tax" in text:
        return ("ProfitBeforeTax", "SegmentProfitBeforeTaxCurrent", "profit_before_tax", 10)
    if ("純利益" in text or "親会社" in text or "profit attributable" in text) and "売上" not in text:
        return ("ProfitLoss", "SegmentProfitLossCurrent", "profit_loss", 10)
    if "セグメント利益" in text or "segment profit" in text:
        return ("SegmentProfit", "SegmentProfitCurrent", "segment_profit", 10)
    if "外部顧客" in text or "external" in text:
        return ("NetSales", "SegmentExternalNetSalesCurrent", "external", 30)
    if any(marker in text for marker in ("売上", "収益", "営業収益", "収入", "revenue", "sales")):
        return ("NetSales", "SegmentNetSalesCurrent", "total", 10)
    if text == "計" and section_metric == "NetSales":
        return ("NetSales", "SegmentNetSalesCurrent", "total", 10)
    return None


def _textblock_section_metric(label: str) -> str | None:
    info = _textblock_metric_info(label)
    return info[0] if info else None


def _textblock_segment_kind(name: str) -> str | None:
    text = _normalize_cell_text(name)
    if not text:
        return None
    if any(marker in text for marker in ("消去", "全社", "調整")):
        return None
    if text in {"合計", "連結", "連結合計", "総計"}:
        return "total"
    if _looks_like_region_label(text):
        return "region"
    return None


def _looks_like_region_label(text: str) -> bool:
    normalized = _normalize_cell_text(text)
    if not normalized:
        return False
    if any(marker in normalized for marker in ("年度", "期間", "会計", "項目", "金額", "百万円", "千円", "億円")):
        return False
    return any(
        marker in normalized
        for marker in (
            "日本",
            "国内",
            "海外",
            "北米",
            "米州",
            "米国",
            "アメリカ",
            "欧州",
            "ヨーロッパ",
            "アジア",
            "中国",
            "韓国",
            "インド",
            "シンガポール",
            "英国",
            "イギリス",
            "オーストラリア",
            "台湾",
            "ASEAN",
            "アフリカ",
            "オセアニア",
            "その他",
            "Japan",
            "Domestic",
            "Overseas",
            "North America",
            "America",
            "Europe",
            "Asia",
            "China",
            "Korea",
            "India",
            "Singapore",
            "United Kingdom",
            "UK",
            "Australia",
            "Taiwan",
            "ASEAN",
            "Africa",
            "Oceania",
        )
    )


def _textblock_tables(value_text: str) -> list[list[list[str]]]:
    text = str(value_text or "").strip()
    if not text:
        return []
    try:
        root = lxml_html.fromstring(f"<div>{text}</div>")
    except Exception:
        return []
    tables: list[list[list[str]]] = []
    for table in root.xpath(".//table"):
        rows = _html_table_rows(table)
        if rows:
            tables.append(rows)
    return tables


def _html_table_rows(table: Any) -> list[list[str]]:
    """Expand colspan/rowspan so header columns stay aligned with value columns."""
    rows: list[list[str]] = []
    active_rowspans: dict[int, tuple[int, str]] = {}
    for tr in table.xpath(".//tr"):
        row: list[str] = []
        column = 0

        def consume_active() -> None:
            nonlocal column
            while column in active_rowspans:
                remaining, value = active_rowspans[column]
                row.append(value)
                if remaining <= 1:
                    del active_rowspans[column]
                else:
                    active_rowspans[column] = (remaining - 1, value)
                column += 1

        for cell in tr.xpath("./th|./td"):
            consume_active()
            value = _normalize_cell_text(cell.text_content())
            try:
                colspan = max(int(cell.get("colspan") or 1), 1)
            except (TypeError, ValueError):
                colspan = 1
            try:
                rowspan = max(int(cell.get("rowspan") or 1), 1)
            except (TypeError, ValueError):
                rowspan = 1
            for offset in range(colspan):
                row.append(value)
                if rowspan > 1:
                    active_rowspans[column + offset] = (rowspan - 1, value)
            column += colspan
        consume_active()
        if any(row):
            rows.append(row)
    return rows


def _is_matrix_geography_table(rows: list[list[str]]) -> bool:
    if not rows:
        return False
    header = rows[0]
    if len(header) < 3:
        return False
    region_count = sum(1 for cell in header[1:] if _textblock_segment_kind(cell) in {"region", "total"})
    metric_count = sum(1 for row in rows[1:] if row and _textblock_metric_info(row[0]) is not None)
    return region_count >= 2 and metric_count >= 1


def _matrix_textblock_entries(rows: list[list[str]]) -> list[tuple[str, str, str, float]]:
    entries: list[tuple[str, str, str, float]] = []
    if not rows:
        return entries
    headers = rows[0]
    section_metric: str | None = None
    for row in rows[1:]:
        if not row:
            continue
        label = row[0]
        values = row[1:]
        if not any(_textblock_number(cell) is not None for cell in values):
            section_metric = _textblock_section_metric(label) or section_metric
            continue
        info = _textblock_metric_info(label, section_metric)
        if info is None:
            continue
        metric_base, metric_key, value_kind, _priority = info
        for index, cell in enumerate(values, start=1):
            if index >= len(headers):
                continue
            segment_name = headers[index]
            kind = _textblock_segment_kind(segment_name)
            if kind is None:
                continue
            value = _textblock_number(cell)
            if value is None:
                continue
            entries.append((kind, segment_name if kind == "region" else "合計", metric_base, value))
    return entries


def _row_textblock_entries(rows: list[list[str]]) -> list[tuple[str, str, str, float]]:
    entries: list[tuple[str, str, str, float]] = []
    table_text = " ".join(" ".join(row) for row in rows)
    if any(marker in table_text for marker in ("非流動資産", "有形固定資産", "年度末", "期末", "資産")) and not any(
        marker in table_text for marker in ("売上", "収益", "営業収益", "収入")
    ):
        return entries

    section_metric: str | None = None
    value_column: int | None = None
    for row in rows:
        for index in range(len(row) - 1, 0, -1):
            if _textblock_number(row[index]) is not None:
                value_column = max(value_column or 0, index)
                break
    if value_column is None:
        value_column = max((len(row) - 1 for row in rows), default=1)

    for table_index, row in enumerate(rows):
        if not row:
            continue
        label = row[0]
        if _textblock_metric_info(label) is not None and all(_textblock_number(cell) is None for cell in row[1:]):
            section_metric = _textblock_section_metric(label)
            continue
        kind = _textblock_segment_kind(label)
        if kind is None:
            continue
        if value_column >= len(row):
            continue
        value = _textblock_number(row[value_column])
        if value is None:
            continue
        metric_info = ("NetSales", "SegmentNetSalesCurrent", "total", 10)
        if section_metric:
            metric_info = _textblock_metric_info(section_metric) or metric_info
        elif table_index > 0 and any(marker in table_text for marker in ("営業利益", "税引前", "純利益")):
            metric_info = _textblock_metric_info(table_text) or metric_info
        metric_base = metric_info[0]
        if metric_base != "NetSales":
            # Row-oriented geographical tables are usually sales or assets.  Avoid over-reading profit labels
            # from surrounding explanatory text unless the row itself clearly says the metric.
            metric_base = "NetSales"
        entries.append((kind, label if kind == "region" else "合計", metric_base, value))
    return entries


def _geographical_textblock_entries(value_text: str) -> list[tuple[str, str, str, float, int]]:
    tables = _textblock_tables(value_text)
    entries: list[tuple[str, str, str, float, int]] = []
    block_multiplier = _textblock_unit_multiplier(value_text)
    matrix_tables = [rows for rows in tables if _is_matrix_geography_table(rows)]
    if matrix_tables:
        rows = matrix_tables[-1]
        multiplier = _textblock_unit_multiplier(" ".join(" ".join(row) for row in rows))
        if multiplier == 1:
            multiplier = block_multiplier
        for kind, segment_name, metric_base, value in _matrix_textblock_entries(rows):
            entries.append((kind, segment_name, metric_base, value * multiplier, multiplier))
        return entries

    for rows in tables:
        table_text = " ".join(" ".join(row) for row in rows)
        if any(marker in table_text for marker in ("非流動資産", "有形固定資産", "年度末", "期末", "資産")) and not any(
            marker in table_text for marker in ("売上", "収益", "営業収益", "収入", "revenue", "sales")
        ):
            continue
        if not any(_textblock_segment_kind(row[0] if row else "") in {"region", "total"} for row in rows):
            continue
        multiplier = _textblock_unit_multiplier(table_text)
        if multiplier == 1:
            multiplier = block_multiplier
        for kind, segment_name, metric_base, value in _row_textblock_entries(rows):
            entries.append((kind, segment_name, metric_base, value * multiplier, multiplier))
    return entries


def _is_segment_note_textblock_tag(tag_name: str) -> bool:
    normalized = str(tag_name or "").lower()
    return str(tag_name or "") in GEOGRAPHICAL_AREA_TEXTBLOCK_TAGS or (
        "segmentinformation" in normalized and "textblock" in normalized
    )


def _segment_note_textblock_source_priority(tag_name: str) -> int:
    tag = str(tag_name or "")
    if tag.startswith("NotesSegmentInformation"):
        return 0
    if tag in GEOGRAPHICAL_AREA_TEXTBLOCK_TAGS:
        return 10
    if tag.startswith("Footnotes"):
        return 20
    return 30


def _textblock_table_contexts(value_text: str) -> list[tuple[list[list[str]], str]]:
    text = str(value_text or "").strip()
    if not text:
        return []
    try:
        root = lxml_html.fromstring(f"<div>{text}</div>")
    except Exception:
        return []

    table_starts = [match.start() for match in re.finditer(r"<table(?:\s|>)", text, flags=re.IGNORECASE)]
    contexts: list[tuple[list[list[str]], str]] = []
    for table_index, table in enumerate(root.xpath(".//table")):
        rows = _html_table_rows(table)
        if not rows:
            continue
        heading_parts = [
            _normalize_cell_text(text_part)
            for text_part in table.xpath("./caption//text()")
            if _normalize_cell_text(text_part)
        ]
        preceding = table.xpath(
            "./preceding::p[position() <= 3] | ./preceding::h1[position() <= 2] | "
            "./preceding::h2[position() <= 2] | ./preceding::h3[position() <= 2]"
        )
        heading_parts.extend(
            _normalize_cell_text(node.text_content())
            for node in preceding[-5:]
            if _normalize_cell_text(node.text_content())
        )
        if table_index < len(table_starts):
            prefix = re.sub(r"<[^>]+>", " ", text[max(0, table_starts[table_index] - 2400) : table_starts[table_index]])
            period_matches = list(
                re.finditer(
                    r"(?:当年度|前年度|当期|前期|current\s+year|prior\s+year)[^\r\n]{0,160}",
                    prefix,
                    flags=re.IGNORECASE,
                )
            )
            if period_matches:
                heading_parts.append(_normalize_cell_text(period_matches[-1].group(0)))
        contexts.append((rows, " ".join(heading_parts)))
    return contexts


def _textblock_table_kind(
    rows: list[list[str]],
    *,
    source_tag: str,
    source_heading: str,
) -> tuple[str | None, str]:
    table_text = " ".join(" ".join(row) for row in rows)
    semantic_text = f"{source_tag} {source_heading} {table_text}"
    metric_labels = [row[0] for row in rows if row]
    has_metric = any(
        (info := _textblock_metric_info(label)) is not None and info[0] in TEXTBLOCK_METRIC_BASES
        for label in metric_labels
    )
    has_asset = any(marker in semantic_text for marker in ("非流動資産", "有形固定資産", "資産合計", "資産"))
    if has_asset and not has_metric:
        return None, "excluded_asset_table"
    if not has_metric:
        return None, "excluded_no_supported_metric"

    normalized = semantic_text.lower()
    if any(marker in normalized for marker in ("報告セグメント", "事業セグメント", "セグメント情報", "reportable segment")):
        return "business", "selected_reportable_segment_table"
    if any(marker in normalized for marker in ("クラスター", "地域別", "地理", "geographical", "geographic", "cluster")):
        return "region", "selected_region_or_cluster_table"
    if str(source_tag or "") in GEOGRAPHICAL_AREA_TEXTBLOCK_TAGS:
        return "region", "selected_geographical_area_textblock"

    header_cells = [cell for row in rows[:4] for cell in row[1:]]
    region_headers = sum(
        _textblock_segment_kind(cell) in {"region", "total"}
        for cell in header_cells
    )
    if region_headers >= 2:
        return "region", "selected_region_headers"
    return None, "excluded_unclassified_table"


def _is_textblock_unit_label(value: str) -> bool:
    normalized = _normalize_cell_text(value).lower()
    return any(
        marker in normalized
        for marker in ("百万円", "千円", "億円", "円", "million", "thousand", "yen")
    )


def _textblock_headers(rows: list[list[str]], table_kind: str) -> list[str]:
    first_metric_row_index = next(
        (
            index
            for index, row in enumerate(rows)
            if row and _textblock_metric_info(row[0]) is not None
        ),
        len(rows),
    )
    header_rows = rows[:first_metric_row_index]
    width = max((len(row) for row in rows), default=0)
    if width < 2:
        return rows[0] if rows else []

    headers = ["" for _ in range(width)]
    for column in range(1, width):
        labels = [
            _normalize_cell_text(row[column])
            for row in header_rows
            if column < len(row)
            and _normalize_cell_text(row[column])
            and not _is_textblock_unit_label(_normalize_cell_text(row[column]))
            and _textblock_number(_normalize_cell_text(row[column])) is None
        ]
        if not labels:
            continue
        if table_kind == "region":
            region_labels = [
                label for label in labels if _textblock_segment_kind(label) in {"region", "total"}
            ]
            headers[column] = region_labels[-1] if region_labels else labels[-1]
        else:
            headers[column] = labels[-1]
    return headers


def _textblock_column_kind(name: str, table_kind: str) -> str | None:
    normalized = _normalize_cell_text(name)
    if normalized in {"計", "合計", "連結", "連結合計"}:
        return "total"
    if (
        not normalized
        or _is_textblock_unit_label(normalized)
        or _textblock_number(normalized) is not None
        or any(marker in normalized for marker in ("消去", "全社", "調整", "連結財務"))
    ):
        return None
    if table_kind == "region":
        known_kind = _textblock_segment_kind(normalized)
        if known_kind is not None:
            return known_kind
        if _textblock_number(normalized) is not None or any(
            marker in normalized for marker in ("年度", "当期", "前期", "単位", "year", "current", "prior")
        ):
            return None
        return "region"
    return "business"


def _textblock_current_value_column(rows: list[list[str]], period_end: str) -> int | None:
    current_year = str(period_end or "")[:4]
    current_columns: list[int] = []
    for row in rows[:5]:
        for index, cell in enumerate(row[1:], start=1):
            text = _normalize_cell_text(cell).lower()
            if text and ("当期" in text or "current" in text or (current_year and current_year in text)):
                current_columns.append(index)
    if current_columns:
        return max(current_columns)
    numeric_columns = [
        index
        for row in rows
        for index, cell in enumerate(row[1:], start=1)
        if _textblock_number(cell) is not None
    ]
    return max(numeric_columns) if numeric_columns else None


def _textblock_matrix_entries(
    rows: list[list[str]],
    *,
    table_kind: str,
) -> list[tuple[str, str, tuple[str, str, str, int], float, str]]:
    headers = _textblock_headers(rows, table_kind)
    if len(headers) < 2:
        return []
    entries: list[tuple[str, str, tuple[str, str, str, int], float, str]] = []
    section_metric: str | None = None
    for row in rows:
        if not row:
            continue
        label = row[0]
        info = _textblock_metric_info(label, section_metric)
        values = row[1:]
        if info is not None and not any(_textblock_number(cell) is not None for cell in values):
            section_metric = info[0]
            continue
        if info is None or info[0] not in TEXTBLOCK_METRIC_BASES:
            continue
        for index, cell in enumerate(values, start=1):
            if index >= len(headers):
                continue
            kind = _textblock_column_kind(headers[index], table_kind)
            value = _textblock_number(cell)
            if kind is None or value is None:
                continue
            entries.append((kind, headers[index] if kind != "total" else "合計", info, value, label))
    return entries


def _textblock_row_entries(
    rows: list[list[str]],
    *,
    table_kind: str,
    period_end: str,
) -> list[tuple[str, str, tuple[str, str, str, int], float, str]]:
    value_column = _textblock_current_value_column(rows, period_end)
    if value_column is None:
        return []
    section_metric: tuple[str, str, str, int] | None = None
    entries: list[tuple[str, str, tuple[str, str, str, int], float, str]] = []
    for row in rows:
        if not row:
            continue
        label = row[0]
        info = _textblock_metric_info(label)
        if info is not None and not any(_textblock_number(cell) is not None for cell in row[1:]):
            section_metric = info
            continue
        if value_column >= len(row):
            continue
        kind = _textblock_column_kind(label, table_kind)
        value = _textblock_number(row[value_column])
        if kind is None or value is None or section_metric is None or section_metric[0] not in TEXTBLOCK_METRIC_BASES:
            continue
        entries.append((kind, label if kind != "total" else "合計", section_metric, value, label))
    return entries


def _textblock_table_period_status(source_heading: str, period_end: str) -> str:
    text = _normalize_cell_text(source_heading).lower()
    current_year = str(period_end or "")[:4]
    current_markers = ("当年度", "当期", "current year", "current period")
    prior_markers = ("前年度", "前期", "prior year", "previous year", "prior period")
    if any(marker in text for marker in current_markers):
        return "current"
    if any(marker in text for marker in prior_markers):
        return "prior"
    if current_year and current_year in text:
        return "current"
    return "unknown"


def _segment_note_textblock_entries(
    value_text: str,
    *,
    source_tag: str,
    period_end: str,
) -> tuple[list[TextblockSegmentEntry], list[str]]:
    entries: list[TextblockSegmentEntry] = []
    rejections: list[str] = []
    block_multiplier = _textblock_unit_multiplier(value_text)
    table_infos: list[tuple[int, list[list[str]], str, str, str, str]] = []
    for table_index, (rows, heading) in enumerate(_textblock_table_contexts(value_text)):
        table_kind, decision = _textblock_table_kind(rows, source_tag=source_tag, source_heading=heading)
        if table_kind is None:
            rejections.append(f"table_{table_index}:{decision}")
            continue
        table_infos.append(
            (
                table_index,
                rows,
                heading,
                table_kind,
                decision,
                _textblock_table_period_status(heading, period_end),
            )
        )
    # Cluster detail tables commonly follow a period-labelled reportable-segment
    # table without repeating the current/prior heading.  Carry that explicit
    # period marker forward only within this TextBlock.
    period_status = "unknown"
    resolved_table_infos: list[tuple[int, list[list[str]], str, str, str, str]] = []
    for table_info in table_infos:
        table_index, rows, heading, table_kind, decision, table_period_status = table_info
        if table_period_status != "unknown":
            period_status = table_period_status
        elif period_status != "unknown":
            table_period_status = period_status
        resolved_table_infos.append(
            (table_index, rows, heading, table_kind, decision, table_period_status)
        )
    table_infos = resolved_table_infos
    kinds_with_current_table = {
        table_kind
        for _, _, _, table_kind, _, period_status in table_infos
        if period_status == "current"
    }
    for table_index, rows, heading, table_kind, decision, period_status in table_infos:
        if table_kind in kinds_with_current_table and period_status != "current":
            rejections.append(f"table_{table_index}:excluded_non_current_period_table")
            continue
        table_text = " ".join(" ".join(row) for row in rows)
        multiplier = _textblock_unit_multiplier(table_text)
        if multiplier == 1:
            multiplier = block_multiplier
        raw_entries = _textblock_matrix_entries(rows, table_kind=table_kind)
        if not raw_entries:
            raw_entries = _textblock_row_entries(rows, table_kind=table_kind, period_end=period_end)
        if not raw_entries:
            rejections.append(f"table_{table_index}:excluded_no_current_values")
            continue
        for kind, segment_name, info, value, metric_label in raw_entries:
            metric_base, metric_key, value_kind, _priority = info
            entries.append(
                TextblockSegmentEntry(
                    segment_kind=kind,
                    segment_name=segment_name,
                    metric_base=metric_base,
                    metric_key=metric_key,
                    value_kind=value_kind,
                    value_num=value * multiplier,
                    unit_multiplier=multiplier,
                    table_kind=table_kind,
                    source_heading=heading,
                    metric_label=metric_label,
                    table_index=table_index,
                )
            )
    return entries, rejections


def _value_unit(row: sqlite3.Row) -> str:
    unit_ref = str(row["unit_ref"] or "").strip()
    if unit_ref.upper() == "JPY":
        return "yen"
    return unit_ref or "unknown"


def _metric_info(tag_name: str) -> tuple[str, str, str, int] | None:
    return METRIC_INFO_BY_TAG.get(str(tag_name or ""))


def _profit_metric_classification(
    *,
    tag_name: str,
    line_item_label: str,
) -> tuple[tuple[str, str, str, int] | None, str, str, str]:
    label = _normalize_cell_text(line_item_label).lower()
    if label:
        if (
            "セグメント利益" in label
            or "セグメント損失" in label
            or "segment profit" in label
            or "segment loss" in label
        ):
            return PROFIT_METRIC_INFO_BY_BASE["SegmentProfit"], "segment_profit", "linkbase_label", "confirmed"
        if "営業利益" in label or "operating profit" in label or "operating income" in label:
            return PROFIT_METRIC_INFO_BY_BASE["OperatingIncome"], "operating_income", "linkbase_label", "confirmed"
        return None, "", "linkbase_label", "review"

    if tag_name in OPERATING_INCOME_TAGS:
        return PROFIT_METRIC_INFO_BY_BASE["OperatingIncome"], "operating_income", "standard_tag", "confirmed"
    if tag_name in SEGMENT_PROFIT_TAGS - {"OrdinaryIncome"}:
        return PROFIT_METRIC_INFO_BY_BASE["SegmentProfit"], "segment_profit", "standard_tag", "confirmed"
    return None, "", "", "review"


def _profit_group_key(row: SegmentMetricRow) -> tuple[str, str, str, str, str]:
    member_qname = "TOTAL" if row.segment_kind == "total" else row.member_qname
    return row.doc_id, row.segment_kind, member_qname, row.period_start, row.period_end


def _apply_profit_metric_conflicts(rows: list[SegmentMetricRow]) -> list[SegmentMetricRow]:
    bases_by_group: dict[tuple[str, str, str, str, str], set[str]] = {}
    for row in rows:
        if row.calc_status != "ok" or row.metric_base not in PROFIT_METRIC_INFO_BY_BASE:
            continue
        bases_by_group.setdefault(_profit_group_key(row), set()).add(row.metric_base)

    conflict_keys = {key for key, bases in bases_by_group.items() if len(bases) > 1}
    if not conflict_keys:
        return rows

    out: list[SegmentMetricRow] = []
    for row in rows:
        if _profit_group_key(row) not in conflict_keys or row.metric_base not in PROFIT_METRIC_INFO_BY_BASE:
            out.append(row)
            continue
        detail = _safe_json(row.source_detail_json)
        detail["profit_metric_display_preference"] = "OperatingIncome"
        detail["profit_metric_classification_reason"] = "operating_income_preferred_for_excel"
        out.append(
            replace(
                row,
                source_detail_json=json.dumps(detail, ensure_ascii=False, sort_keys=True),
            )
        )
    return out


def _semantic_segment_name_key(name: str) -> str:
    return re.sub(r"\W+", "", _normalize_cell_text(name), flags=re.UNICODE).lower()


def _is_segment_note_textblock_row(row: SegmentMetricRow) -> bool:
    return str(_safe_json(row.source_detail_json).get("source") or "") == "segment_note_textblock"


def _textblock_semantic_key(row: SegmentMetricRow) -> tuple[str, str, str, str, str, str]:
    return (
        row.doc_id,
        row.segment_kind,
        _semantic_segment_name_key(row.segment_name),
        row.metric_base,
        row.period_start,
        row.period_end,
    )


def _textblock_metric_scope_key(row: SegmentMetricRow) -> tuple[str, str, str, str, str]:
    return (
        row.doc_id,
        row.segment_kind,
        row.metric_base,
        row.period_start,
        row.period_end,
    )


def _apply_textblock_source_conflicts(rows: list[SegmentMetricRow]) -> list[SegmentMetricRow]:
    direct_values: dict[tuple[str, str, str, str, str, str], set[float | None]] = {}
    direct_metric_scopes: set[tuple[str, str, str, str, str]] = set()
    for row in rows:
        if _is_segment_note_textblock_row(row) or row.calc_status != "ok":
            continue
        direct_values.setdefault(_textblock_semantic_key(row), set()).add(row.value_num)
        direct_metric_scopes.add(_textblock_metric_scope_key(row))

    out: list[SegmentMetricRow] = []
    for row in rows:
        if not _is_segment_note_textblock_row(row):
            out.append(row)
            continue
        detail = _safe_json(row.source_detail_json)
        if (
            detail.get("table_kind") == "business"
            and _textblock_metric_scope_key(row) in direct_metric_scopes
        ):
            # Reportable-segment tables are supplemental only.  A direct XBRL
            # fact for the same period and metric is authoritative even when its
            # member label is not textually identical to the table header.
            continue
        competing_values = direct_values.get(_textblock_semantic_key(row))
        if not competing_values:
            out.append(row)
            continue
        if row.value_num in competing_values:
            continue
        detail["semantic_conflict_status"] = "review"
        detail["semantic_conflict_reason"] = "conflicting_dimensioned_xbrl_value"
        out.append(
            replace(
                row,
                calc_status="review",
                source_detail_json=json.dumps(detail, ensure_ascii=False, sort_keys=True),
            )
        )
    return out


def _apply_fiscal_year_anchors(
    rows: list[SegmentMetricRow],
    *,
    filings_by_doc: dict[str, sqlite3.Row],
    current_fiscal_year_ends: dict[str, str],
    annual_period_ends_by_edinet_code: dict[str, list[str]],
) -> list[SegmentMetricRow]:
    out: list[SegmentMetricRow] = []
    for row in rows:
        filing = filings_by_doc.get(row.doc_id)
        if filing is None:
            out.append(row)
            continue
        anchor = resolve_segment_fiscal_year_anchor(
            filing=filing,
            fact_period_end=row.period_end,
            current_fiscal_year_ends=current_fiscal_year_ends,
            annual_period_ends_by_edinet_code=annual_period_ends_by_edinet_code,
        )
        detail = _safe_json(row.source_detail_json)
        detail["fiscal_year_anchor_period_end"] = anchor.fiscal_year_end
        detail["fiscal_year_anchor_source"] = anchor.source
        detail["fiscal_year_anchor_status"] = anchor.status
        calc_status = row.calc_status
        if row.period_scope == "quarter" and row.quarter_type == "2Q" and anchor.status != "ok":
            calc_status = "review"
        out.append(
            replace(
                row,
                fiscal_year=anchor.fiscal_year,
                calc_status=calc_status,
                source_detail_json=json.dumps(detail, ensure_ascii=False, sort_keys=True),
            )
        )
    return out


def _chunked(items: list[str], size: int = 500) -> list[list[str]]:
    return [items[index : index + size] for index in range(0, len(items), size)]


def _fetch_target_filings(
    conn: sqlite3.Connection,
    *,
    doc_ids: list[str] | None,
    codes: list[str] | None,
    date_from: str | None,
    date_to: str | None,
    form_codes: list[str] | None,
) -> list[sqlite3.Row]:
    normalized_form_codes = _normalize_form_codes(form_codes)
    form_where = "f.form_type IN (" + ",".join("?" for _ in normalized_form_codes) + ")"

    def fetch_with_where(extra_where: list[str], extra_params: list[Any]) -> list[sqlite3.Row]:
        where = [form_where, *extra_where]
        params: list[Any] = [*normalized_form_codes, *extra_params]
        return conn.execute(
            f"""
            SELECT
              f.doc_id,
              f.edinet_code,
              coalesce(f.security_code, im.security_code, '') AS security_code,
              f.form_type,
              f.period_end,
              f.zip_path,
              f.xbrl_path,
              im.company_name,
              im.industry_33,
              im.market
            FROM filings f
            LEFT JOIN issuer_master im
              ON im.edinet_code = f.edinet_code
            WHERE {' AND '.join(where)}
            ORDER BY f.period_end, f.doc_id
            """,
            params,
        ).fetchall()

    normalized_doc_ids = [str(doc_id).strip() for doc_id in (doc_ids or []) if str(doc_id).strip()]
    if normalized_doc_ids:
        rows: list[sqlite3.Row] = []
        for chunk in _chunked(normalized_doc_ids):
            rows.extend(
                fetch_with_where(
                    ["f.doc_id IN (" + ",".join("?" for _ in chunk) + ")"],
                    list(chunk),
                )
            )
        return rows

    where = ["coalesce(im.is_listed, 0) = 1", "coalesce(im.exchange, '') = 'TSE'"]
    params: list[Any] = []
    normalized_codes = [_normalize_security_code(code) for code in (codes or []) if str(code).strip()]
    if normalized_codes:
        placeholders = ",".join("?" for _ in normalized_codes)
        where.append(
            f"(substr(coalesce(im.security_code, ''), 1, 4) IN ({placeholders}) "
            f"OR coalesce(im.security_code, '') IN ({placeholders}))"
        )
        params.extend(normalized_codes)
        params.extend(normalized_codes)
    if date_from:
        where.append("coalesce(f.period_end, '') >= ?")
        params.append(date_from)
    if date_to:
        where.append("coalesce(f.period_end, '') <= ?")
        params.append(date_to)
    return fetch_with_where(where, params)


def _fetch_raw_facts(conn: sqlite3.Connection, doc_ids: list[str]) -> list[sqlite3.Row]:
    if not doc_ids:
        return []
    rows: list[sqlite3.Row] = []
    metric_tags = sorted(METRIC_INFO_BY_TAG)
    metric_placeholders = ",".join("?" for _ in metric_tags)
    for chunk in _chunked(doc_ids):
        placeholders = ",".join("?" for _ in chunk)
        rows.extend(
            conn.execute(
                f"""
                SELECT
                  doc_id,
                  tag_name,
                  tag_qname,
                  context_ref,
                  unit_ref,
                  decimals,
                  period_type,
                  period_start,
                  period_end,
                  instant_date,
                  is_nil,
                  context_dimensions_json,
                  unit_measures_json,
                  value_text
                FROM raw_facts
                WHERE doc_id IN ({placeholders})
                  AND (
                    coalesce(context_dimensions_json, '') <> ''
                    OR (
                      tag_name IN ({metric_placeholders})
                      AND (
                        context_ref LIKE '%ReportableSegmentsMember%'
                        OR context_ref LIKE '%ReportableSegmentMember%'
                        OR context_ref LIKE '%OperatingSegmentMember%'
                        OR context_ref LIKE '%OperatingSegmentsNotIncludedInReportableSegmentsAndOtherRevenueGeneratingBusinessActivitiesMember%'
                        OR context_ref LIKE '%TotalOfReportableSegmentsAndOthersMember%'
                        OR context_ref LIKE '%TotalOfReportableSegmentsMember%'
                        OR context_ref LIKE '%ReconcilingItemsMember%'
                      )
                    )
                    OR tag_name IN (
                      'InformationAboutGeographicalAreasIFRSTextBlock',
                      'InformationAboutGeographicalAreasTextBlock'
                    )
                    OR (
                      lower(tag_name) LIKE '%segmentinformation%'
                      AND lower(tag_name) LIKE '%textblock%'
                    )
                  )
                """,
                [*chunk, *metric_tags],
            ).fetchall()
        )
    return rows


def _linkbase_structure_for_filing(filing: sqlite3.Row) -> dict[str, dict[str, Any]]:
    try:
        return analyze_linkbase_structure(
            xbrl_path=str(filing["xbrl_path"] or ""),
            zip_path=str(filing["zip_path"] or ""),
        )
    except Exception:
        return {}


def _labels_from_structure(structure: dict[str, dict[str, Any]]) -> dict[str, str]:
    return {
        str(concept): str(info.get("label") or "").strip()
        for concept, info in structure.items()
        if isinstance(info, dict) and str(info.get("label") or "").strip()
    }


def _apply_preferred_segment_names(rows: list[SegmentMetricRow]) -> list[SegmentMetricRow]:
    preferred = preferred_segment_name_map(
        SegmentNameCandidate(
            edinet_code=row.edinet_code,
            segment_kind=row.segment_kind,
            member_qname=row.member_qname,
            segment_name=row.segment_name,
            period_end=row.period_end,
        )
        for row in rows
    )
    out: list[SegmentMetricRow] = []
    for row in rows:
        key = (
            row.edinet_code,
            row.segment_kind,
            canonical_segment_key(row.member_qname, row.segment_name),
        )
        segment_name = preferred.get(key, row.segment_name)
        out.append(replace(row, segment_name=segment_name) if segment_name != row.segment_name else row)
    return out


def build_segment_metric_rows(
    conn: sqlite3.Connection,
    *,
    doc_ids: list[str] | None = None,
    codes: list[str] | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
    form_codes: list[str] | None = None,
    rule_version: str = SEGMENT_RULE_VERSION,
) -> SegmentMetricBuildResult:
    warnings: list[str] = []
    filings = _fetch_target_filings(
        conn,
        doc_ids=doc_ids,
        codes=codes,
        date_from=date_from,
        date_to=date_to,
        form_codes=form_codes,
    )
    filings_by_doc = {str(row["doc_id"]): row for row in filings}
    raw_facts = _fetch_raw_facts(conn, list(filings_by_doc))
    current_fiscal_year_ends = _fetch_current_fiscal_year_ends(conn, list(filings_by_doc))
    annual_period_ends_by_edinet_code = _annual_period_ends_by_edinet_code(conn)
    raw_doc_ids = {str(row["doc_id"] or "") for row in raw_facts}
    linkbase_by_doc = {
        doc_id: _linkbase_structure_for_filing(row)
        for doc_id, row in filings_by_doc.items()
        if doc_id in raw_doc_ids
    }
    labels_by_doc = {
        doc_id: _labels_from_structure(structure)
        for doc_id, structure in linkbase_by_doc.items()
    }

    selected: dict[tuple[Any, ...], tuple[int, int, SegmentMetricRow]] = {}
    candidates: list[SegmentCandidate] = []

    for raw in raw_facts:
        filing = filings_by_doc.get(str(raw["doc_id"]))
        if filing is None:
            continue
        tag_name = str(raw["tag_name"] or "")
        info = _metric_info(tag_name)
        axis_member = _segment_axis_member(raw["context_dimensions_json"], raw["context_ref"])
        value_num = _to_float(raw["value_text"])
        company_name = str(filing["company_name"] or "")
        security_code = _normalize_security_code(filing["security_code"] or "")
        period_end = str(raw["period_end"] or filing["period_end"] or "")
        candidate_common = {
            "doc_id": str(raw["doc_id"] or ""),
            "security_code": security_code,
            "company_name": company_name,
            "period_end": period_end,
            "source_tag": tag_name,
            "context_ref": str(raw["context_ref"] or ""),
        }
        if _is_segment_note_textblock_tag(tag_name):
            form_type = str(filing["form_type"] or "")
            period_scope, quarter_type = SEGMENT_PERIOD_SCOPE_BY_FORM_TYPE.get(form_type, (form_type, ""))
            text_entries, rejections = _segment_note_textblock_entries(
                str(raw["value_text"] or ""),
                source_tag=tag_name,
                period_end=period_end,
            )
            for rejection in rejections:
                candidates.append(
                    SegmentCandidate(
                        **candidate_common,
                        segment_kind="excluded",
                        segment_name="",
                        member_qname="",
                        metric_base="",
                        value_kind="",
                        value_num=None,
                        status="excluded",
                        reason=rejection,
                    )
                )
            for entry in text_entries:
                info_from_text = TEXTBLOCK_METRIC_INFO_BY_BASE.get(entry.metric_base)
                if info_from_text is None:
                    continue
                metric_base, metric_key, value_kind, tag_priority = info_from_text
                member_qname = f"textblock:{entry.segment_kind}:{entry.segment_name}"
                row = SegmentMetricRow(
                    doc_id=str(raw["doc_id"] or ""),
                    edinet_code=str(filing["edinet_code"] or ""),
                    security_code=security_code,
                    form_type=form_type,
                    period_scope=period_scope,
                    quarter_type=quarter_type,
                    fiscal_year=_parse_year(raw["period_end"] or filing["period_end"]),
                    period_start=str(raw["period_start"] or ""),
                    period_end=period_end,
                    segment_kind=entry.segment_kind,
                    segment_name=entry.segment_name,
                    axis_qname=f"textblock:{entry.table_kind}",
                    member_qname=member_qname,
                    metric_base=metric_base,
                    metric_key=metric_key,
                    value_kind=value_kind,
                    value_num=entry.value_num,
                    value_unit="yen",
                    source_tag=tag_name,
                    tag_qname=str(raw["tag_qname"] or ""),
                    context_ref=str(raw["context_ref"] or ""),
                    decimals="",
                    calc_status="ok",
                    source_detail_json=json.dumps(
                        {
                            "axis_qname": f"textblock:{entry.table_kind}",
                            "member_qname": member_qname,
                            "member_priority": 50,
                            "segment_order": entry.table_index,
                            "tag_priority": tag_priority,
                            "company_name": company_name,
                            "source": "segment_note_textblock",
                            "table_kind": entry.table_kind,
                            "source_heading": entry.source_heading[:500],
                            "metric_label": entry.metric_label,
                            "unit_multiplier": entry.unit_multiplier,
                            "selection_reason": "selected_semantic_segment_note_table",
                        },
                        ensure_ascii=False,
                        sort_keys=True,
                    ),
                    rule_version=rule_version,
                )
                key = (
                    row.doc_id,
                    row.segment_kind,
                    "TOTAL" if row.segment_kind == "total" else row.member_qname,
                    row.metric_key,
                    row.value_kind,
                    row.period_start,
                    row.period_end,
                )
                priority = (50, tag_priority + 100 * _segment_note_textblock_source_priority(tag_name))
                previous = selected.get(key)
                if previous is None or priority < (previous[0], previous[1]):
                    selected[key] = (priority[0], priority[1], row)
                    candidates.append(
                        SegmentCandidate(
                            **candidate_common,
                            segment_kind=entry.segment_kind,
                            segment_name=entry.segment_name,
                            member_qname=member_qname,
                            metric_base=metric_base,
                            value_kind=value_kind,
                            value_num=entry.value_num,
                            status="selected",
                            reason="selected_semantic_segment_note_table",
                        )
                    )
            continue
        if axis_member is None:
            continue
        axis_qname, member_qname = axis_member
        kind = _segment_kind(member_qname, axis_qname)
        segment_order = _linkbase_presentation_sequence(
            member_qname,
            linkbase_by_doc.get(str(raw["doc_id"]), {}),
        )
        segment_name = _humanize_member_name(member_qname, labels_by_doc.get(str(raw["doc_id"]), {}))
        if kind is None:
            candidates.append(
                SegmentCandidate(
                    **candidate_common,
                    segment_kind="excluded",
                    segment_name=segment_name,
                    member_qname=member_qname,
                    metric_base="",
                    value_kind="",
                    value_num=value_num,
                    status="excluded",
                    reason="excluded_adjustment_or_elimination_member",
                )
            )
            continue
        if info is None:
            candidates.append(
                SegmentCandidate(
                    **candidate_common,
                    segment_kind=kind,
                    segment_name=segment_name,
                    member_qname=member_qname,
                    metric_base="",
                    value_kind="",
                    value_num=value_num,
                    status="excluded",
                    reason="unsupported_segment_tag",
                )
            )
            continue
        if int(raw["is_nil"] or 0):
            candidates.append(
                SegmentCandidate(
                    **candidate_common,
                    segment_kind=kind,
                    segment_name=segment_name,
                    member_qname=member_qname,
                    metric_base=info[0],
                    value_kind=info[2],
                    value_num=None,
                    status="excluded",
                    reason="nil_fact",
                )
            )
            continue
        if value_num is None:
            candidates.append(
                SegmentCandidate(
                    **candidate_common,
                    segment_kind=kind,
                    segment_name=segment_name,
                    member_qname=member_qname,
                    metric_base=info[0],
                    value_kind=info[2],
                    value_num=None,
                    status="excluded",
                    reason="not_numeric",
                )
            )
            continue
        if str(raw["period_type"] or "") != "duration":
            candidates.append(
                SegmentCandidate(
                    **candidate_common,
                    segment_kind=kind,
                    segment_name=segment_name,
                    member_qname=member_qname,
                    metric_base=info[0],
                    value_kind=info[2],
                    value_num=value_num,
                    status="excluded",
                    reason="not_duration_fact",
                )
            )
            continue
        filing_period_end = str(filing["period_end"] or "")[:10]
        raw_period_end = str(raw["period_end"] or "")[:10]
        if filing_period_end and raw_period_end and raw_period_end != filing_period_end:
            candidates.append(
                SegmentCandidate(
                    **candidate_common,
                    segment_kind=kind,
                    segment_name=segment_name,
                    member_qname=member_qname,
                    metric_base=info[0],
                    value_kind=info[2],
                    value_num=value_num,
                    status="excluded",
                    reason="period_end_mismatch",
                )
            )
            continue

        metric_base, metric_key, value_kind, tag_priority = info
        line_item_label = ""
        profit_classification = ""
        profit_classification_source = ""
        profit_classification_status = ""
        calc_status = "ok"
        if metric_base in PROFIT_METRIC_INFO_BY_BASE:
            line_item_label = _linkbase_label_for_qname(
                str(raw["tag_qname"] or ""),
                labels_by_doc.get(str(raw["doc_id"]), {}),
            )
            classified_info, profit_classification, profit_classification_source, profit_classification_status = (
                _profit_metric_classification(
                    tag_name=tag_name,
                    line_item_label=line_item_label,
                )
            )
            if classified_info is None:
                calc_status = "review"
            else:
                metric_base, metric_key, value_kind, tag_priority = classified_info
        form_type = str(filing["form_type"] or "")
        period_scope, quarter_type = SEGMENT_PERIOD_SCOPE_BY_FORM_TYPE.get(form_type, (form_type, ""))
        row = SegmentMetricRow(
            doc_id=str(raw["doc_id"] or ""),
            edinet_code=str(filing["edinet_code"] or ""),
            security_code=security_code,
            form_type=form_type,
            period_scope=period_scope,
            quarter_type=quarter_type,
            fiscal_year=_parse_year(raw["period_end"] or filing["period_end"]),
            period_start=str(raw["period_start"] or ""),
            period_end=period_end,
            segment_kind=kind,
            segment_name=segment_name,
            axis_qname=axis_qname,
            member_qname=member_qname,
            metric_base=metric_base,
            metric_key=metric_key,
            value_kind=value_kind,
            value_num=value_num,
            value_unit=_value_unit(raw),
            source_tag=tag_name,
            tag_qname=str(raw["tag_qname"] or ""),
            context_ref=str(raw["context_ref"] or ""),
            decimals=str(raw["decimals"] or ""),
            calc_status=calc_status,
            source_detail_json=json.dumps(
                {
                    "axis_qname": axis_qname,
                    "member_qname": member_qname,
                    "member_priority": _member_priority(member_qname),
                    "segment_order": segment_order,
                    "tag_priority": tag_priority,
                    "company_name": company_name,
                    "source": "raw_facts",
                    **(
                        {
                            "line_item_label": line_item_label,
                            "profit_metric_classification": profit_classification,
                            "profit_metric_classification_source": profit_classification_source,
                            "profit_metric_classification_status": profit_classification_status,
                        }
                        if info[0] in PROFIT_METRIC_INFO_BY_BASE
                        else {}
                    ),
                },
                ensure_ascii=False,
                sort_keys=True,
            ),
            rule_version=rule_version,
        )
        key = (
            row.doc_id,
            row.segment_kind,
            "TOTAL" if row.segment_kind == "total" else row.member_qname,
            row.metric_key,
            row.value_kind,
            row.period_start,
            row.period_end,
        )
        priority = (_member_priority(member_qname), tag_priority)
        previous = selected.get(key)
        if previous is None or priority < (previous[0], previous[1]):
            if previous is not None:
                previous_row = previous[2]
                candidates.append(
                    SegmentCandidate(
                        doc_id=previous_row.doc_id,
                        security_code=previous_row.security_code,
                        company_name=company_name,
                        period_end=previous_row.period_end,
                        segment_kind=previous_row.segment_kind,
                        segment_name=previous_row.segment_name,
                        member_qname=previous_row.member_qname,
                        source_tag=previous_row.source_tag,
                        metric_base=previous_row.metric_base,
                        value_kind=previous_row.value_kind,
                        value_num=previous_row.value_num,
                        status="excluded",
                        reason="lower_priority_duplicate",
                        context_ref=previous_row.context_ref,
                    )
                )
            selected[key] = (priority[0], priority[1], row)
            candidates.append(
                SegmentCandidate(
                    **candidate_common,
                    segment_kind=kind,
                    segment_name=segment_name,
                    member_qname=member_qname,
                    metric_base=metric_base,
                    value_kind=value_kind,
                    value_num=value_num,
                    status="selected" if calc_status == "ok" else "review",
                    reason="selected" if calc_status == "ok" else "profit_metric_label_unclassified",
                )
            )
        else:
            candidates.append(
                SegmentCandidate(
                    **candidate_common,
                    segment_kind=kind,
                    segment_name=segment_name,
                    member_qname=member_qname,
                    metric_base=metric_base,
                    value_kind=value_kind,
                    value_num=value_num,
                    status="excluded",
                    reason="lower_priority_duplicate",
                )
            )

    rows = _apply_textblock_source_conflicts([item[2] for item in selected.values()])
    rows = _apply_profit_metric_conflicts(rows)
    rows = _apply_fiscal_year_anchors(
        rows,
        filings_by_doc=filings_by_doc,
        current_fiscal_year_ends=current_fiscal_year_ends,
        annual_period_ends_by_edinet_code=annual_period_ends_by_edinet_code,
    )
    rows = _apply_preferred_segment_names(rows)
    rows.sort(
        key=lambda row: (
            row.security_code,
            row.period_scope,
            row.quarter_type,
            row.period_end,
            row.segment_kind,
            _segment_order_from_detail(row.source_detail_json),
            row.segment_name,
            row.metric_base,
            row.value_kind,
        )
    )
    if filings and not rows:
        warnings.append("segment_candidates_not_found")
    return SegmentMetricBuildResult(rows=rows, candidates=candidates, warnings=warnings)


def replace_segment_metrics(
    conn: sqlite3.Connection,
    rows: list[SegmentMetricRow],
    *,
    replace_doc_ids: list[str] | None = None,
) -> int:
    target_doc_ids = sorted(
        {
            str(doc_id or "").strip()
            for doc_id in (replace_doc_ids or [row.doc_id for row in rows])
            if str(doc_id or "").strip()
        }
    )
    if target_doc_ids:
        for chunk in _chunked(target_doc_ids):
            placeholders = ",".join("?" for _ in chunk)
            conn.execute(
                f"DELETE FROM segment_metrics WHERE doc_id IN ({placeholders})",
                chunk,
            )
    if not rows:
        conn.commit()
        return 0
    now = _now_text()
    conn.executemany(
        """
        INSERT INTO segment_metrics (
            doc_id, edinet_code, security_code, form_type, period_scope, quarter_type,
            fiscal_year, period_start, period_end, segment_kind, segment_name,
            axis_qname, member_qname, metric_base, metric_key, value_kind,
            value_num, value_unit, source_tag, tag_qname, context_ref, decimals,
            calc_status, source_detail_json, rule_version, created_at, updated_at
        ) VALUES (
            :doc_id, :edinet_code, :security_code, :form_type, :period_scope, :quarter_type,
            :fiscal_year, :period_start, :period_end, :segment_kind, :segment_name,
            :axis_qname, :member_qname, :metric_base, :metric_key, :value_kind,
            :value_num, :value_unit, :source_tag, :tag_qname, :context_ref, :decimals,
            :calc_status, :source_detail_json, :rule_version, :created_at, :updated_at
        )
        """,
        [
            {
                **row.__dict__,
                "created_at": now,
                "updated_at": now,
            }
            for row in rows
        ],
    )
    conn.commit()
    return len(rows)


def _display_width(text: Any) -> int:
    import unicodedata

    width = 0
    for ch in str(text):
        width += 2 if unicodedata.east_asian_width(ch) in ("F", "W", "A") else 1
    return width


def _pad_right(text: Any, width: int) -> str:
    text = str(text)
    return text + " " * max(0, width - _display_width(text))


def _pad_left(text: Any, width: int) -> str:
    text = str(text)
    return " " * max(0, width - _display_width(text)) + text


def write_segment_metric_report(
    *,
    result: SegmentMetricBuildResult,
    output_dir: str | Path,
    mode: str,
    date_from: str | None = None,
    date_to: str | None = None,
    selected_row_count: int | None = None,
    candidate_count: int | None = None,
) -> Path:
    directory = Path(output_dir)
    directory.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = directory / f"segment_metrics_{mode}_{timestamp}.txt"
    columns = [
        ("status", "status", 10, "left"),
        ("security_code", "code", 8, "left"),
        ("company_name", "company", 22, "left"),
        ("period_end", "period_end", 10, "left"),
        ("segment_kind", "kind", 8, "left"),
        ("segment_name", "segment", 24, "left"),
        ("metric_base", "metric", 16, "left"),
        ("value_kind", "value_kind", 18, "left"),
        ("value_num", "value", 16, "right"),
        ("source_tag", "source_tag", 32, "left"),
        ("reason", "reason", 28, "left"),
    ]
    lines = [
        f"generated_at: {datetime.now().isoformat(timespec='seconds')}",
        f"mode: {mode}",
        f"date_from: {date_from or ''}",
        f"date_to: {date_to or ''}",
        f"selected_rows: {len(result.rows) if selected_row_count is None else selected_row_count}",
        f"candidates: {len(result.candidates) if candidate_count is None else candidate_count}",
        f"warnings: {len(result.warnings)}",
        "",
        " | ".join(_pad_right(label, width) for _, label, width, _ in columns),
        "-+-".join("-" * width for _, _, width, _ in columns),
    ]
    for item in result.candidates[:500]:
        data = item.__dict__
        line_parts = []
        for key, _, width, align in columns:
            value = data.get(key, "")
            if key == "value_num" and value is not None:
                value = f"{float(value):,.0f}"
            line_parts.append(_pad_left(value, width) if align == "right" else _pad_right(value, width))
        lines.append(" | ".join(line_parts))
    if len(result.candidates) > 500:
        lines.append(f"... truncated candidates: {len(result.candidates) - 500}")
    if result.warnings:
        lines.append("")
        lines.append("warnings:")
        lines.extend(f"- {warning}" for warning in result.warnings)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8-sig")
    return path


def save_segment_metrics(
    conn: sqlite3.Connection,
    *,
    doc_ids: list[str] | None = None,
    codes: list[str] | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
    form_codes: list[str] | None = None,
    apply: bool = False,
    output_dir: str | Path = "logs/operation",
) -> SegmentMetricSaveResult:
    requested_doc_ids = [str(doc_id).strip() for doc_id in (doc_ids or []) if str(doc_id).strip()]
    batch_doc_ids = (
        _chunked(requested_doc_ids, SEGMENT_SAVE_DOC_ID_BATCH_SIZE)
        if apply and len(requested_doc_ids) > SEGMENT_SAVE_DOC_ID_BATCH_SIZE
        else [requested_doc_ids]
    )
    aggregate_rows: list[SegmentMetricRow] = []
    aggregate_candidates: list[SegmentCandidate] = []
    aggregate_warnings: list[str] = []
    built_row_count = 0
    candidate_count = 0
    saved_rows = 0
    replaced_doc_count = 0
    empty_doc_ids: list[str] = []

    for batch in batch_doc_ids:
        build = build_segment_metric_rows(
            conn,
            doc_ids=batch or None,
            codes=codes,
            date_from=date_from,
            date_to=date_to,
            form_codes=form_codes,
        )
        built_row_count += len(build.rows)
        candidate_count += len(build.candidates)
        aggregate_warnings.extend(build.warnings)
        aggregate_candidates.extend(build.candidates[: max(500 - len(aggregate_candidates), 0)])

        if batch:
            built_doc_ids = sorted({row.doc_id for row in build.rows})
            empty_doc_ids.extend(sorted(set(batch) - set(built_doc_ids)))
            if apply and built_doc_ids:
                saved_rows += replace_segment_metrics(
                    conn,
                    build.rows,
                    replace_doc_ids=built_doc_ids,
                )
                replaced_doc_count += len(built_doc_ids)
        elif apply:
            saved_rows += replace_segment_metrics(conn, build.rows)
            replaced_doc_count += len({row.doc_id for row in build.rows})

        if len(batch_doc_ids) == 1:
            aggregate_rows = build.rows
        del build
        gc.collect()

    if apply and requested_doc_ids and built_row_count == 0:
        raise RuntimeError("No segment metrics were built for the requested doc IDs.")
    if apply and empty_doc_ids:
        replace_segment_metrics(conn, [], replace_doc_ids=empty_doc_ids)
        replaced_doc_count += len(set(empty_doc_ids))
        aggregate_warnings.append(
            f"removed_doc_ids_without_current_segment_rows={len(set(empty_doc_ids))}"
        )
    if empty_doc_ids and not apply:
        aggregate_warnings.append(
            f"requested_doc_ids_without_segment_rows={len(set(empty_doc_ids))}"
        )
    result = SegmentMetricBuildResult(
        rows=aggregate_rows,
        candidates=aggregate_candidates,
        warnings=aggregate_warnings,
    )
    report_path = write_segment_metric_report(
        result=result,
        output_dir=output_dir,
        mode="apply" if apply else "dry_run",
        date_from=date_from,
        date_to=date_to,
        selected_row_count=built_row_count,
        candidate_count=candidate_count,
    )
    return SegmentMetricSaveResult(
        rows=aggregate_rows,
        candidates=aggregate_candidates,
        saved_rows=saved_rows,
        warnings=aggregate_warnings,
        output_path=report_path,
        built_row_count=built_row_count,
        candidate_count=candidate_count,
        replaced_doc_count=replaced_doc_count,
    )
