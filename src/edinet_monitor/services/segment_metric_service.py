from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
import json
import re
import sqlite3
from typing import Any

from edinet_pipeline.services.linkbase_analyzer import analyze_linkbase_structure


SEGMENT_RULE_VERSION = "segment-metrics-2026-05-15-v1"
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

SEGMENT_EXCEL_METRIC_LABELS = {
    "NetSales": "売上高",
    "OperatingIncome": "営業利益",
    "ProfitBeforeTax": "経常利益相当",
    "ProfitLoss": "純利益",
    "SegmentProfit": "セグメント利益",
}


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


def _contains_any(text: str, markers: tuple[str, ...]) -> bool:
    return any(marker.lower() in text.lower() for marker in markers)


def _segment_axis_member(dimensions_json: Any) -> tuple[str, str] | None:
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
    return None


def _member_priority(member_qname: str) -> int:
    local = _local_name(member_qname)
    if "TotalOfReportableSegmentsAndOthersMember" in local:
        return 0
    if "TotalOfReportableSegmentsMember" in local:
        return 1
    if "ReportableSegmentsMember" in local:
        return 2
    return 10


def _segment_kind(member_qname: str, axis_qname: str) -> str | None:
    text = f"{member_qname} {axis_qname}"
    if _contains_any(text, TOTAL_MEMBER_MARKERS):
        return "total"
    if _contains_any(text, EXCLUDED_MEMBER_MARKERS):
        return None
    local_upper = _local_name(member_qname).upper()
    if any(marker in local_upper for marker in REGION_MEMBER_LABELS):
        return "region"
    if _contains_any(axis_qname, ("Geographical", "Geographic", "Region", "Area")):
        return "region"
    return "business"


def _humanize_member_name(member_qname: str, labels_by_concept: dict[str, str]) -> str:
    local = _local_name(member_qname)
    if _contains_any(local, TOTAL_MEMBER_MARKERS):
        return "合計"

    label = labels_by_concept.get(local, "").strip()
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


def _value_unit(row: sqlite3.Row) -> str:
    unit_ref = str(row["unit_ref"] or "").strip()
    if unit_ref.upper() == "JPY":
        return "yen"
    return unit_ref or "unknown"


def _metric_info(tag_name: str) -> tuple[str, str, str, int] | None:
    return METRIC_INFO_BY_TAG.get(str(tag_name or ""))


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
                  AND coalesce(context_dimensions_json, '') <> ''
                """,
                chunk,
            ).fetchall()
        )
    return rows


def _labels_for_filing(filing: sqlite3.Row) -> dict[str, str]:
    try:
        structure = analyze_linkbase_structure(
            xbrl_path=str(filing["xbrl_path"] or ""),
            zip_path=str(filing["zip_path"] or ""),
        )
    except Exception:
        return {}
    return {
        str(concept): str(info.get("label") or "").strip()
        for concept, info in structure.items()
        if isinstance(info, dict) and str(info.get("label") or "").strip()
    }


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
    raw_doc_ids = {str(row["doc_id"] or "") for row in raw_facts}
    labels_by_doc = {
        doc_id: _labels_for_filing(row)
        for doc_id, row in filings_by_doc.items()
        if doc_id in raw_doc_ids
    }

    selected: dict[tuple[Any, ...], tuple[int, int, SegmentMetricRow]] = {}
    candidates: list[SegmentCandidate] = []

    for raw in raw_facts:
        filing = filings_by_doc.get(str(raw["doc_id"]))
        if filing is None:
            continue
        tag_name = str(raw["tag_name"] or "")
        info = _metric_info(tag_name)
        axis_member = _segment_axis_member(raw["context_dimensions_json"])
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
        if axis_member is None:
            continue
        axis_qname, member_qname = axis_member
        kind = _segment_kind(member_qname, axis_qname)
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
            calc_status="ok",
            source_detail_json=json.dumps(
                {
                    "axis_qname": axis_qname,
                    "member_qname": member_qname,
                    "member_priority": _member_priority(member_qname),
                    "tag_priority": tag_priority,
                    "company_name": company_name,
                    "source": "raw_facts",
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
                    status="selected",
                    reason="selected",
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

    rows = [item[2] for item in selected.values()]
    rows.sort(
        key=lambda row: (
            row.security_code,
            row.period_scope,
            row.quarter_type,
            row.period_end,
            row.segment_kind,
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
        f"selected_rows: {len(result.rows)}",
        f"candidates: {len(result.candidates)}",
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
    build = build_segment_metric_rows(
        conn,
        doc_ids=doc_ids,
        codes=codes,
        date_from=date_from,
        date_to=date_to,
        form_codes=form_codes,
    )
    saved_rows = (
        replace_segment_metrics(conn, build.rows, replace_doc_ids=doc_ids)
        if apply
        else 0
    )
    report_path = write_segment_metric_report(
        result=build,
        output_dir=output_dir,
        mode="apply" if apply else "dry_run",
        date_from=date_from,
        date_to=date_to,
    )
    return SegmentMetricSaveResult(
        rows=build.rows,
        candidates=build.candidates,
        saved_rows=saved_rows,
        warnings=build.warnings,
        output_path=report_path,
    )
