from __future__ import annotations

import json
import math
import sqlite3
import unicodedata
from datetime import datetime
from pathlib import Path
from typing import Any

from edinet_monitor.services.normalizer.metric_normalize_service import (
    build_normalization_candidates,
    select_best_normalization_candidates,
)
from edinet_monitor.services.normalizer.structure_classifier import classify_structure
from edinet_pipeline.domain.metric_labels import (
    metric_base_to_display_name,
    metric_key_to_display_name,
    split_metric_key,
    tag_name_to_display_name,
)
from edinet_pipeline.domain.tag_alias import normalize_tag_to_metric
from edinet_pipeline.services.linkbase_analyzer import analyze_linkbase_structure


RAW_FACT_AUDIT_COLUMNS = [
    "doc_id",
    "tag_name",
    "tag_qname",
    "namespace_uri",
    "namespace_prefix",
    "taxonomy_kind",
    "context_ref",
    "unit_ref",
    "decimals",
    "period_type",
    "period_start",
    "period_end",
    "instant_date",
    "consolidation",
    "is_nil",
    "context_dimensions_json",
    "unit_measures_json",
    "xbrl_member_name",
    "value_text",
    "created_at",
]


METRIC_SEARCH_TOKENS = {
    "NetSales": ["sales", "revenue", "operatingrevenue", "businessrevenue"],
    "CostOfSales": ["cost", "sales", "revenue", "goods", "operatingcost"],
    "GrossProfit": ["gross", "profit"],
    "SellingExpenses": ["selling", "general", "administrative", "sga", "distribution"],
    "CostOfSalesAndSellingGeneralAndAdministrativeExpenses": ["expense", "expenses", "costs", "operatingexpenses"],
    "OperatingIncome": ["operating", "income", "profit"],
    "OrdinaryIncome": ["ordinary", "income", "profitbeforetax", "beforetax"],
    "ProfitLoss": ["profitloss", "profit", "owners", "parent"],
    "OperatingCash": ["operating", "cash", "activities"],
    "InvestmentCash": ["investing", "investment", "cash", "activities"],
    "FinancingCash": ["financing", "cash", "activities"],
    "TotalAssets": ["assets", "totalassets"],
    "NetAssets": ["netassets", "equity"],
    "CashAndCashEquivalents": ["cash", "equivalents"],
    "IssuedShares": ["issued", "shares"],
    "TreasuryShares": ["treasury", "shares"],
}


def now_stamp() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def display_width(text: Any) -> int:
    width = 0
    for ch in str(text):
        width += 2 if unicodedata.east_asian_width(ch) in {"F", "W", "A"} else 1
    return width


def pad_right(text: Any, width: int) -> str:
    value = str(text)
    return value + " " * max(0, width - display_width(value))


def pad_left(text: Any, width: int) -> str:
    value = str(text)
    return " " * max(0, width - display_width(value)) + value


def write_text_report(path: Path, lines: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8-sig")


def format_number(value: Any) -> str:
    if value in (None, ""):
        return ""
    try:
        value_num = float(value)
    except Exception:
        return str(value)
    if not math.isfinite(value_num):
        return str(value)
    if value_num.is_integer():
        return f"{int(value_num):,}"
    return f"{value_num:,.6g}"


def to_number(value_text: Any) -> float | None:
    if value_text in (None, ""):
        return None
    try:
        return float(str(value_text).replace(",", ""))
    except Exception:
        return None


def security_code_variants(security_code: str) -> list[str]:
    code = str(security_code or "").strip()
    if not code:
        return []
    variants = {code}
    if code.endswith("0") and len(code) == 5:
        variants.add(code[:-1])
    elif len(code) == 4:
        variants.add(f"{code}0")
    return sorted(variants)


def fetch_filing(
    conn: sqlite3.Connection,
    *,
    doc_id: str = "",
    security_code: str = "",
    period_end: str = "",
) -> dict[str, Any] | None:
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    if doc_id:
        row = cur.execute(
            """
            SELECT
                f.doc_id,
                f.edinet_code,
                COALESCE(f.security_code, im.security_code) AS security_code,
                im.company_name,
                im.industry_33,
                f.form_type,
                f.period_end,
                f.submit_date,
                f.xbrl_path,
                f.zip_path
            FROM filings f
            LEFT JOIN issuer_master im
                ON im.edinet_code = f.edinet_code
            WHERE f.doc_id = ?
            LIMIT 1
            """,
            (doc_id,),
        ).fetchone()
        return dict(row) if row else None

    variants = security_code_variants(security_code)
    if not variants:
        return None
    placeholders = ",".join("?" for _ in variants)
    params: list[Any] = list(variants)
    period_clause = ""
    if period_end:
        period_clause = "AND f.period_end = ?"
        params.append(period_end)

    row = cur.execute(
        f"""
        SELECT
            f.doc_id,
            f.edinet_code,
            COALESCE(f.security_code, im.security_code) AS security_code,
            im.company_name,
            im.industry_33,
            f.form_type,
            f.period_end,
            f.submit_date,
            f.xbrl_path,
            f.zip_path
        FROM filings f
        LEFT JOIN issuer_master im
            ON im.edinet_code = f.edinet_code
        WHERE COALESCE(f.security_code, im.security_code) IN ({placeholders})
          AND f.form_type = '030000'
          {period_clause}
        ORDER BY COALESCE(f.period_end, '') DESC,
                 COALESCE(f.submit_date, '') DESC,
                 f.doc_id DESC
        LIMIT 1
        """,
        tuple(params),
    ).fetchone()
    return dict(row) if row else None


def fetch_raw_fact_audit_rows(conn: sqlite3.Connection, doc_id: str) -> list[dict[str, Any]]:
    columns = {
        str(row[1])
        for row in conn.execute("PRAGMA table_info(raw_facts)").fetchall()
    }
    select_parts = [
        column if column in columns else f"'' AS {column}"
        for column in RAW_FACT_AUDIT_COLUMNS
    ]
    rows = conn.execute(
        f"""
        SELECT {", ".join(select_parts)}
        FROM raw_facts
        WHERE doc_id = ?
        ORDER BY period_end DESC, instant_date DESC, context_ref ASC, tag_name ASC
        """,
        (doc_id,),
    ).fetchall()
    return [dict(row) for row in rows]


def _raw_match_key(row: dict[str, Any]) -> tuple[str, str, str, str, str]:
    value_num = to_number(row.get("value_text"))
    value_key = "" if value_num is None else format(value_num, ".17g")
    return (
        str(row.get("tag_name") or ""),
        str(row.get("period_end") or row.get("instant_date") or ""),
        value_key,
        str(row.get("consolidation") or ""),
        str(row.get("period_type") or ""),
    )


def _candidate_match_key(row: dict[str, Any]) -> tuple[str, str, str, str, str]:
    value = row.get("value_num")
    value_key = "" if value is None else format(float(value), ".17g")
    period_type = "instant" if str(row.get("metric_key") or "").endswith("Instant") else ""
    return (
        str(row.get("source_tag") or ""),
        str(row.get("period_end") or ""),
        value_key,
        str(row.get("consolidation") or ""),
        period_type,
    )


def _candidate_rank(row: dict[str, Any]) -> tuple[Any, ...]:
    return (
        row.get("_consolidation_rank", 9999),
        row.get("_tag_priority", 9999),
        row.get("_structure_priority", 9999),
        row.get("_manual_override_priority", 9999),
        str(row.get("source_tag") or ""),
    )


def _safe_json_loads(text: Any) -> Any:
    if not text:
        return {}
    try:
        return json.loads(str(text))
    except Exception:
        return {}


def _compact_json(text: Any, max_len: int = 120) -> str:
    value = _safe_json_loads(text)
    if not value:
        return ""
    rendered = json.dumps(value, ensure_ascii=False, sort_keys=True)
    if len(rendered) > max_len:
        return rendered[: max_len - 3] + "..."
    return rendered


def _enrich_candidates_with_raw_rows(
    *,
    candidates: list[dict[str, Any]],
    raw_rows: list[dict[str, Any]],
    structure_map: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    raw_by_key: dict[tuple[str, str, str, str, str], list[dict[str, Any]]] = {}
    raw_by_fallback_key: dict[tuple[str, str, str, str], list[dict[str, Any]]] = {}
    for row in raw_rows:
        key = _raw_match_key(row)
        raw_by_key.setdefault(key, []).append(row)
        raw_by_fallback_key.setdefault(key[:4], []).append(row)

    enriched: list[dict[str, Any]] = []
    for candidate in candidates:
        candidate_copy = dict(candidate)
        key = _candidate_match_key(candidate_copy)
        raw = (raw_by_key.get(key) or raw_by_fallback_key.get(key[:4]) or [{}])[0]
        structure_info = structure_map.get(str(candidate_copy.get("source_tag") or ""), {})
        schema = structure_info.get("schema") or {}
        candidate_copy.update(
            {
                "_raw_context_ref": raw.get("context_ref", ""),
                "_raw_period_type": raw.get("period_type", ""),
                "_raw_period_start": raw.get("period_start", ""),
                "_raw_period_end": raw.get("period_end") or raw.get("instant_date") or "",
                "_raw_tag_qname": raw.get("tag_qname", ""),
                "_raw_namespace_uri": raw.get("namespace_uri", ""),
                "_raw_namespace_prefix": raw.get("namespace_prefix", ""),
                "_raw_taxonomy_kind": raw.get("taxonomy_kind", ""),
                "_raw_unit_ref": raw.get("unit_ref", ""),
                "_raw_unit_measures": _compact_json(raw.get("unit_measures_json")),
                "_raw_decimals": raw.get("decimals", ""),
                "_raw_is_nil": raw.get("is_nil", ""),
                "_raw_dimensions": _compact_json(raw.get("context_dimensions_json")),
                "_raw_xbrl_member_name": raw.get("xbrl_member_name", ""),
                "_label": structure_info.get("label", ""),
                "_labels_by_role": structure_info.get("labels_by_role", {}),
                "_presentation_roles": structure_info.get("presentation_roles", []),
                "_calculation_roles": structure_info.get("calculation_roles", []),
                "_schema_type": schema.get("type", ""),
                "_schema_period_type": schema.get("period_type", ""),
                "_schema_balance": schema.get("balance", ""),
                "_schema_abstract": schema.get("abstract", ""),
            }
        )
        enriched.append(candidate_copy)
    return enriched


def build_metric_audit_rows(
    *,
    filing: dict[str, Any],
    raw_rows: list[dict[str, Any]],
    metric_base: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    structure_map = analyze_linkbase_structure(
        xbrl_path=str(filing.get("xbrl_path") or ""),
        zip_path=str(filing.get("zip_path") or ""),
    )
    base_rows = fetch_raw_fact_rows_from_audit_rows(raw_rows)
    candidates = build_normalization_candidates(
        base_rows,
        edinet_code=str(filing.get("edinet_code") or ""),
        security_code=str(filing.get("security_code") or ""),
        xbrl_path=str(filing.get("xbrl_path") or ""),
        zip_path=str(filing.get("zip_path") or ""),
    )
    selected = select_best_normalization_candidates(candidates)
    filtered_candidates = [
        row for row in candidates if str(row.get("_metric_base") or "") == metric_base
    ]
    enriched_candidates = _enrich_candidates_with_raw_rows(
        candidates=filtered_candidates,
        raw_rows=raw_rows,
        structure_map=structure_map,
    )
    enriched_selected = _enrich_candidates_with_raw_rows(
        candidates=[
            row for row in selected if str(row.get("_metric_base") or "") == metric_base
        ],
        raw_rows=raw_rows,
        structure_map=structure_map,
    )
    selected_keys = {
        (
            str(row.get("metric_key") or ""),
            str(row.get("source_tag") or ""),
            str(row.get("period_end") or ""),
            str(row.get("value_num") or ""),
            str(row.get("consolidation") or ""),
        )
        for row in enriched_selected
    }
    for row in enriched_candidates:
        key = (
            str(row.get("metric_key") or ""),
            str(row.get("source_tag") or ""),
            str(row.get("period_end") or ""),
            str(row.get("value_num") or ""),
            str(row.get("consolidation") or ""),
        )
        row["_selected"] = "YES" if key in selected_keys else ""
        row["_selection_reason"] = "selected_best_rank" if key in selected_keys else "not_best_rank"

    enriched_candidates.sort(
        key=lambda row: (
            str(row.get("metric_key") or ""),
            _candidate_rank(row),
            str(row.get("_raw_context_ref") or ""),
        )
    )
    return enriched_candidates, enriched_selected


def fetch_raw_fact_rows_from_audit_rows(raw_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    # Keep normalization behavior identical by passing only the columns it already used.
    return [
        {
            "doc_id": row.get("doc_id"),
            "tag_name": row.get("tag_name"),
            "context_ref": row.get("context_ref"),
            "unit_ref": row.get("unit_ref"),
            "period_type": row.get("period_type"),
            "period_start": row.get("period_start"),
            "period_end": row.get("period_end"),
            "instant_date": row.get("instant_date"),
            "consolidation": row.get("consolidation"),
            "value_text": row.get("value_text"),
        }
        for row in raw_rows
    ]


def build_metric_audit_report(
    *,
    filing: dict[str, Any],
    candidates: list[dict[str, Any]],
    selected: list[dict[str, Any]],
    metric_base: str,
    all_periods: bool,
    target_metric_key: str = "",
) -> list[str]:
    industry = str(filing.get("industry_33") or "")
    metric_label = metric_base_to_display_name(metric_base, industry)
    visible_candidates = [
        row
        for row in candidates
        if (
            (target_metric_key and str(row.get("metric_key") or "") == target_metric_key)
            or (
                not target_metric_key
                and (all_periods or str(row.get("metric_key") or "").endswith("Current"))
            )
        )
    ]

    lines = [
        f"generated_at: {datetime.now().isoformat(timespec='seconds')}",
        "report: metric_selection_audit",
        f"doc_id: {filing.get('doc_id', '')}",
        f"company_name: {filing.get('company_name', '')}",
        f"security_code: {filing.get('security_code', '')}",
        f"industry_33: {industry}",
        f"period_end: {filing.get('period_end', '')}",
        f"submit_date: {filing.get('submit_date', '')}",
        f"xbrl_path: {filing.get('xbrl_path', '')}",
        f"zip_path: {filing.get('zip_path', '')}",
        "",
        f"metric_base: {metric_base}",
        f"target_metric_key: {target_metric_key}",
        f"metric_label: {metric_label}",
        f"selected_count: {len(selected)}",
        f"candidate_count: {len(visible_candidates)}",
        "",
    ]

    if selected:
        lines.append("=== selected ===")
        for row in selected:
            lines.extend(_detail_lines(row, industry))
            lines.append("")

    lines.append("=== candidates ===")
    if not visible_candidates:
        lines.append("candidates=none")
        return lines

    columns = [
        ("selected", "採用", 6, "left"),
        ("metric_key", "指標", 34, "left"),
        ("source_tag", "タグ", 42, "left"),
        ("value", "値", 18, "right"),
        ("rank", "順位", 20, "left"),
        ("label", "ラベル", 30, "left"),
        ("context", "context", 40, "left"),
        ("schema", "schema", 28, "left"),
    ]
    lines.append(_table_header(columns))
    for row in visible_candidates:
        lines.append(
            _table_row(
                columns,
                {
                    "selected": row.get("_selected", ""),
                    "metric_key": row.get("metric_key", ""),
                    "source_tag": row.get("source_tag", ""),
                    "value": format_number(row.get("value_num")),
                    "rank": "/".join(str(x) for x in _candidate_rank(row)[:4]),
                    "label": row.get("_label") or tag_name_to_display_name(row.get("source_tag"), industry),
                    "context": row.get("_raw_context_ref", ""),
                    "schema": row.get("_schema_type", ""),
                },
            )
        )
    lines.append("")
    lines.append("=== candidate details ===")
    for idx, row in enumerate(visible_candidates, start=1):
        lines.append(f"[{idx}]")
        lines.extend(_detail_lines(row, industry))
        lines.append("")
    return lines


def _detail_lines(row: dict[str, Any], industry: str) -> list[str]:
    labels_by_role = row.get("_labels_by_role") or {}
    return [
        f"selected: {row.get('_selected', '')}",
        f"selection_reason: {row.get('_selection_reason', '')}",
        f"metric_key: {row.get('metric_key', '')}",
        f"metric_label: {metric_key_to_display_name(row.get('metric_key'), industry)}",
        f"source_tag: {row.get('source_tag', '')}",
        f"source_label: {tag_name_to_display_name(row.get('source_tag'), industry)}",
        f"tag_qname: {row.get('_raw_tag_qname', '')}",
        f"namespace_prefix: {row.get('_raw_namespace_prefix', '')}",
        f"namespace_uri: {row.get('_raw_namespace_uri', '')}",
        f"taxonomy_kind: {row.get('_raw_taxonomy_kind', '')}",
        f"label: {row.get('_label', '')}",
        f"labels_by_role: {json.dumps(labels_by_role, ensure_ascii=False, sort_keys=True)}",
        f"presentation_roles: {','.join(str(x) for x in row.get('_presentation_roles') or [])}",
        f"calculation_roles: {','.join(str(x) for x in row.get('_calculation_roles') or [])}",
        f"context_ref: {row.get('_raw_context_ref', '')}",
        f"period_type: {row.get('_raw_period_type', '')}",
        f"period_start: {row.get('_raw_period_start', '')}",
        f"period_end: {row.get('_raw_period_end', '')}",
        f"consolidation: {row.get('consolidation', '')}",
        f"dimension: {row.get('_raw_dimensions', '')}",
        f"unit_ref: {row.get('_raw_unit_ref', '')}",
        f"unit_measures: {row.get('_raw_unit_measures', '')}",
        f"decimals: {row.get('_raw_decimals', '')}",
        f"is_nil: {row.get('_raw_is_nil', '')}",
        f"schema_type: {row.get('_schema_type', '')}",
        f"schema_period_type: {row.get('_schema_period_type', '')}",
        f"schema_balance: {row.get('_schema_balance', '')}",
        f"schema_abstract: {row.get('_schema_abstract', '')}",
        f"value: {format_number(row.get('value_num'))}",
    ]


def _table_header(columns: list[tuple[str, str, int, str]]) -> str:
    return " | ".join(pad_right(label, width) for _, label, width, _ in columns)


def _table_row(columns: list[tuple[str, str, int, str]], row: dict[str, Any]) -> str:
    parts = []
    for key, _, width, align in columns:
        value = row.get(key, "")
        parts.append(pad_left(value, width) if align == "right" else pad_right(value, width))
    return " | ".join(parts)


def discover_extension_tag_candidates(
    *,
    filing: dict[str, Any],
    raw_rows: list[dict[str, Any]],
    metric_base: str,
    include_mapped: bool,
    current_only: bool,
    limit: int,
) -> list[dict[str, Any]]:
    structure_map = analyze_linkbase_structure(
        xbrl_path=str(filing.get("xbrl_path") or ""),
        zip_path=str(filing.get("zip_path") or ""),
    )
    tokens = METRIC_SEARCH_TOKENS.get(metric_base, _tokens_from_metric_base(metric_base))
    scored: list[dict[str, Any]] = []
    for row in raw_rows:
        tag_name = str(row.get("tag_name") or "")
        mapped_metric = normalize_tag_to_metric(tag_name)
        if mapped_metric and not include_mapped:
            continue
        row_period_end = str(row.get("period_end") or row.get("instant_date") or "")
        if (
            current_only
            and "CurrentYear" not in str(row.get("context_ref") or "")
            and row_period_end != str(filing.get("period_end") or "")
        ):
            continue
        value_num = to_number(row.get("value_text"))
        if value_num is None:
            continue
        if str(row.get("is_nil") or "") in {"1", "true", "True"}:
            continue

        structure_info = structure_map.get(tag_name, {})
        classification = classify_structure(
            metric_base=metric_base,
            tag_name=tag_name,
            structure_info=structure_info,
        )
        schema = structure_info.get("schema") or {}
        score = _candidate_discovery_score(
            row=row,
            structure_info=structure_info,
            classification=classification,
            schema=schema,
            tokens=tokens,
        )
        if score <= 0:
            continue
        scored.append(
            {
                "score": score,
                "tag_name": tag_name,
                "tag_qname": row.get("tag_qname", ""),
                "taxonomy_kind": row.get("taxonomy_kind", ""),
                "mapped_metric": mapped_metric or "",
                "label": structure_info.get("label", ""),
                "role": classification.get("role", ""),
                "confidence": classification.get("confidence", ""),
                "is_total": str(classification.get("is_total", "")),
                "context_ref": row.get("context_ref", ""),
                "period_type": row.get("period_type", ""),
                "period_start": row.get("period_start", ""),
                "period_end": row.get("period_end") or row.get("instant_date") or "",
                "consolidation": row.get("consolidation", ""),
                "dimension": _compact_json(row.get("context_dimensions_json")),
                "unit": _compact_json(row.get("unit_measures_json")),
                "decimals": row.get("decimals", ""),
                "schema_type": schema.get("type", ""),
                "schema_period_type": schema.get("period_type", ""),
                "value": format_number(value_num),
            }
        )

    scored.sort(
        key=lambda row: (
            -int(row.get("score") or 0),
            str(row.get("taxonomy_kind") or ""),
            str(row.get("tag_name") or ""),
            str(row.get("context_ref") or ""),
        )
    )
    return scored[:limit]


def build_extension_candidate_report(
    *,
    filing: dict[str, Any],
    metric_base: str,
    rows: list[dict[str, Any]],
) -> list[str]:
    industry = str(filing.get("industry_33") or "")
    lines = [
        f"generated_at: {datetime.now().isoformat(timespec='seconds')}",
        "report: extension_tag_candidates",
        f"doc_id: {filing.get('doc_id', '')}",
        f"company_name: {filing.get('company_name', '')}",
        f"security_code: {filing.get('security_code', '')}",
        f"industry_33: {industry}",
        f"period_end: {filing.get('period_end', '')}",
        f"metric_base: {metric_base}",
        f"metric_label: {metric_base_to_display_name(metric_base, industry)}",
        f"candidate_count: {len(rows)}",
        "",
    ]
    columns = [
        ("score", "score", 6, "right"),
        ("tag_name", "tag", 44, "left"),
        ("taxonomy_kind", "taxonomy", 12, "left"),
        ("mapped_metric", "mapped", 22, "left"),
        ("label", "label", 28, "left"),
        ("role", "role", 14, "left"),
        ("context_ref", "context", 38, "left"),
        ("value", "value", 18, "right"),
    ]
    lines.append(_table_header(columns))
    for row in rows:
        lines.append(_table_row(columns, row))
    lines.append("")
    lines.append("=== details ===")
    for idx, row in enumerate(rows, start=1):
        lines.append(f"[{idx}]")
        for key in [
            "score",
            "tag_name",
            "tag_qname",
            "taxonomy_kind",
            "mapped_metric",
            "label",
            "role",
            "confidence",
            "is_total",
            "context_ref",
            "period_type",
            "period_start",
            "period_end",
            "consolidation",
            "dimension",
            "unit",
            "decimals",
            "schema_type",
            "schema_period_type",
            "value",
        ]:
            lines.append(f"{key}: {row.get(key, '')}")
        lines.append("")
    return lines


def _tokens_from_metric_base(metric_base: str) -> list[str]:
    tokens: list[str] = []
    current = ""
    for ch in metric_base:
        if ch.isupper() and current:
            tokens.append(current.lower())
            current = ch
        else:
            current += ch
    if current:
        tokens.append(current.lower())
    return tokens


def _candidate_discovery_score(
    *,
    row: dict[str, Any],
    structure_info: dict[str, Any],
    classification: dict[str, Any],
    schema: dict[str, Any],
    tokens: list[str],
) -> int:
    text = " ".join(
        [
            str(row.get("tag_name") or ""),
            str(row.get("tag_qname") or ""),
            str(structure_info.get("label") or ""),
            " ".join(str(x) for x in structure_info.get("presentation_parent_labels") or []),
        ]
    ).lower()
    score = 0
    for token in tokens:
        if token and token.lower() in text:
            score += 10
    taxonomy_kind = str(row.get("taxonomy_kind") or "")
    if taxonomy_kind == "extension":
        score += 8
    elif taxonomy_kind == "ifrs":
        score += 4
    if classification.get("confidence") == "high":
        score += 8
    elif classification.get("confidence") == "medium":
        score += 4
    if classification.get("is_total"):
        score += 3
    schema_type = str(schema.get("type") or "").lower()
    if "monetary" in schema_type:
        score += 2
    if str(schema.get("abstract") or "").lower() == "true":
        score -= 30
    if "textblock" in schema_type:
        score -= 30
    return score
