from __future__ import annotations

import csv
import math
import sqlite3
import unicodedata
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

from edinet_monitor.config.settings import DEFAULT_DERIVED_METRICS_RULE_VERSION
from edinet_monitor.services.derived_metrics.derived_metric_service import calculate_derived_metrics
from edinet_monitor.services.normalizer.metric_normalize_service import (
    build_normalization_candidates,
    select_best_normalization_candidates,
)


IMPACT_TSV_COLUMNS = [
    "metric_source",
    "change_type",
    "doc_id",
    "security_code",
    "company_name",
    "industry_33",
    "filing_period_end",
    "metric_key",
    "metric_period_end",
    "before_value_num",
    "after_value_num",
    "before_source_tag",
    "after_source_tag",
    "before_consolidation",
    "after_consolidation",
    "before_calc_status",
    "after_calc_status",
    "before_formula_name",
    "after_formula_name",
    "candidate_validation_status",
    "candidate_validation_issues",
    "period_source",
    "period_fallback_used",
]


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


def _format_cell(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float):
        if not math.isfinite(value):
            return str(value)
        return format(value, ".17g")
    return str(value)


def _row_metric_source(row: dict[str, Any]) -> str:
    return str(row.get("metric_source") or "normalized_metrics")


def _row_key(row: dict[str, Any]) -> tuple[str, ...]:
    source = _row_metric_source(row)
    base_key = (
        source,
        str(row.get("doc_id") or ""),
        str(row.get("metric_key") or ""),
        str(row.get("period_end") or ""),
    )
    if source == "derived_metrics":
        return (*base_key, str(row.get("consolidation") or ""))
    return base_key


def _metric_row_map(rows: Iterable[dict[str, Any]]) -> dict[tuple[str, ...], dict[str, Any]]:
    result: dict[tuple[str, ...], dict[str, Any]] = {}
    for row in rows:
        result[_row_key(row)] = dict(row)
    return result


def _same_metric_value(before: dict[str, Any], after: dict[str, Any]) -> bool:
    if _row_metric_source(before) == "derived_metrics" or _row_metric_source(after) == "derived_metrics":
        return (
            _format_cell(before.get("value_num")) == _format_cell(after.get("value_num"))
            and str(before.get("calc_status") or "") == str(after.get("calc_status") or "")
            and str(before.get("formula_name") or "") == str(after.get("formula_name") or "")
            and str(before.get("value_unit") or "") == str(after.get("value_unit") or "")
            and str(before.get("consolidation") or "") == str(after.get("consolidation") or "")
        )
    return (
        _format_cell(before.get("value_num")) == _format_cell(after.get("value_num"))
        and str(before.get("source_tag") or "") == str(after.get("source_tag") or "")
        and str(before.get("consolidation") or "") == str(after.get("consolidation") or "")
    )


def compare_normalized_rows(
    *,
    current_rows: list[dict[str, Any]],
    recalculated_rows: list[dict[str, Any]],
    filing_by_doc_id: dict[str, dict[str, Any]],
    include_unchanged: bool = False,
) -> list[dict[str, str]]:
    current_by_key = _metric_row_map(current_rows)
    recalculated_by_key = _metric_row_map(recalculated_rows)
    all_keys = sorted(set(current_by_key) | set(recalculated_by_key))

    diff_rows: list[dict[str, str]] = []
    for key in all_keys:
        before = current_by_key.get(key)
        after = recalculated_by_key.get(key)
        source = after or before or {}
        filing = filing_by_doc_id.get(str(source.get("doc_id") or ""), {})

        if before and after:
            change_type = "unchanged" if _same_metric_value(before, after) else "changed"
        elif after:
            change_type = "added"
        else:
            change_type = "removed"

        if change_type == "unchanged" and not include_unchanged:
            continue

        diff_rows.append(
            {
                "metric_source": _format_cell(source.get("metric_source") or _row_metric_source(source)),
                "change_type": change_type,
                "doc_id": _format_cell(source.get("doc_id")),
                "security_code": _format_cell(filing.get("security_code") or source.get("security_code")),
                "company_name": _format_cell(filing.get("company_name")),
                "industry_33": _format_cell(filing.get("industry_33")),
                "filing_period_end": _format_cell(filing.get("period_end")),
                "metric_key": _format_cell(source.get("metric_key")),
                "metric_period_end": _format_cell(source.get("period_end")),
                "before_value_num": _format_cell(before.get("value_num") if before else ""),
                "after_value_num": _format_cell(after.get("value_num") if after else ""),
                "before_source_tag": _format_cell(before.get("source_tag") if before else ""),
                "after_source_tag": _format_cell(after.get("source_tag") if after else ""),
                "before_consolidation": _format_cell(before.get("consolidation") if before else ""),
                "after_consolidation": _format_cell(after.get("consolidation") if after else ""),
                "before_calc_status": _format_cell(before.get("calc_status") if before else ""),
                "after_calc_status": _format_cell(after.get("calc_status") if after else ""),
                "before_formula_name": _format_cell(before.get("formula_name") if before else ""),
                "after_formula_name": _format_cell(after.get("formula_name") if after else ""),
                "candidate_validation_status": _format_cell((after or {}).get("_candidate_validation_status")),
                "candidate_validation_issues": _format_cell((after or {}).get("_candidate_validation_issues")),
                "period_source": _format_cell((after or {}).get("_period_source")),
                "period_fallback_used": _format_cell((after or {}).get("_period_fallback_used")),
            }
        )
    return diff_rows


def summarize_impact_rows(rows: Iterable[dict[str, Any]]) -> dict[str, int]:
    counts = Counter(str(row.get("change_type") or "") for row in rows)
    return {
        "added": counts.get("added", 0),
        "removed": counts.get("removed", 0),
        "changed": counts.get("changed", 0),
        "unchanged": counts.get("unchanged", 0),
    }


def security_code_variants(security_code: str) -> list[str]:
    code = str(security_code or "").strip()
    if not code:
        return []
    variants = {code}
    if len(code) == 4:
        variants.add(f"{code}0")
    if len(code) == 5 and code.endswith("0"):
        variants.add(code[:-1])
    return sorted(variants)


def fetch_preview_scope_filings(
    conn: sqlite3.Connection,
    *,
    doc_ids: list[str],
    industry_33_list: list[str],
    security_codes: list[str],
    latest_only: bool,
    limit: int,
) -> list[dict[str, Any]]:
    security_variants: list[str] = []
    for code in security_codes:
        security_variants.extend(security_code_variants(code))
    security_variants = sorted(set(security_variants))

    where_clauses = ["f.form_type = '030000'"]
    params: list[Any] = []

    if doc_ids:
        placeholders = ",".join("?" for _ in doc_ids)
        where_clauses.append(f"f.doc_id IN ({placeholders})")
        params.extend(doc_ids)

    if industry_33_list:
        placeholders = ",".join("?" for _ in industry_33_list)
        where_clauses.append(f"im.industry_33 IN ({placeholders})")
        params.extend(industry_33_list)

    if security_variants:
        placeholders = ",".join("?" for _ in security_variants)
        where_clauses.append(f"COALESCE(f.security_code, im.security_code) IN ({placeholders})")
        params.extend(security_variants)

    where_sql = " AND ".join(where_clauses)
    if latest_only and not doc_ids:
        sql = f"""
        WITH scoped AS (
            SELECT
                f.doc_id,
                f.edinet_code,
                COALESCE(f.security_code, im.security_code) AS security_code,
                im.company_name,
                im.industry_33,
                f.form_type,
                f.period_end,
                f.submit_date,
                f.accounting_standard,
                f.document_display_unit,
                f.xbrl_path,
                f.zip_path,
                ROW_NUMBER() OVER (
                    PARTITION BY f.edinet_code
                    ORDER BY COALESCE(f.submit_date, '') DESC,
                             COALESCE(f.period_end, '') DESC,
                             f.doc_id DESC
                ) AS row_num
            FROM filings f
            INNER JOIN issuer_master im
                ON im.edinet_code = f.edinet_code
            WHERE {where_sql}
        )
        SELECT
            doc_id,
            edinet_code,
            security_code,
            company_name,
            industry_33,
            form_type,
            period_end,
            submit_date,
            accounting_standard,
            document_display_unit,
            xbrl_path,
            zip_path
        FROM scoped
        WHERE row_num = 1
        ORDER BY COALESCE(submit_date, '') DESC,
                 COALESCE(period_end, '') DESC,
                 doc_id DESC
        """
    else:
        sql = f"""
        SELECT
            f.doc_id,
            f.edinet_code,
            COALESCE(f.security_code, im.security_code) AS security_code,
            im.company_name,
            im.industry_33,
            f.form_type,
            f.period_end,
            f.submit_date,
            f.accounting_standard,
            f.document_display_unit,
            f.xbrl_path,
            f.zip_path
        FROM filings f
        INNER JOIN issuer_master im
            ON im.edinet_code = f.edinet_code
        WHERE {where_sql}
        ORDER BY COALESCE(f.submit_date, '') DESC,
                 COALESCE(f.period_end, '') DESC,
                 f.doc_id DESC
        """

    if limit > 0:
        sql += "\nLIMIT ?"
        params.append(limit)

    return [dict(row) for row in conn.execute(sql, tuple(params)).fetchall()]


def _table_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    return {
        str(row[1])
        for row in conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    }


def fetch_raw_fact_rows_for_preview(conn: sqlite3.Connection, doc_id: str) -> list[dict[str, Any]]:
    columns = _table_columns(conn, "raw_facts")
    wanted_columns = [
        "doc_id",
        "tag_name",
        "context_ref",
        "unit_ref",
        "period_type",
        "period_start",
        "period_end",
        "instant_date",
        "consolidation",
        "decimals",
        "is_nil",
        "context_dimensions_json",
        "unit_measures_json",
        "value_text",
    ]
    select_parts = [
        column if column in columns else f"'' AS {column}"
        for column in wanted_columns
    ]
    rows = conn.execute(
        f"""
        SELECT {", ".join(select_parts)}
        FROM raw_facts
        WHERE doc_id = ?
        """,
        (doc_id,),
    ).fetchall()
    return [dict(row) for row in rows]


def fetch_current_normalized_rows(conn: sqlite3.Connection, doc_ids: list[str]) -> list[dict[str, Any]]:
    if not doc_ids:
        return []
    placeholders = ",".join("?" for _ in doc_ids)
    rows = conn.execute(
        f"""
        SELECT
            doc_id,
            edinet_code,
            security_code,
            metric_key,
            fiscal_year,
            period_end,
            value_num,
            source_tag,
            consolidation,
            rule_version
        FROM normalized_metrics
        WHERE doc_id IN ({placeholders})
        ORDER BY doc_id, metric_key, COALESCE(period_end, '')
        """,
        tuple(doc_ids),
    ).fetchall()
    return [dict(row) for row in rows]


def fetch_current_derived_rows(conn: sqlite3.Connection, doc_ids: list[str]) -> list[dict[str, Any]]:
    if not doc_ids:
        return []
    placeholders = ",".join("?" for _ in doc_ids)
    rows = conn.execute(
        f"""
        SELECT
            doc_id,
            edinet_code,
            security_code,
            metric_key,
            metric_base,
            metric_group,
            fiscal_year,
            period_end,
            period_scope,
            period_offset,
            consolidation,
            accounting_standard,
            document_display_unit,
            value_num,
            value_unit,
            calc_status,
            formula_name,
            rule_version
        FROM derived_metrics
        WHERE doc_id IN ({placeholders})
        ORDER BY doc_id, metric_key, COALESCE(period_end, ''), COALESCE(consolidation, '')
        """,
        tuple(doc_ids),
    ).fetchall()
    result = [dict(row) for row in rows]
    for row in result:
        row["metric_source"] = "derived_metrics"
    return result


def recalculate_normalized_rows_for_preview(
    conn: sqlite3.Connection,
    filings: list[dict[str, Any]],
    *,
    enable_period_fallback: bool,
    enforce_candidate_validation: bool,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for filing in filings:
        raw_rows = fetch_raw_fact_rows_for_preview(conn, str(filing["doc_id"]))
        candidates = build_normalization_candidates(
            raw_rows,
            edinet_code=str(filing.get("edinet_code") or ""),
            security_code=str(filing.get("security_code") or ""),
            xbrl_path=str(filing.get("xbrl_path") or ""),
            zip_path=str(filing.get("zip_path") or ""),
            filing_period_end=str(filing.get("period_end") or ""),
            enable_period_fallback=enable_period_fallback,
            enforce_candidate_validation=enforce_candidate_validation,
        )
        rows.extend(select_best_normalization_candidates(candidates))
    return rows


def recalculate_derived_rows_for_preview(
    filings: list[dict[str, Any]],
    normalized_rows: list[dict[str, Any]],
    *,
    rule_version: str = DEFAULT_DERIVED_METRICS_RULE_VERSION,
) -> list[dict[str, Any]]:
    normalized_by_doc_id: dict[str, list[dict[str, Any]]] = {}
    for row in normalized_rows:
        normalized_by_doc_id.setdefault(str(row.get("doc_id") or ""), []).append(row)

    rows: list[dict[str, Any]] = []
    for filing in filings:
        doc_id = str(filing.get("doc_id") or "")
        derived_rows = calculate_derived_metrics(
            normalized_by_doc_id.get(doc_id, []),
            form_type=str(filing.get("form_type") or ""),
            accounting_standard=str(filing.get("accounting_standard") or ""),
            document_display_unit=str(filing.get("document_display_unit") or ""),
            rule_version=rule_version,
        )
        for row in derived_rows:
            row["metric_source"] = "derived_metrics"
        rows.extend(derived_rows)
    return rows


def build_normalization_impact_preview(
    conn: sqlite3.Connection,
    *,
    filings: list[dict[str, Any]],
    enable_period_fallback: bool,
    enforce_candidate_validation: bool,
    include_unchanged: bool = False,
    include_derived: bool = True,
) -> tuple[list[dict[str, str]], dict[str, int]]:
    filing_by_doc_id = {str(filing.get("doc_id") or ""): filing for filing in filings}
    doc_ids = sorted(filing_by_doc_id)
    current_rows = fetch_current_normalized_rows(conn, doc_ids)
    for row in current_rows:
        row["metric_source"] = "normalized_metrics"
    recalculated_rows = recalculate_normalized_rows_for_preview(
        conn,
        filings,
        enable_period_fallback=enable_period_fallback,
        enforce_candidate_validation=enforce_candidate_validation,
    )
    diff_rows = compare_normalized_rows(
        current_rows=current_rows,
        recalculated_rows=recalculated_rows,
        filing_by_doc_id=filing_by_doc_id,
        include_unchanged=include_unchanged,
    )
    current_derived_rows: list[dict[str, Any]] = []
    recalculated_derived_rows: list[dict[str, Any]] = []
    if include_derived:
        current_derived_rows = fetch_current_derived_rows(conn, doc_ids)
        recalculated_derived_rows = recalculate_derived_rows_for_preview(
            filings,
            recalculated_rows,
        )
        diff_rows.extend(
            compare_normalized_rows(
                current_rows=current_derived_rows,
                recalculated_rows=recalculated_derived_rows,
                filing_by_doc_id=filing_by_doc_id,
                include_unchanged=include_unchanged,
            )
        )
    summary = summarize_impact_rows(diff_rows)
    summary["target_docs"] = len(filings)
    summary["current_normalized_rows"] = len(current_rows)
    summary["recalculated_normalized_rows"] = len(recalculated_rows)
    summary["current_derived_rows"] = len(current_derived_rows)
    summary["recalculated_derived_rows"] = len(recalculated_derived_rows)
    summary["current_rows"] = summary["current_normalized_rows"] + summary["current_derived_rows"]
    summary["recalculated_rows"] = (
        summary["recalculated_normalized_rows"] + summary["recalculated_derived_rows"]
    )
    return diff_rows, summary


def write_impact_tsv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=IMPACT_TSV_COLUMNS,
            delimiter="\t",
            lineterminator="\n",
            extrasaction="ignore",
        )
        writer.writeheader()
        for row in rows:
            writer.writerow({column: _format_cell(row.get(column)) for column in IMPACT_TSV_COLUMNS})


def build_impact_text_report(
    *,
    rows: list[dict[str, Any]],
    summary: dict[str, int],
    enable_period_fallback: bool,
    enforce_candidate_validation: bool,
    include_unchanged: bool,
    include_derived: bool,
    scope_description: str,
    tsv_path: Path,
) -> list[str]:
    lines = [
        f"generated_at: {datetime.now().isoformat(timespec='seconds')}",
        "report: normalization_impact_preview",
        f"scope: {scope_description}",
        f"enable_period_fallback: {int(enable_period_fallback)}",
        f"enforce_candidate_validation: {int(enforce_candidate_validation)}",
        f"include_unchanged: {int(include_unchanged)}",
        f"include_derived: {int(include_derived)}",
        f"target_docs: {summary.get('target_docs', 0)}",
        f"current_rows: {summary.get('current_rows', 0)}",
        f"recalculated_rows: {summary.get('recalculated_rows', 0)}",
        f"current_normalized_rows: {summary.get('current_normalized_rows', 0)}",
        f"recalculated_normalized_rows: {summary.get('recalculated_normalized_rows', 0)}",
        f"current_derived_rows: {summary.get('current_derived_rows', 0)}",
        f"recalculated_derived_rows: {summary.get('recalculated_derived_rows', 0)}",
        f"added: {summary.get('added', 0)}",
        f"removed: {summary.get('removed', 0)}",
        f"changed: {summary.get('changed', 0)}",
        f"unchanged: {summary.get('unchanged', 0)}",
        f"tsv_path: {tsv_path}",
        "",
    ]
    columns = [
        ("metric_source", "source", 18, "left"),
        ("change_type", "change", 9, "left"),
        ("security_code", "code", 7, "left"),
        ("company_name", "company", 24, "left"),
        ("metric_key", "metric", 34, "left"),
        ("before_value_num", "before", 16, "right"),
        ("after_value_num", "after", 16, "right"),
        ("before_source_tag", "before_tag", 32, "left"),
        ("after_source_tag", "after_tag", 32, "left"),
        ("candidate_validation_status", "validation", 10, "left"),
    ]
    lines.append(" | ".join(pad_right(label, width) for _, label, width, _ in columns))
    preview_rows = rows[:200]
    if not preview_rows:
        lines.append("diff_rows=none")
    for row in preview_rows:
        parts = []
        for key, _, width, align in columns:
            value = row.get(key, "")
            parts.append(pad_left(value, width) if align == "right" else pad_right(value, width))
        lines.append(" | ".join(parts))
    if len(rows) > len(preview_rows):
        lines.append(f"... truncated in txt: {len(rows) - len(preview_rows)} rows. See TSV for all rows.")
    return lines
