from __future__ import annotations

import csv
import json
import sqlite3
import unicodedata
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any

from edinet_pipeline.domain.metric_labels import metric_key_to_display_name


CHANGE_FILES = [
    ("added", "added_rows.tsv"),
    ("removed", "removed_rows.tsv"),
    ("value_changed", "value_changes.tsv"),
    ("full_changed_same_value", "full_changes_same_value.tsv"),
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


def _read_tsv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle, delimiter="\t")]


def load_comparison_rows(comparison_dir: Path) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for change_type, file_name in CHANGE_FILES:
        for row in _read_tsv(comparison_dir / file_name):
            item = dict(row)
            item["change_type"] = change_type
            rows.append(item)
    return rows


def load_comparison_summary(comparison_dir: Path) -> dict[str, Any]:
    path = comparison_dir / "comparison_summary.json"
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def fetch_filing_metadata_by_doc_id(
    conn: sqlite3.Connection,
    doc_ids: list[str],
) -> dict[str, dict[str, str]]:
    doc_ids = sorted({str(doc_id).strip() for doc_id in doc_ids if str(doc_id).strip()})
    if not doc_ids:
        return {}
    placeholders = ",".join("?" for _ in doc_ids)
    rows = conn.execute(
        f"""
        SELECT
            f.doc_id,
            COALESCE(f.security_code, im.security_code) AS security_code,
            im.company_name,
            im.industry_33,
            f.period_end,
            f.submit_date
        FROM filings f
        LEFT JOIN issuer_master im
            ON im.edinet_code = f.edinet_code
        WHERE f.doc_id IN ({placeholders})
        """,
        tuple(doc_ids),
    ).fetchall()
    return {str(row["doc_id"]): dict(row) for row in rows}


def enrich_comparison_rows(
    rows: list[dict[str, str]],
    filing_by_doc_id: dict[str, dict[str, str]],
) -> list[dict[str, str]]:
    enriched: list[dict[str, str]] = []
    for row in rows:
        item = dict(row)
        filing = filing_by_doc_id.get(str(item.get("doc_id") or ""), {})
        industry = str(filing.get("industry_33") or "")
        item["security_code"] = str(filing.get("security_code") or item.get("security_code") or "")
        item["company_name"] = str(filing.get("company_name") or "")
        item["industry_33"] = industry
        item["filing_period_end"] = str(filing.get("period_end") or "")
        item["metric_label"] = metric_key_to_display_name(item.get("metric_key"), industry)
        enriched.append(item)
    return enriched


def summarize_rows(rows: list[dict[str, str]]) -> dict[str, Counter[str]]:
    return {
        "by_change_type": Counter(str(row.get("change_type") or "") for row in rows),
        "by_source": Counter(str(row.get("source") or "") for row in rows),
        "by_doc_id": Counter(str(row.get("doc_id") or "") for row in rows),
        "by_metric_key": Counter(str(row.get("metric_key") or "") for row in rows),
    }


def _table_header(columns: list[tuple[str, str, int, str]]) -> str:
    return " | ".join(pad_right(label, width) for _, label, width, _ in columns)


def _table_row(columns: list[tuple[str, str, int, str]], row: dict[str, Any]) -> str:
    parts = []
    for key, _, width, align in columns:
        value = row.get(key, "")
        parts.append(pad_left(value, width) if align == "right" else pad_right(value, width))
    return " | ".join(parts)


def build_snapshot_comparison_review(
    *,
    comparison_dir: Path,
    summary: dict[str, Any],
    rows: list[dict[str, str]],
    detail_limit: int = 300,
) -> list[str]:
    counters = summarize_rows(rows)
    generated_at = datetime.now().isoformat(timespec="seconds")
    lines = [
        f"generated_at: {generated_at}",
        "report: metric_snapshot_comparison_review",
        f"comparison_dir: {comparison_dir}",
        f"before_dir: {summary.get('before_dir', '')}",
        f"after_dir: {summary.get('after_dir', '')}",
        f"before_row_count: {summary.get('before_row_count', '')}",
        f"after_row_count: {summary.get('after_row_count', '')}",
        f"added_count: {summary.get('added_count', counters['by_change_type'].get('added', 0))}",
        f"removed_count: {summary.get('removed_count', counters['by_change_type'].get('removed', 0))}",
        f"value_changed_count: {summary.get('value_changed_count', counters['by_change_type'].get('value_changed', 0))}",
        "full_changed_same_value_count: "
        f"{summary.get('full_changed_same_value_count', counters['by_change_type'].get('full_changed_same_value', 0))}",
        "",
    ]
    if not rows:
        lines.append("result: OK - 差分はありません。")
        return lines

    lines.append("=== change_type summary ===")
    for key, count in counters["by_change_type"].most_common():
        lines.append(f"{pad_right(key, 28)} {pad_left(count, 8)}")
    lines.append("")

    lines.append("=== source summary ===")
    for key, count in counters["by_source"].most_common():
        lines.append(f"{pad_right(key, 28)} {pad_left(count, 8)}")
    lines.append("")

    lines.append("=== top docs ===")
    for doc_id, count in counters["by_doc_id"].most_common(30):
        sample = next((row for row in rows if row.get("doc_id") == doc_id), {})
        company = sample.get("company_name", "")
        code = sample.get("security_code", "")
        lines.append(f"{pad_right(code, 8)} {pad_right(company, 30)} {pad_right(doc_id, 14)} {pad_left(count, 8)}")
    lines.append("")

    lines.append("=== top metrics ===")
    for metric_key, count in counters["by_metric_key"].most_common(50):
        sample = next((row for row in rows if row.get("metric_key") == metric_key), {})
        lines.append(f"{pad_right(metric_key, 42)} {pad_right(sample.get('metric_label', ''), 30)} {pad_left(count, 8)}")
    lines.append("")

    lines.append("=== details ===")
    columns = [
        ("change_type", "change", 24, "left"),
        ("source", "source", 18, "left"),
        ("security_code", "code", 7, "left"),
        ("company_name", "company", 24, "left"),
        ("metric_key", "metric", 34, "left"),
        ("metric_label", "label", 26, "left"),
        ("period_end", "period", 10, "left"),
        ("before_value_num", "before", 16, "right"),
        ("after_value_num", "after", 16, "right"),
        ("before_calc_status", "before_status", 13, "left"),
        ("after_calc_status", "after_status", 12, "left"),
        ("before_source_tag", "before_tag", 28, "left"),
        ("after_source_tag", "after_tag", 28, "left"),
    ]
    lines.append(_table_header(columns))
    for row in rows[: max(detail_limit, 0)]:
        lines.append(_table_row(columns, row))
    if len(rows) > detail_limit:
        lines.append(f"... truncated: {len(rows) - detail_limit} rows")
    return lines


def write_review_report(path: Path, lines: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8-sig")
