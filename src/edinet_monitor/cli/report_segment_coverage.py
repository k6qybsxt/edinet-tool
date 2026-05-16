from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import datetime
from pathlib import Path
import sqlite3
import unicodedata
from typing import Any

from edinet_monitor.config.settings import OPERATION_LOG_ROOT
from edinet_monitor.db.schema import get_connection
from edinet_monitor.services.collector.document_filter_service import normalize_form_codes
from edinet_monitor.services.segment_scope_service import fetch_segment_scope_filings, parse_period_rank_specs


def _split_csv(value: str | None) -> list[str]:
    text = str(value or "").strip()
    if not text or text.lower() == "all":
        return []
    return [part.strip() for part in text.split(",") if part.strip()]


def _display_width(text: Any) -> int:
    return sum(2 if unicodedata.east_asian_width(ch) in ("F", "W", "A") else 1 for ch in str(text))


def _pad_right(text: Any, width: int) -> str:
    text = str(text)
    return text + " " * max(0, width - _display_width(text))


def _pad_left(text: Any, width: int) -> str:
    text = str(text)
    return " " * max(0, width - _display_width(text)) + text


def _table(rows: list[dict[str, Any]], columns: list[tuple[str, str, str]]) -> list[str]:
    widths = {
        key: max([_display_width(label)] + [_display_width(row.get(key, "")) for row in rows])
        for key, label, _ in columns
    }
    lines = [
        " | ".join(
            _pad_left(label, widths[key]) if align == "right" else _pad_right(label, widths[key])
            for key, label, align in columns
        ),
        "-+-".join("-" * widths[key] for key, _, _ in columns),
    ]
    for row in rows:
        lines.append(
            " | ".join(
                _pad_left(row.get(key, ""), widths[key]) if align == "right" else _pad_right(row.get(key, ""), widths[key])
                for key, _, align in columns
            )
        )
    return lines


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Report segment metric coverage for selected filing ranks.")
    parser.add_argument("--doc-id", default="")
    parser.add_argument("--codes", default="all")
    parser.add_argument("--form-codes", default="030000,043A00")
    parser.add_argument("--period-ranks", default="latest,5,10")
    parser.add_argument("--output-dir", default=str(OPERATION_LOG_ROOT))
    return parser


def _fetch_segment_rows(conn: sqlite3.Connection, doc_ids: list[str]) -> list[sqlite3.Row]:
    if not doc_ids:
        return []
    rows: list[sqlite3.Row] = []
    for start in range(0, len(doc_ids), 500):
        chunk = doc_ids[start : start + 500]
        placeholders = ",".join("?" for _ in chunk)
        rows.extend(
            conn.execute(
                f"""
                SELECT doc_id, segment_kind, metric_base, COUNT(*) AS rows
                FROM segment_metrics
                WHERE doc_id IN ({placeholders})
                GROUP BY doc_id, segment_kind, metric_base
                """,
                chunk,
            ).fetchall()
        )
    return rows


def main() -> None:
    args = build_parser().parse_args()
    conn = get_connection()
    conn.row_factory = sqlite3.Row
    try:
        filings = fetch_segment_scope_filings(
            conn,
            form_codes=_split_csv(args.form_codes),
            period_ranks=args.period_ranks,
            codes=_split_csv(args.codes),
            doc_ids=_split_csv(args.doc_id),
        )
        segment_rows = _fetch_segment_rows(conn, [str(row["doc_id"]) for row in filings])
    finally:
        conn.close()

    rows_by_doc: dict[str, list[sqlite3.Row]] = defaultdict(list)
    for row in segment_rows:
        rows_by_doc[str(row["doc_id"])].append(row)

    summary: dict[tuple[str, str], dict[str, Any]] = {}
    for form_code in normalize_form_codes(_split_csv(args.form_codes)):
        form_label = "半期" if form_code in {"043A00", "043000"} else "有報"
        for spec in parse_period_rank_specs(args.period_ranks):
            summary[(form_label, spec.label)] = {
                "form": form_label,
                "rank": spec.label,
                "target_docs": 0,
                "docs_with_segment": 0,
                "segment_rows": 0,
            }
    by_kind: dict[tuple[str, str, str], dict[str, Any]] = {}
    by_metric: dict[tuple[str, str, str], dict[str, Any]] = {}
    by_industry: dict[tuple[str, str, str], dict[str, Any]] = {}
    missing: list[dict[str, Any]] = []

    for filing in filings:
        form_label = "有報" if str(filing.get("form_group") or filing.get("form_type")) == "030000" else "半期"
        rank_label = str(filing.get("period_rank_label") or "doc_id")
        doc_id = str(filing["doc_id"])
        doc_segment_rows = rows_by_doc.get(doc_id, [])
        key = (form_label, rank_label)
        item = summary.setdefault(key, {"form": form_label, "rank": rank_label, "target_docs": 0, "docs_with_segment": 0, "segment_rows": 0})
        item["target_docs"] += 1
        if doc_segment_rows:
            item["docs_with_segment"] += 1
            item["segment_rows"] += sum(int(row["rows"] or 0) for row in doc_segment_rows)
        else:
            missing.append(
                {
                    "form": form_label,
                    "rank": rank_label,
                    "security_code": filing.get("security_code", ""),
                    "company_name": filing.get("company_name", ""),
                    "industry": filing.get("industry_33", ""),
                    "period_end": filing.get("period_end", ""),
                    "doc_id": doc_id,
                }
            )
        industry_key = (form_label, rank_label, str(filing.get("industry_33") or ""))
        industry_item = by_industry.setdefault(
            industry_key,
            {"form": form_label, "rank": rank_label, "industry": filing.get("industry_33", ""), "target_docs": 0, "docs_with_segment": 0, "segment_rows": 0},
        )
        industry_item["target_docs"] += 1
        if doc_segment_rows:
            industry_item["docs_with_segment"] += 1
            industry_item["segment_rows"] += sum(int(row["rows"] or 0) for row in doc_segment_rows)
        for segment_row in doc_segment_rows:
            kind_key = (form_label, rank_label, str(segment_row["segment_kind"]))
            kind_item = by_kind.setdefault(kind_key, {"form": form_label, "rank": rank_label, "kind": segment_row["segment_kind"], "docs": set(), "rows": 0})
            kind_item["docs"].add(doc_id)
            kind_item["rows"] += int(segment_row["rows"] or 0)
            metric_key = (form_label, rank_label, str(segment_row["metric_base"]))
            metric_item = by_metric.setdefault(metric_key, {"form": form_label, "rank": rank_label, "metric": segment_row["metric_base"], "docs": set(), "rows": 0})
            metric_item["docs"].add(doc_id)
            metric_item["rows"] += int(segment_row["rows"] or 0)

    summary_rows = []
    for item in summary.values():
        total = int(item["target_docs"] or 0)
        hit = int(item["docs_with_segment"] or 0)
        summary_rows.append({**item, "coverage": f"{(hit / total * 100 if total else 0):.1f}%"})
    kind_rows = [{**item, "docs": len(item["docs"])} for item in by_kind.values()]
    metric_rows = [{**item, "docs": len(item["docs"])} for item in by_metric.values()]
    industry_rows = []
    for item in by_industry.values():
        total = int(item["target_docs"] or 0)
        hit = int(item["docs_with_segment"] or 0)
        industry_rows.append({**item, "coverage": f"{(hit / total * 100 if total else 0):.1f}%"})

    sort_key = lambda row: (str(row.get("form", "")), str(row.get("rank", "")), str(row.get("industry", row.get("metric", row.get("kind", "")))))
    summary_rows.sort(key=sort_key)
    kind_rows.sort(key=sort_key)
    metric_rows.sort(key=sort_key)
    industry_rows.sort(key=sort_key)

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    txt_path = output_dir / f"segment_coverage_{timestamp}.txt"
    tsv_path = output_dir / f"segment_coverage_{timestamp}.tsv"

    lines = [
        f"generated_at: {datetime.now().isoformat(timespec='seconds')}",
        f"form_codes: {args.form_codes}",
        f"period_ranks: {args.period_ranks}",
        f"target_docs: {len(filings)}",
        "",
        "=== summary ===",
    ]
    lines += _table(summary_rows, [("form", "対象", "left"), ("rank", "期間", "left"), ("target_docs", "対象doc", "right"), ("docs_with_segment", "取得doc", "right"), ("coverage", "取得率", "right"), ("segment_rows", "行数", "right")])
    lines += ["", "=== by kind ==="]
    lines += _table(kind_rows, [("form", "対象", "left"), ("rank", "期間", "left"), ("kind", "区分", "left"), ("docs", "doc数", "right"), ("rows", "行数", "right")])
    lines += ["", "=== by metric ==="]
    lines += _table(metric_rows, [("form", "対象", "left"), ("rank", "期間", "left"), ("metric", "指標", "left"), ("docs", "doc数", "right"), ("rows", "行数", "right")])
    lines += ["", "=== by industry ==="]
    lines += _table(industry_rows, [("form", "対象", "left"), ("rank", "期間", "left"), ("industry", "業種", "left"), ("target_docs", "対象doc", "right"), ("docs_with_segment", "取得doc", "right"), ("coverage", "取得率", "right"), ("segment_rows", "行数", "right")])
    lines += ["", "=== missing sample max 100 ==="]
    lines += _table(missing[:100], [("form", "対象", "left"), ("rank", "期間", "left"), ("security_code", "証券コード", "left"), ("company_name", "企業名", "left"), ("industry", "業種", "left"), ("period_end", "期末日", "left"), ("doc_id", "doc_id", "left")])
    txt_path.write_text("\n".join(lines) + "\n", encoding="utf-8-sig")

    tsv_headers = ["section", "form", "rank", "key", "target_docs", "docs", "coverage", "rows"]
    tsv_lines = ["\t".join(tsv_headers)]
    for row in summary_rows:
        tsv_lines.append("\t".join(["summary", row["form"], row["rank"], "", str(row["target_docs"]), str(row["docs_with_segment"]), row["coverage"], str(row["segment_rows"])]))
    for row in kind_rows:
        tsv_lines.append("\t".join(["kind", row["form"], row["rank"], str(row["kind"]), "", str(row["docs"]), "", str(row["rows"])]))
    for row in metric_rows:
        tsv_lines.append("\t".join(["metric", row["form"], row["rank"], str(row["metric"]), "", str(row["docs"]), "", str(row["rows"])]))
    for row in industry_rows:
        tsv_lines.append("\t".join(["industry", row["form"], row["rank"], str(row["industry"]), str(row["target_docs"]), str(row["docs_with_segment"]), row["coverage"], str(row["segment_rows"])]))
    tsv_path.write_text("\n".join(tsv_lines) + "\n", encoding="utf-8-sig")

    print(f"target_docs={len(filings)}")
    print(f"txt_path={txt_path}")
    print(f"tsv_path={tsv_path}")
    for row in summary_rows:
        print(f"{row['form']} {row['rank']}: {row['docs_with_segment']}/{row['target_docs']} {row['coverage']}")


if __name__ == "__main__":
    main()
