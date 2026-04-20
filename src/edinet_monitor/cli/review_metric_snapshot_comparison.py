from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path

from edinet_monitor.config.settings import DB_PATH
from edinet_monitor.services.metric_snapshot_review_service import (
    build_snapshot_comparison_review,
    enrich_comparison_rows,
    fetch_filing_metadata_by_doc_id,
    load_comparison_rows,
    load_comparison_summary,
    now_stamp,
    write_review_report,
)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Create a readable Japanese-label review report for metric snapshot comparison results."
    )
    parser.add_argument("--comparison-dir", required=True)
    parser.add_argument("--output-dir", default="")
    parser.add_argument("--detail-limit", type=int, default=300)
    parser.add_argument("--db-path", default=str(DB_PATH))
    parser.add_argument("--fail-on-any-change", action="store_true")
    parser.add_argument("--fail-on-value-change", action="store_true")
    parser.add_argument("--fail-on-added-removed", action="store_true")
    return parser


def _connect_readonly(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only = ON")
    conn.execute("PRAGMA busy_timeout = 30000")
    return conn


def _should_fail(
    *,
    args: argparse.Namespace,
    added_count: int,
    removed_count: int,
    value_changed_count: int,
    full_changed_same_value_count: int,
) -> bool:
    if args.fail_on_any_change:
        return any([added_count, removed_count, value_changed_count, full_changed_same_value_count])
    if args.fail_on_value_change and value_changed_count:
        return True
    if args.fail_on_added_removed and (added_count or removed_count):
        return True
    return False


def main() -> None:
    args = build_arg_parser().parse_args()
    comparison_dir = Path(args.comparison_dir)
    if not comparison_dir.exists():
        raise SystemExit(f"comparison_dir_not_found: {comparison_dir}")

    rows = load_comparison_rows(comparison_dir)
    summary = load_comparison_summary(comparison_dir)
    doc_ids = [str(row.get("doc_id") or "") for row in rows]

    conn = _connect_readonly(Path(args.db_path))
    try:
        filing_by_doc_id = fetch_filing_metadata_by_doc_id(conn, doc_ids)
    finally:
        conn.close()

    enriched_rows = enrich_comparison_rows(rows, filing_by_doc_id)
    output_dir = Path(args.output_dir) if args.output_dir else comparison_dir
    output_path = output_dir / f"metric_snapshot_comparison_review_{now_stamp()}.txt"
    lines = build_snapshot_comparison_review(
        comparison_dir=comparison_dir,
        summary=summary,
        rows=enriched_rows,
        detail_limit=args.detail_limit,
    )
    write_review_report(output_path, lines)

    added_count = int(summary.get("added_count") or 0)
    removed_count = int(summary.get("removed_count") or 0)
    value_changed_count = int(summary.get("value_changed_count") or 0)
    full_changed_same_value_count = int(summary.get("full_changed_same_value_count") or 0)

    print(f"saved={output_path}")
    print(f"rows={len(enriched_rows)}")
    print(f"added_count={added_count}")
    print(f"removed_count={removed_count}")
    print(f"value_changed_count={value_changed_count}")
    print(f"full_changed_same_value_count={full_changed_same_value_count}")

    if _should_fail(
        args=args,
        added_count=added_count,
        removed_count=removed_count,
        value_changed_count=value_changed_count,
        full_changed_same_value_count=full_changed_same_value_count,
    ):
        raise SystemExit(2)


if __name__ == "__main__":
    main()
