from __future__ import annotations

import argparse
from pathlib import Path

from edinet_monitor.db.schema import get_connection
from edinet_monitor.services.metric_audit_service import (
    build_extension_candidate_report,
    discover_extension_tag_candidates,
    fetch_filing,
    fetch_raw_fact_audit_rows,
    now_stamp,
    write_text_report,
)
from edinet_pipeline.domain.metric_labels import split_metric_key


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Discover unmapped extension tag candidates for one filing and metric."
    )
    parser.add_argument("--doc-id", default="")
    parser.add_argument("--security-code", default="")
    parser.add_argument("--period-end", default="")
    parser.add_argument("--metric-key", default="")
    parser.add_argument("--metric-base", default="")
    parser.add_argument("--include-mapped", action="store_true")
    parser.add_argument("--all-periods", action="store_true")
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument("--output-dir", default=r"D:\作業用")
    return parser


def main() -> None:
    args = build_arg_parser().parse_args()
    metric_base = args.metric_base.strip()
    if not metric_base and args.metric_key:
        metric_base = split_metric_key(args.metric_key)[0]
    if not metric_base:
        raise SystemExit("--metric-base or --metric-key is required")

    conn = get_connection()
    try:
        filing = fetch_filing(
            conn,
            doc_id=args.doc_id,
            security_code=args.security_code,
            period_end=args.period_end,
        )
        if not filing:
            raise SystemExit("target_filing_not_found")
        raw_rows = fetch_raw_fact_audit_rows(conn, str(filing["doc_id"]))
    finally:
        conn.close()

    candidates = discover_extension_tag_candidates(
        filing=filing,
        raw_rows=raw_rows,
        metric_base=metric_base,
        include_mapped=args.include_mapped,
        current_only=not args.all_periods,
        limit=args.limit,
    )
    lines = build_extension_candidate_report(
        filing=filing,
        metric_base=metric_base,
        rows=candidates,
    )
    output_path = Path(args.output_dir) / f"extension_tag_candidates_{filing['doc_id']}_{metric_base}_{now_stamp()}.txt"
    write_text_report(output_path, lines)
    print(f"saved: {output_path}")
    print(f"candidate_count={len(candidates)}")


if __name__ == "__main__":
    main()
