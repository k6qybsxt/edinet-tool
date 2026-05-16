from __future__ import annotations

import argparse
from collections import Counter

from edinet_monitor.config.settings import OPERATION_LOG_ROOT
from edinet_monitor.db.schema import create_tables, get_connection
from edinet_monitor.services.segment_raw_rebuild_service import rebuild_segment_raw_facts
from edinet_monitor.services.segment_scope_service import fetch_segment_scope_filings


def _split_csv(value: str | None) -> list[str]:
    text = str(value or "").strip()
    if not text or text.lower() == "all":
        return []
    return [part.strip() for part in text.split(",") if part.strip()]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Re-extract target XBRL files and rebuild raw_facts for segment analysis only."
    )
    parser.add_argument("--doc-id", default="", help="Comma-separated doc_id list.")
    parser.add_argument("--codes", default="all", help="Comma-separated security codes, or all.")
    parser.add_argument("--form-codes", default="030000,043A00")
    parser.add_argument("--period-ranks", default="latest,5,10")
    parser.add_argument("--force-extract", action="store_true")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--output-dir", default=str(OPERATION_LOG_ROOT))
    return parser


def main() -> None:
    args = build_parser().parse_args()
    if args.apply:
        create_tables()
    conn = get_connection()
    try:
        filings = fetch_segment_scope_filings(
            conn,
            form_codes=_split_csv(args.form_codes),
            period_ranks=args.period_ranks,
            codes=_split_csv(args.codes),
            doc_ids=_split_csv(args.doc_id),
        )
        result = rebuild_segment_raw_facts(
            conn,
            filings=filings,
            apply=args.apply,
            force_extract=args.force_extract,
            output_dir=args.output_dir,
        )
    finally:
        conn.close()

    counts = Counter(row.status for row in result.rows)
    print(f"mode={'apply' if args.apply else 'dry_run'}")
    print(f"target_docs={len(result.rows)}")
    for status, count in sorted(counts.items()):
        print(f"status_{status}={count}")
    print(f"raw_rows={sum(row.raw_rows for row in result.rows)}")
    print(f"dimension_rows={sum(row.dimension_rows for row in result.rows)}")
    print(f"output_path={result.output_path}")
    if not args.apply:
        print("dry_run_only=1")


if __name__ == "__main__":
    main()
