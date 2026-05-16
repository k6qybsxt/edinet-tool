from __future__ import annotations

import argparse

from edinet_monitor.config.settings import OPERATION_LOG_ROOT
from edinet_monitor.db.schema import create_tables, get_connection
from edinet_monitor.services.segment_metric_service import save_segment_metrics
from edinet_monitor.services.segment_scope_service import fetch_segment_scope_filings


def _split_csv(value: str | None) -> list[str]:
    text = str(value or "").strip()
    if not text or text.lower() == "all":
        return []
    return [part.strip() for part in text.split(",") if part.strip()]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build EDINET XBRL segment metrics from dimensioned raw_facts."
    )
    parser.add_argument("--doc-id", default="", help="Comma-separated doc_id list.")
    parser.add_argument("--codes", default="all", help="Comma-separated security codes, or all.")
    parser.add_argument("--date-from", default=None)
    parser.add_argument("--date-to", default=None)
    parser.add_argument("--form-codes", default="030000,043A00")
    parser.add_argument("--period-ranks", default="", help="Comma-separated: latest,5,10")
    parser.add_argument("--apply", action="store_true", help="Save rows to DB. Omit for dry-run.")
    parser.add_argument("--output-dir", default=str(OPERATION_LOG_ROOT))
    return parser


def main() -> None:
    args = build_parser().parse_args()
    if args.apply:
        create_tables()
    conn = get_connection()
    try:
        doc_ids = _split_csv(args.doc_id)
        if args.period_ranks:
            scope_rows = fetch_segment_scope_filings(
                conn,
                form_codes=_split_csv(args.form_codes),
                period_ranks=args.period_ranks,
                codes=_split_csv(args.codes),
                doc_ids=doc_ids,
            )
            doc_ids = [str(row["doc_id"]) for row in scope_rows]
            if not doc_ids:
                doc_ids = ["__segment_scope_empty__"]
        result = save_segment_metrics(
            conn,
            doc_ids=doc_ids,
            codes=[] if args.period_ranks else _split_csv(args.codes),
            date_from=args.date_from,
            date_to=args.date_to,
            form_codes=_split_csv(args.form_codes),
            apply=args.apply,
            output_dir=args.output_dir,
        )
    finally:
        conn.close()

    print(f"mode={'apply' if args.apply else 'dry_run'}")
    print(f"rows={len(result.rows)}")
    print(f"candidates={len(result.candidates)}")
    print(f"saved_rows={result.saved_rows}")
    print(f"warnings={len(result.warnings)}")
    print(f"output_path={result.output_path}")
    if not args.apply:
        print("dry_run_only=1")
        print("hint=DBへ保存する場合は --apply を付けてください。")


if __name__ == "__main__":
    main()
