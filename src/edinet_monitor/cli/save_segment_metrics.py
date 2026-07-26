from __future__ import annotations

import argparse

from edinet_monitor.config.settings import OPERATION_LOG_ROOT
from edinet_monitor.db.schema import create_tables, get_connection
from edinet_monitor.services.db_reflection_preflight_guard_service import (
    mark_db_reflection_preflight_guard_success,
    run_db_reflection_preflight_guard,
)
from edinet_monitor.services.doc_id_file_service import load_doc_ids_file, normalize_doc_ids
from edinet_monitor.services.segment_metric_service import save_segment_metrics
from edinet_monitor.services.segment_scope_service import fetch_segment_scope_filings


def _split_csv(value: str | None) -> list[str]:
    text = str(value or "").strip()
    if not text or text.lower() == "all":
        return []
    return [part.strip() for part in text.split(",") if part.strip()]


def _resolve_doc_ids(doc_id: str | None, doc_ids_file: str | None) -> tuple[str, ...]:
    file_doc_ids = load_doc_ids_file(doc_ids_file) if doc_ids_file else ()
    return normalize_doc_ids(doc_id, file_doc_ids)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build EDINET XBRL segment metrics from dimensioned raw_facts."
    )
    parser.add_argument("--doc-id", default="", help="Comma-separated doc_id list.")
    parser.add_argument("--doc-ids-file", default="", help="UTF-8 text file with one doc ID per line.")
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
    doc_ids = _resolve_doc_ids(args.doc_id or None, args.doc_ids_file or None)
    if args.period_ranks and doc_ids:
        raise ValueError("--period-ranks cannot be combined with --doc-id or --doc-ids-file.")
    guard_result = None
    if args.apply:
        guard_result = run_db_reflection_preflight_guard(
            cli_name="save_segment_metrics"
        )
    if args.apply:
        create_tables()
    conn = get_connection()
    try:
        if args.period_ranks:
            scope_rows = fetch_segment_scope_filings(
                conn,
                form_codes=_split_csv(args.form_codes),
                period_ranks=args.period_ranks,
                codes=_split_csv(args.codes),
                doc_ids=doc_ids,
            )
            doc_ids = tuple(str(row["doc_id"]) for row in scope_rows)
            if not doc_ids:
                doc_ids = ("__segment_scope_empty__",)
        result = save_segment_metrics(
            conn,
            doc_ids=list(doc_ids),
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
    print(f"rows={getattr(result, 'built_row_count', len(result.rows))}")
    print(f"candidates={getattr(result, 'candidate_count', len(result.candidates))}")
    print(f"target_doc_ids={len(doc_ids)}")
    print(f"saved_rows={result.saved_rows}")
    print(f"replaced_doc_ids={getattr(result, 'replaced_doc_count', 0)}")
    print(f"warnings={len(result.warnings)}")
    print(f"output_path={result.output_path}")
    if args.apply and doc_ids and result.saved_rows == 0:
        raise RuntimeError("No segment metrics were saved for the requested doc IDs.")
    if args.apply:
        mark_db_reflection_preflight_guard_success(guard_result)
    if not args.apply:
        print("dry_run_only=1")
        print("hint=DBへ保存する場合は --apply を付けてください。")


if __name__ == "__main__":
    main()
