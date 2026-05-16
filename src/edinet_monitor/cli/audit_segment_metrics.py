from __future__ import annotations

import argparse

from edinet_monitor.config.settings import OPERATION_LOG_ROOT
from edinet_monitor.db.schema import get_connection
from edinet_monitor.services.segment_metric_service import (
    build_segment_metric_rows,
    write_segment_metric_report,
)


def _split_csv(value: str | None) -> list[str]:
    text = str(value or "").strip()
    if not text or text.lower() == "all":
        return []
    return [part.strip() for part in text.split(",") if part.strip()]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Audit candidate EDINET XBRL segment metrics without writing DB rows."
    )
    parser.add_argument("--doc-id", default="", help="Comma-separated doc_id list.")
    parser.add_argument("--codes", default="all", help="Comma-separated security codes, or all.")
    parser.add_argument("--date-from", default=None)
    parser.add_argument("--date-to", default=None)
    parser.add_argument("--form-codes", default="030000,043A00")
    parser.add_argument("--output-dir", default=str(OPERATION_LOG_ROOT))
    return parser


def main() -> None:
    args = build_parser().parse_args()
    conn = get_connection()
    try:
        result = build_segment_metric_rows(
            conn,
            doc_ids=_split_csv(args.doc_id),
            codes=_split_csv(args.codes),
            date_from=args.date_from,
            date_to=args.date_to,
            form_codes=_split_csv(args.form_codes),
        )
        output_path = write_segment_metric_report(
            result=result,
            output_dir=args.output_dir,
            mode="audit",
            date_from=args.date_from,
            date_to=args.date_to,
        )
    finally:
        conn.close()

    selected_count = sum(1 for item in result.candidates if item.status == "selected")
    print("mode=audit")
    print(f"rows={len(result.rows)}")
    print(f"selected_candidates={selected_count}")
    print(f"candidates={len(result.candidates)}")
    print(f"warnings={len(result.warnings)}")
    print(f"output_path={output_path}")


if __name__ == "__main__":
    main()

