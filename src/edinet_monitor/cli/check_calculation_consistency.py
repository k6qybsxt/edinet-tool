from __future__ import annotations

import argparse
from pathlib import Path

from edinet_monitor.db.schema import get_connection
from edinet_monitor.services.metric_audit_service import (
    build_calculation_consistency_report,
    check_calculation_consistency,
    fetch_filing,
    fetch_raw_fact_audit_rows,
    now_stamp,
    write_text_report,
)


DEFAULT_OUTPUT_DIR = str(Path("D:/") / "\u4f5c\u696d\u7528")


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Check calculation linkbase consistency for one filing."
    )
    parser.add_argument("--doc-id", default="")
    parser.add_argument("--security-code", default="")
    parser.add_argument("--period-end", default="")
    parser.add_argument("--metric-base", default="")
    parser.add_argument("--tolerance-ratio", type=float, default=0.01)
    parser.add_argument("--tolerance-abs", type=float, default=1.0)
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR)
    return parser


def main() -> None:
    args = build_arg_parser().parse_args()
    if not args.doc_id.strip() and (not args.security_code.strip() or not args.period_end.strip()):
        raise SystemExit("--doc-id or --security-code and --period-end are required")

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

    rows = check_calculation_consistency(
        filing=filing,
        raw_rows=raw_rows,
        metric_base=args.metric_base.strip(),
        tolerance_ratio=args.tolerance_ratio,
        tolerance_abs=args.tolerance_abs,
        limit=args.limit,
    )
    lines = build_calculation_consistency_report(
        filing=filing,
        rows=rows,
        metric_base=args.metric_base.strip(),
        tolerance_ratio=args.tolerance_ratio,
        tolerance_abs=args.tolerance_abs,
    )
    output_path = Path(args.output_dir) / f"calculation_consistency_{filing['doc_id']}_{now_stamp()}.txt"
    write_text_report(output_path, lines)
    warning_count = sum(1 for row in rows if row.get("status") == "WARNING")
    ok_count = sum(1 for row in rows if row.get("status") == "OK")
    skipped_count = sum(1 for row in rows if row.get("status") == "SKIPPED")
    print(f"saved: {output_path}")
    print(f"rows={len(rows)}")
    print(f"warnings={warning_count}")
    print(f"oks={ok_count}")
    print(f"skipped={skipped_count}")


if __name__ == "__main__":
    main()
