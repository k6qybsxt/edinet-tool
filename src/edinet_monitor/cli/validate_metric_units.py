from __future__ import annotations

import argparse
from pathlib import Path

from edinet_monitor.db.schema import get_connection
from edinet_monitor.services.metric_audit_service import (
    build_unit_validation_report,
    fetch_filing,
    fetch_raw_fact_audit_rows,
    now_stamp,
    validate_unit_decimals,
    write_text_report,
)
from edinet_pipeline.domain.metric_labels import split_metric_key


DEFAULT_OUTPUT_DIR = str(Path("D:/") / "\u4f5c\u696d\u7528")


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Validate unitRef, decimals, and schema type consistency for one filing."
    )
    parser.add_argument("--doc-id", default="")
    parser.add_argument("--security-code", default="")
    parser.add_argument("--period-end", default="")
    parser.add_argument("--metric-key", default="")
    parser.add_argument("--metric-base", default="")
    parser.add_argument("--all-periods", action="store_true")
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR)
    return parser


def main() -> None:
    args = build_arg_parser().parse_args()
    if not args.doc_id.strip() and (not args.security_code.strip() or not args.period_end.strip()):
        raise SystemExit("--doc-id or --security-code and --period-end are required")
    metric_base = args.metric_base.strip()
    metric_key = args.metric_key.strip()
    if not metric_base and metric_key:
        metric_base = split_metric_key(metric_key)[0]

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

    rows = validate_unit_decimals(
        filing=filing,
        raw_rows=raw_rows,
        metric_base=metric_base,
        metric_key=metric_key,
        all_periods=args.all_periods,
    )
    lines = build_unit_validation_report(
        filing=filing,
        rows=rows,
        metric_base=metric_base,
        metric_key=metric_key,
    )
    output_path = Path(args.output_dir) / f"unit_decimals_validation_{filing['doc_id']}_{now_stamp()}.txt"
    write_text_report(output_path, lines)
    warning_count = sum(1 for row in rows if row.get("status") == "WARNING")
    info_count = sum(1 for row in rows if row.get("status") == "INFO")
    ok_count = sum(1 for row in rows if row.get("status") == "OK")
    print(f"saved: {output_path}")
    print(f"rows={len(rows)}")
    print(f"warnings={warning_count}")
    print(f"infos={info_count}")
    print(f"oks={ok_count}")


if __name__ == "__main__":
    main()
