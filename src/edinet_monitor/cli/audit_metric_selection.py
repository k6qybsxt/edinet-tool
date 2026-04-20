from __future__ import annotations

import argparse
from pathlib import Path

from edinet_monitor.db.schema import get_connection
from edinet_monitor.services.metric_audit_service import (
    build_metric_audit_report,
    build_metric_audit_rows,
    fetch_filing,
    fetch_raw_fact_audit_rows,
    now_stamp,
    write_text_report,
)
from edinet_pipeline.domain.metric_labels import split_metric_key


DEFAULT_OUTPUT_DIR = str(Path("D:/") / "\u4f5c\u696d\u7528")


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Audit why normalized metric candidates were selected for one filing."
    )
    parser.add_argument("--doc-id", default="")
    parser.add_argument("--security-code", default="")
    parser.add_argument("--period-end", default="")
    parser.add_argument("--metric-key", default="")
    parser.add_argument("--metric-base", default="")
    parser.add_argument("--all-periods", action="store_true")
    parser.add_argument("--enforce-candidate-validation", action="store_true")
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR)
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

    candidates, selected = build_metric_audit_rows(
        filing=filing,
        raw_rows=raw_rows,
        metric_base=metric_base,
        enforce_candidate_validation=args.enforce_candidate_validation,
    )
    lines = build_metric_audit_report(
        filing=filing,
        candidates=candidates,
        selected=selected,
        metric_base=metric_base,
        all_periods=args.all_periods,
        target_metric_key=args.metric_key.strip(),
    )
    output_path = Path(args.output_dir) / f"metric_selection_audit_{filing['doc_id']}_{metric_base}_{now_stamp()}.txt"
    write_text_report(output_path, lines)
    print(f"saved: {output_path}")
    print(f"candidate_count={len(candidates)}")
    print(f"selected_count={len(selected)}")


if __name__ == "__main__":
    main()
