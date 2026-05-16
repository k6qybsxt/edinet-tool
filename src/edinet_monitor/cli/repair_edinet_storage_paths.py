from __future__ import annotations

import argparse
from collections import Counter
from pathlib import Path

from edinet_monitor.config.settings import OPERATION_LOG_ROOT
from edinet_monitor.db.schema import create_tables, get_connection
from edinet_monitor.services.edinet_storage_path_service import repair_storage_paths
from edinet_monitor.services.segment_scope_service import fetch_segment_scope_filings


def _split_csv(value: str | None) -> list[str]:
    text = str(value or "").strip()
    if not text or text.lower() == "all":
        return []
    return [part.strip() for part in text.split(",") if part.strip()]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Repair EDINET zip/xbrl paths after storage-root moves.")
    parser.add_argument("--doc-id", default="", help="Comma-separated doc_id list.")
    parser.add_argument("--codes", default="all", help="Comma-separated security codes, or all.")
    parser.add_argument("--form-codes", default="030000,043A00")
    parser.add_argument("--period-ranks", default="latest,5,10")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--output-dir", default=str(OPERATION_LOG_ROOT))
    return parser


def _write_report(actions, *, output_dir: str | Path, apply: bool) -> Path:
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    from datetime import datetime

    path = out_dir / f"edinet_storage_path_repair_{'apply' if apply else 'dry_run'}_{datetime.now():%Y%m%d_%H%M%S}.tsv"
    headers = [
        "doc_id",
        "zip_resolved",
        "xbrl_resolved",
        "old_zip_path",
        "new_zip_path",
        "old_xbrl_path",
        "new_xbrl_path",
    ]
    lines = ["\t".join(headers)]
    for action in actions:
        payload = action.__dict__
        lines.append("\t".join(str(payload.get(header, "") or "") for header in headers))
    path.write_text("\n".join(lines) + "\n", encoding="utf-8-sig")
    return path


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
        actions = repair_storage_paths(conn, filings, apply=args.apply)
    finally:
        conn.close()

    counts = Counter(
        (
            "both"
            if action.zip_resolved and action.xbrl_resolved
            else "zip_only"
            if action.zip_resolved
            else "xbrl_only"
            if action.xbrl_resolved
            else "missing"
        )
        for action in actions
    )
    output_path = _write_report(actions, output_dir=args.output_dir, apply=args.apply)
    print(f"mode={'apply' if args.apply else 'dry_run'}")
    print(f"target_docs={len(actions)}")
    print(f"zip_resolved={sum(1 for action in actions if action.zip_resolved)}")
    print(f"xbrl_resolved={sum(1 for action in actions if action.xbrl_resolved)}")
    print(f"both_resolved={counts.get('both', 0)}")
    print(f"missing={counts.get('missing', 0)}")
    print(f"output_path={output_path}")
    if not args.apply:
        print("dry_run_only=1")


if __name__ == "__main__":
    main()
