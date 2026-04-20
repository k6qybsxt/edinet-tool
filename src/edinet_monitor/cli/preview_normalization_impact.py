from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path

from edinet_monitor.config.settings import DB_PATH
from edinet_monitor.services.normalization_impact_service import (
    build_impact_text_report,
    build_normalization_impact_preview,
    fetch_preview_scope_filings,
    now_stamp,
    write_impact_tsv,
)


DEFAULT_OUTPUT_DIR = str(Path("D:/") / "\u4f5c\u696d\u7528" / "normalization_impact")


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Preview normalized_metrics changes without updating the database."
    )
    parser.add_argument("--doc-id", action="append", dest="doc_ids", default=[])
    parser.add_argument("--security-code", action="append", dest="security_codes", default=[])
    parser.add_argument("--industry-33", action="append", dest="industry_33_list", default=[])
    parser.add_argument("--latest-only", action="store_true")
    parser.add_argument("--limit", type=int, default=20)
    parser.add_argument("--allow-all", action="store_true")
    parser.add_argument("--enable-period-fallback", action="store_true")
    parser.add_argument("--enforce-candidate-validation", action="store_true")
    parser.add_argument("--include-unchanged", action="store_true")
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--db-path", default=str(DB_PATH))
    return parser


def _connect_readonly(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only = ON")
    conn.execute("PRAGMA busy_timeout = 30000")
    return conn


def _scope_description(args: argparse.Namespace) -> str:
    parts = []
    if args.doc_ids:
        parts.append(f"doc_id={','.join(args.doc_ids)}")
    if args.security_codes:
        parts.append(f"security_code={','.join(args.security_codes)}")
    if args.industry_33_list:
        parts.append(f"industry_33={','.join(args.industry_33_list)}")
    if args.latest_only:
        parts.append("latest_only=1")
    parts.append(f"limit={args.limit}")
    return " | ".join(parts)


def _validate_scope(args: argparse.Namespace) -> None:
    has_filter = bool(args.doc_ids or args.security_codes or args.industry_33_list)
    if has_filter:
        return
    if not args.allow_all:
        raise SystemExit("Specify --doc-id, --security-code, or --industry-33. Use --allow-all only with a small --limit.")
    if args.limit <= 0:
        raise SystemExit("--allow-all requires --limit > 0")


def main() -> None:
    args = build_arg_parser().parse_args()
    _validate_scope(args)

    db_path = Path(args.db_path)
    output_dir = Path(args.output_dir)
    timestamp = now_stamp()
    output_dir.mkdir(parents=True, exist_ok=True)
    txt_path = output_dir / f"normalization_impact_{timestamp}.txt"
    tsv_path = output_dir / f"normalization_impact_{timestamp}.tsv"

    conn = _connect_readonly(db_path)
    try:
        filings = fetch_preview_scope_filings(
            conn,
            doc_ids=[str(x).strip() for x in args.doc_ids if str(x).strip()],
            industry_33_list=[str(x).strip() for x in args.industry_33_list if str(x).strip()],
            security_codes=[str(x).strip() for x in args.security_codes if str(x).strip()],
            latest_only=args.latest_only,
            limit=args.limit,
        )
        diff_rows, summary = build_normalization_impact_preview(
            conn,
            filings=filings,
            enable_period_fallback=args.enable_period_fallback,
            enforce_candidate_validation=args.enforce_candidate_validation,
            include_unchanged=args.include_unchanged,
        )
    finally:
        conn.close()

    write_impact_tsv(tsv_path, diff_rows)
    lines = build_impact_text_report(
        rows=diff_rows,
        summary=summary,
        enable_period_fallback=args.enable_period_fallback,
        enforce_candidate_validation=args.enforce_candidate_validation,
        include_unchanged=args.include_unchanged,
        scope_description=_scope_description(args),
        tsv_path=tsv_path,
    )
    txt_path.write_text("\n".join(lines) + "\n", encoding="utf-8-sig")

    print(f"saved_txt={txt_path}")
    print(f"saved_tsv={tsv_path}")
    print(f"target_docs={summary.get('target_docs', 0)}")
    print(f"added={summary.get('added', 0)}")
    print(f"removed={summary.get('removed', 0)}")
    print(f"changed={summary.get('changed', 0)}")
    print(f"unchanged={summary.get('unchanged', 0)}")


if __name__ == "__main__":
    main()
