from __future__ import annotations

import argparse
from pathlib import Path
import sqlite3

from edinet_monitor.config.settings import DB_PATH
from edinet_monitor.services.jquants_spec_review_service import (
    DEFAULT_JQUANTS_SCHEMA_BASELINE_DIR,
    DEFAULT_JQUANTS_SPEC_REVIEW_OUTPUT_DIR,
    JQuantsSpecReviewOptions,
    build_jquants_spec_review,
)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Review J-Quants official CLI schema changes and local raw DB differences."
    )
    parser.add_argument("--db-path", default=str(DB_PATH))
    parser.add_argument("--endpoints", default="fins.summary,eq.daily")
    parser.add_argument("--date", dest="date_value", default="")
    parser.add_argument("--code", default="")
    parser.add_argument("--baseline-dir", default=str(DEFAULT_JQUANTS_SCHEMA_BASELINE_DIR))
    parser.add_argument("--output-dir", default=str(DEFAULT_JQUANTS_SPEC_REVIEW_OUTPUT_DIR))
    parser.add_argument("--official-cli", default=None)
    parser.add_argument("--update-baseline", action="store_true")
    parser.add_argument("--limit-preview", type=int, default=10)
    return parser


def _split_endpoints(value: str) -> tuple[str, ...]:
    return tuple(part.strip() for part in str(value or "").split(",") if part.strip())


def _get_read_only_connection(db_path: Path) -> sqlite3.Connection:
    uri = db_path.resolve().as_uri() + "?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only = ON")
    return conn


def main() -> None:
    args = build_arg_parser().parse_args()
    db_path = Path(args.db_path)
    conn = _get_read_only_connection(db_path)
    try:
        result = build_jquants_spec_review(
            conn,
            JQuantsSpecReviewOptions(
                endpoints=_split_endpoints(args.endpoints),
                date_value=str(args.date_value or "").strip(),
                code=str(args.code or "").strip(),
                baseline_dir=Path(args.baseline_dir),
                output_dir=Path(args.output_dir),
                official_cli=args.official_cli,
                update_baseline=bool(args.update_baseline),
            ),
        )
    finally:
        conn.close()

    print(f"review_id={result.review_id}")
    print(f"status={result.status}")
    print(f"critical={result.counts_by_severity.get('critical', 0)}")
    print(f"warning={result.counts_by_severity.get('warning', 0)}")
    print(f"json_path={result.json_path}")
    print(f"excel_path={result.excel_path}")
    preview_limit = max(int(args.limit_preview), 0)
    for issue in result.issues[:preview_limit]:
        print(
            "issue="
            f"{issue.severity}|{issue.category}|{issue.check_name}|{issue.endpoint}|"
            f"{issue.field_name}|{issue.message}"
        )


if __name__ == "__main__":
    main()
