from __future__ import annotations

import argparse
from pathlib import Path
import sqlite3

from edinet_monitor.config.settings import DB_PATH
from edinet_monitor.services.daily_review_service import (
    DEFAULT_DAILY_REVIEW_OUTPUT_DIR,
    DEFAULT_DAILY_REVIEW_RETENTION_COUNT,
    DEFAULT_KNOWN_ISSUE_GOLDEN_JSON_PATH,
    DEFAULT_NORMAL_GOLDEN_JSON_PATH,
    DailyReviewOptions,
    build_daily_review,
)
from edinet_monitor.services.metric_excel_audit_service import DEFAULT_TARGET_CONFIG_PATH


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build a read-only daily pipeline review report after the daily run."
    )
    parser.add_argument("--db-path", default=str(DB_PATH))
    parser.add_argument("--normal-excel", default="")
    parser.add_argument("--known-issue-excel", default="")
    parser.add_argument("--normal-golden-json", default=str(DEFAULT_NORMAL_GOLDEN_JSON_PATH))
    parser.add_argument("--known-issue-golden-json", default=str(DEFAULT_KNOWN_ISSUE_GOLDEN_JSON_PATH))
    parser.add_argument("--target-config", default=str(DEFAULT_TARGET_CONFIG_PATH))
    parser.add_argument("--output-dir", default=str(DEFAULT_DAILY_REVIEW_OUTPUT_DIR))
    parser.add_argument("--retention-count", type=int, default=DEFAULT_DAILY_REVIEW_RETENTION_COUNT)
    parser.add_argument("--limit-preview", type=int, default=20)
    parser.add_argument("--skip-excel-audit", action="store_true")
    parser.add_argument("--skip-golden-master-diff", action="store_true")
    return parser


def _get_read_only_connection(db_path: Path) -> sqlite3.Connection:
    uri = db_path.resolve().as_uri() + "?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only = ON")
    return conn


def _optional_path(value: str) -> Path | None:
    clean = str(value or "").strip()
    return Path(clean) if clean else None


def main() -> None:
    args = build_arg_parser().parse_args()
    db_path = Path(args.db_path)
    conn = _get_read_only_connection(db_path)
    try:
        result = build_daily_review(
            conn,
            DailyReviewOptions(
                db_path=db_path,
                normal_excel_path=_optional_path(args.normal_excel),
                known_issue_excel_path=_optional_path(args.known_issue_excel),
                normal_golden_json_path=Path(args.normal_golden_json),
                known_issue_golden_json_path=Path(args.known_issue_golden_json),
                target_config_path=Path(args.target_config),
                output_dir=Path(args.output_dir),
                retention_count=int(args.retention_count),
                issue_preview_limit=max(int(args.limit_preview), 0),
                run_excel_audit=not bool(args.skip_excel_audit),
                run_golden_master_diff=not bool(args.skip_golden_master_diff),
            ),
        )
    finally:
        conn.close()

    summary = result.summary
    print(f"review_id={result.review_id}")
    print(f"status={result.status}")
    print(f"pipeline_failure_policy={summary.get('pipeline_failure_policy', '')}")
    print(f"pipeline_failed={summary.get('pipeline_failed', False)}")
    print(f"schema_missing={summary.get('schema_missing_count', 0)}")
    print(f"db_reflection_pending={summary.get('db_reflection_pending_count', 0)}")
    print(f"data_quality_critical={summary.get('data_quality_critical_count', 0)}")
    print(f"data_quality_warning={summary.get('data_quality_warning_count', 0)}")
    print(f"excel_audit_critical={summary.get('excel_audit_critical_count', 0)}")
    print(f"excel_audit_warning={summary.get('excel_audit_warning_count', 0)}")
    print(f"golden_master_critical={summary.get('golden_master_critical_count', 0)}")
    print(f"golden_master_warning={summary.get('golden_master_warning_count', 0)}")
    print(f"review_error_count={summary.get('review_error_count', 0)}")
    print(f"json_path={result.json_path}")
    print(f"excel_path={result.excel_path}")


if __name__ == "__main__":
    main()
