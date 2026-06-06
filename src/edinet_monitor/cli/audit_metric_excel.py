from __future__ import annotations

import argparse
from pathlib import Path
import sqlite3

from edinet_monitor.config.settings import DB_PATH, OPERATION_LOG_ROOT
from edinet_monitor.services.metric_excel_audit_service import (
    DEFAULT_TARGET_CONFIG_PATH,
    ExcelAuditOptions,
    audit_metric_excel,
)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Audit an existing metric Excel workbook against monitor DB-derived expected rows."
    )
    parser.add_argument("--excel-path", required=True)
    parser.add_argument("--db-path", default=str(DB_PATH))
    parser.add_argument("--target-set", default="normal", choices=["normal", "known_issue", "all"])
    parser.add_argument("--target-config", default=str(DEFAULT_TARGET_CONFIG_PATH))
    parser.add_argument("--output-dir", default=str(OPERATION_LOG_ROOT / "excel_audit"))
    parser.add_argument("--limit-preview", type=int, default=10)
    return parser


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
        result = audit_metric_excel(
            conn,
            ExcelAuditOptions(
                excel_path=Path(args.excel_path),
                db_path=db_path,
                target_set=str(args.target_set),
                target_config_path=Path(args.target_config),
                output_dir=Path(args.output_dir),
            ),
        )
    finally:
        conn.close()

    print(f"audit_id={result.audit_id}")
    print(f"target_set={result.target_set}")
    print(f"target_count={len(result.targets)}")
    print(f"expected_rows={result.expected_rows}")
    print(f"actual_rows={result.actual_rows}")
    print(f"issue_count={result.issue_count}")
    print(f"critical={result.counts_by_severity.get('critical', 0)}")
    print(f"warning={result.counts_by_severity.get('warning', 0)}")
    print(f"json_path={result.json_path}")
    print(f"excel_path={result.report_excel_path}")
    preview_limit = max(int(args.limit_preview), 0)
    for issue in result.issues[:preview_limit]:
        print(
            "issue="
            f"{issue.severity}|{issue.category}|{issue.check_name}|{issue.security_code}|"
            f"{issue.metric_label}|{issue.period_label}|{issue.message}"
        )


if __name__ == "__main__":
    main()
