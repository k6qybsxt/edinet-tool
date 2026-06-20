from __future__ import annotations

import argparse
from pathlib import Path
import sqlite3

from edinet_monitor.config.settings import DB_PATH
from edinet_monitor.db.schema import get_connection
from edinet_monitor.services.db_reflection_preflight_service import (
    DEFAULT_DB_REFLECTION_PREFLIGHT_OUTPUT_DIR,
    DbReflectionPreflightOptions,
    build_db_reflection_preflight,
)
from edinet_monitor.services.preflight_history_service import save_preflight_history
from edinet_monitor.services.prevention_catalog_service import DEFAULT_PREVENTION_CATALOG_PATH


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build a report-only preflight review for pending DB reflection items."
    )
    parser.add_argument("--db-path", default=str(DB_PATH))
    parser.add_argument("--catalog-path", default=str(DEFAULT_PREVENTION_CATALOG_PATH))
    parser.add_argument("--item-id", type=int, default=None)
    parser.add_argument("--output-dir", default=str(DEFAULT_DB_REFLECTION_PREFLIGHT_OUTPUT_DIR))
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
        result = build_db_reflection_preflight(
            conn,
            DbReflectionPreflightOptions(
                db_path=db_path,
                catalog_path=Path(args.catalog_path),
                item_id=args.item_id,
                output_dir=Path(args.output_dir),
            ),
        )
    finally:
        conn.close()

    history_conn = get_connection(db_path)
    try:
        history_save = save_preflight_history(history_conn, result, report_only=True)
    finally:
        history_conn.close()

    print(f"preflight_id={result.preflight_id}")
    print(f"status={result.status}")
    print(f"pipeline_failure_policy={result.summary.get('pipeline_failure_policy', '')}")
    print(f"db_reflection_blocked={result.summary.get('db_reflection_blocked', False)}")
    print(f"pending_count={len(result.pending_items)}")
    print(f"critical={result.counts_by_severity.get('critical', 0)}")
    print(f"warning={result.counts_by_severity.get('warning', 0)}")
    print(f"info={result.counts_by_severity.get('info', 0)}")
    print(f"json_path={result.json_path}")
    print(f"excel_path={result.excel_path}")
    print(f"history_saved={history_save.history_saved}")
    print(f"history_status={history_save.status}")
    print(f"history_preflight_id={history_save.preflight_id}")
    preview_limit = max(int(args.limit_preview), 0)
    for issue in result.issues[:preview_limit]:
        print(
            "issue="
            f"{issue.severity}|{issue.category}|{issue.check_name}|"
            f"{issue.item_id}|{issue.title}|{issue.message}"
        )


if __name__ == "__main__":
    main()
