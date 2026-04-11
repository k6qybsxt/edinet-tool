from __future__ import annotations

import argparse
from pathlib import Path

from edinet_monitor.config.settings import (
    ZIP_BACKFILL_CHUNK_LOG_PATH,
    ZIP_BACKFILL_RUN_LOG_PATH,
)
from edinet_monitor.db.schema import create_tables, get_connection
from edinet_monitor.services.storage.pipeline_log_import_service import (
    import_zip_backfill_run_logs,
)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--run-log-path",
        default=str(ZIP_BACKFILL_RUN_LOG_PATH),
    )
    parser.add_argument(
        "--chunk-log-path",
        default=str(ZIP_BACKFILL_CHUNK_LOG_PATH),
    )
    return parser


def main() -> None:
    args = build_arg_parser().parse_args()
    create_tables()
    conn = get_connection()
    try:
        summary = import_zip_backfill_run_logs(
            conn,
            run_log_path=Path(args.run_log_path),
            chunk_log_path=Path(args.chunk_log_path),
        )
    finally:
        conn.close()

    print(f"run_log_path={summary['run_log_path']}")
    print(f"chunk_log_path={summary['chunk_log_path']}")
    print(f"run_rows={summary['run_rows']}")
    print(f"chunk_rows={summary['chunk_rows']}")
    print(f"inserted_runs={summary['inserted_runs']}")
    print(f"inserted_chunks={summary['inserted_chunks']}")


if __name__ == "__main__":
    main()
