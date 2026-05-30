from __future__ import annotations

import argparse
from pathlib import Path

from edinet_monitor.config.settings import DB_PATH
from edinet_monitor.db.schema import create_tables, get_connection
from edinet_monitor.services.performance_log_service import (
    PerformanceRun,
    list_performance_runs,
)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Show EDINET monitor CLI performance logs.")
    parser.add_argument("--db-path", default=str(DB_PATH))
    subparsers = parser.add_subparsers(dest="command", required=True)

    list_parser = subparsers.add_parser("list")
    list_parser.add_argument("--command-name", default="")
    list_parser.add_argument("--limit", type=int, default=10)

    latest_parser = subparsers.add_parser("latest")
    latest_parser.add_argument("--command-name", required=True)
    return parser


def _connect(db_path: str):
    path = Path(db_path)
    create_tables(path)
    return get_connection(path)


def _print_run(row: PerformanceRun) -> None:
    print(
        "\t".join(
            [
                row.run_id,
                row.command_name,
                row.status,
                row.started_at,
                str(round(row.elapsed_seconds, 3)),
                str(row.target_total),
                str(row.success_total),
                str(row.error_total),
                str(round(row.processed_per_minute, 3)),
            ]
        )
    )


def _print_latest(latest: PerformanceRun, previous: PerformanceRun | None) -> None:
    print(f"run_id={latest.run_id}")
    print(f"command_name={latest.command_name}")
    print(f"status={latest.status}")
    print(f"started_at={latest.started_at}")
    print(f"elapsed_seconds={round(latest.elapsed_seconds, 3)}")
    print(f"target_total={latest.target_total}")
    print(f"success_total={latest.success_total}")
    print(f"error_total={latest.error_total}")
    print(f"processed_per_minute={round(latest.processed_per_minute, 3)}")
    print(f"db_read_elapsed_seconds={round(latest.db_read_elapsed_seconds, 3)}")
    print(f"parse_elapsed_seconds={round(latest.parse_elapsed_seconds, 3)}")
    print(f"compute_elapsed_seconds={round(latest.compute_elapsed_seconds, 3)}")
    print(f"db_write_elapsed_seconds={round(latest.db_write_elapsed_seconds, 3)}")
    print(f"file_io_elapsed_seconds={round(latest.file_io_elapsed_seconds, 3)}")

    if previous is None:
        print("previous_run_id=")
        print("elapsed_delta_seconds=")
        print("processed_per_minute_delta=")
        print("error_delta=")
        return

    print(f"previous_run_id={previous.run_id}")
    print(f"previous_elapsed_seconds={round(previous.elapsed_seconds, 3)}")
    print(f"previous_processed_per_minute={round(previous.processed_per_minute, 3)}")
    print(f"previous_error_total={previous.error_total}")
    print(f"elapsed_delta_seconds={round(latest.elapsed_seconds - previous.elapsed_seconds, 3)}")
    print(
        "processed_per_minute_delta="
        f"{round(latest.processed_per_minute - previous.processed_per_minute, 3)}"
    )
    print(f"error_delta={latest.error_total - previous.error_total}")


def main() -> None:
    args = build_arg_parser().parse_args()
    conn = _connect(args.db_path)
    try:
        if args.command == "list":
            rows = list_performance_runs(
                conn,
                command_name=args.command_name,
                limit=args.limit,
            )
            if not rows:
                print("no_runs=1")
                return
            print(
                "run_id\tcommand_name\tstatus\tstarted_at\telapsed_seconds\t"
                "target_total\tsuccess_total\terror_total\tprocessed_per_minute"
            )
            for row in rows:
                _print_run(row)
        elif args.command == "latest":
            rows = list_performance_runs(conn, command_name=args.command_name, limit=2)
            if not rows:
                print("no_runs=1")
                return
            _print_latest(rows[0], rows[1] if len(rows) > 1 else None)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
