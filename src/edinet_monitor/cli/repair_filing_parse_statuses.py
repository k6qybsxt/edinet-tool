from __future__ import annotations

import argparse

from edinet_monitor.db.schema import create_tables, get_connection
from edinet_monitor.services.collector.filing_parse_status_repair_service import (
    repair_filing_parse_statuses,
)


def build_arg_parser() -> argparse.ArgumentParser:
    return argparse.ArgumentParser()


def main() -> None:
    build_arg_parser().parse_args()
    create_tables()
    conn = get_connection()
    try:
        summary = repair_filing_parse_statuses(conn)
    finally:
        conn.close()

    print(f"checked_total={summary['checked_total']}")
    print(f"updated_total={summary['updated_total']}")
    if summary["status_change_totals"]:
        for status_change, count in summary["status_change_totals"].items():
            print(f"status_change={status_change}|{count}")
    else:
        print("status_change=none")


if __name__ == "__main__":
    main()
