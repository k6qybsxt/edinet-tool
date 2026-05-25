from __future__ import annotations

import argparse

from edinet_monitor.config.settings import OPERATION_LOG_ROOT
from edinet_monitor.db.schema import get_connection
from edinet_monitor.services.jquants_official_cli_compare_service import (
    OfficialCliCompareError,
    run_jquants_official_cli_compare,
)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Compare official jquants CLI output with locally stored J-Quants raw DB rows."
    )
    parser.add_argument("--endpoint", required=True, choices=["fins.summary", "eq.daily", "fins.details"])
    parser.add_argument("--date", dest="date_value", default=None)
    parser.add_argument("--code", default=None)
    parser.add_argument("--official-cli", default=None)
    parser.add_argument("--output-dir", default=str(OPERATION_LOG_ROOT))
    return parser


def main() -> None:
    args = build_arg_parser().parse_args()
    conn = get_connection()
    try:
        result = run_jquants_official_cli_compare(
            conn,
            endpoint=args.endpoint,
            date_value=args.date_value,
            code=args.code,
            output_dir=args.output_dir,
            official_cli=args.official_cli,
        )
    except OfficialCliCompareError as exc:
        raise SystemExit(str(exc)) from exc
    finally:
        conn.close()
    print(f"endpoint={result.endpoint}")
    print(f"official_rows={result.official_rows}")
    print(f"db_rows={result.db_rows}")
    print(f"matched_rows={result.matched_rows}")
    print(f"missing_in_db={result.missing_in_db}")
    print(f"extra_in_db={result.extra_in_db}")
    print(f"field_diff_rows={result.field_diff_rows}")
    print(f"diff_count={result.diff_count}")
    print(f"txt_path={result.txt_path}")
    print(f"tsv_path={result.tsv_path}")


if __name__ == "__main__":
    main()
