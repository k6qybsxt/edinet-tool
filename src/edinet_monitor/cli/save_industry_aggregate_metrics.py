from __future__ import annotations

import argparse

from edinet_monitor.config.settings import DEFAULT_DERIVED_METRICS_RULE_VERSION
from edinet_monitor.db.schema import create_tables, get_connection
from edinet_monitor.services.industry_aggregate_metric_service import (
    build_industry_aggregate_metric_rows,
    count_industry_aggregate_metrics,
    replace_industry_aggregate_metrics,
    write_industry_aggregate_report,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="TSE上場企業の通期データから業種のみ集計指標を作成します。"
    )
    parser.add_argument("--apply", action="store_true", help="指定した場合だけDBへ保存します。")
    parser.add_argument("--output-dir", default=r"D:\作業用")
    parser.add_argument("--rule-version", default=DEFAULT_DERIVED_METRICS_RULE_VERSION)
    return parser


def main() -> None:
    args = build_parser().parse_args()
    create_tables()
    conn = get_connection()
    try:
        before_counts = count_industry_aggregate_metrics(conn)
        build_result = build_industry_aggregate_metric_rows(
            conn,
            rule_version=args.rule_version,
        )
        after_counts = None
        if args.apply:
            saved_count = replace_industry_aggregate_metrics(conn, build_result.rows)
            after_counts = count_industry_aggregate_metrics(conn)
        else:
            saved_count = 0

        mode = "apply" if args.apply else "dry_run"
        report_path = write_industry_aggregate_report(
            output_dir=args.output_dir,
            mode=mode,
            build_result=build_result,
            before_counts=before_counts,
            after_counts=after_counts,
        )
    finally:
        conn.close()

    print(f"mode={mode}")
    print(f"source_company_count={build_result.source_company_count}")
    print(f"industry_count={build_result.industry_count}")
    print(f"fiscal_year_count={build_result.fiscal_year_count}")
    print(f"built_rows={len(build_result.rows)}")
    print(f"built_ok_rows={build_result.ok_count}")
    print(f"built_missing_rows={build_result.missing_count}")
    print(f"saved_rows={saved_count}")
    print(f"report_path={report_path}")
    if not args.apply:
        print("dry_run_only=1")
        print("hint=DBへ保存する場合は --apply を付けてください。")


if __name__ == "__main__":
    main()
