from __future__ import annotations

import argparse
from typing import Iterable

from edinet_monitor.config.settings import DB_PATH
from edinet_monitor.db.schema import create_tables, get_connection
from edinet_monitor.services.summary_view_service import (
    fetch_latest_filing_status_rows,
    fetch_metric_coverage_rows,
    fetch_monthly_collection_status_rows,
    fetch_screening_hit_summary_rows,
    fetch_table_counts,
)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--latest-limit", type=int, default=10)
    parser.add_argument("--month-limit", type=int, default=12)
    parser.add_argument("--metric-limit", type=int, default=20)
    parser.add_argument(
        "--metric-source",
        choices=["all", "normalized_metrics", "derived_metrics"],
        default="all",
    )
    parser.add_argument("--metric-key-like", default="")
    parser.add_argument("--screening-limit", type=int, default=10)
    parser.add_argument(
        "--all-issuers",
        action="store_true",
        help="Include issuers even if listed flag is off.",
    )
    return parser


def print_section(title: str) -> None:
    print(f"[{title}]")


def print_rows(rows: Iterable[str]) -> None:
    for row in rows:
        print(row)


def main() -> None:
    args = build_arg_parser().parse_args()
    create_tables()

    conn = get_connection()
    try:
        counts = fetch_table_counts(conn)
        latest_rows = fetch_latest_filing_status_rows(
            conn,
            limit=args.latest_limit,
            listed_only=not args.all_issuers,
        )
        monthly_rows = fetch_monthly_collection_status_rows(
            conn,
            limit=args.month_limit,
        )
        metric_rows = fetch_metric_coverage_rows(
            conn,
            metric_source=None if args.metric_source == "all" else args.metric_source,
            metric_key_like=args.metric_key_like or None,
            limit=args.metric_limit,
        )
        screening_rows = fetch_screening_hit_summary_rows(
            conn,
            limit=args.screening_limit,
        )

        print(f"db_path={DB_PATH}")

        print_section("table_counts")
        for table_name, count in counts.items():
            print(f"{table_name}={count}")

        print_section("issuer_latest_filing_status")
        if not latest_rows:
            print("none")
        else:
            print_rows(
                (
                    " | ".join(
                        [
                            f"company={row['company_name']}",
                            f"edinet={row['edinet_code']}",
                            f"doc_id={row['doc_id'] or ''}",
                            f"submit_date={row['submit_date'] or ''}",
                            f"parse_status={row['parse_status'] or ''}",
                            f"normalized={row['normalized_metric_count']}",
                            f"derived_ok={row['derived_metric_ok_count']}",
                            f"xbrl={row['has_xbrl_path']}",
                        ]
                    )
                    for row in latest_rows
                )
            )

        print_section("monthly_collection_status")
        if not monthly_rows:
            print("none")
        else:
            print_rows(
                (
                    " | ".join(
                        [
                            f"month={row['submit_month']}",
                            f"filings={row['filing_count']}",
                            f"issuers={row['issuer_count']}",
                            f"downloaded={row['downloaded_count']}",
                            f"xbrl_ready={row['xbrl_ready_count']}",
                            f"derived_saved={row['derived_metrics_saved_count']}",
                            f"errors={row['raw_facts_error_count'] + row['normalized_metrics_error_count'] + row['derived_metrics_error_count']}",
                        ]
                    )
                    for row in monthly_rows
                )
            )

        print_section("metric_coverage_summary")
        if not metric_rows:
            print("none")
        else:
            print_rows(
                (
                    " | ".join(
                        [
                            f"source={row['metric_source']}",
                            f"group={row['metric_group'] or ''}",
                            f"metric={row['metric_key']}",
                            f"docs={row['doc_count']}",
                            f"issuers={row['issuer_count']}",
                            f"ok_rows={row['ok_row_count']}",
                            f"max_period_end={row['max_period_end'] or ''}",
                        ]
                    )
                    for row in metric_rows
                )
            )

        print_section("screening_hit_summary")
        if not screening_rows:
            print("none")
        else:
            print_rows(
                (
                    " | ".join(
                        [
                            f"date={row['screening_date']}",
                            f"rule={row['rule_name']}",
                            f"targets={row['target_count']}",
                            f"hits={row['hit_count']}",
                            f"hit_ratio={round(float(row['hit_ratio'] or 0.0), 4)}",
                            f"avg_hit_score={'' if row['avg_hit_score'] is None else round(float(row['avg_hit_score']), 4)}",
                        ]
                    )
                    for row in screening_rows
                )
            )
    finally:
        conn.close()


if __name__ == "__main__":
    main()
