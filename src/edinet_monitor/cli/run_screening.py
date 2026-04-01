from __future__ import annotations

import argparse
from datetime import datetime
from typing import Any

from edinet_monitor.db.schema import create_tables, get_connection
from edinet_monitor.screening.screening_query_service import (
    fetch_latest_metrics_by_edinet_code,
    fetch_target_edinet_codes,
)
from edinet_monitor.screening.screening_result_store_service import (
    delete_screening_results_by_date_rule,
    insert_screening_result,
    insert_screening_run,
)
from edinet_monitor.screening.screening_rule_service import (
    RULE_NAME,
    RULE_VERSION,
    evaluate_minimum_viable_value_check,
)


def run_screening(*, screening_date: str | None = None) -> dict[str, Any]:
    create_tables()

    conn = get_connection()
    try:
        effective_screening_date = screening_date or datetime.now().strftime("%Y-%m-%d")
        edinet_codes = fetch_target_edinet_codes(conn)

        pending_results: list[dict] = []

        for edinet_code in edinet_codes:
            metrics = fetch_latest_metrics_by_edinet_code(conn, edinet_code)
            if not metrics:
                continue

            sample_row = next(iter(metrics.values()))
            result = evaluate_minimum_viable_value_check(metrics)

            pending_results.append(
                {
                    "edinet_code": edinet_code,
                    "security_code": sample_row.get("security_code"),
                    "company_name": None,
                    "period_end": sample_row.get("period_end"),
                    "result_flag": int(result["result_flag"]),
                    "score": result["score"],
                    "detail": result["detail"],
                }
            )

        hit_count = sum(1 for row in pending_results if row["result_flag"] == 1)
        delete_screening_results_by_date_rule(
            conn,
            screening_date=effective_screening_date,
            rule_name=RULE_NAME,
        )

        screening_run_id = insert_screening_run(
            conn,
            screening_date=effective_screening_date,
            rule_name=RULE_NAME,
            rule_version=RULE_VERSION,
            target_count=len(pending_results),
            hit_count=hit_count,
        )

        for row in pending_results:
            insert_screening_result(
                conn,
                screening_run_id=screening_run_id,
                screening_date=effective_screening_date,
                rule_name=RULE_NAME,
                rule_version=RULE_VERSION,
                edinet_code=row["edinet_code"],
                security_code=row["security_code"],
                company_name=row["company_name"],
                period_end=row["period_end"],
                result_flag=row["result_flag"],
                score=row["score"],
                detail=row["detail"],
            )

        print(f"screening_date={effective_screening_date}")
        print(f"target_count={len(pending_results)}")
        print(f"hit_count={hit_count}")

        return {
            "screening_date": effective_screening_date,
            "target_count": len(pending_results),
            "hit_count": hit_count,
            "screening_run_id": screening_run_id,
        }
    finally:
        conn.close()


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--screening-date", default="")
    return parser


def main() -> None:
    args = build_arg_parser().parse_args()
    run_screening(screening_date=args.screening_date or None)


if __name__ == "__main__":
    main()
