from __future__ import annotations

from datetime import datetime

from edinet_monitor.db.schema import create_tables, get_connection
from edinet_monitor.screening.screening_query_service import (
    fetch_latest_metrics_by_edinet_code,
    fetch_target_edinet_codes,
)
from edinet_monitor.screening.screening_result_store_service import (
    insert_screening_result,
    insert_screening_run,
)
from edinet_monitor.screening.screening_rule_service import (
    RULE_NAME,
    RULE_VERSION,
    evaluate_minimum_viable_value_check,
)


def main() -> None:
    create_tables()

    conn = get_connection()
    try:
        screening_date = datetime.now().strftime("%Y-%m-%d")
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

        screening_run_id = insert_screening_run(
            conn,
            screening_date=screening_date,
            rule_name=RULE_NAME,
            rule_version=RULE_VERSION,
            target_count=len(pending_results),
            hit_count=hit_count,
        )

        for row in pending_results:
            insert_screening_result(
                conn,
                screening_run_id=screening_run_id,
                screening_date=screening_date,
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

        print(f"screening_date={screening_date}")
        print(f"target_count={len(pending_results)}")
        print(f"hit_count={hit_count}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()