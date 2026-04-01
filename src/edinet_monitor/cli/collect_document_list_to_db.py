from __future__ import annotations

import argparse
import os
from datetime import date
from typing import Any

from edinet_monitor.db.schema import create_tables, get_connection
from edinet_monitor.services.collector.document_filter_service import filter_target_filings
from edinet_monitor.services.collector.document_list_service import fetch_document_list
from edinet_monitor.services.collector.document_row_mapper import to_filing_record
from edinet_monitor.services.collector.filing_store_service import upsert_filings
from edinet_monitor.services.collector.target_date_service import resolve_target_dates


def _fetch_allowed_edinet_codes(conn) -> set[str]:
    rows = conn.execute(
        """
        SELECT edinet_code
        FROM issuer_master
        WHERE is_listed = 1
          AND exchange = 'TSE'
        """
    ).fetchall()
    return {str(row["edinet_code"]) for row in rows}


def collect_document_list_for_date(target_date: date, *, api_key: str) -> dict[str, Any]:
    result = fetch_document_list(
        target_date=target_date,
        api_key=api_key,
        list_type=2,
    )
    filtered_rows = filter_target_filings(result.results)

    conn = get_connection()
    try:
        allowed_edinet_codes = _fetch_allowed_edinet_codes(conn)
        issuer_rows = [
            row for row in filtered_rows
            if str(row.get("edinetCode") or "") in allowed_edinet_codes
        ]
        filing_records = [to_filing_record(row) for row in issuer_rows]
        filing_saved_count = upsert_filings(conn, filing_records)
    finally:
        conn.close()

    return {
        "target_date": target_date.isoformat(),
        "metadata_date": result.metadata.get("date"),
        "metadata_status": result.metadata.get("status"),
        "metadata_message": result.metadata.get("message"),
        "all_results": len(result.results),
        "target_results": len(filtered_rows),
        "issuer_target_results": len(issuer_rows),
        "filing_saved_count": filing_saved_count,
        "sample_rows": issuer_rows[:5],
    }


def collect_document_list_for_dates(target_dates: list[date], *, api_key: str) -> dict[str, Any]:
    create_tables()

    daily_summaries: list[dict[str, Any]] = []
    totals = {
        "dates": len(target_dates),
        "all_results": 0,
        "target_results": 0,
        "issuer_target_results": 0,
        "filing_saved_count": 0,
    }

    for target_date in target_dates:
        summary = collect_document_list_for_date(target_date, api_key=api_key)
        daily_summaries.append(summary)

        totals["all_results"] += int(summary["all_results"])
        totals["target_results"] += int(summary["target_results"])
        totals["issuer_target_results"] += int(summary["issuer_target_results"])
        totals["filing_saved_count"] += int(summary["filing_saved_count"])

        print(f"target_date={summary['target_date']}")
        print(f"metadata_date={summary['metadata_date']}")
        print(f"metadata_status={summary['metadata_status']}")
        print(f"metadata_message={summary['metadata_message']}")
        print(f"all_results={summary['all_results']}")
        print(f"target_results={summary['target_results']}")
        print(f"issuer_target_results={summary['issuer_target_results']}")
        print(f"filing_saved_count={summary['filing_saved_count']}")

        for row in summary["sample_rows"]:
            print(
                {
                    "docID": row.get("docID"),
                    "edinetCode": row.get("edinetCode"),
                    "secCode": row.get("secCode"),
                    "filerName": row.get("filerName"),
                    "docDescription": row.get("docDescription"),
                    "submitDateTime": row.get("submitDateTime"),
                    "legalStatus": row.get("legalStatus"),
                    "docInfoEditStatus": row.get("docInfoEditStatus"),
                }
            )

    print(f"target_dates_count={totals['dates']}")
    print(f"total_all_results={totals['all_results']}")
    print(f"total_target_results={totals['target_results']}")
    print(f"total_issuer_target_results={totals['issuer_target_results']}")
    print(f"total_filing_saved_count={totals['filing_saved_count']}")

    return {
        "target_dates": [target_date.isoformat() for target_date in target_dates],
        "daily_summaries": daily_summaries,
        "totals": totals,
    }


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--target-date",
        default=os.getenv("EDINET_TARGET_DATE", "").strip(),
        help="Single target date in YYYY-MM-DD format.",
    )
    parser.add_argument(
        "--date-from",
        default=os.getenv("EDINET_DATE_FROM", "").strip(),
        help="Start date in YYYY-MM-DD format.",
    )
    parser.add_argument(
        "--date-to",
        default=os.getenv("EDINET_DATE_TO", "").strip(),
        help="End date in YYYY-MM-DD format.",
    )
    return parser


def main() -> None:
    api_key = os.getenv("EDINET_API_KEY")
    if not api_key:
        raise RuntimeError("Set EDINET_API_KEY before running.")

    args = build_arg_parser().parse_args()
    target_dates = resolve_target_dates(
        target_date_text=args.target_date,
        date_from_text=args.date_from,
        date_to_text=args.date_to,
    )
    collect_document_list_for_dates(target_dates, api_key=api_key)


if __name__ == "__main__":
    main()
