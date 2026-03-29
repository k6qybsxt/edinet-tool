from __future__ import annotations

import os
from datetime import date, timedelta

from edinet_monitor.db.schema import get_connection
from edinet_monitor.services.collector.document_filter_service import filter_target_filings
from edinet_monitor.services.collector.document_list_service import fetch_document_list
from edinet_monitor.services.collector.document_row_mapper import to_filing_record
from edinet_monitor.services.collector.filing_store_service import upsert_filings


def main() -> None:
    api_key = os.getenv("EDINET_API_KEY")
    if not api_key:
        raise RuntimeError("環境変数 EDINET_API_KEY が未設定です。")

    target_date = date.today() - timedelta(days=2)

    result = fetch_document_list(
        target_date=target_date,
        api_key=api_key,
        list_type=2,
    )

    filtered_rows = filter_target_filings(result.results)
    filing_records = [to_filing_record(row) for row in filtered_rows]

    conn = get_connection()
    try:
        saved_count = upsert_filings(conn, filing_records)
    finally:
        conn.close()

    print(f"target_date={target_date.isoformat()}")
    print(f"all_results={len(result.results)}")
    print(f"target_results={len(filtered_rows)}")
    print(f"saved_count={saved_count}")

    for row in filtered_rows[:5]:
        print(
            {
                "docID": row.get("docID"),
                "edinetCode": row.get("edinetCode"),
                "secCode": row.get("secCode"),
                "filerName": row.get("filerName"),
                "docDescription": row.get("docDescription"),
                "submitDateTime": row.get("submitDateTime"),
            }
        )


if __name__ == "__main__":
    main()