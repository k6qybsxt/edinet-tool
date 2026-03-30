from __future__ import annotations

import os
from datetime import date, timedelta

from edinet_monitor.db.schema import create_tables, get_connection
from edinet_monitor.services.collector.document_filter_service import filter_target_filings
from edinet_monitor.services.collector.document_list_service import fetch_document_list
from edinet_monitor.services.collector.document_row_mapper import (
    to_filing_record,
    to_issuer_record,
)
from edinet_monitor.services.collector.filing_store_service import upsert_filings
from edinet_monitor.services.collector.issuer_store_service import upsert_issuers


def _dedupe_issuers_by_edinet_code(rows: list[dict]) -> list[dict]:
    out: dict[str, dict] = {}

    for row in rows:
        edinet_code = str(row.get("edinetCode") or "")
        if not edinet_code:
            continue
        out[edinet_code] = to_issuer_record(row)

    return list(out.values())

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

def main() -> None:
    api_key = os.getenv("EDINET_API_KEY")
    if not api_key:
        raise RuntimeError("環境変数 EDINET_API_KEY が未設定です。")

    create_tables()

    target_date = date.today() - timedelta(days=2)

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
        issuer_records = _dedupe_issuers_by_edinet_code(issuer_rows)

        issuer_saved_count = upsert_issuers(conn, issuer_records)
        filing_saved_count = upsert_filings(conn, filing_records)
    finally:
        conn.close()

    print(f"target_date={target_date.isoformat()}")
    print(f"all_results={len(result.results)}")
    print(f"target_results={len(filtered_rows)}")
    print(f"issuer_target_results={len(issuer_rows)}")
    print(f"issuer_saved_count={issuer_saved_count}")
    print(f"filing_saved_count={filing_saved_count}")

    for row in issuer_rows[:5]:
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


if __name__ == "__main__":
    main()