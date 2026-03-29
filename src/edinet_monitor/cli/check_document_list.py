from __future__ import annotations

import os
from datetime import date, timedelta

from edinet_monitor.services.collector.document_filter_service import filter_target_filings
from edinet_monitor.services.collector.document_list_service import fetch_document_list


def main() -> None:
    api_key = os.getenv("EDINET_API_KEY")
    if not api_key:
        raise RuntimeError("環境変数 EDINET_API_KEY が未設定です。")

    for days_ago in range(1, 8):
        target_date = date.today() - timedelta(days=days_ago)

        result = fetch_document_list(
            target_date=target_date,
            api_key=api_key,
            list_type=2,
        )

        filtered = filter_target_filings(result.results)

        print("=" * 80)
        print(f"target_date={target_date.isoformat()}")
        print(f"metadata={result.metadata}")
        print(f"all_results={len(result.results)}")
        print(f"target_results={len(filtered)}")

        for row in filtered[:5]:
            print(
                {
                    "docID": row.get("docID"),
                    "edinetCode": row.get("edinetCode"),
                    "secCode": row.get("secCode"),
                    "filerName": row.get("filerName"),
                    "docDescription": row.get("docDescription"),
                    "submitDateTime": row.get("submitDateTime"),
                    "formCode": row.get("formCode"),
                    "docTypeCode": row.get("docTypeCode"),
                }
            )


if __name__ == "__main__":
    main()