from __future__ import annotations

import os

from edinet_monitor.db.schema import get_connection
from edinet_monitor.services.collector.document_download_service import download_document_zip
from edinet_monitor.services.collector.download_queue_service import (
    fetch_pending_filings,
    mark_download_error,
    mark_download_success,
)
from edinet_monitor.services.storage.path_service import build_zip_save_path


def main() -> None:
    api_key = os.getenv("EDINET_API_KEY")
    if not api_key:
        raise RuntimeError("環境変数 EDINET_API_KEY が未設定です。")

    conn = get_connection()
    try:
        rows = fetch_pending_filings(conn, limit=1)

        print(f"[DEBUG] pending_rows={len(rows)}")

        for row in rows:
            doc_id = row["doc_id"]
            submit_date = row["submit_date"]

            print(f"[DEBUG] target_doc_id={doc_id} submit_date={submit_date}")

            output_path = build_zip_save_path(submit_date, doc_id)
            print(f"[DEBUG] output_path={output_path}")

            try:
                saved_path = download_document_zip(
                    doc_id=doc_id,
                    api_key=api_key,
                    output_path=output_path,
                    timeout_sec=30,
                )
                mark_download_success(conn, doc_id, str(saved_path))
                print(f"downloaded doc_id={doc_id} path={saved_path}")
            except Exception as e:
                mark_download_error(conn, doc_id)
                print(f"download_error doc_id={doc_id} error={repr(e)}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()