from __future__ import annotations

from pathlib import Path

from edinet_monitor.db.schema import get_connection
from edinet_monitor.services.collector.download_queue_service import (
    fetch_downloaded_filings_without_xbrl,
    mark_xbrl_extract_error,
    mark_xbrl_extract_success,
)
from edinet_monitor.services.storage.path_service import build_xbrl_save_path
from edinet_monitor.services.storage.zip_extract_service import extract_first_xbrl, find_xbrl_member_names


def main() -> None:
    conn = get_connection()
    try:
        rows = fetch_downloaded_filings_without_xbrl(conn, limit=20)
        print(f"downloaded_rows_without_xbrl={len(rows)}")

        for row in rows:
            doc_id = row["doc_id"]
            submit_date = row["submit_date"]
            zip_path = Path(row["zip_path"])

            print(f"[DEBUG] target_doc_id={doc_id}")
            print(f"[DEBUG] zip_path={zip_path}")

            try:
                member_names = find_xbrl_member_names(zip_path)
                print(f"[DEBUG] xbrl_members={member_names[:5]}")

                xbrl_path = build_xbrl_save_path(submit_date, doc_id)
                saved_path = extract_first_xbrl(zip_path, xbrl_path)

                mark_xbrl_extract_success(conn, doc_id, str(saved_path))
                print(f"extracted doc_id={doc_id} xbrl_path={saved_path}")
            except Exception as e:
                mark_xbrl_extract_error(conn, doc_id)
                print(f"extract_error doc_id={doc_id} error={repr(e)}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()