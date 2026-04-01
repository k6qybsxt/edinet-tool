from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

from edinet_monitor.db.schema import get_connection
from edinet_monitor.services.collector.download_queue_service import (
    fetch_downloaded_filings_without_xbrl,
    mark_xbrl_extract_error,
    mark_xbrl_extract_success,
)
from edinet_monitor.services.storage.path_service import build_xbrl_save_path
from edinet_monitor.services.storage.zip_extract_service import extract_first_xbrl, find_xbrl_member_names


def run_extract_xbrl_from_zips(*, batch_size: int = 20, run_all: bool = False) -> dict[str, Any]:
    conn = get_connection()
    total_target = 0
    total_extracted = 0
    total_errors = 0
    loop_count = 0

    try:
        while True:
            rows = fetch_downloaded_filings_without_xbrl(conn, limit=batch_size)
            print(f"downloaded_rows_without_xbrl={len(rows)}")

            if not rows:
                break

            loop_count += 1
            total_target += len(rows)

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
                    total_extracted += 1
                    print(f"extracted doc_id={doc_id} xbrl_path={saved_path}")
                except Exception as e:
                    mark_xbrl_extract_error(conn, doc_id)
                    total_errors += 1
                    print(f"extract_error doc_id={doc_id} error={repr(e)}")

            if not run_all:
                break
    finally:
        conn.close()

    print(f"xbrl_extract_target_total={total_target}")
    print(f"xbrl_extracted_total={total_extracted}")
    print(f"xbrl_extract_error_total={total_errors}")

    return {
        "loop_count": loop_count,
        "target_total": total_target,
        "extracted_total": total_extracted,
        "error_total": total_errors,
    }


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-size", type=int, default=20)
    parser.add_argument("--run-all", action="store_true")
    return parser


def main() -> None:
    args = build_arg_parser().parse_args()
    run_extract_xbrl_from_zips(
        batch_size=args.batch_size,
        run_all=args.run_all,
    )


if __name__ == "__main__":
    main()
