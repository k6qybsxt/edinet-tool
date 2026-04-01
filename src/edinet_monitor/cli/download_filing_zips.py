from __future__ import annotations

import argparse
import os
from typing import Any

from edinet_monitor.db.schema import get_connection
from edinet_monitor.services.collector.document_download_service import download_document_zip
from edinet_monitor.services.collector.download_queue_service import (
    fetch_pending_filings,
    mark_download_error,
    mark_download_success,
)
from edinet_monitor.services.storage.path_service import build_zip_save_path


def run_download_filing_zips(
    *,
    api_key: str,
    batch_size: int = 20,
    run_all: bool = False,
) -> dict[str, Any]:
    conn = get_connection()
    total_target = 0
    total_downloaded = 0
    total_errors = 0
    loop_count = 0

    try:
        while True:
            rows = fetch_pending_filings(conn, limit=batch_size)
            print(f"[DEBUG] pending_rows={len(rows)}")

            if not rows:
                break

            loop_count += 1
            total_target += len(rows)

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
                    total_downloaded += 1
                    print(f"downloaded doc_id={doc_id} path={saved_path}")
                except Exception as e:
                    mark_download_error(conn, doc_id)
                    total_errors += 1
                    print(f"download_error doc_id={doc_id} error={repr(e)}")

            if not run_all:
                break
    finally:
        conn.close()

    print(f"download_target_total={total_target}")
    print(f"downloaded_total={total_downloaded}")
    print(f"download_error_total={total_errors}")

    return {
        "loop_count": loop_count,
        "target_total": total_target,
        "downloaded_total": total_downloaded,
        "error_total": total_errors,
    }


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-size", type=int, default=20)
    parser.add_argument("--run-all", action="store_true")
    return parser


def main() -> None:
    api_key = os.getenv("EDINET_API_KEY")
    if not api_key:
        raise RuntimeError("Set EDINET_API_KEY before running.")

    args = build_arg_parser().parse_args()
    run_download_filing_zips(
        api_key=api_key,
        batch_size=args.batch_size,
        run_all=args.run_all,
    )


if __name__ == "__main__":
    main()
