from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import Any, Callable

from edinet_monitor.config.settings import ensure_data_dirs
from edinet_monitor.services.collector.manifest_download_service import (
    mark_manifest_download_error,
    process_manifest_download_row,
    select_manifest_row_indexes,
)
from edinet_monitor.services.collector.document_download_service import download_document_zip
from edinet_monitor.services.storage.manifest_service import (
    build_manifest_path,
    read_manifest_rows,
    write_manifest_rows,
)


def run_download_manifest_zips(
    *,
    api_key: str,
    manifest_path: Path,
    batch_size: int = 20,
    run_all: bool = False,
    retry_errors: bool = False,
    max_docs: int = 0,
    downloader: Callable[..., Path] = download_document_zip,
) -> dict[str, Any]:
    rows = read_manifest_rows(manifest_path)
    if not rows:
        print(f"manifest_path={manifest_path}")
        print("manifest_rows=0")
        return {
            "manifest_path": str(manifest_path),
            "manifest_rows": 0,
            "target_total": 0,
            "downloaded_total": 0,
            "existing_total": 0,
            "error_total": 0,
            "processed_total": 0,
        }

    total_target = 0
    total_downloaded = 0
    total_existing = 0
    total_errors = 0
    total_processed = 0

    while True:
        remaining_limit = batch_size
        if max_docs > 0:
            remaining_limit = min(remaining_limit, max(max_docs - total_processed, 0))

        if remaining_limit <= 0:
            break

        target_indexes = select_manifest_row_indexes(
            rows,
            limit=remaining_limit,
            retry_errors=retry_errors,
        )

        print(f"manifest_target_rows={len(target_indexes)}")

        if not target_indexes:
            break

        total_target += len(target_indexes)

        for idx in target_indexes:
            row = rows[idx]
            doc_id = str(row.get("doc_id") or "")
            submit_date = str(row.get("submit_date") or "")
            zip_path = str(row.get("zip_path") or "")

            print(f"[DEBUG] target_doc_id={doc_id} submit_date={submit_date}")
            print(f"[DEBUG] output_path={zip_path}")

            try:
                result = process_manifest_download_row(
                    row,
                    api_key=api_key,
                    downloader=downloader,
                )
                rows[idx] = row
                total_processed += 1

                if result["result"] == "existing":
                    total_existing += 1
                    print(f"existing_zip doc_id={doc_id} path={result['path']}")
                else:
                    total_downloaded += 1
                    print(f"downloaded doc_id={doc_id} path={result['path']}")
            except Exception as e:
                row["download_attempts"] = int(row.get("download_attempts") or 0) + 1
                mark_manifest_download_error(row, e)
                rows[idx] = row
                total_processed += 1
                total_errors += 1
                print(f"download_error doc_id={doc_id} error={repr(e)}")

            write_manifest_rows(manifest_path, rows)

            if max_docs > 0 and total_processed >= max_docs:
                break

        if not run_all:
            break

        if max_docs > 0 and total_processed >= max_docs:
            break

    print(f"manifest_path={manifest_path}")
    print(f"manifest_rows={len(rows)}")
    print(f"download_target_total={total_target}")
    print(f"downloaded_total={total_downloaded}")
    print(f"existing_total={total_existing}")
    print(f"download_error_total={total_errors}")
    print(f"processed_total={total_processed}")

    return {
        "manifest_path": str(manifest_path),
        "manifest_rows": len(rows),
        "target_total": total_target,
        "downloaded_total": total_downloaded,
        "existing_total": total_existing,
        "error_total": total_errors,
        "processed_total": total_processed,
    }


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--manifest-name",
        default=os.getenv("EDINET_MANIFEST_NAME", "").strip(),
        help="Manifest name without extension.",
    )
    parser.add_argument(
        "--manifest-path",
        default=os.getenv("EDINET_MANIFEST_PATH", "").strip(),
        help="Optional full path to manifest JSONL.",
    )
    parser.add_argument("--batch-size", type=int, default=20)
    parser.add_argument("--run-all", action="store_true")
    parser.add_argument("--retry-errors", action="store_true")
    parser.add_argument(
        "--max-docs",
        type=int,
        default=0,
        help="Optional cap for processed documents in this run.",
    )
    return parser


def resolve_manifest_path(*, manifest_name: str, manifest_path_text: str) -> Path:
    if manifest_path_text:
        return Path(manifest_path_text)

    if manifest_name:
        return build_manifest_path(manifest_name)

    raise ValueError("Specify either manifest_name or manifest_path.")


def main() -> None:
    api_key = os.getenv("EDINET_API_KEY")
    if not api_key:
        raise RuntimeError("Set EDINET_API_KEY before running.")

    args = build_arg_parser().parse_args()
    ensure_data_dirs()

    manifest_path = resolve_manifest_path(
        manifest_name=args.manifest_name,
        manifest_path_text=args.manifest_path,
    )

    run_download_manifest_zips(
        api_key=api_key,
        manifest_path=manifest_path,
        batch_size=args.batch_size,
        run_all=args.run_all,
        retry_errors=args.retry_errors,
        max_docs=args.max_docs,
    )


if __name__ == "__main__":
    main()
