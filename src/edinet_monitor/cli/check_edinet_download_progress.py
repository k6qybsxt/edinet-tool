from __future__ import annotations

import argparse
import os

from edinet_monitor.services.collector.document_filter_service import normalize_form_codes
from edinet_monitor.services.edinet_download_progress_service import export_edinet_download_progress


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Export EDINET ZIP download progress from manifest files.")
    parser.add_argument("--date-from", required=True)
    parser.add_argument("--date-to", required=True)
    parser.add_argument("--manifest-prefix", default=os.getenv("EDINET_MANIFEST_PREFIX", "document_manifest").strip())
    parser.add_argument("--manifest-granularity", choices=["month", "day"], default="month")
    parser.add_argument(
        "--form-codes",
        default=os.getenv("EDINET_TARGET_FORM_CODES", "").strip(),
        help="Comma-separated form codes. Example: 030000,043000",
    )
    parser.add_argument("--output-dir", default="D:\\\u4f5c\u696d\u7528")
    return parser


def main() -> None:
    args = build_arg_parser().parse_args()
    result = export_edinet_download_progress(
        date_from=args.date_from,
        date_to=args.date_to,
        manifest_prefix=args.manifest_prefix,
        manifest_granularity=args.manifest_granularity,
        form_codes=normalize_form_codes(args.form_codes or None),
        output_dir=args.output_dir,
    )
    print(f"output_path={result.output_path}")
    print(f"chunks={len(result.chunks)}")
    print(f"missing_manifest_chunks={result.missing_manifest_chunks}")
    print(f"incomplete_chunks={result.incomplete_chunks}")
    print(f"manifest_rows={result.manifest_rows}")
    print(f"downloaded_rows={result.downloaded_rows}")
    print(f"pending_rows={result.pending_rows}")
    print(f"error_rows={result.error_rows}")
    print(f"retryable_error_rows={result.retryable_error_rows}")


if __name__ == "__main__":
    main()
