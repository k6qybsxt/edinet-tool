from __future__ import annotations

import argparse
import os
from pathlib import Path

from edinet_monitor.services.storage.manifest_service import (
    build_manifest_path,
    read_manifest_rows,
    summarize_manifest_rows,
)


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
    return parser


def resolve_manifest_path(*, manifest_name: str, manifest_path_text: str) -> Path:
    if manifest_path_text:
        return Path(manifest_path_text)

    if manifest_name:
        return build_manifest_path(manifest_name)

    raise ValueError("Specify either manifest_name or manifest_path.")


def main() -> None:
    args = build_arg_parser().parse_args()
    manifest_path = resolve_manifest_path(
        manifest_name=args.manifest_name,
        manifest_path_text=args.manifest_path,
    )
    rows = read_manifest_rows(manifest_path)
    summary = summarize_manifest_rows(rows)

    print(f"manifest_path={manifest_path}")
    print(f"manifest_rows={summary['manifest_rows']}")
    print(f"pending_rows={summary['pending_rows']}")
    print(f"downloaded_rows={summary['downloaded_rows']}")
    print(f"error_rows={summary['error_rows']}")
    print(f"other_rows={summary['other_rows']}")

    for row in summary["sample_errors"]:
        print(row)


if __name__ == "__main__":
    main()
