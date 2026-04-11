from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import Any

from edinet_monitor.cli.import_tse_listing_master import load_csv_rows, row_to_issuer_record
from edinet_monitor.config.settings import DB_PATH, MANIFEST_ROOT, TSE_LISTING_MASTER_CSV_PATH
from edinet_monitor.db.schema import create_tables, get_connection
from edinet_monitor.services.collector.issuer_store_service import upsert_issuers
from edinet_monitor.services.collector.manifest_filing_import_service import (
    build_filing_record_from_manifest_row,
    load_manifest_rows_for_filing_sync,
    resolve_manifest_paths,
    upsert_manifest_filing_records,
)
from edinet_monitor.services.storage.manifest_service import read_manifest_rows


def run_import_manifest_filings_to_db(
    *,
    manifest_name: str = "",
    manifest_path_text: str = "",
    import_all_manifests: bool = False,
    import_issuer_master: bool = True,
    master_csv_path: Path | None = None,
    manifest_root: Path = MANIFEST_ROOT,
) -> dict[str, Any]:
    if manifest_name and manifest_path_text:
        raise ValueError("Use either manifest_name or manifest_path, not both.")

    create_tables()

    csv_path = Path(master_csv_path or TSE_LISTING_MASTER_CSV_PATH)
    manifest_paths = resolve_manifest_paths(
        manifest_root=manifest_root,
        manifest_name="" if import_all_manifests else manifest_name,
        manifest_path="" if import_all_manifests else manifest_path_text,
    )
    if not manifest_paths:
        raise FileNotFoundError(f"No manifest files found under {manifest_root}")

    missing_manifest_paths = [path for path in manifest_paths if not path.exists()]
    if missing_manifest_paths:
        missing_text = ", ".join(str(path) for path in missing_manifest_paths[:5])
        raise FileNotFoundError(f"Manifest not found: {missing_text}")

    summary: dict[str, Any] = {
        "db_path": str(DB_PATH),
        "manifest_file_count": len(manifest_paths),
        "manifest_paths": [str(path) for path in manifest_paths[:10]],
        "issuer_rows": 0,
        "issuer_saved_rows": 0,
        "manifest_row_count": 0,
        "unique_doc_id_count": 0,
    }

    conn = get_connection()
    try:
        if import_issuer_master:
            issuer_csv_rows = load_csv_rows(str(csv_path))
            issuer_rows = [
                row_to_issuer_record(row)
                for row in issuer_csv_rows
                if str(row.get("edinet_code") or "").strip()
            ]
            summary["issuer_rows"] = len(issuer_rows)
            summary["issuer_saved_rows"] = upsert_issuers(conn, issuer_rows)

        manifest_rows = load_manifest_rows_for_filing_sync(manifest_paths)
        summary["manifest_row_count"] = sum(len(read_manifest_rows(path)) for path in manifest_paths)
        summary["unique_doc_id_count"] = len(manifest_rows)

        timestamp_text = None
        filing_records = [
            build_filing_record_from_manifest_row(row, timestamp_text=timestamp_text)
            for row in manifest_rows
        ]
        filing_summary = upsert_manifest_filing_records(conn, filing_records)
        summary.update(filing_summary)
    finally:
        conn.close()

    print(f"db_path={summary['db_path']}")
    print(f"manifest_file_count={summary['manifest_file_count']}")
    print(f"unique_doc_id_count={summary['unique_doc_id_count']}")
    print(f"issuer_rows={summary['issuer_rows']}")
    print(f"issuer_saved_rows={summary['issuer_saved_rows']}")
    print(f"filing_total_rows={summary['total_rows']}")
    print(f"filing_inserted_rows={summary['inserted_rows']}")
    print(f"filing_updated_rows={summary['updated_rows']}")
    print(f"filing_downloaded_rows={summary['downloaded_rows']}")
    print(f"filing_xbrl_ready_rows={summary['xbrl_ready_rows']}")

    for manifest_path in summary["manifest_paths"]:
        print(f"manifest_path={manifest_path}")

    return summary


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--manifest-name",
        default=os.getenv("EDINET_MANIFEST_NAME", "").strip(),
        help="Optional manifest name without extension.",
    )
    parser.add_argument(
        "--manifest-path",
        default=os.getenv("EDINET_MANIFEST_PATH", "").strip(),
        help="Optional full path to one manifest JSONL.",
    )
    parser.add_argument(
        "--all-manifests",
        action="store_true",
        help="Import all manifest JSONL files under MANIFEST_ROOT.",
    )
    parser.add_argument(
        "--master-csv-path",
        default=os.getenv("EDINET_TSE_MASTER_CSV", "").strip(),
        help="Optional TSE issuer master CSV path.",
    )
    parser.add_argument(
        "--skip-issuer-master",
        action="store_true",
        help="Skip issuer_master import and sync filings only.",
    )
    return parser


def main() -> None:
    args = build_arg_parser().parse_args()
    run_import_manifest_filings_to_db(
        manifest_name=args.manifest_name,
        manifest_path_text=args.manifest_path,
        import_all_manifests=args.all_manifests or (not args.manifest_name and not args.manifest_path),
        import_issuer_master=not args.skip_issuer_master,
        master_csv_path=Path(args.master_csv_path) if args.master_csv_path else None,
    )


if __name__ == "__main__":
    main()
