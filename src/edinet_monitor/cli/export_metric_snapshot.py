from __future__ import annotations

import argparse
from pathlib import Path

from edinet_monitor.config.settings import DB_PATH
from edinet_monitor.services.metric_snapshot_service import export_metric_snapshot


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Export normalized_metrics and derived_metrics snapshots for before/after comparison."
    )
    parser.add_argument("--label", required=True, help="Snapshot label, e.g. before_taxonomy_change")
    parser.add_argument("--output-dir", required=True, help="Directory that will contain the snapshot directory")
    parser.add_argument("--db-path", default=str(DB_PATH), help="SQLite DB path")
    parser.add_argument("--timestamp", default=None, help="Optional fixed timestamp YYYYMMDD_HHMMSS")
    return parser


def main() -> None:
    args = build_arg_parser().parse_args()
    result = export_metric_snapshot(
        label=args.label,
        output_dir=Path(args.output_dir),
        db_path=Path(args.db_path),
        timestamp=args.timestamp,
    )
    print(f"saved: {result.snapshot_dir}")
    print(f"manifest: {result.manifest_path}")
    print(f"normalized_rows={result.normalized_rows}")
    print(f"derived_rows={result.derived_rows}")


if __name__ == "__main__":
    main()
