from __future__ import annotations

import argparse
from pathlib import Path

from edinet_monitor.services.metric_snapshot_service import compare_metric_snapshots


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Compare two metric snapshots exported by export_metric_snapshot."
    )
    parser.add_argument("--before", required=True, help="Before snapshot directory")
    parser.add_argument("--after", required=True, help="After snapshot directory")
    parser.add_argument("--output-dir", required=True, help="Directory that will contain the comparison directory")
    parser.add_argument("--timestamp", default=None, help="Optional fixed timestamp YYYYMMDD_HHMMSS")
    return parser


def main() -> None:
    args = build_arg_parser().parse_args()
    result = compare_metric_snapshots(
        before_dir=Path(args.before),
        after_dir=Path(args.after),
        output_dir=Path(args.output_dir),
        timestamp=args.timestamp,
    )
    print(f"saved: {result.comparison_dir}")
    print(f"added_count={result.added_count}")
    print(f"removed_count={result.removed_count}")
    print(f"value_changed_count={result.value_changed_count}")
    print(f"full_changed_same_value_count={result.full_changed_same_value_count}")


if __name__ == "__main__":
    main()
