from __future__ import annotations

import argparse
from pathlib import Path

from edinet_monitor.config.settings import DB_BACKUP_ROOT, DB_PATH
from edinet_monitor.services.db_backup_service import backup_sqlite_db


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Back up the edinet_monitor SQLite DB file.")
    parser.add_argument("--db-path", default=str(DB_PATH), help="Source SQLite DB path.")
    parser.add_argument(
        "--output-dir",
        default=str(DB_BACKUP_ROOT),
        help="Backup output directory. Default: D:\\EDINET_Backup",
    )
    parser.add_argument("--label", default="", help="Optional label appended to the backup file name.")
    return parser


def main() -> None:
    args = build_arg_parser().parse_args()
    result = backup_sqlite_db(
        source_path=Path(args.db_path),
        output_dir=Path(args.output_dir),
        label=args.label,
    )
    print(f"source_path={result.source_path}")
    print(f"backup_path={result.backup_path}")
    print(f"source_size_bytes={result.source_size_bytes}")
    print(f"backup_size_bytes={result.backup_size_bytes}")


if __name__ == "__main__":
    main()
