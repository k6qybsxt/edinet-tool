from __future__ import annotations

import argparse

from edinet_monitor.db.schema import create_tables, get_connection
from edinet_monitor.services.jquants.coverage_service import export_jquants_coverage


def _split_csv(value: str) -> list[str]:
    text = str(value or "").strip()
    if not text or text.lower() == "all":
        return []
    return [part.strip() for part in text.split(",") if part.strip()]


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Export J-Quants V2 ingestion coverage report.")
    parser.add_argument("--date-from", required=True)
    parser.add_argument("--date-to", required=True)
    parser.add_argument("--target", choices=["all", "statements", "quotes"], default="all")
    parser.add_argument("--codes", default="all")
    parser.add_argument("--output-dir", default=r"D:\作業用")
    return parser


def main() -> None:
    args = build_arg_parser().parse_args()
    create_tables()
    conn = get_connection()
    try:
        result = export_jquants_coverage(
            conn,
            date_from=args.date_from,
            date_to=args.date_to,
            target=args.target,
            codes=_split_csv(args.codes),
            output_dir=args.output_dir,
        )
    finally:
        conn.close()

    print(f"output_path={result.output_path}")
    print(f"rows={result.rows}")
    print(f"warnings={len(result.warnings)}")
    for warning in result.warnings[:10]:
        print(f"warning={warning}")


if __name__ == "__main__":
    main()
