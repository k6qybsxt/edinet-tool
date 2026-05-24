from __future__ import annotations

import argparse

from edinet_monitor.config.settings import OPERATION_LOG_ROOT
from edinet_monitor.db.schema import get_connection
from edinet_monitor.services.jquants_quality_audit_service import export_jquants_quality_audit


def _split_csv(value: str) -> list[str]:
    text = str(value or "").strip()
    if not text or text.lower() == "all":
        return []
    return [part.strip() for part in text.split(",") if part.strip()]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Export a read-only J-Quants quality anomaly report.")
    parser.add_argument("--date-from", required=True)
    parser.add_argument("--date-to", required=True)
    parser.add_argument("--codes", default="all")
    parser.add_argument("--output-dir", default=str(OPERATION_LOG_ROOT))
    return parser


def main() -> None:
    args = build_parser().parse_args()
    conn = get_connection()
    try:
        result = export_jquants_quality_audit(
            conn,
            date_from=args.date_from,
            date_to=args.date_to,
            codes=_split_csv(args.codes),
            output_dir=args.output_dir,
        )
    finally:
        conn.close()

    print(f"issue_count={result.issue_count}")
    for severity, count in sorted(result.counts_by_severity.items()):
        print(f"{severity}={count}")
    print(f"output_path={result.output_path}")
    print(f"tsv_path={result.tsv_path}")


if __name__ == "__main__":
    main()
