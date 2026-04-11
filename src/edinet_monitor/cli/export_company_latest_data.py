from __future__ import annotations

import argparse
import json
from pathlib import Path

from edinet_monitor.config.settings import DB_PATH
from edinet_monitor.db.schema import create_tables, get_connection
from edinet_monitor.services.company_export_service import export_company_latest_dataset


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--security-code", required=True)
    parser.add_argument("--years", type=int, default=5)
    parser.add_argument("--screening-limit", type=int, default=20)
    parser.add_argument("--output-path", default="")
    return parser


def main() -> None:
    args = build_arg_parser().parse_args()
    create_tables()

    conn = get_connection()
    try:
        payload = export_company_latest_dataset(
            conn,
            security_code=args.security_code,
            years=args.years,
            screening_limit=args.screening_limit,
        )
    finally:
        conn.close()

    output_path = Path(args.output_path) if args.output_path else None
    if output_path is not None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        print(f"output_path={output_path}")

    company = payload.get("\u4f1a\u793e\u60c5\u5831", {})
    counts = payload.get("\u4ef6\u6570", {})
    print(f"db_path={DB_PATH}")
    print(f"security_code={args.security_code}")
    print(f"company_name={company.get('company_name', '')}")
    print(f"edinet_code={company.get('edinet_code', '')}")
    print(f"filings={counts.get('\u63d0\u51fa\u66f8\u985e', 0)}")
    print(f"normalized_metrics={counts.get('\u6b63\u898f\u5316\u6307\u6a19', 0)}")
    print(f"derived_metrics={counts.get('\u6d3e\u751f\u6307\u6a19', 0)}")
    print(f"raw_facts={counts.get('\u751f\u30d5\u30a1\u30af\u30c8', 0)}")
    print(f"screening_results_recent={counts.get('\u76f4\u8fd1screening\u7d50\u679c', 0)}")


if __name__ == "__main__":
    main()
