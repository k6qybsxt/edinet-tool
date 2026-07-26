from __future__ import annotations

import argparse
from pathlib import Path
import sqlite3

from edinet_monitor.config.settings import DB_PATH
from edinet_monitor.services.doc_id_file_service import load_doc_ids_file
from edinet_monitor.services.two_q_issued_shares_audit_service import (
    DEFAULT_2Q_ISSUED_SHARES_AUDIT_OUTPUT_DIR,
    build_two_q_issued_shares_audit,
    write_two_q_issued_shares_audit,
)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Audit 2Q issued-share normalization without DB updates.")
    parser.add_argument("--db-path", default=str(DB_PATH))
    parser.add_argument("--doc-ids-file", default="", help="Optional UTF-8 doc ID file to audit.")
    parser.add_argument("--output-dir", default=str(DEFAULT_2Q_ISSUED_SHARES_AUDIT_OUTPUT_DIR))
    parser.add_argument("--limit-preview", type=int, default=10)
    return parser


def _connect_readonly(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only = ON")
    return conn


def main() -> None:
    args = build_arg_parser().parse_args()
    doc_ids = load_doc_ids_file(args.doc_ids_file) if args.doc_ids_file else ()
    conn = _connect_readonly(Path(args.db_path))
    try:
        result = build_two_q_issued_shares_audit(conn, doc_ids=doc_ids)
    finally:
        conn.close()
    paths = write_two_q_issued_shares_audit(result, output_dir=Path(args.output_dir))
    for key, value in result.summary.items():
        print(f"{key}={value}")
    print(f"doc_ids_path={paths.doc_ids_path}")
    print(f"json_path={paths.json_path}")
    print(f"tsv_path={paths.tsv_path}")
    for row in result.rows[: max(int(args.limit_preview), 0)]:
        print(
            "row="
            f"{row.doc_id}|{row.status}|{row.source_tag}|{row.candidate_value}|"
            f"{row.existing_issued_value}|{row.outstanding_calc_status}"
        )


if __name__ == "__main__":
    main()
