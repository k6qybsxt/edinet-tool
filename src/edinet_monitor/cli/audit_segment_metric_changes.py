from __future__ import annotations

import argparse
from pathlib import Path
import sqlite3

from edinet_monitor.config.settings import DB_PATH
from edinet_monitor.services.doc_id_file_service import load_doc_ids_file
from edinet_monitor.services.segment_metric_change_audit_service import (
    DEFAULT_SEGMENT_METRIC_CHANGE_AUDIT_OUTPUT_DIR,
    build_segment_metric_change_audit,
    write_segment_metric_change_audit,
)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Audit segment fiscal-year anchors and profit classifications without DB updates."
    )
    parser.add_argument("--db-path", default=str(DB_PATH))
    parser.add_argument("--doc-ids-file", default="", help="Optional UTF-8 doc ID file to audit.")
    parser.add_argument("--output-dir", default=str(DEFAULT_SEGMENT_METRIC_CHANGE_AUDIT_OUTPUT_DIR))
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
        result = build_segment_metric_change_audit(conn, doc_ids=doc_ids)
    finally:
        conn.close()
    paths = write_segment_metric_change_audit(result, output_dir=Path(args.output_dir))
    for key, value in result.summary.items():
        print(f"{key}={value}")
    print(f"rebuild_doc_ids_path={paths.rebuild_doc_ids_path}")
    print(f"fiscal_anchor_json_path={paths.fiscal_anchor_json_path}")
    print(f"fiscal_anchor_tsv_path={paths.fiscal_anchor_tsv_path}")
    print(f"profit_classification_json_path={paths.profit_classification_json_path}")
    print(f"profit_classification_tsv_path={paths.profit_classification_tsv_path}")
    print(f"summary_json_path={paths.summary_json_path}")
    for row in result.fiscal_anchor_rows[: max(int(args.limit_preview), 0)]:
        print(
            "fiscal_anchor="
            f"{row.doc_id}|{row.status}|{row.existing_fiscal_years}|"
            f"{row.expected_fiscal_year}|{row.anchor_source}"
        )
    for row in result.profit_classification_rows[: max(int(args.limit_preview), 0)]:
        print(
            "profit_classification="
            f"{row.doc_id}|{row.status}|{row.existing_metric_bases}|"
            f"{row.proposed_metric_bases}|{row.classification_status}"
        )


if __name__ == "__main__":
    main()
