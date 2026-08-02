from __future__ import annotations

import argparse
from pathlib import Path
import sqlite3

from edinet_monitor.config.settings import DB_PATH
from edinet_monitor.services.doc_id_file_service import load_doc_ids_file
from edinet_monitor.services.segment_note_semantics_audit_service import (
    DEFAULT_SEGMENT_NOTE_SEMANTICS_AUDIT_OUTPUT_DIR,
    build_segment_note_semantics_audit,
    write_segment_note_semantics_audit,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Audit segment-note table semantics without updating DB rows."
    )
    parser.add_argument("--db-path", default=str(DB_PATH))
    parser.add_argument("--doc-ids-file", default="", help="Optional UTF-8 doc ID file to audit.")
    parser.add_argument(
        "--exact",
        action="store_true",
        help="Rebuild the supplied doc IDs in memory for an exact post-apply comparison.",
    )
    parser.add_argument("--output-dir", default=str(DEFAULT_SEGMENT_NOTE_SEMANTICS_AUDIT_OUTPUT_DIR))
    parser.add_argument("--limit-preview", type=int, default=10)
    return parser


def _connect_readonly(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only = ON")
    return conn


def main() -> None:
    args = build_parser().parse_args()
    doc_ids = load_doc_ids_file(args.doc_ids_file) if args.doc_ids_file else ()
    if args.exact and not doc_ids:
        raise ValueError("--exact requires --doc-ids-file.")
    conn = _connect_readonly(Path(args.db_path))
    try:
        result = build_segment_note_semantics_audit(conn, doc_ids=doc_ids, exact=args.exact)
    finally:
        conn.close()
    paths = write_segment_note_semantics_audit(result, output_dir=Path(args.output_dir))
    for key, value in result.summary.items():
        print(f"{key}={value}")
    print(f"rebuild_doc_ids_path={paths.rebuild_doc_ids_path}")
    print(f"rows_json_path={paths.rows_json_path}")
    print(f"rows_tsv_path={paths.rows_tsv_path}")
    print(f"summary_json_path={paths.summary_json_path}")
    for row in result.rows[: max(int(args.limit_preview), 0)]:
        print(
            "semantic_candidate="
            f"{row.doc_id}|{row.status}|{row.selected_candidate_count}|"
            f"{row.excluded_candidate_count}|{row.review_row_count}"
        )


if __name__ == "__main__":
    main()
