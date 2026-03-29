from __future__ import annotations

from pathlib import Path

from edinet_monitor.db.schema import get_connection
from edinet_monitor.services.collector.download_queue_service import (
    fetch_xbrl_ready_filings,
    mark_raw_facts_error,
    mark_raw_facts_saved,
)
from edinet_monitor.services.parser.raw_fact_mapper import to_raw_fact_rows
from edinet_monitor.services.parser.raw_fact_store_service import (
    delete_raw_facts_by_doc_id,
    insert_raw_facts,
)
from edinet_monitor.services.parser.xbrl_parse_service import parse_xbrl_to_raw


def main() -> None:
    conn = get_connection()
    try:
        rows = fetch_xbrl_ready_filings(conn, limit=5)
        print(f"xbrl_ready_rows={len(rows)}")

        for row in rows:
            doc_id = row["doc_id"]
            xbrl_path = Path(row["xbrl_path"])

            print(f"[DEBUG] target_doc_id={doc_id}")
            print(f"[DEBUG] xbrl_path={xbrl_path}")

            try:
                parsed = parse_xbrl_to_raw(xbrl_path)
                raw_rows = to_raw_fact_rows(doc_id, parsed)

                delete_raw_facts_by_doc_id(conn, doc_id)
                saved_count = insert_raw_facts(conn, raw_rows)
                mark_raw_facts_saved(conn, doc_id)

                print(f"saved_raw_facts doc_id={doc_id} count={saved_count}")
            except Exception as e:
                mark_raw_facts_error(conn, doc_id)
                print(f"raw_facts_error doc_id={doc_id} error={repr(e)}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()