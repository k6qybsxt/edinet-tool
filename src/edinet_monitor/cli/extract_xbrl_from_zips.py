from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

from edinet_monitor.db.schema import get_connection
from edinet_monitor.services.collector.download_queue_service import (
    fetch_downloaded_filings_without_xbrl,
    mark_xbrl_extract_error,
    mark_xbrl_extract_success,
)
from edinet_monitor.services.collector.document_filter_service import normalize_form_codes
from edinet_monitor.services.edinet_storage_path_service import resolve_storage_paths
from edinet_monitor.services.segment_scope_service import fetch_segment_scope_filings
from edinet_monitor.services.storage.path_service import build_xbrl_save_path
from edinet_monitor.services.storage.zip_extract_service import (
    extract_period_end_from_xbrl_member_name,
    extract_preferred_xbrl,
    find_xbrl_member_names,
)


def run_extract_xbrl_from_zips(
    *,
    batch_size: int = 20,
    run_all: bool = False,
    form_codes: tuple[str, ...] | None = None,
    period_ranks: str | None = None,
    codes: tuple[str, ...] | None = None,
    force: bool = False,
) -> dict[str, Any]:
    conn = get_connection()
    target_form_codes = normalize_form_codes(form_codes)
    total_target = 0
    total_extracted = 0
    total_errors = 0
    loop_count = 0

    try:
        if period_ranks:
            rows = fetch_segment_scope_filings(
                conn,
                form_codes=target_form_codes,
                period_ranks=period_ranks,
                codes=list(codes or ()),
            )
            print(f"period_rank_scope_rows={len(rows)}")
            total_target = len(rows)
            for row in rows:
                doc_id = row["doc_id"]
                form_type = str(row["form_type"] or "")
                submit_date = row["submit_date"]
                resolved = resolve_storage_paths(row)
                zip_path = resolved.zip_path
                current_xbrl = resolved.xbrl_path
                if zip_path is None:
                    total_errors += 1
                    print(f"extract_error doc_id={doc_id} error=zip_missing")
                    continue
                if current_xbrl is not None and not force:
                    total_extracted += 1
                    print(f"existing_xbrl doc_id={doc_id} xbrl_path={current_xbrl}")
                    continue
                try:
                    member_names = find_xbrl_member_names(zip_path)
                    print(f"[DEBUG] target_doc_id={doc_id}")
                    print(f"[DEBUG] zip_path={zip_path}")
                    print(f"[DEBUG] xbrl_members={member_names[:5]}")
                    xbrl_path = build_xbrl_save_path(submit_date, doc_id)
                    extracted = extract_preferred_xbrl(zip_path, xbrl_path, form_type=form_type)
                    conn.execute(
                        """
                        UPDATE filings
                        SET zip_path = ?,
                            xbrl_path = ?,
                            xbrl_member_name = ?,
                            period_end = CASE WHEN ? <> '' THEN ? ELSE period_end END
                        WHERE doc_id = ?
                        """,
                        (
                            str(zip_path),
                            str(extracted.output_path),
                            extracted.member_name,
                            extract_period_end_from_xbrl_member_name(extracted.member_name),
                            extract_period_end_from_xbrl_member_name(extracted.member_name),
                            doc_id,
                        ),
                    )
                    conn.commit()
                    total_extracted += 1
                    print(
                        f"extracted doc_id={doc_id} "
                        f"xbrl_path={extracted.output_path} "
                        f"xbrl_member_name={extracted.member_name}"
                    )
                except Exception as e:
                    total_errors += 1
                    print(f"extract_error doc_id={doc_id} error={repr(e)}")
            print(f"xbrl_extract_target_total={total_target}")
            print(f"xbrl_extracted_total={total_extracted}")
            print(f"xbrl_extract_error_total={total_errors}")
            return {
                "loop_count": 1,
                "target_total": total_target,
                "extracted_total": total_extracted,
                "error_total": total_errors,
            }

        while True:
            rows = fetch_downloaded_filings_without_xbrl(
                conn,
                limit=batch_size,
                form_codes=target_form_codes,
            )
            print(f"downloaded_rows_without_xbrl={len(rows)}")

            if not rows:
                break

            loop_count += 1
            total_target += len(rows)

            for row in rows:
                doc_id = row["doc_id"]
                form_type = str(row["form_type"] or "")
                submit_date = row["submit_date"]
                zip_path = Path(row["zip_path"])

                print(f"[DEBUG] target_doc_id={doc_id}")
                print(f"[DEBUG] zip_path={zip_path}")

                try:
                    member_names = find_xbrl_member_names(zip_path)
                    print(f"[DEBUG] xbrl_members={member_names[:5]}")

                    xbrl_path = build_xbrl_save_path(submit_date, doc_id)
                    extracted = extract_preferred_xbrl(zip_path, xbrl_path, form_type=form_type)

                    mark_xbrl_extract_success(
                        conn,
                        doc_id,
                        str(extracted.output_path),
                        extracted.member_name,
                        period_end=extract_period_end_from_xbrl_member_name(extracted.member_name),
                    )
                    total_extracted += 1
                    print(
                        f"extracted doc_id={doc_id} "
                        f"xbrl_path={extracted.output_path} "
                        f"xbrl_member_name={extracted.member_name}"
                    )
                except Exception as e:
                    mark_xbrl_extract_error(conn, doc_id)
                    total_errors += 1
                    print(f"extract_error doc_id={doc_id} error={repr(e)}")

            if not run_all:
                break
    finally:
        conn.close()

    print(f"xbrl_extract_target_total={total_target}")
    print(f"xbrl_extracted_total={total_extracted}")
    print(f"xbrl_extract_error_total={total_errors}")

    return {
        "loop_count": loop_count,
        "target_total": total_target,
        "extracted_total": total_extracted,
        "error_total": total_errors,
    }


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-size", type=int, default=20)
    parser.add_argument("--run-all", action="store_true")
    parser.add_argument("--form-codes", default="", help="Comma-separated form codes. Example: 043A00")
    parser.add_argument("--period-ranks", default="", help="Comma-separated: latest,5,10")
    parser.add_argument("--codes", default="all", help="Comma-separated security codes, or all.")
    parser.add_argument("--force", action="store_true", help="Re-extract even when xbrl_path already exists.")
    return parser


def main() -> None:
    args = build_arg_parser().parse_args()
    run_extract_xbrl_from_zips(
        batch_size=args.batch_size,
        run_all=args.run_all,
        form_codes=normalize_form_codes(args.form_codes or None),
        period_ranks=args.period_ranks or None,
        codes=tuple(part.strip() for part in str(args.codes or "").split(",") if part.strip() and part.strip().lower() != "all"),
        force=args.force,
    )


if __name__ == "__main__":
    main()
