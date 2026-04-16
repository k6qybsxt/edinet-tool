from __future__ import annotations

import argparse
import sqlite3
from typing import Any

from edinet_monitor.config.settings import DEFAULT_DERIVED_METRICS_RULE_VERSION
from edinet_monitor.db.schema import create_tables, get_connection
from edinet_monitor.cli.save_derived_metrics import (
    ensure_filing_parse_metadata,
    fetch_normalized_metric_rows,
)
from edinet_monitor.cli.save_normalized_metrics import fetch_raw_fact_rows
from edinet_monitor.services.collector.download_queue_service import (
    mark_derived_metrics_saved,
    mark_normalized_metrics_saved,
)
from edinet_monitor.services.derived_metrics.derived_metric_service import calculate_derived_metrics
from edinet_monitor.services.derived_metrics.derived_metric_store_service import (
    delete_derived_metrics_by_doc_id,
    insert_derived_metrics,
)
from edinet_monitor.services.normalizer.metric_normalize_service import normalize_raw_fact_rows
from edinet_monitor.services.normalizer.normalized_metric_store_service import (
    delete_normalized_metrics_by_doc_id,
    insert_normalized_metrics,
)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--industry-33",
        action="append",
        dest="industry_33_list",
        default=[],
        help="Repeatable. Filter by issuer_master.industry_33.",
    )
    parser.add_argument(
        "--security-code",
        action="append",
        dest="security_codes",
        default=[],
        help="Repeatable. 4 or 5 digit code accepted.",
    )
    parser.add_argument(
        "--latest-only",
        action="store_true",
        help="Rebuild only the latest annual filing per issuer.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Optional hard limit after filtering.",
    )
    return parser


def _security_code_variants(security_code: str) -> list[str]:
    code = str(security_code or "").strip()
    if not code:
        return []
    variants = {code}
    if len(code) == 4:
        variants.add(f"{code}0")
    if len(code) == 5 and code.endswith("0"):
        variants.add(code[:-1])
    return sorted(variants)


def fetch_scope_filings(
    conn: sqlite3.Connection,
    *,
    industry_33_list: list[str],
    security_codes: list[str],
    latest_only: bool,
    limit: int,
) -> list[dict[str, Any]]:
    security_variants: list[str] = []
    for code in security_codes:
        security_variants.extend(_security_code_variants(code))
    security_variants = sorted(set(security_variants))

    where_clauses = ["f.form_type = '030000'"]
    params: list[Any] = []

    if industry_33_list:
        placeholders = ",".join("?" for _ in industry_33_list)
        where_clauses.append(f"im.industry_33 IN ({placeholders})")
        params.extend(industry_33_list)

    if security_variants:
        placeholders = ",".join("?" for _ in security_variants)
        where_clauses.append(f"COALESCE(f.security_code, im.security_code) IN ({placeholders})")
        params.extend(security_variants)

    where_sql = " AND ".join(where_clauses)

    if latest_only:
        sql = f"""
        WITH scoped AS (
            SELECT
                f.doc_id,
                f.edinet_code,
                COALESCE(f.security_code, im.security_code) AS security_code,
                im.company_name,
                im.industry_33,
                f.form_type,
                f.period_end,
                f.submit_date,
                f.accounting_standard,
                f.document_display_unit,
                f.xbrl_path,
                f.zip_path,
                ROW_NUMBER() OVER (
                    PARTITION BY f.edinet_code
                    ORDER BY COALESCE(f.submit_date, '') DESC,
                             COALESCE(f.period_end, '') DESC,
                             f.doc_id DESC
                ) AS row_num
            FROM filings f
            INNER JOIN issuer_master im
                ON im.edinet_code = f.edinet_code
            WHERE {where_sql}
        )
        SELECT
            doc_id,
            edinet_code,
            security_code,
            company_name,
            industry_33,
            form_type,
            period_end,
            submit_date,
            accounting_standard,
            document_display_unit,
            xbrl_path,
            zip_path
        FROM scoped
        WHERE row_num = 1
        ORDER BY COALESCE(submit_date, '') DESC,
                 COALESCE(period_end, '') DESC,
                 doc_id DESC
        """
    else:
        sql = f"""
        SELECT
            f.doc_id,
            f.edinet_code,
            COALESCE(f.security_code, im.security_code) AS security_code,
            im.company_name,
            im.industry_33,
            f.form_type,
            f.period_end,
            f.submit_date,
            f.accounting_standard,
            f.document_display_unit,
            f.xbrl_path,
            f.zip_path
        FROM filings f
        INNER JOIN issuer_master im
            ON im.edinet_code = f.edinet_code
        WHERE {where_sql}
        ORDER BY COALESCE(f.submit_date, '') DESC,
                 COALESCE(f.period_end, '') DESC,
                 f.doc_id DESC
        """

    if limit > 0:
        sql += "\nLIMIT ?"
        params.append(limit)

    rows = conn.execute(sql, tuple(params)).fetchall()
    return [dict(row) for row in rows]


def rebuild_metrics_for_scope(
    *,
    industry_33_list: list[str],
    security_codes: list[str],
    latest_only: bool,
    limit: int,
    rule_version: str = DEFAULT_DERIVED_METRICS_RULE_VERSION,
) -> dict[str, Any]:
    create_tables()
    conn = get_connection()
    conn.row_factory = sqlite3.Row

    summary = {
        "target_docs": 0,
        "normalized_saved_docs": 0,
        "normalized_saved_rows": 0,
        "derived_saved_docs": 0,
        "derived_saved_rows": 0,
    }

    try:
        filings = fetch_scope_filings(
            conn,
            industry_33_list=industry_33_list,
            security_codes=security_codes,
            latest_only=latest_only,
            limit=limit,
        )
        summary["target_docs"] = len(filings)
        print(f"target_docs={len(filings)}")

        for filing in filings:
            doc_id = str(filing["doc_id"])
            print(f"[DEBUG] target_doc_id={doc_id} company={filing.get('company_name', '')}")

            raw_rows = fetch_raw_fact_rows(conn, doc_id)
            normalized_rows = normalize_raw_fact_rows(
                raw_rows,
                edinet_code=str(filing.get("edinet_code") or ""),
                security_code=str(filing.get("security_code") or ""),
                xbrl_path=str(filing.get("xbrl_path") or ""),
                zip_path=str(filing.get("zip_path") or ""),
            )
            delete_normalized_metrics_by_doc_id(conn, doc_id)
            normalized_saved_count = insert_normalized_metrics(conn, normalized_rows)
            mark_normalized_metrics_saved(conn, doc_id)
            summary["normalized_saved_docs"] += 1
            summary["normalized_saved_rows"] += normalized_saved_count

            filing = ensure_filing_parse_metadata(conn, filing)
            normalized_rows = fetch_normalized_metric_rows(conn, doc_id)
            derived_rows = calculate_derived_metrics(
                normalized_rows,
                form_type=str(filing.get("form_type") or ""),
                accounting_standard=str(filing.get("accounting_standard") or ""),
                document_display_unit=str(filing.get("document_display_unit") or ""),
                rule_version=rule_version,
            )
            delete_derived_metrics_by_doc_id(conn, doc_id)
            derived_saved_count = insert_derived_metrics(conn, derived_rows)
            mark_derived_metrics_saved(conn, doc_id)
            summary["derived_saved_docs"] += 1
            summary["derived_saved_rows"] += derived_saved_count

            print(
                " | ".join(
                    [
                        f"doc_id={doc_id}",
                        f"normalized_rows={normalized_saved_count}",
                        f"derived_rows={derived_saved_count}",
                    ]
                )
            )
    finally:
        conn.close()

    print(f"normalized_saved_docs={summary['normalized_saved_docs']}")
    print(f"normalized_saved_rows={summary['normalized_saved_rows']}")
    print(f"derived_saved_docs={summary['derived_saved_docs']}")
    print(f"derived_saved_rows={summary['derived_saved_rows']}")
    print(f"rule_version={rule_version}")
    return summary


def main() -> None:
    args = build_arg_parser().parse_args()
    rebuild_metrics_for_scope(
        industry_33_list=args.industry_33_list,
        security_codes=args.security_codes,
        latest_only=args.latest_only,
        limit=args.limit,
    )


if __name__ == "__main__":
    main()
