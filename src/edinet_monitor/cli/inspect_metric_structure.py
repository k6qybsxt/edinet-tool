from __future__ import annotations

import argparse
from typing import Any

from edinet_monitor.config.settings import DB_PATH
from edinet_monitor.db.schema import get_connection
from edinet_monitor.cli.save_normalized_metrics import fetch_raw_fact_rows
from edinet_monitor.services.normalizer.metric_normalize_service import (
    build_normalization_candidates,
    select_best_normalization_candidates,
)
from edinet_pipeline.domain.metric_labels import (
    metric_base_to_display_name,
    tag_name_to_display_name,
)


DEFAULT_METRIC_BASES = [
    "CostOfSales",
    "SellingExpenses",
    "CostOfSalesAndSellingGeneralAndAdministrativeExpenses",
    "GrossProfit",
]


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--doc-id", default="")
    parser.add_argument("--security-code", default="")
    parser.add_argument(
        "--metric-base",
        action="append",
        dest="metric_bases",
        default=[],
        help="Repeatable. Defaults to CostOfSales/SellingExpenses/CostOfSalesAndSellingGeneralAndAdministrativeExpenses/GrossProfit.",
    )
    parser.add_argument(
        "--all-periods",
        action="store_true",
        help="Show Prior rows too. Default shows Current only.",
    )
    return parser


def _security_code_variants(security_code: str) -> list[str]:
    code = str(security_code or "").strip()
    if not code:
        return []
    variants = {code}
    if code.endswith("0") and len(code) == 5:
        variants.add(code[:-1])
    elif len(code) == 4:
        variants.add(f"{code}0")
    return sorted(variants)


def _fetch_filing_by_doc_id(conn: sqlite3.Connection, doc_id: str) -> dict[str, Any] | None:
    cur = conn.cursor()
    row = cur.execute(
        """
        SELECT
            f.doc_id,
            f.edinet_code,
            f.security_code,
            im.company_name,
            im.industry_33,
            f.form_type,
            f.period_end,
            f.submit_date,
            f.xbrl_path,
            f.zip_path
        FROM filings f
        LEFT JOIN issuer_master im
            ON im.edinet_code = f.edinet_code
        WHERE f.doc_id = ?
        LIMIT 1
        """,
        (doc_id,),
    ).fetchone()
    return dict(row) if row else None


def _fetch_latest_filing_by_security_code(
    conn: sqlite3.Connection,
    security_code: str,
) -> dict[str, Any] | None:
    variants = _security_code_variants(security_code)
    if not variants:
        return None
    placeholders = ",".join("?" for _ in variants)
    cur = conn.cursor()
    row = cur.execute(
        f"""
        SELECT
            f.doc_id,
            f.edinet_code,
            COALESCE(f.security_code, im.security_code) AS security_code,
            im.company_name,
            im.industry_33,
            f.form_type,
            f.period_end,
            f.submit_date,
            f.xbrl_path,
            f.zip_path
        FROM filings f
        LEFT JOIN issuer_master im
            ON im.edinet_code = f.edinet_code
        WHERE COALESCE(f.security_code, im.security_code) IN ({placeholders})
          AND f.form_type = '030000'
        ORDER BY COALESCE(f.submit_date, '') DESC,
                 COALESCE(f.period_end, '') DESC,
                 f.doc_id DESC
        LIMIT 1
        """,
        tuple(variants),
    ).fetchone()
    return dict(row) if row else None


def _format_number(value: Any) -> str:
    if value is None:
        return ""
    try:
        value_num = float(value)
    except Exception:
        return str(value)
    if value_num.is_integer():
        return f"{int(value_num):,}"
    return f"{value_num:,.4f}"


def _print_filing(filing: dict[str, Any]) -> None:
    print(f"db_path={DB_PATH}")
    print(f"doc_id={filing.get('doc_id', '')}")
    print(f"company_name={filing.get('company_name', '')}")
    print(f"security_code={filing.get('security_code', '')}")
    print(f"industry_33={filing.get('industry_33', '')}")
    print(f"period_end={filing.get('period_end', '')}")
    print(f"submit_date={filing.get('submit_date', '')}")
    print(f"xbrl_path={filing.get('xbrl_path', '')}")
    print(f"zip_path={filing.get('zip_path', '')}")


def main() -> None:
    args = build_arg_parser().parse_args()

    conn = get_connection()
    try:
        if args.doc_id:
            filing = _fetch_filing_by_doc_id(conn, args.doc_id)
        else:
            filing = _fetch_latest_filing_by_security_code(conn, args.security_code)

        if not filing:
            raise SystemExit("target_filing_not_found")

        metric_bases = args.metric_bases or list(DEFAULT_METRIC_BASES)
        raw_rows = fetch_raw_fact_rows(conn, str(filing["doc_id"]))
        candidates = build_normalization_candidates(
            raw_rows,
            edinet_code=str(filing.get("edinet_code") or ""),
            security_code=str(filing.get("security_code") or ""),
            xbrl_path=str(filing.get("xbrl_path") or ""),
            zip_path=str(filing.get("zip_path") or ""),
        )
        selected_rows = select_best_normalization_candidates(candidates)
    finally:
        conn.close()

    selected_by_key = {
        str(row.get("metric_key") or ""): row
        for row in selected_rows
    }

    _print_filing(filing)

    for metric_base in metric_bases:
        display_name = metric_base_to_display_name(metric_base, filing.get("industry_33"))
        print("")
        print(f"[{metric_base} | {display_name}]")

        metric_candidates = [
            row
            for row in candidates
            if str(row.get("_metric_base") or "") == metric_base
            and (args.all_periods or str(row.get("metric_key") or "").endswith("Current"))
        ]
        metric_candidates.sort(
            key=lambda row: (
                str(row.get("metric_key") or ""),
                row.get("_consolidation_rank", 9999),
                row.get("_tag_priority", 9999),
                row.get("_structure_priority", 9999),
                row.get("_manual_override_priority", 9999),
                str(row.get("source_tag") or ""),
            )
        )

        selected_key = f"{metric_base}Current"
        selected = selected_by_key.get(selected_key)
        if selected is None and args.all_periods:
            selected = next(
                (row for row in selected_rows if str(row.get("metric_key") or "").startswith(metric_base)),
                None,
            )

        if selected is None:
            print("selected=none")
        else:
            print(
                "selected="
                + " | ".join(
                    [
                        f"metric_key={selected.get('metric_key', '')}",
                        f"source_tag={selected.get('source_tag', '')}",
                        f"source_label={tag_name_to_display_name(selected.get('source_tag'), filing.get('industry_33'))}",
                        f"value={_format_number(selected.get('value_num'))}",
                        f"role={selected.get('_structure_role', '')}",
                        f"is_total={selected.get('_structure_is_total', False)}",
                    ]
                )
            )

        if not metric_candidates:
            print("candidates=none")
            continue

        print(f"candidate_count={len(metric_candidates)}")
        for idx, row in enumerate(metric_candidates, start=1):
            parent_labels = ",".join(row.get("_structure_parent_labels") or [])
            print(
                f"{idx}. "
                + " | ".join(
                    [
                        f"metric_key={row.get('metric_key', '')}",
                        f"source_tag={row.get('source_tag', '')}",
                        f"source_label={tag_name_to_display_name(row.get('source_tag'), filing.get('industry_33'))}",
                        f"value={_format_number(row.get('value_num'))}",
                        f"consolidation={row.get('consolidation', '')}",
                        f"tag_priority={row.get('_tag_priority', '')}",
                        f"structure_priority={row.get('_structure_priority', '')}",
                        f"manual_priority={row.get('_manual_override_priority', '')}",
                        f"role={row.get('_structure_role', '')}",
                        f"confidence={row.get('_structure_confidence', '')}",
                        f"is_total={row.get('_structure_is_total', False)}",
                        f"parents={parent_labels}",
                    ]
                )
            )


if __name__ == "__main__":
    main()
