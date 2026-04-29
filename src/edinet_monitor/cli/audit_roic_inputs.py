from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path
from typing import Any

from edinet_monitor.config.settings import DEFAULT_DERIVED_METRICS_RULE_VERSION
from edinet_monitor.db.schema import get_connection
from edinet_monitor.services.derived_metrics.derived_metric_service import calculate_derived_metrics
from edinet_monitor.services.derived_metrics.historical_growth_reference_service import (
    fetch_historical_growth_values,
)
from edinet_monitor.services.metric_audit_service import (
    format_number,
    now_stamp,
    pad_left,
    pad_right,
    write_text_report,
)
from edinet_pipeline.domain.metric_labels import metric_key_to_display_name


DEFAULT_OUTPUT_DIR = Path(r"D:\作業用")


def _normalize_security_code(value: str) -> str:
    text = str(value or "").strip()
    if len(text) == 5 and text.endswith("0"):
        return text[:4]
    return text


def _fetch_filing(
    conn: sqlite3.Connection,
    *,
    doc_id: str | None,
    security_code: str | None,
    period_end: str | None,
) -> dict[str, Any]:
    if doc_id:
        row = conn.execute(
            """
            SELECT
              f.*,
              im.company_name,
              im.industry_33
            FROM filings f
            JOIN issuer_master im
              ON im.edinet_code = f.edinet_code
            WHERE f.doc_id = ?
            """,
            (doc_id,),
        ).fetchone()
    else:
        code = _normalize_security_code(str(security_code or ""))
        variants = [code, f"{code}0"] if code else []
        placeholders = ",".join("?" for _ in variants)
        row = conn.execute(
            f"""
            SELECT
              f.*,
              im.company_name,
              im.industry_33
            FROM filings f
            JOIN issuer_master im
              ON im.edinet_code = f.edinet_code
            WHERE COALESCE(NULLIF(f.security_code, ''), NULLIF(im.security_code, '')) IN ({placeholders})
              AND f.period_end = ?
            ORDER BY f.submit_date DESC, f.doc_id DESC
            LIMIT 1
            """,
            (*variants, period_end),
        ).fetchone()
    if not row:
        raise SystemExit("対象filingが見つかりません。--doc-id または --security-code + --period-end を確認してください。")
    return dict(row)


def _fetch_normalized_rows(conn: sqlite3.Connection, doc_id: str) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT
          doc_id,
          edinet_code,
          security_code,
          metric_key,
          fiscal_year,
          period_end,
          value_num,
          source_tag,
          consolidation,
          rule_version
        FROM normalized_metrics
        WHERE doc_id = ?
        ORDER BY metric_key
        """,
        (doc_id,),
    ).fetchall()
    return [dict(row) for row in rows]


def _metric_row(rows: list[dict[str, Any]], metric_key: str) -> dict[str, Any] | None:
    for row in rows:
        if str(row.get("metric_key") or "") == metric_key:
            return row
    return None


def _detail(row: dict[str, Any] | None) -> dict[str, Any]:
    if not row:
        return {}
    value = row.get("source_detail_json")
    if isinstance(value, dict):
        return value
    try:
        return json.loads(str(value or "{}"))
    except Exception:
        return {}


def _format_inputs_table(inputs: dict[str, Any]) -> list[str]:
    rows = [(key, value) for key, value in sorted(inputs.items())]
    if not rows:
        return ["(no inputs)"]
    key_width = max(10, *(len(key) for key, _value in rows))
    lines = [f"{pad_right('input', key_width)} | {pad_left('value', 16)}"]
    lines.append(f"{'-' * key_width}-+-{'-' * 16}")
    for key, value in rows:
        lines.append(f"{pad_right(key, key_width)} | {pad_left(format_number(value), 16)}")
    return lines


def build_roic_audit_lines(
    *,
    filing: dict[str, Any],
    normalized_rows: list[dict[str, Any]],
    derived_rows: list[dict[str, Any]],
) -> list[str]:
    roic_row = _metric_row(derived_rows, "ROICCurrent")
    debt_row = _metric_row(derived_rows, "InterestBearingDebtCurrent")
    roic_detail = _detail(roic_row)
    debt_detail = _detail(debt_row)
    lines = [
        f"generated_at: {now_stamp()}",
        "report: roic_input_audit",
        f"doc_id: {filing.get('doc_id', '')}",
        f"company_name: {filing.get('company_name', '')}",
        f"security_code: {_normalize_security_code(str(filing.get('security_code') or ''))}",
        f"industry_33: {filing.get('industry_33', '')}",
        f"form_type: {filing.get('form_type', '')}",
        f"period_end: {filing.get('period_end', '')}",
        "",
        "=== derived result ===",
    ]
    for row in [debt_row, roic_row]:
        if not row:
            lines.append("missing derived row")
            continue
        lines.append(
            " | ".join(
                [
                    f"metric={row['metric_key']}",
                    f"label={metric_key_to_display_name(row['metric_key'], filing.get('industry_33'))}",
                    f"value={format_number(row.get('value_num'))}",
                    f"status={row.get('calc_status', '')}",
                ]
            )
        )
    lines.extend(
        [
            "",
            "=== interest-bearing debt selection ===",
            f"selected_tier: {debt_detail.get('selected_tier', '') or (debt_detail.get('debt_detail') or {}).get('selected_tier', '')}",
            f"included_bases: {', '.join((debt_detail.get('included_bases') or []))}",
            f"excluded_bases_due_to_precedence: {', '.join((debt_detail.get('excluded_bases_due_to_precedence') or []))}",
            "",
            "=== ROIC inputs ===",
        ]
    )
    lines.extend(_format_inputs_table(roic_detail.get("inputs") or {}))
    lines.extend(
        [
            "",
            "=== normalized ROIC-related rows ===",
        ]
    )
    related_prefixes = (
        "OperatingIncome",
        "ProfitBeforeTax",
        "IncomeTaxes",
        "NetAssets",
        "CashAndCashEquivalents",
        "InterestBearing",
        "BondsAndBorrowings",
        "Borrowings",
        "LeaseLiabilities",
        "ShortTermLoansPayable",
        "CurrentPortionOfLongTermLoansPayable",
        "LongTermLoansPayable",
        "BondsPayable",
        "CurrentPortionOfBonds",
        "ShortTermBondsPayable",
        "CommercialPapersLiabilities",
    )
    for row in normalized_rows:
        key = str(row.get("metric_key") or "")
        if not key.startswith(related_prefixes):
            continue
        lines.append(
            " | ".join(
                [
                    f"metric={key}",
                    f"value={format_number(row.get('value_num'))}",
                    f"tag={row.get('source_tag', '')}",
                    f"consolidation={row.get('consolidation', '')}",
                ]
            )
        )
    return lines


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="ROICと有利子負債の採用根拠を監査します。")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--doc-id")
    group.add_argument("--security-code")
    parser.add_argument("--period-end", help="--security-code 指定時に必須。例: 2026-03-31")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    return parser


def main() -> None:
    args = build_parser().parse_args()
    if args.security_code and not args.period_end:
        raise SystemExit("--security-code を使う場合は --period-end も指定してください。")
    conn = get_connection()
    conn.row_factory = sqlite3.Row
    try:
        filing = _fetch_filing(
            conn,
            doc_id=args.doc_id,
            security_code=args.security_code,
            period_end=args.period_end,
        )
        normalized_rows = _fetch_normalized_rows(conn, str(filing["doc_id"]))
        historical_growth_values = fetch_historical_growth_values(conn, filing)
        derived_rows = calculate_derived_metrics(
            normalized_rows,
            form_type=str(filing.get("form_type") or ""),
            industry_33=str(filing.get("industry_33") or ""),
            accounting_standard=str(filing.get("accounting_standard") or ""),
            document_display_unit=str(filing.get("document_display_unit") or ""),
            rule_version=DEFAULT_DERIVED_METRICS_RULE_VERSION,
            historical_growth_values=historical_growth_values,
        )
        lines = build_roic_audit_lines(
            filing=filing,
            normalized_rows=normalized_rows,
            derived_rows=derived_rows,
        )
        output_dir = Path(args.output_dir)
        output_path = output_dir / f"roic_audit_{filing['doc_id']}_{now_stamp()}.txt"
        write_text_report(output_path, lines)
        print(f"saved: {output_path}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
