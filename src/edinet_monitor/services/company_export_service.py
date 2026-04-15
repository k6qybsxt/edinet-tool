from __future__ import annotations

import sqlite3
from typing import Any

from edinet_pipeline.domain.metric_labels import (
    metric_base_to_display_name,
    metric_group_to_display_name,
    metric_key_to_display_name,
    tag_name_to_display_name,
)


UNIT_HYAKUMAN = "\u767e\u4e07\u5186"
UNIT_SEN = "\u5343\u5186"


def _scale_value_for_display(
    value_num: float | None,
    *,
    value_unit: str | None,
    document_display_unit: str | None,
) -> float | None:
    if value_num is None:
        return None

    unit = str(value_unit or "").strip()
    display_unit = str(document_display_unit or "").strip()

    if unit != "yen":
        return value_num
    if display_unit == UNIT_HYAKUMAN:
        return value_num / 1_000_000
    if display_unit == UNIT_SEN:
        return value_num / 1_000
    return value_num


def _build_in_clause_params(values: list[str]) -> tuple[str, list[str]]:
    placeholders = ",".join("?" for _ in values)
    return placeholders, values


def _normalize_security_code(security_code: str) -> str:
    return str(security_code or "").strip().replace("-", "")


def _fetch_issuer_row(conn: sqlite3.Connection, security_code: str) -> sqlite3.Row | None:
    normalized = _normalize_security_code(security_code)
    if not normalized:
        return None

    return conn.execute(
        """
        SELECT *
        FROM issuer_master
        WHERE security_code = ?
           OR security_code = ?
        LIMIT 1
        """,
        (normalized, f"{normalized}0"),
    ).fetchone()


def _fetch_latest_filings(
    conn: sqlite3.Connection,
    *,
    edinet_code: str,
    years: int,
) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT *
        FROM filings
        WHERE edinet_code = ?
          AND period_end IS NOT NULL
        ORDER BY period_end DESC, submit_date DESC
        LIMIT ?
        """,
        (edinet_code, years),
    ).fetchall()


def _fetch_rows_by_doc_ids(
    conn: sqlite3.Connection,
    *,
    table_name: str,
    doc_ids: list[str],
    order_sql: str,
) -> list[sqlite3.Row]:
    if not doc_ids:
        return []

    placeholders, params = _build_in_clause_params(doc_ids)
    return conn.execute(
        f"""
        SELECT *
        FROM {table_name}
        WHERE doc_id IN ({placeholders})
        ORDER BY {order_sql}
        """,
        params,
    ).fetchall()


def _fetch_recent_screening_results(
    conn: sqlite3.Connection,
    *,
    edinet_code: str,
    limit: int,
) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT *
        FROM screening_results
        WHERE edinet_code = ?
        ORDER BY screening_date DESC, rule_name ASC
        LIMIT ?
        """,
        (edinet_code, limit),
    ).fetchall()


def _build_filing_payload(row: sqlite3.Row) -> dict[str, Any]:
    item = dict(row)
    security_code = str(item.get("security_code") or "").strip()
    if len(security_code) == 5 and security_code.endswith("0"):
        security_code = security_code[:-1]
    item["security_code"] = security_code
    return item


def _build_normalized_metric_payload(row: sqlite3.Row, filing_by_doc_id: dict[str, dict[str, Any]]) -> dict[str, Any]:
    item = dict(row)
    filing = filing_by_doc_id.get(str(row["doc_id"]), {})
    document_display_unit = filing.get("document_display_unit")
    industry_33 = filing.get("industry_33")
    item["metric_label"] = metric_key_to_display_name(row["metric_key"], industry_33)
    item["metric_base"] = metric_key_to_display_name(row["metric_key"], industry_33).split("\uff08", 1)[0]
    item["source_tag_label"] = tag_name_to_display_name(item.get("source_tag"), industry_33)
    item["document_display_unit"] = document_display_unit
    item["display_value_num"] = _scale_value_for_display(
        row["value_num"],
        value_unit="yen",
        document_display_unit=document_display_unit,
    )
    return item


def _build_derived_metric_payload(row: sqlite3.Row | dict[str, Any]) -> dict[str, Any]:
    item = dict(row)
    industry_33 = item.get("industry_33")
    item["metric_label"] = metric_key_to_display_name(item["metric_key"], industry_33)
    item["metric_base_label"] = metric_base_to_display_name(item["metric_base"], industry_33)
    item["metric_group_label"] = metric_group_to_display_name(row["metric_group"])
    item["display_value_num"] = _scale_value_for_display(
        item["value_num"],
        value_unit=item.get("value_unit"),
        document_display_unit=item.get("document_display_unit"),
    )
    return item


def _build_raw_fact_payload(row: sqlite3.Row | dict[str, Any]) -> dict[str, Any]:
    item = dict(row)
    item["tag_label"] = tag_name_to_display_name(item["tag_name"], item.get("industry_33"))
    return item


def _build_screening_result_payload(row: sqlite3.Row) -> dict[str, Any]:
    item = dict(row)
    return item


def export_company_latest_dataset(
    conn: sqlite3.Connection,
    *,
    security_code: str,
    years: int = 5,
    screening_limit: int = 20,
) -> dict[str, Any]:
    issuer = _fetch_issuer_row(conn, security_code)
    if issuer is None:
        raise ValueError(f"Company not found for security_code={security_code}")

    filings = _fetch_latest_filings(
        conn,
        edinet_code=str(issuer["edinet_code"]),
        years=years,
    )
    filing_payloads = [_build_filing_payload(row) for row in filings]
    issuer_industry_33 = str(issuer["industry_33"] or "").strip()
    for filing_payload in filing_payloads:
        filing_payload["industry_33"] = issuer_industry_33
    filing_by_doc_id = {
        str(row["doc_id"]): payload
        for row, payload in zip(filings, filing_payloads)
    }
    doc_ids = [str(row["doc_id"]) for row in filings]

    normalized_rows = _fetch_rows_by_doc_ids(
        conn,
        table_name="normalized_metrics",
        doc_ids=doc_ids,
        order_sql="period_end DESC, metric_key ASC",
    )
    derived_rows = _fetch_rows_by_doc_ids(
        conn,
        table_name="derived_metrics",
        doc_ids=doc_ids,
        order_sql="period_end DESC, metric_key ASC",
    )
    raw_fact_rows = _fetch_rows_by_doc_ids(
        conn,
        table_name="raw_facts",
        doc_ids=doc_ids,
        order_sql="period_end DESC, context_ref ASC, tag_name ASC",
    )
    screening_rows = _fetch_recent_screening_results(
        conn,
        edinet_code=str(issuer["edinet_code"]),
        limit=screening_limit,
    )

    normalized_payloads = [
        _build_normalized_metric_payload(row, filing_by_doc_id) for row in normalized_rows
    ]

    if issuer_industry_33 == "\u9280\u884c\u696d":
        existing_keys = {
            (str(item.get("doc_id") or ""), str(item.get("metric_key") or ""))
            for item in normalized_payloads
        }
        for filing_payload in filing_payloads:
            placeholder_key = (str(filing_payload["doc_id"]), "OperatingIncomeCurrent")
            if placeholder_key in existing_keys:
                continue
            normalized_payloads.append(
                {
                    "doc_id": filing_payload["doc_id"],
                    "edinet_code": filing_payload["edinet_code"],
                    "security_code": filing_payload["security_code"],
                    "metric_key": "OperatingIncomeCurrent",
                    "fiscal_year": None,
                    "period_end": filing_payload.get("period_end"),
                    "value_num": None,
                    "source_tag": None,
                    "source_tag_label": "",
                    "consolidation": None,
                    "rule_version": None,
                    "metric_label": metric_key_to_display_name("OperatingIncomeCurrent", issuer_industry_33),
                    "metric_base": metric_key_to_display_name("OperatingIncomeCurrent", issuer_industry_33).split("\uff08", 1)[0],
                    "document_display_unit": filing_payload.get("document_display_unit"),
                    "display_value_num": "-",
                    "industry_33": issuer_industry_33,
                }
            )

    return {
        "\u4f1a\u793e\u60c5\u5831": dict(issuer),
        "\u63d0\u51fa\u66f8\u985e": filing_payloads,
        "\u6b63\u898f\u5316\u6307\u6a19": normalized_payloads,
        "\u6d3e\u751f\u6307\u6a19": [
            _build_derived_metric_payload(dict(row) | {"industry_33": issuer_industry_33}) for row in derived_rows
        ],
        "\u751f\u30d5\u30a1\u30af\u30c8": [
            _build_raw_fact_payload(dict(row) | {"industry_33": issuer_industry_33}) for row in raw_fact_rows
        ],
        "\u76f4\u8fd1screening\u7d50\u679c": [
            _build_screening_result_payload(row) for row in screening_rows
        ],
        "\u4ef6\u6570": {
            "\u63d0\u51fa\u66f8\u985e": len(filing_payloads),
            "\u6b63\u898f\u5316\u6307\u6a19": len(normalized_payloads),
            "\u6d3e\u751f\u6307\u6a19": len(derived_rows),
            "\u751f\u30d5\u30a1\u30af\u30c8": len(raw_fact_rows),
            "\u76f4\u8fd1screening\u7d50\u679c": len(screening_rows),
        },
    }
