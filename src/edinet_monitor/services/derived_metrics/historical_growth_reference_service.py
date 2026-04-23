from __future__ import annotations

import sqlite3
from datetime import date
from typing import Any


HISTORICAL_GROWTH_OFFSETS = (9,)
HISTORICAL_NORMALIZED_METRIC_KEYS = {
    "NetSales": "NetSalesCurrent",
    "OrdinaryIncome": "OrdinaryIncomeCurrent",
    "CashAndCashEquivalents": "CashAndCashEquivalentsCurrent",
}
OUTSTANDING_SHARES_COMPONENT_KEYS = {
    "IssuedSharesCurrent",
    "TreasurySharesCurrent",
}


def _parse_date(value: Any) -> date | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return date.fromisoformat(text[:10])
    except Exception:
        return None


def _shift_year(value: date, years: int) -> date:
    try:
        return value.replace(year=value.year - years)
    except ValueError:
        return value.replace(year=value.year - years, day=28)


def _to_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except Exception:
        return None


def _outstanding_shares_value(issued_shares: float | None, treasury_shares: float | None) -> tuple[float | None, str]:
    if issued_shares is None:
        return None, "missing_input"
    if treasury_shares is None or treasury_shares < 1000:
        return issued_shares, "ok"
    return issued_shares - treasury_shares, "ok"


def fetch_historical_growth_values(
    conn: sqlite3.Connection,
    filing: dict[str, Any],
    *,
    offsets: tuple[int, ...] = HISTORICAL_GROWTH_OFFSETS,
) -> dict[str, dict[int, dict[str, Any]]]:
    """Fetch same-issuer annual current values used as long-term growth bases."""

    edinet_code = str(filing.get("edinet_code") or "").strip()
    current_period_end = _parse_date(filing.get("period_end"))
    if not edinet_code or not current_period_end:
        return {}
    conn.row_factory = sqlite3.Row

    target_period_ends = {
        offset: _shift_year(current_period_end, offset).isoformat()
        for offset in offsets
    }
    if not target_period_ends:
        return {}

    period_params = tuple(target_period_ends.values())
    period_placeholders = ",".join("?" for _ in period_params)

    result: dict[str, dict[int, dict[str, Any]]] = {
        metric_base: {}
        for metric_base in [*HISTORICAL_NORMALIZED_METRIC_KEYS.keys(), "OutstandingShares"]
    }

    normalized_keys = tuple(HISTORICAL_NORMALIZED_METRIC_KEYS.values())
    metric_placeholders = ",".join("?" for _ in normalized_keys)
    rows = conn.execute(
        f"""
        SELECT
            f.doc_id,
            f.period_end AS filing_period_end,
            nm.metric_key,
            nm.value_num,
            nm.period_end AS metric_period_end,
            nm.consolidation
        FROM filings f
        INNER JOIN normalized_metrics nm
            ON nm.doc_id = f.doc_id
        WHERE f.edinet_code = ?
          AND f.form_type = '030000'
          AND f.period_end IN ({period_placeholders})
          AND nm.metric_key IN ({metric_placeholders})
        """,
        (edinet_code, *period_params, *normalized_keys),
    ).fetchall()
    metric_key_to_base = {
        metric_key: metric_base
        for metric_base, metric_key in HISTORICAL_NORMALIZED_METRIC_KEYS.items()
    }
    period_end_to_offset = {period_end: offset for offset, period_end in target_period_ends.items()}
    for row in rows:
        row_dict = dict(row)
        offset = period_end_to_offset.get(str(row_dict.get("filing_period_end") or ""))
        metric_base = metric_key_to_base.get(str(row_dict.get("metric_key") or ""))
        if offset is None or not metric_base:
            continue
        result[metric_base][offset] = {
            "doc_id": row_dict.get("doc_id"),
            "metric_key": row_dict.get("metric_key"),
            "period_end": row_dict.get("metric_period_end") or row_dict.get("filing_period_end"),
            "consolidation": row_dict.get("consolidation"),
            "value_num": _to_float(row_dict.get("value_num")),
        }

    share_keys = tuple(OUTSTANDING_SHARES_COMPONENT_KEYS)
    share_placeholders = ",".join("?" for _ in share_keys)
    share_rows = conn.execute(
        f"""
        SELECT
            f.doc_id,
            f.period_end AS filing_period_end,
            nm.metric_key,
            nm.value_num,
            nm.period_end AS metric_period_end,
            nm.consolidation
        FROM filings f
        INNER JOIN normalized_metrics nm
            ON nm.doc_id = f.doc_id
        WHERE f.edinet_code = ?
          AND f.form_type = '030000'
          AND f.period_end IN ({period_placeholders})
          AND nm.metric_key IN ({share_placeholders})
        """,
        (edinet_code, *period_params, *share_keys),
    ).fetchall()
    share_components: dict[tuple[int, str], dict[str, Any]] = {}
    for row in share_rows:
        row_dict = dict(row)
        offset = period_end_to_offset.get(str(row_dict.get("filing_period_end") or ""))
        if offset is None:
            continue
        key = (offset, str(row_dict.get("doc_id") or ""))
        component = share_components.setdefault(
            key,
            {
                "doc_id": row_dict.get("doc_id"),
                "period_end": row_dict.get("metric_period_end") or row_dict.get("filing_period_end"),
                "consolidation": row_dict.get("consolidation"),
                "IssuedSharesCurrent": None,
                "TreasurySharesCurrent": None,
            },
        )
        component[str(row_dict.get("metric_key") or "")] = _to_float(row_dict.get("value_num"))

    for (offset, _doc_id), component in share_components.items():
        value_num, calc_status = _outstanding_shares_value(
            component.get("IssuedSharesCurrent"),
            component.get("TreasurySharesCurrent"),
        )
        result["OutstandingShares"][offset] = {
            "doc_id": component.get("doc_id"),
            "metric_key": "OutstandingSharesCurrent",
            "period_end": component.get("period_end"),
            "consolidation": component.get("consolidation"),
            "value_num": value_num,
            "calc_status": calc_status,
            "issued_shares": component.get("IssuedSharesCurrent"),
            "treasury_shares": component.get("TreasurySharesCurrent"),
        }

    return {metric_base: values for metric_base, values in result.items() if values}
