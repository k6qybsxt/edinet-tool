from __future__ import annotations

import sqlite3
from datetime import date
from typing import Any

from edinet_monitor.services.collector.document_filter_service import is_half_form_type


DEFAULT_BULK_CHUNK_SIZE = 200
HISTORICAL_GROWTH_OFFSETS = (1, 4, 9)
HISTORICAL_NORMALIZED_METRIC_KEYS = {
    "NetSales": "NetSalesCurrent",
    "OperatingIncome": "OperatingIncomeCurrent",
    "OrdinaryIncome": "OrdinaryIncomeCurrent",
    "ProfitLoss": "ProfitLossCurrent",
    "CashAndCashEquivalents": "CashAndCashEquivalentsCurrent",
}
HISTORICAL_DERIVED_METRIC_KEYS = {
    "EstimatedNetIncome": "EstimatedNetIncomeCurrent",
    "EPS": "EPSCurrent",
    "BPS": "BPSCurrent",
    "TheoreticalSharePrice": "TheoreticalSharePriceCurrent",
}
OUTSTANDING_SHARES_COMPONENT_KEYS = {
    "IssuedSharesCurrent",
    "TreasurySharesCurrent",
}
HALF_SHARE_REFERENCE_KEYS = {
    "IssuedShares": "IssuedSharesCurrent",
    "TreasuryShares": "TreasurySharesCurrent",
}
HALF_PROGRESS_ANNUAL_METRIC_KEYS = {
    "NetSales": "NetSalesCurrent",
    "OrdinaryIncome": "OrdinaryIncomeCurrent",
    "ProfitBeforeTax": "ProfitBeforeTaxCurrent",
    "ProfitLoss": "ProfitLossCurrent",
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


def _chunked(items: list[Any], chunk_size: int) -> list[list[Any]]:
    size = max(int(chunk_size or 1), 1)
    return [items[index:index + size] for index in range(0, len(items), size)]


def _reference_group_for_form_type(form_type: Any) -> str:
    return "half" if is_half_form_type(form_type) else "annual"


def _reference_group_for_reference_form_type(form_type: Any) -> str | None:
    text = str(form_type or "")
    if text == "030000":
        return "annual"
    if text in {"043A00", "043000"}:
        return "half"
    return None


def fetch_historical_growth_values(
    conn: sqlite3.Connection,
    filing: dict[str, Any],
    *,
    offsets: tuple[int, ...] = HISTORICAL_GROWTH_OFFSETS,
) -> dict[str, dict[int, dict[str, Any]]]:
    """Fetch same-issuer current values used as long-term growth bases."""

    edinet_code = str(filing.get("edinet_code") or "").strip()
    current_period_end = _parse_date(filing.get("period_end"))
    if not edinet_code or not current_period_end:
        return {}
    conn.row_factory = sqlite3.Row
    reference_form_types = ("043A00", "043000") if is_half_form_type(filing.get("form_type")) else ("030000",)
    form_type_placeholders = ",".join("?" for _ in reference_form_types)

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
        for metric_base in [
            *HISTORICAL_NORMALIZED_METRIC_KEYS.keys(),
            *HISTORICAL_DERIVED_METRIC_KEYS.keys(),
            "OutstandingShares",
        ]
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
          AND f.form_type IN ({form_type_placeholders})
          AND f.period_end IN ({period_placeholders})
          AND nm.metric_key IN ({metric_placeholders})
        """,
        (edinet_code, *reference_form_types, *period_params, *normalized_keys),
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

    derived_keys = tuple(HISTORICAL_DERIVED_METRIC_KEYS.values())
    derived_placeholders = ",".join("?" for _ in derived_keys)
    derived_rows = conn.execute(
        f"""
        SELECT
            f.doc_id,
            f.period_end AS filing_period_end,
            dm.metric_key,
            dm.value_num,
            dm.period_end AS metric_period_end,
            dm.consolidation,
            dm.calc_status
        FROM filings f
        INNER JOIN derived_metrics dm
            ON dm.doc_id = f.doc_id
        WHERE f.edinet_code = ?
          AND f.form_type IN ({form_type_placeholders})
          AND f.period_end IN ({period_placeholders})
          AND dm.metric_key IN ({derived_placeholders})
        """,
        (edinet_code, *reference_form_types, *period_params, *derived_keys),
    ).fetchall()
    derived_key_to_base = {
        metric_key: metric_base
        for metric_base, metric_key in HISTORICAL_DERIVED_METRIC_KEYS.items()
    }
    for row in derived_rows:
        row_dict = dict(row)
        offset = period_end_to_offset.get(str(row_dict.get("filing_period_end") or ""))
        metric_base = derived_key_to_base.get(str(row_dict.get("metric_key") or ""))
        if offset is None or not metric_base:
            continue
        if str(row_dict.get("calc_status") or "") == "missing_input":
            value_num = None
        else:
            value_num = _to_float(row_dict.get("value_num"))
        result[metric_base][offset] = {
            "doc_id": row_dict.get("doc_id"),
            "metric_key": row_dict.get("metric_key"),
            "period_end": row_dict.get("metric_period_end") or row_dict.get("filing_period_end"),
            "consolidation": row_dict.get("consolidation"),
            "value_num": value_num,
            "calc_status": row_dict.get("calc_status"),
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
          AND f.form_type IN ({form_type_placeholders})
          AND f.period_end IN ({period_placeholders})
          AND nm.metric_key IN ({share_placeholders})
        """,
        (edinet_code, *reference_form_types, *period_params, *share_keys),
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


def fetch_historical_growth_values_bulk(
    conn: sqlite3.Connection,
    filings: list[dict[str, Any]],
    *,
    offsets: tuple[int, ...] = HISTORICAL_GROWTH_OFFSETS,
    chunk_size: int = DEFAULT_BULK_CHUNK_SIZE,
) -> dict[str, dict[str, dict[int, dict[str, Any]]]]:
    """Fetch historical growth references for multiple filings in batch."""

    conn.row_factory = sqlite3.Row
    requests: list[dict[str, Any]] = []
    for filing in filings:
        doc_id = str(filing.get("doc_id") or "").strip()
        edinet_code = str(filing.get("edinet_code") or "").strip()
        current_period_end = _parse_date(filing.get("period_end"))
        if not doc_id or not edinet_code or not current_period_end:
            continue
        target_period_ends = {
            offset: _shift_year(current_period_end, offset).isoformat()
            for offset in offsets
        }
        if not target_period_ends:
            continue
        requests.append(
            {
                "doc_id": doc_id,
                "edinet_code": edinet_code,
                "reference_group": _reference_group_for_form_type(filing.get("form_type")),
                "target_period_ends": target_period_ends,
            }
        )

    if not requests:
        return {}

    result: dict[str, dict[str, dict[int, dict[str, Any]]]] = {}

    def add_result(doc_id: str, metric_base: str, offset: int, value: dict[str, Any]) -> None:
        result.setdefault(doc_id, {}).setdefault(metric_base, {})[offset] = value

    normalized_key_to_base = {
        metric_key: metric_base
        for metric_base, metric_key in HISTORICAL_NORMALIZED_METRIC_KEYS.items()
    }
    derived_key_to_base = {
        metric_key: metric_base
        for metric_base, metric_key in HISTORICAL_DERIVED_METRIC_KEYS.items()
    }

    for chunk in _chunked(requests, chunk_size):
        target_lookup: dict[tuple[str, str, str], list[tuple[str, int]]] = {}
        edinet_codes: set[str] = set()
        period_ends: set[str] = set()
        for request in chunk:
            edinet_codes.add(str(request["edinet_code"]))
            for offset, period_end in dict(request["target_period_ends"]).items():
                period_ends.add(str(period_end))
                target_lookup.setdefault(
                    (
                        str(request["edinet_code"]),
                        str(period_end),
                        str(request["reference_group"]),
                    ),
                    [],
                ).append((str(request["doc_id"]), int(offset)))

        if not edinet_codes or not period_ends:
            continue

        code_params = tuple(sorted(edinet_codes))
        period_params = tuple(sorted(period_ends))
        code_placeholders = ",".join("?" for _ in code_params)
        period_placeholders = ",".join("?" for _ in period_params)
        form_types = ("030000", "043000", "043A00")
        form_placeholders = ",".join("?" for _ in form_types)

        normalized_keys = tuple(HISTORICAL_NORMALIZED_METRIC_KEYS.values())
        metric_placeholders = ",".join("?" for _ in normalized_keys)
        rows = conn.execute(
            f"""
            SELECT
                f.edinet_code,
                f.form_type,
                f.doc_id,
                f.period_end AS filing_period_end,
                nm.metric_key,
                nm.value_num,
                nm.period_end AS metric_period_end,
                nm.consolidation
            FROM filings f
            INNER JOIN normalized_metrics nm
                ON nm.doc_id = f.doc_id
            WHERE f.edinet_code IN ({code_placeholders})
              AND f.form_type IN ({form_placeholders})
              AND f.period_end IN ({period_placeholders})
              AND nm.metric_key IN ({metric_placeholders})
            """,
            (*code_params, *form_types, *period_params, *normalized_keys),
        ).fetchall()
        for row in rows:
            row_dict = dict(row)
            reference_group = _reference_group_for_reference_form_type(row_dict.get("form_type"))
            if not reference_group:
                continue
            targets = target_lookup.get(
                (
                    str(row_dict.get("edinet_code") or ""),
                    str(row_dict.get("filing_period_end") or ""),
                    reference_group,
                ),
                [],
            )
            metric_base = normalized_key_to_base.get(str(row_dict.get("metric_key") or ""))
            if not targets or not metric_base:
                continue
            value = {
                "doc_id": row_dict.get("doc_id"),
                "metric_key": row_dict.get("metric_key"),
                "period_end": row_dict.get("metric_period_end") or row_dict.get("filing_period_end"),
                "consolidation": row_dict.get("consolidation"),
                "value_num": _to_float(row_dict.get("value_num")),
            }
            for target_doc_id, offset in targets:
                add_result(target_doc_id, metric_base, offset, dict(value))

        derived_keys = tuple(HISTORICAL_DERIVED_METRIC_KEYS.values())
        derived_placeholders = ",".join("?" for _ in derived_keys)
        derived_rows = conn.execute(
            f"""
            SELECT
                f.edinet_code,
                f.form_type,
                f.doc_id,
                f.period_end AS filing_period_end,
                dm.metric_key,
                dm.value_num,
                dm.period_end AS metric_period_end,
                dm.consolidation,
                dm.calc_status
            FROM filings f
            INNER JOIN derived_metrics dm
                ON dm.doc_id = f.doc_id
            WHERE f.edinet_code IN ({code_placeholders})
              AND f.form_type IN ({form_placeholders})
              AND f.period_end IN ({period_placeholders})
              AND dm.metric_key IN ({derived_placeholders})
            """,
            (*code_params, *form_types, *period_params, *derived_keys),
        ).fetchall()
        for row in derived_rows:
            row_dict = dict(row)
            reference_group = _reference_group_for_reference_form_type(row_dict.get("form_type"))
            if not reference_group:
                continue
            targets = target_lookup.get(
                (
                    str(row_dict.get("edinet_code") or ""),
                    str(row_dict.get("filing_period_end") or ""),
                    reference_group,
                ),
                [],
            )
            metric_base = derived_key_to_base.get(str(row_dict.get("metric_key") or ""))
            if not targets or not metric_base:
                continue
            if str(row_dict.get("calc_status") or "") == "missing_input":
                value_num = None
            else:
                value_num = _to_float(row_dict.get("value_num"))
            value = {
                "doc_id": row_dict.get("doc_id"),
                "metric_key": row_dict.get("metric_key"),
                "period_end": row_dict.get("metric_period_end") or row_dict.get("filing_period_end"),
                "consolidation": row_dict.get("consolidation"),
                "value_num": value_num,
                "calc_status": row_dict.get("calc_status"),
            }
            for target_doc_id, offset in targets:
                add_result(target_doc_id, metric_base, offset, dict(value))

        share_keys = tuple(OUTSTANDING_SHARES_COMPONENT_KEYS)
        share_placeholders = ",".join("?" for _ in share_keys)
        share_rows = conn.execute(
            f"""
            SELECT
                f.edinet_code,
                f.form_type,
                f.doc_id,
                f.period_end AS filing_period_end,
                nm.metric_key,
                nm.value_num,
                nm.period_end AS metric_period_end,
                nm.consolidation
            FROM filings f
            INNER JOIN normalized_metrics nm
                ON nm.doc_id = f.doc_id
            WHERE f.edinet_code IN ({code_placeholders})
              AND f.form_type IN ({form_placeholders})
              AND f.period_end IN ({period_placeholders})
              AND nm.metric_key IN ({share_placeholders})
            """,
            (*code_params, *form_types, *period_params, *share_keys),
        ).fetchall()
        share_components: dict[tuple[str, int, str], dict[str, Any]] = {}
        for row in share_rows:
            row_dict = dict(row)
            reference_group = _reference_group_for_reference_form_type(row_dict.get("form_type"))
            if not reference_group:
                continue
            targets = target_lookup.get(
                (
                    str(row_dict.get("edinet_code") or ""),
                    str(row_dict.get("filing_period_end") or ""),
                    reference_group,
                ),
                [],
            )
            if not targets:
                continue
            for target_doc_id, offset in targets:
                key = (target_doc_id, offset, str(row_dict.get("doc_id") or ""))
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

        for (target_doc_id, offset, _reference_doc_id), component in share_components.items():
            value_num, calc_status = _outstanding_shares_value(
                component.get("IssuedSharesCurrent"),
                component.get("TreasurySharesCurrent"),
            )
            add_result(
                target_doc_id,
                "OutstandingShares",
                offset,
                {
                    "doc_id": component.get("doc_id"),
                    "metric_key": "OutstandingSharesCurrent",
                    "period_end": component.get("period_end"),
                    "consolidation": component.get("consolidation"),
                    "value_num": value_num,
                    "calc_status": calc_status,
                    "issued_shares": component.get("IssuedSharesCurrent"),
                    "treasury_shares": component.get("TreasurySharesCurrent"),
                },
            )

    return result


def fetch_half_progress_annual_values(
    conn: sqlite3.Connection,
    filing: dict[str, Any],
) -> dict[str, dict[str, Any]]:
    """Fetch the annual filing that corresponds to a half-year filing."""

    if not is_half_form_type(filing.get("form_type")):
        return {}

    edinet_code = str(filing.get("edinet_code") or "").strip()
    half_period_end = _parse_date(filing.get("period_end"))
    if not edinet_code or not half_period_end:
        return {}

    conn.row_factory = sqlite3.Row
    annual_doc = conn.execute(
        """
        SELECT doc_id, period_end
        FROM filings
        WHERE edinet_code = ?
          AND form_type = '030000'
          AND COALESCE(period_end, '') > ?
          AND date(period_end) <= date(?, '+9 months')
        ORDER BY period_end ASC, COALESCE(submit_date, '') ASC, doc_id ASC
        LIMIT 1
        """,
        (edinet_code, half_period_end.isoformat(), half_period_end.isoformat()),
    ).fetchone()
    out: dict[str, dict[str, Any]] = {}
    if annual_doc:
        metric_keys = tuple(HALF_PROGRESS_ANNUAL_METRIC_KEYS.values())
        placeholders = ",".join("?" for _ in metric_keys)
        rows = conn.execute(
            f"""
            SELECT metric_key, value_num, consolidation
            FROM normalized_metrics
            WHERE doc_id = ?
              AND metric_key IN ({placeholders})
            """,
            (annual_doc["doc_id"], *metric_keys),
        ).fetchall()

        key_to_base = {
            metric_key: metric_base
            for metric_base, metric_key in HALF_PROGRESS_ANNUAL_METRIC_KEYS.items()
        }
        for row in rows:
            metric_base = key_to_base.get(str(row["metric_key"] or ""))
            if not metric_base:
                continue
            out[metric_base] = {
                "doc_id": annual_doc["doc_id"],
                "metric_key": row["metric_key"],
                "period_end": annual_doc["period_end"],
                "consolidation": row["consolidation"],
                "value_num": _to_float(row["value_num"]),
            }

    share_doc = conn.execute(
        """
        SELECT doc_id, period_end
        FROM filings
        WHERE edinet_code = ?
          AND form_type = '030000'
          AND COALESCE(period_end, '') <= ?
        ORDER BY period_end DESC, COALESCE(submit_date, '') DESC, doc_id DESC
        LIMIT 1
        """,
        (edinet_code, half_period_end.isoformat()),
    ).fetchone()
    if not share_doc:
        return out

    share_keys = tuple(HALF_SHARE_REFERENCE_KEYS.values())
    share_placeholders = ",".join("?" for _ in share_keys)
    share_rows = conn.execute(
        f"""
        SELECT metric_key, value_num, consolidation
        FROM normalized_metrics
        WHERE doc_id = ?
          AND metric_key IN ({share_placeholders})
        """,
        (share_doc["doc_id"], *share_keys),
    ).fetchall()
    share_key_to_base = {
        metric_key: metric_base
        for metric_base, metric_key in HALF_SHARE_REFERENCE_KEYS.items()
    }
    for row in share_rows:
        metric_base = share_key_to_base.get(str(row["metric_key"] or ""))
        if not metric_base:
            continue
        out[metric_base] = {
            "doc_id": share_doc["doc_id"],
            "metric_key": row["metric_key"],
            "period_end": share_doc["period_end"],
            "consolidation": row["consolidation"],
            "value_num": _to_float(row["value_num"]),
        }
    return out


def fetch_half_progress_annual_values_bulk(
    conn: sqlite3.Connection,
    filings: list[dict[str, Any]],
    *,
    chunk_size: int = DEFAULT_BULK_CHUNK_SIZE,
) -> dict[str, dict[str, dict[str, Any]]]:
    """Fetch half-year progress annual references for multiple filings in batch."""

    conn.row_factory = sqlite3.Row
    requests: list[tuple[str, str, str]] = []
    for filing in filings:
        if not is_half_form_type(filing.get("form_type")):
            continue
        doc_id = str(filing.get("doc_id") or "").strip()
        edinet_code = str(filing.get("edinet_code") or "").strip()
        half_period_end = _parse_date(filing.get("period_end"))
        if not doc_id or not edinet_code or not half_period_end:
            continue
        requests.append((doc_id, edinet_code, half_period_end.isoformat()))

    if not requests:
        return {}

    key_to_base = {
        metric_key: metric_base
        for metric_base, metric_key in HALF_PROGRESS_ANNUAL_METRIC_KEYS.items()
    }
    result: dict[str, dict[str, dict[str, Any]]] = {}

    for chunk in _chunked(requests, chunk_size):
        values_sql = ",".join("(?, ?, ?)" for _ in chunk)
        params: list[Any] = []
        for doc_id, edinet_code, half_period_end in chunk:
            params.extend([doc_id, edinet_code, half_period_end])

        annual_rows = conn.execute(
            f"""
            WITH wanted(request_doc_id, edinet_code, half_period_end) AS (
                VALUES {values_sql}
            )
            SELECT
                w.request_doc_id,
                f.doc_id AS annual_doc_id,
                f.period_end AS annual_period_end
            FROM wanted w
            INNER JOIN filings f
                ON f.edinet_code = w.edinet_code
            WHERE f.form_type = '030000'
              AND COALESCE(f.period_end, '') > w.half_period_end
              AND date(f.period_end) <= date(w.half_period_end, '+9 months')
            ORDER BY
                w.request_doc_id ASC,
                f.period_end ASC,
                COALESCE(f.submit_date, '') ASC,
                f.doc_id ASC
            """,
            params,
        ).fetchall()

        annual_by_request: dict[str, dict[str, Any]] = {}
        annual_doc_to_requests: dict[str, list[str]] = {}
        for row in annual_rows:
            request_doc_id = str(row["request_doc_id"] or "")
            if request_doc_id in annual_by_request:
                continue
            annual_doc_id = str(row["annual_doc_id"] or "")
            annual_by_request[request_doc_id] = {
                "doc_id": annual_doc_id,
                "period_end": row["annual_period_end"],
            }
            annual_doc_to_requests.setdefault(annual_doc_id, []).append(request_doc_id)

        annual_doc_ids = [doc_id for doc_id in annual_doc_to_requests if doc_id]
        if annual_doc_ids:
            metric_keys = tuple(HALF_PROGRESS_ANNUAL_METRIC_KEYS.values())
            doc_placeholders = ",".join("?" for _ in annual_doc_ids)
            metric_placeholders = ",".join("?" for _ in metric_keys)
            rows = conn.execute(
                f"""
                SELECT doc_id, metric_key, value_num, consolidation
                FROM normalized_metrics
                WHERE doc_id IN ({doc_placeholders})
                  AND metric_key IN ({metric_placeholders})
                ORDER BY doc_id ASC, metric_key ASC
                """,
                (*annual_doc_ids, *metric_keys),
            ).fetchall()

            for row in rows:
                annual_doc_id = str(row["doc_id"] or "")
                metric_base = key_to_base.get(str(row["metric_key"] or ""))
                if not metric_base:
                    continue
                for request_doc_id in annual_doc_to_requests.get(annual_doc_id, []):
                    annual_doc = annual_by_request[request_doc_id]
                    result.setdefault(request_doc_id, {})[metric_base] = {
                        "doc_id": annual_doc["doc_id"],
                        "metric_key": row["metric_key"],
                        "period_end": annual_doc["period_end"],
                        "consolidation": row["consolidation"],
                        "value_num": _to_float(row["value_num"]),
                    }

        share_rows = conn.execute(
            f"""
            WITH wanted(request_doc_id, edinet_code, half_period_end) AS (
                VALUES {values_sql}
            ),
            ranked AS (
                SELECT
                    w.request_doc_id,
                    f.doc_id AS annual_doc_id,
                    f.period_end AS annual_period_end,
                    ROW_NUMBER() OVER (
                        PARTITION BY w.request_doc_id
                        ORDER BY f.period_end DESC, COALESCE(f.submit_date, '') DESC, f.doc_id DESC
                    ) AS row_num
                FROM wanted w
                INNER JOIN filings f
                    ON f.edinet_code = w.edinet_code
                WHERE f.form_type = '030000'
                  AND COALESCE(f.period_end, '') <= w.half_period_end
            )
            SELECT
                ranked.request_doc_id,
                ranked.annual_doc_id,
                ranked.annual_period_end,
                nm.metric_key,
                nm.value_num,
                nm.consolidation
            FROM ranked
            INNER JOIN normalized_metrics nm
                ON nm.doc_id = ranked.annual_doc_id
            WHERE ranked.row_num = 1
              AND nm.metric_key IN ({",".join("?" for _ in HALF_SHARE_REFERENCE_KEYS)})
            ORDER BY ranked.request_doc_id ASC, nm.metric_key ASC
            """,
            (*params, *HALF_SHARE_REFERENCE_KEYS.values()),
        ).fetchall()
        share_key_to_base = {
            metric_key: metric_base
            for metric_base, metric_key in HALF_SHARE_REFERENCE_KEYS.items()
        }
        for row in share_rows:
            metric_base = share_key_to_base.get(str(row["metric_key"] or ""))
            if not metric_base:
                continue
            result.setdefault(str(row["request_doc_id"] or ""), {})[metric_base] = {
                "doc_id": row["annual_doc_id"],
                "metric_key": row["metric_key"],
                "period_end": row["annual_period_end"],
                "consolidation": row["consolidation"],
                "value_num": _to_float(row["value_num"]),
            }

    return result
