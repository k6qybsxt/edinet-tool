from __future__ import annotations

import sqlite3


def _fetch_latest_doc_id_by_edinet_code(
    conn: sqlite3.Connection,
    edinet_code: str,
    *,
    period_scope: str,
) -> str | None:
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    row = cur.execute(
        """
        SELECT
            dm.doc_id
        FROM derived_metrics dm
        LEFT JOIN filings f
            ON dm.doc_id = f.doc_id
        WHERE dm.edinet_code = ?
          AND dm.period_scope = ?
        GROUP BY
            dm.doc_id,
            dm.period_end,
            f.submit_date
        ORDER BY
            COALESCE(dm.period_end, '') DESC,
            COALESCE(f.submit_date, '') DESC,
            dm.doc_id DESC
        LIMIT 1
        """,
        (edinet_code, period_scope),
    ).fetchone()

    if row is None:
        return None

    return str(row["doc_id"])


def fetch_latest_metrics_by_edinet_code(
    conn: sqlite3.Connection,
    edinet_code: str,
    *,
    period_scope: str,
) -> dict[str, dict]:
    latest_doc_id = _fetch_latest_doc_id_by_edinet_code(
        conn,
        edinet_code,
        period_scope=period_scope,
    )
    if latest_doc_id is None:
        return {}

    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    rows = cur.execute(
        """
        SELECT
            'normalized_metrics' AS metric_source,
            nm.doc_id,
            nm.edinet_code,
            nm.security_code,
            im.company_name,
            nm.metric_key,
            NULL AS metric_base,
            NULL AS metric_group,
            nm.fiscal_year,
            nm.period_end,
            NULL AS period_scope,
            nm.value_num,
            NULL AS value_unit,
            NULL AS calc_status,
            NULL AS document_display_unit,
            NULL AS accounting_standard,
            nm.source_tag,
            nm.consolidation,
            nm.rule_version
        FROM normalized_metrics nm
        LEFT JOIN issuer_master im
            ON nm.edinet_code = im.edinet_code
        WHERE nm.doc_id = ?

        UNION ALL

        SELECT
            'derived_metrics' AS metric_source,
            dm.doc_id,
            dm.edinet_code,
            dm.security_code,
            im.company_name,
            dm.metric_key,
            dm.metric_base,
            dm.metric_group,
            dm.fiscal_year,
            dm.period_end,
            dm.period_scope,
            dm.value_num,
            dm.value_unit,
            dm.calc_status,
            dm.document_display_unit,
            dm.accounting_standard,
            NULL AS source_tag,
            dm.consolidation,
            dm.rule_version
        FROM derived_metrics dm
        LEFT JOIN issuer_master im
            ON dm.edinet_code = im.edinet_code
        WHERE dm.doc_id = ?

        ORDER BY metric_key ASC
        """,
        (latest_doc_id, latest_doc_id),
    ).fetchall()

    out: dict[str, dict] = {}
    for row in rows:
        row_dict = dict(row)
        out[str(row_dict["metric_key"])] = row_dict
    return out


def fetch_target_edinet_codes(
    conn: sqlite3.Connection,
    *,
    period_scope: str,
    limit: int | None = None,
) -> list[str]:
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    sql = """
    SELECT DISTINCT edinet_code
    FROM derived_metrics
    WHERE edinet_code IS NOT NULL
      AND edinet_code <> ''
      AND period_scope = ?
    ORDER BY edinet_code
    """
    params: list[str | int] = [period_scope]

    if limit is not None:
        sql += " LIMIT ?"
        params.append(limit)

    rows = cur.execute(sql, tuple(params)).fetchall()
    return [str(row["edinet_code"]) for row in rows]
