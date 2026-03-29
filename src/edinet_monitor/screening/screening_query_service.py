from __future__ import annotations

import sqlite3


def fetch_latest_metrics_by_edinet_code(conn: sqlite3.Connection, edinet_code: str) -> dict[str, dict]:
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    rows = cur.execute(
        """
        WITH latest_period AS (
            SELECT
                edinet_code,
                MAX(period_end) AS max_period_end
            FROM normalized_metrics
            WHERE edinet_code = ?
            GROUP BY edinet_code
        )
        SELECT
            nm.doc_id,
            nm.edinet_code,
            nm.security_code,
            nm.metric_key,
            nm.fiscal_year,
            nm.period_end,
            nm.value_num,
            nm.source_tag,
            nm.consolidation,
            nm.rule_version
        FROM normalized_metrics nm
        INNER JOIN latest_period lp
            ON nm.edinet_code = lp.edinet_code
           AND nm.period_end = lp.max_period_end
        WHERE nm.edinet_code = ?
        ORDER BY nm.metric_key
        """,
        (edinet_code, edinet_code),
    ).fetchall()

    out: dict[str, dict] = {}
    for row in rows:
        row_dict = dict(row)
        out[row_dict["metric_key"]] = row_dict
    return out


def fetch_target_edinet_codes(conn: sqlite3.Connection, limit: int | None = None) -> list[str]:
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    sql = """
    SELECT DISTINCT edinet_code
    FROM normalized_metrics
    WHERE edinet_code IS NOT NULL
      AND edinet_code <> ''
    ORDER BY edinet_code
    """
    params: tuple = ()

    if limit is not None:
        sql += " LIMIT ?"
        params = (limit,)

    rows = cur.execute(sql, params).fetchall()
    return [str(row["edinet_code"]) for row in rows]