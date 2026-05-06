from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
import sqlite3


@dataclass(frozen=True)
class JQuantsCoverageResult:
    output_path: Path
    rows: int
    warnings: list[str]


def export_jquants_coverage(
    conn: sqlite3.Connection,
    *,
    date_from: str,
    date_to: str,
    target: str,
    codes: list[str],
    output_dir: str | Path,
) -> JQuantsCoverageResult:
    lines: list[str] = []
    warnings: list[str] = []
    lines.append(f"generated_at: {datetime.now().isoformat(timespec='seconds')}")
    lines.append(f"date_from: {date_from}")
    lines.append(f"date_to: {date_to}")
    lines.append(f"target: {target}")
    lines.append(f"codes: {','.join(codes) if codes else 'all'}")
    lines.append("api_version: v2")
    lines.append("")

    row_count = 0
    if target in {"all", "statements"}:
        statement_rows = _statement_coverage(conn, date_from=date_from, date_to=date_to, codes=codes)
        lines.append("[statements]")
        lines.append("date | raw | actual_metrics | forecast_metrics | securities")
        lines.append("-----+-----+----------------+------------------+-----------")
        for row in statement_rows:
            row_count += 1
            lines.append(
                f"{row['date']} | {row['raw_count']} | {row['actual_metric_count']} | "
                f"{row['forecast_metric_count']} | {row['security_count']}"
            )
        if not statement_rows:
            warnings.append("statements_not_found")
            lines.append("(no rows)")
        lines.append("")

    if target in {"all", "quotes"}:
        quote_rows = _quote_coverage(conn, date_from=date_from, date_to=date_to, codes=codes)
        lines.append("[daily_quotes]")
        lines.append("date | quotes | securities | min_close | max_close")
        lines.append("-----+--------+------------+-----------+----------")
        for row in quote_rows:
            row_count += 1
            lines.append(
                f"{row['date']} | {row['quote_count']} | {row['security_count']} | "
                f"{row['min_close']} | {row['max_close']}"
            )
        if not quote_rows:
            warnings.append("quotes_not_found")
            lines.append("(no rows)")
        lines.append("")

    run_rows = conn.execute(
        """
        SELECT run_type, date_from, date_to, status, fetched_total, saved_total,
               skipped_total, error_total, started_at, finished_at
        FROM jquants_ingest_runs
        WHERE date_from <= ? AND date_to >= ?
        ORDER BY started_at DESC
        LIMIT 20
        """,
        (date_to, date_from),
    ).fetchall()
    lines.append("[recent_runs]")
    if run_rows:
        for row in run_rows:
            lines.append(
                f"{row['started_at']} | {row['run_type']} | {row['status']} | "
                f"{row['date_from']}..{row['date_to']} | fetched={row['fetched_total']} "
                f"saved={row['saved_total']} errors={row['error_total']}"
            )
    else:
        lines.append("(no runs)")

    path = Path(output_dir) / f"jquants_coverage_{date_from}_to_{date_to}_{datetime.now():%Y%m%d_%H%M%S}.txt"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8-sig")
    return JQuantsCoverageResult(output_path=path, rows=row_count, warnings=warnings)


def _code_filter_sql(column: str, codes: list[str]) -> tuple[str, list[str]]:
    normalized = [code.strip() for code in codes if code.strip()]
    if not normalized:
        return "", []
    placeholders = ",".join("?" for _ in normalized)
    return f" AND ({column} IN ({placeholders}) OR substr({column}, 1, 4) IN ({placeholders}))", [
        *normalized,
        *normalized,
    ]


def _statement_coverage(
    conn: sqlite3.Connection,
    *,
    date_from: str,
    date_to: str,
    codes: list[str],
) -> list[sqlite3.Row]:
    code_sql, params = _code_filter_sql("local_code", codes)
    return conn.execute(
        f"""
        WITH raw AS (
          SELECT disclosed_date AS date,
                 COUNT(*) AS raw_count,
                 COUNT(DISTINCT security_code) AS raw_security_count
          FROM jquants_statement_raw
          WHERE disclosed_date BETWEEN ? AND ?
          {code_sql}
          GROUP BY disclosed_date
        ),
        metrics AS (
          SELECT disclosed_date AS date,
                 SUM(CASE WHEN metric_kind = 'actual' THEN 1 ELSE 0 END) AS actual_metric_count,
                 SUM(CASE WHEN metric_kind = 'forecast' THEN 1 ELSE 0 END) AS forecast_metric_count,
                 COUNT(DISTINCT security_code) AS security_count
          FROM jquants_financial_metrics
          WHERE disclosed_date BETWEEN ? AND ?
          {code_sql}
          GROUP BY disclosed_date
        ),
        dates AS (
          SELECT date FROM raw
          UNION
          SELECT date FROM metrics
        )
        SELECT
          dates.date AS date,
          COALESCE(raw.raw_count, 0) AS raw_count,
          COALESCE(metrics.actual_metric_count, 0) AS actual_metric_count,
          COALESCE(metrics.forecast_metric_count, 0) AS forecast_metric_count,
          COALESCE(metrics.security_count, raw.raw_security_count, 0) AS security_count
        FROM dates
        LEFT JOIN raw ON raw.date = dates.date
        LEFT JOIN metrics ON metrics.date = dates.date
        ORDER BY date
        """,
        [date_from, date_to, *params, date_from, date_to, *params],
    ).fetchall()


def _quote_coverage(
    conn: sqlite3.Connection,
    *,
    date_from: str,
    date_to: str,
    codes: list[str],
) -> list[sqlite3.Row]:
    code_sql, params = _code_filter_sql("local_code", codes)
    return conn.execute(
        f"""
        SELECT trade_date AS date,
               COUNT(*) AS quote_count,
               COUNT(DISTINCT security_code) AS security_count,
               MIN(adjustment_close_rounded) AS min_close,
               MAX(adjustment_close_rounded) AS max_close
        FROM jquants_daily_quotes
        WHERE trade_date BETWEEN ? AND ?
        {code_sql}
        GROUP BY trade_date
        ORDER BY trade_date
        """,
        [date_from, date_to, *params],
    ).fetchall()
