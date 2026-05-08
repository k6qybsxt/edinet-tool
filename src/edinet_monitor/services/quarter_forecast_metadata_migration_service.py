from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime
from pathlib import Path
import sqlite3


FORECAST_BASES_TO_KEEP = {"NetSales", "OrdinaryIncome", "ProfitLoss"}


@dataclass(frozen=True)
class QuarterForecastMigrationResult:
    apply: bool
    annual_derived_candidates: int
    q2_derived_candidates: int
    forecast_stage_candidates: int
    obsolete_forecast_candidates: int
    annual_derived_updated: int
    q2_derived_updated: int
    forecast_stage_updated: int
    obsolete_forecast_deleted: int
    output_path: Path | None = None


def _scalar(conn: sqlite3.Connection, sql: str, params: tuple = ()) -> int:
    row = conn.execute(sql, params).fetchone()
    return int(row[0] or 0) if row else 0


def _has_table(conn: sqlite3.Connection, table_name: str) -> bool:
    return (
        conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
            (table_name,),
        ).fetchone()
        is not None
    )


def _write_report(
    result: QuarterForecastMigrationResult,
    *,
    output_dir: str | Path | None,
) -> Path | None:
    if not output_dir:
        return None
    directory = Path(output_dir)
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"quarter_forecast_metadata_migration_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    lines = [
        f"generated_at={datetime.now().isoformat(timespec='seconds')}",
        f"apply={int(result.apply)}",
        f"annual_derived_candidates={result.annual_derived_candidates}",
        f"q2_derived_candidates={result.q2_derived_candidates}",
        f"forecast_stage_candidates={result.forecast_stage_candidates}",
        f"obsolete_forecast_candidates={result.obsolete_forecast_candidates}",
        f"annual_derived_updated={result.annual_derived_updated}",
        f"q2_derived_updated={result.q2_derived_updated}",
        f"forecast_stage_updated={result.forecast_stage_updated}",
        f"obsolete_forecast_deleted={result.obsolete_forecast_deleted}",
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8-sig")
    return path


def migrate_quarter_forecast_metadata(
    conn: sqlite3.Connection,
    *,
    apply: bool = False,
    output_dir: str | Path | None = None,
) -> QuarterForecastMigrationResult:
    annual_candidates = 0
    q2_candidates = 0
    forecast_stage_candidates = 0
    obsolete_forecast_candidates = 0

    if _has_table(conn, "derived_metrics"):
        annual_candidates = _scalar(
            conn,
            """
            SELECT COUNT(*)
            FROM derived_metrics
            WHERE period_scope = 'annual'
              AND COALESCE(period_key, '') <> 'annual:FY'
            """,
        )
        q2_candidates = _scalar(
            conn,
            """
            SELECT COUNT(*)
            FROM derived_metrics dm
            JOIN filings f ON f.doc_id = dm.doc_id
            WHERE f.form_type = '043A00'
              AND (
                dm.period_scope <> 'quarter'
                OR COALESCE(dm.period_key, '') <> 'actual:2Q'
                OR COALESCE(dm.quarter_type, '') <> '2Q'
              )
            """,
        )

    if _has_table(conn, "jquants_financial_metrics"):
        forecast_stage_candidates = _scalar(
            conn,
            """
            SELECT COUNT(*)
            FROM jquants_financial_metrics m
            JOIN jquants_statement_raw r
              ON r.disclosure_number = m.disclosure_number
            WHERE m.metric_kind = 'forecast'
              AND COALESCE(m.forecast_stage, '') = ''
              AND COALESCE(r.type_of_current_period, '') IN ('FY', '1Q', '2Q', '3Q')
            """,
        )
        placeholders = ",".join("?" for _ in FORECAST_BASES_TO_KEEP)
        obsolete_forecast_candidates = _scalar(
            conn,
            f"""
            SELECT COUNT(*)
            FROM jquants_financial_metrics
            WHERE metric_kind = 'forecast'
              AND (
                COALESCE(forecast_target, '') <> 'FY'
                OR metric_base NOT IN ({placeholders})
              )
            """,
            tuple(sorted(FORECAST_BASES_TO_KEEP)),
        )

    annual_updated = 0
    q2_updated = 0
    forecast_stage_updated = 0
    obsolete_forecast_deleted = 0

    if apply:
        if _has_table(conn, "derived_metrics"):
            annual_cursor = conn.execute(
                """
                UPDATE derived_metrics
                SET period_key = 'annual:FY',
                    quarter_type = NULL
                WHERE period_scope = 'annual'
                  AND COALESCE(period_key, '') <> 'annual:FY'
                """
            )
            annual_updated = int(annual_cursor.rowcount or 0)
            q2_cursor = conn.execute(
                """
                UPDATE derived_metrics
                SET period_scope = 'quarter',
                    period_key = 'actual:2Q',
                    quarter_type = '2Q'
                WHERE doc_id IN (
                    SELECT doc_id FROM filings WHERE form_type = '043A00'
                )
                  AND (
                    period_scope <> 'quarter'
                    OR COALESCE(period_key, '') <> 'actual:2Q'
                    OR COALESCE(quarter_type, '') <> '2Q'
                  )
                """
            )
            q2_updated = int(q2_cursor.rowcount or 0)

        if _has_table(conn, "jquants_financial_metrics"):
            forecast_cursor = conn.execute(
                """
                UPDATE jquants_financial_metrics
                SET forecast_stage = CASE (
                    SELECT r.type_of_current_period
                    FROM jquants_statement_raw r
                    WHERE r.disclosure_number = jquants_financial_metrics.disclosure_number
                )
                    WHEN 'FY' THEN 'initial'
                    WHEN '1Q' THEN '1Q'
                    WHEN '2Q' THEN '2Q'
                    WHEN '3Q' THEN '3Q'
                    ELSE forecast_stage
                END
                WHERE metric_kind = 'forecast'
                  AND COALESCE(forecast_stage, '') = ''
                  AND disclosure_number IN (
                    SELECT disclosure_number
                    FROM jquants_statement_raw
                    WHERE COALESCE(type_of_current_period, '') IN ('FY', '1Q', '2Q', '3Q')
                  )
                """
            )
            forecast_stage_updated = int(forecast_cursor.rowcount or 0)
            placeholders = ",".join("?" for _ in FORECAST_BASES_TO_KEEP)
            delete_cursor = conn.execute(
                f"""
                DELETE FROM jquants_financial_metrics
                WHERE metric_kind = 'forecast'
                  AND (
                    COALESCE(forecast_target, '') <> 'FY'
                    OR metric_base NOT IN ({placeholders})
                  )
                """,
                tuple(sorted(FORECAST_BASES_TO_KEEP)),
            )
            obsolete_forecast_deleted = int(delete_cursor.rowcount or 0)
        conn.commit()

    result = QuarterForecastMigrationResult(
        apply=apply,
        annual_derived_candidates=annual_candidates,
        q2_derived_candidates=q2_candidates,
        forecast_stage_candidates=forecast_stage_candidates,
        obsolete_forecast_candidates=obsolete_forecast_candidates,
        annual_derived_updated=annual_updated,
        q2_derived_updated=q2_updated,
        forecast_stage_updated=forecast_stage_updated,
        obsolete_forecast_deleted=obsolete_forecast_deleted,
    )
    output_path = _write_report(result, output_dir=output_dir)
    return replace(result, output_path=output_path)
