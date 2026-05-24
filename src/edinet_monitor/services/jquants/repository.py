from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import json
import sqlite3
from typing import Any

from edinet_monitor.services.jquants.mapper import (
    JQuantsQuote,
    JQuantsStatementMetric,
    JQuantsStatementRaw,
)
from edinet_monitor.services.jquants.audit_mapper import (
    JQuantsFsDetailItem,
    JQuantsFsDetailsRaw,
    JQuantsListedInfoRaw,
)


@dataclass(frozen=True)
class SaveCounts:
    raw_saved: int = 0
    metrics_saved: int = 0
    quotes_saved: int = 0


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _edinet_code_by_security(conn: sqlite3.Connection, security_codes: set[str]) -> dict[str, str]:
    if not security_codes:
        return {}
    placeholders = ",".join("?" for _ in security_codes)
    rows = conn.execute(
        f"""
        SELECT security_code, edinet_code
        FROM issuer_master
        WHERE security_code IN ({placeholders})
           OR substr(security_code, 1, 4) IN ({placeholders})
        """,
        [*security_codes, *security_codes],
    ).fetchall()
    result: dict[str, str] = {}
    for row in rows:
        code = str(row["security_code"] or "")
        result[code] = str(row["edinet_code"] or "")
        if len(code) >= 4:
            result[code[:4]] = str(row["edinet_code"] or "")
    return result


def _ensure_forecast_stage_column(conn: sqlite3.Connection) -> None:
    columns = {
        str(row[1])
        for row in conn.execute("PRAGMA table_info(jquants_financial_metrics)").fetchall()
    }
    if columns and "forecast_stage" not in columns:
        conn.execute("ALTER TABLE jquants_financial_metrics ADD COLUMN forecast_stage TEXT")


def upsert_statement_raw(conn: sqlite3.Connection, raw: JQuantsStatementRaw) -> None:
    now = _now()
    conn.execute(
        """
        INSERT INTO jquants_statement_raw (
            disclosure_number, disclosed_date, disclosed_time, local_code, security_code,
            type_of_document, type_of_current_period, current_period_start_date,
            current_period_end_date, current_fiscal_year_start_date,
            current_fiscal_year_end_date, fiscal_year, raw_json, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(disclosure_number) DO UPDATE SET
            disclosed_date = excluded.disclosed_date,
            disclosed_time = excluded.disclosed_time,
            local_code = excluded.local_code,
            security_code = excluded.security_code,
            type_of_document = excluded.type_of_document,
            type_of_current_period = excluded.type_of_current_period,
            current_period_start_date = excluded.current_period_start_date,
            current_period_end_date = excluded.current_period_end_date,
            current_fiscal_year_start_date = excluded.current_fiscal_year_start_date,
            current_fiscal_year_end_date = excluded.current_fiscal_year_end_date,
            fiscal_year = excluded.fiscal_year,
            raw_json = excluded.raw_json,
            updated_at = excluded.updated_at
        """,
        (
            raw.disclosure_number,
            raw.disclosed_date,
            raw.disclosed_time,
            raw.local_code,
            raw.security_code,
            raw.type_of_document,
            raw.type_of_current_period,
            raw.current_period_start_date,
            raw.current_period_end_date,
            raw.current_fiscal_year_start_date,
            raw.current_fiscal_year_end_date,
            raw.fiscal_year,
            raw.raw_json,
            now,
            now,
        ),
    )


def upsert_financial_metrics(
    conn: sqlite3.Connection,
    metrics: list[JQuantsStatementMetric],
) -> int:
    if not metrics:
        return 0
    _ensure_forecast_stage_column(conn)
    edinet_by_security = _edinet_code_by_security(conn, {metric.security_code for metric in metrics})
    now = _now()
    rows = [
        (
            metric.disclosure_number,
            metric.local_code,
            metric.security_code,
            edinet_by_security.get(metric.security_code, ""),
            metric.metric_kind,
            metric.period_scope,
            metric.period_key,
            metric.quarter_type,
            metric.forecast_target,
            metric.forecast_stage,
            metric.fiscal_year,
            metric.period_start,
            metric.period_end,
            metric.disclosed_date,
            metric.disclosed_time,
            metric.metric_key,
            metric.metric_base,
            metric.metric_group,
            metric.value_num,
            metric.value_unit,
            metric.calc_status,
            metric.source_field,
            metric.source_detail_json,
            metric.rule_version,
            now,
            now,
        )
        for metric in metrics
    ]
    conn.executemany(
        """
        INSERT INTO jquants_financial_metrics (
            disclosure_number, local_code, security_code, edinet_code, metric_kind,
            period_scope, period_key, quarter_type, forecast_target, forecast_stage, fiscal_year,
            period_start, period_end, disclosed_date, disclosed_time, metric_key,
            metric_base, metric_group, value_num, value_unit, calc_status,
            source_field, source_detail_json, rule_version, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(disclosure_number, period_key, metric_key) DO UPDATE SET
            local_code = excluded.local_code,
            security_code = excluded.security_code,
            edinet_code = excluded.edinet_code,
            metric_kind = excluded.metric_kind,
            period_scope = excluded.period_scope,
            quarter_type = excluded.quarter_type,
            forecast_target = excluded.forecast_target,
            forecast_stage = excluded.forecast_stage,
            fiscal_year = excluded.fiscal_year,
            period_start = excluded.period_start,
            period_end = excluded.period_end,
            disclosed_date = excluded.disclosed_date,
            disclosed_time = excluded.disclosed_time,
            metric_base = excluded.metric_base,
            metric_group = excluded.metric_group,
            value_num = excluded.value_num,
            value_unit = excluded.value_unit,
            calc_status = excluded.calc_status,
            source_field = excluded.source_field,
            source_detail_json = excluded.source_detail_json,
            rule_version = excluded.rule_version,
            updated_at = excluded.updated_at
        """,
        rows,
    )
    return len(rows)


def upsert_quote(conn: sqlite3.Connection, quote: JQuantsQuote) -> None:
    now = _now()
    conn.execute(
        """
        INSERT INTO jquants_daily_quotes (
            local_code, security_code, trade_date, open, high, low, close, volume,
            turnover_value, adjustment_factor, adjustment_open, adjustment_high,
            adjustment_low, adjustment_close, adjustment_close_rounded,
            adjustment_volume, raw_json, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(local_code, trade_date) DO UPDATE SET
            security_code = excluded.security_code,
            open = excluded.open,
            high = excluded.high,
            low = excluded.low,
            close = excluded.close,
            volume = excluded.volume,
            turnover_value = excluded.turnover_value,
            adjustment_factor = excluded.adjustment_factor,
            adjustment_open = excluded.adjustment_open,
            adjustment_high = excluded.adjustment_high,
            adjustment_low = excluded.adjustment_low,
            adjustment_close = excluded.adjustment_close,
            adjustment_close_rounded = excluded.adjustment_close_rounded,
            adjustment_volume = excluded.adjustment_volume,
            raw_json = excluded.raw_json,
            updated_at = excluded.updated_at
        """,
        (
            quote.local_code,
            quote.security_code,
            quote.trade_date,
            quote.open,
            quote.high,
            quote.low,
            quote.close,
            quote.volume,
            quote.turnover_value,
            quote.adjustment_factor,
            quote.adjustment_open,
            quote.adjustment_high,
            quote.adjustment_low,
            quote.adjustment_close,
            quote.adjustment_close_rounded,
            quote.adjustment_volume,
            quote.raw_json,
            now,
            now,
        ),
    )


def upsert_listed_info_raw(conn: sqlite3.Connection, rows: list[JQuantsListedInfoRaw]) -> int:
    if not rows:
        return 0
    now = _now()
    conn.executemany(
        """
        INSERT INTO jquants_listed_info_raw (
            listing_date, local_code, security_code, company_name, company_name_en,
            sector_17_code, sector_17_name, sector_33_code, sector_33_name,
            scale_category, market_code, market_name, margin_code, margin_name,
            raw_json, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(local_code, listing_date) DO UPDATE SET
            security_code = excluded.security_code,
            company_name = excluded.company_name,
            company_name_en = excluded.company_name_en,
            sector_17_code = excluded.sector_17_code,
            sector_17_name = excluded.sector_17_name,
            sector_33_code = excluded.sector_33_code,
            sector_33_name = excluded.sector_33_name,
            scale_category = excluded.scale_category,
            market_code = excluded.market_code,
            market_name = excluded.market_name,
            margin_code = excluded.margin_code,
            margin_name = excluded.margin_name,
            raw_json = excluded.raw_json,
            updated_at = excluded.updated_at
        """,
        [
            (
                row.listing_date,
                row.local_code,
                row.security_code,
                row.company_name,
                row.company_name_en,
                row.sector_17_code,
                row.sector_17_name,
                row.sector_33_code,
                row.sector_33_name,
                row.scale_category,
                row.market_code,
                row.market_name,
                row.margin_code,
                row.margin_name,
                row.raw_json,
                now,
                now,
            )
            for row in rows
        ],
    )
    return len(rows)


def upsert_fs_details_raw(conn: sqlite3.Connection, raw: JQuantsFsDetailsRaw) -> None:
    now = _now()
    conn.execute(
        """
        INSERT INTO jquants_fs_details_raw (
            disclosure_number, disclosed_date, disclosed_time, local_code,
            security_code, type_of_document, raw_json, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(disclosure_number) DO UPDATE SET
            disclosed_date = excluded.disclosed_date,
            disclosed_time = excluded.disclosed_time,
            local_code = excluded.local_code,
            security_code = excluded.security_code,
            type_of_document = excluded.type_of_document,
            raw_json = excluded.raw_json,
            updated_at = excluded.updated_at
        """,
        (
            raw.disclosure_number,
            raw.disclosed_date,
            raw.disclosed_time,
            raw.local_code,
            raw.security_code,
            raw.type_of_document,
            raw.raw_json,
            now,
            now,
        ),
    )


def replace_fs_detail_items(
    conn: sqlite3.Connection,
    disclosure_number: str,
    items: list[JQuantsFsDetailItem],
) -> int:
    conn.execute(
        "DELETE FROM jquants_fs_detail_items WHERE disclosure_number = ?",
        (disclosure_number,),
    )
    if not items:
        return 0
    now = _now()
    conn.executemany(
        """
        INSERT INTO jquants_fs_detail_items (
            disclosure_number, local_code, security_code, disclosed_date, item_key,
            metric_hint, detail_label, value_num, value_text, source_path,
            created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                item.disclosure_number,
                item.local_code,
                item.security_code,
                item.disclosed_date,
                item.item_key,
                item.metric_hint,
                item.detail_label,
                item.value_num,
                item.value_text,
                item.source_path,
                now,
                now,
            )
            for item in items
        ],
    )
    return len(items)


def record_ingest_run(
    conn: sqlite3.Connection,
    *,
    run_id: str,
    run_type: str,
    date_from: str,
    date_to: str,
    codes: list[str],
    started_at: str,
    finished_at: str,
    status: str,
    fetched_total: int,
    saved_total: int,
    skipped_total: int,
    error_total: int,
    summary: dict[str, Any],
) -> None:
    now = _now()
    conn.execute(
        """
        INSERT INTO jquants_ingest_runs (
            run_id, run_type, date_from, date_to, codes_json, started_at, finished_at,
            status, fetched_total, saved_total, skipped_total, error_total,
            summary_json, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(run_id) DO UPDATE SET
            status = excluded.status,
            finished_at = excluded.finished_at,
            fetched_total = excluded.fetched_total,
            saved_total = excluded.saved_total,
            skipped_total = excluded.skipped_total,
            error_total = excluded.error_total,
            summary_json = excluded.summary_json,
            updated_at = excluded.updated_at
        """,
        (
            run_id,
            run_type,
            date_from,
            date_to,
            json.dumps(codes, ensure_ascii=False),
            started_at,
            finished_at,
            status,
            fetched_total,
            saved_total,
            skipped_total,
            error_total,
            json.dumps(summary, ensure_ascii=False, sort_keys=True),
            now,
            now,
        ),
    )


def record_ingest_progress(
    conn: sqlite3.Connection,
    *,
    run_id: str,
    run_type: str,
    target_kind: str,
    target_value: str,
    status: str,
    fetched_count: int = 0,
    saved_count: int = 0,
    skipped_count: int = 0,
    error_message: str = "",
    started_at: str | None = None,
    finished_at: str | None = None,
) -> None:
    now = _now()
    conn.execute(
        """
        INSERT INTO jquants_ingest_progress (
            run_id, run_type, target_kind, target_value, status,
            fetched_count, saved_count, skipped_count, error_message,
            started_at, finished_at, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(run_id, run_type, target_kind, target_value) DO UPDATE SET
            status = excluded.status,
            fetched_count = excluded.fetched_count,
            saved_count = excluded.saved_count,
            skipped_count = excluded.skipped_count,
            error_message = excluded.error_message,
            started_at = COALESCE(jquants_ingest_progress.started_at, excluded.started_at),
            finished_at = excluded.finished_at,
            updated_at = excluded.updated_at
        """,
        (
            run_id,
            run_type,
            target_kind,
            target_value,
            status,
            fetched_count,
            saved_count,
            skipped_count,
            error_message,
            started_at or now,
            finished_at,
            now,
            now,
        ),
    )

