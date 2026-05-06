from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
import sqlite3
import uuid

from edinet_monitor.services.jquants.client import JQuantsClient
from edinet_monitor.services.jquants.mapper import (
    ACTUAL_PERIODS,
    build_statement_raw,
    quote_from_row,
    statement_metrics_from_row,
)
from edinet_monitor.services.jquants.repository import (
    record_ingest_run,
    upsert_financial_metrics,
    upsert_quote,
    upsert_statement_raw,
)


@dataclass(frozen=True)
class JQuantsIngestResult:
    run_id: str
    run_type: str
    date_from: str
    date_to: str
    fetched_total: int
    saved_total: int
    skipped_total: int
    error_total: int
    output_path: Path | None
    messages: list[str] = field(default_factory=list)


def _parse_date(value: str) -> date:
    return date.fromisoformat(str(value).replace("/", "-"))


def _iter_dates(date_from: str, date_to: str):
    current = _parse_date(date_from)
    end = _parse_date(date_to)
    while current <= end:
        yield current
        current += timedelta(days=1)


def _write_summary(path: Path | None, lines: list[str]) -> Path | None:
    if path is None:
        return None
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8-sig")
    return path


def save_jquants_statements(
    conn: sqlite3.Connection,
    *,
    client: JQuantsClient,
    date_from: str,
    date_to: str,
    periods: set[str] | None = None,
    include_forecasts: bool = True,
    codes: list[str] | None = None,
    output_dir: str | Path | None = None,
) -> JQuantsIngestResult:
    run_id = f"jquants_statements_{uuid.uuid4().hex[:12]}"
    started_at = datetime.now().isoformat(timespec="seconds")
    allowed_periods = periods or ACTUAL_PERIODS
    requested_codes = [code.strip() for code in (codes or []) if code.strip()]
    fetched_total = 0
    saved_total = 0
    skipped_total = 0
    error_total = 0
    messages: list[str] = []

    try:
        if requested_codes:
            for code in requested_codes:
                for item in client.iter_fin_summary(code=code):
                    raw, metrics = _convert_statement_item(
                        item,
                        periods=allowed_periods,
                        include_forecasts=include_forecasts,
                    )
                    fetched_total += 1
                    if _statement_in_range(raw.disclosed_date, date_from, date_to):
                        upsert_statement_raw(conn, raw)
                        saved_total += upsert_financial_metrics(conn, metrics)
                    else:
                        skipped_total += 1
        else:
            for current in _iter_dates(date_from, date_to):
                api_date = current.isoformat()
                day_fetched = 0
                for item in client.iter_fin_summary(date=api_date):
                    raw, metrics = _convert_statement_item(
                        item,
                        periods=allowed_periods,
                        include_forecasts=include_forecasts,
                    )
                    fetched_total += 1
                    day_fetched += 1
                    upsert_statement_raw(conn, raw)
                    saved_total += upsert_financial_metrics(conn, metrics)
                messages.append(f"{api_date}: fetched={day_fetched}")
        status = "ok"
    except Exception as exc:  # pragma: no cover - CLI safety path
        error_total += 1
        status = "error"
        messages.append(f"error={type(exc).__name__}: {exc}")
        raise
    finally:
        finished_at = datetime.now().isoformat(timespec="seconds")
        record_ingest_run(
            conn,
            run_id=run_id,
            run_type="jquants_statements",
            date_from=date_from,
            date_to=date_to,
            codes=requested_codes,
            started_at=started_at,
            finished_at=finished_at,
            status=locals().get("status", "error"),
            fetched_total=fetched_total,
            saved_total=saved_total,
            skipped_total=skipped_total,
            error_total=error_total,
            summary={"api_version": "v2", "periods": sorted(allowed_periods), "include_forecasts": include_forecasts},
        )
        conn.commit()

    output_path = None
    if output_dir is not None:
        output_path = Path(output_dir) / f"jquants_statements_{date_from}_to_{date_to}_{datetime.now():%Y%m%d_%H%M%S}.txt"
        _write_summary(
            output_path,
            [
                f"run_id: {run_id}",
                f"date_from: {date_from}",
                f"date_to: {date_to}",
                f"periods: {','.join(sorted(allowed_periods))}",
                f"include_forecasts: {include_forecasts}",
                f"fetched_total: {fetched_total}",
                f"saved_metrics_total: {saved_total}",
                f"skipped_total: {skipped_total}",
                f"error_total: {error_total}",
                "",
                *messages,
            ],
        )
    return JQuantsIngestResult(
        run_id=run_id,
        run_type="jquants_statements",
        date_from=date_from,
        date_to=date_to,
        fetched_total=fetched_total,
        saved_total=saved_total,
        skipped_total=skipped_total,
        error_total=error_total,
        output_path=output_path,
        messages=messages,
    )


def _convert_statement_item(item: dict, *, periods: set[str], include_forecasts: bool):
    raw = build_statement_raw(item)
    metrics = statement_metrics_from_row(
        item,
        periods=periods,
        include_forecasts=include_forecasts,
    )
    return raw, metrics


def _statement_in_range(disclosed_date: str, date_from: str, date_to: str) -> bool:
    if not disclosed_date:
        return False
    return date_from <= disclosed_date <= date_to


def save_jquants_daily_quotes(
    conn: sqlite3.Connection,
    *,
    client: JQuantsClient,
    date_from: str,
    date_to: str,
    codes: list[str] | None = None,
    output_dir: str | Path | None = None,
) -> JQuantsIngestResult:
    run_id = f"jquants_quotes_{uuid.uuid4().hex[:12]}"
    started_at = datetime.now().isoformat(timespec="seconds")
    requested_codes = [code.strip() for code in (codes or []) if code.strip()]
    fetched_total = 0
    saved_total = 0
    skipped_total = 0
    error_total = 0
    messages: list[str] = []

    try:
        if requested_codes:
            for code in requested_codes:
                count = 0
                for item in client.iter_eq_bars_daily(code=code, date_from=date_from, date_to=date_to):
                    quote = quote_from_row(item)
                    fetched_total += 1
                    count += 1
                    upsert_quote(conn, quote)
                    saved_total += 1
                messages.append(f"{code}: fetched={count}")
        else:
            for current in _iter_dates(date_from, date_to):
                api_date = current.isoformat()
                count = 0
                for item in client.iter_eq_bars_daily(date=api_date):
                    quote = quote_from_row(item)
                    fetched_total += 1
                    count += 1
                    upsert_quote(conn, quote)
                    saved_total += 1
                messages.append(f"{api_date}: fetched={count}")
        status = "ok"
    except Exception as exc:  # pragma: no cover - CLI safety path
        error_total += 1
        status = "error"
        messages.append(f"error={type(exc).__name__}: {exc}")
        raise
    finally:
        finished_at = datetime.now().isoformat(timespec="seconds")
        record_ingest_run(
            conn,
            run_id=run_id,
            run_type="jquants_daily_quotes",
            date_from=date_from,
            date_to=date_to,
            codes=requested_codes,
            started_at=started_at,
            finished_at=finished_at,
            status=locals().get("status", "error"),
            fetched_total=fetched_total,
            saved_total=saved_total,
            skipped_total=skipped_total,
            error_total=error_total,
            summary={"api_version": "v2"},
        )
        conn.commit()

    output_path = None
    if output_dir is not None:
        output_path = Path(output_dir) / f"jquants_daily_quotes_{date_from}_to_{date_to}_{datetime.now():%Y%m%d_%H%M%S}.txt"
        _write_summary(
            output_path,
            [
                f"run_id: {run_id}",
                f"date_from: {date_from}",
                f"date_to: {date_to}",
                f"codes: {','.join(requested_codes) if requested_codes else 'all'}",
                f"fetched_total: {fetched_total}",
                f"saved_quotes_total: {saved_total}",
                f"skipped_total: {skipped_total}",
                f"error_total: {error_total}",
                "",
                *messages,
            ],
        )
    return JQuantsIngestResult(
        run_id=run_id,
        run_type="jquants_daily_quotes",
        date_from=date_from,
        date_to=date_to,
        fetched_total=fetched_total,
        saved_total=saved_total,
        skipped_total=skipped_total,
        error_total=error_total,
        output_path=output_path,
        messages=messages,
    )
