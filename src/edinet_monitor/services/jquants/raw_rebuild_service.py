from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
import json
from pathlib import Path
import sqlite3

from edinet_monitor.services.jquants.mapper import ACTUAL_PERIODS, statement_metrics_from_row
from edinet_monitor.services.jquants.repository import (
    upsert_cash_balance_growth_metrics,
    upsert_financial_metrics,
)


@dataclass(frozen=True)
class JQuantsRawRebuildResult:
    date_from: str
    date_to: str
    periods: tuple[str, ...]
    apply: bool
    raw_rows: int
    metrics_built: int
    metrics_saved: int
    skipped_rows: int
    error_rows: int
    output_path: Path | None
    messages: list[str] = field(default_factory=list)


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table_name,),
    ).fetchone()
    return row is not None


def _split_codes(codes: list[str] | tuple[str, ...] | None) -> list[str]:
    result: list[str] = []
    for code in codes or []:
        text = str(code or "").strip()
        if not text or text.lower() == "all":
            continue
        result.append(text[:4] if len(text) >= 4 else text)
    return result


def _write_summary(path: Path | None, lines: list[str]) -> Path | None:
    if path is None:
        return None
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8-sig")
    return path


def rebuild_jquants_financial_metrics_from_raw(
    conn: sqlite3.Connection,
    *,
    date_from: str,
    date_to: str,
    periods: set[str] | None = None,
    include_forecasts: bool = True,
    codes: list[str] | tuple[str, ...] | None = None,
    apply: bool = False,
    output_dir: str | Path | None = None,
) -> JQuantsRawRebuildResult:
    """Rebuild jquants_financial_metrics from stored raw JSON without API calls."""

    if not _table_exists(conn, "jquants_statement_raw"):
        raise RuntimeError("jquants_statement_raw table does not exist")
    if not _table_exists(conn, "jquants_financial_metrics"):
        raise RuntimeError("jquants_financial_metrics table does not exist")

    selected_periods = tuple(sorted(periods or (ACTUAL_PERIODS | {"FY", "4Q"})))
    actual_periods = set(selected_periods) & ACTUAL_PERIODS
    mapper_periods = actual_periods if actual_periods else {"__none__"}
    normalized_codes = _split_codes(list(codes or []))
    where = [
        "COALESCE(disclosed_date, '') BETWEEN ? AND ?",
        "COALESCE(raw_json, '') <> ''",
    ]
    params: list[str] = [date_from, date_to]
    if selected_periods:
        where.append(
            "COALESCE(type_of_current_period, '') IN ("
            + ",".join("?" for _ in selected_periods)
            + ")"
        )
        params.extend(selected_periods)
    if normalized_codes:
        where.append(
            "(substr(COALESCE(local_code, ''), 1, 4) IN ("
            + ",".join("?" for _ in normalized_codes)
            + ") OR COALESCE(security_code, '') IN ("
            + ",".join("?" for _ in normalized_codes)
            + "))"
        )
        params.extend(normalized_codes)
        params.extend(normalized_codes)

    rows = conn.execute(
        f"""
        SELECT disclosure_number, disclosed_date, local_code, security_code,
               type_of_current_period, raw_json
        FROM jquants_statement_raw
        WHERE {' AND '.join(where)}
        ORDER BY disclosed_date, disclosure_number
        """,
        params,
    ).fetchall()

    raw_rows = 0
    metrics_built = 0
    metrics_saved = 0
    skipped_rows = 0
    error_rows = 0
    sample_errors: list[str] = []
    saved_disclosures: list[str] = []

    for row in rows:
        raw_rows += 1
        try:
            item = json.loads(row["raw_json"])
        except Exception as exc:
            error_rows += 1
            if len(sample_errors) < 20:
                sample_errors.append(
                    f"{row['disclosure_number']}: raw_json_parse_error={type(exc).__name__}: {exc}"
                )
            continue
        if not isinstance(item, dict):
            skipped_rows += 1
            if len(sample_errors) < 20:
                sample_errors.append(f"{row['disclosure_number']}: raw_json_not_object")
            continue

        metrics = statement_metrics_from_row(
            item,
            periods=mapper_periods,
            include_forecasts=include_forecasts,
        )
        metrics_built += len(metrics)
        if apply and metrics:
            metrics_saved += upsert_financial_metrics(conn, metrics)
            saved_disclosures.append(str(row["disclosure_number"]))

    if apply:
        metrics_saved += upsert_cash_balance_growth_metrics(
            conn,
            disclosure_numbers=saved_disclosures,
        )
        conn.commit()

    output_path = None
    if output_dir is not None:
        output_path = (
            Path(output_dir)
            / f"jquants_metrics_rebuild_from_raw_{date_from}_to_{date_to}_{datetime.now():%Y%m%d_%H%M%S}.txt"
        )
        _write_summary(
            output_path,
            [
                f"generated_at: {datetime.now().isoformat(timespec='seconds')}",
                f"date_from: {date_from}",
                f"date_to: {date_to}",
                f"periods: {','.join(selected_periods)}",
                f"actual_periods: {','.join(sorted(actual_periods)) if actual_periods else '(none)'}",
                f"codes: {','.join(normalized_codes) if normalized_codes else 'all'}",
                f"include_forecasts: {int(include_forecasts)}",
                f"apply: {int(apply)}",
                f"raw_rows: {raw_rows}",
                f"metrics_built: {metrics_built}",
                f"metrics_saved: {metrics_saved}",
                f"skipped_rows: {skipped_rows}",
                f"error_rows: {error_rows}",
                "",
                *sample_errors,
            ],
        )

    return JQuantsRawRebuildResult(
        date_from=date_from,
        date_to=date_to,
        periods=selected_periods,
        apply=apply,
        raw_rows=raw_rows,
        metrics_built=metrics_built,
        metrics_saved=metrics_saved,
        skipped_rows=skipped_rows,
        error_rows=error_rows,
        output_path=output_path,
        messages=sample_errors,
    )
