from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
import json
import sqlite3
import time
import uuid
from typing import Any, Iterator

from edinet_monitor.db.migrations import apply_schema_migrations


DEFAULT_KEEP_RUNS_PER_COMMAND = 100
SPAN_CATEGORIES = {"db_read", "parse", "compute", "db_write", "file_io", "other"}


@dataclass(frozen=True)
class PerformanceSpan:
    span_category: str
    span_name: str
    elapsed_seconds: float
    count_total: int = 0
    detail: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class PerformanceRun:
    run_id: str
    command_name: str
    stage_name: str
    started_at: str
    finished_at: str
    elapsed_seconds: float
    status: str
    workers: int
    batch_size: int
    target_total: int
    success_total: int
    skipped_total: int
    error_total: int
    output_rows_total: int
    processed_per_minute: float
    db_read_elapsed_seconds: float
    parse_elapsed_seconds: float
    compute_elapsed_seconds: float
    db_write_elapsed_seconds: float
    file_io_elapsed_seconds: float
    parameters: dict[str, Any]
    error_summary: dict[str, Any]
    summary: dict[str, Any]


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _json_text(value: Any) -> str:
    return json.dumps(value or {}, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _json_dict(text: Any) -> dict[str, Any]:
    if not text:
        return {}
    try:
        value = json.loads(str(text))
    except Exception:
        return {}
    return value if isinstance(value, dict) else {}


def _normalize_category(span_category: str) -> str:
    category = str(span_category or "other").strip() or "other"
    return category if category in SPAN_CATEGORIES else "other"


def _ensure_ready(conn: sqlite3.Connection) -> None:
    apply_schema_migrations(conn)
    conn.commit()


def _row_to_run(row: sqlite3.Row) -> PerformanceRun:
    return PerformanceRun(
        run_id=str(row["run_id"] or ""),
        command_name=str(row["command_name"] or ""),
        stage_name=str(row["stage_name"] or ""),
        started_at=str(row["started_at"] or ""),
        finished_at=str(row["finished_at"] or ""),
        elapsed_seconds=float(row["elapsed_seconds"] or 0.0),
        status=str(row["status"] or ""),
        workers=int(row["workers"] or 0),
        batch_size=int(row["batch_size"] or 0),
        target_total=int(row["target_total"] or 0),
        success_total=int(row["success_total"] or 0),
        skipped_total=int(row["skipped_total"] or 0),
        error_total=int(row["error_total"] or 0),
        output_rows_total=int(row["output_rows_total"] or 0),
        processed_per_minute=float(row["processed_per_minute"] or 0.0),
        db_read_elapsed_seconds=float(row["db_read_elapsed_seconds"] or 0.0),
        parse_elapsed_seconds=float(row["parse_elapsed_seconds"] or 0.0),
        compute_elapsed_seconds=float(row["compute_elapsed_seconds"] or 0.0),
        db_write_elapsed_seconds=float(row["db_write_elapsed_seconds"] or 0.0),
        file_io_elapsed_seconds=float(row["file_io_elapsed_seconds"] or 0.0),
        parameters=_json_dict(row["parameters_json"]),
        error_summary=_json_dict(row["error_summary_json"]),
        summary=_json_dict(row["summary_json"]),
    )


def _elapsed_by_category(spans: list[PerformanceSpan], category: str) -> float:
    return round(sum(span.elapsed_seconds for span in spans if span.span_category == category), 6)


def _processed_per_minute(*, elapsed_seconds: float, success_total: int, skipped_total: int, error_total: int) -> float:
    processed = int(success_total or 0) + int(skipped_total or 0) + int(error_total or 0)
    if elapsed_seconds <= 0 or processed <= 0:
        return 0.0
    return round(processed / elapsed_seconds * 60.0, 3)


def prune_performance_runs(
    conn: sqlite3.Connection,
    *,
    command_name: str,
    keep_runs: int = DEFAULT_KEEP_RUNS_PER_COMMAND,
) -> int:
    _ensure_ready(conn)
    rows = conn.execute(
        """
        SELECT run_id
        FROM pipeline_performance_runs
        WHERE command_name = ?
        ORDER BY started_at DESC, id DESC
        LIMIT -1 OFFSET ?
        """,
        (str(command_name), int(keep_runs)),
    ).fetchall()
    old_run_ids = [str(row[0]) for row in rows]
    if not old_run_ids:
        return 0

    placeholders = ",".join("?" for _ in old_run_ids)
    conn.execute(
        f"DELETE FROM pipeline_performance_spans WHERE run_id IN ({placeholders})",
        old_run_ids,
    )
    conn.execute(
        f"DELETE FROM pipeline_performance_runs WHERE run_id IN ({placeholders})",
        old_run_ids,
    )
    conn.commit()
    return len(old_run_ids)


def save_performance_run(
    conn: sqlite3.Connection,
    *,
    run_id: str,
    command_name: str,
    stage_name: str,
    started_at: str,
    finished_at: str,
    elapsed_seconds: float,
    status: str,
    workers: int = 1,
    batch_size: int = 0,
    target_total: int = 0,
    success_total: int = 0,
    skipped_total: int = 0,
    error_total: int = 0,
    output_rows_total: int = 0,
    parameters: dict[str, Any] | None = None,
    error_summary: dict[str, Any] | None = None,
    summary: dict[str, Any] | None = None,
    spans: list[PerformanceSpan] | None = None,
    keep_runs_per_command: int = DEFAULT_KEEP_RUNS_PER_COMMAND,
) -> PerformanceRun:
    _ensure_ready(conn)
    clean_spans = list(spans or [])
    db_read_elapsed = _elapsed_by_category(clean_spans, "db_read")
    parse_elapsed = _elapsed_by_category(clean_spans, "parse")
    compute_elapsed = _elapsed_by_category(clean_spans, "compute")
    db_write_elapsed = _elapsed_by_category(clean_spans, "db_write")
    file_io_elapsed = _elapsed_by_category(clean_spans, "file_io")
    processed_per_minute = _processed_per_minute(
        elapsed_seconds=float(elapsed_seconds or 0.0),
        success_total=int(success_total or 0),
        skipped_total=int(skipped_total or 0),
        error_total=int(error_total or 0),
    )
    created_at = str(started_at or finished_at or _now())
    updated_at = str(finished_at or started_at or _now())

    conn.execute(
        """
        INSERT INTO pipeline_performance_runs (
            run_id, command_name, stage_name, started_at, finished_at,
            elapsed_seconds, status, workers, batch_size, target_total,
            success_total, skipped_total, error_total, output_rows_total,
            processed_per_minute, db_read_elapsed_seconds, parse_elapsed_seconds,
            compute_elapsed_seconds, db_write_elapsed_seconds,
            file_io_elapsed_seconds, parameters_json, error_summary_json,
            summary_json, created_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(run_id) DO UPDATE SET
            command_name = excluded.command_name,
            stage_name = excluded.stage_name,
            started_at = excluded.started_at,
            finished_at = excluded.finished_at,
            elapsed_seconds = excluded.elapsed_seconds,
            status = excluded.status,
            workers = excluded.workers,
            batch_size = excluded.batch_size,
            target_total = excluded.target_total,
            success_total = excluded.success_total,
            skipped_total = excluded.skipped_total,
            error_total = excluded.error_total,
            output_rows_total = excluded.output_rows_total,
            processed_per_minute = excluded.processed_per_minute,
            db_read_elapsed_seconds = excluded.db_read_elapsed_seconds,
            parse_elapsed_seconds = excluded.parse_elapsed_seconds,
            compute_elapsed_seconds = excluded.compute_elapsed_seconds,
            db_write_elapsed_seconds = excluded.db_write_elapsed_seconds,
            file_io_elapsed_seconds = excluded.file_io_elapsed_seconds,
            parameters_json = excluded.parameters_json,
            error_summary_json = excluded.error_summary_json,
            summary_json = excluded.summary_json,
            updated_at = excluded.updated_at
        """,
        (
            str(run_id),
            str(command_name),
            str(stage_name or command_name),
            str(started_at),
            str(finished_at),
            float(elapsed_seconds or 0.0),
            str(status or "unknown"),
            int(workers or 1),
            int(batch_size or 0),
            int(target_total or 0),
            int(success_total or 0),
            int(skipped_total or 0),
            int(error_total or 0),
            int(output_rows_total or 0),
            processed_per_minute,
            db_read_elapsed,
            parse_elapsed,
            compute_elapsed,
            db_write_elapsed,
            file_io_elapsed,
            _json_text(parameters or {}),
            _json_text(error_summary or {}),
            _json_text(summary or {}),
            created_at,
            updated_at,
        ),
    )
    conn.execute("DELETE FROM pipeline_performance_spans WHERE run_id = ?", (str(run_id),))
    for span in clean_spans:
        conn.execute(
            """
            INSERT INTO pipeline_performance_spans (
                run_id, span_category, span_name, elapsed_seconds,
                count_total, detail_json, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(run_id),
                _normalize_category(span.span_category),
                str(span.span_name or ""),
                float(span.elapsed_seconds or 0.0),
                int(span.count_total or 0),
                _json_text(span.detail),
                updated_at,
            ),
        )
    conn.commit()
    prune_performance_runs(conn, command_name=str(command_name), keep_runs=keep_runs_per_command)
    return get_performance_run(conn, str(run_id))  # type: ignore[return-value]


def get_performance_run(conn: sqlite3.Connection, run_id: str) -> PerformanceRun | None:
    _ensure_ready(conn)
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        """
        SELECT *
        FROM pipeline_performance_runs
        WHERE run_id = ?
        """,
        (str(run_id),),
    ).fetchone()
    return _row_to_run(row) if row else None


def list_performance_runs(
    conn: sqlite3.Connection,
    *,
    command_name: str = "",
    limit: int = 10,
) -> list[PerformanceRun]:
    _ensure_ready(conn)
    conn.row_factory = sqlite3.Row
    if command_name:
        rows = conn.execute(
            """
            SELECT *
            FROM pipeline_performance_runs
            WHERE command_name = ?
            ORDER BY started_at DESC, id DESC
            LIMIT ?
            """,
            (str(command_name), int(limit)),
        ).fetchall()
    else:
        rows = conn.execute(
            """
            SELECT *
            FROM pipeline_performance_runs
            ORDER BY started_at DESC, id DESC
            LIMIT ?
            """,
            (int(limit),),
        ).fetchall()
    return [_row_to_run(row) for row in rows]


class PerformanceLog:
    def __init__(
        self,
        *,
        command_name: str,
        stage_name: str = "",
        workers: int = 1,
        batch_size: int = 0,
        parameters: dict[str, Any] | None = None,
        timer_func: Any = time.perf_counter,
    ) -> None:
        self.command_name = str(command_name)
        self.stage_name = str(stage_name or command_name)
        self.workers = int(workers or 1)
        self.batch_size = int(batch_size or 0)
        self.parameters = dict(parameters or {})
        self.run_id = f"{self.command_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
        self.started_at = _now()
        self._timer_func = timer_func
        self._started_timer = float(timer_func())
        self.spans: list[PerformanceSpan] = []

    @contextmanager
    def measure(
        self,
        span_category: str,
        span_name: str,
        *,
        count_total: int = 0,
        detail: dict[str, Any] | None = None,
    ) -> Iterator[None]:
        started = float(self._timer_func())
        try:
            yield
        finally:
            elapsed = max(float(self._timer_func()) - started, 0.0)
            self.add_span(
                span_category,
                span_name,
                elapsed_seconds=elapsed,
                count_total=count_total,
                detail=detail,
            )

    def add_span(
        self,
        span_category: str,
        span_name: str,
        *,
        elapsed_seconds: float,
        count_total: int = 0,
        detail: dict[str, Any] | None = None,
    ) -> None:
        self.spans.append(
            PerformanceSpan(
                span_category=_normalize_category(span_category),
                span_name=str(span_name),
                elapsed_seconds=round(float(elapsed_seconds or 0.0), 6),
                count_total=int(count_total or 0),
                detail=dict(detail or {}),
            )
        )

    def finish(
        self,
        conn: sqlite3.Connection,
        *,
        status: str,
        target_total: int = 0,
        success_total: int = 0,
        skipped_total: int = 0,
        error_total: int = 0,
        output_rows_total: int = 0,
        error_summary: dict[str, Any] | None = None,
        summary: dict[str, Any] | None = None,
    ) -> PerformanceRun:
        finished_at = _now()
        elapsed = round(max(float(self._timer_func()) - self._started_timer, 0.0), 6)
        return save_performance_run(
            conn,
            run_id=self.run_id,
            command_name=self.command_name,
            stage_name=self.stage_name,
            started_at=self.started_at,
            finished_at=finished_at,
            elapsed_seconds=elapsed,
            status=status,
            workers=self.workers,
            batch_size=self.batch_size,
            target_total=target_total,
            success_total=success_total,
            skipped_total=skipped_total,
            error_total=error_total,
            output_rows_total=output_rows_total,
            parameters=self.parameters,
            error_summary=error_summary or {},
            summary=summary or {},
            spans=self.spans,
        )
