from __future__ import annotations

from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
import csv
import hashlib
import io
import sqlite3
import time
from typing import Any

from edinet_monitor.db.schema import get_connection
from edinet_monitor.services.performance_log_service import PerformanceLog


COMMAND_NAME = "audit_jquants_equity_ratio_outliers"


@dataclass(frozen=True)
class EquityRatioOutlier:
    company_name: str
    security_code: str
    fiscal_year: int
    period_key: str
    quarter_type: str
    period_end: str
    disclosed_date: str
    disclosure_number: str
    value_num: float
    classification: str

    @property
    def identity(self) -> tuple[str, str, int, str, str, float, str]:
        return (
            self.security_code,
            self.disclosure_number,
            self.fiscal_year,
            self.period_key,
            self.period_end,
            self.value_num,
            self.classification,
        )


@dataclass(frozen=True)
class EquityRatioOutlierAuditResult:
    run_id: str
    workers: int
    checked_total: int
    partition_total: int
    anomaly_total: int
    negative_total: int
    over_150_percent_total: int
    elapsed_seconds: float
    db_read_elapsed_seconds: float
    compute_elapsed_seconds: float
    outliers: list[EquityRatioOutlier]


@dataclass(frozen=True)
class EquityRatioOutlierBenchmarkResult:
    serial: EquityRatioOutlierAuditResult
    parallel: EquityRatioOutlierAuditResult
    equivalent: bool


def _readonly_connection(db_path: str | Path) -> sqlite3.Connection:
    path = Path(db_path)
    conn = sqlite3.connect(f"file:{path.as_posix()}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def _fetch_latest_equity_ratio_rows(
    db_path: str | Path,
    *,
    date_from: str,
    date_to: str,
) -> list[dict[str, Any]]:
    conn = _readonly_connection(db_path)
    try:
        rows = conn.execute(
            """
            WITH ranked AS (
                SELECT
                    m.*,
                    CASE
                        WHEN COALESCE(m.security_code, '') <> '' THEN m.security_code
                        WHEN length(COALESCE(m.local_code, '')) = 5
                             AND substr(m.local_code, 5, 1) = '0'
                        THEN substr(m.local_code, 1, 4)
                        ELSE COALESCE(m.local_code, '')
                    END AS normalized_security_code,
                    ROW_NUMBER() OVER (
                        PARTITION BY
                            CASE
                                WHEN COALESCE(m.security_code, '') <> '' THEN m.security_code
                                WHEN length(COALESCE(m.local_code, '')) = 5
                                     AND substr(m.local_code, 5, 1) = '0'
                                THEN substr(m.local_code, 1, 4)
                                ELSE COALESCE(m.local_code, '')
                            END,
                            COALESCE(m.metric_kind, ''),
                            COALESCE(m.period_key, ''),
                            COALESCE(m.forecast_target, ''),
                            COALESCE(m.forecast_stage, ''),
                            COALESCE(m.fiscal_year, -1),
                            COALESCE(m.metric_base, '')
                        ORDER BY
                            COALESCE(m.disclosed_date, '') DESC,
                            COALESCE(m.disclosed_time, '') DESC,
                            COALESCE(m.disclosure_number, '') DESC,
                            m.id DESC
                    ) AS row_num
                FROM jquants_financial_metrics m
                WHERE m.metric_base = 'EquityRatio'
            ),
            issuer_by_code AS (
                SELECT
                    CASE
                        WHEN length(COALESCE(security_code, '')) = 5
                             AND substr(security_code, 5, 1) = '0'
                        THEN substr(security_code, 1, 4)
                        ELSE COALESCE(security_code, '')
                    END AS normalized_security_code,
                    MAX(company_name) AS company_name
                FROM issuer_master
                GROUP BY
                    CASE
                        WHEN length(COALESCE(security_code, '')) = 5
                             AND substr(security_code, 5, 1) = '0'
                        THEN substr(security_code, 1, 4)
                        ELSE COALESCE(security_code, '')
                    END
            ),
            listed_ranked AS (
                SELECT
                    CASE
                        WHEN COALESCE(security_code, '') <> '' THEN security_code
                        WHEN length(COALESCE(local_code, '')) = 5
                             AND substr(local_code, 5, 1) = '0'
                        THEN substr(local_code, 1, 4)
                        ELSE COALESCE(local_code, '')
                    END AS normalized_security_code,
                    company_name,
                    ROW_NUMBER() OVER (
                        PARTITION BY
                            CASE
                                WHEN COALESCE(security_code, '') <> '' THEN security_code
                                WHEN length(COALESCE(local_code, '')) = 5
                                     AND substr(local_code, 5, 1) = '0'
                                THEN substr(local_code, 1, 4)
                                ELSE COALESCE(local_code, '')
                            END
                        ORDER BY listing_date DESC, id DESC
                    ) AS row_num
                FROM jquants_listed_info_raw
            ),
            listed_by_code AS (
                SELECT normalized_security_code, company_name
                FROM listed_ranked
                WHERE row_num = 1
            )
            SELECT
                COALESCE(
                    im_edinet.company_name,
                    im_code.company_name,
                    jq_code.company_name,
                    ''
                ) AS company_name,
                ranked.normalized_security_code AS security_code,
                ranked.fiscal_year,
                ranked.period_key,
                COALESCE(ranked.quarter_type, '') AS quarter_type,
                COALESCE(ranked.period_end, '') AS period_end,
                COALESCE(ranked.disclosed_date, '') AS disclosed_date,
                ranked.disclosure_number,
                ranked.value_num
            FROM ranked
            LEFT JOIN issuer_master im_edinet
                ON im_edinet.edinet_code = ranked.edinet_code
            LEFT JOIN issuer_by_code im_code
                ON im_code.normalized_security_code = ranked.normalized_security_code
            LEFT JOIN listed_by_code jq_code
                ON jq_code.normalized_security_code = ranked.normalized_security_code
            WHERE ranked.row_num = 1
              AND ranked.fiscal_year IS NOT NULL
              AND ranked.calc_status = 'ok'
              AND ranked.value_num IS NOT NULL
              AND COALESCE(ranked.period_end, ranked.disclosed_date, '') BETWEEN ? AND ?
            """,
            (date_from, date_to),
        ).fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


def _partition_rows_by_fiscal_year(rows: list[dict[str, Any]]) -> list[list[dict[str, Any]]]:
    by_fiscal_year: dict[int, list[dict[str, Any]]] = {}
    for row in rows:
        fiscal_year = int(row.get("fiscal_year") or 0)
        by_fiscal_year.setdefault(fiscal_year, []).append(row)
    return [by_fiscal_year[fiscal_year] for fiscal_year in sorted(by_fiscal_year)]


def _classify_equity_ratio_partition(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    outliers: list[dict[str, Any]] = []
    for row in rows:
        value_num = float(row.get("value_num") or 0.0)
        if value_num < 0:
            classification = "negative"
        elif value_num > 1.5:
            classification = "over_150_percent"
        else:
            continue
        outliers.append({**row, "value_num": value_num, "classification": classification})
    return outliers


def _classify_partitions(
    partitions: list[list[dict[str, Any]]],
    *,
    workers: int,
) -> list[dict[str, Any]]:
    if workers <= 1:
        outliers: list[dict[str, Any]] = []
        for partition in partitions:
            outliers.extend(_classify_equity_ratio_partition(partition))
        return outliers

    outliers = []
    with ProcessPoolExecutor(max_workers=workers) as executor:
        futures = [
            executor.submit(_classify_equity_ratio_partition, partition)
            for partition in partitions
        ]
        for future in as_completed(futures):
            outliers.extend(future.result())
    return outliers


def _quarter_rank(period_key: str, quarter_type: str) -> int:
    text = f"{period_key} {quarter_type}".upper()
    for label, rank in (("FY", 4), ("4Q", 4), ("3Q", 3), ("2Q", 2), ("1Q", 1)):
        if label in text:
            return rank
    return 0


def _to_outlier(row: dict[str, Any]) -> EquityRatioOutlier:
    return EquityRatioOutlier(
        company_name=str(row.get("company_name") or ""),
        security_code=str(row.get("security_code") or ""),
        fiscal_year=int(row.get("fiscal_year") or 0),
        period_key=str(row.get("period_key") or ""),
        quarter_type=str(row.get("quarter_type") or ""),
        period_end=str(row.get("period_end") or ""),
        disclosed_date=str(row.get("disclosed_date") or ""),
        disclosure_number=str(row.get("disclosure_number") or ""),
        value_num=float(row.get("value_num") or 0.0),
        classification=str(row.get("classification") or ""),
    )


def _sort_outliers(outliers: list[EquityRatioOutlier]) -> list[EquityRatioOutlier]:
    return sorted(
        outliers,
        key=lambda row: (
            row.period_end,
            row.fiscal_year,
            _quarter_rank(row.period_key, row.quarter_type),
            row.disclosed_date,
            row.disclosure_number,
        ),
        reverse=True,
    )


def _result_digest(outliers: list[EquityRatioOutlier]) -> str:
    text = "\n".join(repr(outlier.identity) for outlier in sorted(outliers, key=lambda row: row.identity))
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def run_jquants_equity_ratio_outlier_audit(
    *,
    db_path: str | Path,
    date_from: str,
    date_to: str,
    workers: int = 1,
    save_performance_log: bool = True,
) -> EquityRatioOutlierAuditResult:
    target_workers = max(int(workers or 1), 1)
    perf_log = PerformanceLog(
        command_name=COMMAND_NAME,
        workers=target_workers,
        parameters={
            "date_from": date_from,
            "date_to": date_to,
            "workers": target_workers,
            "scan_strategy": "single_readonly_bulk_join_then_fiscal_year_compute_partitions",
        },
    )
    started = time.perf_counter()

    with perf_log.measure("db_read", "bulk_join_latest_equity_ratio"):
        rows = _fetch_latest_equity_ratio_rows(
            db_path,
            date_from=date_from,
            date_to=date_to,
        )
    db_read_elapsed = perf_log.spans[-1].elapsed_seconds

    partitions = _partition_rows_by_fiscal_year(rows)
    with perf_log.measure(
        "compute",
        "classify_fiscal_year_partitions",
        count_total=len(rows),
        detail={"partition_total": len(partitions)},
    ):
        raw_outliers = _classify_partitions(partitions, workers=target_workers)
        outliers = _sort_outliers([_to_outlier(row) for row in raw_outliers])
    compute_elapsed = perf_log.spans[-1].elapsed_seconds

    negative_total = sum(outlier.classification == "negative" for outlier in outliers)
    over_150_percent_total = sum(
        outlier.classification == "over_150_percent"
        for outlier in outliers
    )
    elapsed = round(max(time.perf_counter() - started, 0.0), 6)
    run_id = perf_log.run_id
    if save_performance_log:
        conn = get_connection(db_path)
        try:
            performance_run = perf_log.finish(
                conn,
                status="success",
                target_total=len(rows),
                success_total=len(rows),
                output_rows_total=len(outliers),
                summary={
                    "anomaly_total": len(outliers),
                    "negative_total": negative_total,
                    "over_150_percent_total": over_150_percent_total,
                    "partition_total": len(partitions),
                    "result_digest": _result_digest(outliers),
                },
            )
            run_id = performance_run.run_id
            elapsed = performance_run.elapsed_seconds
        finally:
            conn.close()

    return EquityRatioOutlierAuditResult(
        run_id=run_id,
        workers=target_workers,
        checked_total=len(rows),
        partition_total=len(partitions),
        anomaly_total=len(outliers),
        negative_total=negative_total,
        over_150_percent_total=over_150_percent_total,
        elapsed_seconds=elapsed,
        db_read_elapsed_seconds=db_read_elapsed,
        compute_elapsed_seconds=compute_elapsed,
        outliers=outliers,
    )


def benchmark_jquants_equity_ratio_outlier_audit(
    *,
    db_path: str | Path,
    date_from: str,
    date_to: str,
    parallel_workers: int = 2,
) -> EquityRatioOutlierBenchmarkResult:
    serial = run_jquants_equity_ratio_outlier_audit(
        db_path=db_path,
        date_from=date_from,
        date_to=date_to,
        workers=1,
    )
    parallel = run_jquants_equity_ratio_outlier_audit(
        db_path=db_path,
        date_from=date_from,
        date_to=date_to,
        workers=max(int(parallel_workers or 2), 2),
    )
    equivalent = (
        serial.checked_total == parallel.checked_total
        and _result_digest(serial.outliers) == _result_digest(parallel.outliers)
    )
    if not equivalent:
        raise RuntimeError("serial and parallel equity-ratio audit results do not match")
    return EquityRatioOutlierBenchmarkResult(
        serial=serial,
        parallel=parallel,
        equivalent=equivalent,
    )


def write_jquants_equity_ratio_outlier_tsv(
    *,
    result: EquityRatioOutlierAuditResult,
    output_dir: str | Path,
    date_from: str,
    date_to: str,
) -> Path:
    output_root = Path(output_dir)
    output_root.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = output_root / (
        f"jquants_equity_ratio_outliers_{date_from}_to_{date_to}_{timestamp}.tsv"
    )
    headers = [
        "company_name",
        "security_code",
        "fiscal_year",
        "period_key",
        "quarter_type",
        "period_end",
        "disclosed_date",
        "value_num",
        "value_percent",
        "classification",
        "disclosure_number",
    ]
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=headers, delimiter="\t", lineterminator="\n")
    writer.writeheader()
    for row in result.outliers:
        writer.writerow(
            {
                "company_name": row.company_name,
                "security_code": row.security_code,
                "fiscal_year": row.fiscal_year,
                "period_key": row.period_key,
                "quarter_type": row.quarter_type,
                "period_end": row.period_end,
                "disclosed_date": row.disclosed_date,
                "value_num": row.value_num,
                "value_percent": row.value_num * 100,
                "classification": row.classification,
                "disclosure_number": row.disclosure_number,
            }
        )
    output_path.write_text(buffer.getvalue(), encoding="utf-8-sig")
    return output_path
