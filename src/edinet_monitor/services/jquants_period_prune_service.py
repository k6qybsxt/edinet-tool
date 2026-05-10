from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
import json
import sqlite3

from edinet_monitor.config.settings import JQUANTS_STORAGE_ROOT
from edinet_monitor.domain.issuer_flags import TENBAGGER_LEARNING_SECURITY_CODES
from edinet_monitor.services.jquants.raw_json_store import (
    fins_summary_raw_path,
    fins_summary_record_key,
)


@dataclass(frozen=True)
class JQuantsPruneCandidate:
    disclosure_number: str
    local_code: str
    security_code: str
    quarter_type: str
    fiscal_year: int | None
    period_end: str
    disclosed_date: str
    rank: int


@dataclass(frozen=True)
class JQuantsPruneResult:
    apply: bool
    keep_latest: int
    quarter_types: tuple[str, ...]
    candidate_count: int
    deleted_counts: dict[str, int]
    file_counts: dict[str, int]
    output_path: Path | None
    candidates: list[JQuantsPruneCandidate] = field(default_factory=list)


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table_name,),
    ).fetchone()
    return row is not None


def _table_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    if not _table_exists(conn, table_name):
        return set()
    return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table_name})").fetchall()}


def _chunked(items: list[str], size: int = 800) -> list[list[str]]:
    return [items[index : index + size] for index in range(0, len(items), size)]


def _normalize_quarter_types(quarter_types: tuple[str, ...] | None) -> tuple[str, ...]:
    values = tuple(str(item).strip().upper() for item in (quarter_types or ()) if str(item).strip())
    return values or ("1Q", "2Q", "3Q")


def fetch_old_jquants_quarter_candidates(
    conn: sqlite3.Connection,
    *,
    keep_latest: int = 11,
    quarter_types: tuple[str, ...] = ("1Q", "2Q", "3Q"),
    exclude_security_codes: set[str] | frozenset[str] | None = TENBAGGER_LEARNING_SECURITY_CODES,
) -> list[JQuantsPruneCandidate]:
    selected_quarters = _normalize_quarter_types(quarter_types)
    if not _table_exists(conn, "jquants_statement_raw"):
        return []
    placeholders = ",".join("?" for _ in selected_quarters)
    if exclude_security_codes is None:
        exclude_security_codes = TENBAGGER_LEARNING_SECURITY_CODES
    excluded_codes = tuple(sorted(str(code).strip() for code in exclude_security_codes if str(code).strip()))
    exclude_clause = ""
    if excluded_codes:
        exclude_placeholders = ",".join("?" for _ in excluded_codes)
        exclude_clause = (
            "AND substr(COALESCE(r.security_code, r.local_code, ''), 1, 4) "
            f"NOT IN ({exclude_placeholders})"
        )
    rows = conn.execute(
        f"""
        WITH ranked AS (
          SELECT
            r.disclosure_number,
            COALESCE(r.local_code, '') AS local_code,
            COALESCE(r.security_code, '') AS security_code,
            COALESCE(r.type_of_current_period, '') AS quarter_type,
            r.fiscal_year,
            COALESCE(r.current_period_end_date, r.current_fiscal_year_end_date, '') AS period_end,
            COALESCE(r.disclosed_date, '') AS disclosed_date,
            ROW_NUMBER() OVER (
              PARTITION BY r.local_code, r.type_of_current_period
              ORDER BY
                COALESCE(r.current_period_end_date, r.current_fiscal_year_end_date, '') DESC,
                COALESCE(r.disclosed_date, '') DESC,
                COALESCE(r.disclosed_time, '') DESC,
                r.disclosure_number DESC
            ) AS rn
          FROM jquants_statement_raw r
          WHERE r.type_of_current_period IN ({placeholders})
            {exclude_clause}
        )
        SELECT *
        FROM ranked
        WHERE rn > ?
        ORDER BY local_code, quarter_type, rn
        """,
        (*selected_quarters, *excluded_codes, keep_latest),
    ).fetchall()
    return [
        JQuantsPruneCandidate(
            disclosure_number=str(row["disclosure_number"]),
            local_code=str(row["local_code"]),
            security_code=str(row["security_code"]),
            quarter_type=str(row["quarter_type"]),
            fiscal_year=row["fiscal_year"],
            period_end=str(row["period_end"]),
            disclosed_date=str(row["disclosed_date"]),
            rank=int(row["rn"]),
        )
        for row in rows
    ]


def _count_by_disclosure(conn: sqlite3.Connection, table_name: str, disclosure_numbers: list[str]) -> int:
    columns = _table_columns(conn, table_name)
    if not disclosure_numbers or "disclosure_number" not in columns:
        return 0
    total = 0
    for chunk in _chunked(disclosure_numbers):
        placeholders = ",".join("?" for _ in chunk)
        total += int(
            conn.execute(
                f"SELECT COUNT(*) FROM {table_name} WHERE disclosure_number IN ({placeholders})",
                chunk,
            ).fetchone()[0]
            or 0
        )
    return total


def _count_market_rows(conn: sqlite3.Connection, disclosure_numbers: list[str]) -> int:
    columns = _table_columns(conn, "market_derived_metrics")
    if not disclosure_numbers or not {"source_type", "source_id"} <= columns:
        return 0
    total = 0
    for chunk in _chunked(disclosure_numbers):
        placeholders = ",".join("?" for _ in chunk)
        total += int(
            conn.execute(
                f"""
                SELECT COUNT(*)
                FROM market_derived_metrics
                WHERE source_type = 'jquants'
                  AND source_id IN ({placeholders})
                """,
                chunk,
            ).fetchone()[0]
            or 0
        )
    return total


def _delete_by_disclosure(conn: sqlite3.Connection, table_name: str, disclosure_numbers: list[str]) -> int:
    columns = _table_columns(conn, table_name)
    if not disclosure_numbers or "disclosure_number" not in columns:
        return 0
    deleted = 0
    for chunk in _chunked(disclosure_numbers):
        placeholders = ",".join("?" for _ in chunk)
        cursor = conn.execute(
            f"DELETE FROM {table_name} WHERE disclosure_number IN ({placeholders})",
            chunk,
        )
        deleted += int(cursor.rowcount or 0)
    return deleted


def _delete_market_rows(conn: sqlite3.Connection, disclosure_numbers: list[str]) -> int:
    columns = _table_columns(conn, "market_derived_metrics")
    if not disclosure_numbers or not {"source_type", "source_id"} <= columns:
        return 0
    deleted = 0
    for chunk in _chunked(disclosure_numbers):
        placeholders = ",".join("?" for _ in chunk)
        cursor = conn.execute(
            f"""
            DELETE FROM market_derived_metrics
            WHERE source_type = 'jquants'
              AND source_id IN ({placeholders})
            """,
            chunk,
        )
        deleted += int(cursor.rowcount or 0)
    return deleted


def _prune_raw_json_records(
    candidates: list[JQuantsPruneCandidate],
    *,
    storage_root: str | Path,
    apply: bool,
) -> dict[str, int]:
    counts = {
        "raw_json_records": 0,
        "raw_json_files_scanned": 0,
        "raw_json_files_updated": 0,
        "raw_json_files_deleted": 0,
        "raw_json_files_missing": 0,
    }
    if not candidates:
        return counts

    disclosure_numbers = {candidate.disclosure_number for candidate in candidates}
    dates = sorted({candidate.disclosed_date for candidate in candidates if candidate.disclosed_date})
    for date_text in dates:
        path = fins_summary_raw_path(date_text, storage_root=storage_root)
        if not path.exists():
            counts["raw_json_files_missing"] += 1
            continue
        counts["raw_json_files_scanned"] += 1
        kept_rows: list[dict[str, object]] = []
        removed = 0
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                kept_rows.append({"__raw_line__": line})
                continue
            if isinstance(row, dict) and fins_summary_record_key(row) in disclosure_numbers:
                removed += 1
                continue
            if isinstance(row, dict):
                kept_rows.append(row)
        if removed == 0:
            continue
        counts["raw_json_records"] += removed
        serializable_rows = [row for row in kept_rows if "__raw_line__" not in row]
        raw_lines = [str(row["__raw_line__"]) for row in kept_rows if "__raw_line__" in row]
        if serializable_rows or raw_lines:
            counts["raw_json_files_updated"] += 1
            if apply:
                lines = [
                    json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
                    for row in serializable_rows
                ] + raw_lines
                path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        else:
            counts["raw_json_files_deleted"] += 1
            if apply:
                path.unlink(missing_ok=True)
                try:
                    path.parent.rmdir()
                except OSError:
                    pass
    return counts


def _write_report(
    *,
    output_dir: str | Path | None,
    apply: bool,
    keep_latest: int,
    quarter_types: tuple[str, ...],
    candidates: list[JQuantsPruneCandidate],
    counts: dict[str, int],
    file_counts: dict[str, int],
) -> Path | None:
    if output_dir is None:
        return None
    directory = Path(output_dir)
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"jquants_quarter_prune_{datetime.now():%Y%m%d_%H%M%S}.txt"
    lines: list[str] = [
        f"generated_at: {datetime.now().isoformat(timespec='seconds')}",
        f"apply: {1 if apply else 0}",
        f"keep_latest: {keep_latest}",
        f"quarter_types: {','.join(quarter_types)}",
        f"candidate_disclosures: {len(candidates)}",
        "",
        "[counts]",
    ]
    lines.extend(f"{key}: {value}" for key, value in counts.items())
    lines.extend(["", "[files]"])
    lines.extend(f"{key}: {value}" for key, value in file_counts.items())
    lines.extend(["", "[candidate_disclosures]"])
    for candidate in candidates[:300]:
        lines.append(
            " | ".join(
                [
                    candidate.local_code,
                    candidate.security_code,
                    candidate.quarter_type,
                    f"rank={candidate.rank}",
                    candidate.period_end,
                    candidate.disclosed_date,
                    candidate.disclosure_number,
                ]
            )
        )
    if len(candidates) > 300:
        lines.append(f"... omitted {len(candidates) - 300} candidates")
    lines.extend(
        [
            "",
            "[note]",
            "DB rows only. Raw JSONL files and jquants_daily_quotes are not deleted.",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8-sig")
    return path


def prune_old_jquants_quarter_data(
    conn: sqlite3.Connection,
    *,
    keep_latest: int = 11,
    quarter_types: tuple[str, ...] = ("1Q", "2Q", "3Q"),
    exclude_security_codes: set[str] | frozenset[str] | None = TENBAGGER_LEARNING_SECURITY_CODES,
    delete_files: bool = False,
    apply: bool = False,
    output_dir: str | Path | None = None,
    storage_root: str | Path = JQUANTS_STORAGE_ROOT,
) -> JQuantsPruneResult:
    selected_quarters = _normalize_quarter_types(quarter_types)
    if exclude_security_codes is None:
        exclude_security_codes = TENBAGGER_LEARNING_SECURITY_CODES
    candidates = fetch_old_jquants_quarter_candidates(
        conn,
        keep_latest=keep_latest,
        quarter_types=selected_quarters,
        exclude_security_codes=exclude_security_codes,
    )
    disclosure_numbers = [candidate.disclosure_number for candidate in candidates]
    counts = {
        "jquants_financial_metrics": _count_by_disclosure(
            conn, "jquants_financial_metrics", disclosure_numbers
        ),
        "market_derived_metrics": _count_market_rows(conn, disclosure_numbers),
        "jquants_statement_raw": _count_by_disclosure(
            conn, "jquants_statement_raw", disclosure_numbers
        ),
    }
    deleted_counts = dict(counts)
    if apply and disclosure_numbers:
        deleted_counts = {
            "jquants_financial_metrics": _delete_by_disclosure(
                conn, "jquants_financial_metrics", disclosure_numbers
            ),
            "market_derived_metrics": _delete_market_rows(conn, disclosure_numbers),
            "jquants_statement_raw": _delete_by_disclosure(
                conn, "jquants_statement_raw", disclosure_numbers
            ),
        }
        conn.commit()
    file_counts = (
        _prune_raw_json_records(candidates, storage_root=storage_root, apply=apply)
        if delete_files
        else {}
    )
    output_path = _write_report(
        output_dir=output_dir,
        apply=apply,
        keep_latest=keep_latest,
        quarter_types=selected_quarters,
        candidates=candidates,
        counts=deleted_counts if apply else counts,
        file_counts=file_counts,
    )
    return JQuantsPruneResult(
        apply=apply,
        keep_latest=keep_latest,
        quarter_types=selected_quarters,
        candidate_count=len(candidates),
        deleted_counts=deleted_counts if apply else counts,
        file_counts=file_counts,
        output_path=output_path,
        candidates=candidates,
    )
