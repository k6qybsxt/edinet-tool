from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
import sqlite3
from typing import Any


@dataclass(frozen=True)
class EdinetPruneCandidate:
    doc_id: str
    edinet_code: str
    security_code: str
    company_name: str
    period_end: str
    submit_date: str
    rank: int
    zip_path: str
    xbrl_path: str


@dataclass(frozen=True)
class EdinetPruneResult:
    apply: bool
    keep_latest: int
    candidate_count: int
    deleted_counts: dict[str, int]
    output_path: Path | None
    candidates: list[EdinetPruneCandidate] = field(default_factory=list)


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


def fetch_old_edinet_period_candidates(
    conn: sqlite3.Connection,
    *,
    keep_latest: int = 11,
    form_type: str = "030000",
) -> list[EdinetPruneCandidate]:
    rows = conn.execute(
        """
        WITH ranked AS (
          SELECT
            f.doc_id,
            f.edinet_code,
            COALESCE(im.security_code, f.security_code, '') AS security_code,
            COALESCE(im.company_name, '') AS company_name,
            COALESCE(f.period_end, '') AS period_end,
            COALESCE(f.submit_date, '') AS submit_date,
            COALESCE(f.zip_path, '') AS zip_path,
            COALESCE(f.xbrl_path, '') AS xbrl_path,
            ROW_NUMBER() OVER (
              PARTITION BY f.edinet_code
              ORDER BY f.period_end DESC, COALESCE(f.submit_date, '') DESC, f.doc_id DESC
            ) AS rn
          FROM filings f
          JOIN issuer_master im
            ON im.edinet_code = f.edinet_code
          WHERE f.form_type = ?
            AND COALESCE(im.is_listed, 0) = 1
            AND COALESCE(im.exchange, '') = 'TSE'
        )
        SELECT *
        FROM ranked
        WHERE rn > ?
        ORDER BY edinet_code, rn
        """,
        (form_type, keep_latest),
    ).fetchall()
    return [
        EdinetPruneCandidate(
            doc_id=str(row["doc_id"]),
            edinet_code=str(row["edinet_code"]),
            security_code=str(row["security_code"]),
            company_name=str(row["company_name"]),
            period_end=str(row["period_end"]),
            submit_date=str(row["submit_date"]),
            rank=int(row["rn"]),
            zip_path=str(row["zip_path"]),
            xbrl_path=str(row["xbrl_path"]),
        )
        for row in rows
    ]


def _count_by_doc_id(conn: sqlite3.Connection, table_name: str, doc_ids: list[str]) -> int:
    if not doc_ids or "doc_id" not in _table_columns(conn, table_name):
        return 0
    total = 0
    for chunk in _chunked(doc_ids):
        placeholders = ",".join("?" for _ in chunk)
        total += int(
            conn.execute(
                f"SELECT COUNT(*) FROM {table_name} WHERE doc_id IN ({placeholders})",
                chunk,
            ).fetchone()[0]
            or 0
        )
    return total


def _count_market_rows(conn: sqlite3.Connection, doc_ids: list[str]) -> int:
    columns = _table_columns(conn, "market_derived_metrics")
    if not doc_ids or not {"source_type", "source_id"} <= columns:
        return 0
    total = 0
    for chunk in _chunked(doc_ids):
        placeholders = ",".join("?" for _ in chunk)
        total += int(
            conn.execute(
                f"""
                SELECT COUNT(*)
                FROM market_derived_metrics
                WHERE source_type = 'edinet'
                  AND source_id IN ({placeholders})
                """,
                chunk,
            ).fetchone()[0]
            or 0
        )
    return total


def _delete_by_doc_id(conn: sqlite3.Connection, table_name: str, doc_ids: list[str]) -> int:
    if not doc_ids or "doc_id" not in _table_columns(conn, table_name):
        return 0
    deleted = 0
    for chunk in _chunked(doc_ids):
        placeholders = ",".join("?" for _ in chunk)
        cursor = conn.execute(
            f"DELETE FROM {table_name} WHERE doc_id IN ({placeholders})",
            chunk,
        )
        deleted += int(cursor.rowcount or 0)
    return deleted


def _delete_market_rows(conn: sqlite3.Connection, doc_ids: list[str]) -> int:
    columns = _table_columns(conn, "market_derived_metrics")
    if not doc_ids or not {"source_type", "source_id"} <= columns:
        return 0
    deleted = 0
    for chunk in _chunked(doc_ids):
        placeholders = ",".join("?" for _ in chunk)
        cursor = conn.execute(
            f"""
            DELETE FROM market_derived_metrics
            WHERE source_type = 'edinet'
              AND source_id IN ({placeholders})
            """,
            chunk,
        )
        deleted += int(cursor.rowcount or 0)
    return deleted


def _write_report(
    *,
    output_dir: str | Path | None,
    apply: bool,
    keep_latest: int,
    candidates: list[EdinetPruneCandidate],
    counts: dict[str, int],
) -> Path | None:
    if output_dir is None:
        return None
    directory = Path(output_dir)
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"edinet_period_prune_{datetime.now():%Y%m%d_%H%M%S}.txt"
    lines: list[str] = [
        f"generated_at: {datetime.now().isoformat(timespec='seconds')}",
        f"apply: {1 if apply else 0}",
        f"keep_latest: {keep_latest}",
        f"candidate_filings: {len(candidates)}",
        "",
        "[counts]",
    ]
    lines.extend(f"{key}: {value}" for key, value in counts.items())
    lines.extend(["", "[candidate_filings]"])
    for candidate in candidates[:200]:
        lines.append(
            " | ".join(
                [
                    candidate.edinet_code,
                    candidate.security_code,
                    candidate.company_name,
                    f"rank={candidate.rank}",
                    candidate.period_end,
                    candidate.doc_id,
                ]
            )
        )
    if len(candidates) > 200:
        lines.append(f"... omitted {len(candidates) - 200} candidates")
    lines.extend(["", "[candidate_paths]"])
    for candidate in candidates[:200]:
        if candidate.zip_path:
            lines.append(f"zip | {candidate.doc_id} | {candidate.zip_path}")
        if candidate.xbrl_path:
            lines.append(f"xbrl | {candidate.doc_id} | {candidate.xbrl_path}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8-sig")
    return path


def prune_old_edinet_period_data(
    conn: sqlite3.Connection,
    *,
    keep_latest: int = 11,
    apply: bool = False,
    output_dir: str | Path | None = None,
) -> EdinetPruneResult:
    candidates = fetch_old_edinet_period_candidates(conn, keep_latest=keep_latest)
    doc_ids = [candidate.doc_id for candidate in candidates]
    counts = {
        "raw_facts": _count_by_doc_id(conn, "raw_facts", doc_ids),
        "normalized_metrics": _count_by_doc_id(conn, "normalized_metrics", doc_ids),
        "derived_metrics": _count_by_doc_id(conn, "derived_metrics", doc_ids),
        "market_derived_metrics": _count_market_rows(conn, doc_ids),
        "filings": len(doc_ids),
    }
    deleted_counts = dict(counts)
    if apply and doc_ids:
        deleted_counts = {
            "raw_facts": _delete_by_doc_id(conn, "raw_facts", doc_ids),
            "normalized_metrics": _delete_by_doc_id(conn, "normalized_metrics", doc_ids),
            "derived_metrics": _delete_by_doc_id(conn, "derived_metrics", doc_ids),
            "market_derived_metrics": _delete_market_rows(conn, doc_ids),
            "filings": _delete_by_doc_id(conn, "filings", doc_ids),
        }
        conn.commit()
    output_path = _write_report(
        output_dir=output_dir,
        apply=apply,
        keep_latest=keep_latest,
        candidates=candidates,
        counts=deleted_counts if apply else counts,
    )
    return EdinetPruneResult(
        apply=apply,
        keep_latest=keep_latest,
        candidate_count=len(candidates),
        deleted_counts=deleted_counts if apply else counts,
        output_path=output_path,
        candidates=candidates,
    )
