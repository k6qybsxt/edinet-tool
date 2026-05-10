from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
import json
from pathlib import Path
import sqlite3
from typing import Any

from edinet_monitor.config.settings import MANIFEST_ROOT, XBRL_ROOT, ZIP_ROOT
from edinet_monitor.domain.issuer_flags import TENBAGGER_LEARNING_SECURITY_CODES


@dataclass(frozen=True)
class EdinetPruneCandidate:
    doc_id: str
    edinet_code: str
    security_code: str
    company_name: str
    form_type: str
    period_end: str
    submit_date: str
    rank: int
    zip_path: str
    xbrl_path: str


@dataclass(frozen=True)
class EdinetPruneResult:
    apply: bool
    keep_latest: int
    form_types: tuple[str, ...]
    candidate_count: int
    deleted_counts: dict[str, int]
    file_counts: dict[str, int]
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


def _is_under_root(path: Path, roots: tuple[Path, ...]) -> bool:
    if not roots:
        return True
    try:
        resolved = path.resolve(strict=False)
    except OSError:
        return False
    for root in roots:
        try:
            resolved.relative_to(root.resolve(strict=False))
            return True
        except (OSError, ValueError):
            continue
    return False


def fetch_old_edinet_period_candidates(
    conn: sqlite3.Connection,
    *,
    keep_latest: int = 11,
    form_type: str = "030000",
    form_types: tuple[str, ...] | None = None,
    exclude_security_codes: set[str] | frozenset[str] | None = TENBAGGER_LEARNING_SECURITY_CODES,
) -> list[EdinetPruneCandidate]:
    selected_form_types = form_types or (form_type,)
    selected_form_types = tuple(str(item).strip() for item in selected_form_types if str(item).strip())
    if not selected_form_types:
        selected_form_types = ("030000",)
    placeholders = ",".join("?" for _ in selected_form_types)
    if exclude_security_codes is None:
        exclude_security_codes = TENBAGGER_LEARNING_SECURITY_CODES
    excluded_codes = tuple(sorted(str(code).strip() for code in exclude_security_codes if str(code).strip()))
    exclude_clause = ""
    if excluded_codes:
        exclude_placeholders = ",".join("?" for _ in excluded_codes)
        exclude_clause = (
            "AND substr(COALESCE(im.security_code, f.security_code, ''), 1, 4) "
            f"NOT IN ({exclude_placeholders})"
        )
    rows = conn.execute(
        f"""
        WITH ranked AS (
          SELECT
            f.doc_id,
            f.edinet_code,
            COALESCE(im.security_code, f.security_code, '') AS security_code,
            COALESCE(im.company_name, '') AS company_name,
            COALESCE(f.form_type, '') AS form_type,
            COALESCE(f.period_end, '') AS period_end,
            COALESCE(f.submit_date, '') AS submit_date,
            COALESCE(f.zip_path, '') AS zip_path,
            COALESCE(f.xbrl_path, '') AS xbrl_path,
            ROW_NUMBER() OVER (
              PARTITION BY f.edinet_code, f.form_type
              ORDER BY f.period_end DESC, COALESCE(f.submit_date, '') DESC, f.doc_id DESC
            ) AS rn
          FROM filings f
          JOIN issuer_master im
            ON im.edinet_code = f.edinet_code
          WHERE f.form_type IN ({placeholders})
            AND COALESCE(im.is_listed, 0) = 1
            AND COALESCE(im.exchange, '') = 'TSE'
            {exclude_clause}
        )
        SELECT *
        FROM ranked
        WHERE rn > ?
        ORDER BY edinet_code, rn
        """,
        (*selected_form_types, *excluded_codes, keep_latest),
    ).fetchall()
    return [
        EdinetPruneCandidate(
            doc_id=str(row["doc_id"]),
            edinet_code=str(row["edinet_code"]),
            security_code=str(row["security_code"]),
            company_name=str(row["company_name"]),
            form_type=str(row["form_type"]),
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


def _delete_file_path(
    path_text: str,
    *,
    allowed_roots: tuple[Path, ...],
    apply: bool,
) -> tuple[str, Path | None]:
    path_text = str(path_text or "").strip()
    if not path_text:
        return "empty", None
    path = Path(path_text)
    if not _is_under_root(path, allowed_roots):
        return "skipped_outside_root", path
    if not path.exists():
        return "missing", path
    if not path.is_file():
        return "skipped_not_file", path
    if apply:
        path.unlink()
    return "deleted" if apply else "candidate", path


def _remove_empty_parent_dir(path: Path | None, *, root: Path, apply: bool) -> int:
    if path is None:
        return 0
    parent = path.parent
    if not _is_under_root(parent, (root,)):
        return 0
    if not parent.exists() or not parent.is_dir():
        return 0
    try:
        next(parent.iterdir())
        return 0
    except StopIteration:
        if apply:
            parent.rmdir()
        return 1


def _prune_manifest_rows(
    doc_ids: set[str],
    *,
    manifest_root: Path,
    apply: bool,
) -> dict[str, int]:
    counts = {
        "manifest_rows": 0,
        "manifest_files_updated": 0,
        "manifest_files_deleted": 0,
        "manifest_files_scanned": 0,
    }
    root = Path(manifest_root)
    if not doc_ids or not root.exists():
        return counts

    for path in sorted(root.glob("*.jsonl"), key=lambda item: item.name):
        if not path.is_file() or not _is_under_root(path, (root,)):
            continue
        counts["manifest_files_scanned"] += 1
        kept_lines: list[str] = []
        removed = 0
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                kept_lines.append(line)
                continue
            if str(row.get("doc_id") or "") in doc_ids:
                removed += 1
                continue
            kept_lines.append(line)
        if removed == 0:
            continue
        counts["manifest_rows"] += removed
        if kept_lines:
            counts["manifest_files_updated"] += 1
            if apply:
                path.write_text("\n".join(kept_lines) + "\n", encoding="utf-8")
        else:
            counts["manifest_files_deleted"] += 1
            if apply:
                path.unlink(missing_ok=True)
    return counts


def _delete_candidate_files(
    candidates: list[EdinetPruneCandidate],
    *,
    apply: bool,
    zip_root: Path,
    xbrl_root: Path,
    manifest_root: Path,
) -> dict[str, int]:
    counts = {
        "zip_files": 0,
        "xbrl_files": 0,
        "missing_files": 0,
        "skipped_files": 0,
        "empty_dirs": 0,
    }
    for candidate in candidates:
        status, path = _delete_file_path(
            candidate.zip_path,
            allowed_roots=(Path(zip_root),),
            apply=apply,
        )
        if status in {"candidate", "deleted"}:
            counts["zip_files"] += 1
            counts["empty_dirs"] += _remove_empty_parent_dir(path, root=Path(zip_root), apply=apply)
        elif status == "missing":
            counts["missing_files"] += 1
        elif status != "empty":
            counts["skipped_files"] += 1

        status, path = _delete_file_path(
            candidate.xbrl_path,
            allowed_roots=(Path(xbrl_root),),
            apply=apply,
        )
        if status in {"candidate", "deleted"}:
            counts["xbrl_files"] += 1
            counts["empty_dirs"] += _remove_empty_parent_dir(path, root=Path(xbrl_root), apply=apply)
        elif status == "missing":
            counts["missing_files"] += 1
        elif status != "empty":
            counts["skipped_files"] += 1

    manifest_counts = _prune_manifest_rows(
        {candidate.doc_id for candidate in candidates},
        manifest_root=Path(manifest_root),
        apply=apply,
    )
    counts.update(manifest_counts)
    return counts


def _write_report(
    *,
    output_dir: str | Path | None,
    apply: bool,
    keep_latest: int,
    form_types: tuple[str, ...],
    candidates: list[EdinetPruneCandidate],
    counts: dict[str, int],
    file_counts: dict[str, int],
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
        f"form_types: {','.join(form_types)}",
        f"candidate_filings: {len(candidates)}",
        "",
        "[counts]",
    ]
    lines.extend(f"{key}: {value}" for key, value in counts.items())
    lines.extend(["", "[files]"])
    lines.extend(f"{key}: {value}" for key, value in file_counts.items())
    lines.extend(["", "[candidate_filings]"])
    for candidate in candidates[:200]:
        lines.append(
            " | ".join(
                [
                    candidate.edinet_code,
                    candidate.security_code,
                    candidate.company_name,
                    f"form_type={candidate.form_type}",
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
    form_types: tuple[str, ...] = ("030000",),
    exclude_security_codes: set[str] | frozenset[str] | None = TENBAGGER_LEARNING_SECURITY_CODES,
    delete_files: bool = False,
    apply: bool = False,
    output_dir: str | Path | None = None,
    zip_root: str | Path = ZIP_ROOT,
    xbrl_root: str | Path = XBRL_ROOT,
    manifest_root: str | Path = MANIFEST_ROOT,
) -> EdinetPruneResult:
    if exclude_security_codes is None:
        exclude_security_codes = TENBAGGER_LEARNING_SECURITY_CODES
    selected_form_types = tuple(str(item).strip() for item in form_types if str(item).strip())
    if not selected_form_types:
        selected_form_types = ("030000",)
    candidates = fetch_old_edinet_period_candidates(
        conn,
        keep_latest=keep_latest,
        form_types=selected_form_types,
        exclude_security_codes=exclude_security_codes,
    )
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
    file_counts = (
        _delete_candidate_files(
            candidates,
            apply=apply,
            zip_root=Path(zip_root),
            xbrl_root=Path(xbrl_root),
            manifest_root=Path(manifest_root),
        )
        if delete_files
        else {}
    )
    output_path = _write_report(
        output_dir=output_dir,
        apply=apply,
        keep_latest=keep_latest,
        form_types=selected_form_types,
        candidates=candidates,
        counts=deleted_counts if apply else counts,
        file_counts=file_counts,
    )
    return EdinetPruneResult(
        apply=apply,
        keep_latest=keep_latest,
        form_types=selected_form_types,
        candidate_count=len(candidates),
        deleted_counts=deleted_counts if apply else counts,
        file_counts=file_counts,
        output_path=output_path,
        candidates=candidates,
    )
