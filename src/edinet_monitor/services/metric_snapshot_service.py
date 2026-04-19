from __future__ import annotations

import csv
import hashlib
import json
import math
import sqlite3
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

from edinet_monitor.config.settings import DB_PATH


SNAPSHOT_FORMAT_VERSION = "metric-snapshot-v1"

COMMON_COLUMNS = [
    "source",
    "doc_id",
    "edinet_code",
    "security_code",
    "metric_key",
    "metric_base",
    "metric_group",
    "fiscal_year",
    "period_end",
    "period_scope",
    "period_offset",
    "consolidation",
    "accounting_standard",
    "document_display_unit",
    "value_num",
    "value_unit",
    "calc_status",
    "formula_name",
    "source_tag",
    "rule_version",
]

METRIC_TSV_COLUMNS = ["row_key", *COMMON_COLUMNS, "value_hash", "full_hash"]
ROW_HASH_COLUMNS = [
    "row_key",
    "source",
    "doc_id",
    "metric_key",
    "period_end",
    "period_scope",
    "period_offset",
    "consolidation",
    "value_num",
    "calc_status",
    "source_tag",
    "value_hash",
    "full_hash",
]
COUNT_COLUMNS = [
    "source",
    "metric_group",
    "metric_key",
    "calc_status",
    "row_count",
    "non_null_value_count",
]


@dataclass(frozen=True)
class SnapshotExportResult:
    snapshot_dir: Path
    manifest_path: Path
    normalized_rows: int
    derived_rows: int


@dataclass(frozen=True)
class SnapshotCompareResult:
    comparison_dir: Path
    added_count: int
    removed_count: int
    value_changed_count: int
    full_changed_same_value_count: int


def _timestamp() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _connect_readonly(db_path: Path) -> sqlite3.Connection:
    uri = f"file:{db_path}?mode=ro"
    conn = sqlite3.connect(uri, uri=True, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only = ON")
    conn.execute("PRAGMA busy_timeout = 30000")
    return conn


def _format_cell(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        if not math.isfinite(value):
            return str(value)
        return format(value, ".17g")
    return str(value)


def _hash_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _file_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _row_key_base(row: dict[str, str]) -> str:
    parts = [
        row.get("source", ""),
        row.get("doc_id", ""),
        row.get("metric_key", ""),
        row.get("fiscal_year", ""),
        row.get("period_end", ""),
        row.get("period_scope", ""),
        row.get("period_offset", ""),
        row.get("consolidation", ""),
    ]
    return "|".join(parts)


def _value_hash(row: dict[str, str]) -> str:
    return _hash_text("|".join([row.get("source", ""), row.get("metric_key", ""), row.get("value_num", "")]))


def _full_hash(row: dict[str, str]) -> str:
    return _hash_text("|".join(row.get(column, "") for column in COMMON_COLUMNS))


def _normalized_rows(conn: sqlite3.Connection) -> Iterable[dict[str, str]]:
    rows = conn.execute(
        """
        SELECT
            'normalized_metrics' AS source,
            doc_id,
            edinet_code,
            security_code,
            metric_key,
            '' AS metric_base,
            '' AS metric_group,
            fiscal_year,
            period_end,
            '' AS period_scope,
            '' AS period_offset,
            consolidation,
            '' AS accounting_standard,
            '' AS document_display_unit,
            value_num,
            '' AS value_unit,
            '' AS calc_status,
            '' AS formula_name,
            source_tag,
            rule_version
        FROM normalized_metrics
        ORDER BY
            doc_id,
            metric_key,
            COALESCE(fiscal_year, -1),
            COALESCE(period_end, ''),
            COALESCE(consolidation, ''),
            COALESCE(source_tag, ''),
            COALESCE(rule_version, ''),
            id
        """
    )
    for row in rows:
        yield {column: _format_cell(row[column]) for column in COMMON_COLUMNS}


def _derived_rows(conn: sqlite3.Connection) -> Iterable[dict[str, str]]:
    rows = conn.execute(
        """
        SELECT
            'derived_metrics' AS source,
            doc_id,
            edinet_code,
            security_code,
            metric_key,
            metric_base,
            metric_group,
            fiscal_year,
            period_end,
            period_scope,
            period_offset,
            consolidation,
            accounting_standard,
            document_display_unit,
            value_num,
            value_unit,
            calc_status,
            formula_name,
            '' AS source_tag,
            rule_version
        FROM derived_metrics
        ORDER BY
            doc_id,
            metric_key,
            COALESCE(fiscal_year, -1),
            COALESCE(period_end, ''),
            COALESCE(period_scope, ''),
            COALESCE(period_offset, -1),
            COALESCE(consolidation, ''),
            COALESCE(calc_status, ''),
            COALESCE(formula_name, ''),
            COALESCE(rule_version, ''),
            id
        """
    )
    for row in rows:
        yield {column: _format_cell(row[column]) for column in COMMON_COLUMNS}


def _write_metric_file(
    *,
    path: Path,
    rows: Iterable[dict[str, str]],
    hash_writer: csv.DictWriter,
    count_counter: Counter[tuple[str, str, str, str]],
) -> int:
    duplicate_counts: defaultdict[str, int] = defaultdict(int)
    row_count = 0

    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=METRIC_TSV_COLUMNS,
            delimiter="\t",
            lineterminator="\n",
            extrasaction="ignore",
        )
        writer.writeheader()

        for base_row in rows:
            row = dict(base_row)
            base_key = _row_key_base(row)
            duplicate_counts[base_key] += 1
            row["row_key"] = f"{base_key}|#{duplicate_counts[base_key]:04d}"
            row["value_hash"] = _value_hash(row)
            row["full_hash"] = _full_hash(row)
            writer.writerow(row)
            hash_writer.writerow({column: row.get(column, "") for column in ROW_HASH_COLUMNS})

            count_key = (
                row.get("source", ""),
                row.get("metric_group", ""),
                row.get("metric_key", ""),
                row.get("calc_status", ""),
            )
            count_counter[count_key] += 1
            if row.get("value_num", ""):
                count_counter[(count_key[0], count_key[1], count_key[2], f"{count_key[3]}__non_null")] += 1

            row_count += 1

    return row_count


def _write_counts(path: Path, count_counter: Counter[tuple[str, str, str, str]]) -> int:
    count_rows: list[dict[str, str]] = []
    for source, metric_group, metric_key, calc_status in sorted(count_counter):
        if calc_status.endswith("__non_null"):
            continue
        non_null_key = (source, metric_group, metric_key, f"{calc_status}__non_null")
        count_rows.append(
            {
                "source": source,
                "metric_group": metric_group,
                "metric_key": metric_key,
                "calc_status": calc_status,
                "row_count": str(count_counter[(source, metric_group, metric_key, calc_status)]),
                "non_null_value_count": str(count_counter.get(non_null_key, 0)),
            }
        )

    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=COUNT_COLUMNS,
            delimiter="\t",
            lineterminator="\n",
        )
        writer.writeheader()
        writer.writerows(count_rows)

    return len(count_rows)


def export_metric_snapshot(
    *,
    label: str,
    output_dir: Path,
    db_path: Path = DB_PATH,
    timestamp: str | None = None,
) -> SnapshotExportResult:
    safe_label = "".join(ch if ch.isalnum() or ch in {"_", "-"} else "_" for ch in label.strip())
    if not safe_label:
        safe_label = "metric_snapshot"

    snapshot_timestamp = timestamp or _timestamp()
    snapshot_dir = output_dir / f"{safe_label}_{snapshot_timestamp}"
    snapshot_dir.mkdir(parents=True, exist_ok=True)

    normalized_path = snapshot_dir / "normalized_metrics.tsv"
    derived_path = snapshot_dir / "derived_metrics.tsv"
    row_hashes_path = snapshot_dir / "row_hashes.tsv"
    counts_path = snapshot_dir / "metric_counts.tsv"
    manifest_path = snapshot_dir / "snapshot_manifest.json"

    count_counter: Counter[tuple[str, str, str, str]] = Counter()
    with row_hashes_path.open("w", encoding="utf-8", newline="") as hash_handle:
        hash_writer = csv.DictWriter(
            hash_handle,
            fieldnames=ROW_HASH_COLUMNS,
            delimiter="\t",
            lineterminator="\n",
        )
        hash_writer.writeheader()

        conn = _connect_readonly(db_path)
        try:
            conn.execute("BEGIN")
            normalized_rows = _write_metric_file(
                path=normalized_path,
                rows=_normalized_rows(conn),
                hash_writer=hash_writer,
                count_counter=count_counter,
            )
            derived_rows = _write_metric_file(
                path=derived_path,
                rows=_derived_rows(conn),
                hash_writer=hash_writer,
                count_counter=count_counter,
            )
        finally:
            conn.close()

    count_rows = _write_counts(counts_path, count_counter)

    files = {}
    for name, path in {
        "normalized_metrics.tsv": normalized_path,
        "derived_metrics.tsv": derived_path,
        "metric_counts.tsv": counts_path,
        "row_hashes.tsv": row_hashes_path,
    }.items():
        files[name] = {
            "sha256": _file_sha256(path),
            "size_bytes": path.stat().st_size,
        }

    manifest = {
        "snapshot_format_version": SNAPSHOT_FORMAT_VERSION,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "label": safe_label,
        "db_path": str(db_path),
        "snapshot_dir": str(snapshot_dir),
        "row_counts": {
            "normalized_metrics": normalized_rows,
            "derived_metrics": derived_rows,
            "metric_counts": count_rows,
            "row_hashes": normalized_rows + derived_rows,
        },
        "columns": {
            "metric_files": METRIC_TSV_COLUMNS,
            "row_hashes": ROW_HASH_COLUMNS,
            "metric_counts": COUNT_COLUMNS,
        },
        "files": files,
    }
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )

    return SnapshotExportResult(
        snapshot_dir=snapshot_dir,
        manifest_path=manifest_path,
        normalized_rows=normalized_rows,
        derived_rows=derived_rows,
    )


def _write_rows(path: Path, rows: list[dict[str, str]], columns: list[str]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=columns,
            delimiter="\t",
            lineterminator="\n",
            extrasaction="ignore",
        )
        writer.writeheader()
        writer.writerows(rows)


def _create_hash_table(conn: sqlite3.Connection, table_name: str) -> None:
    conn.execute(
        f"""
        CREATE TABLE {table_name} (
            row_key TEXT PRIMARY KEY,
            source TEXT,
            doc_id TEXT,
            metric_key TEXT,
            period_end TEXT,
            period_scope TEXT,
            period_offset TEXT,
            consolidation TEXT,
            value_num TEXT,
            calc_status TEXT,
            source_tag TEXT,
            value_hash TEXT,
            full_hash TEXT
        )
        """
    )


def _import_row_hashes(conn: sqlite3.Connection, table_name: str, path: Path) -> int:
    insert_sql = f"""
    INSERT OR REPLACE INTO {table_name} (
        row_key,
        source,
        doc_id,
        metric_key,
        period_end,
        period_scope,
        period_offset,
        consolidation,
        value_num,
        calc_status,
        source_tag,
        value_hash,
        full_hash
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    batch: list[tuple[str, ...]] = []
    total = 0
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        for row in reader:
            batch.append(tuple(str(row.get(column) or "") for column in ROW_HASH_COLUMNS))
            if len(batch) >= 10000:
                conn.executemany(insert_sql, batch)
                total += len(batch)
                batch.clear()
        if batch:
            conn.executemany(insert_sql, batch)
            total += len(batch)
    conn.commit()
    return total


def _write_query_rows(
    *,
    conn: sqlite3.Connection,
    path: Path,
    query: str,
    columns: list[str],
) -> int:
    count = 0
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=columns,
            delimiter="\t",
            lineterminator="\n",
            extrasaction="ignore",
        )
        writer.writeheader()
        for row in conn.execute(query):
            writer.writerow({column: str(row[column] or "") for column in columns})
            count += 1
    return count


def compare_metric_snapshots(
    *,
    before_dir: Path,
    after_dir: Path,
    output_dir: Path,
    timestamp: str | None = None,
) -> SnapshotCompareResult:
    comparison_timestamp = timestamp or _timestamp()
    comparison_dir = output_dir / f"metric_snapshot_compare_{comparison_timestamp}"
    comparison_dir.mkdir(parents=True, exist_ok=True)

    change_columns = [
        "row_key",
        "source",
        "doc_id",
        "metric_key",
        "period_end",
        "period_scope",
        "period_offset",
        "consolidation",
        "before_value_num",
        "after_value_num",
        "before_calc_status",
        "after_calc_status",
        "before_source_tag",
        "after_source_tag",
        "before_full_hash",
        "after_full_hash",
    ]

    work_db_path = comparison_dir / "_compare_work.sqlite"
    conn = sqlite3.connect(work_db_path)
    try:
        conn.row_factory = sqlite3.Row
        _create_hash_table(conn, "before_hashes")
        _create_hash_table(conn, "after_hashes")
        before_row_count = _import_row_hashes(conn, "before_hashes", before_dir / "row_hashes.tsv")
        after_row_count = _import_row_hashes(conn, "after_hashes", after_dir / "row_hashes.tsv")

        added_count = _write_query_rows(
            conn=conn,
            path=comparison_dir / "added_rows.tsv",
            columns=ROW_HASH_COLUMNS,
            query="""
                SELECT a.*
                FROM after_hashes a
                LEFT JOIN before_hashes b ON b.row_key = a.row_key
                WHERE b.row_key IS NULL
                ORDER BY a.row_key
            """,
        )
        removed_count = _write_query_rows(
            conn=conn,
            path=comparison_dir / "removed_rows.tsv",
            columns=ROW_HASH_COLUMNS,
            query="""
                SELECT b.*
                FROM before_hashes b
                LEFT JOIN after_hashes a ON a.row_key = b.row_key
                WHERE a.row_key IS NULL
                ORDER BY b.row_key
            """,
        )
        value_changed_count = _write_query_rows(
            conn=conn,
            path=comparison_dir / "value_changes.tsv",
            columns=change_columns,
            query="""
                SELECT
                    a.row_key AS row_key,
                    a.source AS source,
                    a.doc_id AS doc_id,
                    a.metric_key AS metric_key,
                    a.period_end AS period_end,
                    a.period_scope AS period_scope,
                    a.period_offset AS period_offset,
                    a.consolidation AS consolidation,
                    b.value_num AS before_value_num,
                    a.value_num AS after_value_num,
                    b.calc_status AS before_calc_status,
                    a.calc_status AS after_calc_status,
                    b.source_tag AS before_source_tag,
                    a.source_tag AS after_source_tag,
                    b.full_hash AS before_full_hash,
                    a.full_hash AS after_full_hash
                FROM after_hashes a
                JOIN before_hashes b ON b.row_key = a.row_key
                WHERE b.value_hash <> a.value_hash
                ORDER BY a.row_key
            """,
        )
        full_changed_same_value_count = _write_query_rows(
            conn=conn,
            path=comparison_dir / "full_changes_same_value.tsv",
            columns=change_columns,
            query="""
                SELECT
                    a.row_key AS row_key,
                    a.source AS source,
                    a.doc_id AS doc_id,
                    a.metric_key AS metric_key,
                    a.period_end AS period_end,
                    a.period_scope AS period_scope,
                    a.period_offset AS period_offset,
                    a.consolidation AS consolidation,
                    b.value_num AS before_value_num,
                    a.value_num AS after_value_num,
                    b.calc_status AS before_calc_status,
                    a.calc_status AS after_calc_status,
                    b.source_tag AS before_source_tag,
                    a.source_tag AS after_source_tag,
                    b.full_hash AS before_full_hash,
                    a.full_hash AS after_full_hash
                FROM after_hashes a
                JOIN before_hashes b ON b.row_key = a.row_key
                WHERE b.value_hash = a.value_hash
                  AND b.full_hash <> a.full_hash
                ORDER BY a.row_key
            """,
        )
    finally:
        conn.close()
    try:
        work_db_path.unlink()
    except FileNotFoundError:
        pass

    summary = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "before_dir": str(before_dir),
        "after_dir": str(after_dir),
        "before_row_count": before_row_count,
        "after_row_count": after_row_count,
        "added_count": added_count,
        "removed_count": removed_count,
        "value_changed_count": value_changed_count,
        "full_changed_same_value_count": full_changed_same_value_count,
    }
    (comparison_dir / "comparison_summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    (comparison_dir / "comparison_report.txt").write_text(
        "\n".join(
            [
                f"generated_at: {summary['generated_at']}",
                f"before_dir: {before_dir}",
                f"after_dir: {after_dir}",
                f"before_row_count: {before_row_count}",
                f"after_row_count: {after_row_count}",
                f"added_count: {added_count}",
                f"removed_count: {removed_count}",
                f"value_changed_count: {value_changed_count}",
                f"full_changed_same_value_count: {full_changed_same_value_count}",
            ]
        )
        + "\n",
        encoding="utf-8-sig",
    )

    return SnapshotCompareResult(
        comparison_dir=comparison_dir,
        added_count=added_count,
        removed_count=removed_count,
        value_changed_count=value_changed_count,
        full_changed_same_value_count=full_changed_same_value_count,
    )
