from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
import csv
import json
from pathlib import Path
import sqlite3
from typing import Any, Iterator

from edinet_monitor.config.settings import OPERATION_LOG_ROOT
from edinet_monitor.services.segment_metric_service import (
    SEGMENT_FORM_CODES,
    TEXTBLOCK_METRIC_INFO_BY_BASE,
    SegmentMetricRow,
    build_segment_metric_rows,
    _is_segment_note_textblock_tag,
    _segment_note_textblock_source_priority,
    _segment_note_textblock_entries,
    _semantic_segment_name_key,
)


DEFAULT_SEGMENT_NOTE_SEMANTICS_AUDIT_OUTPUT_DIR = OPERATION_LOG_ROOT / "segment_note_semantics"
AUDIT_DOC_ID_CHUNK_SIZE = 250


@dataclass(frozen=True)
class SegmentNoteSemanticsAuditRow:
    doc_id: str
    security_code: str
    form_type: str
    period_end: str
    source_tags: str
    existing_row_count: int
    proposed_row_count: int
    selected_candidate_count: int
    excluded_candidate_count: int
    review_row_count: int
    excel_suppressed_segment_profit_count: int
    status: str


@dataclass(frozen=True)
class SegmentNoteSemanticsAuditResult:
    rows: list[SegmentNoteSemanticsAuditRow]
    rebuild_doc_ids: tuple[str, ...]
    summary: dict[str, int]


@dataclass(frozen=True)
class SegmentNoteSemanticsAuditPaths:
    run_dir: Path
    rebuild_doc_ids_path: Path
    rows_json_path: Path
    rows_tsv_path: Path
    summary_json_path: Path


def _chunked(values: list[str], size: int = AUDIT_DOC_ID_CHUNK_SIZE) -> list[list[str]]:
    return [values[index : index + size] for index in range(0, len(values), size)]


def _segment_note_tag_names(conn: sqlite3.Connection) -> tuple[str, ...]:
    # Only the tag-name index is read here. TextBlock bodies are parsed after the
    # candidate document set has been narrowed.
    rows = conn.execute(
        """
        SELECT DISTINCT tag_name
        FROM raw_facts
        WHERE tag_name IN (
          'InformationAboutGeographicalAreasIFRSTextBlock',
          'InformationAboutGeographicalAreasTextBlock'
        )
           OR tag_name GLOB 'NotesSegmentInformation*TextBlock'
           OR tag_name GLOB 'Footnotes*SegmentInformation*TextBlock'
           OR tag_name GLOB 'InformationAboutSegment*TextBlock'
           OR tag_name GLOB 'SegmentInformation*TextBlock'
        ORDER BY tag_name
        """
    ).fetchall()
    return tuple(
        str(row[0])
        for row in rows
        if _is_segment_note_textblock_tag(str(row[0] or ""))
    )


def _note_raw_rows(
    conn: sqlite3.Connection,
    *,
    source_tags: tuple[str, ...],
    doc_ids: tuple[str, ...],
) -> Iterator[sqlite3.Row]:
    if doc_ids:
        for chunk in _chunked(list(doc_ids)):
            doc_placeholders = ",".join("?" for _ in chunk)
            tag_placeholders = ",".join("?" for _ in source_tags)
            for row in conn.execute(
                    f"""
                    SELECT
                      f.doc_id, f.security_code, f.form_type, f.period_end,
                      rf.rowid AS raw_rowid, rf.tag_name, rf.period_start,
                      rf.period_end AS fact_period_end, rf.value_text
                    FROM filings f
                    LEFT JOIN raw_facts rf
                      ON rf.doc_id = f.doc_id
                     AND rf.tag_name IN ({tag_placeholders})
                    WHERE f.doc_id IN ({doc_placeholders})
                    ORDER BY f.doc_id, rf.tag_name
                    """,
                    [*source_tags, *chunk],
                ):
                yield row
        return
    if not source_tags:
        return
    for chunk in _chunked(list(source_tags)):
        placeholders = ",".join("?" for _ in chunk)
        for row in conn.execute(
                f"""
                SELECT
                  f.doc_id, f.security_code, f.form_type, f.period_end,
                  rf.rowid AS raw_rowid, rf.tag_name, rf.period_start,
                  rf.period_end AS fact_period_end, rf.value_text
                FROM raw_facts rf
                JOIN filings f ON f.doc_id = rf.doc_id
                WHERE rf.tag_name IN ({placeholders})
                  AND f.form_type IN ({','.join('?' for _ in SEGMENT_FORM_CODES)})
                ORDER BY f.doc_id, rf.tag_name
                """,
                [*chunk, *SEGMENT_FORM_CODES],
            ):
            yield row


def _profit_conflict_doc_ids(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute(
        """
        SELECT doc_id
        FROM segment_metrics
        WHERE calc_status = 'ok'
          AND metric_base IN ('OperatingIncome', 'SegmentProfit')
        GROUP BY doc_id, segment_kind,
                 CASE WHEN segment_kind = 'total' THEN 'TOTAL' ELSE member_qname END,
                 period_start, period_end
        HAVING COUNT(DISTINCT metric_base) > 1
        """
    ).fetchall()
    return {str(row[0]) for row in rows}


def _semantic_candidate_doc_ids(
    conn: sqlite3.Connection,
    *,
    source_tags: tuple[str, ...],
) -> set[str]:
    """Return all note-tag documents using only the raw_facts tag-name index.

    A TextBlock tag alone does not distinguish a regional, cluster, reportable
    segment, or asset table.  The audit must inspect each candidate body to make
    that decision, but it never uses a full-text SQL search.
    """
    result = _profit_conflict_doc_ids(conn)
    if source_tags:
        for chunk in _chunked(list(source_tags)):
            placeholders = ",".join("?" for _ in chunk)
            result.update(
                str(row[0])
                for row in conn.execute(
                    f"""
                    SELECT DISTINCT f.doc_id
                    FROM raw_facts rf
                    JOIN filings f ON f.doc_id = rf.doc_id
                    WHERE rf.tag_name IN ({placeholders})
                      AND f.form_type IN ({','.join('?' for _ in SEGMENT_FORM_CODES)})
                    """,
                    [*chunk, *SEGMENT_FORM_CODES],
                ).fetchall()
            )
    return result


def _load_existing_segment_state(
    conn: sqlite3.Connection,
    *,
    doc_ids: list[str],
) -> tuple[
    dict[str, int],
    dict[tuple[str, str, str, str, str, str], set[float | None]],
    dict[tuple[str, str, str, str, str, str], set[str]],
    set[tuple[str, str, str, str, str]],
    dict[str, list[tuple[Any, ...]]],
    dict[str, int],
]:
    """Load only comparison fields; never materialize all segment-metric rows."""
    row_counts = {doc_id: 0 for doc_id in doc_ids}
    direct_values: dict[tuple[str, str, str, str, str, str], set[float | None]] = {}
    direct_metric_keys: dict[tuple[str, str, str, str, str, str], set[str]] = {}
    direct_metric_scopes: set[tuple[str, str, str, str, str]] = set()
    semantic_signatures: dict[str, list[tuple[Any, ...]]] = {}
    operating_groups_by_doc: dict[str, set[tuple[str, str, str, str]]] = {}
    segment_profit_groups_by_doc: dict[str, set[tuple[str, str, str, str]]] = {}

    for chunk in _chunked(doc_ids):
        placeholders = ",".join("?" for _ in chunk)
        for row in conn.execute(
            f"SELECT doc_id, COUNT(*) AS row_count FROM segment_metrics "
            f"WHERE doc_id IN ({placeholders}) GROUP BY doc_id",
            chunk,
        ):
            row_counts[str(row["doc_id"])] = int(row["row_count"] or 0)

        for row in conn.execute(
            f"""
            SELECT
              doc_id, segment_kind, segment_name, member_qname, metric_base,
              metric_key, value_num, calc_status, source_tag, period_start, period_end,
              json_extract(source_detail_json, '$.source') AS source_name
            FROM segment_metrics
            WHERE doc_id IN ({placeholders})
            """,
            chunk,
        ):
            doc_id = str(row["doc_id"] or "")
            source_name = str(row["source_name"] or "")
            key = _semantic_key(
                doc_id=doc_id,
                segment_kind=str(row["segment_kind"] or ""),
                segment_name=str(row["segment_name"] or ""),
                metric_base=str(row["metric_base"] or ""),
                period_start=str(row["period_start"] or ""),
                period_end=str(row["period_end"] or ""),
            )
            value_num = float(row["value_num"]) if row["value_num"] is not None else None
            if source_name in {"geographical_area_textblock", "segment_note_textblock"}:
                semantic_signatures.setdefault(doc_id, []).append(
                    (*key, value_num, str(row["source_tag"] or ""), source_name)
                )
            else:
                # build_segment_metric_rows selects a direct XBRL candidate before
                # a TextBlock candidate even when the direct candidate is review.
                # Treat that selected value as an existing semantic competitor so
                # the audit does not request a rebuild that would be discarded by
                # the same priority rule.
                direct_values.setdefault(key, set()).add(value_num)
                direct_metric_keys.setdefault(key, set()).add(str(row["metric_key"] or ""))
                if str(row["calc_status"] or "") == "ok":
                    direct_metric_scopes.add(
                        (
                            doc_id,
                            str(row["segment_kind"] or ""),
                            str(row["metric_base"] or ""),
                            str(row["period_start"] or ""),
                            str(row["period_end"] or ""),
                        )
                    )

            if str(row["calc_status"] or "") != "ok" or str(row["metric_base"] or "") not in {
                "OperatingIncome",
                "SegmentProfit",
            }:
                continue
            member_qname = "TOTAL" if str(row["segment_kind"] or "") == "total" else str(row["member_qname"] or "")
            group_key = (
                str(row["segment_kind"] or ""),
                member_qname,
                str(row["period_start"] or ""),
                str(row["period_end"] or ""),
            )
            if str(row["metric_base"] or "") == "OperatingIncome":
                operating_groups_by_doc.setdefault(doc_id, set()).add(group_key)
            else:
                segment_profit_groups_by_doc.setdefault(doc_id, set()).add(group_key)

    excel_suppressed_counts = {
        doc_id: len(groups & segment_profit_groups_by_doc.get(doc_id, set()))
        for doc_id, groups in operating_groups_by_doc.items()
    }
    return (
        row_counts,
        direct_values,
        direct_metric_keys,
        direct_metric_scopes,
        semantic_signatures,
        excel_suppressed_counts,
    )


@dataclass(frozen=True)
class _ExpectedSemanticRow:
    doc_id: str
    segment_kind: str
    segment_name: str
    metric_base: str
    value_num: float
    period_start: str
    period_end: str
    source_tag: str
    calc_status: str


def _semantic_key(
    *,
    doc_id: str,
    segment_kind: str,
    segment_name: str,
    metric_base: str,
    period_start: str,
    period_end: str,
) -> tuple[str, str, str, str, str, str]:
    return (
        doc_id,
        segment_kind,
        _semantic_segment_name_key(segment_name),
        metric_base,
        period_start,
        period_end,
    )


def _expected_semantic_signature(row: _ExpectedSemanticRow) -> tuple[Any, ...]:
    return (
        *_semantic_key(
            doc_id=row.doc_id,
            segment_kind=row.segment_kind,
            segment_name=row.segment_name,
            metric_base=row.metric_base,
            period_start=row.period_start,
            period_end=row.period_end,
        ),
        row.value_num,
        row.source_tag,
        "segment_note_textblock",
    )


def _built_semantic_signature(row: SegmentMetricRow) -> tuple[Any, ...]:
    return (
        *_semantic_key(
            doc_id=row.doc_id,
            segment_kind=row.segment_kind,
            segment_name=row.segment_name,
            metric_base=row.metric_base,
            period_start=row.period_start,
            period_end=row.period_end,
        ),
        row.value_num,
        row.source_tag,
        "segment_note_textblock",
    )


def _is_built_textblock_row(row: SegmentMetricRow) -> bool:
    try:
        return json.loads(row.source_detail_json).get("source") == "segment_note_textblock"
    except (TypeError, ValueError, json.JSONDecodeError):
        return False


def _build_exact_segment_note_semantics_audit(
    conn: sqlite3.Connection,
    *,
    doc_ids: tuple[str, ...],
) -> SegmentNoteSemanticsAuditResult:
    if not doc_ids:
        raise ValueError("exact segment-note audit requires explicit doc_ids.")
    target_doc_ids = sorted(set(doc_ids))
    (
        existing_row_counts,
        _direct_values,
        _direct_metric_keys,
        _direct_metric_scopes,
        existing_semantic_signatures_by_doc,
        excel_suppressed_counts,
    ) = _load_existing_segment_state(conn, doc_ids=target_doc_ids)
    build = build_segment_metric_rows(
        conn,
        doc_ids=target_doc_ids,
        form_codes=list(SEGMENT_FORM_CODES),
    )
    proposed_by_doc: dict[str, list[SegmentMetricRow]] = {doc_id: [] for doc_id in target_doc_ids}
    for row in build.rows:
        if _is_built_textblock_row(row):
            proposed_by_doc.setdefault(row.doc_id, []).append(row)
    candidates_by_doc: dict[str, list[Any]] = {doc_id: [] for doc_id in target_doc_ids}
    for candidate in build.candidates:
        candidates_by_doc.setdefault(candidate.doc_id, []).append(candidate)
    filing_by_doc = {
        str(row["doc_id"]): row
        for row in _note_raw_rows(
            conn,
            source_tags=_segment_note_tag_names(conn),
            doc_ids=tuple(target_doc_ids),
        )
        if str(row["doc_id"] or "")
    }
    tags_by_doc: dict[str, set[str]] = {}
    for candidate in build.candidates:
        if _is_segment_note_textblock_tag(candidate.source_tag):
            tags_by_doc.setdefault(candidate.doc_id, set()).add(candidate.source_tag)

    rows: list[SegmentNoteSemanticsAuditRow] = []
    rebuild_doc_ids: list[str] = []
    for doc_id in target_doc_ids:
        proposed_rows = proposed_by_doc.get(doc_id, [])
        existing = sorted(existing_semantic_signatures_by_doc.get(doc_id, []))
        proposed = sorted(_built_semantic_signature(row) for row in proposed_rows)
        status = "match" if existing == proposed else "rebuild"
        if not existing_row_counts.get(doc_id, 0) and not proposed:
            status = "no_metrics"
        if status == "rebuild":
            rebuild_doc_ids.append(doc_id)
        candidates = candidates_by_doc.get(doc_id, [])
        filing = filing_by_doc.get(doc_id)
        rows.append(
            SegmentNoteSemanticsAuditRow(
                doc_id=doc_id,
                security_code=str(filing["security_code"] or "") if filing is not None else "",
                form_type=str(filing["form_type"] or "") if filing is not None else "",
                period_end=str(filing["period_end"] or "") if filing is not None else "",
                source_tags=" | ".join(sorted(tags_by_doc.get(doc_id, set()))),
                existing_row_count=existing_row_counts.get(doc_id, 0),
                proposed_row_count=len(proposed_rows),
                selected_candidate_count=sum(candidate.status == "selected" for candidate in candidates),
                excluded_candidate_count=sum(candidate.status == "excluded" for candidate in candidates),
                review_row_count=sum(row.calc_status == "review" for row in proposed_rows),
                excel_suppressed_segment_profit_count=excel_suppressed_counts.get(doc_id, 0),
                status=status,
            )
        )
    return SegmentNoteSemanticsAuditResult(
        rows=rows,
        rebuild_doc_ids=tuple(rebuild_doc_ids),
        summary={
            "candidate_doc_count": len(target_doc_ids),
            "rebuild_doc_count": len(rebuild_doc_ids),
            "match_doc_count": sum(row.status == "match" for row in rows),
            "review_doc_count": sum(row.review_row_count > 0 for row in rows),
            "excluded_candidate_count": sum(row.excluded_candidate_count for row in rows),
            "excel_suppressed_segment_profit_count": sum(
                row.excel_suppressed_segment_profit_count for row in rows
            ),
        },
    )


def _expected_textblock_rows(
    raw_rows: Iterator[sqlite3.Row],
    *,
    direct_values: dict[tuple[str, str, str, str, str, str], set[float | None]],
    direct_metric_keys: dict[tuple[str, str, str, str, str, str], set[str]],
    direct_metric_scopes: set[tuple[str, str, str, str, str]],
) -> tuple[
    dict[str, list[_ExpectedSemanticRow]],
    dict[str, int],
    dict[str, int],
    dict[str, int],
    dict[str, set[str]],
    dict[str, sqlite3.Row],
]:
    expected_by_doc: dict[str, list[_ExpectedSemanticRow]] = {}
    selected_by_doc: dict[str, int] = {}
    excluded_by_doc: dict[str, int] = {}
    review_by_doc: dict[str, int] = {}
    tags_by_doc: dict[str, set[str]] = {}
    filing_by_doc: dict[str, sqlite3.Row] = {}
    selected_by_candidate: dict[
        tuple[str, str, str, str, str, str], tuple[int, int, _ExpectedSemanticRow]
    ] = {}
    for raw in raw_rows:
        doc_id = str(raw["doc_id"] or "")
        if not doc_id:
            continue
        filing_by_doc.setdefault(doc_id, raw)
        tag_name = str(raw["tag_name"] or "")
        if not _is_segment_note_textblock_tag(tag_name):
            continue
        tags_by_doc.setdefault(doc_id, set()).add(tag_name)
        fact_period_end = str(raw["fact_period_end"] or raw["period_end"] or "")
        entries, rejections = _segment_note_textblock_entries(
            str(raw["value_text"] or ""),
            source_tag=tag_name,
            period_end=fact_period_end,
        )
        excluded_by_doc[doc_id] = excluded_by_doc.get(doc_id, 0) + len(rejections)
        for entry in entries:
            selected_by_doc[doc_id] = selected_by_doc.get(doc_id, 0) + 1
            period_start = str(raw["period_start"] or "")
            period_end = fact_period_end
            key = _semantic_key(
                doc_id=doc_id,
                segment_kind=entry.segment_kind,
                segment_name=entry.segment_name,
                metric_base=entry.metric_base,
                period_start=period_start,
                period_end=period_end,
            )
            # The normal builder keys TextBlock candidates by their exact member
            # name and keeps the first equal-priority source.  Use the same rule
            # so another note tag cannot create a phantom audit difference.
            candidate_key = (
                doc_id,
                entry.segment_kind,
                entry.segment_name,
                entry.metric_base,
                period_start,
                period_end,
            )
            if (
                entry.table_kind == "business"
                and (doc_id, entry.segment_kind, entry.metric_base, period_start, period_end)
                in direct_metric_scopes
            ):
                continue
            competing_values = direct_values.get(key, set())
            text_metric_info = TEXTBLOCK_METRIC_INFO_BY_BASE.get(entry.metric_base)
            text_metric_key = text_metric_info[1] if text_metric_info is not None else entry.metric_key
            if entry.segment_kind == "total" and text_metric_key in direct_metric_keys.get(key, set()):
                # The normal builder keys total rows as TOTAL, so the direct
                # XBRL candidate wins before the TextBlock candidate when both
                # candidates use the same metric key.
                continue
            if entry.value_num in competing_values:
                # The rebuild deliberately keeps the direct XBRL fact and omits a
                # semantically identical TextBlock duplicate.
                continue
            calc_status = "review" if competing_values else "ok"
            expected_row = _ExpectedSemanticRow(
                doc_id=doc_id,
                segment_kind=entry.segment_kind,
                segment_name=entry.segment_name,
                metric_base=entry.metric_base,
                value_num=entry.value_num,
                period_start=period_start,
                period_end=period_end,
                source_tag=tag_name,
                calc_status=calc_status,
            )
            raw_rowid = int(raw["raw_rowid"] or 0)
            previous = selected_by_candidate.get(candidate_key)
            source_priority = _segment_note_textblock_source_priority(tag_name)
            if previous is None or (source_priority, raw_rowid) < (previous[0], previous[1]):
                selected_by_candidate[candidate_key] = (source_priority, raw_rowid, expected_row)

    for _, _, expected_row in selected_by_candidate.values():
        expected_by_doc.setdefault(expected_row.doc_id, []).append(expected_row)
        if expected_row.calc_status == "review":
            review_by_doc[expected_row.doc_id] = review_by_doc.get(expected_row.doc_id, 0) + 1
    return (
        expected_by_doc,
        selected_by_doc,
        excluded_by_doc,
        review_by_doc,
        tags_by_doc,
        filing_by_doc,
    )


def build_segment_note_semantics_audit(
    conn: sqlite3.Connection,
    *,
    doc_ids: tuple[str, ...] = (),
    exact: bool = False,
) -> SegmentNoteSemanticsAuditResult:
    if exact:
        return _build_exact_segment_note_semantics_audit(conn, doc_ids=doc_ids)
    source_tags = _segment_note_tag_names(conn)
    if doc_ids:
        target_doc_ids = set(doc_ids)
    else:
        target_doc_ids = _semantic_candidate_doc_ids(conn, source_tags=source_tags)
    if not target_doc_ids:
        return SegmentNoteSemanticsAuditResult(
            rows=[],
            rebuild_doc_ids=(),
            summary={
                "candidate_doc_count": 0,
                "rebuild_doc_count": 0,
                "match_doc_count": 0,
                "review_doc_count": 0,
                "excluded_candidate_count": 0,
                "excel_suppressed_segment_profit_count": 0,
            },
        )

    target_doc_id_list = sorted(target_doc_ids)
    (
        existing_row_counts,
        direct_values,
        direct_metric_keys,
        direct_metric_scopes,
        existing_semantic_signatures_by_doc,
        excel_suppressed_counts,
    ) = _load_existing_segment_state(conn, doc_ids=target_doc_id_list)
    raw_rows = _note_raw_rows(conn, source_tags=source_tags, doc_ids=tuple(target_doc_id_list))
    (
        expected_by_doc,
        selected_by_doc,
        excluded_by_doc,
        review_by_doc,
        tags_by_doc_id,
        filing_by_doc_id,
    ) = _expected_textblock_rows(
        raw_rows,
        direct_values=direct_values,
        direct_metric_keys=direct_metric_keys,
        direct_metric_scopes=direct_metric_scopes,
    )

    audit_rows: list[SegmentNoteSemanticsAuditRow] = []
    rebuild_doc_ids: list[str] = []
    for doc_id in target_doc_id_list:
        expected_rows = expected_by_doc.get(doc_id, [])
        existing_fingerprints = sorted(existing_semantic_signatures_by_doc.get(doc_id, []))
        expected_fingerprints = sorted(_expected_semantic_signature(row) for row in expected_rows)
        has_output = bool(existing_row_counts.get(doc_id, 0) or expected_rows)
        status = "match" if existing_fingerprints == expected_fingerprints else "rebuild"
        if not has_output:
            status = "no_metrics"
        if status == "rebuild":
            rebuild_doc_ids.append(doc_id)
        filing = filing_by_doc_id.get(doc_id)
        audit_rows.append(
            SegmentNoteSemanticsAuditRow(
                doc_id=doc_id,
                security_code=str(filing["security_code"] or "") if filing is not None else "",
                form_type=str(filing["form_type"] or "") if filing is not None else "",
                period_end=str(filing["period_end"] or "") if filing is not None else "",
                source_tags=" | ".join(sorted(tags_by_doc_id.get(doc_id, set()))),
                existing_row_count=existing_row_counts.get(doc_id, 0),
                proposed_row_count=len(expected_rows),
                selected_candidate_count=selected_by_doc.get(doc_id, 0),
                excluded_candidate_count=excluded_by_doc.get(doc_id, 0),
                review_row_count=review_by_doc.get(doc_id, 0),
                excel_suppressed_segment_profit_count=excel_suppressed_counts.get(doc_id, 0),
                status=status,
            )
        )
    summary = {
        "candidate_doc_count": len(target_doc_id_list),
        "rebuild_doc_count": len(rebuild_doc_ids),
        "match_doc_count": sum(row.status == "match" for row in audit_rows),
        "review_doc_count": sum(row.review_row_count > 0 for row in audit_rows),
        "excluded_candidate_count": sum(row.excluded_candidate_count for row in audit_rows),
        "excel_suppressed_segment_profit_count": sum(
            row.excel_suppressed_segment_profit_count for row in audit_rows
        ),
    }
    return SegmentNoteSemanticsAuditResult(
        rows=sorted(audit_rows, key=lambda row: row.doc_id),
        rebuild_doc_ids=tuple(sorted(rebuild_doc_ids)),
        summary=summary,
    )


def _write_tsv(path: Path, rows: list[SegmentNoteSemanticsAuditRow]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(SegmentNoteSemanticsAuditRow.__annotations__), delimiter="\t")
        writer.writeheader()
        for row in rows:
            writer.writerow(asdict(row))


def write_segment_note_semantics_audit(
    result: SegmentNoteSemanticsAuditResult,
    *,
    output_dir: Path = DEFAULT_SEGMENT_NOTE_SEMANTICS_AUDIT_OUTPUT_DIR,
) -> SegmentNoteSemanticsAuditPaths:
    run_dir = output_dir / datetime.now().strftime("segment_note_semantics_%Y%m%d_%H%M%S")
    run_dir.mkdir(parents=True, exist_ok=True)
    rebuild_doc_ids_path = run_dir / "rebuild_doc_ids.txt"
    rows_json_path = run_dir / "semantic_candidates.json"
    rows_tsv_path = run_dir / "semantic_candidates.tsv"
    summary_json_path = run_dir / "summary.json"
    rebuild_doc_ids_path.write_text("".join(f"{doc_id}\n" for doc_id in result.rebuild_doc_ids), encoding="utf-8")
    rows_json_path.write_text(
        json.dumps([asdict(row) for row in result.rows], ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    _write_tsv(rows_tsv_path, result.rows)
    summary_json_path.write_text(
        json.dumps(result.summary, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return SegmentNoteSemanticsAuditPaths(
        run_dir=run_dir,
        rebuild_doc_ids_path=rebuild_doc_ids_path,
        rows_json_path=rows_json_path,
        rows_tsv_path=rows_tsv_path,
        summary_json_path=summary_json_path,
    )
