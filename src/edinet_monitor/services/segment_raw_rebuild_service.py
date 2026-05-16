from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
import sqlite3
from typing import Any

from edinet_monitor.services.collector.document_filter_service import is_half_form_type
from edinet_monitor.services.edinet_storage_path_service import resolve_storage_paths
from edinet_monitor.services.parser.raw_fact_mapper import to_raw_fact_rows
from edinet_monitor.services.parser.raw_fact_store_service import (
    delete_raw_facts_by_doc_id,
    insert_raw_facts,
)
from edinet_monitor.services.parser.xbrl_parse_service import parse_xbrl_to_raw
from edinet_monitor.services.storage.zip_extract_service import (
    extract_period_end_from_xbrl_member_name,
    extract_preferred_xbrl,
)


@dataclass(frozen=True)
class SegmentRawRebuildRow:
    doc_id: str
    security_code: str
    company_name: str
    form_type: str
    period_rank_label: str
    period_end: str
    status: str
    zip_path: str
    xbrl_path: str
    xbrl_member_name: str
    raw_rows: int
    dimension_rows: int
    error: str = ""


@dataclass(frozen=True)
class SegmentRawRebuildResult:
    rows: list[SegmentRawRebuildRow]
    output_path: Path


def _now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _parse_mode(form_type: str) -> str:
    return "half" if is_half_form_type(form_type) else "full"


def _dimension_row_count(rows: list[dict[str, Any]]) -> int:
    return sum(
        1
        for row in rows
        if str(row.get("context_dimensions_json") or "").strip() not in {"", "{}"}
    )


def _update_filing_after_rebuild(
    conn: sqlite3.Connection,
    *,
    doc_id: str,
    zip_path: Path,
    xbrl_path: Path,
    xbrl_member_name: str,
    accounting_standard: str,
    document_display_unit: str,
) -> None:
    conn.execute(
        """
        UPDATE filings
        SET zip_path = ?,
            xbrl_path = ?,
            xbrl_member_name = ?,
            accounting_standard = ?,
            document_display_unit = ?,
            period_end = CASE WHEN ? <> '' THEN ? ELSE period_end END
        WHERE doc_id = ?
        """,
        (
            str(zip_path),
            str(xbrl_path),
            xbrl_member_name,
            accounting_standard,
            document_display_unit,
            extract_period_end_from_xbrl_member_name(xbrl_member_name),
            extract_period_end_from_xbrl_member_name(xbrl_member_name),
            doc_id,
        ),
    )


def rebuild_segment_raw_facts(
    conn: sqlite3.Connection,
    *,
    filings: list[dict[str, Any]],
    apply: bool,
    force_extract: bool = False,
    output_dir: str | Path,
) -> SegmentRawRebuildResult:
    rows: list[SegmentRawRebuildRow] = []
    for filing in filings:
        doc_id = str(filing.get("doc_id") or "")
        form_type = str(filing.get("form_type") or "")
        resolved = resolve_storage_paths(filing)
        zip_path = resolved.zip_path
        xbrl_path = resolved.xbrl_path
        xbrl_member_name = str(filing.get("xbrl_member_name") or "")

        if zip_path is None:
            rows.append(
                SegmentRawRebuildRow(
                    doc_id=doc_id,
                    security_code=str(filing.get("security_code") or ""),
                    company_name=str(filing.get("company_name") or ""),
                    form_type=form_type,
                    period_rank_label=str(filing.get("period_rank_label") or ""),
                    period_end=str(filing.get("period_end") or ""),
                    status="zip_missing",
                    zip_path=str(filing.get("zip_path") or ""),
                    xbrl_path=str(filing.get("xbrl_path") or ""),
                    xbrl_member_name=xbrl_member_name,
                    raw_rows=0,
                    dimension_rows=0,
                )
            )
            continue

        try:
            if apply and (force_extract or xbrl_path is None):
                extracted = extract_preferred_xbrl(
                    zip_path,
                    resolved.expected_xbrl_path,
                    form_type=form_type,
                )
                xbrl_path = extracted.output_path
                xbrl_member_name = extracted.member_name
            elif xbrl_path is None:
                rows.append(
                    SegmentRawRebuildRow(
                        doc_id=doc_id,
                        security_code=str(filing.get("security_code") or ""),
                        company_name=str(filing.get("company_name") or ""),
                        form_type=form_type,
                        period_rank_label=str(filing.get("period_rank_label") or ""),
                        period_end=str(filing.get("period_end") or ""),
                        status="xbrl_missing",
                        zip_path=str(zip_path),
                        xbrl_path="",
                        xbrl_member_name=xbrl_member_name,
                        raw_rows=0,
                        dimension_rows=0,
                    )
                )
                continue

            if not apply:
                rows.append(
                    SegmentRawRebuildRow(
                        doc_id=doc_id,
                        security_code=str(filing.get("security_code") or ""),
                        company_name=str(filing.get("company_name") or ""),
                        form_type=form_type,
                        period_rank_label=str(filing.get("period_rank_label") or ""),
                        period_end=str(filing.get("period_end") or ""),
                        status="dry_run_ready",
                        zip_path=str(zip_path),
                        xbrl_path=str(xbrl_path),
                        xbrl_member_name=xbrl_member_name,
                        raw_rows=0,
                        dimension_rows=0,
                    )
                )
                continue

            parsed = parse_xbrl_to_raw(xbrl_path, mode=_parse_mode(form_type))
            raw_rows = to_raw_fact_rows(doc_id, parsed, xbrl_member_name=xbrl_member_name)
            parsed_meta = dict(parsed.get("meta") or {})
            parsed_out = dict(parsed.get("out") or {})
            accounting_standard = str(parsed_meta.get("accounting_standard") or "")
            document_display_unit = str(
                parsed_meta.get("document_display_unit")
                or parsed_out.get("DocumentDisplayUnit")
                or ""
            )
            delete_raw_facts_by_doc_id(conn, doc_id)
            saved_rows = insert_raw_facts(conn, raw_rows)
            _update_filing_after_rebuild(
                conn,
                doc_id=doc_id,
                zip_path=zip_path,
                xbrl_path=xbrl_path,
                xbrl_member_name=xbrl_member_name,
                accounting_standard=accounting_standard,
                document_display_unit=document_display_unit,
            )
            conn.commit()
            rows.append(
                SegmentRawRebuildRow(
                    doc_id=doc_id,
                    security_code=str(filing.get("security_code") or ""),
                    company_name=str(filing.get("company_name") or ""),
                    form_type=form_type,
                    period_rank_label=str(filing.get("period_rank_label") or ""),
                    period_end=str(filing.get("period_end") or ""),
                    status="rebuilt",
                    zip_path=str(zip_path),
                    xbrl_path=str(xbrl_path),
                    xbrl_member_name=xbrl_member_name,
                    raw_rows=saved_rows,
                    dimension_rows=_dimension_row_count(raw_rows),
                )
            )
        except Exception as exc:
            rows.append(
                SegmentRawRebuildRow(
                    doc_id=doc_id,
                    security_code=str(filing.get("security_code") or ""),
                    company_name=str(filing.get("company_name") or ""),
                    form_type=form_type,
                    period_rank_label=str(filing.get("period_rank_label") or ""),
                    period_end=str(filing.get("period_end") or ""),
                    status="error",
                    zip_path=str(zip_path or ""),
                    xbrl_path=str(xbrl_path or ""),
                    xbrl_member_name=xbrl_member_name,
                    raw_rows=0,
                    dimension_rows=0,
                    error=repr(exc),
                )
            )

    output_path = write_segment_raw_rebuild_report(rows, output_dir=output_dir, apply=apply)
    return SegmentRawRebuildResult(rows=rows, output_path=output_path)


def write_segment_raw_rebuild_report(
    rows: list[SegmentRawRebuildRow],
    *,
    output_dir: str | Path,
    apply: bool,
) -> Path:
    directory = Path(output_dir)
    directory.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = directory / f"segment_raw_rebuild_{'apply' if apply else 'dry_run'}_{timestamp}.tsv"
    headers = [
        "doc_id",
        "security_code",
        "company_name",
        "form_type",
        "period_rank_label",
        "period_end",
        "status",
        "raw_rows",
        "dimension_rows",
        "zip_path",
        "xbrl_path",
        "xbrl_member_name",
        "error",
    ]
    lines = ["\t".join(headers)]
    for row in rows:
        payload = row.__dict__
        lines.append("\t".join(str(payload.get(header, "") or "") for header in headers))
    path.write_text("\n".join(lines) + "\n", encoding="utf-8-sig")
    return path
