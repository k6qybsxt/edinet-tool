from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
import csv
import json
from pathlib import Path
import sqlite3
from typing import Any

from edinet_monitor.services.collector.document_filter_service import HALF_REPORT_FORM_CODES
from edinet_monitor.services.normalizer.metric_normalize_service import (
    ISSUED_FILING_DATE_TAGS,
    normalize_raw_fact_rows,
)


DEFAULT_2Q_ISSUED_SHARES_AUDIT_OUTPUT_DIR = Path("logs/operation/2q_issued_shares_audit")
UNSAFE_ISSUED_SOURCE_TAGS = {
    "NumberOfSharesIssuedSharesVotingRights",
    "TotalNumberOfIssuedSharesSummaryOfBusinessResults",
}
RAW_FACT_COLUMNS = (
    "doc_id, tag_name, context_ref, unit_ref, period_type, period_start, period_end, "
    "instant_date, consolidation, context_dimensions_json, unit_measures_json, value_text"
)


@dataclass(frozen=True)
class TwoQIssuedSharesAuditRow:
    doc_id: str
    edinet_code: str
    security_code: str
    form_type: str
    period_end: str
    source_tag: str
    context_ref: str
    source_instant_date: str
    candidate_value: float
    existing_issued_value: float | None
    existing_issued_source_tag: str
    outstanding_value: float | None
    outstanding_calc_status: str
    status: str


@dataclass(frozen=True)
class TwoQIssuedSharesAuditResult:
    rows: list[TwoQIssuedSharesAuditRow]
    summary: dict[str, int]


@dataclass(frozen=True)
class TwoQIssuedSharesAuditPaths:
    run_dir: Path
    doc_ids_path: Path
    json_path: Path
    tsv_path: Path


def _chunked(values: list[str], size: int) -> list[list[str]]:
    target_size = max(int(size or 1), 1)
    return [values[index:index + target_size] for index in range(0, len(values), target_size)]


def _to_float(value: Any) -> float | None:
    try:
        return float(value) if value not in (None, "") else None
    except (TypeError, ValueError):
        return None


def _fetch_filings(
    conn: sqlite3.Connection,
    *,
    doc_ids: tuple[str, ...] = (),
) -> list[sqlite3.Row]:
    conn.row_factory = sqlite3.Row
    form_placeholders = ",".join("?" for _ in HALF_REPORT_FORM_CODES)
    params: list[Any] = list(sorted(HALF_REPORT_FORM_CODES))
    doc_filter = ""
    if doc_ids:
        doc_placeholders = ",".join("?" for _ in doc_ids)
        doc_filter = f"AND f.doc_id IN ({doc_placeholders})"
        params.extend(doc_ids)
    return conn.execute(
        f"""
        SELECT
            f.doc_id,
            f.edinet_code,
            COALESCE(f.security_code, im.security_code, '') AS security_code,
            f.form_type,
            COALESCE(f.period_end, '') AS period_end
        FROM filings f
        INNER JOIN issuer_master im
            ON im.edinet_code = f.edinet_code
        WHERE f.form_type IN ({form_placeholders})
          AND COALESCE(im.is_listed, 0) = 1
          AND COALESCE(im.exchange, '') = 'TSE'
          {doc_filter}
        ORDER BY f.doc_id
        """,
        tuple(params),
    ).fetchall()


def _fetch_raw_rows_by_doc_id(
    conn: sqlite3.Connection,
    doc_ids: list[str],
) -> dict[str, list[dict[str, Any]]]:
    out = {doc_id: [] for doc_id in doc_ids}
    tags = tuple(sorted(ISSUED_FILING_DATE_TAGS))
    tag_placeholders = ",".join("?" for _ in tags)
    for chunk in _chunked(doc_ids, 400):
        doc_placeholders = ",".join("?" for _ in chunk)
        rows = conn.execute(
            f"""
            SELECT {RAW_FACT_COLUMNS}
            FROM raw_facts
            WHERE doc_id IN ({doc_placeholders})
              AND tag_name IN ({tag_placeholders})
              AND context_ref LIKE 'FilingDateInstant%'
              AND value_text IS NOT NULL
              AND TRIM(value_text) <> ''
            ORDER BY id
            """,
            tuple(chunk) + tags,
        ).fetchall()
        for row in rows:
            out[str(row["doc_id"])].append(dict(row))
    return out


def _fetch_current_rows_by_doc_id(
    conn: sqlite3.Connection,
    doc_ids: list[str],
) -> tuple[dict[str, sqlite3.Row], dict[str, sqlite3.Row]]:
    issued_by_doc_id: dict[str, sqlite3.Row] = {}
    outstanding_by_doc_id: dict[str, sqlite3.Row] = {}
    for chunk in _chunked(doc_ids, 400):
        placeholders = ",".join("?" for _ in chunk)
        issued_rows = conn.execute(
            f"""
            SELECT doc_id, value_num, source_tag
            FROM normalized_metrics
            WHERE doc_id IN ({placeholders})
              AND metric_key = 'IssuedSharesCurrent'
            """,
            chunk,
        ).fetchall()
        outstanding_rows = conn.execute(
            f"""
            SELECT doc_id, value_num, calc_status
            FROM derived_metrics
            WHERE doc_id IN ({placeholders})
              AND metric_key = 'OutstandingSharesCurrent'
            """,
            chunk,
        ).fetchall()
        issued_by_doc_id.update({str(row["doc_id"]): row for row in issued_rows})
        outstanding_by_doc_id.update({str(row["doc_id"]): row for row in outstanding_rows})
    return issued_by_doc_id, outstanding_by_doc_id


def _selected_candidate(
    raw_rows: list[dict[str, Any]],
    *,
    filing: sqlite3.Row,
) -> dict[str, Any] | None:
    candidates = normalize_raw_fact_rows(
        raw_rows,
        edinet_code=str(filing["edinet_code"] or ""),
        security_code=str(filing["security_code"] or ""),
        filing_period_end=str(filing["period_end"] or ""),
        form_type=str(filing["form_type"] or ""),
        enforce_candidate_validation=True,
    )
    for candidate in candidates:
        if (
            candidate.get("metric_key") == "IssuedSharesCurrent"
            and candidate.get("period_end") == str(filing["period_end"] or "")
            and _to_float(candidate.get("value_num")) not in (None, 0.0)
        ):
            return candidate
    return None


def _audit_status(existing: sqlite3.Row | None) -> str:
    if existing is None or _to_float(existing["value_num"]) is None:
        return "missing"
    if str(existing["source_tag"] or "") in UNSAFE_ISSUED_SOURCE_TAGS:
        return "unsafe_source"
    return "match"


def _source_raw_row(
    raw_rows: list[dict[str, Any]],
    candidate: dict[str, Any],
) -> dict[str, Any]:
    candidate_value = _to_float(candidate.get("value_num"))
    for raw_row in raw_rows:
        if (
            raw_row.get("tag_name") == candidate.get("source_tag")
            and _to_float(raw_row.get("value_text")) == candidate_value
        ):
            return raw_row
    return {}


def build_two_q_issued_shares_audit(
    conn: sqlite3.Connection,
    *,
    doc_ids: tuple[str, ...] = (),
) -> TwoQIssuedSharesAuditResult:
    filings = _fetch_filings(conn, doc_ids=doc_ids)
    filing_by_doc_id = {str(row["doc_id"]): row for row in filings}
    target_doc_ids = list(filing_by_doc_id)
    raw_rows_by_doc_id = _fetch_raw_rows_by_doc_id(conn, target_doc_ids)
    issued_by_doc_id, outstanding_by_doc_id = _fetch_current_rows_by_doc_id(conn, target_doc_ids)
    rows: list[TwoQIssuedSharesAuditRow] = []

    for doc_id in target_doc_ids:
        filing = filing_by_doc_id[doc_id]
        candidate = _selected_candidate(raw_rows_by_doc_id[doc_id], filing=filing)
        if candidate is None:
            continue
        source_raw_row = _source_raw_row(raw_rows_by_doc_id[doc_id], candidate)
        existing = issued_by_doc_id.get(doc_id)
        outstanding = outstanding_by_doc_id.get(doc_id)
        rows.append(
            TwoQIssuedSharesAuditRow(
                doc_id=doc_id,
                edinet_code=str(filing["edinet_code"] or ""),
                security_code=str(filing["security_code"] or ""),
                form_type=str(filing["form_type"] or ""),
                period_end=str(filing["period_end"] or ""),
                source_tag=str(candidate.get("source_tag") or ""),
                context_ref=str(source_raw_row.get("context_ref") or ""),
                source_instant_date=str(source_raw_row.get("instant_date") or ""),
                candidate_value=float(candidate["value_num"]),
                existing_issued_value=_to_float(existing["value_num"]) if existing is not None else None,
                existing_issued_source_tag=str(existing["source_tag"] or "") if existing is not None else "",
                outstanding_value=_to_float(outstanding["value_num"]) if outstanding is not None else None,
                outstanding_calc_status=str(outstanding["calc_status"] or "") if outstanding is not None else "",
                status=_audit_status(existing),
            )
        )

    rows.sort(key=lambda row: row.doc_id)
    summary = {
        "eligible_count": len(rows),
        "actionable_count": sum(row.status in {"missing", "unsafe_source"} for row in rows),
        "match_count": sum(row.status == "match" for row in rows),
        "missing_count": sum(row.status == "missing" for row in rows),
        "unsafe_source_count": sum(row.status == "unsafe_source" for row in rows),
    }
    return TwoQIssuedSharesAuditResult(rows=rows, summary=summary)


def write_two_q_issued_shares_audit(
    result: TwoQIssuedSharesAuditResult,
    *,
    output_dir: Path = DEFAULT_2Q_ISSUED_SHARES_AUDIT_OUTPUT_DIR,
) -> TwoQIssuedSharesAuditPaths:
    run_id = datetime.now().strftime("two_q_issued_shares_%Y%m%d_%H%M%S")
    run_dir = output_dir / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    doc_ids_path = run_dir / "doc_ids.txt"
    json_path = run_dir / "audit.json"
    tsv_path = run_dir / "audit.tsv"
    actionable_rows = [row for row in result.rows if row.status in {"missing", "unsafe_source"}]
    doc_ids_path.write_text(
        "".join(f"{row.doc_id}\n" for row in actionable_rows),
        encoding="utf-8",
    )
    json_path.write_text(
        json.dumps(
            {
                "summary": result.summary,
                "rows": [asdict(row) for row in result.rows],
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    fieldnames = list(TwoQIssuedSharesAuditRow.__annotations__)
    with tsv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, delimiter="\t")
        writer.writeheader()
        for row in result.rows:
            writer.writerow(asdict(row))
    return TwoQIssuedSharesAuditPaths(
        run_dir=run_dir,
        doc_ids_path=doc_ids_path,
        json_path=json_path,
        tsv_path=tsv_path,
    )
