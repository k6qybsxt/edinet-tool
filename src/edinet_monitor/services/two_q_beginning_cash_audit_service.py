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
    select_beginning_cash_source,
    select_2q_beginning_cash_source,
)


DEFAULT_2Q_BEGINNING_CASH_AUDIT_OUTPUT_DIR = Path("logs/operation/2q_beginning_cash_audit")
RAW_FACT_COLUMNS = (
    "doc_id, tag_name, context_ref, unit_ref, period_type, period_start, period_end, "
    "instant_date, consolidation, context_dimensions_json, unit_measures_json, value_text"
)


@dataclass(frozen=True)
class TwoQBeginningCashAuditRow:
    doc_id: str
    edinet_code: str
    security_code: str
    form_type: str
    period_end: str
    source_group: str
    source_tag: str
    selected_value: float
    prior1year_value: float | None
    current_value: float | None
    current_calc_status: str
    status: str


@dataclass(frozen=True)
class TwoQBeginningCashAuditResult:
    rows: list[TwoQBeginningCashAuditRow]
    summary: dict[str, int]


@dataclass(frozen=True)
class TwoQBeginningCashAuditPaths:
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
    if doc_ids:
        placeholders = ",".join("?" for _ in doc_ids)
        return conn.execute(
            f"""
            SELECT doc_id, edinet_code, security_code, form_type, period_end
            FROM filings
            WHERE doc_id IN ({placeholders})
            ORDER BY doc_id
            """,
            doc_ids,
        ).fetchall()
    placeholders = ",".join("?" for _ in HALF_REPORT_FORM_CODES)
    return conn.execute(
        f"""
        SELECT doc_id, edinet_code, security_code, form_type, period_end
        FROM filings
        WHERE form_type IN ({placeholders})
        ORDER BY doc_id
        """,
        tuple(sorted(HALF_REPORT_FORM_CODES)),
    ).fetchall()


def _fetch_raw_rows_by_doc_id(
    conn: sqlite3.Connection,
    doc_ids: list[str],
) -> dict[str, list[dict[str, Any]]]:
    out = {doc_id: [] for doc_id in doc_ids}
    for chunk in _chunked(doc_ids, 400):
        placeholders = ",".join("?" for _ in chunk)
        rows = conn.execute(
            f"""
            SELECT {RAW_FACT_COLUMNS}
            FROM raw_facts
            WHERE doc_id IN ({placeholders})
              AND tag_name IN (
                  'CashAndCashEquivalents',
                  'CashAndCashEquivalentsIFRS',
                  'CashAndCashEquivalentsSummaryOfBusinessResults',
                  'CashAndCashEquivalentsIFRSSummaryOfBusinessResults',
                  'CashAndCashEquivalentsUSGAAPSummaryOfBusinessResults',
                  'CashAndCashEquivalentsAtBeginningOfPeriod',
                  'CashAndCashEquivalentsAtBeginningOfYear',
                  'CashAndCashEquivalentsAtBeginningOfFiscalYear',
                  'CashAndCashEquivalentsAtBeginningOfInterimPeriod'
              )
            """,
            chunk,
        ).fetchall()
        for row in rows:
            out[str(row["doc_id"])].append(dict(row))
    explicit_doc_ids = [
        doc_id
        for doc_id, raw_rows in out.items()
        if (selected := select_beginning_cash_source(raw_rows)) is not None
        and selected[1] == "explicit"
    ]
    for chunk in _chunked(explicit_doc_ids, 400):
        placeholders = ",".join("?" for _ in chunk)
        rows = conn.execute(
            f"""
            SELECT {RAW_FACT_COLUMNS}
            FROM raw_facts
            WHERE doc_id IN ({placeholders})
              AND context_ref LIKE 'CurrentYTDDuration%'
            """,
            chunk,
        ).fetchall()
        for row in rows:
            out[str(row["doc_id"])].append(dict(row))
    return out


def _fetch_current_beginning_cash_by_doc_id(
    conn: sqlite3.Connection,
    doc_ids: list[str],
) -> dict[str, sqlite3.Row]:
    out: dict[str, sqlite3.Row] = {}
    for chunk in _chunked(doc_ids, 400):
        placeholders = ",".join("?" for _ in chunk)
        rows = conn.execute(
            f"""
            SELECT doc_id, value_num, calc_status
            FROM derived_metrics
            WHERE doc_id IN ({placeholders})
              AND metric_key = 'BeginningCashBalanceCurrent'
            """,
            chunk,
        ).fetchall()
        out.update({str(row["doc_id"]): row for row in rows})
    return out


def build_two_q_beginning_cash_audit(
    conn: sqlite3.Connection,
    *,
    doc_ids: tuple[str, ...] = (),
) -> TwoQBeginningCashAuditResult:
    filings = _fetch_filings(conn, doc_ids=doc_ids)
    filing_by_doc_id = {str(row["doc_id"]): row for row in filings}
    target_doc_ids = list(filing_by_doc_id)
    raw_rows_by_doc_id = _fetch_raw_rows_by_doc_id(conn, target_doc_ids)
    current_by_doc_id = _fetch_current_beginning_cash_by_doc_id(conn, target_doc_ids)
    rows: list[TwoQBeginningCashAuditRow] = []

    for doc_id in target_doc_ids:
        filing = filing_by_doc_id[doc_id]
        selected = select_2q_beginning_cash_source(
            raw_rows_by_doc_id[doc_id],
            filing_period_end=str(filing["period_end"] or ""),
            form_type=str(filing["form_type"] or ""),
        )
        if selected is None:
            continue
        _, source_group, source_row = selected
        selected_value = _to_float(source_row.get("value_text"))
        if selected_value is None:
            continue
        current = current_by_doc_id.get(doc_id)
        current_value = _to_float(current["value_num"]) if current is not None else None
        current_calc_status = str(current["calc_status"] or "") if current is not None else ""
        if current_value is None:
            status = "missing"
        elif current_value == selected_value:
            status = "match"
        else:
            status = "mismatch"
        prior1year_value = selected_value if source_group != "explicit" else None
        rows.append(
            TwoQBeginningCashAuditRow(
                doc_id=doc_id,
                edinet_code=str(filing["edinet_code"] or ""),
                security_code=str(filing["security_code"] or ""),
                form_type=str(filing["form_type"] or ""),
                period_end=str(filing["period_end"] or ""),
                source_group=source_group,
                source_tag=str(source_row.get("tag_name") or ""),
                selected_value=selected_value,
                prior1year_value=prior1year_value,
                current_value=current_value,
                current_calc_status=current_calc_status,
                status=status,
            )
        )

    rows.sort(key=lambda row: row.doc_id)
    summary = {
        "target_count": len(rows),
        "match_count": sum(row.status == "match" for row in rows),
        "mismatch_count": sum(row.status == "mismatch" for row in rows),
        "missing_count": sum(row.status == "missing" for row in rows),
    }
    return TwoQBeginningCashAuditResult(rows=rows, summary=summary)


def write_two_q_beginning_cash_audit(
    result: TwoQBeginningCashAuditResult,
    *,
    output_dir: Path = DEFAULT_2Q_BEGINNING_CASH_AUDIT_OUTPUT_DIR,
) -> TwoQBeginningCashAuditPaths:
    run_id = datetime.now().strftime("two_q_beginning_cash_%Y%m%d_%H%M%S")
    run_dir = output_dir / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    doc_ids_path = run_dir / "doc_ids.txt"
    json_path = run_dir / "audit.json"
    tsv_path = run_dir / "audit.tsv"
    doc_ids_path.write_text(
        "".join(f"{row.doc_id}\n" for row in result.rows),
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
    with tsv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=list(asdict(result.rows[0]).keys()) if result.rows else ["doc_id"],
            delimiter="\t",
        )
        writer.writeheader()
        for row in result.rows:
            writer.writerow(asdict(row))
    return TwoQBeginningCashAuditPaths(
        run_dir=run_dir,
        doc_ids_path=doc_ids_path,
        json_path=json_path,
        tsv_path=tsv_path,
    )
