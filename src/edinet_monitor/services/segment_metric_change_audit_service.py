from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
import csv
import json
from pathlib import Path
import sqlite3
from typing import Any

from edinet_monitor.config.settings import OPERATION_LOG_ROOT
from edinet_monitor.services.segment_metric_service import (
    SEGMENT_RULE_VERSION,
    resolve_segment_fiscal_year_anchor,
)


DEFAULT_SEGMENT_METRIC_CHANGE_AUDIT_OUTPUT_DIR = OPERATION_LOG_ROOT / "segment_metric_changes"
AUDIT_DOC_ID_CHUNK_SIZE = 250
PROFIT_METRIC_BASES = {"OperatingIncome", "SegmentProfit"}


@dataclass(frozen=True)
class SegmentFiscalAnchorAuditRow:
    doc_id: str
    security_code: str
    period_end: str
    existing_fiscal_years: str
    expected_fiscal_year: int | None
    fiscal_year_end: str
    anchor_source: str
    anchor_status: str
    status: str


@dataclass(frozen=True)
class SegmentProfitClassificationAuditRow:
    doc_id: str
    security_code: str
    member_qname: str
    period_start: str
    period_end: str
    existing_metric_bases: str
    proposed_metric_bases: str
    line_item_labels: str
    classification_status: str
    status: str


@dataclass(frozen=True)
class SegmentMetricChangeAuditResult:
    fiscal_anchor_rows: list[SegmentFiscalAnchorAuditRow]
    profit_classification_rows: list[SegmentProfitClassificationAuditRow]
    rebuild_doc_ids: tuple[str, ...]
    summary: dict[str, int]


@dataclass(frozen=True)
class SegmentMetricChangeAuditPaths:
    run_dir: Path
    rebuild_doc_ids_path: Path
    fiscal_anchor_json_path: Path
    fiscal_anchor_tsv_path: Path
    profit_classification_json_path: Path
    profit_classification_tsv_path: Path
    summary_json_path: Path


def _chunked(values: list[str], size: int = AUDIT_DOC_ID_CHUNK_SIZE) -> list[list[str]]:
    return [values[index : index + size] for index in range(0, len(values), size)]


def _metric_group_key(row: sqlite3.Row) -> tuple[str, str, str, str, str]:
    segment_kind = str(row["segment_kind"])
    member_qname = str(row["member_qname"])
    if segment_kind == "total":
        member_qname = "TOTAL"
    return (
        str(row["doc_id"]),
        segment_kind,
        member_qname,
        str(row["period_start"]),
        str(row["period_end"]),
    )


def _detail(value: Any) -> dict[str, Any]:
    try:
        parsed = json.loads(str(value or "{}"))
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _candidate_doc_ids(conn: sqlite3.Connection, doc_ids: tuple[str, ...]) -> list[str]:
    if doc_ids:
        return list(doc_ids)
    rows = conn.execute(
        """
        SELECT DISTINCT doc_id
        FROM segment_metrics
        WHERE (period_scope = 'quarter' AND quarter_type = '2Q')
           OR metric_base IN ('OperatingIncome', 'SegmentProfit')
        ORDER BY doc_id
        """
    ).fetchall()
    return [str(row[0]) for row in rows]


def _existing_rows_by_doc_id(
    conn: sqlite3.Connection,
    doc_ids: list[str],
) -> dict[str, list[sqlite3.Row]]:
    out = {doc_id: [] for doc_id in doc_ids}
    for chunk in _chunked(doc_ids):
        placeholders = ",".join("?" for _ in chunk)
        rows = conn.execute(
            f"""
            SELECT *
            FROM segment_metrics
            WHERE doc_id IN ({placeholders})
            """,
            chunk,
        ).fetchall()
        for row in rows:
            out.setdefault(str(row["doc_id"]), []).append(row)
    return out


def _filings_by_doc_id(
    conn: sqlite3.Connection,
    doc_ids: list[str],
) -> dict[str, sqlite3.Row]:
    out: dict[str, sqlite3.Row] = {}
    for chunk in _chunked(doc_ids):
        placeholders = ",".join("?" for _ in chunk)
        rows = conn.execute(
            f"""
            SELECT doc_id, edinet_code, form_type, period_end
            FROM filings
            WHERE doc_id IN ({placeholders})
            """,
            chunk,
        ).fetchall()
        out.update({str(row["doc_id"]): row for row in rows})
    return out


def _current_fiscal_year_ends(
    conn: sqlite3.Connection,
    doc_ids: list[str],
) -> dict[str, str]:
    values_by_doc: dict[str, set[str]] = {}
    for chunk in _chunked(doc_ids):
        placeholders = ",".join("?" for _ in chunk)
        rows = conn.execute(
            f"""
            SELECT doc_id, value_text
            FROM raw_facts
            WHERE doc_id IN ({placeholders})
              AND tag_name = 'CurrentFiscalYearEndDateDEI'
            """,
            chunk,
        ).fetchall()
        for row in rows:
            value = str(row["value_text"] or "").strip()[:10]
            if len(value) == 10:
                values_by_doc.setdefault(str(row["doc_id"]), set()).add(value)
    return {
        doc_id: next(iter(values))
        for doc_id, values in values_by_doc.items()
        if len(values) == 1
    }


def _annual_period_ends_by_edinet_code(conn: sqlite3.Connection) -> dict[str, list[str]]:
    rows = conn.execute(
        """
        SELECT edinet_code, period_end
        FROM filings
        WHERE form_type = '030000'
          AND period_end IS NOT NULL
        ORDER BY edinet_code, period_end
        """
    ).fetchall()
    result: dict[str, list[str]] = {}
    for row in rows:
        edinet_code = str(row["edinet_code"] or "")
        period_end = str(row["period_end"] or "").strip()[:10]
        if edinet_code and len(period_end) == 10:
            result.setdefault(edinet_code, []).append(period_end)
    return result


def _anchor_row(
    *,
    doc_id: str,
    existing_rows: list[sqlite3.Row],
    filing: sqlite3.Row | None,
    current_fiscal_year_ends: dict[str, str],
    annual_period_ends_by_edinet_code: dict[str, list[str]],
) -> SegmentFiscalAnchorAuditRow | None:
    existing_quarter_rows = [
        row
        for row in existing_rows
        if str(row["period_scope"] or "") == "quarter" and str(row["quarter_type"] or "") == "2Q"
    ]
    if not existing_quarter_rows:
        return None
    first = existing_quarter_rows[0]
    existing_years = sorted(
        {
            int(row["fiscal_year"])
            for row in existing_quarter_rows
            if row["fiscal_year"] is not None
        }
    )
    if filing is None:
        expected_year, fiscal_year_end, source, anchor_status = None, "", "", "unresolved"
    else:
        anchor = resolve_segment_fiscal_year_anchor(
            filing=filing,
            fact_period_end=str(first["period_end"] or ""),
            current_fiscal_year_ends=current_fiscal_year_ends,
            annual_period_ends_by_edinet_code=annual_period_ends_by_edinet_code,
        )
        expected_year = anchor.fiscal_year
        fiscal_year_end = anchor.fiscal_year_end
        source = anchor.source
        anchor_status = anchor.status
    if anchor_status != "ok" or expected_year is None:
        # A re-run has converged when the rebuilt row is already retained as
        # review. Keep it visible in the audit without scheduling it forever.
        existing_is_review = bool(existing_quarter_rows) and all(
            str(row["calc_status"] or "") == "review" for row in existing_quarter_rows
        )
        status = "review_match" if existing_is_review else "review"
    elif existing_years != [expected_year]:
        status = "mismatch"
    else:
        status = "match"
    period_end = str(first["period_end"] or "")
    security_code = str(first["security_code"] or "")
    return SegmentFiscalAnchorAuditRow(
        doc_id=doc_id,
        security_code=security_code,
        period_end=period_end,
        existing_fiscal_years=",".join(str(value) for value in existing_years),
        expected_fiscal_year=expected_year,
        fiscal_year_end=fiscal_year_end,
        anchor_source=source,
        anchor_status=anchor_status,
        status=status,
    )


def _profit_rows(
    *,
    existing_rows: list[sqlite3.Row],
) -> list[SegmentProfitClassificationAuditRow]:
    existing_by_group: dict[tuple[str, str, str, str, str], list[sqlite3.Row]] = {}
    for row in existing_rows:
        if str(row["metric_base"] or "") in PROFIT_METRIC_BASES:
            existing_by_group.setdefault(_metric_group_key(row), []).append(row)

    out: list[SegmentProfitClassificationAuditRow] = []
    for key in sorted(existing_by_group):
        existing_group = existing_by_group.get(key, [])
        existing_bases = sorted(
            {
                str(row["metric_base"])
                for row in existing_group
                if str(row["calc_status"] or "") == "ok"
            }
        )
        details = [_detail(row["source_detail_json"]) for row in existing_group]
        labels = sorted(
            {
                str(detail.get("line_item_label") or "")
                for detail in details
                if str(detail.get("line_item_label") or "")
            }
        )
        review = any(str(row["calc_status"] or "") == "review" for row in existing_group)
        legacy_fallback = any(
            str(detail.get("source") or "") == "operating_income_segment_profit_fallback"
            for detail in details
        )
        current_rule = bool(existing_group) and all(
            str(row["rule_version"] or "") == SEGMENT_RULE_VERSION for row in existing_group
        )
        classifications = {
            str(detail.get("profit_metric_classification_status") or "")
            for detail in details
        }
        if review or "review" in classifications:
            status = "review_match" if current_rule else "review"
        elif legacy_fallback:
            status = "changed"
        elif len(existing_bases) > 1:
            status = "review"
        elif current_rule:
            status = "match"
        else:
            status = "candidate"
        sample = existing_group[0]
        security_code = str(sample["security_code"] or "")
        out.append(
            SegmentProfitClassificationAuditRow(
                doc_id=key[0],
                security_code=security_code,
                member_qname=key[2],
                period_start=key[3],
                period_end=key[4],
                existing_metric_bases=",".join(existing_bases),
                proposed_metric_bases="",
                line_item_labels=" | ".join(labels),
                classification_status=(
                    "review"
                    if review
                    else "legacy_fallback"
                    if legacy_fallback
                    else "confirmed"
                    if current_rule
                    else "pending_rebuild"
                ),
                status=status,
            )
        )
    return out


def build_segment_metric_change_audit(
    conn: sqlite3.Connection,
    *,
    doc_ids: tuple[str, ...] = (),
) -> SegmentMetricChangeAuditResult:
    target_doc_ids = _candidate_doc_ids(conn, doc_ids)
    existing_by_doc_id = _existing_rows_by_doc_id(conn, target_doc_ids)
    filings_by_doc_id = _filings_by_doc_id(conn, target_doc_ids)
    current_fiscal_year_ends = _current_fiscal_year_ends(conn, target_doc_ids)
    annual_period_ends_by_edinet_code = _annual_period_ends_by_edinet_code(conn)
    fiscal_anchor_rows: list[SegmentFiscalAnchorAuditRow] = []
    profit_classification_rows: list[SegmentProfitClassificationAuditRow] = []

    for doc_id in target_doc_ids:
        existing_rows = existing_by_doc_id.get(doc_id, [])
        anchor = _anchor_row(
            doc_id=doc_id,
            existing_rows=existing_rows,
            filing=filings_by_doc_id.get(doc_id),
            current_fiscal_year_ends=current_fiscal_year_ends,
            annual_period_ends_by_edinet_code=annual_period_ends_by_edinet_code,
        )
        if anchor is not None:
            fiscal_anchor_rows.append(anchor)
        profit_classification_rows.extend(_profit_rows(existing_rows=existing_rows))

    rebuild_doc_ids = tuple(
        sorted(
            {
                row.doc_id
                for row in fiscal_anchor_rows
                if row.status in {"mismatch", "review"}
            }
            | {
                row.doc_id
                for row in profit_classification_rows
                if row.status in {"changed", "review", "candidate"}
            }
        )
    )
    summary = {
        "candidate_doc_count": len(target_doc_ids),
        "fiscal_anchor_row_count": len(fiscal_anchor_rows),
        "fiscal_anchor_mismatch_count": sum(row.status == "mismatch" for row in fiscal_anchor_rows),
        "fiscal_anchor_review_count": sum(
            row.status in {"review", "review_match"} for row in fiscal_anchor_rows
        ),
        "profit_classification_row_count": len(profit_classification_rows),
        "profit_classification_changed_count": sum(row.status == "changed" for row in profit_classification_rows),
        "profit_classification_candidate_count": sum(row.status == "candidate" for row in profit_classification_rows),
        "profit_classification_review_count": sum(
            row.status in {"review", "review_match"} for row in profit_classification_rows
        ),
        "rebuild_doc_count": len(rebuild_doc_ids),
    }
    return SegmentMetricChangeAuditResult(
        fiscal_anchor_rows=sorted(fiscal_anchor_rows, key=lambda row: row.doc_id),
        profit_classification_rows=sorted(
            profit_classification_rows,
            key=lambda row: (row.doc_id, row.member_qname, row.period_end),
        ),
        rebuild_doc_ids=rebuild_doc_ids,
        summary=summary,
    )


def _write_tsv(path: Path, rows: list[Any], fieldnames: list[str]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, delimiter="\t")
        writer.writeheader()
        for row in rows:
            writer.writerow(asdict(row))


def write_segment_metric_change_audit(
    result: SegmentMetricChangeAuditResult,
    *,
    output_dir: Path = DEFAULT_SEGMENT_METRIC_CHANGE_AUDIT_OUTPUT_DIR,
) -> SegmentMetricChangeAuditPaths:
    run_dir = output_dir / datetime.now().strftime("segment_metric_changes_%Y%m%d_%H%M%S")
    run_dir.mkdir(parents=True, exist_ok=True)
    rebuild_doc_ids_path = run_dir / "rebuild_doc_ids.txt"
    fiscal_anchor_json_path = run_dir / "fiscal_anchor.json"
    fiscal_anchor_tsv_path = run_dir / "fiscal_anchor.tsv"
    profit_classification_json_path = run_dir / "profit_classification.json"
    profit_classification_tsv_path = run_dir / "profit_classification.tsv"
    summary_json_path = run_dir / "summary.json"

    rebuild_doc_ids_path.write_text(
        "".join(f"{doc_id}\n" for doc_id in result.rebuild_doc_ids),
        encoding="utf-8",
    )
    fiscal_anchor_json_path.write_text(
        json.dumps([asdict(row) for row in result.fiscal_anchor_rows], ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    profit_classification_json_path.write_text(
        json.dumps([asdict(row) for row in result.profit_classification_rows], ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    summary_json_path.write_text(
        json.dumps(result.summary, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    _write_tsv(
        fiscal_anchor_tsv_path,
        result.fiscal_anchor_rows,
        list(SegmentFiscalAnchorAuditRow.__annotations__),
    )
    _write_tsv(
        profit_classification_tsv_path,
        result.profit_classification_rows,
        list(SegmentProfitClassificationAuditRow.__annotations__),
    )
    return SegmentMetricChangeAuditPaths(
        run_dir=run_dir,
        rebuild_doc_ids_path=rebuild_doc_ids_path,
        fiscal_anchor_json_path=fiscal_anchor_json_path,
        fiscal_anchor_tsv_path=fiscal_anchor_tsv_path,
        profit_classification_json_path=profit_classification_json_path,
        profit_classification_tsv_path=profit_classification_tsv_path,
        summary_json_path=summary_json_path,
    )
