from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from edinet_monitor.config.settings import (
    DEFAULT_DERIVED_METRICS_RULE_VERSION,
    DEFAULT_RULE_VERSION,
)
from edinet_monitor.services.derived_metrics.derived_metric_service import (
    _outstanding_shares_status,
    infer_period_info,
)


ISSUED_SHARE_TAGS = {
    "NumberOfIssuedSharesAsOfFiscalYearEndIssuedSharesTotalNumberOfSharesEtc",
    "NumberOfIssuedSharesAsOfFilingDateIssuedSharesTotalNumberOfSharesEtc",
    "TotalNumberOfIssuedSharesIssuedSharesTotalNumberOfSharesEtc",
    "TotalNumberOfIssuedSharesCommonStockIssuedSharesTotalNumberOfSharesEtc",
    "TotalNumberOfIssuedSharesOrdinaryShareIssuedSharesTotalNumberOfSharesEtc",
}

TREASURY_SHARE_TAGS = {
    "TotalNumberOfSharesHeldTreasurySharesEtc",
    "NumberOfSharesHeldInOwnNameTreasurySharesEtc",
    "TotalNumberOfSharesHeldInTheNameOfOthersTreasurySharesEtc",
    "TotalNumberOfSharesHeldInOwnNameTreasurySharesEtc",
    "TotalNumberOfTreasurySharesSummaryOfBusinessResults",
    "NumberOfTreasurySharesAsOfFiscalYearEndIssuedSharesTotalNumberOfSharesEtc",
    "TreasurySharesAtTheEndOfFiscalYearIssuedSharesTotalNumberOfSharesEtc",
    "NumberOfSharesIssuedSharesVotingRights",
}

HALF_CURRENT_INSTANT_CONTEXT_MARKERS = (
    "CurrentQuarterInstant",
    "InterimInstant",
)

ISSUED_FILING_DATE_TAGS = {
    "NumberOfIssuedSharesAsOfFiscalYearEndIssuedSharesTotalNumberOfSharesEtc",
    "NumberOfIssuedSharesAsOfFilingDateIssuedSharesTotalNumberOfSharesEtc",
}

TREASURY_AGGREGATE_TAG_PRIORITY = (
    "TotalNumberOfSharesHeldTreasurySharesEtc",
    "NumberOfSharesHeldInOwnNameTreasurySharesEtc",
    "TotalNumberOfSharesHeldInOwnNameTreasurySharesEtc",
    "TotalNumberOfSharesHeldInTheNameOfOthersTreasurySharesEtc",
    "TotalNumberOfTreasurySharesSummaryOfBusinessResults",
    "NumberOfTreasurySharesAsOfFiscalYearEndIssuedSharesTotalNumberOfSharesEtc",
    "TreasurySharesAtTheEndOfFiscalYearIssuedSharesTotalNumberOfSharesEtc",
)

UNSAFE_ISSUED_SOURCE_TAGS = {
    "NumberOfSharesIssuedSharesVotingRights",
    "TotalNumberOfIssuedSharesSummaryOfBusinessResults",
}


@dataclass(frozen=True)
class ShareCandidate:
    value_num: float
    source_tag: str
    context_ref: str
    source_method: str
    priority: int


@dataclass(frozen=True)
class BackfillTarget:
    doc_id: str
    edinet_code: str
    security_code: str
    company_name: str
    industry_33: str
    form_type: str
    period_end: str
    fiscal_year: int | None
    accounting_standard: str
    document_display_unit: str
    issued_existing: float | None
    issued_source_tag: str
    treasury_existing: float | None
    treasury_source_tag: str
    outstanding_existing: float | None
    outstanding_status: str
    consolidation: str


@dataclass(frozen=True)
class BackfillAction:
    target: BackfillTarget
    issued: ShareCandidate | None
    treasury: ShareCandidate | None
    value_num: float | None
    calc_status: str
    effective_treasury: float | None
    normalized_inserts: tuple[str, ...]


def _now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _to_number(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        parsed = float(str(value).replace(",", ""))
    except Exception:
        return None
    if parsed < 0:
        return None
    return parsed


def _parse_dimensions(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        return raw
    text = str(raw or "").strip()
    if not text:
        return {}
    try:
        parsed = json.loads(text)
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _has_dimensions(fact: dict[str, Any]) -> bool:
    dims = _parse_dimensions(fact.get("context_dimensions_json"))
    if dims.get("has_scenario") or dims.get("has_segment"):
        return True
    if dims.get("explicit_members") or dims.get("typed_members"):
        return True
    if dims.get("axis_members"):
        return True
    return "_" in str(fact.get("context_ref") or "")


def _context_contains_any(context_ref: str, markers: tuple[str, ...]) -> bool:
    text = str(context_ref or "")
    return any(marker in text for marker in markers)


def _is_current_half_instant(fact: dict[str, Any]) -> bool:
    return _context_contains_any(str(fact.get("context_ref") or ""), HALF_CURRENT_INSTANT_CONTEXT_MARKERS)


def _is_ordinary_share_context(context_ref: str) -> bool:
    return "OrdinaryShare" in str(context_ref or "") or "OrdinaryShares" in str(context_ref or "")


def _is_treasury_member_context(context_ref: str, dimensions_json: Any) -> bool:
    text = f"{context_ref} {json.dumps(_parse_dimensions(dimensions_json), ensure_ascii=False)}"
    return "TreasuryShares" in text and "SharesWithFullVotingRights" in text


def _is_row_member_context(context_ref: str) -> bool:
    return "_Row" in str(context_ref or "") and str(context_ref or "").endswith("Member")


def _issued_candidate_priority(fact: dict[str, Any]) -> int | None:
    tag = str(fact.get("tag_name") or "")
    context_ref = str(fact.get("context_ref") or "")
    if tag not in ISSUED_SHARE_TAGS:
        return None

    if tag in ISSUED_FILING_DATE_TAGS:
        if context_ref == "FilingDateInstant":
            return 10
        if context_ref.startswith("FilingDateInstant") and _is_ordinary_share_context(context_ref):
            return 20

    if tag.startswith("TotalNumberOfIssuedShares"):
        if _is_current_half_instant(fact) and not _has_dimensions(fact):
            return 50

    return None


def _select_issued_candidate(facts: list[dict[str, Any]]) -> ShareCandidate | None:
    candidates: list[ShareCandidate] = []
    for fact in facts:
        priority = _issued_candidate_priority(fact)
        if priority is None:
            continue
        value = _to_number(fact.get("value_text"))
        if value is None or value <= 0:
            continue
        candidates.append(
            ShareCandidate(
                value_num=value,
                source_tag=str(fact.get("tag_name") or ""),
                context_ref=str(fact.get("context_ref") or ""),
                source_method="issued_candidate",
                priority=priority,
            )
        )
    if not candidates:
        return None
    # Same-priority duplicates are common. The larger one is usually the total,
    # while smaller rows can be class-specific shares.
    candidates.sort(key=lambda c: (c.priority, -c.value_num, c.source_tag, c.context_ref))
    return candidates[0]


def _direct_treasury_candidates(facts: list[dict[str, Any]]) -> list[ShareCandidate]:
    out: list[ShareCandidate] = []
    for fact in facts:
        tag = str(fact.get("tag_name") or "")
        context_ref = str(fact.get("context_ref") or "")
        value = _to_number(fact.get("value_text"))
        if value is None:
            continue

        if tag in TREASURY_AGGREGATE_TAG_PRIORITY and _is_current_half_instant(fact) and not _is_row_member_context(context_ref):
            if not _has_dimensions(fact):
                priority = 10 + TREASURY_AGGREGATE_TAG_PRIORITY.index(tag)
            else:
                priority = 40 + TREASURY_AGGREGATE_TAG_PRIORITY.index(tag)
            out.append(
                ShareCandidate(
                    value_num=value,
                    source_tag=tag,
                    context_ref=context_ref,
                    source_method="treasury_direct",
                    priority=priority,
                )
            )
        elif tag == "NumberOfSharesIssuedSharesVotingRights" and _is_treasury_member_context(
            context_ref,
            fact.get("context_dimensions_json"),
        ):
            out.append(
                ShareCandidate(
                    value_num=value,
                    source_tag=tag,
                    context_ref=context_ref,
                    source_method="treasury_voting_rights_member",
                    priority=70,
                )
            )
    return out


def _row_sum_treasury_candidates(facts: list[dict[str, Any]]) -> list[ShareCandidate]:
    out: list[ShareCandidate] = []
    for tag_priority, tag in enumerate(TREASURY_AGGREGATE_TAG_PRIORITY, start=0):
        row_values: list[tuple[str, float]] = []
        for fact in facts:
            if str(fact.get("tag_name") or "") != tag:
                continue
            context_ref = str(fact.get("context_ref") or "")
            if not (_is_current_half_instant(fact) and _is_row_member_context(context_ref)):
                continue
            value = _to_number(fact.get("value_text"))
            if value is None:
                continue
            row_values.append((context_ref, value))
        if not row_values:
            continue
        out.append(
            ShareCandidate(
                value_num=sum(value for _, value in row_values),
                source_tag=tag,
                context_ref="+".join(context for context, _ in row_values[:5]),
                source_method=f"treasury_row_sum_{len(row_values)}",
                priority=90 + tag_priority,
            )
        )
    return out


def _select_treasury_candidate(facts: list[dict[str, Any]]) -> ShareCandidate | None:
    candidates = _direct_treasury_candidates(facts)
    candidates.extend(_row_sum_treasury_candidates(facts))
    if not candidates:
        return None
    candidates.sort(key=lambda c: (c.priority, -c.value_num, c.source_tag, c.context_ref))
    return candidates[0]


def _fetch_targets(conn: sqlite3.Connection) -> list[BackfillTarget]:
    rows = conn.execute(
        """
        SELECT
            f.doc_id,
            f.edinet_code,
            COALESCE(f.security_code, im.security_code) AS security_code,
            COALESCE(im.company_name, '') AS company_name,
            COALESCE(im.industry_33, '') AS industry_33,
            f.form_type,
            COALESCE(f.period_end, '') AS period_end,
            f.accounting_standard,
            f.document_display_unit,
            issued.value_num AS issued_existing,
            COALESCE(issued.source_tag, '') AS issued_source_tag,
            treasury.value_num AS treasury_existing,
            COALESCE(treasury.source_tag, '') AS treasury_source_tag,
            outstanding.value_num AS outstanding_existing,
            COALESCE(outstanding.calc_status, '') AS outstanding_status,
            COALESCE(outstanding.consolidation, issued.consolidation, treasury.consolidation, '') AS consolidation
        FROM filings f
        INNER JOIN issuer_master im
            ON im.edinet_code = f.edinet_code
        LEFT JOIN normalized_metrics issued
            ON issued.doc_id = f.doc_id
           AND issued.metric_key = 'IssuedSharesCurrent'
        LEFT JOIN normalized_metrics treasury
            ON treasury.doc_id = f.doc_id
           AND treasury.metric_key = 'TreasurySharesCurrent'
        LEFT JOIN derived_metrics outstanding
            ON outstanding.doc_id = f.doc_id
           AND outstanding.metric_key = 'OutstandingSharesCurrent'
        WHERE f.form_type = '043A00'
          AND (
              issued.doc_id IS NULL
              OR COALESCE(issued.source_tag, '') IN (
                  'NumberOfSharesIssuedSharesVotingRights',
                  'TotalNumberOfIssuedSharesSummaryOfBusinessResults'
              )
              OR outstanding.doc_id IS NULL
              OR COALESCE(outstanding.calc_status, '') <> 'ok'
              OR COALESCE(outstanding.value_num, 1) <= 0
          )
        ORDER BY f.period_end DESC, f.doc_id
        """
    ).fetchall()
    out: list[BackfillTarget] = []
    for row in rows:
        period_end = str(row["period_end"] or "")
        fiscal_year = None
        if len(period_end) >= 4 and period_end[:4].isdigit():
            fiscal_year = int(period_end[:4])
        out.append(
            BackfillTarget(
                doc_id=str(row["doc_id"]),
                edinet_code=str(row["edinet_code"] or ""),
                security_code=str(row["security_code"] or ""),
                company_name=str(row["company_name"] or ""),
                industry_33=str(row["industry_33"] or ""),
                form_type=str(row["form_type"] or ""),
                period_end=period_end,
                fiscal_year=fiscal_year,
                accounting_standard=str(row["accounting_standard"] or ""),
                document_display_unit=str(row["document_display_unit"] or ""),
                issued_existing=row["issued_existing"],
                issued_source_tag=str(row["issued_source_tag"] or ""),
                treasury_existing=row["treasury_existing"],
                treasury_source_tag=str(row["treasury_source_tag"] or ""),
                outstanding_existing=row["outstanding_existing"],
                outstanding_status=str(row["outstanding_status"] or ""),
                consolidation=str(row["consolidation"] or ""),
            )
        )
    return out


def _fetch_candidate_facts(conn: sqlite3.Connection, doc_ids: list[str]) -> dict[str, list[dict[str, Any]]]:
    if not doc_ids:
        return {}
    tag_names = sorted(ISSUED_SHARE_TAGS | TREASURY_SHARE_TAGS)
    by_doc: dict[str, list[dict[str, Any]]] = {doc_id: [] for doc_id in doc_ids}
    for doc_chunk_start in range(0, len(doc_ids), 500):
        doc_chunk = doc_ids[doc_chunk_start : doc_chunk_start + 500]
        doc_placeholders = ",".join("?" for _ in doc_chunk)
        tag_placeholders = ",".join("?" for _ in tag_names)
        rows = conn.execute(
            f"""
            SELECT
                doc_id,
                tag_name,
                context_ref,
                value_text,
                unit_ref,
                period_type,
                period_end,
                instant_date,
                consolidation,
                context_dimensions_json
            FROM raw_facts
            WHERE doc_id IN ({doc_placeholders})
              AND tag_name IN ({tag_placeholders})
              AND value_text IS NOT NULL
              AND TRIM(value_text) <> ''
            """,
            tuple(doc_chunk + tag_names),
        ).fetchall()
        for row in rows:
            by_doc.setdefault(str(row["doc_id"]), []).append(dict(row))
    return by_doc


def _build_actions(targets: list[BackfillTarget], facts_by_doc: dict[str, list[dict[str, Any]]]) -> list[BackfillAction]:
    actions: list[BackfillAction] = []
    for target in targets:
        facts = facts_by_doc.get(target.doc_id, [])
        issued = None
        treasury = None
        normalized_inserts: list[str] = []

        issued_value = target.issued_existing
        replace_issued = target.issued_source_tag in UNSAFE_ISSUED_SOURCE_TAGS
        if issued_value is None or replace_issued:
            issued = _select_issued_candidate(facts)
            if issued is not None:
                issued_value = issued.value_num
                normalized_inserts.append("IssuedSharesCurrent")
            elif replace_issued:
                issued_value = None

        treasury_value = target.treasury_existing
        if treasury_value is None:
            treasury = _select_treasury_candidate(facts)
            if treasury is not None:
                treasury_value = treasury.value_num
                normalized_inserts.append("TreasurySharesCurrent")

        value_num, calc_status, effective_treasury = _outstanding_shares_status(
            issued_shares=issued_value,
            treasury_shares=treasury_value,
        )
        if calc_status != "ok" and issued is None and treasury is None:
            continue
        if calc_status == target.outstanding_status and value_num == target.outstanding_existing and not normalized_inserts:
            continue

        actions.append(
            BackfillAction(
                target=target,
                issued=issued,
                treasury=treasury,
                value_num=value_num,
                calc_status=calc_status,
                effective_treasury=effective_treasury,
                normalized_inserts=tuple(normalized_inserts),
            )
        )
    return actions


def _insert_normalized_candidate(
    conn: sqlite3.Connection,
    *,
    target: BackfillTarget,
    metric_key: str,
    candidate: ShareCandidate,
    now: str,
) -> None:
    conn.execute(
        """
        INSERT INTO normalized_metrics (
            doc_id,
            edinet_code,
            security_code,
            metric_key,
            fiscal_year,
            period_end,
            value_num,
            source_tag,
            consolidation,
            rule_version,
            created_at,
            updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(doc_id, metric_key, period_end)
        DO UPDATE SET
            value_num = excluded.value_num,
            source_tag = excluded.source_tag,
            consolidation = excluded.consolidation,
            rule_version = excluded.rule_version,
            updated_at = excluded.updated_at
        """,
        (
            target.doc_id,
            target.edinet_code,
            target.security_code,
            metric_key,
            target.fiscal_year,
            target.period_end,
            candidate.value_num,
            candidate.source_tag,
            target.consolidation or "Consolidated",
            DEFAULT_RULE_VERSION,
            now,
            now,
        ),
    )


def _upsert_outstanding_derived(conn: sqlite3.Connection, *, action: BackfillAction, now: str) -> None:
    target = action.target
    period_scope, period_key, quarter_type = infer_period_info(target.form_type)
    source_detail = {
        "inputs": {
            "IssuedSharesCurrent": target.issued_existing if action.issued is None else action.issued.value_num,
            "TreasurySharesCurrent": target.treasury_existing if action.treasury is None else action.treasury.value_num,
            "TreasurySharesCurrent_effective": action.effective_treasury,
        },
        "display_formula": "issued_shares - treasury_shares (treat blank or <1000 treasury_shares as 0)",
        "stored_formula": "issued_shares - treasury_shares_effective",
        "calc_status": action.calc_status,
        "document_display_unit": target.document_display_unit,
        "selected_source": "2q_share_backfill",
        "issued_source_tag": action.issued.source_tag if action.issued else "normalized_metrics",
        "issued_context_ref": action.issued.context_ref if action.issued else "",
        "treasury_source_tag": action.treasury.source_tag if action.treasury else "normalized_metrics_or_blank",
        "treasury_context_ref": action.treasury.context_ref if action.treasury else "",
        "treasury_source_method": action.treasury.source_method if action.treasury else "",
    }
    conn.execute(
        """
        INSERT INTO derived_metrics (
            doc_id,
            edinet_code,
            security_code,
            metric_key,
            metric_base,
            metric_group,
            fiscal_year,
            period_end,
            period_scope,
            period_key,
            quarter_type,
            period_offset,
            consolidation,
            accounting_standard,
            document_display_unit,
            value_num,
            value_unit,
            calc_status,
            formula_name,
            source_detail_json,
            rule_version,
            created_at,
            updated_at
        )
        VALUES (?, ?, ?, 'OutstandingSharesCurrent', 'OutstandingShares', 'share', ?, ?, ?, ?, ?, 0, ?, ?, ?, ?, 'shares', ?, 'outstanding_shares', ?, ?, ?, ?)
        ON CONFLICT(doc_id, metric_key, period_end, consolidation)
        DO UPDATE SET
            value_num = excluded.value_num,
            calc_status = excluded.calc_status,
            source_detail_json = excluded.source_detail_json,
            rule_version = excluded.rule_version,
            period_scope = excluded.period_scope,
            period_key = excluded.period_key,
            quarter_type = excluded.quarter_type,
            updated_at = excluded.updated_at
        """,
        (
            target.doc_id,
            target.edinet_code,
            target.security_code,
            target.fiscal_year,
            target.period_end,
            period_scope or "quarter",
            period_key or "actual:2Q",
            quarter_type or "2Q",
            target.consolidation or "Consolidated",
            target.accounting_standard,
            target.document_display_unit,
            action.value_num,
            action.calc_status,
            json.dumps(source_detail, ensure_ascii=False),
            DEFAULT_DERIVED_METRICS_RULE_VERSION,
            now,
            now,
        ),
    )


def _apply_actions(conn: sqlite3.Connection, actions: list[BackfillAction]) -> None:
    now = _now_text()
    with conn:
        for action in actions:
            if action.issued is not None:
                _insert_normalized_candidate(
                    conn,
                    target=action.target,
                    metric_key="IssuedSharesCurrent",
                    candidate=action.issued,
                    now=now,
                )
            if action.treasury is not None:
                _insert_normalized_candidate(
                    conn,
                    target=action.target,
                    metric_key="TreasurySharesCurrent",
                    candidate=action.treasury,
                    now=now,
                )
            _upsert_outstanding_derived(conn, action=action, now=now)


def _status_counts(conn: sqlite3.Connection) -> dict[str, int]:
    rows = conn.execute(
        """
        SELECT COALESCE(dm.calc_status, '(missing_row)') AS status, COUNT(*) AS count
        FROM filings f
        LEFT JOIN derived_metrics dm
            ON dm.doc_id = f.doc_id
           AND dm.metric_key = 'OutstandingSharesCurrent'
        WHERE f.form_type = '043A00'
        GROUP BY COALESCE(dm.calc_status, '(missing_row)')
        """
    ).fetchall()
    return {str(row["status"]): int(row["count"]) for row in rows}


def _write_report(
    *,
    output_dir: Path | None,
    apply: bool,
    target_count: int,
    actions: list[BackfillAction],
    before_counts: dict[str, int],
    after_counts: dict[str, int] | None,
) -> Path | None:
    if output_dir is None:
        return None
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = output_dir / f"backfill_2q_outstanding_shares_{timestamp}.txt"
    ok_before = before_counts.get("ok", 0)
    total_before = sum(before_counts.values())
    ok_after = (after_counts or before_counts).get("ok", 0)
    total_after = sum((after_counts or before_counts).values())
    lines = [
        f"generated_at: {datetime.now().isoformat(timespec='seconds')}",
        f"apply: {int(apply)}",
        f"target_docs: {target_count}",
        f"candidate_actions: {len(actions)}",
        f"ok_before: {ok_before}/{total_before} ({(ok_before / total_before * 100) if total_before else 0:.1f}%)",
        f"ok_after: {ok_after}/{total_after} ({(ok_after / total_after * 100) if total_after else 0:.1f}%)",
        "",
        "status_before:",
    ]
    for status, count in sorted(before_counts.items()):
        lines.append(f"  {status}: {count}")
    if after_counts is not None:
        lines.append("")
        lines.append("status_after:")
        for status, count in sorted(after_counts.items()):
            lines.append(f"  {status}: {count}")
    lines.extend(["", "sample_actions:"])
    for action in actions[:50]:
        target = action.target
        lines.append(
            " | ".join(
                [
                    f"doc_id={target.doc_id}",
                    f"code={target.security_code}",
                    f"name={target.company_name}",
                    f"period_end={target.period_end}",
                    f"issued={action.issued.value_num if action.issued else target.issued_existing}",
                    f"issued_tag={action.issued.source_tag if action.issued else 'existing'}",
                    f"treasury={action.treasury.value_num if action.treasury else target.treasury_existing}",
                    f"treasury_tag={action.treasury.source_tag if action.treasury else 'existing_or_blank'}",
                    f"outstanding={action.value_num}",
                    f"status={action.calc_status}",
                ]
            )
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8-sig")
    return path


def backfill_2q_outstanding_shares(
    conn: sqlite3.Connection,
    *,
    apply: bool = False,
    output_dir: str | Path | None = None,
) -> dict[str, Any]:
    conn.row_factory = sqlite3.Row
    before_counts = _status_counts(conn)
    targets = _fetch_targets(conn)
    facts_by_doc = _fetch_candidate_facts(conn, [target.doc_id for target in targets])
    actions = _build_actions(targets, facts_by_doc)

    if apply:
        _apply_actions(conn, actions)
        after_counts = _status_counts(conn)
    else:
        after_counts = None

    report_path = _write_report(
        output_dir=Path(output_dir) if output_dir else None,
        apply=apply,
        target_count=len(targets),
        actions=actions,
        before_counts=before_counts,
        after_counts=after_counts,
    )

    active_counts = after_counts or before_counts
    total = sum(active_counts.values())
    ok = active_counts.get("ok", 0)
    return {
        "apply": int(apply),
        "target_docs": len(targets),
        "candidate_actions": len(actions),
        "ok_rows": ok,
        "total_rows": total,
        "ok_rate": (ok / total) if total else 0.0,
        "before_counts": before_counts,
        "after_counts": after_counts,
        "report_path": str(report_path) if report_path else "",
    }
