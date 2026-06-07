from __future__ import annotations

from dataclasses import dataclass
import sqlite3
from typing import Any

from edinet_monitor.services.collector.document_filter_service import normalize_form_codes


@dataclass(frozen=True)
class PeriodRankSpec:
    label: str
    rank: int


def normalize_security_code(value: Any) -> str:
    text = str(value or "").strip().replace("-", "")
    if len(text) == 5 and text.endswith("0"):
        return text[:-1]
    return text


def security_code_variants(value: str) -> list[str]:
    code = normalize_security_code(value)
    if not code:
        return []
    variants = {code}
    if len(code) == 4:
        variants.add(f"{code}0")
    if len(code) == 5 and code.endswith("0"):
        variants.add(code[:-1])
    return sorted(variants)


def parse_period_rank_specs(value: str | list[str] | tuple[str, ...] | None) -> list[PeriodRankSpec]:
    if value is None or value == "":
        raw_items = ["latest", "5", "10"]
    elif isinstance(value, str):
        raw_items = [part.strip() for part in value.split(",")]
    else:
        raw_items = [str(part).strip() for part in value]

    specs: list[PeriodRankSpec] = []
    seen: set[int] = set()
    for item in raw_items:
        text = str(item or "").strip().lower()
        if not text:
            continue
        if text in {"recent3", "latest3", "recent_3", "latest_3"}:
            for spec in (
                PeriodRankSpec("latest", 1),
                PeriodRankSpec("1_prior", 2),
                PeriodRankSpec("2_prior", 3),
            ):
                if spec.rank not in seen:
                    specs.append(spec)
                    seen.add(spec.rank)
            continue
        if text in {"latest", "current", "当期", "最新"}:
            spec = PeriodRankSpec("latest", 1)
        elif text in {"5", "5期前", "5_prior", "prior5"}:
            spec = PeriodRankSpec("5_prior", 6)
        elif text in {"10", "10期前", "10_prior", "prior10"}:
            spec = PeriodRankSpec("10_prior", 11)
        elif text.isdigit():
            number = int(text)
            spec = PeriodRankSpec(f"rank_{number}", number)
        else:
            raise ValueError(f"Unsupported period rank: {item}")
        if spec.rank not in seen:
            specs.append(spec)
            seen.add(spec.rank)
    return specs


def _form_group_sql() -> str:
    return "CASE WHEN f.form_type IN ('043A00', '043000') THEN '043A00' ELSE f.form_type END"


def fetch_segment_scope_filings(
    conn: sqlite3.Connection,
    *,
    form_codes: str | list[str] | tuple[str, ...] | None = None,
    period_ranks: str | list[str] | tuple[str, ...] | None = None,
    codes: list[str] | tuple[str, ...] | None = None,
    doc_ids: list[str] | tuple[str, ...] | None = None,
) -> list[dict[str, Any]]:
    target_form_codes = normalize_form_codes(form_codes)
    if not target_form_codes:
        target_form_codes = ("030000", "043A00")
    normalized_doc_ids = [str(doc_id).strip() for doc_id in (doc_ids or []) if str(doc_id).strip()]

    if normalized_doc_ids:
        placeholders = ",".join("?" for _ in normalized_doc_ids)
        form_filter = ""
        params: list[Any] = list(normalized_doc_ids)
        if target_form_codes:
            form_placeholders = ",".join("?" for _ in target_form_codes)
            form_filter = f" AND f.form_type IN ({form_placeholders})"
            params.extend(target_form_codes)
        rows = conn.execute(
            f"""
            SELECT
                f.doc_id,
                f.edinet_code,
                COALESCE(NULLIF(f.security_code, ''), im.security_code, '') AS security_code,
                im.company_name,
                im.industry_33,
                im.market,
                f.form_type,
                f.period_end,
                f.submit_date,
                f.zip_path,
                f.xbrl_path,
                f.xbrl_member_name,
                f.download_status,
                f.parse_status,
                0 AS period_rank,
                'doc_id' AS period_rank_label
            FROM filings f
            LEFT JOIN issuer_master im
              ON im.edinet_code = f.edinet_code
            WHERE f.doc_id IN ({placeholders})
              {form_filter}
            ORDER BY f.form_type, f.period_end DESC, f.doc_id
            """,
            params,
        ).fetchall()
        return [dict(row) for row in rows]

    specs = parse_period_rank_specs(period_ranks)
    rank_by_number = {spec.rank: spec.label for spec in specs}
    rank_numbers = [spec.rank for spec in specs]
    if not rank_numbers:
        return []

    form_placeholders = ",".join("?" for _ in target_form_codes)
    rank_placeholders = ",".join("?" for _ in rank_numbers)
    params = list(target_form_codes)

    code_filter = ""
    code_variants: list[str] = []
    for code in codes or []:
        code_variants.extend(security_code_variants(str(code)))
    code_variants = sorted(set(code_variants))
    if code_variants:
        code_placeholders = ",".join("?" for _ in code_variants)
        code_filter = (
            f" AND (COALESCE(NULLIF(f.security_code, ''), im.security_code, '') IN ({code_placeholders}) "
            f"OR substr(COALESCE(NULLIF(f.security_code, ''), im.security_code, ''), 1, 4) IN ({code_placeholders}))"
        )
        params.extend(code_variants)
        params.extend([normalize_security_code(code) for code in code_variants])
    params.extend(rank_numbers)

    rows = conn.execute(
        f"""
        WITH ranked AS (
            SELECT
                f.doc_id,
                f.edinet_code,
                COALESCE(NULLIF(f.security_code, ''), im.security_code, '') AS security_code,
                im.company_name,
                im.industry_33,
                im.market,
                f.form_type,
                {_form_group_sql()} AS form_group,
                f.period_end,
                f.submit_date,
                f.zip_path,
                f.xbrl_path,
                f.xbrl_member_name,
                f.download_status,
                f.parse_status,
                ROW_NUMBER() OVER (
                    PARTITION BY f.edinet_code, {_form_group_sql()}
                    ORDER BY COALESCE(f.period_end, '') DESC,
                             COALESCE(f.submit_date, '') DESC,
                             f.doc_id DESC
                ) AS period_rank
            FROM filings f
            INNER JOIN issuer_master im
              ON im.edinet_code = f.edinet_code
            WHERE f.form_type IN ({form_placeholders})
              AND COALESCE(im.is_listed, 0) = 1
              AND COALESCE(im.exchange, '') = 'TSE'
              {code_filter}
        )
        SELECT *
        FROM ranked
        WHERE period_rank IN ({rank_placeholders})
        ORDER BY form_group, period_rank, security_code, period_end
        """,
        params,
    ).fetchall()

    out: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        item["period_rank_label"] = rank_by_number.get(int(item.get("period_rank") or 0), "")
        out.append(item)
    return out
