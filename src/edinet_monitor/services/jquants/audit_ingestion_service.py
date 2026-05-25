from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
import sqlite3

from edinet_monitor.services.jquants.audit_mapper import (
    listed_info_from_row,
)
from edinet_monitor.services.jquants.client import JQuantsClient
from edinet_monitor.services.jquants.mapper import normalize_security_code
from edinet_monitor.services.jquants.repository import (
    upsert_listed_info_raw,
)


@dataclass(frozen=True)
class JQuantsListedInfoResult:
    fetched_total: int
    saved_total: int
    output_path: Path | None
    warnings: list[str] = field(default_factory=list)


def _write(path: Path | None, lines: list[str]) -> Path | None:
    if path is None:
        return None
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8-sig")
    return path


def save_jquants_listed_info(
    conn: sqlite3.Connection,
    *,
    client: JQuantsClient,
    date_value: str,
    codes: list[str] | None = None,
    output_dir: str | Path | None = None,
) -> JQuantsListedInfoResult:
    requested_codes = [code.strip() for code in (codes or []) if code.strip()]
    fetched_rows = []
    if requested_codes:
        for code in requested_codes:
            fetched_rows.extend(client.iter_equities_master(date=date_value, code=code))
    else:
        fetched_rows.extend(client.iter_equities_master(date=date_value))

    mapped = [listed_info_from_row(row) for row in fetched_rows]
    saved_total = upsert_listed_info_raw(conn, mapped)
    conn.commit()
    warnings = _listed_info_warnings(conn, date_value)

    output_path = None
    if output_dir is not None:
        output_path = Path(output_dir) / f"jquants_listed_info_{date_value}_{datetime.now():%Y%m%d_%H%M%S}.txt"
        lines = [
            f"generated_at: {datetime.now().isoformat(timespec='seconds')}",
            f"date: {date_value}",
            f"codes: {','.join(requested_codes) if requested_codes else 'all'}",
            f"fetched_total: {len(fetched_rows)}",
            f"saved_total: {saved_total}",
            "",
            "[validation_warnings]",
            *warnings,
        ]
        _write(output_path, lines)
    return JQuantsListedInfoResult(
        fetched_total=len(fetched_rows),
        saved_total=saved_total,
        output_path=output_path,
        warnings=warnings,
    )


def _listed_info_warnings(conn: sqlite3.Connection, date_value: str) -> list[str]:
    conn.row_factory = sqlite3.Row
    warnings: list[str] = []
    rows = conn.execute(
        """
        SELECT
            im.edinet_code,
            im.security_code AS issuer_security_code,
            im.company_name AS issuer_company_name,
            im.market AS issuer_market,
            im.exchange AS issuer_exchange,
            jq.local_code,
            jq.security_code AS jq_security_code,
            jq.company_name AS jq_company_name,
            jq.market_code,
            jq.market_name
        FROM issuer_master im
        LEFT JOIN jquants_listed_info_raw jq
          ON jq.listing_date = ?
         AND (
            jq.security_code = im.security_code
            OR jq.security_code = substr(COALESCE(im.security_code, ''), 1, 4)
            OR substr(jq.local_code, 1, 4) = substr(COALESCE(im.security_code, ''), 1, 4)
         )
        WHERE COALESCE(im.is_listed, 0) = 1
        """,
        (date_value,),
    ).fetchall()
    for row in rows:
        issuer_code = normalize_security_code(row["issuer_security_code"])
        jq_code = normalize_security_code(row["jq_security_code"] or row["local_code"])
        if not jq_code:
            warnings.append(f"missing_in_jquants security_code={issuer_code} edinet_code={row['edinet_code']}")
            continue
        if issuer_code and jq_code and issuer_code != jq_code:
            warnings.append(f"security_code_mismatch issuer={issuer_code} jquants={jq_code} edinet_code={row['edinet_code']}")
        issuer_name = str(row["issuer_company_name"] or "").replace(" ", "")
        jq_name = str(row["jq_company_name"] or "").replace(" ", "")
        if issuer_name and jq_name and issuer_name != jq_name:
            warnings.append(f"company_name_diff security_code={issuer_code} issuer={row['issuer_company_name']} jquants={row['jq_company_name']}")
        if str(row["issuer_exchange"] or "") == "TSE" and not str(row["market_name"] or row["market_code"] or ""):
            warnings.append(f"market_missing security_code={issuer_code} issuer_market={row['issuer_market']}")

    non_common_rows = conn.execute(
        """
        SELECT local_code, company_name, market_name
        FROM jquants_listed_info_raw
        WHERE listing_date = ?
          AND length(COALESCE(local_code, '')) = 5
          AND substr(local_code, 5, 1) <> '0'
        ORDER BY local_code
        LIMIT 100
        """,
        (date_value,),
    ).fetchall()
    for row in non_common_rows:
        warnings.append(f"non_common_stock_candidate local_code={row['local_code']} company_name={row['company_name']} market={row['market_name']}")
    return warnings
