from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
import csv
import io
import json
import sqlite3
from typing import Any

from edinet_monitor.services.jquants.mapper import normalize_security_code


RAPID_CHANGE_BASES = {
    "EPS",
    "BPS",
    "NetSales",
    "OperatingIncome",
    "OrdinaryIncome",
    "ProfitLoss",
}
MARKET_AUDIT_BASES = {
    "StockPrice",
    "MarketCapitalization",
    "StockPriceGrowthRate",
    "PBR",
    "PER",
}


@dataclass(frozen=True)
class JQuantsQualityAuditResult:
    output_path: Path
    tsv_path: Path
    issue_count: int
    counts_by_severity: dict[str, int]


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type IN ('table', 'view') AND name = ?",
        (table_name,),
    ).fetchone()
    return row is not None


def _to_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_json(text: Any) -> dict[str, Any]:
    if not text:
        return {}
    try:
        value = json.loads(str(text))
    except Exception:
        return {}
    return value if isinstance(value, dict) else {}


def _split_codes(codes: list[str] | tuple[str, ...] | None) -> list[str]:
    return [normalize_security_code(code) for code in (codes or []) if normalize_security_code(code)]


def _current_listing_statuses(
    conn: sqlite3.Connection,
    security_codes: set[str],
) -> dict[str, bool]:
    clean_codes = {normalize_security_code(code) for code in security_codes if normalize_security_code(code)}
    if not clean_codes:
        return {}
    if not _table_exists(conn, "issuer_master"):
        return {code: True for code in clean_codes}

    rows = conn.execute(
        """
        SELECT security_code, is_listed
        FROM issuer_master
        WHERE COALESCE(security_code, '') <> ''
        """
    ).fetchall()
    if not rows:
        return {code: True for code in clean_codes}

    listed_codes = {
        normalize_security_code(row["security_code"])
        for row in rows
        if normalize_security_code(row["security_code"]) and int(row["is_listed"] or 0) == 1
    }
    return {code: code in listed_codes for code in clean_codes}


def _issue(
    *,
    severity: str,
    check_name: str,
    security_code: str,
    fiscal_year: int | None,
    period_key: str,
    metric_base: str,
    message: str,
    value_num: float | None = None,
    reference_value: float | None = None,
    disclosure_number: str = "",
    is_currently_listed: bool | None = None,
) -> dict[str, Any]:
    issue = {
        "severity": severity,
        "check_name": check_name,
        "security_code": security_code,
        "fiscal_year": "" if fiscal_year is None else fiscal_year,
        "period_key": period_key,
        "metric_base": metric_base,
        "value_num": "" if value_num is None else value_num,
        "reference_value": "" if reference_value is None else reference_value,
        "disclosure_number": disclosure_number,
        "message": message,
    }
    if is_currently_listed is not None:
        issue["is_currently_listed"] = int(is_currently_listed)
    return issue


def build_jquants_quality_issues(
    conn: sqlite3.Connection,
    *,
    date_from: str,
    date_to: str,
    codes: list[str] | None = None,
) -> list[dict[str, Any]]:
    conn.row_factory = sqlite3.Row
    normalized_codes = _split_codes(codes)
    issues: list[dict[str, Any]] = []
    if not _table_exists(conn, "active_latest_jquants_metrics"):
        return [
            _issue(
                severity="critical",
                check_name="active_latest_view_missing",
                security_code="",
                fiscal_year=None,
                period_key="",
                metric_base="",
                message="active_latest_jquants_metrics view does not exist",
            )
        ]

    rows = _fetch_active_rows(conn, date_from=date_from, date_to=date_to, codes=normalized_codes)
    by_period: dict[tuple[str, str, int, str], dict[str, sqlite3.Row]] = {}
    by_metric_history: dict[tuple[str, str, str], list[sqlite3.Row]] = {}
    for row in rows:
        fiscal_year = row["fiscal_year"]
        if fiscal_year is None:
            continue
        security_code = normalize_security_code(row["normalized_security_code"] or row["security_code"] or row["local_code"])
        period_key = str(row["period_key"] or "")
        forecast_stage = str(row["forecast_stage"] or "")
        metric_base = str(row["metric_base"] or "")
        by_period.setdefault((security_code, period_key, int(fiscal_year), forecast_stage), {})[metric_base] = row
        by_metric_history.setdefault((security_code, period_key, metric_base), []).append(row)

    listing_statuses = _current_listing_statuses(
        conn,
        {security_code for security_code, _, _, _ in by_period},
    )
    for (security_code, period_key, fiscal_year, forecast_stage), metrics in by_period.items():
        issues.extend(
            _period_issues(
                security_code,
                period_key,
                fiscal_year,
                metrics,
                is_currently_listed=listing_statuses.get(security_code, True),
            )
        )

    for key, history in by_metric_history.items():
        issues.extend(_history_issues(key, history))

    issues.extend(_market_issues(conn, date_from=date_from, date_to=date_to, codes=normalized_codes))
    severity_order = {"critical": 0, "warning": 1, "info": 2}
    issues.sort(
        key=lambda item: (
            severity_order.get(str(item["severity"]), 99),
            str(item["security_code"]),
            str(item["period_key"]),
            str(item["metric_base"]),
            str(item["check_name"]),
        )
    )
    return issues


def _fetch_active_rows(
    conn: sqlite3.Connection,
    *,
    date_from: str,
    date_to: str,
    codes: list[str],
) -> list[sqlite3.Row]:
    where = ["COALESCE(period_end, disclosed_date, '') BETWEEN ? AND ?"]
    params: list[Any] = [date_from, date_to]
    if codes:
        placeholders = ",".join("?" for _ in codes)
        where.append(
            f"(normalized_security_code IN ({placeholders}) OR security_code IN ({placeholders}) OR substr(local_code, 1, 4) IN ({placeholders}))"
        )
        params.extend(codes)
        params.extend(codes)
        params.extend(codes)
    return conn.execute(
        f"""
        SELECT *
        FROM active_latest_jquants_metrics
        WHERE {" AND ".join(where)}
        """,
        params,
    ).fetchall()


def _ok_value(row: sqlite3.Row | None) -> float | None:
    if row is None or str(row["calc_status"] or "") != "ok":
        return None
    return _to_float(row["value_num"])


def _period_issues(
    security_code: str,
    period_key: str,
    fiscal_year: int,
    metrics: dict[str, sqlite3.Row],
    *,
    is_currently_listed: bool = True,
) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    issued = _ok_value(metrics.get("IssuedShares"))
    treasury = _ok_value(metrics.get("TreasuryShares"))
    outstanding = _ok_value(metrics.get("OutstandingShares"))
    disclosure = str(next(iter(metrics.values()))["disclosure_number"] or "") if metrics else ""
    if outstanding is not None and outstanding <= 0:
        issues.append(_issue(severity="critical", check_name="outstanding_shares_non_positive", security_code=security_code, fiscal_year=fiscal_year, period_key=period_key, metric_base="OutstandingShares", value_num=outstanding, disclosure_number=disclosure, message="OutstandingShares is <= 0"))
    if issued is not None and treasury is not None and issued <= treasury:
        issues.append(_issue(severity="warning", check_name="issued_shares_not_greater_than_treasury", security_code=security_code, fiscal_year=fiscal_year, period_key=period_key, metric_base="IssuedShares", value_num=issued, reference_value=treasury, disclosure_number=disclosure, message="IssuedShares is <= TreasuryShares; OutstandingShares is not treated as ok when this makes it non-positive"))
    equity_ratio = _ok_value(metrics.get("EquityRatio"))
    if equity_ratio is not None and equity_ratio > 1.5:
        severity = "critical" if is_currently_listed else "warning"
        message = "EquityRatio exceeds expected scale"
        if not is_currently_listed:
            message += " for non-currently-listed issuer"
        issues.append(_issue(severity=severity, check_name="equity_ratio_scale_or_range", security_code=security_code, fiscal_year=fiscal_year, period_key=period_key, metric_base="EquityRatio", value_num=equity_ratio, disclosure_number=disclosure, message=message, is_currently_listed=is_currently_listed))
    if _ok_value(metrics.get("NetSales")) is not None and all(_ok_value(metrics.get(base)) is None for base in ("OperatingIncome", "OrdinaryIncome", "ProfitLoss")):
        issues.append(_issue(severity="warning", check_name="sales_exists_profit_metrics_missing", security_code=security_code, fiscal_year=fiscal_year, period_key=period_key, metric_base="NetSales", value_num=_ok_value(metrics.get("NetSales")), disclosure_number=disclosure, message="NetSales exists but OperatingIncome, OrdinaryIncome, and ProfitLoss are all missing"))
    ordinary_row = metrics.get("OrdinaryIncome")
    if ordinary_row is not None:
        detail = _safe_json(ordinary_row["source_detail_json"])
        if detail.get("semantic_status") == "proxy":
            issues.append(_issue(severity="warning", check_name="ordinary_income_proxy", security_code=security_code, fiscal_year=fiscal_year, period_key=period_key, metric_base="OrdinaryIncome", value_num=_to_float(ordinary_row["value_num"]), disclosure_number=str(ordinary_row["disclosure_number"] or ""), message=f"OrdinaryIncome uses proxy field {ordinary_row['source_field']}"))
    return issues


def _history_issues(
    key: tuple[str, str, str],
    history: list[sqlite3.Row],
) -> list[dict[str, Any]]:
    security_code, period_key, metric_base = key
    rows = sorted(
        [row for row in history if row["fiscal_year"] is not None],
        key=lambda row: int(row["fiscal_year"]),
    )
    by_year = {int(row["fiscal_year"]): row for row in rows}
    issues: list[dict[str, Any]] = []
    if not rows:
        return issues
    latest = rows[-1]
    latest_year = int(latest["fiscal_year"])
    prev = by_year.get(latest_year - 1)
    if prev is not None and str(latest["calc_status"] or "") != "ok" and str(prev["calc_status"] or "") == "ok":
        issues.append(_issue(severity="warning", check_name="latest_missing_prior_year_ok", security_code=security_code, fiscal_year=latest_year, period_key=period_key, metric_base=metric_base, reference_value=_to_float(prev["value_num"]), disclosure_number=str(latest["disclosure_number"] or ""), message="latest row is not ok while prior fiscal year is ok"))
    if metric_base not in RAPID_CHANGE_BASES:
        return issues
    for current in rows:
        year = int(current["fiscal_year"])
        prior = by_year.get(year - 1)
        current_value = _ok_value(current)
        prior_value = _ok_value(prior)
        if current_value is None or prior_value is None:
            continue
        if prior_value > 0 and current_value > 0:
            ratio = current_value / prior_value
            if ratio > 3 or ratio < (1 / 3):
                issues.append(_issue(severity="warning", check_name="rapid_yoy_change", security_code=security_code, fiscal_year=year, period_key=period_key, metric_base=metric_base, value_num=current_value, reference_value=prior_value, disclosure_number=str(current["disclosure_number"] or ""), message=f"year-over-year ratio={ratio:.4g}"))
        elif prior_value * current_value < 0:
            issues.append(_issue(severity="info", check_name="metric_sign_change", security_code=security_code, fiscal_year=year, period_key=period_key, metric_base=metric_base, value_num=current_value, reference_value=prior_value, disclosure_number=str(current["disclosure_number"] or ""), message="metric sign changed from prior fiscal year"))
    return issues


def _market_issues(
    conn: sqlite3.Connection,
    *,
    date_from: str,
    date_to: str,
    codes: list[str],
) -> list[dict[str, Any]]:
    if not _table_exists(conn, "market_derived_metrics"):
        return []
    where = [
        "source_type = 'jquants'",
        "period_end BETWEEN ? AND ?",
        f"metric_base IN ({','.join('?' for _ in MARKET_AUDIT_BASES)})",
    ]
    params: list[Any] = [date_from, date_to, *sorted(MARKET_AUDIT_BASES)]
    if codes:
        placeholders = ",".join("?" for _ in codes)
        where.append(f"security_code IN ({placeholders})")
        params.extend(codes)
    rows = conn.execute(
        f"""
        SELECT *
        FROM market_derived_metrics
        WHERE {" AND ".join(where)}
        """,
        params,
    ).fetchall()
    issues = []
    for row in rows:
        if str(row["calc_status"] or "") != "ok":
            issues.append(_issue(severity="warning", check_name="market_metric_not_ok", security_code=str(row["security_code"] or ""), fiscal_year=row["fiscal_year"], period_key=str(row["period_key"] or ""), metric_base=str(row["metric_base"] or ""), disclosure_number=str(row["source_id"] or ""), message=f"market_derived_metrics calc_status={row['calc_status']}"))
    return issues


def export_jquants_quality_audit(
    conn: sqlite3.Connection,
    *,
    date_from: str,
    date_to: str,
    codes: list[str] | None = None,
    output_dir: str | Path,
) -> JQuantsQualityAuditResult:
    issues = build_jquants_quality_issues(conn, date_from=date_from, date_to=date_to, codes=codes)
    output_root = Path(output_dir)
    output_root.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    txt_path = output_root / f"jquants_quality_audit_{date_from}_to_{date_to}_{timestamp}.txt"
    tsv_path = output_root / f"jquants_quality_audit_{date_from}_to_{date_to}_{timestamp}.tsv"
    counts: dict[str, int] = {}
    for issue in issues:
        severity = str(issue["severity"])
        counts[severity] = counts.get(severity, 0) + 1

    lines = [
        f"generated_at: {datetime.now().isoformat(timespec='seconds')}",
        f"date_from: {date_from}",
        f"date_to: {date_to}",
        f"codes: {','.join(codes or []) if codes else 'all'}",
        f"issue_count: {len(issues)}",
        "",
        "[severity]",
        *[f"{key}: {value}" for key, value in sorted(counts.items())],
        "",
        "[issues]",
    ]
    for issue in issues[:500]:
        lines.append(
            f"{issue['severity']} | {issue['check_name']} | {issue['security_code']} | "
            f"{issue['period_key']} | {issue['fiscal_year']} | {issue['metric_base']} | {issue['message']}"
        )
    if len(issues) > 500:
        lines.append(f"... truncated {len(issues) - 500} issues; see TSV")
    txt_path.write_text("\n".join(lines) + "\n", encoding="utf-8-sig")

    headers = [
        "severity",
        "check_name",
        "security_code",
        "fiscal_year",
        "period_key",
        "metric_base",
        "value_num",
        "reference_value",
        "disclosure_number",
        "message",
    ]
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=headers, delimiter="\t", lineterminator="\n")
    writer.writeheader()
    for issue in issues:
        writer.writerow({key: issue.get(key, "") for key in headers})
    tsv_path.write_text(buffer.getvalue(), encoding="utf-8-sig")
    return JQuantsQualityAuditResult(
        output_path=txt_path,
        tsv_path=tsv_path,
        issue_count=len(issues),
        counts_by_severity=counts,
    )
