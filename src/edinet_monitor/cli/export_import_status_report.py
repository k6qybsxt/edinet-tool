from __future__ import annotations

import argparse
import os
import sqlite3
import unicodedata
from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any

from edinet_monitor.config.settings import DB_PATH


DEFAULT_OUTPUT_DIR = Path(r"D:\作業用")


@dataclass(frozen=True)
class ReportFilters:
    period_mode: str
    period_from: str
    period_to: str
    industry_33_list: list[str]
    security_codes: list[str]
    status_list: list[str]
    output_dir: Path
    db_path: Path


def _security_code_variants(security_code: str) -> list[str]:
    code = str(security_code or "").strip()
    if not code:
        return []
    variants = {code}
    if len(code) == 4:
        variants.add(f"{code}0")
    if len(code) == 5 and code.endswith("0"):
        variants.add(code[:-1])
    return sorted(variants)


def _parse_multi_values(values: list[str]) -> list[str]:
    items: list[str] = []
    for value in values:
        for chunk in str(value or "").split(","):
            text = chunk.strip()
            if text:
                items.append(text)
    lowered = {item.lower() for item in items}
    if not items or "all" in lowered:
        return []
    return sorted(dict.fromkeys(items))


def _parse_filters(args: argparse.Namespace) -> ReportFilters:
    period_mode = str(args.period_mode or "all").strip().lower()
    if period_mode not in {"all", "range"}:
        raise ValueError("--period-mode must be 'all' or 'range'.")

    period_from = str(args.period_from or "").strip()
    period_to = str(args.period_to or "").strip()

    if period_mode == "range":
        if not period_from or not period_to:
            raise ValueError("--period-from and --period-to are required when --period-mode range.")
        start_date = date.fromisoformat(period_from)
        end_date = date.fromisoformat(period_to)
        if start_date > end_date:
            raise ValueError("--period-from must be earlier than or equal to --period-to.")

    return ReportFilters(
        period_mode=period_mode,
        period_from=period_from,
        period_to=period_to,
        industry_33_list=_parse_multi_values(args.industry_33_list),
        security_codes=_parse_multi_values(args.security_codes),
        status_list=[item.upper() for item in _parse_multi_values(args.status_list)],
        output_dir=Path(str(args.output_dir or DEFAULT_OUTPUT_DIR)),
        db_path=Path(str(args.db_path or DB_PATH)),
    )


def _connect_readonly(db_path: Path) -> sqlite3.Connection:
    return sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)


def _build_scope_where(filters: ReportFilters) -> tuple[str, list[Any]]:
    clauses = ["f.form_type = '030000'"]
    params: list[Any] = []

    if filters.period_mode == "range":
        clauses.append("f.period_end BETWEEN ? AND ?")
        params.extend([filters.period_from, filters.period_to])

    if filters.industry_33_list:
        placeholders = ",".join("?" for _ in filters.industry_33_list)
        clauses.append(f"COALESCE(im.industry_33, '') IN ({placeholders})")
        params.extend(filters.industry_33_list)

    if filters.security_codes:
        variants: list[str] = []
        for code in filters.security_codes:
            variants.extend(_security_code_variants(code))
        variants = sorted(set(variants))
        placeholders = ",".join("?" for _ in variants)
        clauses.append(f"COALESCE(NULLIF(f.security_code, ''), NULLIF(im.security_code, '')) IN ({placeholders})")
        params.extend(variants)

    return " AND ".join(clauses), params


def fetch_actual_status_rows(conn: sqlite3.Connection, filters: ReportFilters) -> list[dict[str, Any]]:
    conn.row_factory = sqlite3.Row
    where_sql, params = _build_scope_where(filters)
    sql = f"""
    WITH scope AS (
        SELECT
            f.doc_id,
            f.edinet_code,
            COALESCE(NULLIF(f.security_code, ''), NULLIF(im.security_code, '')) AS security_code,
            COALESCE(im.company_name, '') AS company_name,
            COALESCE(im.industry_33, '') AS industry_33,
            COALESCE(f.period_end, '') AS period_end,
            COALESCE(f.submit_date, '') AS submit_date,
            COALESCE(f.download_status, '') AS download_status,
            COALESCE(f.parse_status, '') AS parse_status,
            COALESCE(f.zip_path, '') AS zip_path,
            COALESCE(f.xbrl_path, '') AS xbrl_path
        FROM filings f
        LEFT JOIN issuer_master im
          ON im.edinet_code = f.edinet_code
        WHERE {where_sql}
    ),
    normalized_agg AS (
        SELECT n.doc_id, COUNT(*) AS normalized_count
        FROM normalized_metrics n
        JOIN scope s
          ON s.doc_id = n.doc_id
        GROUP BY n.doc_id
    ),
    derived_agg AS (
        SELECT d.doc_id, COUNT(*) AS derived_count
        FROM derived_metrics d
        JOIN scope s
          ON s.doc_id = d.doc_id
        GROUP BY d.doc_id
    )
    SELECT
        s.doc_id,
        s.edinet_code,
        s.security_code,
        s.company_name,
        s.industry_33,
        s.period_end,
        s.submit_date,
        s.download_status,
        s.parse_status,
        s.zip_path,
        s.xbrl_path,
        COALESCE(n.normalized_count, 0) AS normalized_count,
        COALESCE(d.derived_count, 0) AS derived_count
    FROM scope s
    LEFT JOIN normalized_agg n
      ON n.doc_id = s.doc_id
    LEFT JOIN derived_agg d
      ON d.doc_id = s.doc_id
    ORDER BY
        COALESCE(s.security_code, ''),
        COALESCE(s.period_end, '') DESC,
        COALESCE(s.submit_date, '') DESC,
        COALESCE(s.doc_id, '')
    """
    return [dict(row) for row in conn.execute(sql, params).fetchall()]


def _fetch_target_issuers(conn: sqlite3.Connection, filters: ReportFilters) -> list[dict[str, Any]]:
    conn.row_factory = sqlite3.Row
    clauses = ["1 = 1"]
    params: list[Any] = []

    if filters.industry_33_list:
        placeholders = ",".join("?" for _ in filters.industry_33_list)
        clauses.append(f"COALESCE(industry_33, '') IN ({placeholders})")
        params.extend(filters.industry_33_list)

    if filters.security_codes:
        variants: list[str] = []
        for code in filters.security_codes:
            variants.extend(_security_code_variants(code))
        variants = sorted(set(variants))
        placeholders = ",".join("?" for _ in variants)
        clauses.append(f"COALESCE(security_code, '') IN ({placeholders})")
        params.extend(variants)

    sql = f"""
    SELECT
        edinet_code,
        COALESCE(security_code, '') AS security_code,
        COALESCE(company_name, '') AS company_name,
        COALESCE(industry_33, '') AS industry_33
    FROM issuer_master
    WHERE {" AND ".join(clauses)}
    ORDER BY COALESCE(security_code, ''), edinet_code
    """
    return [dict(row) for row in conn.execute(sql, params).fetchall()]


def _fetch_target_issuer_specs(conn: sqlite3.Connection, filters: ReportFilters) -> list[dict[str, Any]]:
    conn.row_factory = sqlite3.Row
    clauses = ["f.form_type = '030000'", "COALESCE(f.period_end, '') <> ''"]
    params: list[Any] = []

    if filters.industry_33_list:
        placeholders = ",".join("?" for _ in filters.industry_33_list)
        clauses.append(f"COALESCE(im.industry_33, '') IN ({placeholders})")
        params.extend(filters.industry_33_list)

    if filters.security_codes:
        variants: list[str] = []
        for code in filters.security_codes:
            variants.extend(_security_code_variants(code))
        variants = sorted(set(variants))
        placeholders = ",".join("?" for _ in variants)
        clauses.append(f"COALESCE(NULLIF(f.security_code, ''), NULLIF(im.security_code, '')) IN ({placeholders})")
        params.extend(variants)

    sql = f"""
    SELECT
        f.edinet_code,
        COALESCE(NULLIF(im.security_code, ''), NULLIF(f.security_code, '')) AS security_code,
        COALESCE(im.company_name, '') AS company_name,
        COALESCE(im.industry_33, '') AS industry_33,
        MIN(f.period_end) AS min_period_end,
        MAX(f.period_end) AS max_period_end
    FROM filings f
    LEFT JOIN issuer_master im
      ON im.edinet_code = f.edinet_code
    WHERE {" AND ".join(clauses)}
    GROUP BY
        f.edinet_code,
        COALESCE(NULLIF(im.security_code, ''), NULLIF(f.security_code, '')),
        COALESCE(im.company_name, ''),
        COALESCE(im.industry_33, '')
    ORDER BY COALESCE(NULLIF(im.security_code, ''), NULLIF(f.security_code, '')), f.edinet_code
    """
    return [dict(row) for row in conn.execute(sql, params).fetchall()]


def _infer_fiscal_month_day(conn: sqlite3.Connection, edinet_code: str) -> str:
    row = conn.execute(
        """
        SELECT period_end
        FROM filings
        WHERE edinet_code = ?
          AND form_type = '030000'
          AND COALESCE(period_end, '') <> ''
        ORDER BY period_end DESC, submit_date DESC, doc_id DESC
        LIMIT 1
        """,
        (edinet_code,),
    ).fetchone()
    if not row or not row[0]:
        return ""
    return str(row[0])[5:10]


def _annual_period_ends(period_from: str, period_to: str, month_day: str) -> list[str]:
    start_date = date.fromisoformat(period_from)
    end_date = date.fromisoformat(period_to)
    month = int(month_day[:2])
    day = int(month_day[3:5])

    period_ends: list[str] = []
    for year in range(start_date.year, end_date.year + 1):
        try:
            candidate = date(year, month, day)
        except ValueError:
            continue
        if start_date <= candidate <= end_date:
            period_ends.append(candidate.isoformat())
    return period_ends


def _month_day_for_gap_detection(period_end: str) -> str:
    text = str(period_end or "").strip()
    if len(text) < 10:
        return ""
    month_day = text[5:10]
    if month_day == "02-29":
        return "02-28"
    return month_day


def _leap_year_actual_period_end(period_end: str, month_day: str) -> str:
    if month_day != "02-28":
        return ""

    try:
        target_date = date.fromisoformat(period_end)
        leap_candidate = date(target_date.year, 2, 29)
    except ValueError:
        return ""

    if target_date.month == 2 and target_date.day == 28:
        return leap_candidate.isoformat()
    return ""


def _period_end_gap_specs_for_issuer(
    actual_rows: list[dict[str, Any]],
) -> list[tuple[str, str, str]]:
    by_month_day: dict[str, list[str]] = {}
    for row in actual_rows:
        period_end = str(row.get("period_end") or "")
        month_day = _month_day_for_gap_detection(period_end)
        if not month_day:
            continue
        by_month_day.setdefault(month_day, []).append(period_end)

    specs: list[tuple[str, str, str]] = []
    for month_day, period_ends in by_month_day.items():
        unique_period_ends = sorted(set(period_ends))
        if len(unique_period_ends) < 2:
            continue
        years = sorted({int(period_end[:4]) for period_end in unique_period_ends if len(period_end) >= 4})
        if not years:
            continue
        year_span = years[-1] - years[0] + 1
        if year_span <= 0 or (len(years) / year_span) < 0.5:
            continue
        specs.append((month_day, unique_period_ends[0], unique_period_ends[-1]))

    return sorted(specs, key=lambda item: (item[2], item[0]), reverse=True)


def _build_status_label(row: dict[str, Any]) -> str:
    if not row.get("doc_id"):
        return "FILING_MISSING"
    if int(row.get("derived_count") or 0) > 0:
        return "OK"
    if int(row.get("normalized_count") or 0) > 0:
        return "NORMALIZED_ONLY"
    parse_status = str(row.get("parse_status") or "")
    if parse_status.endswith("_error"):
        return parse_status.upper()
    if parse_status:
        return parse_status.upper()
    return "PENDING"


def _display_security_code(security_code: str) -> str:
    text = str(security_code or "").strip()
    if len(text) == 5 and text.endswith("0"):
        return text[:-1]
    return text


def _decorate_row(row: dict[str, Any]) -> dict[str, Any]:
    zip_path = str(row.get("zip_path") or "")
    zip_exists = bool(zip_path) and Path(zip_path).exists()
    filing_exists = bool(str(row.get("doc_id") or "").strip())
    normalized_count = int(row.get("normalized_count") or 0)
    derived_count = int(row.get("derived_count") or 0)

    return {
        "security_code": _display_security_code(str(row.get("security_code") or "")),
        "company_name": str(row.get("company_name") or ""),
        "industry_33": str(row.get("industry_33") or ""),
        "period_end": str(row.get("period_end") or ""),
        "doc_id": str(row.get("doc_id") or ""),
        "submit_date": str(row.get("submit_date") or ""),
        "filing": "Y" if filing_exists else "N",
        "zip": "Y" if zip_exists else "N",
        "normalized": normalized_count,
        "derived": derived_count,
        "parse_status": str(row.get("parse_status") or ""),
        "status": _build_status_label(row),
    }


def expand_with_annual_gaps(
    conn: sqlite3.Connection,
    filters: ReportFilters,
    actual_rows: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], bool]:
    should_expand = filters.period_mode == "range" or "FILING_MISSING" in set(filters.status_list)
    if not should_expand:
        return [_decorate_row(row) for row in actual_rows], False

    issuer_specs = _fetch_target_issuer_specs(conn, filters)
    if not issuer_specs:
        return [], True

    actual_by_key = {
        (str(row.get("edinet_code") or ""), str(row.get("period_end") or "")): row
        for row in actual_rows
    }
    actual_by_issuer: dict[str, list[dict[str, Any]]] = {}
    for row in actual_rows:
        actual_by_issuer.setdefault(str(row.get("edinet_code") or ""), []).append(row)
    out_rows: list[dict[str, Any]] = []

    for issuer in issuer_specs:
        edinet_code = str(issuer.get("edinet_code") or "")
        max_period_end = str(issuer.get("max_period_end") or "")
        min_period_end = str(issuer.get("min_period_end") or "")
        if not max_period_end or not min_period_end:
            continue

        if filters.period_mode == "range":
            gap_specs = [(max_period_end[5:10], filters.period_from, filters.period_to)]
        else:
            gap_specs = _period_end_gap_specs_for_issuer(actual_by_issuer.get(edinet_code, []))
            if not gap_specs:
                gap_specs = [(max_period_end[5:10], min_period_end, max_period_end)]

        for month_day, range_from, range_to in gap_specs:
            for period_end in _annual_period_ends(range_from, range_to, month_day):
                actual = actual_by_key.get((edinet_code, period_end))
                if actual is None:
                    leap_period_end = _leap_year_actual_period_end(period_end, month_day)
                    if leap_period_end:
                        actual = actual_by_key.get((edinet_code, leap_period_end))
                if actual is None:
                    actual = {
                        "doc_id": "",
                        "edinet_code": edinet_code,
                        "security_code": issuer.get("security_code", ""),
                        "company_name": issuer.get("company_name", ""),
                        "industry_33": issuer.get("industry_33", ""),
                        "period_end": period_end,
                        "submit_date": "",
                        "download_status": "",
                        "parse_status": "",
                        "zip_path": "",
                        "xbrl_path": "",
                        "normalized_count": 0,
                        "derived_count": 0,
                    }
                out_rows.append(_decorate_row(actual))

    out_rows.sort(key=lambda row: str(row.get("period_end") or ""), reverse=True)
    out_rows.sort(key=lambda row: str(row.get("security_code") or ""))
    return out_rows, True


def display_width(text: Any) -> int:
    width = 0
    for ch in str(text):
        width += 2 if unicodedata.east_asian_width(ch) in ("F", "W", "A") else 1
    return width


def pad_right(text: Any, width: int) -> str:
    text = str(text)
    return text + " " * max(0, width - display_width(text))


def pad_left(text: Any, width: int) -> str:
    text = str(text)
    return " " * max(0, width - display_width(text)) + text


def render_report(rows: list[dict[str, Any]], filters: ReportFilters, *, annual_gap_detection: bool) -> str:
    generated_at = datetime.now().isoformat(timespec="seconds")
    counts = Counter(str(row.get("status") or "") for row in rows)

    lines = [
        f"generated_at: {generated_at}",
        f"db_path: {filters.db_path}",
        f"period_mode: {filters.period_mode}",
        f"period_from: {filters.period_from or 'all'}",
        f"period_to: {filters.period_to or 'all'}",
        f"industry_33: {', '.join(filters.industry_33_list) if filters.industry_33_list else 'all'}",
        f"security_codes: {', '.join(filters.security_codes) if filters.security_codes else 'all'}",
        f"status_filter: {', '.join(filters.status_list) if filters.status_list else 'all'}",
        f"annual_gap_detection: {'on' if annual_gap_detection else 'off'}",
        f"rows: {len(rows)}",
        "status_summary: " + (" | ".join(f"{key}={counts[key]}" for key in sorted(counts)) if counts else "none"),
        "",
    ]

    columns = [
        ("security_code", "証券コード", "left"),
        ("company_name", "会社名", "left"),
        ("industry_33", "業種", "left"),
        ("period_end", "期末日", "left"),
        ("doc_id", "doc_id", "left"),
        ("submit_date", "提出日", "left"),
        ("filing", "filing", "left"),
        ("zip", "zip", "left"),
        ("normalized", "正規化", "right"),
        ("derived", "派生", "right"),
        ("parse_status", "parse_status", "left"),
        ("status", "状況", "left"),
    ]

    widths: dict[str, int] = {}
    for key, label, _ in columns:
        widths[key] = max([display_width(label)] + [display_width(row.get(key, "")) for row in rows] or [display_width(label)])

    header_parts: list[str] = []
    separator_parts: list[str] = []
    for key, label, align in columns:
        if align == "right":
            header_parts.append(pad_left(label, widths[key]))
        else:
            header_parts.append(pad_right(label, widths[key]))
        separator_parts.append("-" * widths[key])

    lines.append(" | ".join(header_parts))
    lines.append("-+-".join(separator_parts))

    for row in rows:
        parts: list[str] = []
        for key, _, align in columns:
            value = row.get(key, "")
            if align == "right":
                parts.append(pad_left(value, widths[key]))
            else:
                parts.append(pad_right(value, widths[key]))
        lines.append(" | ".join(parts).rstrip())

    lines.append("")
    return "\n".join(lines)


def _sanitize_filename_part(text: str) -> str:
    safe = []
    for ch in str(text or ""):
        if ch.isalnum() or ch in {"-", "_"}:
            safe.append(ch)
        elif ch in {",", " "}:
            safe.append("_")
    return "".join(safe).strip("_") or "all"


def build_output_path(filters: ReportFilters) -> Path:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    period_part = "all" if filters.period_mode == "all" else f"{filters.period_from}_to_{filters.period_to}"
    industry_part = _sanitize_filename_part(",".join(filters.industry_33_list) if filters.industry_33_list else "all")
    security_part = _sanitize_filename_part(",".join(filters.security_codes) if filters.security_codes else "all")
    filename = f"import_status_{period_part}_{industry_part}_{security_part}_{timestamp}.txt"
    return filters.output_dir / filename


def generate_report(filters: ReportFilters) -> tuple[Path, list[dict[str, Any]], bool]:
    conn = _connect_readonly(filters.db_path)
    try:
        actual_rows = fetch_actual_status_rows(conn, filters)
        rows, annual_gap_detection = expand_with_annual_gaps(conn, filters, actual_rows)
    finally:
        conn.close()

    if filters.status_list:
        allowed = set(filters.status_list)
        rows = [row for row in rows if str(row.get("status") or "").upper() in allowed]

    text = render_report(rows, filters, annual_gap_detection=annual_gap_detection)
    output_path = build_output_path(filters)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(text, encoding="utf-8-sig")
    return output_path, rows, annual_gap_detection


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--period-mode",
        default=os.getenv("IMPORT_STATUS_PERIOD_MODE", "all").strip() or "all",
        choices=["all", "range"],
        help="Use 'all' for all periods or 'range' with --period-from/--period-to.",
    )
    parser.add_argument(
        "--period-from",
        default=os.getenv("IMPORT_STATUS_PERIOD_FROM", "").strip(),
        help="Period-end start date in YYYY-MM-DD when --period-mode range.",
    )
    parser.add_argument(
        "--period-to",
        default=os.getenv("IMPORT_STATUS_PERIOD_TO", "").strip(),
        help="Period-end end date in YYYY-MM-DD when --period-mode range.",
    )
    parser.add_argument(
        "--industry-33",
        action="append",
        dest="industry_33_list",
        default=[],
        help="Repeatable or comma-separated. Use 'all' to disable the filter.",
    )
    parser.add_argument(
        "--security-code",
        action="append",
        dest="security_codes",
        default=[],
        help="Repeatable or comma-separated. 4 or 5 digit code accepted. Use 'all' to disable the filter.",
    )
    parser.add_argument(
        "--status-only",
        action="append",
        dest="status_list",
        default=[],
        help="Repeatable or comma-separated. Example: FILING_MISSING or OK. Use 'all' to disable the filter.",
    )
    parser.add_argument(
        "--output-dir",
        default=os.getenv("OUTPUT_DIR", str(DEFAULT_OUTPUT_DIR)),
        help="Directory to save the text report.",
    )
    parser.add_argument(
        "--db-path",
        default=os.getenv("DB_PATH", str(DB_PATH)),
        help="SQLite database path.",
    )
    return parser


def main() -> None:
    filters = _parse_filters(build_arg_parser().parse_args())
    output_path, rows, annual_gap_detection = generate_report(filters)
    print(f"saved: {output_path}")
    print(f"rows: {len(rows)}")
    print(f"annual_gap_detection: {'on' if annual_gap_detection else 'off'}")


if __name__ == "__main__":
    main()
