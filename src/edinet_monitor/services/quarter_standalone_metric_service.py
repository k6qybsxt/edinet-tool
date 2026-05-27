from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
import json
import sqlite3
from typing import Any


QUARTER_STANDALONE_RULE_VERSION = "quarter-standalone-2026-05-15-v1"
QUARTER_TYPES = ("1Q", "2Q", "3Q", "4Q")
QUARTER_STANDALONE_PERIOD_SCOPE = "quarter_standalone"

FLOW_BASES = (
    "NetSales",
    "OperatingIncome",
    "OrdinaryIncome",
    "ProfitBeforeTax",
    "ProfitLoss",
    "EstimatedNetIncome",
    "OperatingCash",
    "InvestmentCash",
    "FinancingCash",
    "FCF",
)

GROWTH_BASE_BY_FLOW_BASE = {
    "NetSales": "NetSalesGrowthRate",
    "OperatingIncome": "OperatingIncomeGrowthRate",
    "OrdinaryIncome": "OrdinaryIncomeGrowthRate",
    "ProfitLoss": "ProfitLossGrowthRate",
    "EstimatedNetIncome": "EstimatedNetIncomeGrowthRate",
    "OperatingCash": "OperatingCashGrowthRate",
    "InvestmentCash": "InvestmentCashGrowthRate",
    "FinancingCash": "FinancingCashGrowthRate",
    "FCF": "FCFGrowthRate",
}
SUPPRESSED_GROWTH_BASES_BY_QUARTER = {
    "1Q": {"OperatingCashGrowthRate", "InvestmentCashGrowthRate", "FinancingCashGrowthRate", "FCFGrowthRate"},
    "2Q": {"OperatingCashGrowthRate", "InvestmentCashGrowthRate", "FinancingCashGrowthRate", "FCFGrowthRate"},
    "3Q": {"OperatingCashGrowthRate", "InvestmentCashGrowthRate", "FinancingCashGrowthRate", "FCFGrowthRate"},
    "4Q": {"OperatingCashGrowthRate", "InvestmentCashGrowthRate", "FinancingCashGrowthRate", "FCFGrowthRate"},
}
SUPPRESSED_FLOW_BASES_BY_QUARTER = {
    "1Q": {"OperatingCash", "InvestmentCash", "FinancingCash", "FCF"},
    "2Q": {"OperatingCash", "InvestmentCash", "FinancingCash", "FCF"},
    "3Q": {"OperatingCash", "InvestmentCash", "FinancingCash", "FCF"},
    "4Q": {"OperatingCash", "InvestmentCash", "FinancingCash", "FCF"},
}


def _chunked(items: list[str], size: int = 500) -> list[list[str]]:
    return [items[index : index + size] for index in range(0, len(items), size)]


@dataclass(frozen=True)
class QuarterStandaloneMetricRow:
    security_code: str
    edinet_code: str
    fiscal_year: int
    quarter_type: str
    period_end: str
    metric_key: str
    metric_base: str
    metric_group: str
    value_num: float | None
    value_unit: str
    calc_status: str
    formula_name: str
    source_detail_json: str
    rule_version: str = QUARTER_STANDALONE_RULE_VERSION


@dataclass(frozen=True)
class QuarterStandaloneMetricResult:
    rows: list[QuarterStandaloneMetricRow]
    saved_rows: int
    warnings: list[str]
    output_path: Path


def quarter_standalone_table_exists(conn: sqlite3.Connection) -> bool:
    row = conn.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type = 'table' AND name = 'quarter_standalone_metrics'
        """
    ).fetchone()
    return row is not None


def _normalize_security_code(value: Any) -> str:
    text = str(value or "").strip().replace("-", "")
    if len(text) == 5 and text.endswith("0"):
        return text[:-1]
    return text


def _metric_key(metric_base: str) -> str:
    return f"{metric_base}Current"


def _growth_metric_key(metric_base: str) -> str:
    return f"{metric_base}QuarterStandaloneCurrent"


def _metric_group(metric_base: str) -> str:
    if metric_base in {"OperatingCash", "InvestmentCash", "FinancingCash", "FCF"}:
        return "cashflow"
    if metric_base.endswith("GrowthRate"):
        return "growth"
    return "profitability"


def _value_unit(metric_base: str) -> str:
    if metric_base.endswith("GrowthRate"):
        return "ratio"
    return "yen"


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _parse_year(value: Any) -> int | None:
    text = str(value or "").strip()
    if len(text) < 4:
        return None
    try:
        return int(text[:4])
    except ValueError:
        return None


def _shift_year(date_text: str, years: int) -> str:
    if len(str(date_text or "")) < 10:
        return date_text
    year = int(date_text[:4]) + years
    suffix = date_text[4:10]
    if suffix == "-02-29":
        return f"{year}-02-28"
    return f"{year}{suffix}"


def _expected_fiscal_year_end(
    *,
    security_code: str,
    half_period_end: str,
    annual_periods_by_code: dict[str, list[str]],
) -> tuple[int | None, str]:
    annual_periods = annual_periods_by_code.get(security_code, [])
    future = [period for period in annual_periods if period and period > half_period_end]
    if future:
        period_end = sorted(future)[0]
        return _parse_year(period_end), period_end

    past = [period for period in annual_periods if period and period <= half_period_end]
    if past:
        period_end = _shift_year(sorted(past)[-1], 1)
        return _parse_year(period_end), period_end

    if len(half_period_end) >= 10:
        # Fallback for companies without an annual filing in the DB yet.
        month = int(half_period_end[5:7])
        year = int(half_period_end[:4])
        fiscal_month = month + 6
        if fiscal_month > 12:
            fiscal_month -= 12
            year += 1
        return year, f"{year:04d}-{fiscal_month:02d}-{half_period_end[8:10]}"
    return None, ""


def _fetch_target_companies(
    conn: sqlite3.Connection,
    *,
    codes: list[str] | None,
) -> list[sqlite3.Row]:
    where = [
        "coalesce(im.is_listed, 0) = 1",
        "coalesce(im.exchange, '') = 'TSE'",
    ]
    params: list[Any] = []
    normalized_codes = [_normalize_security_code(code) for code in (codes or []) if str(code).strip()]
    if normalized_codes:
        placeholders = ",".join("?" for _ in normalized_codes)
        where.append(
            f"(substr(coalesce(im.security_code, ''), 1, 4) IN ({placeholders}) "
            f"OR coalesce(im.security_code, '') IN ({placeholders}))"
        )
        params.extend([*normalized_codes, *normalized_codes])
    return conn.execute(
        f"""
        SELECT
          im.edinet_code,
          im.security_code,
          im.company_name,
          im.industry_33
        FROM issuer_master im
        WHERE {" AND ".join(where)}
        ORDER BY im.security_code, im.edinet_code
        """,
        params,
    ).fetchall()


def _annual_periods_by_code(conn: sqlite3.Connection) -> dict[str, list[str]]:
    rows = conn.execute(
        """
        SELECT
          coalesce(nullif(f.security_code, ''), im.security_code) AS security_code,
          f.period_end
        FROM filings f
        JOIN issuer_master im
          ON im.edinet_code = f.edinet_code
        WHERE f.form_type = '030000'
          AND f.period_end IS NOT NULL
        ORDER BY security_code, f.period_end
        """
    ).fetchall()
    result: dict[str, list[str]] = {}
    for row in rows:
        code = _normalize_security_code(row["security_code"])
        if not code:
            continue
        result.setdefault(code, []).append(str(row["period_end"] or ""))
    return result


def _add_value(
    cumulative: dict[tuple[str, int, str], dict[str, Any]],
    *,
    security_code: str,
    edinet_code: str,
    fiscal_year: int,
    quarter_type: str,
    period_end: str,
    metric_base: str,
    value_num: float | None,
    source: str,
) -> None:
    if value_num is None:
        return
    key = (security_code, fiscal_year, quarter_type)
    item = cumulative.setdefault(
        key,
        {
            "security_code": security_code,
            "edinet_code": edinet_code,
            "fiscal_year": fiscal_year,
            "quarter_type": quarter_type,
            "period_end": period_end,
            "values": {},
            "sources": {},
        },
    )
    if not item.get("period_end") and period_end:
        item["period_end"] = period_end
    item["values"][metric_base] = value_num
    item["sources"][metric_base] = source


def _fetch_jquants_cumulative_values(
    conn: sqlite3.Connection,
    *,
    security_codes: list[str],
    date_from: str | None,
    date_to: str | None,
) -> dict[tuple[str, int, str], dict[str, Any]]:
    cumulative: dict[tuple[str, int, str], dict[str, Any]] = {}
    if not security_codes:
        return cumulative

    placeholders = ",".join("?" for _ in security_codes)
    base_placeholders = ",".join("?" for _ in FLOW_BASES)
    where = [
        f"(security_code IN ({placeholders}) OR local_code IN ({placeholders}))",
        "period_scope = 'quarter'",
        "period_key IN ('actual:1Q', 'actual:2Q', 'actual:3Q')",
        f"metric_base IN ({base_placeholders})",
    ]
    params: list[Any] = [*security_codes, *security_codes, *FLOW_BASES]
    if date_from:
        where.append("period_end >= ?")
        params.append(date_from)
    if date_to:
        where.append("period_end <= ?")
        params.append(date_to)

    rows = conn.execute(
        f"""
        SELECT
          local_code,
          security_code,
          edinet_code,
          fiscal_year,
          quarter_type,
          period_end,
          metric_base,
          value_num,
          calc_status,
          disclosure_number
        FROM jquants_financial_metrics
        WHERE {" AND ".join(where)}
        ORDER BY security_code, fiscal_year, quarter_type, metric_base, disclosed_date, disclosed_time
        """,
        params,
    ).fetchall()
    for row in rows:
        if str(row["calc_status"] or "") != "ok":
            continue
        fiscal_year = row["fiscal_year"]
        if fiscal_year is None:
            continue
        security_code = _normalize_security_code(row["security_code"] or row["local_code"])
        quarter_type = str(row["quarter_type"] or "")
        if quarter_type not in {"1Q", "2Q", "3Q"}:
            continue
        _add_value(
            cumulative,
            security_code=security_code,
            edinet_code=str(row["edinet_code"] or ""),
            fiscal_year=int(fiscal_year),
            quarter_type=quarter_type,
            period_end=str(row["period_end"] or ""),
            metric_base=str(row["metric_base"] or ""),
            value_num=_to_float(row["value_num"]),
            source=f"jquants:{row['disclosure_number']}",
        )
    return cumulative


def _fetch_edinet_doc_metric_values(
    conn: sqlite3.Connection,
    *,
    doc_ids: list[str],
) -> dict[tuple[str, str], float | None]:
    if not doc_ids:
        return {}
    metric_keys = [_metric_key(base) for base in FLOW_BASES]
    values: dict[tuple[str, str], float | None] = {}
    key_placeholders = ",".join("?" for _ in metric_keys)
    for doc_id_chunk in _chunked(doc_ids):
        doc_placeholders = ",".join("?" for _ in doc_id_chunk)
        normalized_rows = conn.execute(
            f"""
            SELECT doc_id, metric_key, value_num
            FROM normalized_metrics
            WHERE doc_id IN ({doc_placeholders})
              AND metric_key IN ({key_placeholders})
            """,
            [*doc_id_chunk, *metric_keys],
        ).fetchall()
        for row in normalized_rows:
            values[(str(row["doc_id"]), str(row["metric_key"]))] = _to_float(row["value_num"])

        derived_rows = conn.execute(
            f"""
            SELECT doc_id, metric_key, value_num, calc_status
            FROM derived_metrics
            WHERE doc_id IN ({doc_placeholders})
              AND metric_key IN ({key_placeholders})
            """,
            [*doc_id_chunk, *metric_keys],
        ).fetchall()
        for row in derived_rows:
            key = (str(row["doc_id"]), str(row["metric_key"]))
            if key in values and values[key] is not None:
                continue
            if str(row["calc_status"] or "") == "missing_input":
                values[key] = None
            else:
                values[key] = _to_float(row["value_num"])
    return values


def _fetch_edinet_cumulative_values(
    conn: sqlite3.Connection,
    *,
    security_codes: list[str],
    date_from: str | None,
    date_to: str | None,
) -> dict[tuple[str, int, str], dict[str, Any]]:
    cumulative: dict[tuple[str, int, str], dict[str, Any]] = {}
    if not security_codes:
        return cumulative
    annual_periods = _annual_periods_by_code(conn)
    placeholders = ",".join("?" for _ in security_codes)
    where = [
        "f.form_type IN ('030000', '043A00')",
        "f.parse_status = 'derived_metrics_saved'",
        f"(substr(coalesce(im.security_code, ''), 1, 4) IN ({placeholders}) OR coalesce(f.security_code, '') IN ({placeholders}))",
    ]
    params: list[Any] = [*security_codes, *security_codes]
    if date_from:
        where.append("f.period_end >= ?")
        params.append(date_from)
    if date_to:
        where.append("f.period_end <= ?")
        params.append(date_to)

    filings = conn.execute(
        f"""
        SELECT
          f.doc_id,
          f.edinet_code,
          coalesce(nullif(f.security_code, ''), im.security_code) AS security_code,
          f.form_type,
          f.period_end
        FROM filings f
        JOIN issuer_master im
          ON im.edinet_code = f.edinet_code
        WHERE {" AND ".join(where)}
        ORDER BY security_code, f.period_end
        """,
        params,
    ).fetchall()
    doc_ids = [str(row["doc_id"]) for row in filings]
    values = _fetch_edinet_doc_metric_values(conn, doc_ids=doc_ids)
    for filing in filings:
        security_code = _normalize_security_code(filing["security_code"])
        period_end = str(filing["period_end"] or "")
        form_type = str(filing["form_type"] or "")
        if not security_code or not period_end:
            continue
        if form_type == "030000":
            fiscal_year = _parse_year(period_end)
            quarter_type = "4Q"
            fiscal_period_end = period_end
        else:
            fiscal_year, fiscal_period_end = _expected_fiscal_year_end(
                security_code=security_code,
                half_period_end=period_end,
                annual_periods_by_code=annual_periods,
            )
            quarter_type = "2Q"
        if fiscal_year is None:
            continue
        for base in FLOW_BASES:
            value = values.get((str(filing["doc_id"]), _metric_key(base)))
            _add_value(
                cumulative,
                security_code=security_code,
                edinet_code=str(filing["edinet_code"] or ""),
                fiscal_year=fiscal_year,
                quarter_type=quarter_type,
                period_end=period_end if quarter_type == "2Q" else fiscal_period_end,
                metric_base=base,
                value_num=value,
                source=f"edinet:{filing['doc_id']}",
            )
    return cumulative


def _merge_cumulative_sources(*sources: dict[tuple[str, int, str], dict[str, Any]]) -> dict[tuple[str, int, str], dict[str, Any]]:
    merged: dict[tuple[str, int, str], dict[str, Any]] = {}
    for source in sources:
        for key, item in source.items():
            target = merged.setdefault(
                key,
                {
                    "security_code": item.get("security_code", ""),
                    "edinet_code": item.get("edinet_code", ""),
                    "fiscal_year": item.get("fiscal_year"),
                    "quarter_type": item.get("quarter_type"),
                    "period_end": item.get("period_end", ""),
                    "values": {},
                    "sources": {},
                },
            )
            if item.get("edinet_code"):
                target["edinet_code"] = item.get("edinet_code")
            if item.get("period_end"):
                target["period_end"] = item.get("period_end")
            target["values"].update(item.get("values", {}))
            target["sources"].update(item.get("sources", {}))
    return merged


def _derive_cumulative_values(cumulative: dict[tuple[str, int, str], dict[str, Any]]) -> None:
    for item in cumulative.values():
        values = item.get("values", {})
        sources = item.get("sources", {})
        if "FCF" not in values:
            operating_cash = _to_float(values.get("OperatingCash"))
            investment_cash = _to_float(values.get("InvestmentCash"))
            if operating_cash is not None and investment_cash is not None:
                values["FCF"] = operating_cash + investment_cash
                sources["FCF"] = "derived:OperatingCash+InvestmentCash"
        if "EstimatedNetIncome" not in values:
            ordinary_income = _to_float(values.get("OrdinaryIncome"))
            profit_before_tax = _to_float(values.get("ProfitBeforeTax"))
            profit_base = ordinary_income if ordinary_income is not None else profit_before_tax
            if profit_base is not None:
                values["EstimatedNetIncome"] = profit_base * 0.7
                source_base = "OrdinaryIncome" if ordinary_income is not None else "ProfitBeforeTax"
                sources["EstimatedNetIncome"] = f"derived:{source_base}*0.7"


def _build_rows_from_cumulative(
    cumulative: dict[tuple[str, int, str], dict[str, Any]],
) -> list[QuarterStandaloneMetricRow]:
    _derive_cumulative_values(cumulative)
    rows: list[QuarterStandaloneMetricRow] = []
    standalone_values: dict[tuple[str, int, str, str], float | None] = {}
    scope_keys = sorted({(code, fiscal_year) for code, fiscal_year, _quarter in cumulative})
    for security_code, fiscal_year in scope_keys:
        bases_for_scope = sorted(
            {
                base
                for quarter_type in QUARTER_TYPES
                for base in cumulative.get((security_code, fiscal_year, quarter_type), {}).get("values", {})
                if base in FLOW_BASES
            }
        )
        for base in bases_for_scope:
            previous_cumulative: float | None = None
            previous_quarter: str | None = None
            for quarter_type in QUARTER_TYPES:
                if base in SUPPRESSED_FLOW_BASES_BY_QUARTER.get(quarter_type, set()):
                    continue
                item = cumulative.get((security_code, fiscal_year, quarter_type))
                cumulative_value = (
                    _to_float(item["values"].get(base))
                    if item is not None and base in item.get("values", {})
                    else None
                )
                if quarter_type == "1Q":
                    standalone_value = cumulative_value
                    calc_status = "ok" if cumulative_value is not None else "missing_input"
                    formula_name = "quarter_standalone_1q"
                else:
                    if cumulative_value is None or previous_cumulative is None:
                        standalone_value = None
                        calc_status = "missing_input"
                    else:
                        standalone_value = cumulative_value - previous_cumulative
                        calc_status = "ok"
                    formula_name = f"quarter_standalone_{quarter_type.lower()}_minus_{(previous_quarter or '').lower()}"

                period_end = str(item.get("period_end") if item else "")
                edinet_code = str(item.get("edinet_code") if item else "")
                source_detail = {
                    "metric_base": base,
                    "quarter_type": quarter_type,
                    "cumulative_value": cumulative_value,
                    "previous_cumulative": previous_cumulative,
                    "source": item.get("sources", {}).get(base) if item else None,
                    "previous_quarter": previous_quarter,
                    "rule": "standalone = current cumulative - previous cumulative",
                }
                rows.append(
                    QuarterStandaloneMetricRow(
                        security_code=security_code,
                        edinet_code=edinet_code,
                        fiscal_year=fiscal_year,
                        quarter_type=quarter_type,
                        period_end=period_end,
                        metric_key=_metric_key(base),
                        metric_base=base,
                        metric_group=_metric_group(base),
                        value_num=standalone_value,
                        value_unit=_value_unit(base),
                        calc_status=calc_status,
                        formula_name=formula_name,
                        source_detail_json=json.dumps(source_detail, ensure_ascii=False, sort_keys=True),
                    )
                )
                standalone_values[(security_code, fiscal_year, quarter_type, base)] = standalone_value
                previous_cumulative = cumulative_value
                previous_quarter = quarter_type

    for security_code, fiscal_year in scope_keys:
        bases_for_scope = sorted(
            {
                base
                for quarter_type in QUARTER_TYPES
                for base in cumulative.get((security_code, fiscal_year, quarter_type), {}).get("values", {})
                if base in GROWTH_BASE_BY_FLOW_BASE
            }
        )
        for base in bases_for_scope:
            growth_base = GROWTH_BASE_BY_FLOW_BASE[base]
            for quarter_type in QUARTER_TYPES:
                if growth_base in SUPPRESSED_GROWTH_BASES_BY_QUARTER.get(quarter_type, set()):
                    continue
                current = standalone_values.get((security_code, fiscal_year, quarter_type, base))
                prior = standalone_values.get((security_code, fiscal_year - 1, quarter_type, base))
                if current is None or prior is None or prior <= 0:
                    value = None
                    calc_status = "missing_input"
                else:
                    value = current / prior
                    calc_status = "ok"
                item = cumulative.get((security_code, fiscal_year, quarter_type), {})
                source_detail = {
                    "metric_base": growth_base,
                    "source_metric_base": base,
                    "quarter_type": quarter_type,
                    "current_standalone": current,
                    "prior_year_standalone": prior,
                    "rule": "current quarter standalone / prior-year same-quarter standalone",
                }
                rows.append(
                    QuarterStandaloneMetricRow(
                        security_code=security_code,
                        edinet_code=str(item.get("edinet_code") or ""),
                        fiscal_year=fiscal_year,
                        quarter_type=quarter_type,
                        period_end=str(item.get("period_end") or ""),
                        metric_key=_growth_metric_key(growth_base),
                        metric_base=growth_base,
                        metric_group=_metric_group(growth_base),
                        value_num=value,
                        value_unit=_value_unit(growth_base),
                        calc_status=calc_status,
                        formula_name="quarter_standalone_yoy_growth",
                        source_detail_json=json.dumps(source_detail, ensure_ascii=False, sort_keys=True),
                    )
                )
    return rows


def _delete_obsolete_quarter_standalone_rows(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        DELETE FROM quarter_standalone_metrics
        WHERE quarter_type IN ('1Q', '2Q', '3Q', '4Q')
          AND metric_base IN (
            'OperatingCash',
            'InvestmentCash',
            'FinancingCash',
            'FCF',
            'OperatingCashGrowthRate',
            'InvestmentCashGrowthRate',
            'FinancingCashGrowthRate',
            'FCFGrowthRate'
          )
        """
    )


def _save_rows(conn: sqlite3.Connection, rows: list[QuarterStandaloneMetricRow]) -> int:
    now = datetime.now().isoformat(timespec="seconds")
    _delete_obsolete_quarter_standalone_rows(conn)
    conn.executemany(
        """
        INSERT INTO quarter_standalone_metrics (
            security_code, edinet_code, fiscal_year, quarter_type, period_end,
            metric_key, metric_base, metric_group, value_num, value_unit,
            calc_status, formula_name, source_detail_json, rule_version,
            created_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(security_code, fiscal_year, quarter_type, metric_key)
        DO UPDATE SET
            edinet_code = excluded.edinet_code,
            period_end = excluded.period_end,
            metric_base = excluded.metric_base,
            metric_group = excluded.metric_group,
            value_num = excluded.value_num,
            value_unit = excluded.value_unit,
            calc_status = excluded.calc_status,
            formula_name = excluded.formula_name,
            source_detail_json = excluded.source_detail_json,
            rule_version = excluded.rule_version,
            updated_at = excluded.updated_at
        """,
        [
            (
                row.security_code,
                row.edinet_code,
                row.fiscal_year,
                row.quarter_type,
                row.period_end,
                row.metric_key,
                row.metric_base,
                row.metric_group,
                row.value_num,
                row.value_unit,
                row.calc_status,
                row.formula_name,
                row.source_detail_json,
                row.rule_version,
                now,
                now,
            )
            for row in rows
        ],
    )
    conn.commit()
    return len(rows)


def _write_report(
    *,
    rows: list[QuarterStandaloneMetricRow],
    output_dir: str | Path,
    date_from: str | None,
    date_to: str | None,
    apply: bool,
    warnings: list[str],
) -> Path:
    output_root = Path(output_dir)
    output_root.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    suffix_from = date_from or "all"
    suffix_to = date_to or "all"
    output_path = output_root / f"quarter_standalone_metrics_{suffix_from}_to_{suffix_to}_{timestamp}.txt"

    ok_count = sum(1 for row in rows if row.calc_status == "ok")
    missing_count = sum(1 for row in rows if row.calc_status != "ok")
    by_quarter: dict[str, int] = {}
    by_metric: dict[str, int] = {}
    for row in rows:
        by_quarter[row.quarter_type] = by_quarter.get(row.quarter_type, 0) + 1
        if row.calc_status == "ok":
            by_metric[row.metric_base] = by_metric.get(row.metric_base, 0) + 1

    lines = [
        f"generated_at: {datetime.now().isoformat(timespec='seconds')}",
        f"mode: {'apply' if apply else 'dry_run'}",
        f"date_from: {date_from or 'all'}",
        f"date_to: {date_to or 'all'}",
        f"rows: {len(rows)}",
        f"ok_rows: {ok_count}",
        f"missing_rows: {missing_count}",
        "",
        "[quarter_counts]",
    ]
    for quarter_type in QUARTER_TYPES:
        lines.append(f"{quarter_type}: {by_quarter.get(quarter_type, 0)}")
    lines.extend(["", "[ok_rows_by_metric]"])
    for metric_base in sorted(by_metric):
        lines.append(f"{metric_base}: {by_metric[metric_base]}")
    if warnings:
        lines.extend(["", "[warnings]"])
        lines.extend(warnings)
    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8-sig")
    return output_path


def save_quarter_standalone_metrics(
    conn: sqlite3.Connection,
    *,
    date_from: str | None = None,
    date_to: str | None = None,
    codes: list[str] | None = None,
    apply: bool = False,
    output_dir: str | Path = ".",
) -> QuarterStandaloneMetricResult:
    companies = _fetch_target_companies(conn, codes=codes)
    security_codes = sorted(
        {
            _normalize_security_code(row["security_code"])
            for row in companies
            if _normalize_security_code(row["security_code"])
        }
    )
    warnings: list[str] = []
    if not security_codes:
        warnings.append("target_companies_not_found")

    jquants_values = _fetch_jquants_cumulative_values(
        conn,
        security_codes=security_codes,
        date_from=date_from,
        date_to=date_to,
    )
    edinet_values = _fetch_edinet_cumulative_values(
        conn,
        security_codes=security_codes,
        date_from=date_from,
        date_to=date_to,
    )
    cumulative = _merge_cumulative_sources(jquants_values, edinet_values)
    rows = _build_rows_from_cumulative(cumulative)

    saved_rows = 0
    if apply:
        saved_rows = _save_rows(conn, rows)
    output_path = _write_report(
        rows=rows,
        output_dir=output_dir,
        date_from=date_from,
        date_to=date_to,
        apply=apply,
        warnings=warnings,
    )
    return QuarterStandaloneMetricResult(
        rows=rows,
        saved_rows=saved_rows,
        warnings=warnings,
        output_path=output_path,
    )
