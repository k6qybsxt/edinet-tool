from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
import json
from pathlib import Path
import sqlite3
from typing import Any


MARKET_DERIVED_RULE_VERSION = "market-derived-2026-05-24-v2"
MARKET_METRIC_BASES = {
    "StockPrice",
    "MarketCapitalization",
    "StockPriceGrowthRate",
    "StockPriceGrowthRate5Year",
    "StockPriceGrowthRate10Year",
    "PBR",
    "PER",
    "PCFR",
    "TheoreticalSharePrice",
    "TheoreticalSharePriceGrowthRate",
    "TheoreticalPBR",
    "TheoreticalPER",
}
EDINET_INPUT_BASES = {
    "EPS",
    "BPS",
    "OperatingCashPerShare",
    "OutstandingShares",
}
JQUANTS_INPUT_BASES = {
    "NetSales",
    "OperatingIncome",
    "OrdinaryIncome",
    "ProfitLoss",
    "EPS",
    "TotalAssets",
    "NetAssets",
    "EquityRatio",
    "BPS",
    "OperatingCash",
    "OutstandingShares",
}


@dataclass(frozen=True)
class MarketSourcePeriod:
    source_type: str
    source_id: str
    edinet_code: str
    security_code: str
    period_scope: str
    period_key: str
    quarter_type: str | None
    fiscal_year: int | None
    period_end: str
    values: dict[str, float | None]


@dataclass(frozen=True)
class MarketDerivedResult:
    rows: list[dict[str, Any]]
    output_path: Path | None
    missing_quotes: int
    warnings: list[str]


def _now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table_name,),
    ).fetchone()
    return row is not None


def market_derived_table_exists(conn: sqlite3.Connection) -> bool:
    return _table_exists(conn, "market_derived_metrics")


def _normalize_security_code(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if text.endswith("0") and len(text) == 5:
        text = text[:-1]
    return text


def _parse_date(value: Any) -> date | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


def _to_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _ratio(
    numerator: float | None,
    denominator: float | None,
    *,
    require_positive_denominator: bool = True,
) -> tuple[float | None, str]:
    if numerator is None or denominator is None:
        return None, "missing_input"
    if require_positive_denominator and denominator <= 0:
        return None, "missing_input"
    if not require_positive_denominator and denominator == 0:
        return None, "missing_input"
    return numerator / denominator, "ok"


def _quote_on_or_before(
    conn: sqlite3.Connection,
    security_code: str,
    period_end: str,
    *,
    max_lookback_days: int,
    price_kind: str = "adjusted",
) -> tuple[float | None, str | None, str | None]:
    period_date = _parse_date(period_end)
    if period_date is None or not security_code:
        return None, None, None
    start_date = period_date - timedelta(days=max_lookback_days)
    if price_kind == "raw":
        price_columns = "close, adjustment_close_rounded"
        price_filter = "(close IS NOT NULL OR adjustment_close_rounded IS NOT NULL)"
    else:
        price_columns = "adjustment_close_rounded"
        price_filter = "adjustment_close_rounded IS NOT NULL"
    row = conn.execute(
        f"""
        SELECT trade_date, {price_columns}
        FROM jquants_daily_quotes
        WHERE security_code = ?
          AND trade_date <= ?
          AND trade_date >= ?
          AND {price_filter}
        ORDER BY trade_date DESC
        LIMIT 1
        """,
        (security_code, period_date.isoformat(), start_date.isoformat()),
    ).fetchone()
    if row is None:
        return None, None, None
    if price_kind == "raw":
        close_value = _to_float(row["close"])
        if close_value is not None:
            return close_value, str(row["trade_date"]), "jquants_daily_quotes.close"
        return (
            _to_float(row["adjustment_close_rounded"]),
            str(row["trade_date"]),
            "jquants_daily_quotes.adjustment_close_rounded",
        )
    return (
        _to_float(row["adjustment_close_rounded"]),
        str(row["trade_date"]),
        "jquants_daily_quotes.adjustment_close_rounded",
    )


def _metric_key(metric_base: str) -> str:
    return f"{metric_base}Current"


def _source_detail(**values: Any) -> str:
    return json.dumps(values, ensure_ascii=False, sort_keys=True)


def _build_row(
    source: MarketSourcePeriod,
    *,
    metric_base: str,
    metric_group: str,
    value_num: float | None,
    value_unit: str,
    calc_status: str,
    formula_name: str,
    source_detail: dict[str, Any],
) -> dict[str, Any]:
    return {
        "source_type": source.source_type,
        "source_id": source.source_id,
        "edinet_code": source.edinet_code,
        "security_code": source.security_code,
        "period_scope": source.period_scope,
        "period_key": source.period_key,
        "quarter_type": source.quarter_type,
        "fiscal_year": source.fiscal_year,
        "period_end": source.period_end,
        "metric_key": _metric_key(metric_base),
        "metric_base": metric_base,
        "metric_group": metric_group,
        "value_num": value_num,
        "value_unit": value_unit,
        "calc_status": calc_status,
        "formula_name": formula_name,
        "source_detail_json": _source_detail(**source_detail),
        "rule_version": MARKET_DERIVED_RULE_VERSION,
    }


def _equity_ratio(values: dict[str, float | None]) -> float | None:
    direct = values.get("EquityRatio")
    if direct is not None:
        return direct / 100.0 if direct > 1 else direct
    ratio, status = _ratio(values.get("NetAssets"), values.get("TotalAssets"), require_positive_denominator=False)
    return ratio if status == "ok" else None


def _discount_rate(equity_ratio: float | None) -> float | None:
    if equity_ratio is None:
        return None
    if equity_ratio >= 0.80:
        return 0.80
    if equity_ratio >= 0.67:
        return 0.75
    if equity_ratio >= 0.50:
        return 0.70
    if equity_ratio >= 0.33:
        return 0.65
    if equity_ratio >= 0.10:
        return 0.60
    return 0.50


def _financial_leverage_adjustment(netassets: float | None, totalassets: float | None) -> float | None:
    if netassets is None or totalassets is None:
        return None
    if netassets * totalassets <= 0:
        return 0.0
    value = 1 / (netassets / totalassets + 0.33)
    if value < 1:
        return 1.0
    if value > 1.5:
        return 1.5
    return value


def _quarter_theoretical_share_price(values: dict[str, float | None]) -> tuple[float | None, dict[str, Any]]:
    ordinary_income = values.get("OrdinaryIncome")
    total_assets = values.get("TotalAssets")
    net_assets = values.get("NetAssets")
    outstanding_shares = values.get("OutstandingShares")
    estimated_net_income = ordinary_income * 0.7 if ordinary_income is not None else None
    eps, eps_status = _ratio(estimated_net_income, outstanding_shares)
    bps, bps_status = _ratio(net_assets, outstanding_shares)
    roa, roa_status = _ratio(estimated_net_income, total_assets, require_positive_denominator=False)
    equity = _equity_ratio(values)
    discount = _discount_rate(equity)
    leverage = _financial_leverage_adjustment(net_assets, total_assets)
    asset_value = bps * discount if bps is not None and discount is not None else None
    business_multiplier = roa * 150 * leverage if roa is not None and leverage is not None else None
    business_value = eps * business_multiplier if eps is not None and business_multiplier is not None else None
    value = (
        asset_value + business_value
        if asset_value is not None and business_value is not None
        else None
    )
    return value, {
        "ordinary_income": ordinary_income,
        "estimated_net_income": estimated_net_income,
        "outstanding_shares": outstanding_shares,
        "eps": eps,
        "eps_status": eps_status,
        "bps": bps,
        "bps_status": bps_status,
        "roa": roa,
        "roa_status": roa_status,
        "equity_ratio": equity,
        "discount_rate": discount,
        "financial_leverage_adjustment": leverage,
        "asset_value": asset_value,
        "business_value": business_value,
    }


def _fetch_edinet_sources(
    conn: sqlite3.Connection,
    *,
    date_from: str | None,
    date_to: str | None,
    codes: list[str],
) -> list[MarketSourcePeriod]:
    where = [
        "f.form_type IN ('030000', '043A00', '043000')",
        "f.parse_status = 'derived_metrics_saved'",
        "COALESCE(im.is_listed, 1) = 1",
        "(im.exchange = 'TSE' OR im.exchange IS NULL OR im.exchange = '')",
    ]
    params: list[Any] = []
    if date_from:
        where.append("f.period_end >= ?")
        params.append(date_from)
    if date_to:
        where.append("f.period_end <= ?")
        params.append(date_to)
    if codes:
        placeholders = ",".join("?" for _ in codes)
        where.append(f"(f.security_code IN ({placeholders}) OR im.security_code IN ({placeholders}))")
        params.extend(codes)
        params.extend(codes)

    rows = conn.execute(
        f"""
        SELECT
            f.doc_id,
            f.edinet_code,
            COALESCE(f.security_code, im.security_code) AS security_code,
            f.form_type,
            f.period_end
        FROM filings f
        LEFT JOIN issuer_master im
          ON im.edinet_code = f.edinet_code
        WHERE {" AND ".join(where)}
        ORDER BY f.edinet_code, f.form_type, f.period_end DESC, f.doc_id DESC
        """,
        params,
    ).fetchall()
    values = _fetch_edinet_metric_values(conn, [str(row["doc_id"]) for row in rows])
    sources: list[MarketSourcePeriod] = []
    for row in rows:
        form_type = str(row["form_type"] or "")
        period_scope = "quarter" if form_type in {"043A00", "043000"} else "annual"
        period_key = "actual:2Q" if period_scope == "quarter" else "annual:FY"
        quarter_type = "2Q" if period_scope == "quarter" else None
        period_end = str(row["period_end"] or "")
        fiscal_year = int(period_end[:4]) if len(period_end) >= 4 and period_end[:4].isdigit() else None
        security_code = _normalize_security_code(row["security_code"])
        if not security_code or not period_end:
            continue
        sources.append(
            MarketSourcePeriod(
                source_type="edinet",
                source_id=str(row["doc_id"]),
                edinet_code=str(row["edinet_code"] or ""),
                security_code=security_code,
                period_scope=period_scope,
                period_key=period_key,
                quarter_type=quarter_type,
                fiscal_year=fiscal_year,
                period_end=period_end,
                values=values.get(str(row["doc_id"]), {}),
            )
        )
    return sources


def _fetch_edinet_metric_values(
    conn: sqlite3.Connection,
    doc_ids: list[str],
) -> dict[str, dict[str, float | None]]:
    if not doc_ids:
        return {}
    result: dict[str, dict[str, float | None]] = {doc_id: {} for doc_id in doc_ids}
    metric_keys = [_metric_key(base) for base in EDINET_INPUT_BASES]
    key_placeholders = ",".join("?" for _ in metric_keys)
    for table_name in ("normalized_metrics", "derived_metrics"):
        status_expr = ", 'ok' AS calc_status" if table_name == "normalized_metrics" else ", calc_status"
        # SQLite has a variable limit, so fetch doc_ids in chunks for large DB backfills.
        for start in range(0, len(doc_ids), 500):
            doc_chunk = doc_ids[start : start + 500]
            doc_placeholders = ",".join("?" for _ in doc_chunk)
            rows = conn.execute(
                f"""
                SELECT doc_id, metric_key, value_num {status_expr}
                FROM {table_name}
                WHERE doc_id IN ({doc_placeholders})
                  AND metric_key IN ({key_placeholders})
                """,
                [*doc_chunk, *metric_keys],
            ).fetchall()
            for row in rows:
                if str(row["calc_status"] or "") == "missing_input":
                    value = None
                else:
                    value = _to_float(row["value_num"])
                metric_key = str(row["metric_key"] or "")
                metric_base = metric_key.removesuffix("Current")
                result.setdefault(str(row["doc_id"]), {})[metric_base] = value
    return result


def _fetch_jquants_sources(
    conn: sqlite3.Connection,
    *,
    date_from: str | None,
    date_to: str | None,
    codes: list[str],
) -> list[MarketSourcePeriod]:
    if not _table_exists(conn, "jquants_financial_metrics"):
        return []
    where = [
        "metric_kind = 'actual'",
        "period_scope = 'quarter'",
        "period_key IN ('actual:1Q', 'actual:3Q')",
    ]
    params: list[Any] = []
    if date_from:
        where.append("period_end >= ?")
        params.append(date_from)
    if date_to:
        where.append("period_end <= ?")
        params.append(date_to)
    if codes:
        placeholders = ",".join("?" for _ in codes)
        where.append(f"(security_code IN ({placeholders}) OR local_code IN ({placeholders}))")
        params.extend(codes)
        params.extend(codes)
    rows = conn.execute(
        f"""
        SELECT
            disclosure_number,
            local_code,
            security_code,
            edinet_code,
            period_scope,
            period_key,
            quarter_type,
            fiscal_year,
            period_end,
            metric_base,
            value_num,
            calc_status
        FROM jquants_financial_metrics
        WHERE {" AND ".join(where)}
          AND metric_base IN ({",".join("?" for _ in JQUANTS_INPUT_BASES)})
        ORDER BY security_code, period_key, fiscal_year DESC, disclosure_number, metric_base
        """,
        [*params, *JQUANTS_INPUT_BASES],
    ).fetchall()

    grouped: dict[tuple[str, str], dict[str, Any]] = {}
    for row in rows:
        key = (str(row["disclosure_number"]), str(row["period_key"]))
        bucket = grouped.setdefault(
            key,
            {
                "row": row,
                "values": {},
            },
        )
        bucket["values"][str(row["metric_base"])] = (
            _to_float(row["value_num"])
            if str(row["calc_status"] or "") == "ok"
            else None
        )

    sources: list[MarketSourcePeriod] = []
    for bucket in grouped.values():
        row = bucket["row"]
        security_code = _normalize_security_code(row["security_code"] or row["local_code"])
        period_end = str(row["period_end"] or "")
        if not security_code or not period_end:
            continue
        sources.append(
            MarketSourcePeriod(
                source_type="jquants",
                source_id=str(row["disclosure_number"]),
                edinet_code=str(row["edinet_code"] or ""),
                security_code=security_code,
                period_scope=str(row["period_scope"] or "quarter"),
                period_key=str(row["period_key"] or ""),
                quarter_type=str(row["quarter_type"] or "") or None,
                fiscal_year=int(row["fiscal_year"]) if row["fiscal_year"] is not None else None,
                period_end=period_end,
                values=bucket["values"],
            )
        )
    return sources


def _append_price_rows(
    rows: list[dict[str, Any]],
    source: MarketSourcePeriod,
    *,
    stock_price: float | None,
    quote_trade_date: str | None,
    price_source: str | None,
    include_pcfr: bool,
) -> None:
    stock_status = "ok" if stock_price is not None else "missing_input"
    detail = {
        "period_end": source.period_end,
        "quote_trade_date": quote_trade_date,
        "price_source": price_source,
    }
    rows.append(
        _build_row(
            source,
            metric_base="StockPrice",
            metric_group="market",
            value_num=stock_price,
            value_unit="yen_per_share",
            calc_status=stock_status,
            formula_name="stock_price_at_or_before_period_end",
            source_detail=detail,
        )
    )

    outstanding = source.values.get("OutstandingShares")
    market_cap = stock_price * outstanding if stock_price is not None and outstanding is not None else None
    rows.append(
        _build_row(
            source,
            metric_base="MarketCapitalization",
            metric_group="market",
            value_num=market_cap,
            value_unit="yen",
            calc_status="ok" if market_cap is not None else "missing_input",
            formula_name="market_capitalization",
            source_detail={**detail, "outstanding_shares": outstanding},
        )
    )

    for metric_base, denominator_base, formula_name in (
        ("PBR", "BPS", "pbr"),
        ("PER", "EPS", "per"),
    ):
        denominator = source.values.get(denominator_base)
        value, status = _ratio(stock_price, denominator)
        rows.append(
            _build_row(
                source,
                metric_base=metric_base,
                metric_group="market",
                value_num=value,
                value_unit="ratio",
                calc_status=status,
                formula_name=formula_name,
                source_detail={**detail, denominator_base: denominator},
            )
        )

    if include_pcfr:
        denominator = source.values.get("OperatingCashPerShare")
        value, status = _ratio(stock_price, denominator)
        rows.append(
            _build_row(
                source,
                metric_base="PCFR",
                metric_group="market",
                value_num=value,
                value_unit="ratio",
                calc_status=status,
                formula_name="pcfr",
                source_detail={**detail, "OperatingCashPerShare": denominator},
            )
        )


def _append_stock_growth_rows(
    rows: list[dict[str, Any]],
    current_annual_sources: list[MarketSourcePeriod],
    reference_annual_sources: list[MarketSourcePeriod],
    price_by_source: dict[str, float | None],
) -> None:
    by_code: dict[str, list[MarketSourcePeriod]] = {}
    current_ids = {source.source_id for source in current_annual_sources}
    for source in reference_annual_sources:
        by_code.setdefault(source.security_code, []).append(source)

    for sources in by_code.values():
        ordered = sorted(sources, key=lambda item: item.period_end, reverse=True)
        for index, source in enumerate(ordered):
            if source.source_id not in current_ids:
                continue
            current_price = price_by_source.get(source.source_id)
            for metric_base, period_offset, formula_name in (
                ("StockPriceGrowthRate", 1, "stock_price_growth_rate"),
                ("StockPriceGrowthRate5Year", 4, "stock_price_growth_rate_5year"),
                ("StockPriceGrowthRate10Year", 9, "stock_price_growth_rate_10year"),
            ):
                prior = ordered[index + period_offset] if index + period_offset < len(ordered) else None
                prior_price = price_by_source.get(prior.source_id) if prior is not None else None
                value, status = _ratio(current_price, prior_price)
                rows.append(
                    _build_row(
                        source,
                        metric_base=metric_base,
                        metric_group="growth",
                        value_num=value,
                        value_unit="ratio",
                        calc_status=status,
                        formula_name=formula_name,
                        source_detail={
                            "current_stock_price": current_price,
                            "prior_stock_price": prior_price,
                            "price_source": "jquants_daily_quotes.adjustment_close_rounded",
                            "prior_source_id": prior.source_id if prior is not None else None,
                            "prior_period_end": prior.period_end if prior is not None else None,
                        },
                    )
                )


def _append_jquants_theoretical_rows(rows: list[dict[str, Any]], sources: list[MarketSourcePeriod]) -> None:
    theoretical_by_source: dict[str, float | None] = {}
    for source in sources:
        theoretical, detail = _quarter_theoretical_share_price(source.values)
        theoretical_by_source[source.source_id] = theoretical
        rows.append(
            _build_row(
                source,
                metric_base="TheoreticalSharePrice",
                metric_group="valuation",
                value_num=theoretical,
                value_unit="yen_per_share",
                calc_status="ok" if theoretical is not None else "missing_input",
                formula_name="quarter_theoretical_share_price",
                source_detail=detail,
            )
        )
        for metric_base, denominator_base, formula_name in (
            ("TheoreticalPBR", "BPS", "theoretical_pbr"),
            ("TheoreticalPER", "EPS", "theoretical_per"),
        ):
            denominator = source.values.get(denominator_base)
            value, status = _ratio(theoretical, denominator)
            rows.append(
                _build_row(
                    source,
                    metric_base=metric_base,
                    metric_group="valuation",
                    value_num=value,
                    value_unit="ratio",
                    calc_status=status,
                    formula_name=formula_name,
                    source_detail={"theoretical_share_price": theoretical, denominator_base: denominator},
                )
            )

    by_code_quarter: dict[tuple[str, str], list[MarketSourcePeriod]] = {}
    for source in sources:
        if source.quarter_type:
            by_code_quarter.setdefault((source.security_code, source.quarter_type), []).append(source)
    for source_list in by_code_quarter.values():
        ordered = sorted(source_list, key=lambda item: item.period_end, reverse=True)
        for index, source in enumerate(ordered):
            prior = ordered[index + 1] if index + 1 < len(ordered) else None
            current_value = theoretical_by_source.get(source.source_id)
            prior_value = theoretical_by_source.get(prior.source_id) if prior is not None else None
            value, status = _ratio(current_value, prior_value)
            rows.append(
                _build_row(
                    source,
                    metric_base="TheoreticalSharePriceGrowthRate",
                    metric_group="growth",
                    value_num=value,
                    value_unit="ratio",
                    calc_status=status,
                    formula_name="quarter_theoretical_share_price_growth_rate",
                    source_detail={
                        "current_theoretical_share_price": current_value,
                        "prior_theoretical_share_price": prior_value,
                        "prior_source_id": prior.source_id if prior is not None else None,
                    },
                )
            )


def _append_jquants_stock_growth_rows(
    rows: list[dict[str, Any]],
    sources: list[MarketSourcePeriod],
    price_by_source: dict[str, float | None],
) -> None:
    by_code_quarter: dict[tuple[str, str], list[MarketSourcePeriod]] = {}
    for source in sources:
        if source.quarter_type:
            by_code_quarter.setdefault((source.security_code, source.quarter_type), []).append(source)

    for source_list in by_code_quarter.values():
        ordered = sorted(source_list, key=lambda item: item.period_end, reverse=True)
        for index, source in enumerate(ordered):
            prior = ordered[index + 1] if index + 1 < len(ordered) else None
            current_price = price_by_source.get(source.source_id)
            prior_price = price_by_source.get(prior.source_id) if prior is not None else None
            value, status = _ratio(current_price, prior_price)
            rows.append(
                _build_row(
                    source,
                    metric_base="StockPriceGrowthRate",
                    metric_group="growth",
                    value_num=value,
                    value_unit="ratio",
                    calc_status=status,
                    formula_name="quarter_stock_price_growth_rate",
                    source_detail={
                        "current_stock_price": current_price,
                        "prior_stock_price": prior_price,
                        "price_source": "jquants_daily_quotes.adjustment_close_rounded",
                        "prior_source_id": prior.source_id if prior is not None else None,
                        "prior_period_end": prior.period_end if prior is not None else None,
                    },
                )
            )


def build_market_derived_metrics(
    conn: sqlite3.Connection,
    *,
    date_from: str | None = None,
    date_to: str | None = None,
    codes: list[str] | None = None,
    period_scopes: set[str] | None = None,
    max_lookback_days: int = 10,
) -> tuple[list[dict[str, Any]], int, list[str]]:
    conn.row_factory = sqlite3.Row
    codes = [_normalize_security_code(code) for code in (codes or []) if _normalize_security_code(code)]
    period_scopes = period_scopes or {"annual", "quarter"}
    rows: list[dict[str, Any]] = []
    warnings: list[str] = []
    missing_quotes = 0

    edinet_sources = _fetch_edinet_sources(conn, date_from=date_from, date_to=date_to, codes=codes)
    all_edinet_sources_for_growth = _fetch_edinet_sources(conn, date_from=None, date_to=None, codes=codes)
    if "annual" not in period_scopes:
        edinet_sources = [source for source in edinet_sources if source.period_scope != "annual"]
    if "quarter" not in period_scopes:
        edinet_sources = [source for source in edinet_sources if source.period_scope != "quarter"]
    for source in edinet_sources:
        stock_price, quote_trade_date, price_source = _quote_on_or_before(
            conn,
            source.security_code,
            source.period_end,
            max_lookback_days=max_lookback_days,
            price_kind="raw",
        )
        if stock_price is None:
            missing_quotes += 1
        _append_price_rows(
            rows,
            source,
            stock_price=stock_price,
            quote_trade_date=quote_trade_date,
            price_source=price_source,
            include_pcfr=True,
        )
    annual_reference_sources = [
        source for source in all_edinet_sources_for_growth if source.period_scope == "annual"
    ]
    annual_reference_prices = {
        source.source_id: _quote_on_or_before(
            conn,
            source.security_code,
            source.period_end,
            max_lookback_days=max_lookback_days,
            price_kind="adjusted",
        )[0]
        for source in annual_reference_sources
    }
    _append_stock_growth_rows(
        rows,
        [source for source in edinet_sources if source.period_scope == "annual"],
        annual_reference_sources,
        annual_reference_prices,
    )

    if "quarter" in period_scopes:
        jquants_sources = _fetch_jquants_sources(conn, date_from=date_from, date_to=date_to, codes=codes)
        jquants_price_by_source: dict[str, float | None] = {}
        for source in jquants_sources:
            stock_price, quote_trade_date, price_source = _quote_on_or_before(
                conn,
                source.security_code,
                source.period_end,
                max_lookback_days=max_lookback_days,
                price_kind="raw",
            )
            adjusted_price, _, _ = _quote_on_or_before(
                conn,
                source.security_code,
                source.period_end,
                max_lookback_days=max_lookback_days,
                price_kind="adjusted",
            )
            jquants_price_by_source[source.source_id] = adjusted_price
            if stock_price is None:
                missing_quotes += 1
            _append_price_rows(
                rows,
                source,
                stock_price=stock_price,
                quote_trade_date=quote_trade_date,
                price_source=price_source,
                include_pcfr=False,
            )
        _append_jquants_stock_growth_rows(rows, jquants_sources, jquants_price_by_source)
        _append_jquants_theoretical_rows(rows, jquants_sources)

    if missing_quotes:
        warnings.append(f"missing_quotes={missing_quotes}")
    return rows, missing_quotes, warnings


def upsert_market_derived_metrics(conn: sqlite3.Connection, rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0
    now = _now_text()
    prepared = []
    for row in rows:
        prepared.append({**row, "created_at": now, "updated_at": now})
    conn.executemany(
        """
        INSERT INTO market_derived_metrics (
            source_type, source_id, edinet_code, security_code,
            period_scope, period_key, quarter_type, fiscal_year, period_end,
            metric_key, metric_base, metric_group, value_num, value_unit,
            calc_status, formula_name, source_detail_json, rule_version,
            created_at, updated_at
        ) VALUES (
            :source_type, :source_id, :edinet_code, :security_code,
            :period_scope, :period_key, :quarter_type, :fiscal_year, :period_end,
            :metric_key, :metric_base, :metric_group, :value_num, :value_unit,
            :calc_status, :formula_name, :source_detail_json, :rule_version,
            :created_at, :updated_at
        )
        ON CONFLICT(source_type, source_id, period_key, metric_key) DO UPDATE SET
            edinet_code = excluded.edinet_code,
            security_code = excluded.security_code,
            period_scope = excluded.period_scope,
            quarter_type = excluded.quarter_type,
            fiscal_year = excluded.fiscal_year,
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
        prepared,
    )
    conn.commit()
    return len(prepared)


def save_market_derived_metrics(
    conn: sqlite3.Connection,
    *,
    date_from: str | None = None,
    date_to: str | None = None,
    codes: list[str] | None = None,
    period_scopes: set[str] | None = None,
    max_lookback_days: int = 10,
    apply: bool = False,
    output_dir: str | Path | None = None,
) -> MarketDerivedResult:
    rows, missing_quotes, warnings = build_market_derived_metrics(
        conn,
        date_from=date_from,
        date_to=date_to,
        codes=codes,
        period_scopes=period_scopes,
        max_lookback_days=max_lookback_days,
    )
    if apply:
        upsert_market_derived_metrics(conn, rows)

    output_path: Path | None = None
    if output_dir:
        out_dir = Path(output_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        output_path = out_dir / f"market_derived_metrics_{date_from or 'all'}_to_{date_to or 'all'}_{datetime.now():%Y%m%d_%H%M%S}.txt"
        by_status: dict[str, int] = {}
        by_metric: dict[str, int] = {}
        for row in rows:
            by_status[str(row["calc_status"])] = by_status.get(str(row["calc_status"]), 0) + 1
            by_metric[str(row["metric_base"])] = by_metric.get(str(row["metric_base"]), 0) + 1
        lines = [
            f"generated_at: {datetime.now().isoformat(timespec='seconds')}",
            f"apply: {apply}",
            f"date_from: {date_from or 'all'}",
            f"date_to: {date_to or 'all'}",
            f"rows: {len(rows)}",
            f"missing_quotes: {missing_quotes}",
            "",
            "[status]",
            *[f"{key}: {value}" for key, value in sorted(by_status.items())],
            "",
            "[metrics]",
            *[f"{key}: {value}" for key, value in sorted(by_metric.items())],
        ]
        if warnings:
            lines.extend(["", "[warnings]", *warnings])
        output_path.write_text("\n".join(lines) + "\n", encoding="utf-8-sig")

    return MarketDerivedResult(
        rows=rows,
        output_path=output_path,
        missing_quotes=missing_quotes,
        warnings=warnings,
    )
