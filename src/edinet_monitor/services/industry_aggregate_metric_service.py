from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import json
from pathlib import Path
import sqlite3
from typing import Any

from edinet_monitor.config.settings import DEFAULT_DERIVED_METRICS_RULE_VERSION


INDUSTRY_AGGREGATE_PERIOD_SCOPE = "annual"
INDUSTRY_AGGREGATE_FORM_TYPE = "030000"

SUM_METRIC_BASES = [
    "NetSales",
    "GrossProfit",
    "CostOfSalesAndSellingGeneralAndAdministrativeExpenses",
    "CostOfSales",
    "SellingExpenses",
    "GeneralAndAdministrativeExpenses",
    "SellingExpensesOnly",
    "OperatingIncome",
    "OrdinaryIncome",
    "ProfitLoss",
    "TotalAssets",
    "NetAssets",
    "BeginningCashBalance",
    "CashAndCashEquivalents",
    "OperatingCash",
    "InvestmentCash",
    "FinancingCash",
    "FCF",
    "InterestBearingDebt",
    "OutstandingShares",
    "NumberOfEmployees",
]

FORMULA_METRIC_BASES = [
    "EstimatedNetIncome",
    "EPS",
    "EPSGrowthRate",
    "EPSGrowthRate5Year",
    "EPSGrowthRate10Year",
    "BPS",
    "BPSGrowthRate",
    "BPSGrowthRate5Year",
    "BPSGrowthRate10Year",
    "NetSalesGrowthRate",
    "NetSalesGrowthRate5Year",
    "NetSalesGrowthRate10Year",
    "OrdinaryIncomeGrowthRate",
    "OrdinaryIncomeGrowthRate5Year",
    "OrdinaryIncomeGrowthRate10Year",
    "CashBalanceGrowthRate",
    "CashBalanceGrowthRate5Year",
    "CashBalanceGrowthRate10Year",
    "ROA",
    "ROE",
    "EquityRatio",
    "AverageAge",
    "AverageLengthOfService",
    "AverageAnnualSalary",
    "CostOfSalesRatio",
    "GrossProfitMargin",
    "SellingExpensesRatio",
    "OperatingMargin",
    "OrdinaryIncomeMargin",
    "EstimatedNetMargin",
]

INDUSTRY_AGGREGATE_ROW_BASES = [
    *SUM_METRIC_BASES,
    *[
        base
        for base in FORMULA_METRIC_BASES
        if base
        not in {
            "CostOfSalesRatio",
            "GrossProfitMargin",
            "SellingExpensesRatio",
            "OperatingMargin",
            "OrdinaryIncomeMargin",
            "EstimatedNetMargin",
        }
    ],
]

INDUSTRY_AGGREGATE_SOURCE_BASES = sorted(
    set(
        SUM_METRIC_BASES
        + [
            "AverageAge",
            "AverageLengthOfService",
            "AverageAnnualSalary",
        ]
    )
)

METRIC_GROUP_BY_BASE = {
    "NetSales": "profitability",
    "GrossProfit": "profitability",
    "CostOfSalesAndSellingGeneralAndAdministrativeExpenses": "profitability",
    "CostOfSales": "profitability",
    "SellingExpenses": "profitability",
    "GeneralAndAdministrativeExpenses": "profitability",
    "SellingExpensesOnly": "profitability",
    "OperatingIncome": "profitability",
    "OrdinaryIncome": "profitability",
    "ProfitLoss": "profitability",
    "EstimatedNetIncome": "profitability",
    "CostOfSalesRatio": "profitability",
    "GrossProfitMargin": "profitability",
    "SellingExpensesRatio": "profitability",
    "OperatingMargin": "profitability",
    "OrdinaryIncomeMargin": "profitability",
    "EstimatedNetMargin": "profitability",
    "TotalAssets": "balance",
    "NetAssets": "balance",
    "BeginningCashBalance": "balance",
    "CashAndCashEquivalents": "balance",
    "InterestBearingDebt": "safety",
    "OperatingCash": "cashflow",
    "InvestmentCash": "cashflow",
    "FinancingCash": "cashflow",
    "FCF": "cashflow",
    "OutstandingShares": "share",
    "EPS": "share",
    "EPSGrowthRate": "growth",
    "EPSGrowthRate5Year": "growth",
    "EPSGrowthRate10Year": "growth",
    "BPS": "share",
    "BPSGrowthRate": "growth",
    "BPSGrowthRate5Year": "growth",
    "BPSGrowthRate10Year": "growth",
    "NetSalesGrowthRate": "growth",
    "NetSalesGrowthRate5Year": "growth",
    "NetSalesGrowthRate10Year": "growth",
    "OrdinaryIncomeGrowthRate": "growth",
    "OrdinaryIncomeGrowthRate5Year": "growth",
    "OrdinaryIncomeGrowthRate10Year": "growth",
    "CashBalanceGrowthRate": "growth",
    "CashBalanceGrowthRate5Year": "growth",
    "CashBalanceGrowthRate10Year": "growth",
    "ROA": "return",
    "ROE": "return",
    "EquityRatio": "return",
    "NumberOfEmployees": "workforce",
    "AverageAge": "workforce",
    "AverageLengthOfService": "workforce",
    "AverageAnnualSalary": "workforce",
}

RATIO_BASES = {
    "EPSGrowthRate",
    "EPSGrowthRate5Year",
    "EPSGrowthRate10Year",
    "BPSGrowthRate",
    "BPSGrowthRate5Year",
    "BPSGrowthRate10Year",
    "NetSalesGrowthRate",
    "NetSalesGrowthRate5Year",
    "NetSalesGrowthRate10Year",
    "OrdinaryIncomeGrowthRate",
    "OrdinaryIncomeGrowthRate5Year",
    "OrdinaryIncomeGrowthRate10Year",
    "CashBalanceGrowthRate",
    "CashBalanceGrowthRate5Year",
    "CashBalanceGrowthRate10Year",
    "ROA",
    "ROE",
    "EquityRatio",
    "CostOfSalesRatio",
    "GrossProfitMargin",
    "SellingExpensesRatio",
    "OperatingMargin",
    "OrdinaryIncomeMargin",
    "EstimatedNetMargin",
}

YEN_BASES = {
    "NetSales",
    "GrossProfit",
    "CostOfSalesAndSellingGeneralAndAdministrativeExpenses",
    "CostOfSales",
    "SellingExpenses",
    "GeneralAndAdministrativeExpenses",
    "SellingExpensesOnly",
    "OperatingIncome",
    "OrdinaryIncome",
    "ProfitLoss",
    "EstimatedNetIncome",
    "TotalAssets",
    "NetAssets",
    "BeginningCashBalance",
    "CashAndCashEquivalents",
    "OperatingCash",
    "InvestmentCash",
    "FinancingCash",
    "FCF",
    "InterestBearingDebt",
    "AverageAnnualSalary",
}


@dataclass(frozen=True)
class IndustryAggregateBuildResult:
    rows: list[dict[str, Any]]
    industry_count: int
    fiscal_year_count: int
    source_company_count: int
    ok_count: int
    missing_count: int


def metric_key_for_base(metric_base: str) -> str:
    return f"{metric_base}Current"


def industry_aggregate_table_exists(conn: sqlite3.Connection) -> bool:
    row = conn.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type = 'table'
          AND name = 'industry_aggregate_metrics'
        """
    ).fetchone()
    return row is not None


def _chunked(items: list[str], size: int) -> list[list[str]]:
    return [items[index : index + size] for index in range(0, len(items), size)]


def _fetch_latest_annual_filings(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return conn.execute(
        """
        WITH base AS (
          SELECT
            f.doc_id,
            f.edinet_code,
            COALESCE(im.security_code, f.security_code) AS security_code,
            im.industry_33,
            f.period_end,
            f.submit_date,
            CAST(substr(f.period_end, 1, 4) AS INTEGER) AS fiscal_year,
            ROW_NUMBER() OVER (
              PARTITION BY f.edinet_code, CAST(substr(f.period_end, 1, 4) AS INTEGER)
              ORDER BY f.period_end DESC, COALESCE(f.submit_date, '') DESC, f.doc_id DESC
            ) AS rn
          FROM filings f
          JOIN issuer_master im
            ON im.edinet_code = f.edinet_code
          WHERE f.form_type = ?
            AND f.parse_status = 'derived_metrics_saved'
            AND COALESCE(im.is_listed, 0) = 1
            AND COALESCE(im.exchange, '') = 'TSE'
            AND COALESCE(im.industry_33, '') <> ''
            AND length(COALESCE(f.period_end, '')) >= 4
        )
        SELECT *
        FROM base
        WHERE rn = 1
        ORDER BY industry_33, fiscal_year, edinet_code
        """,
        (INDUSTRY_AGGREGATE_FORM_TYPE,),
    ).fetchall()


def _fetch_metric_values(
    conn: sqlite3.Connection,
    *,
    doc_ids: list[str],
    metric_bases: list[str],
) -> dict[tuple[str, str], float | None]:
    if not doc_ids or not metric_bases:
        return {}

    metric_keys = [metric_key_for_base(base) for base in metric_bases]
    key_placeholders = ",".join("?" for _ in metric_keys)
    values: dict[tuple[str, str], float | None] = {}
    unique_doc_ids = sorted(set(doc_ids))

    for doc_chunk in _chunked(unique_doc_ids, 800):
        doc_placeholders = ",".join("?" for _ in doc_chunk)
        rows = conn.execute(
            f"""
            SELECT doc_id, metric_key, value_num
            FROM normalized_metrics
            WHERE doc_id IN ({doc_placeholders})
              AND metric_key IN ({key_placeholders})
            """,
            [*doc_chunk, *metric_keys],
        ).fetchall()
        for row in rows:
            values[(str(row["doc_id"]), str(row["metric_key"]))] = row["value_num"]

    for doc_chunk in _chunked(unique_doc_ids, 800):
        doc_placeholders = ",".join("?" for _ in doc_chunk)
        rows = conn.execute(
            f"""
            SELECT doc_id, metric_key, value_num, calc_status
            FROM derived_metrics
            WHERE doc_id IN ({doc_placeholders})
              AND metric_key IN ({key_placeholders})
            """,
            [*doc_chunk, *metric_keys],
        ).fetchall()
        for row in rows:
            key = (str(row["doc_id"]), str(row["metric_key"]))
            if key in values and values[key] is not None:
                continue
            values[key] = None if str(row["calc_status"] or "") == "missing_input" else row["value_num"]

    return values


def _value_unit(metric_base: str) -> str:
    if metric_base in RATIO_BASES:
        return "ratio"
    if metric_base in {"EPS", "BPS"}:
        return "yen_per_share"
    if metric_base == "OutstandingShares":
        return "shares"
    if metric_base == "NumberOfEmployees":
        return "person"
    if metric_base in {"AverageAge", "AverageLengthOfService"}:
        return "year"
    if metric_base in YEN_BASES:
        return "yen"
    return "number"


def _period_start(fiscal_year: int) -> str:
    return f"{fiscal_year:04d}-01-01"


def _period_end(fiscal_year: int) -> str:
    return f"{fiscal_year:04d}-12-31"


def _ratio(
    numerator: float | None,
    denominator: float | None,
    *,
    require_positive_denominator: bool = True,
) -> tuple[float | None, str]:
    if numerator is None or denominator is None:
        return None, "missing_input"
    if require_positive_denominator and denominator <= 0:
        return None, "zero_or_negative_base"
    if denominator == 0:
        return None, "division_by_zero"
    return numerator / denominator, "ok"


def _sum_value(bucket: dict[str, Any], metric_base: str) -> float | None:
    if bucket["counts"].get(metric_base, 0) <= 0:
        return None
    return bucket["sums"].get(metric_base)


def _estimated_net_income(bucket: dict[str, Any]) -> tuple[float | None, str]:
    ordinary_income = _sum_value(bucket, "OrdinaryIncome")
    if ordinary_income is None:
        return None, "missing_input"
    return ordinary_income * 0.7, "ok"


def _eps(bucket: dict[str, Any]) -> tuple[float | None, str]:
    estimated_net_income, estimated_status = _estimated_net_income(bucket)
    outstanding_shares = _sum_value(bucket, "OutstandingShares")
    value, status = _ratio(
        estimated_net_income,
        outstanding_shares,
        require_positive_denominator=True,
    )
    if status == "missing_input" and estimated_status != "ok":
        status = estimated_status
    return value, status


def _weighted_average(bucket: dict[str, Any], metric_base: str) -> tuple[float | None, str, int]:
    weight = bucket["weighted_employee_sum"].get(metric_base)
    weighted = bucket["weighted_sums"].get(metric_base)
    count = bucket["weighted_counts"].get(metric_base, 0)
    if weighted is None or weight is None or weight <= 0:
        return None, "missing_input", 0
    return weighted / weight, "ok", count


def _source_count_for_ratio(bucket: dict[str, Any], *bases: str) -> int:
    counts = [bucket["counts"].get(base, 0) for base in bases]
    return min(counts) if counts else 0


def _build_row(
    *,
    industry: str,
    fiscal_year: int,
    metric_base: str,
    value_num: float | None,
    calc_status: str,
    formula_name: str,
    source_company_count: int,
    source_detail: dict[str, Any],
    rule_version: str,
) -> dict[str, Any]:
    return {
        "industry_33": industry,
        "period_scope": INDUSTRY_AGGREGATE_PERIOD_SCOPE,
        "fiscal_year": fiscal_year,
        "period_bucket_start": _period_start(fiscal_year),
        "period_bucket_end": _period_end(fiscal_year),
        "metric_key": metric_key_for_base(metric_base),
        "metric_base": metric_base,
        "metric_group": METRIC_GROUP_BY_BASE.get(metric_base, "industry"),
        "value_num": value_num,
        "value_unit": _value_unit(metric_base),
        "calc_status": calc_status,
        "formula_name": formula_name,
        "source_company_count": source_company_count,
        "source_detail_json": source_detail,
        "rule_version": rule_version,
    }


def build_industry_aggregate_metric_rows(
    conn: sqlite3.Connection,
    *,
    rule_version: str = DEFAULT_DERIVED_METRICS_RULE_VERSION,
) -> IndustryAggregateBuildResult:
    conn.row_factory = sqlite3.Row
    filings = _fetch_latest_annual_filings(conn)
    doc_ids = [str(row["doc_id"]) for row in filings]
    metric_values = _fetch_metric_values(
        conn,
        doc_ids=doc_ids,
        metric_bases=INDUSTRY_AGGREGATE_SOURCE_BASES,
    )

    buckets: dict[tuple[str, int], dict[str, Any]] = {}
    source_companies: set[str] = set()
    for filing in filings:
        industry = str(filing["industry_33"] or "")
        fiscal_year = int(filing["fiscal_year"])
        doc_id = str(filing["doc_id"])
        key = (industry, fiscal_year)
        source_companies.add(str(filing["edinet_code"]))
        bucket = buckets.setdefault(
            key,
            {
                "sums": {},
                "counts": {},
                "company_count": 0,
                "weighted_sums": {},
                "weighted_employee_sum": {},
                "weighted_counts": {},
            },
        )
        bucket["company_count"] += 1

        values_by_base: dict[str, float] = {}
        for base in INDUSTRY_AGGREGATE_SOURCE_BASES:
            value = metric_values.get((doc_id, metric_key_for_base(base)))
            if value is None:
                continue
            numeric_value = float(value)
            values_by_base[base] = numeric_value
            if base in SUM_METRIC_BASES:
                bucket["sums"][base] = bucket["sums"].get(base, 0.0) + numeric_value
                bucket["counts"][base] = bucket["counts"].get(base, 0) + 1

        employee_count = values_by_base.get("NumberOfEmployees")
        if employee_count is None or employee_count <= 0:
            continue
        for base in ("AverageAge", "AverageLengthOfService", "AverageAnnualSalary"):
            value = values_by_base.get(base)
            if value is None:
                continue
            bucket["weighted_sums"][base] = bucket["weighted_sums"].get(base, 0.0) + value * employee_count
            bucket["weighted_employee_sum"][base] = (
                bucket["weighted_employee_sum"].get(base, 0.0) + employee_count
            )
            bucket["weighted_counts"][base] = bucket["weighted_counts"].get(base, 0) + 1

    rows: list[dict[str, Any]] = []
    eps_by_key: dict[tuple[str, int], float | None] = {}
    bps_by_key: dict[tuple[str, int], float | None] = {}
    for key, bucket in buckets.items():
        eps_by_key[key] = _eps(bucket)[0]
        bps_by_key[key] = _ratio(
            _sum_value(bucket, "NetAssets"),
            _sum_value(bucket, "OutstandingShares"),
            require_positive_denominator=True,
        )[0]

    for (industry, fiscal_year), bucket in sorted(buckets.items()):
        for base in SUM_METRIC_BASES:
            count = int(bucket["counts"].get(base, 0))
            value = _sum_value(bucket, base)
            rows.append(
                _build_row(
                    industry=industry,
                    fiscal_year=fiscal_year,
                    metric_base=base,
                    value_num=value,
                    calc_status="ok" if value is not None else "missing_input",
                    formula_name="industry_sum",
                    source_company_count=count,
                    source_detail={"formula": "sum(company_values)", "input_base": base},
                    rule_version=rule_version,
                )
            )

        estimated_net_income, estimated_status = _estimated_net_income(bucket)
        rows.append(
            _build_row(
                industry=industry,
                fiscal_year=fiscal_year,
                metric_base="EstimatedNetIncome",
                value_num=estimated_net_income,
                calc_status=estimated_status,
                formula_name="industry_estimated_net_income",
                source_company_count=int(bucket["counts"].get("OrdinaryIncome", 0)),
                source_detail={"formula": "sum(OrdinaryIncome) * 0.7"},
                rule_version=rule_version,
            )
        )

        eps_value, eps_status = _eps(bucket)
        rows.append(
            _build_row(
                industry=industry,
                fiscal_year=fiscal_year,
                metric_base="EPS",
                value_num=eps_value,
                calc_status=eps_status,
                formula_name="industry_eps",
                source_company_count=_source_count_for_ratio(bucket, "OrdinaryIncome", "OutstandingShares"),
                source_detail={"formula": "sum(OrdinaryIncome) * 0.7 / sum(OutstandingShares)"},
                rule_version=rule_version,
            )
        )

        bps_value, bps_status = _ratio(
            _sum_value(bucket, "NetAssets"),
            _sum_value(bucket, "OutstandingShares"),
            require_positive_denominator=True,
        )
        rows.append(
            _build_row(
                industry=industry,
                fiscal_year=fiscal_year,
                metric_base="BPS",
                value_num=bps_value,
                calc_status=bps_status,
                formula_name="industry_bps",
                source_company_count=_source_count_for_ratio(bucket, "NetAssets", "OutstandingShares"),
                source_detail={"formula": "sum(NetAssets) / sum(OutstandingShares)"},
                rule_version=rule_version,
            )
        )

        ratio_specs = [
            ("GrossProfitMargin", "GrossProfit", "NetSales", "gross_profit_margin"),
            ("CostOfSalesRatio", "CostOfSales", "NetSales", "cost_of_sales_ratio"),
            ("SellingExpensesRatio", "SellingExpenses", "NetSales", "selling_expenses_ratio"),
            ("OperatingMargin", "OperatingIncome", "NetSales", "operating_margin"),
            ("OrdinaryIncomeMargin", "OrdinaryIncome", "NetSales", "ordinary_income_margin"),
            ("EstimatedNetMargin", "ProfitLoss", "NetSales", "net_margin"),
            ("ROA", "EstimatedNetIncome", "TotalAssets", "industry_roa"),
            ("ROE", "EstimatedNetIncome", "NetAssets", "industry_roe"),
            ("EquityRatio", "NetAssets", "TotalAssets", "industry_equity_ratio"),
        ]
        for base, numerator_base, denominator_base, formula_name in ratio_specs:
            numerator = (
                estimated_net_income
                if numerator_base == "EstimatedNetIncome"
                else _sum_value(bucket, numerator_base)
            )
            value, status = _ratio(numerator, _sum_value(bucket, denominator_base))
            source_bases = (
                ("OrdinaryIncome", denominator_base)
                if numerator_base == "EstimatedNetIncome"
                else (numerator_base, denominator_base)
            )
            rows.append(
                _build_row(
                    industry=industry,
                    fiscal_year=fiscal_year,
                    metric_base=base,
                    value_num=value,
                    calc_status=status,
                    formula_name=formula_name,
                    source_company_count=_source_count_for_ratio(bucket, *source_bases),
                    source_detail={
                        "formula": f"sum({numerator_base}) / sum({denominator_base})",
                        "input_bases": list(source_bases),
                    },
                    rule_version=rule_version,
                )
            )

        weighted_specs = [
            ("AverageAge", "industry_weighted_average_age"),
            ("AverageLengthOfService", "industry_weighted_average_length_of_service"),
            ("AverageAnnualSalary", "industry_weighted_average_annual_salary"),
        ]
        for base, formula_name in weighted_specs:
            value, status, count = _weighted_average(bucket, base)
            rows.append(
                _build_row(
                    industry=industry,
                    fiscal_year=fiscal_year,
                    metric_base=base,
                    value_num=value,
                    calc_status=status,
                    formula_name=formula_name,
                    source_company_count=count,
                    source_detail={
                        "formula": f"sum({base} * NumberOfEmployees) / sum(NumberOfEmployees)",
                        "input_bases": [base, "NumberOfEmployees"],
                    },
                    rule_version=rule_version,
                )
            )

        growth_specs = [
            ("EPSGrowthRate", "EPS", 1),
            ("EPSGrowthRate5Year", "EPS", 4),
            ("EPSGrowthRate10Year", "EPS", 9),
            ("BPSGrowthRate", "BPS", 1),
            ("BPSGrowthRate5Year", "BPS", 4),
            ("BPSGrowthRate10Year", "BPS", 9),
            ("NetSalesGrowthRate", "NetSales", 1),
            ("NetSalesGrowthRate5Year", "NetSales", 4),
            ("NetSalesGrowthRate10Year", "NetSales", 9),
            ("OrdinaryIncomeGrowthRate", "OrdinaryIncome", 1),
            ("OrdinaryIncomeGrowthRate5Year", "OrdinaryIncome", 4),
            ("OrdinaryIncomeGrowthRate10Year", "OrdinaryIncome", 9),
            ("CashBalanceGrowthRate", "CashAndCashEquivalents", 1),
            ("CashBalanceGrowthRate5Year", "CashAndCashEquivalents", 4),
            ("CashBalanceGrowthRate10Year", "CashAndCashEquivalents", 9),
        ]
        for base, source_base, years_back in growth_specs:
            prior_bucket = buckets.get((industry, fiscal_year - years_back))
            if source_base == "EPS":
                current_value = eps_value
                prior_value = eps_by_key.get((industry, fiscal_year - years_back))
                source_count = _source_count_for_ratio(bucket, "OrdinaryIncome", "OutstandingShares")
            elif source_base == "BPS":
                current_value = bps_value
                prior_value = bps_by_key.get((industry, fiscal_year - years_back))
                source_count = _source_count_for_ratio(bucket, "NetAssets", "OutstandingShares")
            else:
                current_value = _sum_value(bucket, source_base)
                prior_value = _sum_value(prior_bucket, source_base) if prior_bucket else None
                source_count = int(bucket["counts"].get(source_base, 0))
            value, status = _ratio(current_value, prior_value)
            rows.append(
                _build_row(
                    industry=industry,
                    fiscal_year=fiscal_year,
                    metric_base=base,
                    value_num=value,
                    calc_status=status,
                    formula_name="industry_growth_ratio",
                    source_company_count=source_count,
                    source_detail={
                        "formula": "current_industry_value / prior_industry_value",
                        "source_base": source_base,
                        "years_back": years_back,
                    },
                    rule_version=rule_version,
                )
            )

    ok_count = sum(1 for row in rows if row["calc_status"] == "ok")
    missing_count = len(rows) - ok_count
    return IndustryAggregateBuildResult(
        rows=rows,
        industry_count=len({industry for industry, _year in buckets}),
        fiscal_year_count=len({year for _industry, year in buckets}),
        source_company_count=len(source_companies),
        ok_count=ok_count,
        missing_count=missing_count,
    )


def replace_industry_aggregate_metrics(
    conn: sqlite3.Connection,
    rows: list[dict[str, Any]],
) -> int:
    created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    prepared = [
        {
            **row,
            "source_detail_json": json.dumps(row["source_detail_json"], ensure_ascii=False),
            "created_at": created_at,
            "updated_at": created_at,
        }
        for row in rows
    ]
    conn.execute("DELETE FROM industry_aggregate_metrics WHERE period_scope = ?", (INDUSTRY_AGGREGATE_PERIOD_SCOPE,))
    if prepared:
        conn.executemany(
            """
            INSERT OR REPLACE INTO industry_aggregate_metrics (
                industry_33,
                period_scope,
                fiscal_year,
                period_bucket_start,
                period_bucket_end,
                metric_key,
                metric_base,
                metric_group,
                value_num,
                value_unit,
                calc_status,
                formula_name,
                source_company_count,
                source_detail_json,
                rule_version,
                created_at,
                updated_at
            ) VALUES (
                :industry_33,
                :period_scope,
                :fiscal_year,
                :period_bucket_start,
                :period_bucket_end,
                :metric_key,
                :metric_base,
                :metric_group,
                :value_num,
                :value_unit,
                :calc_status,
                :formula_name,
                :source_company_count,
                :source_detail_json,
                :rule_version,
                :created_at,
                :updated_at
            )
            """,
            prepared,
        )
    conn.commit()
    return len(prepared)


def count_industry_aggregate_metrics(conn: sqlite3.Connection) -> dict[str, int]:
    if not industry_aggregate_table_exists(conn):
        return {"total": 0, "ok": 0, "missing_input": 0}
    row = conn.execute(
        """
        SELECT
          COUNT(*) AS total,
          SUM(CASE WHEN calc_status = 'ok' THEN 1 ELSE 0 END) AS ok,
          SUM(CASE WHEN calc_status <> 'ok' THEN 1 ELSE 0 END) AS missing_input
        FROM industry_aggregate_metrics
        """
    ).fetchone()
    return {
        "total": int(row["total"] or 0),
        "ok": int(row["ok"] or 0),
        "missing_input": int(row["missing_input"] or 0),
    }


def write_industry_aggregate_report(
    *,
    output_dir: str | Path,
    mode: str,
    build_result: IndustryAggregateBuildResult,
    before_counts: dict[str, int],
    after_counts: dict[str, int] | None,
) -> Path:
    directory = Path(output_dir)
    directory.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = directory / f"industry_aggregate_metrics_{mode}_{timestamp}.txt"
    lines = [
        f"generated_at: {datetime.now().isoformat(timespec='seconds')}",
        f"mode: {mode}",
        f"source_company_count: {build_result.source_company_count}",
        f"industry_count: {build_result.industry_count}",
        f"fiscal_year_count: {build_result.fiscal_year_count}",
        f"built_rows: {len(build_result.rows)}",
        f"built_ok_rows: {build_result.ok_count}",
        f"built_missing_rows: {build_result.missing_count}",
        f"before_total_rows: {before_counts.get('total', 0)}",
        f"before_ok_rows: {before_counts.get('ok', 0)}",
        f"before_missing_rows: {before_counts.get('missing_input', 0)}",
    ]
    if after_counts is not None:
        lines.extend(
            [
                f"after_total_rows: {after_counts.get('total', 0)}",
                f"after_ok_rows: {after_counts.get('ok', 0)}",
                f"after_missing_rows: {after_counts.get('missing_input', 0)}",
            ]
        )
    lines.append("")
    lines.append("metric_base\tcalc_status\trow_count")
    summary: dict[tuple[str, str], int] = {}
    for row in build_result.rows:
        key = (str(row["metric_base"]), str(row["calc_status"]))
        summary[key] = summary.get(key, 0) + 1
    for (metric_base, calc_status), count in sorted(summary.items()):
        lines.append(f"{metric_base}\t{calc_status}\t{count}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8-sig")
    return path
