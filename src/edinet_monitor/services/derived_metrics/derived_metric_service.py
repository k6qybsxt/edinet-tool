from __future__ import annotations

from typing import Any

from edinet_monitor.config.settings import DEFAULT_DERIVED_METRICS_RULE_VERSION


ANNUAL_FORM_TYPES = {"030000"}
HALF_FORM_TYPES = {"043000"}
SUFFIX_TO_PERIOD_OFFSET = {
    "Current": 0,
    "Prior1": 1,
    "Prior2": 2,
    "Prior3": 3,
    "Prior4": 4,
}
FULL_SUFFIXES = ["Current", "Prior1", "Prior2", "Prior3", "Prior4"]
GROWTH_SUFFIXES = ["Current", "Prior1", "Prior2", "Prior3"]

SAFE_COST_OF_SALES_SOURCE_TAGS = {
    "CostOfSales",
    "CostOfSalesIFRS",
    "CostOfRevenue",
    "CostOfRevenueIFRS",
    "CostOfOperatingRevenue",
    "CostOfOperatingRevenueIFRS",
    "OperatingCosts",
    "OperatingCostsIFRS",
    "OperatingCost",
    "OperatingCost2",
    "CostOfRevenueFromContractsWithCustomersIFRS",
    "CostOfGoodsSold",
    "CostOfGoodsSoldIFRS",
    "CostOfSalesOfCompletedConstructionContractsCNS",
    "CostOfSalesOfCompletedConstructionContractsSummaryOfBusinessResults",
    "OperatingExpensesAndCostOfSalesOfTransportationRWY",
    "TotalBusinessExpensesCOSExpOA",
    "CostOfBusinessRevenue",
    "CostOfBusinessRevenueBusinessExpenses",
    "CostOfBusinessRevenueCOSExpOA",
    "CostOfRawMaterialsCOS",
    "CostOfCompletedWorkCOSExpOA",
}

SAFE_GROSS_PROFIT_SOURCE_TAGS = {
    "GrossProfit",
    "GrossProfitIFRS",
    "GrossProfitSummaryOfBusinessResults",
    "GrossProfitIFRSSummaryOfBusinessResults",
    "NetRevenueSummaryOfBusinessResults",
    "GrossProfitOnCompletedConstructionContractsCNS",
    "GrossProfitOnCompletedConstructionContractsSummaryOfBusinessResults",
    "BusinessGrossProfitOrLoss",
    "OperatingGrossProfit",
    "OperatingGrossProfitIFRS",
    "OperatingGrossProfitWAT",
    "OperatingGrossProfitNetGP",
}

SAFE_COMBINED_COST_AND_SGA_SOURCE_TAGS = {
    "CostOfSalesAndSellingGeneralAndAdministrativeExpensesIFRS",
    "OperatingExpensesIFRS",
    "OperatingExpenses",
    "OperatingExpensesOILTelecommunications",
    "ElectricUtilityOperatingExpensesELE",
    "ElectricUtilityOperatingExpenses",
    "BusinessExpenses",
    "OperatingExpensesOE",
    "OperatingExpensesINS",
    "OperatingCostsAndExpensesCOSExpOA",
    "OrdinaryExpensesBNK",
    "ExpenseIFRS",
}


def infer_period_scope(form_type: str) -> str | None:
    text = str(form_type or "").strip()
    if text in ANNUAL_FORM_TYPES:
        return "annual"
    if text in HALF_FORM_TYPES:
        return "half"
    return None


def scale_value_for_display(
    value_num: float | None,
    *,
    value_unit: str,
    document_display_unit: str | None,
) -> float | None:
    if value_num is None:
        return None
    if value_unit != "yen":
        return value_num
    if document_display_unit == "百万円":
        return value_num / 1_000_000
    if document_display_unit == "千円":
        return value_num / 1_000
    return value_num


def _build_metric_key(metric_base: str, suffix: str) -> str:
    return f"{metric_base}{suffix}"


def _metric_map(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {str(row["metric_key"]): dict(row) for row in rows}


def _metric_value(metric_rows: dict[str, dict[str, Any]], metric_key: str) -> float | None:
    row = metric_rows.get(metric_key)
    if not row:
        return None
    value_num = row.get("value_num")
    if value_num is None:
        return None
    return float(value_num)


def _metric_row(metric_rows: dict[str, dict[str, Any]], metric_key: str) -> dict[str, Any] | None:
    row = metric_rows.get(metric_key)
    if not row:
        return None
    return row


def _metric_source_tag(metric_rows: dict[str, dict[str, Any]], metric_key: str) -> str:
    row = _metric_row(metric_rows, metric_key)
    if not row:
        return ""
    return str(row.get("source_tag") or "")


def _pick_reference_row(
    metric_rows: dict[str, dict[str, Any]],
    metric_keys: list[str],
) -> dict[str, Any]:
    for metric_key in metric_keys:
        row = metric_rows.get(metric_key)
        if row:
            return row
    return {}


def _build_source_detail(
    *,
    inputs: dict[str, float | None],
    display_formula: str,
    stored_formula: str,
    calc_status: str,
    document_display_unit: str,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    detail = {
        "inputs": inputs,
        "display_formula": display_formula,
        "stored_formula": stored_formula,
        "calc_status": calc_status,
        "document_display_unit": document_display_unit,
    }
    if extra:
        detail.update(extra)
    return detail


def _build_derived_row(
    *,
    sample_row: dict[str, Any],
    metric_base: str,
    metric_group: str,
    suffix: str,
    value_num: float | None,
    value_unit: str,
    calc_status: str,
    formula_name: str,
    source_detail: dict[str, Any],
    accounting_standard: str,
    document_display_unit: str,
    rule_version: str,
) -> dict[str, Any]:
    return {
        "doc_id": str(sample_row.get("doc_id") or ""),
        "edinet_code": str(sample_row.get("edinet_code") or ""),
        "security_code": str(sample_row.get("security_code") or ""),
        "metric_key": _build_metric_key(metric_base, suffix),
        "metric_base": metric_base,
        "metric_group": metric_group,
        "fiscal_year": sample_row.get("fiscal_year"),
        "period_end": sample_row.get("period_end"),
        "period_scope": str(sample_row.get("period_scope") or ""),
        "period_offset": SUFFIX_TO_PERIOD_OFFSET[suffix],
        "consolidation": sample_row.get("consolidation"),
        "accounting_standard": accounting_standard,
        "document_display_unit": document_display_unit,
        "value_num": value_num,
        "value_unit": value_unit,
        "calc_status": calc_status,
        "formula_name": formula_name,
        "source_detail_json": source_detail,
        "rule_version": rule_version,
    }


def _single_metric_input(
    metric_rows: dict[str, dict[str, Any]],
    metric_base: str,
    suffix: str,
) -> dict[str, Any]:
    metric_key = _build_metric_key(metric_base, suffix)
    value_num = _metric_value(metric_rows, metric_key)
    return {
        "value_num": value_num,
        "detail_inputs": {metric_key: value_num},
        "reference_keys": [metric_key],
    }


def _ratio_status(
    *,
    numerator: float | None,
    denominator: float | None,
    require_positive_denominator: bool,
) -> tuple[float | None, str]:
    if numerator is None or denominator is None:
        return None, "missing_input"
    if require_positive_denominator and denominator <= 0:
        return None, "zero_or_negative_base"
    if denominator == 0:
        return None, "division_by_zero"
    return numerator / denominator, "ok"


def _difference_status(
    *,
    left_value: float | None,
    right_value: float | None,
) -> tuple[float | None, str]:
    if left_value is None or right_value is None:
        return None, "missing_input"
    return left_value - right_value, "ok"


def _outstanding_shares_status(
    *,
    issued_shares: float | None,
    treasury_shares: float | None,
) -> tuple[float | None, str, float]:
    if issued_shares is None:
        return None, "missing_input", 0.0
    if treasury_shares is None or treasury_shares < 1000:
        return issued_shares, "ok", 0.0
    return issued_shares - treasury_shares, "ok", treasury_shares


def _sum_status(
    *,
    left_value: float | None,
    right_value: float | None,
) -> tuple[float | None, str]:
    if left_value is None or right_value is None:
        return None, "missing_input"
    return left_value + right_value, "ok"


def _multiply_status(
    *,
    left_value: float | None,
    right_value: float | None,
) -> tuple[float | None, str]:
    if left_value is None or right_value is None:
        return None, "missing_input"
    return left_value * right_value, "ok"


def _scaled_status(
    *,
    value_num: float | None,
    scale: float,
) -> tuple[float | None, str]:
    if value_num is None:
        return None, "missing_input"
    return value_num * scale, "ok"


def _discount_evaluation_rate_status(
    *,
    equity_ratio: float | None,
) -> tuple[float | None, str]:
    if equity_ratio is None:
        return None, "missing_input"
    if equity_ratio >= 0.80:
        return 0.80, "ok"
    if equity_ratio >= 0.67:
        return 0.75, "ok"
    if equity_ratio >= 0.50:
        return 0.70, "ok"
    if equity_ratio >= 0.33:
        return 0.65, "ok"
    if equity_ratio >= 0.10:
        return 0.60, "ok"
    return 0.50, "ok"


def _financial_leverage_adjustment_status(
    *,
    netassets: float | None,
    totalassets: float | None,
) -> tuple[float | None, str, float | None, float | None]:
    if netassets is None or totalassets is None:
        return None, "missing_input", None, None
    if netassets * totalassets <= 0:
        return 0.0, "ok", None, None

    equity_ratio = netassets / totalassets
    raw_value = 1 / (equity_ratio + 0.33)
    if raw_value < 1:
        return 1.0, "ok", equity_ratio, raw_value
    if raw_value > 1.5:
        return 1.5, "ok", equity_ratio, raw_value
    return raw_value, "ok", equity_ratio, raw_value


def _sum_metric_input(
    metric_rows: dict[str, dict[str, Any]],
    *,
    left_metric_base: str,
    right_metric_base: str,
    suffix: str,
) -> dict[str, Any]:
    left_key = _build_metric_key(left_metric_base, suffix)
    right_key = _build_metric_key(right_metric_base, suffix)
    left_value = _metric_value(metric_rows, left_key)
    right_value = _metric_value(metric_rows, right_key)
    value_num, calc_status = _sum_status(left_value=left_value, right_value=right_value)
    return {
        "value_num": value_num,
        "calc_status": calc_status,
        "detail_inputs": {left_key: left_value, right_key: right_value},
        "reference_keys": [left_key, right_key],
    }


def _difference_metric_input(
    metric_rows: dict[str, dict[str, Any]],
    *,
    left_metric_base: str,
    right_metric_base: str,
    suffix: str,
) -> dict[str, Any]:
    left_key = _build_metric_key(left_metric_base, suffix)
    right_key = _build_metric_key(right_metric_base, suffix)
    left_value = _metric_value(metric_rows, left_key)
    right_value = _metric_value(metric_rows, right_key)
    value_num, calc_status = _difference_status(left_value=left_value, right_value=right_value)
    return {
        "value_num": value_num,
        "calc_status": calc_status,
        "detail_inputs": {left_key: left_value, right_key: right_value},
        "reference_keys": [left_key, right_key],
    }


def _outstanding_shares_input(
    metric_rows: dict[str, dict[str, Any]],
    suffix: str,
) -> dict[str, Any]:
    issued_key = _build_metric_key("IssuedShares", suffix)
    treasury_key = _build_metric_key("TreasuryShares", suffix)
    issued_value = _metric_value(metric_rows, issued_key)
    treasury_value = _metric_value(metric_rows, treasury_key)
    value_num, calc_status, effective_treasury_value = _outstanding_shares_status(
        issued_shares=issued_value,
        treasury_shares=treasury_value,
    )
    return {
        "value_num": value_num,
        "calc_status": calc_status,
        "detail_inputs": {
            issued_key: issued_value,
            treasury_key: treasury_value,
            f"{treasury_key}_effective": effective_treasury_value,
        },
        "reference_keys": [issued_key, treasury_key],
        "detail_extra": {
            "selected_source": "issued_shares_minus_treasury_shares",
            "effective_treasury_shares": effective_treasury_value,
        },
        "display_formula": "issued_shares - treasury_shares (treat blank or <1000 treasury_shares as 0)",
        "stored_formula": "issued_shares - treasury_shares_effective",
    }


def _is_safe_cost_of_sales_source(source_tag: str | None) -> bool:
    return str(source_tag or "") in SAFE_COST_OF_SALES_SOURCE_TAGS


def _is_safe_cost_of_sales_row(
    metric_rows: dict[str, dict[str, Any]],
    metric_key: str,
) -> bool:
    source_tag = _metric_source_tag(metric_rows, metric_key)
    if _is_safe_cost_of_sales_source(source_tag):
        return True
    if source_tag != "OperatingExpenses":
        return False
    suffix = metric_key.replace("CostOfSales", "", 1)
    return _metric_row(metric_rows, _build_metric_key("SellingExpenses", suffix)) is not None


def _is_safe_gross_profit_source(source_tag: str | None) -> bool:
    return str(source_tag or "") in SAFE_GROSS_PROFIT_SOURCE_TAGS


def _gross_profit_input(metric_rows: dict[str, dict[str, Any]], suffix: str) -> dict[str, Any]:
    gross_profit_key = _build_metric_key("GrossProfit", suffix)
    net_sales_key = _build_metric_key("NetSales", suffix)
    cost_of_sales_key = _build_metric_key("CostOfSales", suffix)
    funding_income_key = _build_metric_key("FundingIncome", suffix)

    tag_value = _metric_value(metric_rows, gross_profit_key)
    gross_profit_source_tag = _metric_source_tag(metric_rows, gross_profit_key)
    net_sales = _metric_value(metric_rows, net_sales_key)
    net_sales_source_tag = _metric_source_tag(metric_rows, net_sales_key)
    cost_of_sales = _metric_value(metric_rows, cost_of_sales_key)
    cost_of_sales_source_tag = _metric_source_tag(metric_rows, cost_of_sales_key)
    funding_income = _metric_value(metric_rows, funding_income_key)
    funding_income_source_tag = _metric_source_tag(metric_rows, funding_income_key)
    calculated_value, calculated_status = _difference_status(
        left_value=net_sales,
        right_value=cost_of_sales,
    )
    funding_profit_value, funding_profit_status = _difference_status(
        left_value=funding_income,
        right_value=cost_of_sales,
    )

    if tag_value is not None and _is_safe_gross_profit_source(gross_profit_source_tag):
        difference_value = None
        if calculated_value is not None:
            difference_value = tag_value - calculated_value
        return {
            "value_num": tag_value,
            "calc_status": "ok",
            "detail_inputs": {
                gross_profit_key: tag_value,
                net_sales_key: net_sales,
                cost_of_sales_key: cost_of_sales,
                funding_income_key: funding_income,
            },
            "reference_keys": [gross_profit_key, net_sales_key, cost_of_sales_key, funding_income_key],
            "detail_extra": {
                "selected_source": "gross_profit_tag",
                "tag_metric_key": gross_profit_key,
                "gross_profit_source_tag": gross_profit_source_tag,
                "net_sales_source_tag": net_sales_source_tag,
                "cost_of_sales_source_tag": cost_of_sales_source_tag,
                "funding_income_source_tag": funding_income_source_tag,
                "tag_value": tag_value,
                "calculated_value": calculated_value,
                "funding_profit_value": funding_profit_value,
                "difference_tag_minus_calculated": difference_value,
            },
            "display_formula": "gross_profit_tag",
            "stored_formula": "gross_profit_tag",
        }

    if cost_of_sales_source_tag == "FinancingExpensesOpeCFBNK" and funding_income is not None:
        return {
            "value_num": funding_profit_value,
            "calc_status": funding_profit_status,
            "detail_inputs": {
                funding_income_key: funding_income,
                cost_of_sales_key: cost_of_sales,
            },
            "reference_keys": [funding_income_key, cost_of_sales_key],
            "detail_extra": {
                "selected_source": "funding_income_minus_financing_expenses",
                "tag_metric_key": gross_profit_key,
                "gross_profit_source_tag": gross_profit_source_tag,
                "net_sales_source_tag": net_sales_source_tag,
                "cost_of_sales_source_tag": cost_of_sales_source_tag,
                "funding_income_source_tag": funding_income_source_tag,
                "tag_value": tag_value,
                "calculated_value": funding_profit_value,
                "difference_tag_minus_calculated": None,
            },
            "display_formula": "funding_income - financing_expenses",
            "stored_formula": "funding_income - financing_expenses",
        }

    if not _is_safe_cost_of_sales_row(metric_rows, cost_of_sales_key):
        return {
            "value_num": None,
            "calc_status": "missing_input",
            "detail_inputs": {
                net_sales_key: net_sales,
                cost_of_sales_key: cost_of_sales,
                funding_income_key: funding_income,
            },
            "reference_keys": [net_sales_key, cost_of_sales_key, funding_income_key],
            "detail_extra": {
                "selected_source": "missing_input",
                "tag_metric_key": gross_profit_key,
                "gross_profit_source_tag": gross_profit_source_tag,
                "net_sales_source_tag": net_sales_source_tag,
                "cost_of_sales_source_tag": cost_of_sales_source_tag,
                "funding_income_source_tag": funding_income_source_tag,
                "tag_value": tag_value,
                "calculated_value": calculated_value,
                "funding_profit_value": funding_profit_value,
                "difference_tag_minus_calculated": None,
                "missing_reason": "unsafe_cost_of_sales_source_tag",
            },
            "display_formula": "gross_profit_tag or net_sales - cost_of_sales",
            "stored_formula": "gross_profit_tag or net_sales - cost_of_sales",
        }

    return {
        "value_num": calculated_value,
        "calc_status": calculated_status,
        "detail_inputs": {
            net_sales_key: net_sales,
            cost_of_sales_key: cost_of_sales,
        },
        "reference_keys": [net_sales_key, cost_of_sales_key],
        "detail_extra": {
            "selected_source": "net_sales_minus_cost_of_sales",
            "tag_metric_key": gross_profit_key,
            "gross_profit_source_tag": gross_profit_source_tag,
            "net_sales_source_tag": net_sales_source_tag,
            "cost_of_sales_source_tag": cost_of_sales_source_tag,
            "tag_value": tag_value,
            "calculated_value": calculated_value,
            "difference_tag_minus_calculated": None,
        },
        "display_formula": "gross_profit_tag or net_sales - cost_of_sales",
        "stored_formula": "gross_profit_tag or net_sales - cost_of_sales",
    }


def _combined_cost_and_sga_input(metric_rows: dict[str, dict[str, Any]], suffix: str) -> dict[str, Any]:
    combined_key = _build_metric_key("CostOfSalesAndSellingGeneralAndAdministrativeExpenses", suffix)
    cost_of_sales_key = _build_metric_key("CostOfSales", suffix)
    selling_expenses_key = _build_metric_key("SellingExpenses", suffix)

    tag_value = _metric_value(metric_rows, combined_key)
    combined_source_tag = _metric_source_tag(metric_rows, combined_key)
    cost_of_sales = _metric_value(metric_rows, cost_of_sales_key)
    cost_of_sales_source_tag = _metric_source_tag(metric_rows, cost_of_sales_key)
    selling_expenses = _metric_value(metric_rows, selling_expenses_key)
    selling_expenses_source_tag = _metric_source_tag(metric_rows, selling_expenses_key)
    calculated_value, calculated_status = _sum_status(
        left_value=cost_of_sales,
        right_value=selling_expenses,
    )

    if tag_value is not None and str(combined_source_tag or "") in SAFE_COMBINED_COST_AND_SGA_SOURCE_TAGS:
        difference_value = None
        if calculated_value is not None:
            difference_value = tag_value - calculated_value
        return {
            "value_num": tag_value,
            "calc_status": "ok",
            "detail_inputs": {
                combined_key: tag_value,
                cost_of_sales_key: cost_of_sales,
                selling_expenses_key: selling_expenses,
            },
            "reference_keys": [combined_key, cost_of_sales_key, selling_expenses_key],
            "detail_extra": {
                "selected_source": "combined_expense_tag",
                "combined_source_tag": combined_source_tag,
                "cost_of_sales_source_tag": cost_of_sales_source_tag,
                "selling_expenses_source_tag": selling_expenses_source_tag,
                "tag_value": tag_value,
                "calculated_value": calculated_value,
                "difference_tag_minus_calculated": difference_value,
            },
            "display_formula": "combined_expense_tag",
            "stored_formula": "combined_expense_tag",
        }

    if not _is_safe_cost_of_sales_source(cost_of_sales_source_tag):
        return {
            "value_num": None,
            "calc_status": "missing_input",
            "detail_inputs": {
                cost_of_sales_key: cost_of_sales,
                selling_expenses_key: selling_expenses,
            },
            "reference_keys": [cost_of_sales_key, selling_expenses_key],
            "detail_extra": {
                "selected_source": "missing_input",
                "combined_source_tag": combined_source_tag,
                "cost_of_sales_source_tag": cost_of_sales_source_tag,
                "selling_expenses_source_tag": selling_expenses_source_tag,
                "tag_value": tag_value,
                "calculated_value": calculated_value,
                "difference_tag_minus_calculated": None,
                "missing_reason": "unsafe_cost_of_sales_source_tag",
            },
            "display_formula": "combined_expense_tag or cost_of_sales + selling_expenses",
            "stored_formula": "combined_expense_tag or cost_of_sales + selling_expenses",
        }

    return {
        "value_num": calculated_value,
        "calc_status": calculated_status,
        "detail_inputs": {
            cost_of_sales_key: cost_of_sales,
            selling_expenses_key: selling_expenses,
        },
        "reference_keys": [cost_of_sales_key, selling_expenses_key],
        "detail_extra": {
            "selected_source": "cost_of_sales_plus_selling_expenses",
            "combined_source_tag": combined_source_tag,
            "cost_of_sales_source_tag": cost_of_sales_source_tag,
            "selling_expenses_source_tag": selling_expenses_source_tag,
            "tag_value": tag_value,
            "calculated_value": calculated_value,
            "difference_tag_minus_calculated": None,
        },
        "display_formula": "combined_expense_tag or cost_of_sales + selling_expenses",
        "stored_formula": "combined_expense_tag or cost_of_sales + selling_expenses",
    }


def _append_growth_rows(
    out_rows: list[dict[str, Any]],
    *,
    metric_rows: dict[str, dict[str, Any]],
    sample_row: dict[str, Any],
    source_metric_base: str,
    derived_metric_base: str,
    accounting_standard: str,
    document_display_unit: str,
    rule_version: str,
) -> None:
    for current_suffix in GROWTH_SUFFIXES:
        current_offset = SUFFIX_TO_PERIOD_OFFSET[current_suffix]
        prior_suffix = f"Prior{current_offset + 1}"
        current_key = _build_metric_key(source_metric_base, current_suffix)
        prior_key = _build_metric_key(source_metric_base, prior_suffix)
        current_value = _metric_value(metric_rows, current_key)
        prior_value = _metric_value(metric_rows, prior_key)
        value_num, calc_status = _ratio_status(
            numerator=current_value,
            denominator=prior_value,
            require_positive_denominator=True,
        )
        reference_row = _pick_reference_row(metric_rows, [current_key, prior_key, sample_row["metric_key"]])

        out_rows.append(
            _build_derived_row(
                sample_row={
                    **sample_row,
                    "fiscal_year": reference_row.get("fiscal_year"),
                    "period_end": reference_row.get("period_end"),
                    "consolidation": reference_row.get("consolidation"),
                    "period_scope": sample_row["period_scope"],
                },
                metric_base=derived_metric_base,
                metric_group="growth",
                suffix=current_suffix,
                value_num=value_num,
                value_unit="ratio",
                calc_status=calc_status,
                formula_name="growth_ratio",
                source_detail=_build_source_detail(
                    inputs={current_key: current_value, prior_key: prior_value},
                    display_formula="current / prior * 100",
                    stored_formula="current / prior",
                    calc_status=calc_status,
                    document_display_unit=document_display_unit,
                ),
                accounting_standard=accounting_standard,
                document_display_unit=document_display_unit,
                rule_version=rule_version,
            )
        )


def _append_growth_rows_from_inputs(
    out_rows: list[dict[str, Any]],
    *,
    metric_rows: dict[str, dict[str, Any]],
    sample_row: dict[str, Any],
    derived_metric_base: str,
    metric_group: str,
    formula_name: str,
    display_formula: str,
    input_builder,
    accounting_standard: str,
    document_display_unit: str,
    rule_version: str,
    require_positive_denominator: bool = True,
) -> None:
    for current_suffix in GROWTH_SUFFIXES:
        current_offset = SUFFIX_TO_PERIOD_OFFSET[current_suffix]
        prior_suffix = f"Prior{current_offset + 1}"
        current_inputs = input_builder(metric_rows, current_suffix)
        prior_inputs = input_builder(metric_rows, prior_suffix)
        source_detail_extra: dict[str, Any] = {}
        if current_inputs.get("detail_extra"):
            source_detail_extra["current_detail"] = current_inputs["detail_extra"]
        if prior_inputs.get("detail_extra"):
            source_detail_extra["prior_detail"] = prior_inputs["detail_extra"]
        value_num, calc_status = _ratio_status(
            numerator=current_inputs["value_num"],
            denominator=prior_inputs["value_num"],
            require_positive_denominator=require_positive_denominator,
        )
        reference_row = _pick_reference_row(
            metric_rows,
            current_inputs["reference_keys"] + prior_inputs["reference_keys"] + [sample_row["metric_key"]],
        )

        out_rows.append(
            _build_derived_row(
                sample_row={
                    **sample_row,
                    "fiscal_year": reference_row.get("fiscal_year"),
                    "period_end": reference_row.get("period_end"),
                    "consolidation": reference_row.get("consolidation"),
                    "period_scope": sample_row["period_scope"],
                },
                metric_base=derived_metric_base,
                metric_group=metric_group,
                suffix=current_suffix,
                value_num=value_num,
                value_unit="ratio",
                calc_status=calc_status,
                formula_name=formula_name,
                source_detail=_build_source_detail(
                    inputs={
                        **current_inputs["detail_inputs"],
                        **prior_inputs["detail_inputs"],
                    },
                    display_formula=display_formula,
                    stored_formula=display_formula.replace("* 100", "").strip(),
                    calc_status=calc_status,
                    document_display_unit=document_display_unit,
                    extra=source_detail_extra or None,
                ),
                accounting_standard=accounting_standard,
                document_display_unit=document_display_unit,
                rule_version=rule_version,
            )
        )


def _append_rows_from_inputs(
    out_rows: list[dict[str, Any]],
    *,
    metric_rows: dict[str, dict[str, Any]],
    sample_row: dict[str, Any],
    derived_metric_base: str,
    metric_group: str,
    formula_name: str,
    value_unit: str,
    input_builder,
    accounting_standard: str,
    document_display_unit: str,
    rule_version: str,
) -> None:
    for suffix in FULL_SUFFIXES:
        metric_inputs = input_builder(metric_rows, suffix)
        reference_row = _pick_reference_row(
            metric_rows,
            metric_inputs["reference_keys"] + [sample_row["metric_key"]],
        )

        out_rows.append(
            _build_derived_row(
                sample_row={
                    **sample_row,
                    "fiscal_year": reference_row.get("fiscal_year"),
                    "period_end": reference_row.get("period_end"),
                    "consolidation": reference_row.get("consolidation"),
                    "period_scope": sample_row["period_scope"],
                },
                metric_base=derived_metric_base,
                metric_group=metric_group,
                suffix=suffix,
                value_num=metric_inputs["value_num"],
                value_unit=value_unit,
                calc_status=metric_inputs["calc_status"],
                formula_name=formula_name,
                source_detail=_build_source_detail(
                    inputs=metric_inputs["detail_inputs"],
                    display_formula=metric_inputs["display_formula"],
                    stored_formula=metric_inputs["stored_formula"],
                    calc_status=metric_inputs["calc_status"],
                    document_display_unit=document_display_unit,
                    extra=metric_inputs.get("detail_extra"),
                ),
                accounting_standard=accounting_standard,
                document_display_unit=document_display_unit,
                rule_version=rule_version,
            )
        )


def _append_gross_profit_rows(
    out_rows: list[dict[str, Any]],
    *,
    metric_rows: dict[str, dict[str, Any]],
    sample_row: dict[str, Any],
    accounting_standard: str,
    document_display_unit: str,
    rule_version: str,
) -> None:
    for suffix in FULL_SUFFIXES:
        gross_profit_inputs = _gross_profit_input(metric_rows, suffix)
        reference_row = _pick_reference_row(
            metric_rows,
            gross_profit_inputs["reference_keys"] + [sample_row["metric_key"]],
        )

        out_rows.append(
            _build_derived_row(
                sample_row={
                    **sample_row,
                    "fiscal_year": reference_row.get("fiscal_year"),
                    "period_end": reference_row.get("period_end"),
                    "consolidation": reference_row.get("consolidation"),
                    "period_scope": sample_row["period_scope"],
                },
                metric_base="GrossProfit",
                metric_group="profitability",
                suffix=suffix,
                value_num=gross_profit_inputs["value_num"],
                value_unit="yen",
                calc_status=gross_profit_inputs["calc_status"],
                formula_name="gross_profit",
                source_detail=_build_source_detail(
                    inputs=gross_profit_inputs["detail_inputs"],
                    display_formula=gross_profit_inputs["display_formula"],
                    stored_formula=gross_profit_inputs["stored_formula"],
                    calc_status=gross_profit_inputs["calc_status"],
                    document_display_unit=document_display_unit,
                    extra=gross_profit_inputs.get("detail_extra"),
                ),
                accounting_standard=accounting_standard,
                document_display_unit=document_display_unit,
                rule_version=rule_version,
            )
        )


def _append_combined_cost_and_sga_rows(
    out_rows: list[dict[str, Any]],
    *,
    metric_rows: dict[str, dict[str, Any]],
    sample_row: dict[str, Any],
    accounting_standard: str,
    document_display_unit: str,
    rule_version: str,
) -> None:
    for suffix in FULL_SUFFIXES:
        combined_inputs = _combined_cost_and_sga_input(metric_rows, suffix)
        reference_row = _pick_reference_row(
            metric_rows,
            combined_inputs["reference_keys"] + [sample_row["metric_key"]],
        )

        out_rows.append(
            _build_derived_row(
                sample_row={
                    **sample_row,
                    "fiscal_year": reference_row.get("fiscal_year"),
                    "period_end": reference_row.get("period_end"),
                    "consolidation": reference_row.get("consolidation"),
                    "period_scope": sample_row["period_scope"],
                },
                metric_base="CostOfSalesAndSellingGeneralAndAdministrativeExpenses",
                metric_group="profitability",
                suffix=suffix,
                value_num=combined_inputs["value_num"],
                value_unit="yen",
                calc_status=combined_inputs["calc_status"],
                formula_name="cost_of_sales_and_sga",
                source_detail=_build_source_detail(
                    inputs=combined_inputs["detail_inputs"],
                    display_formula=combined_inputs["display_formula"],
                    stored_formula=combined_inputs["stored_formula"],
                    calc_status=combined_inputs["calc_status"],
                    document_display_unit=document_display_unit,
                    extra=combined_inputs.get("detail_extra"),
                ),
                accounting_standard=accounting_standard,
                document_display_unit=document_display_unit,
                rule_version=rule_version,
            )
        )


def _append_difference_rows(
    out_rows: list[dict[str, Any]],
    *,
    metric_rows: dict[str, dict[str, Any]],
    sample_row: dict[str, Any],
    left_metric_base: str,
    right_metric_base: str,
    derived_metric_base: str,
    metric_group: str,
    formula_name: str,
    display_formula: str,
    value_unit: str,
    accounting_standard: str,
    document_display_unit: str,
    rule_version: str,
) -> None:
    for suffix in FULL_SUFFIXES:
        left_key = _build_metric_key(left_metric_base, suffix)
        right_key = _build_metric_key(right_metric_base, suffix)
        left_value = _metric_value(metric_rows, left_key)
        right_value = _metric_value(metric_rows, right_key)
        value_num, calc_status = _difference_status(left_value=left_value, right_value=right_value)
        reference_row = _pick_reference_row(metric_rows, [left_key, right_key, sample_row["metric_key"]])

        out_rows.append(
            _build_derived_row(
                sample_row={
                    **sample_row,
                    "fiscal_year": reference_row.get("fiscal_year"),
                    "period_end": reference_row.get("period_end"),
                    "consolidation": reference_row.get("consolidation"),
                    "period_scope": sample_row["period_scope"],
                },
                metric_base=derived_metric_base,
                metric_group=metric_group,
                suffix=suffix,
                value_num=value_num,
                value_unit=value_unit,
                calc_status=calc_status,
                formula_name=formula_name,
                source_detail=_build_source_detail(
                    inputs={left_key: left_value, right_key: right_value},
                    display_formula=display_formula,
                    stored_formula=display_formula,
                    calc_status=calc_status,
                    document_display_unit=document_display_unit,
                ),
                accounting_standard=accounting_standard,
                document_display_unit=document_display_unit,
                rule_version=rule_version,
            )
        )


def _append_ratio_rows(
    out_rows: list[dict[str, Any]],
    *,
    metric_rows: dict[str, dict[str, Any]],
    sample_row: dict[str, Any],
    derived_metric_base: str,
    metric_group: str,
    formula_name: str,
    display_formula: str,
    numerator_builder,
    denominator_builder,
    accounting_standard: str,
    document_display_unit: str,
    rule_version: str,
    require_positive_denominator: bool = False,
    value_unit: str = "ratio",
) -> None:
    for suffix in FULL_SUFFIXES:
        numerator_inputs = numerator_builder(metric_rows, suffix)
        denominator_inputs = denominator_builder(metric_rows, suffix)
        source_detail_extra: dict[str, Any] = {}
        if numerator_inputs.get("detail_extra"):
            source_detail_extra["numerator_detail"] = numerator_inputs["detail_extra"]
        if denominator_inputs.get("detail_extra"):
            source_detail_extra["denominator_detail"] = denominator_inputs["detail_extra"]
        value_num, calc_status = _ratio_status(
            numerator=numerator_inputs["value_num"],
            denominator=denominator_inputs["value_num"],
            require_positive_denominator=require_positive_denominator,
        )
        reference_row = _pick_reference_row(
            metric_rows,
            numerator_inputs["reference_keys"] + denominator_inputs["reference_keys"] + [sample_row["metric_key"]],
        )

        out_rows.append(
            _build_derived_row(
                sample_row={
                    **sample_row,
                    "fiscal_year": reference_row.get("fiscal_year"),
                    "period_end": reference_row.get("period_end"),
                    "consolidation": reference_row.get("consolidation"),
                    "period_scope": sample_row["period_scope"],
                },
                metric_base=derived_metric_base,
                metric_group=metric_group,
                suffix=suffix,
                value_num=value_num,
                value_unit=value_unit,
                calc_status=calc_status,
                formula_name=formula_name,
                source_detail=_build_source_detail(
                    inputs={
                        **numerator_inputs["detail_inputs"],
                        **denominator_inputs["detail_inputs"],
                    },
                    display_formula=display_formula,
                    stored_formula=display_formula.replace("* 100", "").strip(),
                    calc_status=calc_status,
                    document_display_unit=document_display_unit,
                    extra=source_detail_extra or None,
                ),
                accounting_standard=accounting_standard,
                document_display_unit=document_display_unit,
                rule_version=rule_version,
            )
        )


def _append_scaled_rows(
    out_rows: list[dict[str, Any]],
    *,
    metric_rows: dict[str, dict[str, Any]],
    sample_row: dict[str, Any],
    source_metric_base: str,
    derived_metric_base: str,
    metric_group: str,
    formula_name: str,
    scale: float,
    display_formula: str,
    accounting_standard: str,
    document_display_unit: str,
    rule_version: str,
) -> None:
    for suffix in FULL_SUFFIXES:
        source_key = _build_metric_key(source_metric_base, suffix)
        source_value = _metric_value(metric_rows, source_key)
        value_num, calc_status = _scaled_status(value_num=source_value, scale=scale)
        reference_row = _pick_reference_row(metric_rows, [source_key, sample_row["metric_key"]])

        out_rows.append(
            _build_derived_row(
                sample_row={
                    **sample_row,
                    "fiscal_year": reference_row.get("fiscal_year"),
                    "period_end": reference_row.get("period_end"),
                    "consolidation": reference_row.get("consolidation"),
                    "period_scope": sample_row["period_scope"],
                },
                metric_base=derived_metric_base,
                metric_group=metric_group,
                suffix=suffix,
                value_num=value_num,
                value_unit="yen",
                calc_status=calc_status,
                formula_name=formula_name,
                source_detail=_build_source_detail(
                    inputs={source_key: source_value},
                    display_formula=display_formula,
                    stored_formula=f"{source_key} * {scale}",
                    calc_status=calc_status,
                    document_display_unit=document_display_unit,
                ),
                accounting_standard=accounting_standard,
                document_display_unit=document_display_unit,
                rule_version=rule_version,
            )
        )


def _append_outstanding_shares_rows(
    out_rows: list[dict[str, Any]],
    *,
    metric_rows: dict[str, dict[str, Any]],
    sample_row: dict[str, Any],
    accounting_standard: str,
    document_display_unit: str,
    rule_version: str,
) -> None:
    for suffix in FULL_SUFFIXES:
        shares_inputs = _outstanding_shares_input(metric_rows, suffix)
        reference_row = _pick_reference_row(
            metric_rows,
            shares_inputs["reference_keys"] + [sample_row["metric_key"]],
        )

        out_rows.append(
            _build_derived_row(
                sample_row={
                    **sample_row,
                    "fiscal_year": reference_row.get("fiscal_year"),
                    "period_end": reference_row.get("period_end"),
                    "consolidation": reference_row.get("consolidation"),
                    "period_scope": sample_row["period_scope"],
                },
                metric_base="OutstandingShares",
                metric_group="share",
                suffix=suffix,
                value_num=shares_inputs["value_num"],
                value_unit="shares",
                calc_status=shares_inputs["calc_status"],
                formula_name="outstanding_shares",
                source_detail=_build_source_detail(
                    inputs=shares_inputs["detail_inputs"],
                    display_formula=shares_inputs["display_formula"],
                    stored_formula=shares_inputs["stored_formula"],
                    calc_status=shares_inputs["calc_status"],
                    document_display_unit=document_display_unit,
                    extra=shares_inputs.get("detail_extra"),
                ),
                accounting_standard=accounting_standard,
                document_display_unit=document_display_unit,
                rule_version=rule_version,
            )
        )


def _append_sum_rows(
    out_rows: list[dict[str, Any]],
    *,
    metric_rows: dict[str, dict[str, Any]],
    sample_row: dict[str, Any],
    left_metric_base: str,
    right_metric_base: str,
    derived_metric_base: str,
    metric_group: str,
    formula_name: str,
    display_formula: str,
    accounting_standard: str,
    document_display_unit: str,
    rule_version: str,
) -> None:
    for suffix in FULL_SUFFIXES:
        left_key = _build_metric_key(left_metric_base, suffix)
        right_key = _build_metric_key(right_metric_base, suffix)
        left_value = _metric_value(metric_rows, left_key)
        right_value = _metric_value(metric_rows, right_key)
        value_num, calc_status = _sum_status(left_value=left_value, right_value=right_value)
        reference_row = _pick_reference_row(metric_rows, [left_key, right_key, sample_row["metric_key"]])

        out_rows.append(
            _build_derived_row(
                sample_row={
                    **sample_row,
                    "fiscal_year": reference_row.get("fiscal_year"),
                    "period_end": reference_row.get("period_end"),
                    "consolidation": reference_row.get("consolidation"),
                    "period_scope": sample_row["period_scope"],
                },
                metric_base=derived_metric_base,
                metric_group=metric_group,
                suffix=suffix,
                value_num=value_num,
                value_unit="yen",
                calc_status=calc_status,
                formula_name=formula_name,
                source_detail=_build_source_detail(
                    inputs={left_key: left_value, right_key: right_value},
                    display_formula=display_formula,
                    stored_formula=display_formula,
                    calc_status=calc_status,
                    document_display_unit=document_display_unit,
                ),
                accounting_standard=accounting_standard,
                document_display_unit=document_display_unit,
                rule_version=rule_version,
            )
        )


def calculate_derived_metrics(
    normalized_rows: list[dict[str, Any]],
    *,
    form_type: str,
    accounting_standard: str = "",
    document_display_unit: str = "",
    rule_version: str = DEFAULT_DERIVED_METRICS_RULE_VERSION,
) -> list[dict[str, Any]]:
    if not normalized_rows:
        return []

    period_scope = infer_period_scope(form_type)
    if period_scope is None:
        raise ValueError(f"Unsupported form_type for derived metrics: {form_type}")

    sample_row = dict(normalized_rows[0])
    sample_row["period_scope"] = period_scope
    metric_rows = _metric_map(normalized_rows)
    out_rows: list[dict[str, Any]] = []

    def _single_metric(metric_base: str, suffix: str) -> dict[str, Any]:
        return _single_metric_input(metric_rows, metric_base, suffix)

    def _estimated_net_income_input(metric_rows: dict[str, dict[str, Any]], suffix: str) -> dict[str, Any]:
        source_key = _build_metric_key("OrdinaryIncome", suffix)
        source_value = _metric_value(metric_rows, source_key)
        value_num, calc_status = _scaled_status(value_num=source_value, scale=0.7)
        return {
            "value_num": value_num,
            "calc_status": calc_status,
            "detail_inputs": {source_key: source_value},
            "reference_keys": [source_key],
        }

    def _eps_input(metric_rows: dict[str, dict[str, Any]], suffix: str) -> dict[str, Any]:
        estimated_net_income_inputs = _estimated_net_income_input(metric_rows, suffix)
        outstanding_shares_inputs = _outstanding_shares_input(metric_rows, suffix)
        source_detail_extra: dict[str, Any] = {}
        if outstanding_shares_inputs.get("detail_extra"):
            source_detail_extra["denominator_detail"] = outstanding_shares_inputs["detail_extra"]
        value_num, calc_status = _ratio_status(
            numerator=estimated_net_income_inputs["value_num"],
            denominator=outstanding_shares_inputs["value_num"],
            require_positive_denominator=True,
        )
        return {
            "value_num": value_num,
            "calc_status": calc_status,
            "detail_inputs": {
                **estimated_net_income_inputs["detail_inputs"],
                **outstanding_shares_inputs["detail_inputs"],
            },
            "reference_keys": [
                *estimated_net_income_inputs["reference_keys"],
                *outstanding_shares_inputs["reference_keys"],
            ],
            "detail_extra": source_detail_extra or None,
        }

    def _roa_input(metric_rows: dict[str, dict[str, Any]], suffix: str) -> dict[str, Any]:
        estimated_net_income_inputs = _estimated_net_income_input(metric_rows, suffix)
        total_assets_inputs = _single_metric("TotalAssets", suffix)
        value_num, calc_status = _ratio_status(
            numerator=estimated_net_income_inputs["value_num"],
            denominator=total_assets_inputs["value_num"],
            require_positive_denominator=False,
        )
        return {
            "value_num": value_num,
            "calc_status": calc_status,
            "detail_inputs": {
                **estimated_net_income_inputs["detail_inputs"],
                **total_assets_inputs["detail_inputs"],
            },
            "reference_keys": [
                *estimated_net_income_inputs["reference_keys"],
                *total_assets_inputs["reference_keys"],
            ],
        }

    def _equity_ratio_input(metric_rows: dict[str, dict[str, Any]], suffix: str) -> dict[str, Any]:
        net_assets_inputs = _single_metric("NetAssets", suffix)
        total_assets_inputs = _single_metric("TotalAssets", suffix)
        value_num, calc_status = _ratio_status(
            numerator=net_assets_inputs["value_num"],
            denominator=total_assets_inputs["value_num"],
            require_positive_denominator=False,
        )
        return {
            "value_num": value_num,
            "calc_status": calc_status,
            "detail_inputs": {
                **net_assets_inputs["detail_inputs"],
                **total_assets_inputs["detail_inputs"],
            },
            "reference_keys": [
                *net_assets_inputs["reference_keys"],
                *total_assets_inputs["reference_keys"],
            ],
        }

    def _discount_evaluation_rate_input(metric_rows: dict[str, dict[str, Any]], suffix: str) -> dict[str, Any]:
        equity_ratio_inputs = _equity_ratio_input(metric_rows, suffix)
        value_num, calc_status = _discount_evaluation_rate_status(
            equity_ratio=equity_ratio_inputs["value_num"],
        )
        return {
            "value_num": value_num,
            "calc_status": calc_status,
            "detail_inputs": {
                **equity_ratio_inputs["detail_inputs"],
                "equity_ratio": equity_ratio_inputs["value_num"],
            },
            "reference_keys": equity_ratio_inputs["reference_keys"],
            "detail_extra": {
                "selected_source": "equity_ratio_band",
                "equity_ratio": equity_ratio_inputs["value_num"],
            },
            "display_formula": "discount_rate by equity_ratio band",
            "stored_formula": "discount_rate_by_equity_ratio_band",
        }

    def _financial_leverage_adjustment_input(
        metric_rows: dict[str, dict[str, Any]],
        suffix: str,
    ) -> dict[str, Any]:
        net_assets_inputs = _single_metric("NetAssets", suffix)
        total_assets_inputs = _single_metric("TotalAssets", suffix)
        value_num, calc_status, equity_ratio, raw_value = _financial_leverage_adjustment_status(
            netassets=net_assets_inputs["value_num"],
            totalassets=total_assets_inputs["value_num"],
        )
        return {
            "value_num": value_num,
            "calc_status": calc_status,
            "detail_inputs": {
                **net_assets_inputs["detail_inputs"],
                **total_assets_inputs["detail_inputs"],
                "equity_ratio_for_adjustment": equity_ratio,
                "raw_adjustment_value": raw_value,
            },
            "reference_keys": [
                *net_assets_inputs["reference_keys"],
                *total_assets_inputs["reference_keys"],
            ],
            "detail_extra": {
                "selected_source": "financial_leverage_adjustment_formula",
                "equity_ratio": equity_ratio,
                "raw_value": raw_value,
            },
            "display_formula": "clamp(1 / (netassets / totalassets + 0.33), 1, 1.5), but return 0 when netassets * totalassets <= 0",
            "stored_formula": "financial_leverage_adjustment",
        }

    def _bps_input(metric_rows: dict[str, dict[str, Any]], suffix: str) -> dict[str, Any]:
        net_assets_inputs = _single_metric("NetAssets", suffix)
        outstanding_shares_inputs = _outstanding_shares_input(metric_rows, suffix)
        value_num, calc_status = _ratio_status(
            numerator=net_assets_inputs["value_num"],
            denominator=outstanding_shares_inputs["value_num"],
            require_positive_denominator=True,
        )
        return {
            "value_num": value_num,
            "calc_status": calc_status,
            "detail_inputs": {
                **net_assets_inputs["detail_inputs"],
                **outstanding_shares_inputs["detail_inputs"],
            },
            "reference_keys": [
                *net_assets_inputs["reference_keys"],
                *outstanding_shares_inputs["reference_keys"],
            ],
            "detail_extra": {
                "denominator_detail": outstanding_shares_inputs.get("detail_extra"),
            },
            "display_formula": "net_assets / outstanding_shares",
            "stored_formula": "net_assets / outstanding_shares",
        }

    def _operating_cash_per_share_input(
        metric_rows: dict[str, dict[str, Any]],
        suffix: str,
    ) -> dict[str, Any]:
        operating_cash_inputs = _single_metric("OperatingCash", suffix)
        outstanding_shares_inputs = _outstanding_shares_input(metric_rows, suffix)
        value_num, calc_status = _ratio_status(
            numerator=operating_cash_inputs["value_num"],
            denominator=outstanding_shares_inputs["value_num"],
            require_positive_denominator=True,
        )
        return {
            "value_num": value_num,
            "calc_status": calc_status,
            "detail_inputs": {
                **operating_cash_inputs["detail_inputs"],
                **outstanding_shares_inputs["detail_inputs"],
            },
            "reference_keys": [
                *operating_cash_inputs["reference_keys"],
                *outstanding_shares_inputs["reference_keys"],
            ],
            "detail_extra": {
                "denominator_detail": outstanding_shares_inputs.get("detail_extra"),
            },
            "display_formula": "operating_cash / outstanding_shares",
            "stored_formula": "operating_cash / outstanding_shares",
        }

    def _asset_value_input(metric_rows: dict[str, dict[str, Any]], suffix: str) -> dict[str, Any]:
        bps_inputs = _bps_input(metric_rows, suffix)
        discount_rate_inputs = _discount_evaluation_rate_input(metric_rows, suffix)
        value_num, calc_status = _multiply_status(
            left_value=bps_inputs["value_num"],
            right_value=discount_rate_inputs["value_num"],
        )
        return {
            "value_num": value_num,
            "calc_status": calc_status,
            "detail_inputs": {
                **bps_inputs["detail_inputs"],
                **discount_rate_inputs["detail_inputs"],
            },
            "reference_keys": [
                *bps_inputs["reference_keys"],
                *discount_rate_inputs["reference_keys"],
            ],
            "detail_extra": {
                "bps_detail": bps_inputs.get("detail_extra"),
                "discount_rate_detail": discount_rate_inputs.get("detail_extra"),
            },
            "display_formula": "bps * discount_rate",
            "stored_formula": "bps * discount_rate",
        }

    def _business_value_input(metric_rows: dict[str, dict[str, Any]], suffix: str) -> dict[str, Any]:
        eps_inputs = _eps_input(metric_rows, suffix)
        roa_inputs = _roa_input(metric_rows, suffix)
        financial_leverage_inputs = _financial_leverage_adjustment_input(metric_rows, suffix)
        roa_multiplier, roa_multiplier_status = _scaled_status(
            value_num=roa_inputs["value_num"],
            scale=150.0,
        )
        effective_multiplier, effective_multiplier_status = _multiply_status(
            left_value=roa_multiplier,
            right_value=financial_leverage_inputs["value_num"],
        )
        value_num, calc_status = _multiply_status(
            left_value=eps_inputs["value_num"],
            right_value=effective_multiplier,
        )
        if calc_status == "missing_input" and roa_multiplier_status != "ok":
            calc_status = roa_multiplier_status
        if calc_status == "missing_input" and effective_multiplier_status != "ok":
            calc_status = effective_multiplier_status
        return {
            "value_num": value_num,
            "calc_status": calc_status,
            "detail_inputs": {
                **eps_inputs["detail_inputs"],
                **roa_inputs["detail_inputs"],
                **financial_leverage_inputs["detail_inputs"],
                "roa_x_150": roa_multiplier,
                "business_value_multiplier": effective_multiplier,
            },
            "reference_keys": [
                *eps_inputs["reference_keys"],
                *roa_inputs["reference_keys"],
                *financial_leverage_inputs["reference_keys"],
            ],
            "detail_extra": {
                "eps_detail": eps_inputs.get("detail_extra"),
                "financial_leverage_detail": financial_leverage_inputs.get("detail_extra"),
                "roa_multiplier_status": roa_multiplier_status,
                "effective_multiplier_status": effective_multiplier_status,
            },
            "display_formula": "eps * (roa * 150 * financial_leverage_adjustment)",
            "stored_formula": "eps * (roa * 150 * financial_leverage_adjustment)",
        }

    def _theoretical_share_price_input(metric_rows: dict[str, dict[str, Any]], suffix: str) -> dict[str, Any]:
        asset_value_inputs = _asset_value_input(metric_rows, suffix)
        business_value_inputs = _business_value_input(metric_rows, suffix)
        value_num, calc_status = _sum_status(
            left_value=asset_value_inputs["value_num"],
            right_value=business_value_inputs["value_num"],
        )
        return {
            "value_num": value_num,
            "calc_status": calc_status,
            "detail_inputs": {
                **asset_value_inputs["detail_inputs"],
                **business_value_inputs["detail_inputs"],
            },
            "reference_keys": [
                *asset_value_inputs["reference_keys"],
                *business_value_inputs["reference_keys"],
            ],
            "detail_extra": {
                "asset_value_detail": asset_value_inputs.get("detail_extra"),
                "business_value_detail": business_value_inputs.get("detail_extra"),
            },
            "display_formula": "asset_value + business_value",
            "stored_formula": "asset_value + business_value",
        }

    def _upper_bound_theoretical_share_price_input(
        metric_rows: dict[str, dict[str, Any]],
        suffix: str,
    ) -> dict[str, Any]:
        asset_value_inputs = _asset_value_input(metric_rows, suffix)
        business_value_inputs = _business_value_input(metric_rows, suffix)
        doubled_business_value, doubled_business_status = _scaled_status(
            value_num=business_value_inputs["value_num"],
            scale=2.0,
        )
        value_num, calc_status = _sum_status(
            left_value=asset_value_inputs["value_num"],
            right_value=doubled_business_value,
        )
        if calc_status == "missing_input" and doubled_business_status != "ok":
            calc_status = doubled_business_status
        return {
            "value_num": value_num,
            "calc_status": calc_status,
            "detail_inputs": {
                **asset_value_inputs["detail_inputs"],
                **business_value_inputs["detail_inputs"],
                "business_value_x_2": doubled_business_value,
            },
            "reference_keys": [
                *asset_value_inputs["reference_keys"],
                *business_value_inputs["reference_keys"],
            ],
            "detail_extra": {
                "asset_value_detail": asset_value_inputs.get("detail_extra"),
                "business_value_detail": business_value_inputs.get("detail_extra"),
                "doubled_business_status": doubled_business_status,
            },
            "display_formula": "asset_value + (business_value * 2)",
            "stored_formula": "asset_value + (business_value * 2)",
        }

    def _fcf_input(metric_rows: dict[str, dict[str, Any]], suffix: str) -> dict[str, Any]:
        return _sum_metric_input(
            metric_rows,
            left_metric_base="OperatingCash",
            right_metric_base="InvestmentCash",
            suffix=suffix,
        )

    _append_growth_rows(
        out_rows,
        metric_rows=metric_rows,
        sample_row=sample_row,
        source_metric_base="NetSales",
        derived_metric_base="NetSalesGrowthRate",
        accounting_standard=accounting_standard,
        document_display_unit=document_display_unit,
        rule_version=rule_version,
    )
    _append_growth_rows(
        out_rows,
        metric_rows=metric_rows,
        sample_row=sample_row,
        source_metric_base="OrdinaryIncome",
        derived_metric_base="OrdinaryIncomeGrowthRate",
        accounting_standard=accounting_standard,
        document_display_unit=document_display_unit,
        rule_version=rule_version,
    )
    _append_growth_rows(
        out_rows,
        metric_rows=metric_rows,
        sample_row=sample_row,
        source_metric_base="CashAndCashEquivalents",
        derived_metric_base="CashBalanceGrowthRate",
        accounting_standard=accounting_standard,
        document_display_unit=document_display_unit,
        rule_version=rule_version,
    )
    _append_growth_rows_from_inputs(
        out_rows,
        metric_rows=metric_rows,
        sample_row=sample_row,
        derived_metric_base="EPSGrowthRate",
        metric_group="growth",
        formula_name="eps_growth_rate",
        display_formula="current_eps / prior_eps * 100",
        input_builder=_eps_input,
        accounting_standard=accounting_standard,
        document_display_unit=document_display_unit,
        rule_version=rule_version,
    )

    _append_outstanding_shares_rows(
        out_rows,
        metric_rows=metric_rows,
        sample_row=sample_row,
        accounting_standard=accounting_standard,
        document_display_unit=document_display_unit,
        rule_version=rule_version,
    )
    _append_ratio_rows(
        out_rows,
        metric_rows=metric_rows,
        sample_row=sample_row,
        derived_metric_base="EPS",
        metric_group="share",
        formula_name="eps",
        display_formula="estimated_net_income / outstanding_shares",
        numerator_builder=_estimated_net_income_input,
        denominator_builder=_outstanding_shares_input,
        accounting_standard=accounting_standard,
        document_display_unit=document_display_unit,
        rule_version=rule_version,
        require_positive_denominator=True,
        value_unit="yen_per_share",
    )
    _append_ratio_rows(
        out_rows,
        metric_rows=metric_rows,
        sample_row=sample_row,
        derived_metric_base="BPS",
        metric_group="share",
        formula_name="bps",
        display_formula="net_assets / outstanding_shares",
        numerator_builder=lambda metric_rows, suffix: _single_metric("NetAssets", suffix),
        denominator_builder=_outstanding_shares_input,
        accounting_standard=accounting_standard,
        document_display_unit=document_display_unit,
        rule_version=rule_version,
        require_positive_denominator=True,
        value_unit="yen_per_share",
    )
    _append_ratio_rows(
        out_rows,
        metric_rows=metric_rows,
        sample_row=sample_row,
        derived_metric_base="AssetsPerShare",
        metric_group="share",
        formula_name="assets_per_share",
        display_formula="total_assets / outstanding_shares",
        numerator_builder=lambda metric_rows, suffix: _single_metric("TotalAssets", suffix),
        denominator_builder=_outstanding_shares_input,
        accounting_standard=accounting_standard,
        document_display_unit=document_display_unit,
        rule_version=rule_version,
        require_positive_denominator=True,
        value_unit="yen_per_share",
    )
    _append_ratio_rows(
        out_rows,
        metric_rows=metric_rows,
        sample_row=sample_row,
        derived_metric_base="LiabilitiesPerShare",
        metric_group="share",
        formula_name="liabilities_per_share",
        display_formula="(total_assets - net_assets) / outstanding_shares",
        numerator_builder=lambda metric_rows, suffix: _difference_metric_input(
            metric_rows,
            left_metric_base="TotalAssets",
            right_metric_base="NetAssets",
            suffix=suffix,
        ),
        denominator_builder=_outstanding_shares_input,
        accounting_standard=accounting_standard,
        document_display_unit=document_display_unit,
        rule_version=rule_version,
        require_positive_denominator=True,
        value_unit="yen_per_share",
    )
    _append_ratio_rows(
        out_rows,
        metric_rows=metric_rows,
        sample_row=sample_row,
        derived_metric_base="OperatingCashPerShare",
        metric_group="cashflow",
        formula_name="operating_cash_per_share",
        display_formula="operating_cash / outstanding_shares",
        numerator_builder=lambda metric_rows, suffix: _single_metric("OperatingCash", suffix),
        denominator_builder=_outstanding_shares_input,
        accounting_standard=accounting_standard,
        document_display_unit=document_display_unit,
        rule_version=rule_version,
        require_positive_denominator=True,
        value_unit="yen_per_share",
    )
    _append_ratio_rows(
        out_rows,
        metric_rows=metric_rows,
        sample_row=sample_row,
        derived_metric_base="InvestmentCashPerShare",
        metric_group="cashflow",
        formula_name="investment_cash_per_share",
        display_formula="investment_cash / outstanding_shares",
        numerator_builder=lambda metric_rows, suffix: _single_metric("InvestmentCash", suffix),
        denominator_builder=_outstanding_shares_input,
        accounting_standard=accounting_standard,
        document_display_unit=document_display_unit,
        rule_version=rule_version,
        require_positive_denominator=True,
        value_unit="yen_per_share",
    )
    _append_ratio_rows(
        out_rows,
        metric_rows=metric_rows,
        sample_row=sample_row,
        derived_metric_base="FinancingCashPerShare",
        metric_group="cashflow",
        formula_name="financing_cash_per_share",
        display_formula="financing_cash / outstanding_shares",
        numerator_builder=lambda metric_rows, suffix: _single_metric("FinancingCash", suffix),
        denominator_builder=_outstanding_shares_input,
        accounting_standard=accounting_standard,
        document_display_unit=document_display_unit,
        rule_version=rule_version,
        require_positive_denominator=True,
        value_unit="yen_per_share",
    )

    _append_combined_cost_and_sga_rows(
        out_rows,
        metric_rows=metric_rows,
        sample_row=sample_row,
        accounting_standard=accounting_standard,
        document_display_unit=document_display_unit,
        rule_version=rule_version,
    )

    _append_gross_profit_rows(
        out_rows,
        metric_rows=metric_rows,
        sample_row=sample_row,
        accounting_standard=accounting_standard,
        document_display_unit=document_display_unit,
        rule_version=rule_version,
    )
    _append_ratio_rows(
        out_rows,
        metric_rows=metric_rows,
        sample_row=sample_row,
        derived_metric_base="CostOfSalesRatio",
        metric_group="profitability",
        formula_name="cost_of_sales_ratio",
        display_formula="cost_of_sales / net_sales * 100",
        numerator_builder=lambda metric_rows, suffix: _single_metric("CostOfSales", suffix),
        denominator_builder=lambda metric_rows, suffix: _single_metric("NetSales", suffix),
        accounting_standard=accounting_standard,
        document_display_unit=document_display_unit,
        rule_version=rule_version,
    )
    _append_ratio_rows(
        out_rows,
        metric_rows=metric_rows,
        sample_row=sample_row,
        derived_metric_base="GrossProfitMargin",
        metric_group="profitability",
        formula_name="gross_profit_margin",
        display_formula="gross_profit / net_sales * 100",
        numerator_builder=_gross_profit_input,
        denominator_builder=lambda metric_rows, suffix: _single_metric("NetSales", suffix),
        accounting_standard=accounting_standard,
        document_display_unit=document_display_unit,
        rule_version=rule_version,
    )
    _append_ratio_rows(
        out_rows,
        metric_rows=metric_rows,
        sample_row=sample_row,
        derived_metric_base="SellingExpensesRatio",
        metric_group="profitability",
        formula_name="selling_expenses_ratio",
        display_formula="selling_expenses / net_sales * 100",
        numerator_builder=lambda metric_rows, suffix: _single_metric("SellingExpenses", suffix),
        denominator_builder=lambda metric_rows, suffix: _single_metric("NetSales", suffix),
        accounting_standard=accounting_standard,
        document_display_unit=document_display_unit,
        rule_version=rule_version,
    )
    _append_ratio_rows(
        out_rows,
        metric_rows=metric_rows,
        sample_row=sample_row,
        derived_metric_base="OperatingMargin",
        metric_group="profitability",
        formula_name="operating_margin",
        display_formula="operating_income / net_sales * 100",
        numerator_builder=lambda metric_rows, suffix: _single_metric("OperatingIncome", suffix),
        denominator_builder=lambda metric_rows, suffix: _single_metric("NetSales", suffix),
        accounting_standard=accounting_standard,
        document_display_unit=document_display_unit,
        rule_version=rule_version,
    )
    _append_ratio_rows(
        out_rows,
        metric_rows=metric_rows,
        sample_row=sample_row,
        derived_metric_base="OrdinaryIncomeMargin",
        metric_group="profitability",
        formula_name="ordinary_income_margin",
        display_formula="ordinary_income / net_sales * 100",
        numerator_builder=lambda metric_rows, suffix: _single_metric("OrdinaryIncome", suffix),
        denominator_builder=lambda metric_rows, suffix: _single_metric("NetSales", suffix),
        accounting_standard=accounting_standard,
        document_display_unit=document_display_unit,
        rule_version=rule_version,
    )
    _append_scaled_rows(
        out_rows,
        metric_rows=metric_rows,
        sample_row=sample_row,
        source_metric_base="OrdinaryIncome",
        derived_metric_base="EstimatedNetIncome",
        metric_group="profitability",
        formula_name="estimated_net_income",
        scale=0.7,
        display_formula="ordinary_income * 0.7",
        accounting_standard=accounting_standard,
        document_display_unit=document_display_unit,
        rule_version=rule_version,
    )
    _append_ratio_rows(
        out_rows,
        metric_rows=metric_rows,
        sample_row=sample_row,
        derived_metric_base="EstimatedNetMargin",
        metric_group="profitability",
        formula_name="estimated_net_margin",
        display_formula="estimated_net_income / net_sales * 100",
        numerator_builder=_estimated_net_income_input,
        denominator_builder=lambda metric_rows, suffix: _single_metric("NetSales", suffix),
        accounting_standard=accounting_standard,
        document_display_unit=document_display_unit,
        rule_version=rule_version,
    )
    _append_ratio_rows(
        out_rows,
        metric_rows=metric_rows,
        sample_row=sample_row,
        derived_metric_base="ROA",
        metric_group="return",
        formula_name="roa",
        display_formula="estimated_net_income / total_assets * 100",
        numerator_builder=_estimated_net_income_input,
        denominator_builder=lambda metric_rows, suffix: _single_metric("TotalAssets", suffix),
        accounting_standard=accounting_standard,
        document_display_unit=document_display_unit,
        rule_version=rule_version,
    )
    _append_ratio_rows(
        out_rows,
        metric_rows=metric_rows,
        sample_row=sample_row,
        derived_metric_base="ROE",
        metric_group="return",
        formula_name="roe",
        display_formula="estimated_net_income / net_assets * 100",
        numerator_builder=_estimated_net_income_input,
        denominator_builder=lambda metric_rows, suffix: _single_metric("NetAssets", suffix),
        accounting_standard=accounting_standard,
        document_display_unit=document_display_unit,
        rule_version=rule_version,
    )
    _append_ratio_rows(
        out_rows,
        metric_rows=metric_rows,
        sample_row=sample_row,
        derived_metric_base="EquityRatio",
        metric_group="return",
        formula_name="equity_ratio",
        display_formula="net_assets / total_assets * 100",
        numerator_builder=lambda metric_rows, suffix: _single_metric("NetAssets", suffix),
        denominator_builder=lambda metric_rows, suffix: _single_metric("TotalAssets", suffix),
        accounting_standard=accounting_standard,
        document_display_unit=document_display_unit,
        rule_version=rule_version,
    )
    _append_rows_from_inputs(
        out_rows,
        metric_rows=metric_rows,
        sample_row=sample_row,
        derived_metric_base="FinancialLeverageAdjustment",
        metric_group="valuation",
        formula_name="financial_leverage_adjustment",
        value_unit="ratio",
        input_builder=_financial_leverage_adjustment_input,
        accounting_standard=accounting_standard,
        document_display_unit=document_display_unit,
        rule_version=rule_version,
    )
    _append_rows_from_inputs(
        out_rows,
        metric_rows=metric_rows,
        sample_row=sample_row,
        derived_metric_base="AssetValue",
        metric_group="valuation",
        formula_name="asset_value",
        value_unit="yen_per_share",
        input_builder=_asset_value_input,
        accounting_standard=accounting_standard,
        document_display_unit=document_display_unit,
        rule_version=rule_version,
    )
    _append_rows_from_inputs(
        out_rows,
        metric_rows=metric_rows,
        sample_row=sample_row,
        derived_metric_base="BusinessValue",
        metric_group="valuation",
        formula_name="business_value",
        value_unit="yen_per_share",
        input_builder=_business_value_input,
        accounting_standard=accounting_standard,
        document_display_unit=document_display_unit,
        rule_version=rule_version,
    )
    _append_rows_from_inputs(
        out_rows,
        metric_rows=metric_rows,
        sample_row=sample_row,
        derived_metric_base="TheoreticalSharePrice",
        metric_group="valuation",
        formula_name="theoretical_share_price",
        value_unit="yen_per_share",
        input_builder=_theoretical_share_price_input,
        accounting_standard=accounting_standard,
        document_display_unit=document_display_unit,
        rule_version=rule_version,
    )
    _append_rows_from_inputs(
        out_rows,
        metric_rows=metric_rows,
        sample_row=sample_row,
        derived_metric_base="UpperBoundTheoreticalSharePrice",
        metric_group="valuation",
        formula_name="upper_bound_theoretical_share_price",
        value_unit="yen_per_share",
        input_builder=_upper_bound_theoretical_share_price_input,
        accounting_standard=accounting_standard,
        document_display_unit=document_display_unit,
        rule_version=rule_version,
    )
    _append_ratio_rows(
        out_rows,
        metric_rows=metric_rows,
        sample_row=sample_row,
        derived_metric_base="TheoreticalPBR",
        metric_group="valuation",
        formula_name="theoretical_pbr",
        display_formula="theoretical_share_price / bps",
        numerator_builder=_theoretical_share_price_input,
        denominator_builder=_bps_input,
        accounting_standard=accounting_standard,
        document_display_unit=document_display_unit,
        rule_version=rule_version,
        require_positive_denominator=True,
    )
    _append_ratio_rows(
        out_rows,
        metric_rows=metric_rows,
        sample_row=sample_row,
        derived_metric_base="TheoreticalPER",
        metric_group="valuation",
        formula_name="theoretical_per",
        display_formula="theoretical_share_price / eps",
        numerator_builder=_theoretical_share_price_input,
        denominator_builder=_eps_input,
        accounting_standard=accounting_standard,
        document_display_unit=document_display_unit,
        rule_version=rule_version,
        require_positive_denominator=True,
    )
    _append_ratio_rows(
        out_rows,
        metric_rows=metric_rows,
        sample_row=sample_row,
        derived_metric_base="TheoreticalPCFR",
        metric_group="valuation",
        formula_name="theoretical_pcfr",
        display_formula="theoretical_share_price / operating_cash_per_share",
        numerator_builder=_theoretical_share_price_input,
        denominator_builder=_operating_cash_per_share_input,
        accounting_standard=accounting_standard,
        document_display_unit=document_display_unit,
        rule_version=rule_version,
        require_positive_denominator=True,
    )
    _append_sum_rows(
        out_rows,
        metric_rows=metric_rows,
        sample_row=sample_row,
        left_metric_base="OperatingCash",
        right_metric_base="InvestmentCash",
        derived_metric_base="FCF",
        metric_group="cashflow",
        formula_name="free_cash_flow",
        display_formula="operating_cash + investment_cash",
        accounting_standard=accounting_standard,
        document_display_unit=document_display_unit,
        rule_version=rule_version,
    )
    _append_ratio_rows(
        out_rows,
        metric_rows=metric_rows,
        sample_row=sample_row,
        derived_metric_base="FCFPerShare",
        metric_group="cashflow",
        formula_name="free_cash_flow_per_share",
        display_formula="free_cash_flow / outstanding_shares",
        numerator_builder=_fcf_input,
        denominator_builder=_outstanding_shares_input,
        accounting_standard=accounting_standard,
        document_display_unit=document_display_unit,
        rule_version=rule_version,
        require_positive_denominator=True,
        value_unit="yen_per_share",
    )

    return out_rows
