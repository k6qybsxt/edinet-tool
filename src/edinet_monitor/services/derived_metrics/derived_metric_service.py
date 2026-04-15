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
    "OperatingCostsAndExpensesCOSExpOA",
    "OrdinaryExpensesBNK",
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


def _scaled_status(
    *,
    value_num: float | None,
    scale: float,
) -> tuple[float | None, str]:
    if value_num is None:
        return None, "missing_input"
    return value_num * scale, "ok"


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
                value_unit="ratio",
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
        issued_key = _build_metric_key("IssuedShares", suffix)
        treasury_key = _build_metric_key("TreasuryShares", suffix)
        issued_value = _metric_value(metric_rows, issued_key)
        treasury_value = _metric_value(metric_rows, treasury_key)
        value_num, calc_status, effective_treasury_value = _outstanding_shares_status(
            issued_shares=issued_value,
            treasury_shares=treasury_value,
        )
        reference_row = _pick_reference_row(metric_rows, [issued_key, treasury_key, sample_row["metric_key"]])

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
                value_num=value_num,
                value_unit="shares",
                calc_status=calc_status,
                formula_name="outstanding_shares",
                source_detail=_build_source_detail(
                    inputs={
                        issued_key: issued_value,
                        treasury_key: treasury_value,
                        f"{treasury_key}_effective": effective_treasury_value,
                    },
                    display_formula="issued_shares - treasury_shares (treat blank or <1000 treasury_shares as 0)",
                    stored_formula="issued_shares - treasury_shares_effective",
                    calc_status=calc_status,
                    document_display_unit=document_display_unit,
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
        metric_key = _build_metric_key(metric_base, suffix)
        value_num = _metric_value(metric_rows, metric_key)
        return {
            "value_num": value_num,
            "detail_inputs": {metric_key: value_num},
            "reference_keys": [metric_key],
        }

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

    _append_outstanding_shares_rows(
        out_rows,
        metric_rows=metric_rows,
        sample_row=sample_row,
        accounting_standard=accounting_standard,
        document_display_unit=document_display_unit,
        rule_version=rule_version,
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

    return out_rows
