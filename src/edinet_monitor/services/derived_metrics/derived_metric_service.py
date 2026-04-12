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
) -> dict[str, Any]:
    return {
        "inputs": inputs,
        "display_formula": display_formula,
        "stored_formula": stored_formula,
        "calc_status": calc_status,
        "document_display_unit": document_display_unit,
    }


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

    def _gross_profit_input(metric_rows: dict[str, dict[str, Any]], suffix: str) -> dict[str, Any]:
        net_sales_key = _build_metric_key("NetSales", suffix)
        cost_of_sales_key = _build_metric_key("CostOfSales", suffix)
        net_sales = _metric_value(metric_rows, net_sales_key)
        cost_of_sales = _metric_value(metric_rows, cost_of_sales_key)
        value_num, calc_status = _difference_status(left_value=net_sales, right_value=cost_of_sales)
        return {
            "value_num": value_num,
            "calc_status": calc_status,
            "detail_inputs": {net_sales_key: net_sales, cost_of_sales_key: cost_of_sales},
            "reference_keys": [net_sales_key, cost_of_sales_key],
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

    _append_difference_rows(
        out_rows,
        metric_rows=metric_rows,
        sample_row=sample_row,
        left_metric_base="NetSales",
        right_metric_base="CostOfSales",
        derived_metric_base="GrossProfit",
        metric_group="profitability",
        formula_name="gross_profit",
        display_formula="net_sales - cost_of_sales",
        value_unit="yen",
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
