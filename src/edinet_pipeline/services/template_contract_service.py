from __future__ import annotations

from pathlib import Path

import openpyxl

from edinet_pipeline.services.excel_service import get_defined_name_set


REQUIRED_TEMPLATE_SHEETS = {
    "raw_edinet",
    "決算入力",
    "決算分析",
}

REQUIRED_TEMPLATE_NAMED_RANGES = {
    "CompanyNameCoverPage",
    "SecurityCodeDEI",
    "CurrentFiscalYearEndDateDEIyear",
    "CurrentFiscalYearEndDateDEImonth",
    "UseHalfModeFlag",
    "NetSales_Current",
    "OperatingIncome_Current",
    "OrdinaryIncome_Current",
    "ProfitLoss_Current",
    "TotalAssets_Current",
    "NetAssets_Current",
    "CashAndCashEquivalents_Current",
}

STOCK_TEMPLATE_NAMED_RANGES = {
    "StockPrice_Q1",
    "StockPrice_Q2",
    "StockPrice_Q3",
    "StockPrice_Q4",
    "StockPrice_Prior1",
    "StockPrice_Prior2",
    "StockPrice_Prior3",
    "StockPrice_Prior4",
}

OPTIONAL_TEMPLATE_OUTPUT_NAMES = {
    "CurrentFiscalYearEndDateDEI",
    "CurrentPeriodEndDateDEI",
    "CurrentFiscalYearStartDateDEI",
}


def validate_template_contract(
    template_path: str | Path,
    *,
    include_stock_ranges: bool = True,
) -> dict:
    resolved_path = Path(template_path)
    workbook = openpyxl.load_workbook(
        resolved_path,
        keep_vba=resolved_path.suffix.lower() == ".xlsm",
    )
    try:
        sheet_names = set(workbook.sheetnames)
        defined_names = get_defined_name_set(workbook)
    finally:
        try:
            if getattr(workbook, "vba_archive", None) is not None:
                workbook.vba_archive.close()
                workbook.vba_archive = None
        except Exception:
            pass
        workbook.close()

    required_named_ranges = set(REQUIRED_TEMPLATE_NAMED_RANGES)
    if include_stock_ranges:
        required_named_ranges.update(STOCK_TEMPLATE_NAMED_RANGES)

    missing_sheets = sorted(REQUIRED_TEMPLATE_SHEETS - sheet_names)
    missing_named_ranges = sorted(required_named_ranges - defined_names)

    return {
        "template_path": str(resolved_path),
        "sheet_count": len(sheet_names),
        "defined_name_count": len(defined_names),
        "required_sheet_count": len(REQUIRED_TEMPLATE_SHEETS),
        "required_named_range_count": len(required_named_ranges),
        "missing_sheets": missing_sheets,
        "missing_named_ranges": missing_named_ranges,
    }


def ensure_template_contract(
    template_path: str | Path,
    *,
    include_stock_ranges: bool = True,
) -> dict:
    report = validate_template_contract(
        template_path,
        include_stock_ranges=include_stock_ranges,
    )

    if not report["missing_sheets"] and not report["missing_named_ranges"]:
        return report

    parts = []
    if report["missing_sheets"]:
        parts.append(f"missing_sheets={','.join(report['missing_sheets'])}")
    if report["missing_named_ranges"]:
        parts.append(
            "missing_named_ranges="
            + ",".join(report["missing_named_ranges"])
        )
    raise ValueError(
        f"template contract check failed for {report['template_path']}: " + " ".join(parts)
    )
