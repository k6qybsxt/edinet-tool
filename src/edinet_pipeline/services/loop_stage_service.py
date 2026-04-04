from __future__ import annotations

import os
from calendar import monthrange
from datetime import datetime

from edinet_pipeline.domain.output_buffer import OutputBuffer
from edinet_pipeline.services.excel_service import rename_excel_file, safe_filename
from edinet_pipeline.services.stock_service import build_stock_date_pairs_from_fiscal_year_end


_PERIOD_END_KEYS = (
    "CurrentPeriodEndDateDEI",
    "CurrentFiscalYearEndDateDEI",
    "CurrentQuarterEndDateDEI",
    "PeriodEndDEI",
    "HalfPeriodEndDateDEI",
)

_COMPANY_NAME_KEYS = (
    "CompanyNameCoverPage",
    "FilerNameInJapaneseDEI",
    "CompanyNameInJapaneseDEI",
    "CompanyNameDEI",
    "FilerNameDEI",
)

_VALID_DISPLAY_UNITS = ("百万円", "千円")


def create_loop_event(
    *,
    loop: dict,
    company_code: str | None,
    company_name: str | None,
    has_half: bool | None,
    source_zips: list[str],
    run_id: str,
) -> dict:
    return {
        "ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "run_id": run_id,
        "slot": loop.get("slot"),
        "excel": os.path.basename(loop.get("excel_file_path", "")) if loop.get("excel_file_path") else None,
        "security_code": None,
        "company_code": company_code,
        "company_name": company_name,
        "has_half": has_half,
        "source_zips": source_zips,
        "phases": {},
        "counts": {},
        "errors": [],
    }


def append_initial_annual_output(out_buffer: OutputBuffer, x1: dict | None) -> None:
    if x1 is None:
        return

    out1_write = {"UseHalfModeFlag": 0}

    for key, value in x1.items():
        if value in (None, ""):
            continue
        if key.endswith("Quarter"):
            continue
        if key.endswith(("Prior2", "Prior3", "Prior4")):
            continue
        out1_write[key] = value

    for key in list(out1_write.keys()):
        if key.startswith("TotalNumber"):
            del out1_write[key]

    if x1.get("TotalNumberCurrent") not in (None, ""):
        out1_write["TotalNumberCurrent"] = x1["TotalNumberCurrent"]

    for key, value in out1_write.items():
        if value in (None, ""):
            continue
        out_buffer.put(key, value, "file1_annual")


def pick_period_end(x1: dict | None, x2: dict | None, meta2: dict | None) -> str:
    candidates: list[str] = []

    for src in (x1 or {}, x2 or {}, meta2 or {}):
        if not isinstance(src, dict):
            continue
        for key in _PERIOD_END_KEYS:
            value = src.get(key)
            if value not in (None, ""):
                candidates.append(str(value).strip())

    for value in candidates:
        if len(value) >= 10:
            return value[:10].replace("/", "-")

    return datetime.now().strftime("%Y-%m-%d")


def pick_company_name(
    x1: dict | None,
    x2: dict | None,
    meta2: dict | None,
    company_name_from_job: str | None,
) -> str:
    if company_name_from_job not in (None, ""):
        return str(company_name_from_job).strip()

    for src in (x1 or {}, x2 or {}, meta2 or {}):
        if not isinstance(src, dict):
            continue
        for key in _COMPANY_NAME_KEYS:
            value = src.get(key)
            if value not in (None, ""):
                return str(value).strip()

    return ""


def shift_year_keep_month_end(date_str: str, years: int) -> str:
    normalized = str(date_str or "").strip().replace("/", "-")
    dt = datetime.strptime(normalized[:10], "%Y-%m-%d")
    year = dt.year + years
    day = min(dt.day, monthrange(year, dt.month)[1])
    return dt.replace(year=year, day=day).strftime("%Y-%m-%d")


def build_excel_output_payload(
    out_buffer_dict_for_log: dict,
    *,
    x1: dict | None,
    use_half: bool,
) -> dict:
    out_buffer_dict = dict(out_buffer_dict_for_log)

    if not use_half and isinstance(x1, dict):
        fiscal_year_end = x1.get("CurrentFiscalYearEndDateDEI")
        if fiscal_year_end not in (None, ""):
            fiscal_year_end = str(fiscal_year_end).strip().replace("/", "-")
            out_buffer_dict["CurrentFiscalYearEndDateDEI"] = fiscal_year_end

            parts = fiscal_year_end.split("-")
            if len(parts) >= 2:
                out_buffer_dict["CurrentFiscalYearEndDateDEIyear"] = parts[0]
                out_buffer_dict["CurrentFiscalYearEndDateDEImonth"] = parts[1]

        period_end = x1.get("CurrentPeriodEndDateDEI")
        if period_end not in (None, ""):
            out_buffer_dict["CurrentPeriodEndDateDEI"] = str(period_end).strip().replace("/", "-")

        fiscal_year_start = x1.get("CurrentFiscalYearStartDateDEI")
        if fiscal_year_start not in (None, ""):
            out_buffer_dict["CurrentFiscalYearStartDateDEI"] = str(fiscal_year_start).strip().replace("/", "-")

    if not use_half:
        out_buffer_dict = {
            key: value
            for key, value in out_buffer_dict.items()
            if not key.endswith("Quarter")
        }

    return out_buffer_dict


def resolve_document_display_unit(
    *,
    xbrl_file_paths: dict,
    x1: dict | None,
    x2: dict | None,
    use_half: bool,
    parse_cache,
    logger,
    parse_document_func,
) -> str:
    display_unit = "百万円"

    if parse_cache is not None and x1 is not None:
        file1_list = xbrl_file_paths.get("file1") or []
        if file1_list:
            doc1 = parse_cache.get_or_create(
                file1_list[0],
                parser_func=lambda path: parse_document_func(
                    path,
                    mode="half" if use_half else "full",
                    logger=logger,
                ),
            )
            if doc1.document_display_unit in _VALID_DISPLAY_UNITS:
                display_unit = doc1.document_display_unit
    elif isinstance(x1, dict) and x1.get("DocumentDisplayUnit") in _VALID_DISPLAY_UNITS:
        display_unit = x1["DocumentDisplayUnit"]

    if display_unit == "百万円":
        if parse_cache is not None and x2 is not None:
            file2_list = xbrl_file_paths.get("file2") or []
            if file2_list:
                doc2 = parse_cache.get_or_create(
                    file2_list[0],
                    parser_func=lambda path: parse_document_func(
                        path,
                        mode="full",
                        logger=logger,
                    ),
                )
                if doc2.document_display_unit in _VALID_DISPLAY_UNITS:
                    display_unit = doc2.document_display_unit
        elif isinstance(x2, dict) and x2.get("DocumentDisplayUnit") in _VALID_DISPLAY_UNITS:
            display_unit = x2["DocumentDisplayUnit"]

    return display_unit


def build_stock_write_context(
    *,
    out_buffer_dict: dict,
    x1: dict | None,
    use_half: bool,
    security_code: str | None,
) -> dict:
    fiscal_year_end = out_buffer_dict.get("CurrentFiscalYearEndDateDEI")

    if not fiscal_year_end and isinstance(x1, dict):
        fiscal_year_end = x1.get("CurrentFiscalYearEndDateDEI")

    if fiscal_year_end:
        fiscal_year_end = str(fiscal_year_end).strip().replace("/", "-")
        if use_half:
            fiscal_year_end = shift_year_keep_month_end(fiscal_year_end, -1)
        stock_date_pairs = build_stock_date_pairs_from_fiscal_year_end(fiscal_year_end)
    else:
        stock_date_pairs = []

    return {
        "fiscal_year_end": fiscal_year_end,
        "stock_code": f"{security_code}.T" if security_code else None,
        "stock_date_pairs": stock_date_pairs,
    }


def finalize_output_excel(
    *,
    excel_file_path: str,
    output_root: str | None,
    security_code: str,
    company_name: str,
    period_end_date: str,
    logger,
) -> str:
    if output_root:
        output_excel_dir = os.path.join(output_root, "excel")
        os.makedirs(output_excel_dir, exist_ok=True)

        code = safe_filename(security_code)
        name = safe_filename(company_name)
        date = safe_filename(period_end_date)
        base_name = f"{code}_{name}_{date}".strip("_")
        final_excel_file_path = os.path.join(output_excel_dir, f"{base_name}.xlsm")

        counter = 1
        while os.path.exists(final_excel_file_path):
            final_excel_file_path = os.path.join(output_excel_dir, f"{base_name}_{counter}.xlsm")
            counter += 1

        os.replace(excel_file_path, final_excel_file_path)
        logger.info("Excelファイルが移動されました: %s", final_excel_file_path)
        return final_excel_file_path

    return rename_excel_file(
        excel_file_path,
        security_code,
        company_name,
        period_end_date,
        logger,
    )
