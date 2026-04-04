from __future__ import annotations

import os
from calendar import monthrange
from datetime import datetime
from typing import Any, Callable

from edinet_pipeline.domain.output_buffer import OutputBuffer
from edinet_pipeline.domain.skip import SkipCode, add_skip
from edinet_pipeline.services.excel_service import (
    rename_excel_file,
    safe_filename,
    write_data_to_workbook_namedranges,
    write_rows_to_raw_sheet_workbook,
)
from edinet_pipeline.services.loop_types import (
    ExcelPrepareStageResult,
    LoopEvent,
    LoopInput,
    ParseStageResult,
    ProcessLoopResult,
    RuntimeFlags,
    WorkbookStageResult,
    XbrlFilePaths,
)
from edinet_pipeline.services.parse_service import (
    finalize_half_buffer,
    parse_half_doc,
    parse_latest_annual_doc,
    parse_old_annual_doc,
)
from edinet_pipeline.services.stock_service import build_stock_date_pairs_from_fiscal_year_end
from edinet_pipeline.services.xbrl_parser import parse_xbrl_file_raw


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
    loop: LoopInput,
    company_code: str | None,
    company_name: str | None,
    has_half: bool | None,
    source_zips: list[str],
    run_id: str,
) -> LoopEvent:
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


def build_excel_not_found_result(
    *,
    slot: int | None,
    company_code: str | None,
    company_name: str | None,
) -> ProcessLoopResult:
    return {
        "slot": slot,
        "company_code": company_code,
        "company_name": company_name,
        "status": "failed",
        "stock_status": None,
        "output_excel": None,
    }


def prepare_excel_stage(
    *,
    loop: LoopInput,
    run_id: str,
    skipped_files: list[dict[str, Any]],
    logger,
    prepare_workbook_func: Callable[..., tuple[str | None, str | None, str | None]],
    add_skip_func=add_skip,
) -> ExcelPrepareStageResult:
    selected_file, excel_file_path, excel_base_name = prepare_workbook_func(loop, run_id, logger)
    if selected_file:
        return {
            "selected_file": selected_file,
            "excel_file_path": excel_file_path,
            "excel_base_name": excel_base_name,
            "failed_result": None,
        }

    add_skip_func(
        skipped_files,
        code=SkipCode.EXCEL_NOT_FOUND,
        phase="excel_select",
        loop=loop,
        excel=excel_base_name,
        xbrl=None,
        message="使用するExcelが見つからない",
    )
    logger.warning(
        f"[company failed] slot={loop.get('slot')} "
        f"code={loop.get('company_code')} "
        f"name={loop.get('company_name')} "
        f"phase=excel_select"
    )
    return {
        "selected_file": selected_file,
        "excel_file_path": excel_file_path,
        "excel_base_name": excel_base_name,
        "failed_result": build_excel_not_found_result(
            slot=loop.get("slot"),
            company_code=loop.get("company_code"),
            company_name=loop.get("company_name"),
        ),
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


def run_parse_stages(
    *,
    loop: LoopInput,
    xbrl_file_paths: XbrlFilePaths,
    excel_file_path: str,
    parsed_docs: list[dict[str, Any]],
    skipped_files: list[dict[str, Any]],
    loop_event: LoopEvent,
    out_buffer: OutputBuffer,
    logger,
    perf_counter: Callable[[], float],
    parse_cache=None,
    parse_half_doc_func=parse_half_doc,
    parse_latest_annual_doc_func=parse_latest_annual_doc,
    parse_old_annual_doc_func=parse_old_annual_doc,
    finalize_half_buffer_func=finalize_half_buffer,
    append_initial_annual_output_func=append_initial_annual_output,
) -> ParseStageResult:
    x1, base_year, use_half = parse_half_doc_func(
        loop=loop,
        xbrl_file_paths=xbrl_file_paths,
        excel_file_path=excel_file_path,
        parsed_docs=parsed_docs,
        skipped_files=skipped_files,
        loop_event=loop_event,
        logger=logger,
        perf_counter=perf_counter,
        parse_cache=parse_cache,
    )

    if (not use_half) and x1 is not None:
        append_initial_annual_output_func(out_buffer, x1)

    x2, meta2, _path2, security_code, base_year = parse_latest_annual_doc_func(
        loop=loop,
        xbrl_file_paths=xbrl_file_paths,
        excel_file_path=excel_file_path,
        parsed_docs=parsed_docs,
        skipped_files=skipped_files,
        loop_event=loop_event,
        x1=x1,
        use_half=use_half,
        base_year=base_year,
        out_buffer=out_buffer,
        logger=logger,
        perf_counter=perf_counter,
        parse_cache=parse_cache,
    )

    parse_old_annual_doc_func(
        loop=loop,
        xbrl_file_paths=xbrl_file_paths,
        excel_file_path=excel_file_path,
        parsed_docs=parsed_docs,
        skipped_files=skipped_files,
        loop_event=loop_event,
        x1=x1,
        security_code=security_code,
        base_year=base_year,
        out_buffer=out_buffer,
        logger=logger,
        perf_counter=perf_counter,
        parse_cache=parse_cache,
    )

    finalize_half_buffer_func(
        loop=loop,
        xbrl_file_paths=xbrl_file_paths,
        excel_file_path=excel_file_path,
        skipped_files=skipped_files,
        loop_event=loop_event,
        use_half=use_half,
        x1=x1,
        out_buffer=out_buffer,
        logger=logger,
        perf_counter=perf_counter,
    )

    return {
        "x1": x1,
        "x2": x2,
        "meta2": meta2,
        "security_code": security_code,
        "base_year": base_year,
        "use_half": use_half,
    }


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
    out_buffer_dict_for_log: dict[str, Any],
    *,
    x1: dict | None,
    use_half: bool,
) -> dict[str, Any]:
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
    xbrl_file_paths: XbrlFilePaths,
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


def build_excel_write_inputs_stage(
    *,
    out_buffer: OutputBuffer,
    xbrl_file_paths: XbrlFilePaths,
    x1: dict | None,
    x2: dict | None,
    use_half: bool,
    loop: LoopInput,
    company_code: str | None,
    security_code: str | None,
    company_name: str | None,
    parse_cache,
    logger,
    parse_document_func=parse_xbrl_file_raw,
) -> tuple[dict[str, Any], str]:
    out_buffer_dict_for_log = out_buffer.to_dict()

    logger.info(
        f"[company parsed] slot={loop.get('slot')} "
        f"code={company_code or security_code} "
        f"name={company_name} "
        f"mode={'half' if use_half else 'full'} "
        f"buffer_keys={len(out_buffer_dict_for_log)}"
    )

    collisions = out_buffer.collisions()
    if collisions:
        logger.warning("[excel buffer] collisions=%d", len(collisions))
        for key, old_src, new_src in collisions[:50]:
            winner = out_buffer.winner_of(key)
            logger.debug(" overwrite: %s  %s -> %s (winner=%s)", key, old_src, new_src, winner)

    if out_buffer:
        out_buffer_dict = build_excel_output_payload(
            out_buffer_dict_for_log,
            x1=x1,
            use_half=use_half,
        )
        display_unit = resolve_document_display_unit(
            xbrl_file_paths=xbrl_file_paths,
            x1=x1,
            x2=x2,
            use_half=use_half,
            parse_cache=parse_cache,
            logger=logger,
            parse_document_func=parse_document_func,
        )
    else:
        out_buffer_dict = {}
        display_unit = "百万円"

    return out_buffer_dict, display_unit


def resolve_runtime_flags(runtime) -> RuntimeFlags:
    return {
        "write_raw_sheet": True if runtime is None else bool(getattr(runtime, "write_raw_sheet", True)),
        "enable_stock": True if runtime is None else bool(getattr(runtime, "enable_stock", True)),
    }


def build_stock_write_context(
    *,
    out_buffer_dict: dict[str, Any],
    x1: dict | None,
    use_half: bool,
    security_code: str | None,
) -> dict[str, Any]:
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


def run_workbook_output_stages(
    *,
    excel_file_path: str,
    out_buffer_dict: dict[str, Any],
    display_unit: str,
    raw_rows: list[dict],
    raw_cols: list[str],
    x1: dict | None,
    use_half: bool,
    security_code: str | None,
    company_code: str | None,
    company_name: str | None,
    loop_event: LoopEvent,
    loop: LoopInput,
    logger,
    perf_counter: Callable[[], float],
    optional_output_names: set[str] | frozenset[str],
    write_raw_sheet: bool,
    enable_stock: bool,
    load_workbook_func,
    write_stock_func,
) -> WorkbookStageResult:
    workbook = open_workbook_stage(
        excel_file_path=excel_file_path,
        loop_event=loop_event,
        loop=loop,
        company_code=company_code,
        security_code=security_code,
        logger=logger,
        perf_counter=perf_counter,
        load_workbook_func=load_workbook_func,
    )

    stock_status = None

    try:
        write_named_range_stage(
            workbook=workbook,
            out_buffer_dict=out_buffer_dict,
            display_unit=display_unit,
            loop_event=loop_event,
            loop=loop,
            company_code=company_code,
            security_code=security_code,
            company_name=company_name,
            logger=logger,
            perf_counter=perf_counter,
            optional_output_names=optional_output_names,
        )

        write_raw_sheet_stage(
            workbook=workbook,
            raw_rows=raw_rows,
            raw_cols=raw_cols,
            write_raw_sheet=write_raw_sheet,
            loop_event=loop_event,
            loop=loop,
            company_code=company_code,
            security_code=security_code,
            logger=logger,
            perf_counter=perf_counter,
        )

        stock_context = build_stock_write_context(
            out_buffer_dict=out_buffer_dict,
            x1=x1,
            use_half=use_half,
            security_code=security_code,
        )
        stock_status = execute_stock_write_stage(
            workbook=workbook,
            stock_code=stock_context["stock_code"],
            stock_date_pairs=stock_context["stock_date_pairs"],
            enable_stock=enable_stock,
            loop_event=loop_event,
            loop=loop,
            company_code=company_code,
            security_code=security_code,
            company_name=company_name,
            logger=logger,
            perf_counter=perf_counter,
            write_stock_func=write_stock_func,
        )

        save_workbook_stage(
            workbook=workbook,
            excel_file_path=excel_file_path,
            loop_event=loop_event,
            loop=loop,
            company_code=company_code,
            security_code=security_code,
            logger=logger,
            perf_counter=perf_counter,
        )
    finally:
        close_workbook_quietly(workbook)

    return {"stock_status": stock_status}


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


def finalize_company_result_stage(
    *,
    loop: LoopInput,
    loop_event: LoopEvent,
    x1: dict | None,
    x2: dict | None,
    meta2: dict | None,
    use_half: bool,
    security_code: str | None,
    company_code: str | None,
    company_name: str | None,
    excel_file_path: str,
    output_root: str | None,
    stock_status: str | None,
    raw_rows: list[dict[str, Any]],
    out_buffer_dict: dict[str, Any],
    skipped_files: list[dict[str, Any]],
    t0,
    perf_counter: Callable[[], float],
    logger,
    write_loop_summary_func,
) -> ProcessLoopResult:
    period_end_date = pick_period_end(x1, x2, meta2)
    final_security_code = security_code or company_code or ""
    final_company_name = pick_company_name(x1, x2, meta2, company_name)

    final_excel_file_path = finalize_output_excel(
        excel_file_path=excel_file_path,
        output_root=output_root,
        security_code=final_security_code,
        company_name=final_company_name,
        period_end_date=period_end_date,
        logger=logger,
    )

    loop["final_excel_file_path"] = final_excel_file_path
    loop_event["excel"] = os.path.basename(final_excel_file_path)
    loop_event["company_name"] = final_company_name

    write_loop_summary_func(
        loop_event=loop_event,
        security_code=final_security_code,
        raw_rows=raw_rows,
        out_buffer_dict=out_buffer_dict,
        skipped_files=skipped_files,
        loop=loop,
        t0=t0,
        perf_counter=perf_counter,
        logger=logger,
    )

    logger.info(
        f"[company done] slot={loop.get('slot')} "
        f"code={final_security_code} "
        f"name={final_company_name} "
        f"mode={'half' if use_half else 'full'} "
        f"output_excel={final_excel_file_path}"
    )

    return {
        "slot": loop.get("slot"),
        "company_code": final_security_code,
        "company_name": final_company_name,
        "status": "success",
        "stock_status": stock_status,
        "output_excel": final_excel_file_path,
    }


def build_raw_rows_stage(
    *,
    parsed_docs: list[dict[str, Any]],
    security_code: str | None,
    run_id: str,
    loop_event: LoopEvent,
    loop: LoopInput,
    company_code: str | None,
    logger,
    perf_counter: Callable[[], float],
    build_raw_rows_func,
) -> list[dict[str, Any]]:
    t = perf_counter()
    raw_rows = build_raw_rows_func(
        parsed_docs=parsed_docs,
        security_code=security_code,
        run_id=run_id,
        logger=logger,
    )
    loop_event["phases"]["raw_build"] = {"ok": True, "sec": round(perf_counter() - t, 3)}
    logger.info(
        "[raw build] slot=%s code=%s rows=%d sec=%s",
        loop.get("slot"),
        company_code or security_code,
        len(raw_rows),
        round(perf_counter() - t, 3),
    )
    return raw_rows


def open_workbook_stage(
    *,
    excel_file_path: str,
    loop_event: LoopEvent,
    loop: LoopInput,
    company_code: str | None,
    security_code: str | None,
    logger,
    perf_counter: Callable[[], float],
    load_workbook_func,
):
    t = perf_counter()
    workbook = load_workbook_func(
        excel_file_path,
        keep_vba=excel_file_path.lower().endswith(".xlsm"),
    )
    loop_event["phases"]["workbook_open"] = {"ok": True, "sec": round(perf_counter() - t, 3)}
    logger.info(
        "[workbook open] slot=%s code=%s sec=%s",
        loop.get("slot"),
        company_code or security_code,
        round(perf_counter() - t, 3),
    )
    return workbook


def write_named_range_stage(
    *,
    workbook,
    out_buffer_dict: dict[str, Any],
    display_unit: str,
    loop_event: LoopEvent,
    loop: LoopInput,
    company_code: str | None,
    security_code: str | None,
    company_name: str | None,
    logger,
    perf_counter: Callable[[], float],
    optional_output_names: set[str] | frozenset[str],
    write_namedranges_func=write_data_to_workbook_namedranges,
) -> None:
    if not out_buffer_dict:
        loop_event["counts"]["named_ranges_written"] = 0
        loop_event["counts"]["named_ranges_missing"] = 0
        loop_event["phases"]["excel_write"] = {"ok": True, "sec": 0.0}
        return

    t = perf_counter()
    write_result = write_namedranges_func(
        workbook,
        out_buffer_dict,
        display_unit=display_unit,
    )
    unexpected_missing_named_ranges = [
        name for name in write_result["missing"]
        if name not in optional_output_names
    ]
    loop_event["counts"]["named_ranges_written"] = len(write_result["written"])
    loop_event["counts"]["named_ranges_missing"] = len(unexpected_missing_named_ranges)

    if unexpected_missing_named_ranges:
        loop_event["missing_named_ranges"] = unexpected_missing_named_ranges[:25]
        logger.warning(
            "[template mismatch] slot=%s code=%s missing_named_ranges=%d sample=%s",
            loop.get("slot"),
            company_code or security_code,
            len(unexpected_missing_named_ranges),
            unexpected_missing_named_ranges[:10],
        )

    loop_event["phases"]["excel_write"] = {"ok": True, "sec": round(perf_counter() - t, 3)}
    logger.info(
        "[company excel] slot=%s code=%s name=%s ranges=%d",
        loop.get("slot"),
        company_code or security_code,
        company_name,
        len(out_buffer_dict),
    )


def write_raw_sheet_stage(
    *,
    workbook,
    raw_rows: list[dict[str, Any]],
    raw_cols: list[str],
    write_raw_sheet: bool,
    loop_event: LoopEvent,
    loop: LoopInput,
    company_code: str | None,
    security_code: str | None,
    logger,
    perf_counter: Callable[[], float],
    write_raw_sheet_func=write_rows_to_raw_sheet_workbook,
) -> None:
    if not write_raw_sheet:
        loop_event["phases"]["raw_write"] = {"ok": True, "sec": 0.0}
        logger.info(
            "[raw write] slot=%s code=%s skipped_by_runtime=1",
            loop.get("slot"),
            company_code or security_code,
        )
        return

    t = perf_counter()
    write_raw_sheet_func(
        workbook,
        raw_rows,
        raw_cols,
        sheet_name="raw_edinet",
    )
    loop_event["phases"]["raw_write"] = {"ok": True, "sec": round(perf_counter() - t, 3)}
    logger.info(
        "[raw write] slot=%s code=%s rows=%d sec=%s",
        loop.get("slot"),
        company_code or security_code,
        len(raw_rows),
        round(perf_counter() - t, 3),
    )


def execute_stock_write_stage(
    *,
    workbook,
    stock_code: str | None,
    stock_date_pairs: list[dict],
    enable_stock: bool,
    loop_event: LoopEvent,
    loop: LoopInput,
    company_code: str | None,
    security_code: str | None,
    company_name: str | None,
    logger,
    perf_counter: Callable[[], float],
    write_stock_func,
) -> str:
    if not enable_stock:
        logger.info(
            "[stock write] slot=%s code=%s skipped_by_runtime=1",
            loop.get("slot"),
            company_code or security_code,
        )
        return "disabled"

    if stock_code and stock_date_pairs:
        t = perf_counter()
        stock_result = write_stock_func(
            workbook,
            stock_code,
            stock_date_pairs,
            logger,
        )
        loop_event["phases"]["stock_write"] = {"ok": True, "sec": round(perf_counter() - t, 3)}
        logger.info(
            "[stock write] slot=%s code=%s sec=%s",
            loop.get("slot"),
            company_code or security_code,
            round(perf_counter() - t, 3),
        )
        logger.info(
            "[company stock] slot=%s code=%s name=%s written=%s miss=%s errors=%s",
            loop.get("slot"),
            company_code or security_code,
            company_name,
            stock_result["written"],
            stock_result["miss"],
            stock_result["errors"],
        )
        if stock_result["errors"] > 0:
            return "partial_success"
        return "success"

    if stock_code and not stock_date_pairs:
        logger.warning(
            "[company stock] slot=%s code=%s name=%s skip_reason=no_fiscal_year_end",
            loop.get("slot"),
            company_code or security_code,
            company_name,
        )

    return "success"


def save_workbook_stage(
    *,
    workbook,
    excel_file_path: str,
    loop_event: LoopEvent,
    loop: LoopInput,
    company_code: str | None,
    security_code: str | None,
    logger,
    perf_counter: Callable[[], float],
) -> None:
    t = perf_counter()
    workbook.save(excel_file_path)
    loop_event["phases"]["workbook_save"] = {"ok": True, "sec": round(perf_counter() - t, 3)}
    logger.info(
        "[workbook save] slot=%s code=%s sec=%s",
        loop.get("slot"),
        company_code or security_code,
        round(perf_counter() - t, 3),
    )


def close_workbook_quietly(workbook) -> None:
    if workbook is None:
        return

    try:
        if getattr(workbook, "_archive", None) is not None:
            try:
                workbook._archive.close()
            except Exception:
                pass
            workbook._archive = None
    except Exception:
        pass

    try:
        if getattr(workbook, "vba_archive", None) is not None:
            try:
                workbook.vba_archive.close()
            except Exception:
                pass
            workbook.vba_archive = None
    except Exception:
        pass

    try:
        workbook.close()
    except Exception:
        pass
