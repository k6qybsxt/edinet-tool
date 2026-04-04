import os
from datetime import datetime
from time import perf_counter

from openpyxl import load_workbook

from edinet_pipeline.domain.output_buffer import OutputBuffer
from edinet_pipeline.domain.raw_builder import RAW_COLS
from edinet_pipeline.services.loop_stage_service import (
    build_excel_write_inputs_stage,
    build_raw_rows_stage,
    create_loop_event,
    finalize_company_result_stage,
    prepare_excel_stage,
    resolve_runtime_flags,
    run_parse_stages,
    run_workbook_output_stages,
)
from edinet_pipeline.services.raw_service import build_raw_rows_all_docs
from edinet_pipeline.services.stock_service import write_stock_data_to_workbook
from edinet_pipeline.services.summary_service import write_loop_summary
from edinet_pipeline.services.template_contract_service import OPTIONAL_TEMPLATE_OUTPUT_NAMES
from edinet_pipeline.services.workbook_service import prepare_workbook


def process_one_loop(loop, date_pairs, skipped_files, logger, parse_cache=None, runtime=None):
    company_code_from_job = loop.get("company_code")
    company_name_from_job = loop.get("company_name")
    has_half_from_job = loop.get("has_half")
    source_zips = loop.get("source_zips") or []
    output_root = loop.get("output_root")

    parsed_docs = []
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    t0 = perf_counter()

    loop_event = create_loop_event(
        loop=loop,
        company_code=company_code_from_job,
        company_name=company_name_from_job,
        has_half=has_half_from_job,
        source_zips=source_zips,
        run_id=run_id,
    )

    out_buffer = OutputBuffer()

    excel_stage = prepare_excel_stage(
        loop=loop,
        run_id=run_id,
        skipped_files=skipped_files,
        logger=logger,
        prepare_workbook_func=prepare_workbook,
    )
    if excel_stage["failed_result"] is not None:
        return excel_stage["failed_result"]

    excel_file_path = excel_stage["excel_file_path"]
    loop_event["excel"] = os.path.basename(excel_file_path)
    xbrl_file_paths = loop["xbrl_file_paths"]

    logger.info(
        f"[company detect] slot={loop.get('slot')} "
        f"code={company_code_from_job} "
        f"name={company_name_from_job} "
        f"half={has_half_from_job} "
        f"file1={len(xbrl_file_paths.get('file1') or [])} "
        f"file2={len(xbrl_file_paths.get('file2') or [])} "
        f"file3={len(xbrl_file_paths.get('file3') or [])}"
    )

    parse_stage = run_parse_stages(
        loop=loop,
        xbrl_file_paths=xbrl_file_paths,
        excel_file_path=excel_file_path,
        parsed_docs=parsed_docs,
        skipped_files=skipped_files,
        loop_event=loop_event,
        out_buffer=out_buffer,
        logger=logger,
        perf_counter=perf_counter,
        parse_cache=parse_cache,
    )
    x1 = parse_stage["x1"]
    x2 = parse_stage["x2"]
    meta2 = parse_stage["meta2"]
    security_code = parse_stage["security_code"]
    use_half = parse_stage["use_half"]

    out_buffer_dict, display_unit = build_excel_write_inputs_stage(
        out_buffer=out_buffer,
        xbrl_file_paths=xbrl_file_paths,
        x1=x1,
        x2=x2,
        use_half=use_half,
        loop=loop,
        company_code=company_code_from_job,
        security_code=security_code,
        company_name=company_name_from_job,
        parse_cache=parse_cache,
        logger=logger,
    )

    raw_rows = build_raw_rows_stage(
        parsed_docs=parsed_docs,
        security_code=security_code,
        run_id=run_id,
        loop_event=loop_event,
        loop=loop,
        company_code=company_code_from_job,
        logger=logger,
        perf_counter=perf_counter,
        build_raw_rows_func=build_raw_rows_all_docs,
    )

    runtime_flags = resolve_runtime_flags(runtime)
    workbook_stage = run_workbook_output_stages(
        excel_file_path=excel_file_path,
        out_buffer_dict=out_buffer_dict,
        display_unit=display_unit,
        raw_rows=raw_rows,
        raw_cols=RAW_COLS,
        x1=x1,
        use_half=use_half,
        security_code=security_code,
        company_code=company_code_from_job,
        company_name=company_name_from_job,
        loop_event=loop_event,
        loop=loop,
        logger=logger,
        perf_counter=perf_counter,
        optional_output_names=OPTIONAL_TEMPLATE_OUTPUT_NAMES,
        write_raw_sheet=runtime_flags["write_raw_sheet"],
        enable_stock=runtime_flags["enable_stock"],
        load_workbook_func=load_workbook,
        write_stock_func=write_stock_data_to_workbook,
    )

    return finalize_company_result_stage(
        loop=loop,
        loop_event=loop_event,
        x1=x1,
        x2=x2,
        meta2=meta2,
        use_half=use_half,
        security_code=security_code,
        company_code=company_code_from_job,
        company_name=company_name_from_job,
        excel_file_path=excel_file_path,
        output_root=output_root,
        stock_status=workbook_stage["stock_status"],
        raw_rows=raw_rows,
        out_buffer_dict=out_buffer_dict,
        skipped_files=skipped_files,
        t0=t0,
        perf_counter=perf_counter,
        logger=logger,
        write_loop_summary_func=write_loop_summary,
    )
