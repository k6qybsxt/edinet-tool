from openpyxl import load_workbook
from edinet_pipeline.services.excel_service import (
    write_data_to_workbook_namedranges,
    write_rows_to_raw_sheet_workbook,
)
from edinet_pipeline.services.stock_service import write_stock_data_to_workbook
from edinet_pipeline.services.workbook_service import prepare_workbook
from edinet_pipeline.services.loop_stage_service import (
    append_initial_annual_output,
    build_excel_output_payload,
    build_stock_write_context,
    create_loop_event,
    finalize_output_excel,
    pick_company_name,
    pick_period_end,
    resolve_document_display_unit,
)
from edinet_pipeline.services.parse_service import (
    parse_half_doc,
    parse_latest_annual_doc,
    parse_old_annual_doc,
    finalize_half_buffer,
)
from edinet_pipeline.services.summary_service import write_loop_summary
from edinet_pipeline.services.template_contract_service import OPTIONAL_TEMPLATE_OUTPUT_NAMES
from edinet_pipeline.services.raw_service import build_raw_rows_all_docs
from edinet_pipeline.services.xbrl_parser import parse_xbrl_file_raw

from edinet_pipeline.domain.skip import SkipCode, add_skip
from edinet_pipeline.domain.raw_builder import RAW_COLS
from edinet_pipeline.domain.output_buffer import OutputBuffer

import os
from datetime import datetime
from time import perf_counter

# XBRLデータの取得、証券コードの取得、Excelへの書き込み、株価データ取得までをループ処理に含める
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

    selected_file, excel_file_path, excel_base_name = prepare_workbook(loop, run_id, logger)
    if not selected_file:
        add_skip(
            skipped_files,
            code=SkipCode.EXCEL_NOT_FOUND,
            phase="excel_select",
            loop=loop,
            excel=excel_base_name,
            xbrl=None,
            message="使用するExcelが見つからない"
        )
        logger.warning(
            f"[company failed] slot={loop.get('slot')} "
            f"code={company_code_from_job} "
            f"name={company_name_from_job} "
            f"phase=excel_select"
        )
        return {
            "slot": loop.get("slot"),
            "company_code": company_code_from_job,
            "company_name": company_name_from_job,
            "status": "failed",
            "stock_status": None,
            "output_excel": None,
        }

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

    security_code = None
    base_year = None

    x1, base_year, use_half = parse_half_doc(
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
        append_initial_annual_output(out_buffer, x1)

    x2, meta2, path2, security_code, base_year = parse_latest_annual_doc(
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

    parse_old_annual_doc(
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

    finalize_half_buffer(
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

    out_buffer_dict_for_log = out_buffer.to_dict()

    logger.info(
        f"[company parsed] slot={loop.get('slot')} "
        f"code={company_code_from_job or security_code} "
        f"name={company_name_from_job} "
        f"mode={'half' if use_half else 'full'} "
        f"buffer_keys={len(out_buffer_dict_for_log)}"
    )   

    collisions = out_buffer.collisions()
    if collisions:
        logger.warning("[excel buffer] collisions=%d", len(collisions))
        for k, old_src, new_src in collisions[:50]:
            winner = out_buffer.winner_of(k)
            logger.debug(" overwrite: %s  %s -> %s (winner=%s)", k, old_src, new_src, winner)

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
            parse_document_func=parse_xbrl_file_raw,
        )
    else:
        out_buffer_dict = {}
        display_unit = "百万円"

    t = perf_counter()

    raw_rows = build_raw_rows_all_docs(
        parsed_docs=parsed_docs,
        security_code=security_code,
        run_id=run_id,
        logger=logger,
    )
    loop_event["phases"]["raw_build"] = {"ok": True, "sec": round(perf_counter() - t, 3)}
    logger.info(
        f"[raw build] slot={loop.get('slot')} "
        f"code={company_code_from_job or security_code} "
        f"rows={len(raw_rows)} "
        f"sec={round(perf_counter() - t, 3)}"
    )

    t = perf_counter()

    wb = load_workbook(
        excel_file_path,
        keep_vba=excel_file_path.lower().endswith(".xlsm")
    )
    loop_event["phases"]["workbook_open"] = {"ok": True, "sec": round(perf_counter() - t, 3)}
    logger.info(
        f"[workbook open] slot={loop.get('slot')} "
        f"code={company_code_from_job or security_code} "
        f"sec={round(perf_counter() - t, 3)}"
    )

    stock_date_pairs = []
    stock_status = None
    write_raw_sheet = True if runtime is None else bool(getattr(runtime, "write_raw_sheet", True))
    enable_stock = True if runtime is None else bool(getattr(runtime, "enable_stock", True))


    try:
        if out_buffer_dict:
            t = perf_counter()

            write_result = write_data_to_workbook_namedranges(
                wb,
                out_buffer_dict,
                display_unit=display_unit,
            )
            unexpected_missing_named_ranges = [
                name for name in write_result["missing"]
                if name not in OPTIONAL_TEMPLATE_OUTPUT_NAMES
            ]
            loop_event["counts"]["named_ranges_written"] = len(write_result["written"])
            loop_event["counts"]["named_ranges_missing"] = len(unexpected_missing_named_ranges)

            if unexpected_missing_named_ranges:
                loop_event["missing_named_ranges"] = unexpected_missing_named_ranges[:25]
                logger.warning(
                    "[template mismatch] slot=%s code=%s missing_named_ranges=%d sample=%s",
                    loop.get("slot"),
                    company_code_from_job or security_code,
                    len(unexpected_missing_named_ranges),
                    unexpected_missing_named_ranges[:10],
                )

            loop_event["phases"]["excel_write"] = {"ok": True, "sec": round(perf_counter() - t, 3)}
            logger.info(
                f"[company excel] slot={loop.get('slot')} "
                f"code={company_code_from_job or security_code} "
                f"name={company_name_from_job} "
                f"ranges={len(out_buffer_dict)}"
            )
        else:
            loop_event["counts"]["named_ranges_written"] = 0
            loop_event["counts"]["named_ranges_missing"] = 0
            loop_event["phases"]["excel_write"] = {"ok": True, "sec": 0.0}

        t = perf_counter()

        if write_raw_sheet:
            write_rows_to_raw_sheet_workbook(
                wb,
                raw_rows,
                RAW_COLS,
                sheet_name="raw_edinet",
            )

            loop_event["phases"]["raw_write"] = {"ok": True, "sec": round(perf_counter() - t, 3)}
            logger.info(
                f"[raw write] slot={loop.get('slot')} "
                f"code={company_code_from_job or security_code} "
                f"rows={len(raw_rows)} "
                f"sec={round(perf_counter() - t, 3)}"
            )
        else:
            loop_event["phases"]["raw_write"] = {"ok": True, "sec": 0.0}
            logger.info(
                "[raw write] slot=%s code=%s skipped_by_runtime=1",
                loop.get("slot"),
                company_code_from_job or security_code,
            )

        stock_context = build_stock_write_context(
            out_buffer_dict=out_buffer_dict,
            x1=x1,
            use_half=use_half,
            security_code=security_code,
        )
        stock_date_pairs = stock_context["stock_date_pairs"]
        stock_code = stock_context["stock_code"]

        if not enable_stock:
            logger.info(
                "[stock write] slot=%s code=%s skipped_by_runtime=1",
                loop.get("slot"),
                company_code_from_job or security_code,
            )
            stock_status = "disabled"
        elif stock_code and stock_date_pairs:

            t = perf_counter()

            stock_result = write_stock_data_to_workbook(
                wb,
                stock_code,
                stock_date_pairs,
                logger,
            )

            loop_event["phases"]["stock_write"] = {"ok": True, "sec": round(perf_counter() - t, 3)}
            logger.info(
                f"[stock write] slot={loop.get('slot')} "
                f"code={company_code_from_job or security_code} "
                f"sec={round(perf_counter() - t, 3)}"
            )

            if stock_result["errors"] > 0:
                stock_status = "partial_success"
            else:
                stock_status = "success"

            logger.info(
                f"[company stock] slot={loop.get('slot')} "
                f"code={company_code_from_job or security_code} "
                f"name={company_name_from_job} "
                f"written={stock_result['written']} "
                f"miss={stock_result['miss']} "
                f"errors={stock_result['errors']}"
            )

        elif stock_code and not stock_date_pairs:
            logger.warning(
                f"[company stock] slot={loop.get('slot')} "
                f"code={company_code_from_job or security_code} "
                f"name={company_name_from_job} "
                f"skip_reason=no_fiscal_year_end"
            )
            stock_status = "success"

        else:
            stock_status = "success"

        t = perf_counter()

        wb.save(excel_file_path)

        loop_event["phases"]["workbook_save"] = {"ok": True, "sec": round(perf_counter() - t, 3)}
        logger.info(
            f"[workbook save] slot={loop.get('slot')} "
            f"code={company_code_from_job or security_code} "
            f"sec={round(perf_counter() - t, 3)}"
        )

    finally:
        if wb is not None:

            try:
                if getattr(wb, "_archive", None) is not None:
                    try:
                        wb._archive.close()
                    except Exception:
                        pass
                    wb._archive = None
            except Exception:
                pass

            try:
                if getattr(wb, "vba_archive", None) is not None:
                    try:
                        wb.vba_archive.close()
                    except Exception:
                        pass
                    wb.vba_archive = None
            except Exception:
                pass

            try:
                wb.close()
            except Exception:
                pass

            wb = None

    period_end_date = pick_period_end(x1, x2, meta2)
    final_security_code = security_code or company_code_from_job or ""
    final_company_name = pick_company_name(x1, x2, meta2, company_name_from_job)

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

    write_loop_summary(
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

    status = "success"

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
        "status": status,
        "stock_status": stock_status,
        "output_excel": final_excel_file_path,
    }
