from openpyxl import load_workbook
from edinet_tool.services.excel_service import (
    write_data_to_workbook_namedranges,
    write_rows_to_raw_sheet_workbook,
    rename_excel_file,
)
from edinet_tool.services.stock_service import write_stock_data_to_workbook
from edinet_tool.services.workbook_service import prepare_workbook
from edinet_tool.services.parse_service import (
    parse_half_doc,
    parse_latest_annual_doc,
    parse_old_annual_doc,
    finalize_half_buffer,
)
from edinet_tool.services.summary_service import write_loop_summary
from edinet_tool.services.raw_service import build_raw_rows_all_docs
from edinet_tool.services.xbrl_parser import parse_xbrl_file_raw

from edinet_tool.domain.skip import SkipCode, add_skip
from edinet_tool.domain.raw_builder import RAW_COLS
from edinet_tool.domain.output_buffer import OutputBuffer

import os
from datetime import datetime
from time import perf_counter

# XBRLデータの取得、証券コードの取得、Excelへの書き込み、株価データ取得までをループ処理に含める
def process_one_loop(loop, date_pairs, skipped_files, logger, parse_cache=None):

    def _pick_period_end(x1, x2, meta2):
        candidates = []

        for src in (x1 or {}, x2 or {}, meta2 or {}):
            if not isinstance(src, dict):
                continue
            for key in (
                "CurrentPeriodEndDateDEI",
                "CurrentFiscalYearEndDateDEI",
                "CurrentQuarterEndDateDEI",
                "PeriodEndDEI",
                "HalfPeriodEndDateDEI",
            ):
                v = src.get(key)
                if v not in (None, ""):
                    candidates.append(str(v).strip())

        for v in candidates:
            if len(v) >= 10:
                return v[:10].replace("/", "-")

        return datetime.now().strftime("%Y-%m-%d")

    def _pick_company_name(x1, x2, meta2, company_name_from_job):
        if company_name_from_job not in (None, ""):
            return str(company_name_from_job).strip()

        for src in (x1 or {}, x2 or {}, meta2 or {}):
            if not isinstance(src, dict):
                continue
            for key in (
                "CompanyNameCoverPage",
                "FilerNameInJapaneseDEI",
                "CompanyNameInJapaneseDEI",
                "CompanyNameDEI",
                "FilerNameDEI",
            ):
                v = src.get(key)
                if v not in (None, ""):
                    return str(v).strip()

        return ""

    company_code_from_job = loop.get("company_code")
    company_name_from_job = loop.get("company_name")
    has_half_from_job = loop.get("has_half")
    source_zips = loop.get("source_zips") or []
    output_root = loop.get("output_root")

    parsed_docs = []

    run_id = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    t0 = perf_counter()

    loop_event = {
        "ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "run_id": run_id,
        "slot": loop.get("slot"),
        "excel": os.path.basename(loop.get("excel_file_path", "")) if loop.get("excel_file_path") else None,
        "security_code": None,
        "company_code": company_code_from_job,
        "company_name": company_name_from_job,
        "has_half": has_half_from_job,
        "source_zips": source_zips,
        "phases": {},
        "counts": {},
        "errors": [],
    }

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
        out1_write = {}
        out1_write["UseHalfModeFlag"] = 0

        for k, v in x1.items():
            if v in (None, ""):
                continue
            if k.endswith("Quarter"):
                continue
            elif k.endswith("Prior2"):
                continue
            elif k.endswith("Prior3"):
                continue
            elif k.endswith("Prior4"):
                continue
            else:
                out1_write[k] = v

        for kk in list(out1_write.keys()):
            if kk.startswith("TotalNumber"):
                del out1_write[kk]

        if x1.get("TotalNumberCurrent") not in (None, ""):
            out1_write["TotalNumberCurrent"] = x1["TotalNumberCurrent"]

        for k, v in out1_write.items():
            if v in (None, ""):
                continue
            out_buffer.put(k, v, "file1_annual")

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
        out_buffer_dict = out_buffer_dict_for_log

        if not use_half and isinstance(x1, dict):
            fy_end = x1.get("CurrentFiscalYearEndDateDEI")
            if fy_end not in (None, ""):
                fy_end = str(fy_end).strip().replace("/", "-")
                out_buffer_dict["CurrentFiscalYearEndDateDEI"] = fy_end

                parts = fy_end.split("-")
                if len(parts) >= 2:
                    out_buffer_dict["CurrentFiscalYearEndDateDEIyear"] = parts[0]
                    out_buffer_dict["CurrentFiscalYearEndDateDEImonth"] = parts[1]

            period_end = x1.get("CurrentPeriodEndDateDEI")
            if period_end not in (None, ""):
                out_buffer_dict["CurrentPeriodEndDateDEI"] = str(period_end).strip().replace("/", "-")

            fy_start = x1.get("CurrentFiscalYearStartDateDEI")
            if fy_start not in (None, ""):
                out_buffer_dict["CurrentFiscalYearStartDateDEI"] = str(fy_start).strip().replace("/", "-")

        if not use_half:
            out_buffer_dict = {
                k: v for k, v in out_buffer_dict.items()
                if not k.endswith("Quarter")
            }

        display_unit = "百万円"

        if parse_cache is not None and x1 is not None:
            file1_list = xbrl_file_paths.get("file1") or []
            if file1_list:
                _doc1 = parse_cache.get_or_create(
                    file1_list[0],
                    parser_func=lambda p: parse_xbrl_file_raw(
                        p,
                        mode="half" if use_half else "full",
                        logger=logger,
                    ),
                )
                if _doc1.document_display_unit in ("百万円", "千円"):
                    display_unit = _doc1.document_display_unit
        elif x1 is not None and x1.get("DocumentDisplayUnit") in ("百万円", "千円"):
            display_unit = x1["DocumentDisplayUnit"]

        if display_unit == "百万円":
            if parse_cache is not None and x2 is not None:
                file2_list = xbrl_file_paths.get("file2") or []
                if file2_list:
                    _doc2 = parse_cache.get_or_create(
                        file2_list[0],
                        parser_func=lambda p: parse_xbrl_file_raw(
                            p,
                            mode="full",
                            logger=logger,
                        ),
                    )
                    if _doc2.document_display_unit in ("百万円", "千円"):
                        display_unit = _doc2.document_display_unit
            elif x2 is not None and x2.get("DocumentDisplayUnit") in ("百万円", "千円"):
                display_unit = x2["DocumentDisplayUnit"]
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

    stock_result = None
    stock_status = None


    try:
        if out_buffer_dict:
            t = perf_counter()

            write_data_to_workbook_namedranges(
                wb,
                out_buffer_dict,
                display_unit=display_unit,
            )

            loop_event["phases"]["excel_write"] = {"ok": True, "sec": round(perf_counter() - t, 3)}
            logger.info(
                f"[company excel] slot={loop.get('slot')} "
                f"code={company_code_from_job or security_code} "
                f"name={company_name_from_job} "
                f"ranges={len(out_buffer_dict)}"
            )
        else:
            loop_event["phases"]["excel_write"] = {"ok": True, "sec": 0.0}

        t = perf_counter()

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

        stock_code = f"{security_code}.T" if security_code else None

        if stock_code:

            t = perf_counter()

            stock_result = write_stock_data_to_workbook(
                wb,
                stock_code,
                date_pairs,
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

    period_end_date = _pick_period_end(x1, x2, meta2)
    final_security_code = security_code or company_code_from_job or ""
    final_company_name = _pick_company_name(x1, x2, meta2, company_name_from_job)

    if output_root:
        output_excel_dir = os.path.join(output_root, "excel")
        os.makedirs(output_excel_dir, exist_ok=True)

        base_name = f"{final_security_code}_{final_company_name}_{period_end_date}".strip("_")
        safe_name = "".join("_" if c in '\\/:*?"<>|' else c for c in base_name)
        final_excel_file_path = os.path.join(output_excel_dir, f"{safe_name}.xlsm")

        counter = 1
        while os.path.exists(final_excel_file_path):
            final_excel_file_path = os.path.join(output_excel_dir, f"{safe_name}_{counter}.xlsm")
            counter += 1

        os.replace(excel_file_path, final_excel_file_path)
        logger.info(f"Excelファイルが移動されました: {final_excel_file_path}")
    else:
        final_excel_file_path = rename_excel_file(
            excel_file_path,
            final_security_code,
            final_company_name,
            period_end_date,
            logger,
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