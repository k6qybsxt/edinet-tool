from edinet_tool.services.excel_service import (
    write_data_to_workbook_namedranges,
    write_rows_to_raw_sheet_workbook,
)
from edinet_tool.services.stock_service import write_stock_data_to_workbook
import openpyxl
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
import gc
from datetime import datetime
from time import perf_counter

# XBRLデータの取得、証券コードの取得、Excelへの書き込み、株価データ取得までをループ処理に含める
def process_one_loop(loop, date_pairs, skipped_files, logger, parse_cache=None):
    
    # === ANCHOR: LOOP_START === 
    parsed_docs = []   # ★ここに file1/2/3 の parse結果を溜める（raw書込は最後に1回）
   
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S_%f")

    t0 = perf_counter()

    loop_event = {
        "ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "run_id": run_id,
        "slot": loop.get("slot"),
        "excel": os.path.basename(excel_file_path) if "excel_file_path" in locals() else None,
        "security_code": None,
        "phases": {},        # {"file1_parse": {"ok": True, "sec": 0.12}, ...}
        "counts": {},        # {"raw_rows": 123, "excel_ranges": 45, ...}
        "errors": [],        # 例外など（ここに短い文字列で）
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
        logger.warning("使用するファイルが見つかりませんでした。次のループを実行します。")
        return

    loop_event["excel"] = os.path.basename(excel_file_path)

    xbrl_file_paths = loop["xbrl_file_paths"]
    logger.debug(f"[loop debug] xbrl_file_paths={xbrl_file_paths}")

    security_code = None
    base_year = None

    # -------------------------
    # 0) file1（半期）があれば先に読む（base_year決定）
    # -------------------------
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

        # 上期なしを明示
        out1_write["UseHalfModeFlag"] = 0

        # file1 の通常指標は Current / Prior1 だけ使う
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

        # TotalNumber は専用配置
        for kk in list(out1_write.keys()):
            if kk.startswith("TotalNumber"):
                del out1_write[kk]

        if x1.get("TotalNumberCurrent") not in (None, ""):
            out1_write["TotalNumberCurrent"] = x1["TotalNumberCurrent"]

        logger.debug(f"[buffer debug] file1_annual keys={sorted(list(out1_write.keys()))}")
        logger.debug(f"[buffer debug] file1_annual nonempty={sum(1 for v in out1_write.values() if v not in (None, ''))}")
        logger.debug(f"[buffer debug] file1 TotalNumberCurrent={out1_write.get('TotalNumberCurrent')}")

        for k, v in out1_write.items():
            if v in (None, ""):
                continue
            out_buffer.put(k, v, "file1_annual")

    # -------------------------
    # 1) file2（最新有報）
    # -------------------------
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

    # -------------------------
    # 2) file3（過去有報）→ Prior補完
    # -------------------------
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

    # -------------------------
    # 3) 半期ありなら最後にYTD/Quarter確定
    # -------------------------
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

    logger.info(f"[buffer optimize] final_keys={len(out_buffer.to_dict())}")

    # === ANCHOR: BEFORE_EXCEL_WRITE ===
    collisions = out_buffer.collisions()
    if collisions:
        logger.warning("[excel buffer] collisions=%d", len(collisions))
        logger.debug("[excel buffer] collision details start")
        for k, old_src, new_src in collisions[:50]:
            winner = out_buffer.winner_of(k)
            logger.debug(" overwrite: %s  %s -> %s (winner=%s)", k, old_src, new_src, winner)

    if out_buffer:
        out_buffer_dict = out_buffer.to_dict()

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

        logger.info(f"[excel display unit] {display_unit}")
    else:
        out_buffer_dict = {}
        display_unit = "百万円"

    raw_rows = build_raw_rows_all_docs(
        parsed_docs=parsed_docs,
        security_code=security_code,
        run_id=run_id,
        logger=logger,
    )

    wb = None

    wb = openpyxl.load_workbook(
        excel_file_path,
        keep_vba=excel_file_path.lower().endswith(".xlsm")
    )

    try:
        if out_buffer_dict:
            t = perf_counter()

            write_data_to_workbook_namedranges(
                wb,
                out_buffer_dict,
                display_unit=display_unit,
            )

            loop_event["phases"]["excel_write"] = {"ok": True, "sec": round(perf_counter() - t, 3)}
            logger.info(f"[excel write] ranges={len(out_buffer_dict)}")
        else:
            loop_event["phases"]["excel_write"] = {"ok": True, "sec": 0.0}

        write_rows_to_raw_sheet_workbook(
            wb,
            raw_rows,
            RAW_COLS,
            sheet_name="raw_edinet",
        )
        logger.info(f"[raw] written rows={len(raw_rows)} sheet=raw_edinet")

        stock_code = f"{security_code}.T" if security_code else None
        logger.debug(f"[stock check] security_code={security_code} stock_code={stock_code}")

        if stock_code:
            write_stock_data_to_workbook(
                wb,
                stock_code,
                date_pairs,
                logger,
            )

        wb.save(excel_file_path)

    finally:
        if wb is not None:
            wb.close()
            wb = None

    gc.collect()

    write_loop_summary(
        loop_event=loop_event,
        security_code=security_code,
        raw_rows=raw_rows,
        out_buffer_dict=out_buffer_dict,
        skipped_files=skipped_files,
        loop=loop,
        t0=t0,
        perf_counter=perf_counter,
        logger=logger,
    )