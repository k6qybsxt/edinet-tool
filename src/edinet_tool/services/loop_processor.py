from edinet_tool.services.excel_service import (
    write_data_to_excel_namedranges,
    write_rows_to_raw_sheet,
)
from edinet_tool.services.stock_write_service import write_stock_if_possible
from edinet_tool.services.workbook_service import prepare_workbook
from edinet_tool.services.parse_service import (
    parse_half_doc,
    parse_latest_annual_doc,
    parse_old_annual_doc,
    finalize_half_buffer,
)
from edinet_tool.services.summary_service import write_loop_summary
from edinet_tool.services.raw_service import build_raw_rows_all_docs

from edinet_tool.domain.skip import SkipCode, add_skip
from edinet_tool.domain.raw_builder import RAW_COLS
from edinet_tool.domain.output_buffer import OutputBuffer

import os
from datetime import datetime
from time import perf_counter

# XBRLデータの取得、証券コードの取得、Excelへの書き込み、株価データ取得までをループ処理に含める
def process_one_loop(loop, date_pairs, skipped_files, logger):
    
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
    )

    # -------------------------
    # 1) file2（最新有報）
    # -------------------------
    x2, meta2, path2, security_code = parse_latest_annual_doc(
        loop=loop,
        xbrl_file_paths=xbrl_file_paths,
        excel_file_path=excel_file_path,
        parsed_docs=parsed_docs,
        skipped_files=skipped_files,
        loop_event=loop_event,
        x1=x1,
        use_half=use_half,
        out_buffer=out_buffer,
        logger=logger,
        perf_counter=perf_counter,
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

    # === ANCHOR: BEFORE_EXCEL_WRITE ===
    collisions = out_buffer.collisions()
    if collisions:
        logger.warning("[excel buffer] collisions=%d", len(collisions))
        for k, old_src, new_src in collisions[:50]:
            winner = out_buffer.winner_of(k)
            logger.warning(" overwrite: %s  %s -> %s (winner=%s)", k, old_src, new_src, winner)
    if out_buffer:
        t = perf_counter()

        out_buffer_dict = out_buffer.to_dict()
        write_data_to_excel_namedranges(excel_file_path, out_buffer_dict)

        loop_event["phases"]["excel_write"] = {"ok": True, "sec": round(perf_counter() - t, 3)}
        logger.info(f"[excel write] ranges={len(out_buffer_dict)}")
    else:
        out_buffer_dict = {}
        loop_event["phases"]["excel_write"] = {"ok": True, "sec": 0.0}

    raw_rows = build_raw_rows_all_docs(
    parsed_docs=parsed_docs,
    security_code=security_code,
    run_id=run_id,
    logger=logger,
)

    # === ANCHOR: BEFORE_RAW_WRITE ===
    write_rows_to_raw_sheet(excel_file_path, raw_rows, RAW_COLS, sheet_name="raw_edinet")
    logger.info(f"[raw] written rows={len(raw_rows)} sheet=raw_edinet")

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

    write_stock_if_possible(
        excel_file_path=excel_file_path,
        security_code=security_code,
        date_pairs=date_pairs,
        skipped_files=skipped_files,
        loop=loop,
        logger=logger,
    )