import os

from edinet_tool.services.xbrl_parser import parse_xbrl_file
from edinet_tool.domain.skip import SkipCode, add_skip
from edinet_tool.domain.security_code import ensure_security_code
from edinet_tool.domain.filters import (
    filter_for_annual,
    filter_for_annual_old,
    filter_for_half,
)
from edinet_tool.domain.year_shift import (
    get_fy_end_year,
    shift_with_keep,
    shift_out_meta_by_yeargap,
)


def parse_half_doc(loop, xbrl_file_paths, excel_file_path, parsed_docs, skipped_files, loop_event, logger, perf_counter):
    x1 = None
    base_year = None
    use_half = bool(xbrl_file_paths.get("file1") and xbrl_file_paths["file1"])

    if use_half:
        try:
            t = perf_counter()

            path1 = xbrl_file_paths["file1"][0]
            x1, sc1, meta1 = parse_xbrl_file(path1, mode="half", logger=logger)

            base_year = get_fy_end_year(x1)

            parsed_docs.append({
                "doc_id": os.path.basename(path1),
                "doc_type": "half",
                "out": x1,
                "out_meta": meta1,
                "parsed_code": sc1,
            })

            logger.info(f"[parse bench] mode=half xbrl={os.path.basename(path1)} out={len(x1)} meta={len(meta1)} sec={round(perf_counter()-t,3)}")
            loop_event["phases"]["file1_parse"] = {"ok": True, "sec": round(perf_counter() - t, 3)}

        except Exception as e:
            loop_event["phases"]["file1_parse"] = {"ok": False, "sec": None}
            loop_event["errors"].append("file1_parse_error")

            add_skip(
                skipped_files,
                code=SkipCode.FILE1_PARSE_ERROR,
                phase="file1",
                loop=loop,
                excel=excel_file_path,
                xbrl=(xbrl_file_paths["file1"][0] if xbrl_file_paths.get("file1") else None),
                message="file1(半期) 解析エラー",
                exc=e
            )
            x1 = None
            use_half = False

    return x1, base_year, use_half


def parse_latest_annual_doc(loop, xbrl_file_paths, excel_file_path, parsed_docs, skipped_files, loop_event, x1, use_half, out_buffer, logger, perf_counter):
    x2 = None
    meta2 = None
    path2 = None
    security_code = None

    if xbrl_file_paths.get("file2") and xbrl_file_paths["file2"]:
        try:
            t = perf_counter()

            path2 = xbrl_file_paths["file2"][0]
            x2, parsed_security_code, meta2 = parse_xbrl_file(path2, mode="full", logger=logger)

            parsed_docs.append({
                "doc_id": os.path.basename(path2),
                "doc_type": "annual",
                "out": x2,
                "out_meta": meta2,
                "parsed_code": parsed_security_code,
            })

            logger.info(f"[parse bench] mode=full xbrl={os.path.basename(path2)} out={len(x2)} meta={len(meta2)} sec={round(perf_counter()-t,3)}")

            security_code = ensure_security_code(x2, parsed_security_code, x1)
            loop_event["phases"]["file2_parse"] = {"ok": True, "sec": round(perf_counter() - t, 3)}

            out2_write = filter_for_annual(x2, use_half=use_half)

            logger.warning(f"[buffer debug] file2_annual keys={sorted(list(out2_write.keys()))}")
            logger.warning(f"[buffer debug] file2_annual nonempty={sum(1 for v in out2_write.values() if v not in (None, ''))}")

            for k, v in out2_write.items():
                out_buffer.put(k, v, "file2_annual")

        except Exception as e:
            loop_event["phases"]["file2_parse"] = {"ok": False, "sec": None}
            loop_event["errors"].append("file2_error")

            add_skip(
                skipped_files,
                code=SkipCode.FILE2_ERROR,
                phase="file2",
                loop=loop,
                excel=excel_file_path,
                xbrl=path2,
                message="file2(最新有報) 解析/書込エラー",
                exc=e
            )
    else:
        add_skip(
            skipped_files,
            code=SkipCode.FILE2_NOT_FOUND,
            phase="file2",
            loop=loop,
            excel=excel_file_path,
            xbrl=None,
            message="file2(最新有報) が見つからない"
        )

    return x2, meta2, path2, security_code


def parse_old_annual_doc(loop, xbrl_file_paths, excel_file_path, parsed_docs, skipped_files, loop_event, x1, security_code, base_year, out_buffer, logger, perf_counter):
    if base_year is not None and xbrl_file_paths.get("file3") and xbrl_file_paths["file3"]:
        try:
            t = perf_counter()

            path3 = xbrl_file_paths["file3"][0]
            x3, sc3, meta3 = parse_xbrl_file(path3, mode="full", logger=logger)

            parsed_docs.append({
                "doc_id": os.path.basename(path3),
                "doc_type": "annual",
                "out": x3,
                "out_meta": meta3,
                "parsed_code": sc3,
            })

            y3 = get_fy_end_year(x3)
            company_code = security_code or ensure_security_code(x3, sc3, x1) or ""

            loop_event["phases"]["file3_parse"] = {"ok": True, "sec": round(perf_counter() - t, 3)}

            if y3 is None:
                add_skip(
                    skipped_files,
                    code=SkipCode.FILE3_YEAR_MISS,
                    phase="file3",
                    loop=loop,
                    excel=excel_file_path,
                    xbrl=path3,
                    message="file3 期末年が取れない"
                )
            else:
                year_gap = base_year - y3

                if not (1 <= year_gap <= 4):
                    add_skip(
                        skipped_files,
                        code=SkipCode.FILE3_YEAR_MISS,
                        phase="file3",
                        loop=loop,
                        excel=excel_file_path,
                        xbrl=path3,
                        message=f"file3 year_gap abnormal base={base_year} y3={y3} gap={year_gap}"
                    )
                else:
                    x3_shifted = shift_with_keep(x3, year_gap)
                    meta3_shifted = shift_out_meta_by_yeargap(meta3, year_gap)

                    parsed_docs[-1]["out"] = x3_shifted
                    parsed_docs[-1]["out_meta"] = meta3_shifted

                    out3_write = filter_for_annual_old(x3_shifted)

                    logger.warning(f"[buffer debug] file3_annual keys={sorted(list(out3_write.keys()))}")
                    for k, v in out3_write.items():
                        out_buffer.put(k, v, "file3_annual")

        except Exception as e:
            loop_event["phases"]["file3_parse"] = {"ok": False, "sec": None}
            loop_event["errors"].append("file3_error")

            add_skip(
                skipped_files,
                code=SkipCode.FILE3_ERROR,
                phase="file3",
                loop=loop,
                excel=excel_file_path,
                xbrl=(xbrl_file_paths["file3"][0] if xbrl_file_paths.get("file3") else None),
                message="file3(過去有報) 解析/書込エラー",
                exc=e
            )


def finalize_half_buffer(loop, xbrl_file_paths, excel_file_path, skipped_files, loop_event, use_half, x1, out_buffer, logger, perf_counter):
    if use_half and x1 is not None:
        try:
            t = perf_counter()

            out_half = filter_for_half(x1)

            logger.warning(f"[buffer debug] half_final keys={sorted(list(out_half.keys()))}")
            logger.warning(f"[buffer debug] half_final nonempty={sum(1 for v in out_half.values() if v not in (None, ''))}")

            for k, v in out_half.items():
                out_buffer.put(k, v, "half_final")

            loop_event["phases"]["half_buffer"] = {"ok": True, "sec": round(perf_counter() - t, 3)}
            logger.info(f"[half finalize bench] sec={round(perf_counter()-t,3)}")

        except Exception as e:
            loop_event["phases"]["half_buffer"] = {"ok": False, "sec": None}
            loop_event["errors"].append("half_buffer_error")

            add_skip(
                skipped_files,
                code=SkipCode.HALF_WRITE_ERROR,
                phase="half",
                loop=loop,
                excel=excel_file_path,
                xbrl=(xbrl_file_paths["file1"][0] if xbrl_file_paths.get("file1") else None),
                message="half(半期) 書込エラー",
                exc=e
            )