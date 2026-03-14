import os

from edinet_tool.services.xbrl_parser import parse_xbrl_file
from edinet_tool.services.xbrl_parser import parse_xbrl_file_raw
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


def parse_half_doc(loop, xbrl_file_paths, excel_file_path, parsed_docs, skipped_files, loop_event, logger, perf_counter, parse_cache=None):
    x1 = None
    base_year = None

    path1 = None
    if xbrl_file_paths.get("file1") and xbrl_file_paths["file1"]:
        path1 = xbrl_file_paths["file1"][0]

    is_half_doc = bool(path1 and os.path.basename(path1).startswith(f"{loop.get('slot')}-2"))

    if path1:
        try:
            t = perf_counter()

            if is_half_doc:
                if parse_cache is not None:
                    _cache_doc = parse_cache.get_or_create(
                        path1,
                        parser_func=lambda p: parse_xbrl_file_raw(p, mode="half", logger=logger),
                    )
                    x1, sc1, meta1 = _cache_doc.out, _cache_doc.security_code, _cache_doc.out_meta
                else:
                    x1, sc1, meta1 = parse_xbrl_file(path1, mode="half", logger=logger)

                doc_type = "half"
                phase_name = "file1_parse"
            else:
                if parse_cache is not None:
                    _cache_doc = parse_cache.get_or_create(
                        path1,
                        parser_func=lambda p: parse_xbrl_file_raw(p, mode="full", logger=logger),
                    )
                    x1, sc1, meta1 = _cache_doc.out, _cache_doc.security_code, _cache_doc.out_meta
                else:
                    x1, sc1, meta1 = parse_xbrl_file(path1, mode="full", logger=logger)

                doc_type = "annual"
                phase_name = "file1_parse"

            base_year = get_fy_end_year(x1)

            parsed_docs.append({
                "doc_id": os.path.basename(path1),
                "doc_type": doc_type,
                "out": x1,
                "out_meta": meta1,
                "parsed_code": sc1,
                "facts": (_cache_doc.facts if parse_cache is not None else []),
                "contexts": (_cache_doc.contexts if parse_cache is not None else {}),
                "units": (_cache_doc.units if parse_cache is not None else {}),
                "nsmap": (_cache_doc.nsmap if parse_cache is not None else {}),
                "dei_data": (_cache_doc.dei_data if parse_cache is not None else {}),
                "accounting_standard": (_cache_doc.accounting_standard if parse_cache is not None else "jpgaap"),
                "document_display_unit": (_cache_doc.document_display_unit if parse_cache is not None else None),
            })

            logger.info(
                f"[parse bench] mode={'half' if is_half_doc else 'full'} xbrl={os.path.basename(path1)} "
                f"out={len(x1)} meta={len(meta1)} sec={round(perf_counter()-t,3)}"
            )
            loop_event["phases"][phase_name] = {"ok": True, "sec": round(perf_counter() - t, 3)}

            if parse_cache is not None:
                loop_event["accounting_standard"] = _cache_doc.accounting_standard

        except Exception as e:
            loop_event["phases"]["file1_parse"] = {"ok": False, "sec": None}
            loop_event["errors"].append("file1_parse_error")

            add_skip(
                skipped_files,
                code=SkipCode.FILE1_PARSE_ERROR,
                phase="file1",
                loop=loop,
                excel=excel_file_path,
                xbrl=path1,
                message="file1 解析エラー",
                exc=e
            )
            x1 = None
            base_year = None

    use_half = is_half_doc

    return x1, base_year, use_half

def parse_latest_annual_doc(loop, xbrl_file_paths, excel_file_path, parsed_docs, skipped_files, loop_event, x1, use_half, base_year, out_buffer, logger, perf_counter, parse_cache=None):
    x2 = None
    meta2 = None
    path2 = None
    security_code = None

    if xbrl_file_paths.get("file2") and xbrl_file_paths["file2"]:
        try:
            t = perf_counter()

            path2 = xbrl_file_paths["file2"][0]

            if parse_cache is not None:
                _cache_doc = parse_cache.get_or_create(
                    path2,
                    parser_func=lambda p: parse_xbrl_file_raw(p, mode="full", logger=logger),
                )
                x2, parsed_security_code, meta2 = _cache_doc.out, _cache_doc.security_code, _cache_doc.out_meta
            else:
                x2, parsed_security_code, meta2 = parse_xbrl_file(path2, mode="full", logger=logger)

            parsed_docs.append({
                "doc_id": os.path.basename(path2),
                "doc_type": "annual",
                "out": x2,
                "out_meta": meta2,
                "parsed_code": parsed_security_code,
                "facts": (_cache_doc.facts if parse_cache is not None else []),
                "contexts": (_cache_doc.contexts if parse_cache is not None else {}),
                "units": (_cache_doc.units if parse_cache is not None else {}),
                "nsmap": (_cache_doc.nsmap if parse_cache is not None else {}),
                "dei_data": (_cache_doc.dei_data if parse_cache is not None else {}),
                "accounting_standard": (_cache_doc.accounting_standard if parse_cache is not None else "jpgaap"),
                "document_display_unit": (_cache_doc.document_display_unit if parse_cache is not None else None),
            })

            logger.info(f"[parse bench] mode=full xbrl={os.path.basename(path2)} out={len(x2)} meta={len(meta2)} sec={round(perf_counter()-t,3)}")

            security_code = ensure_security_code(x2, parsed_security_code, x1)
            loop_event["accounting_standard"] = _cache_doc.accounting_standard if parse_cache is not None else "jpgaap"

            if base_year is None:
                base_year = get_fy_end_year(x2)

            loop_event["phases"]["file2_parse"] = {"ok": True, "sec": round(perf_counter() - t, 3)}

            if use_half:
                out2_write = filter_for_annual(x2, use_half=True)
            else:
                y2 = get_fy_end_year(x2)
                if y2 is None or base_year is None:
                    out2_write = filter_for_annual(x2, use_half=False)
                else:
                    year_gap2 = base_year - y2
                    if year_gap2 >= 1:
                        x2_shifted = shift_with_keep(x2, year_gap2)
                        out2_write = filter_for_annual_old(x2_shifted)
                    else:
                        out2_write = filter_for_annual(x2, use_half=False)

                # 上期なしの TotalNumber は専用配置
                for kk in list(out2_write.keys()):
                    if kk.startswith("TotalNumber"):
                        del out2_write[kk]

                if x2.get("TotalNumberCurrent") not in (None, ""):
                    out2_write["TotalNumberPrior2"] = x2["TotalNumberCurrent"]

            logger.debug(f"[buffer debug] file2_annual keys={sorted(list(out2_write.keys()))}")
            logger.debug(f"[buffer debug] file2_annual nonempty={sum(1 for v in out2_write.values() if v not in (None, ''))}")

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

    return x2, meta2, path2, security_code, base_year


def parse_old_annual_doc(loop, xbrl_file_paths, excel_file_path, parsed_docs, skipped_files, loop_event, x1, security_code, base_year, out_buffer, logger, perf_counter, parse_cache=None):
    if base_year is not None and xbrl_file_paths.get("file3") and xbrl_file_paths["file3"]:
        try:
            t = perf_counter()

            path3 = xbrl_file_paths["file3"][0]

            if parse_cache is not None:
                _cache_doc = parse_cache.get_or_create(
                    path3,
                    parser_func=lambda p: parse_xbrl_file_raw(p, mode="full", logger=logger),
                )
                x3, sc3, meta3 = _cache_doc.out, _cache_doc.security_code, _cache_doc.out_meta
            else:
                x3, sc3, meta3 = parse_xbrl_file(path3, mode="full", logger=logger)

            parsed_docs.append({
                "doc_id": os.path.basename(path3),
                "doc_type": "annual",
                "out": x3,
                "out_meta": meta3,
                "parsed_code": sc3,
                "facts": (_cache_doc.facts if parse_cache is not None else []),
                "contexts": (_cache_doc.contexts if parse_cache is not None else {}),
                "units": (_cache_doc.units if parse_cache is not None else {}),
                "nsmap": (_cache_doc.nsmap if parse_cache is not None else {}),
                "dei_data": (_cache_doc.dei_data if parse_cache is not None else {}),
                "accounting_standard": (_cache_doc.accounting_standard if parse_cache is not None else "jpgaap"),
                "document_display_unit": (_cache_doc.document_display_unit if parse_cache is not None else None),
            })

            y3 = get_fy_end_year(x3)
            company_code = security_code or ensure_security_code(x3, sc3, x1) or ""

            loop_event["phases"]["file3_parse"] = {"ok": True, "sec": round(perf_counter() - t, 3)}

            if parse_cache is not None:
                loop_event["accounting_standard"] = _cache_doc.accounting_standard

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

                    logger.debug(f"[buffer debug] file3_annual keys={sorted(list(out3_write.keys()))}")

                    skipped_overlap = 0
                    for k, v in out3_write.items():
                        if out_buffer.has(k):
                            skipped_overlap += 1
                            continue
                        out_buffer.put(k, v, "file3_annual")

                    logger.info(f"[buffer optimize] file3 skipped overlaps={skipped_overlap}")

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

            logger.debug(f"[buffer debug] half_final keys={sorted(list(out_half.keys()))}")
            logger.debug(f"[buffer debug] half_final nonempty={sum(1 for v in out_half.values() if v not in (None, ''))}")

            removed_before_put = 0
            for k in list(out_half.keys()):
                if out_buffer.has(k):
                    out_buffer.pop(k)
                    removed_before_put += 1

            for k, v in out_half.items():
                out_buffer.put(k, v, "half_final")

            logger.info(f"[buffer optimize] half_final removed_before_put={removed_before_put}")

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