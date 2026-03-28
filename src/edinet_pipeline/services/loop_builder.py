import os
import re
from pathlib import Path
from edinet_pipeline.services.zip_loader import collect_xbrl_from_zip


def _extract_end_date(path: str):
    name = Path(path).name
    m = re.search(r"_(\d{4}-\d{2}-\d{2})_\d{2}_\d{4}-\d{2}-\d{2}\.xbrl$", name)
    return m.group(1) if m else ""


def _is_half_xbrl(path: str):
    name = Path(path).name.lower()
    return "jpcrp040300" in name


def _is_annual_xbrl(path: str):
    name = Path(path).name.lower()
    return "jpcrp030000-asr" in name


def build_loops(base_dir, template_dir, max_n=50, logger=None):

    loops = []

    extract_dir = str(Path(base_dir).parent / "_zip_extracted")
    xbrl_paths = collect_xbrl_from_zip(base_dir, extract_dir)

    if logger:
        logger.info(f"[zip loader] xbrl found={len(xbrl_paths)}")

    half_paths = [p for p in xbrl_paths if _is_half_xbrl(p)]
    annual_paths = [p for p in xbrl_paths if _is_annual_xbrl(p)]

    # 新しい順に並べる
    half_paths = sorted(half_paths, key=_extract_end_date, reverse=True)
    annual_paths = sorted(annual_paths, key=_extract_end_date, reverse=True)

    for n in range(1, max_n + 1):

        file1 = []
        file2 = []
        file3 = []

        if half_paths:
            if len(half_paths) >= n:
                file1 = [half_paths[n - 1]]

            annual_start = (n - 1) * 2

            if len(annual_paths) >= annual_start + 1:
                file2 = [annual_paths[annual_start]]

            if len(annual_paths) >= annual_start + 2:
                file3 = [annual_paths[annual_start + 1]]

        else:
            annual_start = (n - 1) * 3

            if len(annual_paths) >= annual_start + 1:
                file1 = [annual_paths[annual_start]]

            if len(annual_paths) >= annual_start + 2:
                file2 = [annual_paths[annual_start + 1]]

            if len(annual_paths) >= annual_start + 3:
                file3 = [annual_paths[annual_start + 2]]

        if logger and (file1 or file2 or file3):
            logger.debug(
                f"[loop detect] slot={n} "
                f"file1={len(file1)} "
                f"file2={len(file2)} "
                f"file3={len(file3)}"
            )
            logger.debug(
                f"[loop files] slot={n} "
                f"file1={file1} "
                f"file2={file2} "
                f"file3={file3}"
            )

        excel_file_path = os.path.join(template_dir, f"決算分析シート_{n}.xlsm")

        loops.append({
            "xbrl_file_paths": {
                "file1": file1,
                "file2": file2,
                "file3": file3,
            },
            "excel_file_path": excel_file_path,
            "slot": n,
        })

    return loops