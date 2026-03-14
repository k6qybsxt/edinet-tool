import os
from edinet_tool.services.file_indexer import build_xbrl_file_index


def build_loops(base_dir, template_dir, max_n=50, logger=None):
    loops = []
    xbrl_index = build_xbrl_file_index(base_dir, max_n=max_n, logger=logger)

    for n in range(1, max_n + 1):
        slot_files = xbrl_index[n]

        half_files = slot_files["2"]
        annual_1 = slot_files["4"]
        annual_2 = slot_files["5"]
        annual_3 = slot_files["6"]

        if half_files:
            file1 = half_files
            file2 = annual_1
            file3 = annual_2
        else:
            file1 = annual_1
            file2 = annual_2
            file3 = annual_3

        if logger is not None and (file1 or file2 or file3):
            logger.debug(
                f"[loop detect] slot={n} "
                f"file1={len(file1)} "
                f"file2={len(file2)} "
                f"file3={len(file3)}"
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