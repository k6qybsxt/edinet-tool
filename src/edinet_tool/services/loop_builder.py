import glob
import os


def build_loops(base_dir, template_dir, max_n=50, logger=None):
    loops = []
    for n in range(1, max_n + 1):
        file1 = glob.glob(os.path.join(base_dir, f"{n}-2*.xbrl"))
        file2 = glob.glob(os.path.join(base_dir, f"{n}-4*.xbrl"))
        file3 = glob.glob(os.path.join(base_dir, f"{n}-5*.xbrl"))

        if logger is not None:
            logger.debug(f"[{n}] file1: {file1}")
            logger.debug(f"[{n}] file2: {file2}")
            logger.debug(f"[{n}] file3: {file3}")

        excel_file_path = os.path.join(template_dir, f"決算分析シート_{n}.xlsx")

        loops.append({
            "xbrl_file_paths": {"file1": file1, "file2": file2, "file3": file3},
            "excel_file_path": excel_file_path,
            "slot": n,
        })

    return loops