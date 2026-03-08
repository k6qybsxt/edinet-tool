import glob
import os


def build_loops(base_dir, template_dir, max_n=50, logger=None):
    loops = []
    for n in range(1, max_n + 1):
        half_files = glob.glob(os.path.join(base_dir, f"{n}-2*.xbrl"))
        annual_1 = glob.glob(os.path.join(base_dir, f"{n}-4*.xbrl"))
        annual_2 = glob.glob(os.path.join(base_dir, f"{n}-5*.xbrl"))
        annual_3 = glob.glob(os.path.join(base_dir, f"{n}-6*.xbrl"))

        if half_files:
            # 上期あり
            file1 = half_files
            file2 = annual_1
            file3 = annual_2
        else:
            # 上期なし
            file1 = annual_1
            file2 = annual_2
            file3 = annual_3

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