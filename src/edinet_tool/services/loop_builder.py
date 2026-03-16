import os
from edinet_tool.services.zip_loader import collect_xbrl_from_zip


def build_loops(base_dir, template_dir, max_n=50, logger=None):

    loops = []

    # ZIPからXBRL取得
    xbrl_entries = collect_xbrl_from_zip(base_dir)

    if logger:
        logger.info(f"[zip loader] xbrl found={len(xbrl_entries)}")

    # 簡易的に3ファイルずつ処理
    for n in range(1, max_n + 1):

        start = (n - 1) * 3
        end = start + 3

        slot_entries = xbrl_entries[start:end]

        file1 = []
        file2 = []
        file3 = []

        if len(slot_entries) >= 1:
            file1 = [slot_entries[0]]

        if len(slot_entries) >= 2:
            file2 = [slot_entries[1]]

        if len(slot_entries) >= 3:
            file3 = [slot_entries[2]]

        if logger and (file1 or file2 or file3):
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