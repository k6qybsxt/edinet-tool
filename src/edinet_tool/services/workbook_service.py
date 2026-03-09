import os
import shutil

from edinet_tool.services.excel_service import find_available_excel_file


def prepare_workbook(loop, run_id, logger):
    excel_base_name = os.path.basename(loop["excel_file_path"]).replace(".xlsm", "")
    excel_directory = os.path.dirname(loop["excel_file_path"])

    selected_file = find_available_excel_file(excel_directory, excel_base_name, logger)
    if not selected_file:
        return None, None, excel_base_name

    logger.info(f"使用するExcelファイル（元）: {selected_file}")

    base_no_ext, ext = os.path.splitext(selected_file)
    work_excel_path = f"{base_no_ext}_work_{run_id}{ext}"
    shutil.copy(selected_file, work_excel_path)

    loop["excel_file_path"] = work_excel_path
    excel_file_path = work_excel_path

    logger.info(f"使用するExcelファイル（作業）: {excel_file_path}")

    if "_work_" not in os.path.splitext(excel_file_path)[0]:
        logger.critical(f"安全ロック発動：workではないExcelに書き込もうとしました: {excel_file_path}")
        raise SystemExit(1)

    return selected_file, excel_file_path, excel_base_name