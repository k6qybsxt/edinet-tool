import os
import shutil

from edinet_tool.services.excel_service import find_available_excel_file


def prepare_workbook(loop, run_id, logger):

    excel_base_name = os.path.basename(loop["excel_file_path"]).replace(".xlsm", "")

    output_root = loop.get("output_root")
    work_dir = os.path.join(output_root, "work")
    os.makedirs(work_dir, exist_ok=True)

    template_file = loop["excel_file_path"]

    if not os.path.exists(template_file):
        logger.critical(f"テンプレートExcelが存在しません: {template_file}")
        raise SystemExit(1)

    work_excel_path = os.path.join(
        work_dir,
        f"{excel_base_name}_work_{run_id}.xlsm"
    )

    shutil.copy(template_file, work_excel_path)

    loop["excel_file_path"] = work_excel_path

    logger.info(f"使用するExcelファイル（作業）: {work_excel_path}")

    if "_work_" not in os.path.splitext(work_excel_path)[0]:
        logger.critical(
            f"安全ロック発動：workではないExcelに書き込もうとしました: {work_excel_path}"
        )
        raise SystemExit(1)

    return template_file, work_excel_path, excel_base_name