import os
import shutil
from time import perf_counter


def prepare_workbook(loop, run_id, logger):
    excel_base_name = os.path.basename(loop["excel_file_path"]).replace(".xlsm", "")

    output_root = loop.get("output_root")
    if output_root:
        work_dir = os.path.join(output_root, "work")
    else:
        work_dir = os.path.dirname(loop["excel_file_path"])

    os.makedirs(work_dir, exist_ok=True)

    template_file = loop["excel_file_path"]

    if not os.path.exists(template_file):
        logger.critical("template workbook was not found: %s", template_file)
        raise SystemExit(1)

    work_excel_path = os.path.join(
        work_dir,
        f"{excel_base_name}_work_{run_id}.xlsm"
    )

    t = perf_counter()
    shutil.copy(template_file, work_excel_path)
    copy_sec = round(perf_counter() - t, 3)

    loop["excel_file_path"] = work_excel_path

    try:
        template_size = os.path.getsize(template_file)
    except Exception:
        template_size = None

    try:
        work_size = os.path.getsize(work_excel_path)
    except Exception:
        work_size = None

    logger.info("created workbook copy: %s", work_excel_path)
    logger.info(
        "[workbook copy] slot=%s template=%s sec=%s template_size=%s work_size=%s",
        loop.get("slot"),
        os.path.basename(template_file),
        copy_sec,
        template_size,
        work_size,
    )

    if "_work_" not in os.path.splitext(work_excel_path)[0]:
        logger.critical("workbook safety check failed: %s", work_excel_path)
        raise SystemExit(1)

    return template_file, work_excel_path, excel_base_name
