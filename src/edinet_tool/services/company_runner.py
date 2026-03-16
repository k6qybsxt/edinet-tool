from pathlib import Path
from edinet_tool.services.loop_processor import process_one_loop
from edinet_tool.services.company_task_result import CompanyTaskResult


def run_company_job(job, date_pairs, output_root, template_dir, skipped_files, logger, parse_cache):
    template_dir = Path(template_dir)
    output_root = Path(output_root)

    loop = {
        "slot": job["slot"],
        "company_code": job["company_code"],
        "company_name": job["company_name"],
        "has_half": job["has_half"],
        "source_zips": job["source_zips"],
        "output_root": str(output_root),
        "xbrl_file_paths": {
            "file1": [job["file1"]] if job["file1"] else [],
            "file2": [job["file2"]] if job["file2"] else [],
            "file3": [job["file3"]] if job["file3"] else [],
        },
        "excel_file_path": str(template_dir / "決算分析シート_1.xlsm"),
    }

    result = process_one_loop(
        loop,
        date_pairs,
        skipped_files,
        logger,
        parse_cache=parse_cache,
    )

    return CompanyTaskResult(
        slot=result.get("slot"),
        company_code=result.get("company_code"),
        company_name=result.get("company_name"),
        status=result.get("status"),
        stock_status=result.get("stock_status"),
        output_excel=result.get("output_excel"),
        failure_reason=result.get("failure_reason"),
    )