from __future__ import annotations

from pathlib import Path
from typing import Any

from edinet_pipeline.config.settings import TEMPLATE_WORKBOOK_NAME
from edinet_pipeline.services.company_task_result import CompanyTaskResult
from edinet_pipeline.services.loop_processor import process_one_loop
from edinet_pipeline.services.loop_types import LoopInput


def build_loop_input(job: dict[str, Any], *, output_root: Path, template_dir: Path) -> LoopInput:
    return {
        "slot": job.get("slot"),
        "company_code": job.get("company_code"),
        "company_name": job.get("company_name"),
        "has_half": job.get("has_half"),
        "source_zips": list(job.get("source_zips") or []),
        "output_root": str(output_root),
        "xbrl_file_paths": {
            "file1": [job["file1"]] if job.get("file1") else [],
            "file2": [job["file2"]] if job.get("file2") else [],
            "file3": [job["file3"]] if job.get("file3") else [],
        },
        "excel_file_path": str(template_dir / TEMPLATE_WORKBOOK_NAME),
    }


def run_company_job(
    job: dict[str, Any],
    date_pairs,
    output_root,
    template_dir,
    skipped_files,
    logger,
    parse_cache,
    runtime,
) -> CompanyTaskResult:
    template_dir = Path(template_dir)
    output_root = Path(output_root)

    loop = build_loop_input(
        job,
        output_root=output_root,
        template_dir=template_dir,
    )

    result = process_one_loop(
        loop,
        date_pairs,
        skipped_files,
        logger,
        parse_cache=parse_cache,
        runtime=runtime,
    )

    return CompanyTaskResult(
        slot=result.get("slot"),
        company_code=result.get("company_code"),
        company_name=result.get("company_name"),
        status=result.get("status"),
        stock_status=result.get("stock_status"),
        output_excel=result.get("output_excel"),
        failure_reason=result.get("failure_reason"),
        error_detail=result.get("error_detail"),
    )
