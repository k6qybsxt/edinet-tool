from dataclasses import asdict
import traceback
from edinet_tool.logging_utils.logger import setup_logger
from edinet_tool.services.company_runner import run_company_job
from edinet_tool.services.parse_cache import XbrlParseCache
from edinet_tool.services.company_task_result import CompanyTaskResult


def run_company_job_worker(
    job,
    date_pairs,
    output_root,
    template_dir,
    log_level,
):
    logger = setup_logger(log_level=log_level)
    skipped_files = []
    parse_cache = XbrlParseCache(logger=logger, max_items=8)

    try:
        result = run_company_job(
            job=job,
            date_pairs=date_pairs,
            output_root=output_root,
            template_dir=template_dir,
            skipped_files=skipped_files,
            logger=logger,
            parse_cache=parse_cache,
        )
        return asdict(result)

    except Exception:
        return asdict(
            CompanyTaskResult(
                slot=job.get("slot", 0),
                company_code=job.get("company_code", ""),
                company_name=job.get("company_name"),
                status="failed",
                stock_status=None,
                output_excel=None,
                failure_reason="worker_exception",
                error_detail=traceback.format_exc(),
            )
        )