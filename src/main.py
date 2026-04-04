import certifi
import os
from datetime import datetime

os.environ["SSL_CERT_FILE"] = certifi.where()

from edinet_pipeline.config.runtime import RuntimeConfig
from edinet_pipeline.config.settings import BASE_DIR, LOG_LEVEL
from edinet_pipeline.domain.run_checks import validate_runtime_before_batch
from edinet_pipeline.domain.skip import log_skip_summary
from edinet_pipeline.logging_utils.logger import setup_logger
from edinet_pipeline.services.batch_input_service import build_all_company_jobs, finalize_company_jobs
from edinet_pipeline.services.cleanup_service import cleanup_empty_company_job_csv, cleanup_extracted_root
from edinet_pipeline.services.company_execution_service import run_company_jobs
from edinet_pipeline.services.main_setup_service import (
    create_main_parse_cache,
    get_main_zip_dir,
    prepare_main_paths,
    validate_main_template_contract,
)
from edinet_pipeline.services.summary_service import (
    log_batch_result_summary,
    write_batch_reports,
)

logger = None


def _clear_main_parse_cache(runtime, parse_cache):
    if runtime.use_process_pool:
        return None

    if parse_cache is None:
        return None

    try:
        parse_cache.clear()
    except Exception:
        pass

    return None


def main():
    runtime = RuntimeConfig()

    logger.info("project root: %s", BASE_DIR)

    zip_dir = get_main_zip_dir()
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    setup_paths = prepare_main_paths(timestamp=timestamp)

    output_root = setup_paths["output_root"]
    extracted_root = setup_paths["extracted_root"]
    template_dir = setup_paths["template_dir"]

    validate_main_template_contract(
        template_dir=template_dir,
        logger=logger,
        include_stock_ranges=runtime.enable_stock,
    )

    logger.info("zip input dir: %s", zip_dir)
    logger.info("output root: %s", output_root)

    jobs = build_all_company_jobs(
        zip_dir,
        extract_root=str(extracted_root),
    )
    job_inputs = finalize_company_jobs(
        jobs=jobs,
        max_companies=runtime.max_companies,
    )

    if not job_inputs:
        logger.warning("no company jobs were detected")
        return

    validate_runtime_before_batch(job_inputs, runtime)

    date_pairs = None
    skipped_files = []
    parse_cache = create_main_parse_cache(
        logger=logger,
        runtime=runtime,
    )

    logger.info("starting financial analysis batch")
    logger.info("[batch detect] companies=%s", len(job_inputs))

    batch_results = run_company_jobs(
        job_inputs=job_inputs,
        date_pairs=date_pairs,
        output_root=output_root,
        template_dir=template_dir,
        skipped_files=skipped_files,
        logger=logger,
        parse_cache=parse_cache,
        runtime=runtime,
    )

    write_batch_reports(
        output_root=output_root,
        job_inputs=job_inputs,
        batch_results=batch_results,
        logger=logger,
        runtime=runtime,
    )

    cleanup_empty_company_job_csv(output_root=output_root, logger=logger)

    log_batch_result_summary(batch_results=batch_results, logger=logger)

    if (not runtime.use_process_pool) and parse_cache is not None:
        logger.info("[parse cache] stats=%s", parse_cache.stats())

    log_skip_summary(logger, skipped_files)

    parse_cache = _clear_main_parse_cache(runtime=runtime, parse_cache=parse_cache)

    cleanup_extracted_root(
        extracted_root=extracted_root,
        cleanup_retry_count=runtime.cleanup_retry_count,
        cleanup_retry_wait_sec=runtime.cleanup_retry_wait_sec,
        logger=logger,
    )


if __name__ == "__main__":
    logger = setup_logger(log_level=LOG_LEVEL)
    try:
        logger.info("===== program start =====")
        logger.info("[log config] level=%s", LOG_LEVEL)
        main()
        logger.info("===== program end =====")
    except SystemExit:
        raise
    except Exception:
        logger.exception("fatal error during program execution")
        raise
