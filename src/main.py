import certifi
import os
from datetime import datetime

os.environ["SSL_CERT_FILE"] = certifi.where()

from edinet_tool.config.settings import BASE_DIR, LOG_LEVEL
from edinet_tool.config.runtime import RuntimeConfig
from edinet_tool.services.batch_input_service import build_all_company_jobs, finalize_company_jobs
from edinet_tool.domain.skip import log_skip_summary
from edinet_tool.services.summary_service import write_batch_reports, log_batch_result_summary
from edinet_tool.services.cleanup_service import cleanup_empty_company_job_csv, cleanup_extracted_root
from edinet_tool.services.company_execution_service import run_company_jobs
from edinet_tool.services.main_setup_service import get_main_zip_dir, prepare_main_paths, create_main_parse_cache
from edinet_tool.domain.run_checks import validate_runtime_before_batch
from edinet_tool.logging_utils.logger import setup_logger

logger = None

def main():

    runtime = RuntimeConfig()

    logger.info(f"プロジェクトルート: {BASE_DIR}")

    zip_dir = get_main_zip_dir()
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    setup_paths = prepare_main_paths(
        runtime=runtime,
        timestamp=timestamp,
    )
    output_root = setup_paths["output_root"]
    extracted_root = setup_paths["extracted_root"]
    template_dir = setup_paths["template_dir"]

    logger.info(f"ZIPフォルダ: {zip_dir}")
    logger.info(f"出力フォルダ: {output_root}")

    jobs = build_all_company_jobs(
        zip_dir,
        extract_root=str(extracted_root),
    )

    job_inputs = finalize_company_jobs(
        jobs=jobs,
        max_companies=runtime.max_companies,
    )

    if not job_inputs:
        logger.warning("処理対象の会社が見つかりませんでした")
        return

    validate_runtime_before_batch(job_inputs, runtime)

    date_pairs = None
    logger.info("決算期の手入力は行いません")
    logger.info(f"[batch detect] companies={len(job_inputs)}")

    skipped_files = []
    parse_cache = create_main_parse_cache(logger=logger)

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

    _ = write_batch_reports(
        output_root=output_root,
        job_inputs=job_inputs,
        batch_results=batch_results,
        logger=logger,
    )
    
    cleanup_empty_company_job_csv(output_root=output_root, logger=logger)

    log_batch_result_summary(batch_results=batch_results, logger=logger)

    if (not runtime.use_process_pool) and parse_cache is not None:
        logger.info(f"[parse cache] stats={parse_cache.stats()}")
    
    log_skip_summary(logger, skipped_files)

    try:
        if (not runtime.use_process_pool) and "parse_cache" in locals() and parse_cache is not None:
            parse_cache.clear()
            parse_cache = None
    except Exception:
        pass

    cleanup_extracted_root(
        extracted_root=extracted_root,
        cleanup_retry_count=runtime.cleanup_retry_count,
        cleanup_retry_wait_sec=runtime.cleanup_retry_wait_sec,
        logger=logger,
    )

if __name__ == "__main__":
    logger = setup_logger(log_level=LOG_LEVEL)
    try:
        logger.info("===== プログラム開始 =====")
        logger.info(f"[log config] level={LOG_LEVEL}")
        main()
        logger.info("===== 正常終了 =====")
    except SystemExit:
        raise
    except Exception:
        logger.exception("致命的エラーで終了しました")
        raise