from dataclasses import asdict
import traceback
from concurrent.futures import ProcessPoolExecutor, as_completed

from edinet_pipeline.config.settings import LOG_LEVEL
from edinet_pipeline.services.company_task_result import CompanyTaskResult
from edinet_pipeline.services.company_runner import run_company_job
from edinet_pipeline.services.company_runner_worker import run_company_job_worker


def _log_company_start(job, logger):
    logger.info(
        f"[company start] code={job['company_code']} "
        f"half={job['has_half']}"
    )


def _build_failed_result(job, failure_reason, error_detail):
    return asdict(
        CompanyTaskResult(
            slot=job.get("slot", 0),
            company_code=job.get("company_code", ""),
            company_name=job.get("company_name"),
            status="failed",
            stock_status=None,
            output_excel=None,
            failure_reason=failure_reason,
            error_detail=error_detail,
        )
    )


def _normalize_result(job, result):
    if isinstance(result, CompanyTaskResult):
        return asdict(result)

    if isinstance(result, dict):
        return result

    return {
        "slot": job.get("slot", 0),
        "company_code": job.get("company_code", ""),
        "company_name": job.get("company_name"),
        "status": "success",
        "stock_status": None,
        "failure_reason": None,
        "error_detail": None,
        "output_excel": job.get("final_excel_file_path"),
    }


def _run_company_jobs_process_pool(job_inputs, date_pairs, output_root, template_dir, logger, runtime):
    batch_results = []
    future_to_job = {}

    with ProcessPoolExecutor(max_workers=runtime.max_workers) as executor:
        for job in job_inputs:
            _log_company_start(job, logger)

            future = executor.submit(
                run_company_job_worker,
                job=job,
                date_pairs=date_pairs,
                output_root=str(output_root),
                template_dir=str(template_dir),
                log_level=LOG_LEVEL,
            )
            future_to_job[future] = job

        for future in as_completed(future_to_job):
            job = future_to_job.get(future, {})

            try:
                result = _normalize_result(job, future.result())

                if result.get("error_detail"):
                    logger.error(
                        "[company error] slot=%s code=%s detail=\n%s",
                        result.get("slot"),
                        result.get("company_code"),
                        result.get("error_detail"),
                    )

                batch_results.append(result)

            except SystemExit:
                raise

            except Exception:
                logger.exception(
                    "[company error] worker_future_exception code=%s",
                    job.get("company_code"),
                )
                batch_results.append(
                    _build_failed_result(
                        job=job,
                        failure_reason="worker_future_exception",
                        error_detail=traceback.format_exc(),
                    )
                )

    return batch_results


def _run_company_jobs_serial(job_inputs, date_pairs, output_root, template_dir, skipped_files, logger, parse_cache):
    batch_results = []

    for job in job_inputs:
        try:
            _log_company_start(job, logger)

            result = run_company_job(
                job=job,
                date_pairs=date_pairs,
                output_root=output_root,
                template_dir=template_dir,
                skipped_files=skipped_files,
                logger=logger,
                parse_cache=parse_cache,
            )

            batch_results.append(_normalize_result(job, result))

        except SystemExit:
            raise

        except Exception:
            logger.exception("[company error] code=%s", job.get("company_code"))
            batch_results.append(
                _build_failed_result(
                    job=job,
                    failure_reason="company_exception",
                    error_detail=traceback.format_exc(),
                )
            )

    return batch_results


def run_company_jobs(job_inputs, date_pairs, output_root, template_dir, skipped_files, logger, parse_cache, runtime):
    if runtime.use_process_pool:
        batch_results = _run_company_jobs_process_pool(
            job_inputs=job_inputs,
            date_pairs=date_pairs,
            output_root=output_root,
            template_dir=template_dir,
            logger=logger,
            runtime=runtime,
        )
    else:
        batch_results = _run_company_jobs_serial(
            job_inputs=job_inputs,
            date_pairs=date_pairs,
            output_root=output_root,
            template_dir=template_dir,
            skipped_files=skipped_files,
            logger=logger,
            parse_cache=parse_cache,
        )

    batch_results.sort(key=lambda x: (x.get("slot") or 0, x.get("company_code") or ""))
    return batch_results