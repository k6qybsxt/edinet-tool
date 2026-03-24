from dataclasses import asdict
import traceback
from concurrent.futures import ProcessPoolExecutor, as_completed

from edinet_tool.config.settings import LOG_LEVEL
from edinet_tool.services.company_task_result import CompanyTaskResult
from edinet_tool.services.company_runner import run_company_job
from edinet_tool.services.company_runner_worker import run_company_job_worker


def run_company_jobs(job_inputs, date_pairs, output_root, template_dir, skipped_files, logger, parse_cache, runtime):
    batch_results = []

    if runtime.use_process_pool:
        future_to_job = {}

        with ProcessPoolExecutor(max_workers=runtime.max_workers) as executor:
            for job in job_inputs:
                logger.info(
                    f"[company start] code={job['company_code']} "
                    f"half={job['has_half']}"
                )

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
                try:
                    result = future.result()

                    if isinstance(result, dict) and result.get("error_detail"):
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
                    job = future_to_job.get(future, {})
                    logger.exception(
                        "[company error] worker_future_exception code=%s",
                        job.get("company_code"),
                    )
                    batch_results.append(asdict(
                        CompanyTaskResult(
                            slot=job.get("slot", 0),
                            company_code=job.get("company_code", ""),
                            company_name=job.get("company_name"),
                            status="failed",
                            stock_status=None,
                            output_excel=None,
                            failure_reason="worker_future_exception",
                            error_detail=traceback.format_exc(),
                        )
                    ))
    else:
        for job in job_inputs:
            try:
                logger.info(
                    f"[company start] code={job['company_code']} "
                    f"half={job['has_half']}"
                )

                result = run_company_job(
                    job=job,
                    date_pairs=date_pairs,
                    output_root=output_root,
                    template_dir=template_dir,
                    skipped_files=skipped_files,
                    logger=logger,
                    parse_cache=parse_cache,
                )

                if isinstance(result, CompanyTaskResult):
                    batch_results.append(asdict(result))
                else:
                    batch_results.append({
                        "slot": job["slot"],
                        "company_code": job["company_code"],
                        "company_name": job["company_name"],
                        "status": "success",
                        "stock_status": None,
                        "failure_reason": None,
                        "error_detail": None,
                        "output_excel": job.get("final_excel_file_path"),
                    })

            except SystemExit:
                raise

            except Exception:
                logger.exception(f"[company error] code={job['company_code']}")
                batch_results.append(asdict(
                    CompanyTaskResult(
                        slot=job["slot"],
                        company_code=job["company_code"],
                        company_name=job["company_name"],
                        status="failed",
                        stock_status=None,
                        output_excel=None,
                        failure_reason="company_exception",
                        error_detail=traceback.format_exc(),
                    )
                ))

    batch_results.sort(key=lambda x: (x.get("slot") or 0, x.get("company_code") or ""))
    return batch_results