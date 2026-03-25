import shutil
import time
from pathlib import Path


def cleanup_empty_company_job_csv(output_root, logger):
    jobs_csv = Path(output_root) / "reports" / "company_jobs.csv"
    if not jobs_csv.exists():
        return

    try:
        lines = jobs_csv.read_text(encoding="utf-8-sig").splitlines()
        if len(lines) <= 1:
            jobs_csv.unlink(missing_ok=True)
            logger.info(f"[cleanup] removed empty company_jobs.csv: {jobs_csv}")
    except Exception:
        logger.exception(f"[cleanup error] company_jobs.csv cleanup failed: {jobs_csv}")


def cleanup_extracted_root(extracted_root, cleanup_retry_count, cleanup_retry_wait_sec, logger):
    extracted_root = Path(extracted_root)
    if not extracted_root.exists():
        return

    last_error = None

    for attempt in range(1, cleanup_retry_count + 1):
        try:
            shutil.rmtree(extracted_root)
            logger.info(f"[cleanup] removed extracted root: {extracted_root}")
            return
        except Exception as e:
            last_error = e
            logger.warning(
                "[cleanup retry] extracted root remove failed: attempt=%d/%d path=%s error=%s",
                attempt,
                cleanup_retry_count,
                extracted_root,
                e,
            )
            if attempt < cleanup_retry_count:
                time.sleep(cleanup_retry_wait_sec)

    logger.error(
        "[cleanup skipped] extracted root remains after retries: path=%s error=%s",
        extracted_root,
        last_error,
    )