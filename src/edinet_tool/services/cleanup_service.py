from pathlib import Path
import gc
import shutil
import time
import subprocess


def cleanup_empty_company_job_csv(output_root, logger):
    reports_dir = Path(output_root) / "reports"
    company_jobs_csv = reports_dir / "company_jobs.csv"

    if not company_jobs_csv.exists():
        return

    try:
        text = company_jobs_csv.read_text(encoding="utf-8-sig")
    except Exception:
        text = company_jobs_csv.read_text(encoding="utf-8")

    lines = [line for line in text.splitlines() if line.strip()]
    if len(lines) <= 1:
        company_jobs_csv.unlink(missing_ok=True)
        logger.info(f"[cleanup] removed empty csv: {company_jobs_csv}")


def cleanup_extracted_root(extracted_root, cleanup_retry_count, cleanup_retry_wait_sec, logger):
    extracted_root = Path(extracted_root)

    gc.collect()

    for _ in range(cleanup_retry_count):
        try:
            if extracted_root.exists():
                shutil.rmtree(extracted_root)
                logger.info(f"[cleanup] removed extracted_root: {extracted_root}")
            break
        except PermissionError:
            time.sleep(cleanup_retry_wait_sec)
            gc.collect()

    try:
        if extracted_root.exists():
            subprocess.run(
                ["cmd", "/c", "rmdir", "/s", "/q", str(extracted_root)],
                check=False,
            )
            if not extracted_root.exists():
                logger.info(f"[cleanup] removed extracted_root by cmd: {extracted_root}")
    except Exception:
        pass