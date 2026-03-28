import csv
import json
import os
from pathlib import Path

from edinet_pipeline.config.settings import BASE_DIR


def write_loop_summary(loop_event, security_code, raw_rows, out_buffer_dict, skipped_files, loop, t0, perf_counter, logger):
    loop_event["security_code"] = security_code
    loop_event["counts"]["raw_rows"] = len(raw_rows)
    loop_event["counts"]["excel_ranges"] = len(out_buffer_dict)
    loop_event["counts"]["skipped_in_loop"] = sum(
        1 for s in skipped_files if s.get("slot") == loop.get("slot")
    )

    loop_event["phases"]["loop_total"] = {
        "ok": True,
        "sec": round(perf_counter() - t0, 3)
    }

    jsonl_path = str(BASE_DIR / "logs" / "loop_summary.jsonl")
    os.makedirs(os.path.dirname(jsonl_path), exist_ok=True)

    with open(jsonl_path, "a", encoding="utf-8") as f:
        f.write(json.dumps(loop_event, ensure_ascii=False) + "\n")

    logger.info(
        f"[loop summary] slot={loop.get('slot')} "
        f"code={security_code} "
        f"excel_ranges={loop_event['counts']['excel_ranges']} "
        f"raw_rows={loop_event['counts']['raw_rows']} "
        f"sec={loop_event['phases']['loop_total']['sec']}"
    )


def _build_result_name_map(batch_results):
    result_name_map = {}
    for row in batch_results:
        code = row.get("company_code")
        name = row.get("company_name")
        if code and name:
            result_name_map[code] = name
    return result_name_map


def _write_company_jobs_csv(jobs_csv, job_inputs, result_name_map):
    with open(jobs_csv, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "slot",
                "company_code",
                "company_name",
                "has_half",
                "file1",
                "file2",
                "file3",
            ],
        )
        writer.writeheader()

        for job in job_inputs:
            writer.writerow({
                "slot": job["slot"],
                "company_code": job["company_code"],
                "company_name": result_name_map.get(job["company_code"], job.get("company_name") or ""),
                "has_half": job["has_half"],
                "file1": os.path.basename(job["file1"]) if job.get("file1") else "",
                "file2": os.path.basename(job["file2"]) if job.get("file2") else "",
                "file3": os.path.basename(job["file3"]) if job.get("file3") else "",
            })


def _write_batch_summary_csv(summary_csv, batch_results):
    with open(summary_csv, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "slot",
                "company_code",
                "company_name",
                "status",
                "stock_status",
                "failure_reason",
                "output_excel",
            ],
        )
        writer.writeheader()

        for row in batch_results:
            writer.writerow({
                "slot": row.get("slot"),
                "company_code": row.get("company_code"),
                "company_name": row.get("company_name"),
                "status": row.get("status"),
                "stock_status": row.get("stock_status"),
                "failure_reason": row.get("failure_reason"),
                "output_excel": row.get("output_excel"),
            })


def _write_failed_jobs_csv(failed_csv, batch_results):
    with open(failed_csv, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "slot",
                "company_code",
                "company_name",
                "status",
                "failure_reason",
                "error_detail",
                "output_excel",
            ]
        )
        writer.writeheader()

        for row in batch_results:
            if row.get("status") in ("failed", "partial_success", "skipped"):
                writer.writerow({
                    "slot": row.get("slot"),
                    "company_code": row.get("company_code"),
                    "company_name": row.get("company_name"),
                    "status": row.get("status"),
                    "failure_reason": row.get("failure_reason"),
                    "error_detail": row.get("error_detail"),
                    "output_excel": row.get("output_excel"),
                })


def write_batch_reports(output_root, job_inputs, batch_results, logger):
    reports_dir = Path(output_root) / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)

    summary_csv = reports_dir / "batch_summary.csv"
    failed_csv = reports_dir / "failed_jobs.csv"
    jobs_csv = reports_dir / "company_jobs.csv"

    result_name_map = _build_result_name_map(batch_results)

    _write_company_jobs_csv(
        jobs_csv=jobs_csv,
        job_inputs=job_inputs,
        result_name_map=result_name_map,
    )
    _write_batch_summary_csv(
        summary_csv=summary_csv,
        batch_results=batch_results,
    )
    _write_failed_jobs_csv(
        failed_csv=failed_csv,
        batch_results=batch_results,
    )

    logger.info(f"[batch summary] total={len(batch_results)} summary_csv={summary_csv}")
    logger.info(f"[batch summary] failed_csv={failed_csv}")
    logger.info(f"[batch summary] jobs_csv={jobs_csv}")

    return {
        "reports_dir": reports_dir,
        "summary_csv": summary_csv,
        "failed_csv": failed_csv,
        "jobs_csv": jobs_csv,
    }


def log_batch_result_summary(batch_results, logger):
    success_count = sum(1 for r in batch_results if r.get("status") == "success")
    partial_count = sum(1 for r in batch_results if r.get("status") == "partial_success")
    failed_count = sum(1 for r in batch_results if r.get("status") == "failed")
    skipped_count = sum(1 for r in batch_results if r.get("status") == "skipped")

    logger.info(
        "[batch result] success=%d partial=%d failed=%d skipped=%d total=%d",
        success_count,
        partial_count,
        failed_count,
        skipped_count,
        len(batch_results),
    )