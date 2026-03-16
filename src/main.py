import certifi
import os
import gc
import csv
import shutil
from datetime import datetime

os.environ["SSL_CERT_FILE"] = certifi.where()

from edinet_tool.config.settings import BASE_DIR, load_config, LOG_LEVEL
from edinet_tool.services.stock_service import validate_stock_date_pairs, clear_stock_price_cache
from edinet_tool.services.loop_processor import process_one_loop
from edinet_tool.services.parse_cache import XbrlParseCache
from edinet_tool.services.batch_input_service import build_all_company_jobs
from edinet_tool.domain.skip import log_skip_summary
from edinet_tool.logging_utils.logger import setup_logger
import time

logger = None

def main():

    clear_stock_price_cache()

    logger.info(f"プロジェクトルート: {BASE_DIR}")

    zip_dir = BASE_DIR / "data" / "input" / "zip"
    template_dir = BASE_DIR / "templates"
    output_root = BASE_DIR / "data" / "output" / datetime.now().strftime("%Y%m%d_%H%M%S")
    output_root.mkdir(parents=True, exist_ok=True)

    logger.info(f"ZIPフォルダ: {zip_dir}")
    logger.info(f"出力フォルダ: {output_root}")

    if not os.path.isdir(zip_dir):
        logger.critical(f"ZIPフォルダが存在しません: {zip_dir}")
        raise SystemExit(1)

    skipped_files = []
    batch_results = []

    parse_cache = XbrlParseCache(logger=logger)

    extracted_root = output_root / "_zip_extracted"
    extracted_root.mkdir(parents=True, exist_ok=True)

    jobs = build_all_company_jobs(zip_dir, extract_root=str(extracted_root))

    if not jobs:
        logger.warning("処理対象の会社が見つかりませんでした")
        return

    logger.info(f"[batch detect] companies={len(jobs)}")

    try:
        config = load_config(BASE_DIR / "config" / "決算期_KANPE.json")
        chosen_period = input("決算期を選択してください（例 25-1）: ")

        if chosen_period not in config:
            logger.critical("無効な選択です")
            raise SystemExit(1)

        date_pairs = config[chosen_period]
        validate_stock_date_pairs(date_pairs)
        logger.info(f"選択された決算期: {chosen_period}")

    except Exception:
        logger.exception("決算期設定エラー")
        raise SystemExit(1)

    for i, job in enumerate(jobs):

        try:
            loop = {
                "slot": i + 1,
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

            logger.info(
                f"[company start] code={job['company_code']} "
                f"half={job['has_half']}"
            )

            result = process_one_loop(
                loop,
                date_pairs,
                skipped_files,
                logger,
                parse_cache=parse_cache,
            )

            if isinstance(result, dict):
                batch_results.append(result)
            else:
                batch_results.append({
                    "slot": i + 1,
                    "company_code": job["company_code"],
                    "company_name": job["company_name"],
                    "status": "success",
                    "output_excel": loop.get("final_excel_file_path"),
                })

        except SystemExit:
            raise

        except Exception:
            logger.exception(f"[company error] code={job['company_code']}")
            batch_results.append({
                "slot": i + 1,
                "company_code": job["company_code"],
                "company_name": job["company_name"],
                "status": "failed",
                "failure_reason": "company_exception",
                "output_excel": None,
            })

    reports_dir = output_root / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)

    summary_csv = reports_dir / "batch_summary.csv"
    failed_csv = reports_dir / "failed_jobs.csv"
    jobs_csv = reports_dir / "company_jobs.csv"

    result_name_map = {}
    for row in batch_results:
        code = row.get("company_code")
        name = row.get("company_name")
        if code and name:
            result_name_map[code] = name

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

        for i, job in enumerate(jobs):
            writer.writerow({
                "slot": i + 1,
                "company_code": job["company_code"],
                "company_name": result_name_map.get(job["company_code"], job.get("company_name") or ""),
                "has_half": job["has_half"],
                "file1": os.path.basename(job["file1"]) if job.get("file1") else "",
                "file2": os.path.basename(job["file2"]) if job.get("file2") else "",
                "file3": os.path.basename(job["file3"]) if job.get("file3") else "",
            })

    with open(summary_csv, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["slot", "company_code", "company_name", "status", "stock_status", "output_excel"]
        )
        writer.writeheader()
        for row in batch_results:
            writer.writerow({
                "slot": row.get("slot"),
                "company_code": row.get("company_code"),
                "company_name": row.get("company_name"),
                "status": row.get("status"),
                "stock_status": row.get("stock_status"),
                "output_excel": row.get("output_excel"),
            })

    with open(failed_csv, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "slot",
                "company_code",
                "company_name",
                "status",
                "failure_reason",
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
                    "output_excel": row.get("output_excel"),
                })

    logger.info(f"[batch summary] total={len(batch_results)} summary_csv={summary_csv}")
    logger.info(f"[batch summary] failed_csv={failed_csv}")

    success_count = sum(1 for r in batch_results if r.get("status") == "success")
    failed_count = sum(1 for r in batch_results if r.get("status") == "failed")

    logger.info(f"[batch result] success={success_count} failed={failed_count}")

    log_skip_summary(logger, skipped_files)

    try:
        if "parse_cache" in locals():
            parse_cache.clear()
            parse_cache = None
    except Exception:
        pass

    gc.collect()

    for _ in range(10):
        try:
            if extracted_root.exists():
                shutil.rmtree(extracted_root)
            break
        except PermissionError:
            time.sleep(1)
            gc.collect()

    # Windowsハンドル残り対策
    try:
        if extracted_root.exists():
            import subprocess
            subprocess.run(["cmd", "/c", "rmdir", "/s", "/q", str(extracted_root)], check=False)
    except Exception:
        pass

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