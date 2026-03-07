import json
import os

from edinet_tool.config.settings import BASE_DIR


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