from edinet_tool.domain.raw_builder import (
    build_raw_rows_from_out,
    append_missing_annual_ytd_rows,
    attach_run_info,
)
from edinet_tool.domain.dedupe import (
    dedupe_raw_rows_keep_best,
    find_duplicate_template_keys,
    raw_key_for_template,
)
from edinet_tool.domain.tag_alias import normalize_tag_to_metric, allowed_raw_fact_tags

DURATION_METRIC_KEYS = [
    "NetSales",
    "CostOfSales",
    "GrossProfit",
    "SellingExpenses",
    "OperatingIncome",
    "OrdinaryIncome",
    "ProfitLoss",
    "OperatingCash",
    "InvestmentCash",
    "FinancingCash",
]

ALLOWED_RAW_FACT_TAGS = allowed_raw_fact_tags()

def build_raw_rows_all_docs(parsed_docs, security_code, run_id, logger):
    raw_rows = []
    seen_template_keys = set()
    
    company_code_for_raw = security_code or ""
    if not company_code_for_raw:
        for d in parsed_docs:
            pc = d.get("parsed_code")
            if pc:
                company_code_for_raw = pc
                break
    company_code_for_raw = company_code_for_raw or ""

    for d in parsed_docs:
        if d.get("facts") is not None:
            doc_rows = []
            metric_counter = {}
            

            for f in d["facts"]:
                tag = f.get("tag")
                if tag not in ALLOWED_RAW_FACT_TAGS:
                    continue

                metric_key = normalize_tag_to_metric(tag)
                if not metric_key:
                    continue

                if metric_key == "ProfitLoss" and f.get("members"):
                    continue

                if metric_key in ("IssuedShares", "TreasuryShares") and f.get("members"):
                    continue

                if metric_key in ("IssuedShares", "TreasuryShares") and f.get("period_kind") != "instant":
                    continue

                if f.get("value") in (None, ""):
                    continue

                if not f.get("period_kind"):
                    continue

                period_kind = f.get("period_kind")
                period_end = f.get("end_date") if period_kind == "duration" else f.get("instant_date")

                row = {
                    "company_code": company_code_for_raw,
                    "doc_id": d["doc_id"],
                    "doc_type": d["doc_type"],
                    "consolidation": "Consolidated" if f.get("is_consolidated") else "NonConsolidated",
                    "metric_key": metric_key,
                    "time_slot": "YTD" if period_kind == "duration" else "Quarter",
                    "period_start": f.get("start_date"),
                    "period_end": period_end,
                    "period_kind": period_kind,
                    "value": f.get("value"),
                    "unit": f.get("unit_ref"),
                    "tag_used": tag,
                    "tag_rank": 0,
                    "status": "parsed",
                }
                doc_rows.append(row)
                metric_counter[metric_key] = metric_counter.get(metric_key, 0) + 1

            if metric_counter:
                logger.info(
                    "[raw metric map] doc=%s metrics=%s",
                    d["doc_id"],
                    dict(sorted(metric_counter.items()))
                )

        else:
            doc_rows = build_raw_rows_from_out(
            company_code=company_code_for_raw,
            doc_id=d["doc_id"],
            doc_type=d["doc_type"],
            out=d["out"],
            out_meta=d["out_meta"],
        )

        skipped_doc_overlap = 0
        for row in doc_rows:
            k = raw_key_for_template(row)
            if k in seen_template_keys:
                skipped_doc_overlap += 1
                continue

            seen_template_keys.add(k)
            raw_rows.append(row)

        if skipped_doc_overlap:
            logger.info(
                "[raw optimize] skipped_doc_overlap doc=%s count=%d",
                d["doc_id"],
                skipped_doc_overlap,
            )

    for d in parsed_docs:
        if d["doc_type"] == "annual":
            before_n = len(raw_rows)

            append_missing_annual_ytd_rows(
                raw_rows,
                company_code=company_code_for_raw,
                doc_id=d["doc_id"],
                out_meta=d["out_meta"],
                duration_metric_keys=DURATION_METRIC_KEYS,
            )

            if len(raw_rows) > before_n:
                for row in raw_rows[before_n:]:
                    seen_template_keys.add(raw_key_for_template(row))

    dup_items = find_duplicate_template_keys(raw_rows)
    if dup_items:
        logger.warning("[raw dup still] groups=%d (show top 10)", len(dup_items))
        for k, v in dup_items[:10]:
            logger.warning(" dup_key=%s count=%d", k, v)

    raw_rows, deduped = dedupe_raw_rows_keep_best(raw_rows)
    if deduped:
        logger.warning("[raw dedupe] removed_duplicates=%d final_rows=%d", deduped, len(raw_rows))

    attach_run_info(raw_rows, run_id)
    return raw_rows