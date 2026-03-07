from edinet_tool.domain.raw_builder import (
    build_raw_rows_from_out,
    append_missing_annual_ytd_rows,
    attach_run_info,
)
from edinet_tool.domain.dedupe import (
    dedupe_raw_rows_keep_best,
    find_duplicate_template_keys,
)


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


def build_raw_rows_all_docs(parsed_docs, security_code, run_id, logger):
    raw_rows = []

    company_code_for_raw = security_code or ""
    if not company_code_for_raw:
        for d in parsed_docs:
            pc = d.get("parsed_code")
            if pc:
                company_code_for_raw = pc
                break
    company_code_for_raw = company_code_for_raw or ""

    for d in parsed_docs:
        raw_rows.extend(
            build_raw_rows_from_out(
                company_code=company_code_for_raw,
                doc_id=d["doc_id"],
                doc_type=d["doc_type"],
                out=d["out"],
                out_meta=d["out_meta"],
            )
        )

    for d in parsed_docs:
        if d["doc_type"] == "annual":
            append_missing_annual_ytd_rows(
                raw_rows,
                company_code=company_code_for_raw,
                doc_id=d["doc_id"],
                out_meta=d["out_meta"],
                duration_metric_keys=DURATION_METRIC_KEYS,
            )

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