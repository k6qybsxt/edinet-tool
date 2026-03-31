import sqlite3

from edinet_monitor.services.normalizer.metric_normalize_service import normalize_raw_fact_row

TARGET_DOC_IDS = [
    "S100XV2A",
    "S100XUQ2",
]

db_path = r"C:\Users\silve\EDINET_Pipeline\data\edinet_monitor\edinet_monitor.db"
conn = sqlite3.connect(db_path)
conn.row_factory = sqlite3.Row
cur = conn.cursor()

for doc_id in TARGET_DOC_IDS:
    print(f"=== {doc_id} ===")

    filing = cur.execute(
        """
        select doc_id, edinet_code, security_code
        from filings
        where doc_id = ?
        """,
        (doc_id,),
    ).fetchone()

    rows = cur.execute(
        """
        select
            doc_id,
            tag_name,
            context_ref,
            unit_ref,
            period_type,
            period_start,
            period_end,
            instant_date,
            consolidation,
            value_text
        from raw_facts
        where doc_id = ?
          and (
                context_ref like 'Prior1YearDuration%'
             or context_ref like 'Prior2YearDuration%'
             or context_ref like 'Prior3YearDuration%'
             or context_ref like 'Prior4YearDuration%'
             or context_ref like 'Prior1YearInstant%'
             or context_ref like 'Prior2YearInstant%'
             or context_ref like 'Prior3YearInstant%'
             or context_ref like 'Prior4YearInstant%'
          )
          and (
                tag_name like '%NetSales%'
             or tag_name like '%Revenue%'
             or tag_name like '%CostOfSales%'
             or tag_name like '%SellingGeneralAndAdministrativeExpenses%'
             or tag_name like '%OperatingIncome%'
             or tag_name like '%OperatingProfit%'
             or tag_name like '%OrdinaryIncome%'
             or tag_name like '%ProfitLoss%'
             or tag_name like '%TotalAssets%'
             or tag_name like '%NetAssets%'
             or tag_name like '%EquityAttributableToOwnersOfParent%'
             or tag_name like '%CashAndCashEquivalents%'
             or tag_name like '%NetCashProvidedByUsedInOperatingActivities%'
             or tag_name like '%CashFlowsFromUsedInOperatingActivities%'
             or tag_name like '%NetCashProvidedByUsedInInvestingActivities%'
             or tag_name like '%CashFlowsFromUsedInInvestingActivities%'
             or tag_name like '%NetCashProvidedByUsedInFinancingActivities%'
             or tag_name like '%CashFlowsFromUsedInFinancingActivities%'
             or tag_name like '%TotalNumberOfSharesHeldTreasurySharesEtc%'
          )
        order by
            context_ref asc,
            tag_name asc,
            consolidation asc
        """,
        (doc_id,),
    ).fetchall()

    for row in rows:
        row_dict = dict(row)
        normalized = normalize_raw_fact_row(
            row_dict,
            edinet_code=filing["edinet_code"],
            security_code=filing["security_code"],
        )

        if normalized is None:
            print(
                "DROP",
                row_dict["context_ref"],
                row_dict["tag_name"],
                row_dict["consolidation"],
                row_dict["value_text"],
            )
        else:
            print(
                "KEEP",
                row_dict["context_ref"],
                row_dict["tag_name"],
                row_dict["consolidation"],
                row_dict["value_text"],
                "=>",
                normalized["metric_key"],
                normalized["period_end"],
            )

    print()

conn.close()