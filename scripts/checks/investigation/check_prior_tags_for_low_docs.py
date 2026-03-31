import sqlite3

TARGET_DOC_IDS = [
    "S100XV2A",
    "S100XUQ2",
    "S100XUP3",
    "S100XV4F",
]

db_path = r"C:\Users\silve\EDINET_Pipeline\data\edinet_monitor\edinet_monitor.db"
conn = sqlite3.connect(db_path)
cur = conn.cursor()

for doc_id in TARGET_DOC_IDS:
    print(f"=== {doc_id} ===")

    rows = cur.execute(
        """
        select
            context_ref,
            tag_name,
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
        print(row)

    print()

conn.close()