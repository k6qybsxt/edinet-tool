import sqlite3

TARGET_DOC_IDS = [
    "S100XUUD",
    "S100XUQB",
    "S100XUPN",
    "S100XUPZ",
    "S100XUJT",
    "S100XURZ",
]

db_path = r"C:\Users\silve\EDINET_Pipeline\data\edinet_monitor\edinet_monitor.db"
conn = sqlite3.connect(db_path)
cur = conn.cursor()

for doc_id in TARGET_DOC_IDS:
    print(f"=== {doc_id} ===")

    print("[normalized_metrics]")
    for row in cur.execute(
        """
        select
            metric_key,
            period_end,
            source_tag,
            consolidation,
            value_num
        from normalized_metrics
        where doc_id = ?
        order by metric_key, period_end
        """,
        (doc_id,),
    ).fetchall():
        print(row)

    print("[raw_facts numeric candidates]")
    for row in cur.execute(
        """
        select
            context_ref,
            tag_name,
            period_type,
            period_end,
            instant_date,
            consolidation,
            value_text
        from raw_facts
        where doc_id = ?
          and value_text is not null
          and value_text <> ''
          and tag_name not like '%TextBlock%'
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
    ).fetchall():
        print(row)

    print()

conn.close()