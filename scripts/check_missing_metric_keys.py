import sqlite3

TARGET_DOC_IDS = [
    "S100XUUD",
    "S100XUQB",
    "S100XUPN",
    "S100XUPZ",
    "S100XUJT",
    "S100XURZ",
    "S100XV2A",
    "S100XUQ2",
    "S100XUP3",
    "S100XV4F",
    "S100XV5Y",
    "S100XUXC",
    "S100XUSL",
    "S100XUXF",
    "S100XUP0",
    "S100XUQY",
    "S100XUHX",
    "S100XU04",
    "S100XV0N",
    "S100XTNP",
    "S100XUPT",
    "S100XV4H",
    "S100XV4M",
    "S100XURW",
]

EXPECTED_KEYS = [
    "NetSalesCurrent",
    "NetSalesPrior1",
    "NetSalesPrior2",
    "NetSalesPrior3",
    "NetSalesPrior4",
    "CostOfSalesCurrent",
    "CostOfSalesPrior1",
    "CostOfSalesPrior2",
    "CostOfSalesPrior3",
    "CostOfSalesPrior4",
    "SellingExpensesCurrent",
    "SellingExpensesPrior1",
    "SellingExpensesPrior2",
    "SellingExpensesPrior3",
    "SellingExpensesPrior4",
    "OperatingIncomeCurrent",
    "OperatingIncomePrior1",
    "OperatingIncomePrior2",
    "OperatingIncomePrior3",
    "OperatingIncomePrior4",
    "OrdinaryIncomeCurrent",
    "OrdinaryIncomePrior1",
    "OrdinaryIncomePrior2",
    "OrdinaryIncomePrior3",
    "OrdinaryIncomePrior4",
    "ProfitLossCurrent",
    "ProfitLossPrior1",
    "ProfitLossPrior2",
    "ProfitLossPrior3",
    "ProfitLossPrior4",
    "TotalAssetsCurrent",
    "TotalAssetsPrior1",
    "TotalAssetsPrior2",
    "TotalAssetsPrior3",
    "TotalAssetsPrior4",
    "NetAssetsCurrent",
    "NetAssetsPrior1",
    "NetAssetsPrior2",
    "NetAssetsPrior3",
    "NetAssetsPrior4",
    "CashAndCashEquivalentsCurrent",
    "CashAndCashEquivalentsPrior1",
    "CashAndCashEquivalentsPrior2",
    "CashAndCashEquivalentsPrior3",
    "CashAndCashEquivalentsPrior4",
    "OperatingCashCurrent",
    "OperatingCashPrior1",
    "OperatingCashPrior2",
    "OperatingCashPrior3",
    "OperatingCashPrior4",
    "InvestmentCashCurrent",
    "InvestmentCashPrior1",
    "InvestmentCashPrior2",
    "InvestmentCashPrior3",
    "InvestmentCashPrior4",
    "FinancingCashCurrent",
    "FinancingCashPrior1",
    "FinancingCashPrior2",
    "FinancingCashPrior3",
    "FinancingCashPrior4",
    "TreasurySharesCurrent",
]

db_path = r"C:\Users\silve\EDINET_Pipeline\data\edinet_monitor\edinet_monitor.db"
conn = sqlite3.connect(db_path)
cur = conn.cursor()

for doc_id in TARGET_DOC_IDS:
    rows = cur.execute(
        """
        select metric_key
        from normalized_metrics
        where doc_id = ?
        order by metric_key
        """,
        (doc_id,),
    ).fetchall()

    actual_keys = {row[0] for row in rows}
    missing_keys = [key for key in EXPECTED_KEYS if key not in actual_keys]

    print(f"=== {doc_id} ===")
    print(f"actual_count={len(actual_keys)}")
    print(f"missing_count={len(missing_keys)}")

    if missing_keys:
        for key in missing_keys:
            print(f"  MISSING: {key}")

    print()

conn.close()