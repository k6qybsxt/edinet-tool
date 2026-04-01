import sqlite3


MIN_METRIC_COUNT = 50
REQUIRED_CURRENT_KEYS = [
    "NetSalesCurrent",
    "OperatingIncomeCurrent",
    "NetAssetsCurrent",
    "CashAndCashEquivalentsCurrent",
]


db_path = r"C:\Users\silve\EDINET_Pipeline\data\edinet_monitor\edinet_monitor.db"
conn = sqlite3.connect(db_path)
conn.row_factory = sqlite3.Row
cur = conn.cursor()

filings = cur.execute(
    """
    select
        f.doc_id,
        f.edinet_code,
        f.security_code,
        ifnull(nm.metric_count, 0) as metric_count,
        f.download_status,
        f.parse_status,
        f.submit_date,
        f.period_end
    from filings f
    inner join issuer_master im
        on f.edinet_code = im.edinet_code
    left join (
        select
            doc_id,
            count(*) as metric_count
        from normalized_metrics
        group by doc_id
    ) nm
        on f.doc_id = nm.doc_id
    where im.is_listed = 1
      and im.exchange = 'TSE'
    order by
        ifnull(nm.metric_count, 0) asc,
        f.submit_date asc,
        f.doc_id asc
    """
).fetchall()

print("=== low metric docs ===")

for filing in filings:
    doc_id = filing["doc_id"]
    metric_count = int(filing["metric_count"] or 0)

    metric_keys = {
        str(row["metric_key"])
        for row in cur.execute(
            """
            select metric_key
            from normalized_metrics
            where doc_id = ?
            """,
            (doc_id,),
        ).fetchall()
    }
    missing_required_keys = [
        key for key in REQUIRED_CURRENT_KEYS
        if key not in metric_keys
    ]

    if metric_count >= MIN_METRIC_COUNT and not missing_required_keys:
        continue

    print(
        (
            filing["doc_id"],
            filing["edinet_code"],
            filing["security_code"],
            metric_count,
            filing["download_status"],
            filing["parse_status"],
            filing["submit_date"],
            filing["period_end"],
            f"missing_required_keys={','.join(missing_required_keys)}",
        )
    )

conn.close()
