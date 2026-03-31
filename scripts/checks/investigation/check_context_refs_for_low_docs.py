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
    print(f"=== {doc_id} : context_ref summary ===")
    rows = cur.execute(
        """
        select
            context_ref,
            period_type,
            period_end,
            instant_date,
            consolidation,
            count(*) as row_count
        from raw_facts
        where doc_id = ?
        group by
            context_ref,
            period_type,
            period_end,
            instant_date,
            consolidation
        order by
            context_ref asc
        """,
        (doc_id,),
    ).fetchall()

    for row in rows:
        print(row)

    print()

conn.close()