import sqlite3

db_path = r"C:\Users\silve\EDINET_Pipeline\data\edinet_monitor\edinet_monitor.db"
conn = sqlite3.connect(db_path)
cur = conn.cursor()

target_doc_ids = [
    row[0]
    for row in cur.execute("""
        SELECT f.doc_id
        FROM filings f
        LEFT JOIN issuer_master im
            ON f.edinet_code = im.edinet_code
        WHERE NOT (
            im.is_listed = 1
            AND im.exchange = 'TSE'
        )
    """).fetchall()
]

print("delete_target_count=", len(target_doc_ids))

for doc_id in target_doc_ids:
    cur.execute("DELETE FROM normalized_metrics WHERE doc_id = ?", (doc_id,))
    cur.execute("DELETE FROM raw_facts WHERE doc_id = ?", (doc_id,))
    cur.execute("DELETE FROM filings WHERE doc_id = ?", (doc_id,))

conn.commit()

print("remaining_tse_filings_count=", cur.execute("""
    SELECT count(*)
    FROM filings f
    INNER JOIN issuer_master im
        ON f.edinet_code = im.edinet_code
    WHERE im.is_listed = 1
      AND im.exchange = 'TSE'
""").fetchone()[0])

print("remaining_all_filings_count=", cur.execute("""
    SELECT count(*)
    FROM filings
""").fetchone()[0])

conn.close()