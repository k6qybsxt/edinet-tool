import sqlite3

db_path = r"C:\Users\silve\EDINET_Pipeline\data\edinet_monitor\edinet_monitor.db"
conn = sqlite3.connect(db_path)
conn.row_factory = sqlite3.Row
cur = conn.cursor()

print("=== pending TSE filings ===")
rows = cur.execute("""
SELECT
    f.doc_id,
    f.edinet_code,
    f.security_code,
    f.submit_date,
    f.download_status,
    im.company_name,
    im.exchange,
    im.is_listed
FROM filings f
INNER JOIN issuer_master im
    ON f.edinet_code = im.edinet_code
WHERE f.download_status = 'pending'
  AND im.is_listed = 1
  AND im.exchange = 'TSE'
ORDER BY f.submit_date ASC, f.doc_id ASC
LIMIT 20
""").fetchall()

for row in rows:
    print(tuple(row))

print(f"pending_count={len(rows)}")

conn.close()