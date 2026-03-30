import sqlite3

db_path = r"C:\Users\silve\EDINET_Pipeline\data\edinet_monitor\edinet_monitor.db"
conn = sqlite3.connect(db_path)
cur = conn.cursor()

print("=== filings columns ===")
for row in cur.execute("PRAGMA table_info(filings)").fetchall():
    print(row)

print("\n=== filings sample ===")
for row in cur.execute("""
select
    doc_id,
    edinet_code,
    security_code,
    form_type,
    amendment_flag,
    doc_info_edit_status,
    legal_status
from filings
order by submit_date desc
limit 10
""").fetchall():
    print(row)

conn.close()