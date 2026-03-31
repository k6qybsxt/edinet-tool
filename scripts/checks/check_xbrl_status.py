import sqlite3

db_path = r"C:\Users\silve\EDINET_Pipeline\data\edinet_monitor\edinet_monitor.db"
conn = sqlite3.connect(db_path)
cur = conn.cursor()

print("=== filings xbrl status ===")
for row in cur.execute("""
select
    f.doc_id,
    f.edinet_code,
    f.security_code,
    f.download_status,
    f.parse_status,
    f.zip_path,
    f.xbrl_path
from filings f
inner join issuer_master im
    on f.edinet_code = im.edinet_code
where im.is_listed = 1
  and im.exchange = 'TSE'
order by f.submit_date asc, f.doc_id asc
limit 20
""").fetchall():
    print(row)

conn.close()