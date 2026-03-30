import sqlite3

db_path = r"C:\Users\silve\EDINET_Pipeline\data\edinet_monitor\edinet_monitor.db"
conn = sqlite3.connect(db_path)
cur = conn.cursor()

print("=== filings joined issuer_master ===")
for row in cur.execute("""
select
    f.doc_id,
    f.edinet_code,
    f.security_code,
    f.submit_date,
    im.company_name,
    im.exchange,
    im.is_listed
from filings f
inner join issuer_master im
    on f.edinet_code = im.edinet_code
order by f.submit_date asc, f.doc_id asc
limit 50
""").fetchall():
    print(row)

print("\n=== filings count joined issuer_master ===")
print(cur.execute("""
select count(*)
from filings f
inner join issuer_master im
    on f.edinet_code = im.edinet_code
where im.exchange = 'TSE'
  and im.is_listed = 1
""").fetchone()[0])

conn.close()