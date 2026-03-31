import sqlite3

db_path = r"C:\Users\silve\EDINET_Pipeline\data\edinet_monitor\edinet_monitor.db"
conn = sqlite3.connect(db_path)
cur = conn.cursor()

print("=== filings raw_facts status ===")
for row in cur.execute("""
select
    f.doc_id,
    f.edinet_code,
    f.security_code,
    f.download_status,
    f.parse_status,
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

print("\n=== raw_facts count by doc_id ===")
for row in cur.execute("""
select
    rf.doc_id,
    count(*) as raw_fact_count
from raw_facts rf
inner join filings f
    on rf.doc_id = f.doc_id
inner join issuer_master im
    on f.edinet_code = im.edinet_code
where im.is_listed = 1
  and im.exchange = 'TSE'
group by rf.doc_id
order by rf.doc_id
""").fetchall():
    print(row)

conn.close()