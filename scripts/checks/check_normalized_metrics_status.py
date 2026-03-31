import sqlite3

db_path = r"C:\Users\silve\EDINET_Pipeline\data\edinet_monitor\edinet_monitor.db"
conn = sqlite3.connect(db_path)
cur = conn.cursor()

print("=== filings normalized_metrics status ===")
for row in cur.execute("""
select
    f.doc_id,
    f.edinet_code,
    f.security_code,
    f.download_status,
    f.parse_status
from filings f
inner join issuer_master im
    on f.edinet_code = im.edinet_code
where im.is_listed = 1
  and im.exchange = 'TSE'
order by f.submit_date asc, f.doc_id asc
limit 20
""").fetchall():
    print(row)

print("\n=== normalized_metrics count by doc_id ===")
for row in cur.execute("""
select
    nm.doc_id,
    count(*) as normalized_metric_count
from normalized_metrics nm
inner join filings f
    on nm.doc_id = f.doc_id
inner join issuer_master im
    on f.edinet_code = im.edinet_code
where im.is_listed = 1
  and im.exchange = 'TSE'
group by nm.doc_id
order by nm.doc_id
""").fetchall():
    print(row)

print("\n=== duplicate normalized_metrics ===")
for row in cur.execute("""
select
    nm.doc_id,
    nm.metric_key,
    nm.period_end,
    count(*) as dup_count
from normalized_metrics nm
inner join filings f
    on nm.doc_id = f.doc_id
inner join issuer_master im
    on f.edinet_code = im.edinet_code
where im.is_listed = 1
  and im.exchange = 'TSE'
group by nm.doc_id, nm.metric_key, nm.period_end
having count(*) >= 2
order by nm.doc_id, nm.metric_key, nm.period_end
""").fetchall():
    print(row)

conn.close()