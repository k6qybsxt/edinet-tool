import sqlite3

db_path = r"C:\Users\silve\EDINET_Pipeline\data\edinet_monitor\edinet_monitor.db"
conn = sqlite3.connect(db_path)
cur = conn.cursor()

print("=== low metric docs ===")
for row in cur.execute("""
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
  and ifnull(nm.metric_count, 0) < 52
order by
    ifnull(nm.metric_count, 0) asc,
    f.submit_date asc,
    f.doc_id asc
""").fetchall():
    print(row)

conn.close()