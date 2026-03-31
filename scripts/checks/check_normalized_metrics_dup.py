import sqlite3

db_path = r"C:\Users\silve\EDINET_Pipeline\data\edinet_monitor\edinet_monitor.db"
conn = sqlite3.connect(db_path)
cur = conn.cursor()

print("=== normalized_metrics_total ===")
print(cur.execute("select count(*) from normalized_metrics").fetchone()[0])

print("\n=== filings status ===")
rows = cur.execute("""
select doc_id, parse_status
from filings
where doc_id in ('S100XUGC', 'S100XUNZ')
order by doc_id
""").fetchall()
for row in rows:
    print(row)

print("\n=== duplicate normalized_metrics ===")
rows = cur.execute("""
select
    doc_id,
    metric_key,
    period_end,
    value_num,
    count(*) as dup_count,
    group_concat(source_tag, ' | ') as tags
from normalized_metrics
group by doc_id, metric_key, period_end, value_num
having count(*) >= 2
order by doc_id, metric_key, period_end, value_num
limit 100
""").fetchall()

for row in rows:
    print(row)

conn.close()