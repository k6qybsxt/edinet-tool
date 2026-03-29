import sqlite3

TARGET_DOC_IDS = ["S100XUGC", "S100XUNZ"]

db_path = r"C:\Users\silve\EDINET_Pipeline\data\edinet_monitor\edinet_monitor.db"
conn = sqlite3.connect(db_path)
cur = conn.cursor()

for doc_id in TARGET_DOC_IDS:
    cur.execute("delete from normalized_metrics where doc_id = ?", (doc_id,))
    cur.execute(
        "update filings set parse_status = ? where doc_id = ?",
        ("raw_facts_saved", doc_id),
    )

conn.commit()

print("reset_done")
for row in cur.execute("""
select doc_id, parse_status
from filings
where doc_id in ('S100XUGC', 'S100XUNZ')
order by doc_id
""").fetchall():
    print(row)

for row in cur.execute("""
select doc_id, count(*)
from normalized_metrics
where doc_id in ('S100XUGC', 'S100XUNZ')
group by doc_id
order by doc_id
""").fetchall():
    print(row)

conn.close()