import sqlite3

db_path = r"C:\Users\silve\EDINET_Pipeline\data\edinet_monitor\edinet_monitor.db"
conn = sqlite3.connect(db_path)
cur = conn.cursor()

groups = cur.execute("""
select
    screening_date,
    rule_name,
    max(id) as keep_run_id,
    count(*) as run_count
from screening_runs
group by screening_date, rule_name
having count(*) >= 2
""").fetchall()

print("duplicate_groups=", len(groups))

for screening_date, rule_name, keep_run_id, run_count in groups:
    print("cleanup_target=", screening_date, rule_name, "keep_run_id=", keep_run_id, "run_count=", run_count)

    delete_run_ids = [
        row[0]
        for row in cur.execute("""
            select id
            from screening_runs
            where screening_date = ?
              and rule_name = ?
              and id <> ?
        """, (screening_date, rule_name, keep_run_id)).fetchall()
    ]

    for delete_run_id in delete_run_ids:
        cur.execute("delete from screening_results where screening_run_id = ?", (delete_run_id,))
        cur.execute("delete from screening_runs where id = ?", (delete_run_id,))

conn.commit()

print("after_cleanup_runs=")
for row in cur.execute("""
select
    screening_date,
    rule_name,
    count(*) as run_count
from screening_runs
group by screening_date, rule_name
order by screening_date desc, rule_name
""").fetchall():
    print(row)

print("after_cleanup_results=")
for row in cur.execute("""
select
    screening_date,
    rule_name,
    count(*) as result_count
from screening_results
group by screening_date, rule_name
order by screening_date desc, rule_name
""").fetchall():
    print(row)

conn.close()