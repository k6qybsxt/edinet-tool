import sqlite3

db_path = r"C:\Users\silve\EDINET_Pipeline\data\edinet_monitor\edinet_monitor.db"
conn = sqlite3.connect(db_path)
cur = conn.cursor()

print("=== screening_runs grouped ===")
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

print("\n=== screening_results grouped ===")
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