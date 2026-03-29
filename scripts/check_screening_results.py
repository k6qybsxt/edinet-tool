import sqlite3

db_path = r"C:\Users\silve\EDINET_Pipeline\data\edinet_monitor\edinet_monitor.db"
conn = sqlite3.connect(db_path)
cur = conn.cursor()

print("=== screening_runs ===")
for row in cur.execute("""
select id, screening_date, rule_name, rule_version, target_count, hit_count
from screening_runs
order by id desc
limit 10
""").fetchall():
    print(row)

print("\n=== screening_results ===")
for row in cur.execute("""
select screening_date, rule_name, edinet_code, security_code, period_end, result_flag, score
from screening_results
order by id desc
limit 20
""").fetchall():
    print(row)

conn.close()