import sqlite3

db_path = r"C:\Users\silve\EDINET_Pipeline\data\edinet_monitor\edinet_monitor.db"
conn = sqlite3.connect(db_path)
cur = conn.cursor()

print("=== issuer_master count ===")
print(cur.execute("select count(*) from issuer_master").fetchone()[0])

print("\n=== issuer_master sample ===")
for row in cur.execute("""
select
    edinet_code,
    security_code,
    company_name,
    market,
    industry,
    exchange,
    listing_source,
    is_listed
from issuer_master
order by edinet_code
limit 20
""").fetchall():
    print(row)

conn.close()