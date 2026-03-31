import sqlite3

db_path = r"C:\Users\silve\EDINET_Pipeline\data\edinet_monitor\edinet_monitor.db"
conn = sqlite3.connect(db_path)
conn.row_factory = sqlite3.Row
cur = conn.cursor()

print("=== issuer_master count ===")
row = cur.execute("select count(*) as cnt from issuer_master").fetchone()
print(row["cnt"])

print()
print("=== issuer_master sample ===")
for row in cur.execute("""
select
    edinet_code,
    security_code,
    company_name,
    market,
    industry_33,
    industry_17,
    exchange,
    listing_category_raw,
    is_listed,
    listing_source,
    updated_at
from issuer_master
order by security_code asc
limit 20
""").fetchall():
    print(dict(row))

conn.close()