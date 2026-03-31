import sqlite3

TARGET_DOC_IDS = [
    "S100XUUD",
    "S100XUQB",
    "S100XUPN",
    "S100XUPZ",
    "S100XUJT",
    "S100XURZ",
    "S100XV2A",
    "S100XUQ2",
    "S100XUP3",
    "S100XV4F",
    "S100XV5Y",
    "S100XUXC",
    "S100XUSL",
    "S100XUXF",
    "S100XUP0",
    "S100XUQY",
    "S100XUHX",
    "S100XU04",
    "S100XV0N",
    "S100XTNP",
    "S100XUPT",
    "S100XV4H",
    "S100XV4M",
    "S100XURW",
]

db_path = r"C:\Users\silve\EDINET_Pipeline\data\edinet_monitor\edinet_monitor.db"
conn = sqlite3.connect(db_path)
cur = conn.cursor()

placeholders = ",".join("?" for _ in TARGET_DOC_IDS)

print("=== normalized metric count ===")
for row in cur.execute(
    f"""
    select
        f.doc_id,
        f.edinet_code,
        f.security_code,
        ifnull(nm.metric_count, 0) as metric_count
    from filings f
    left join (
        select doc_id, count(*) as metric_count
        from normalized_metrics
        group by doc_id
    ) nm
        on f.doc_id = nm.doc_id
    where f.doc_id in ({placeholders})
    order by ifnull(nm.metric_count, 0) asc, f.doc_id asc
    """,
    TARGET_DOC_IDS,
).fetchall():
    print(row)

print("\n=== metric keys by doc_id ===")
for doc_id in TARGET_DOC_IDS:
    print(f"\n--- {doc_id} ---")
    rows = cur.execute(
        """
        select
            metric_key,
            period_end,
            source_tag,
            consolidation,
            value_num
        from normalized_metrics
        where doc_id = ?
        order by metric_key, period_end
        """,
        (doc_id,),
    ).fetchall()

    if not rows:
        print("(no normalized_metrics)")
        continue

    for row in rows:
        print(row)

print("\n=== raw fact summary by doc_id / tag_name / consolidation ===")
for doc_id in TARGET_DOC_IDS:
    print(f"\n--- {doc_id} ---")
    rows = cur.execute(
        """
        select
            tag_name,
            consolidation,
            count(*) as row_count
        from raw_facts
        where doc_id = ?
        group by tag_name, consolidation
        order by row_count desc, tag_name asc
        """,
        (doc_id,),
    ).fetchall()

    if not rows:
        print("(no raw_facts)")
        continue

    for row in rows[:40]:
        print(row)

conn.close()