from __future__ import annotations

import json
import sqlite3
import sys
import unittest
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from edinet_monitor.db.schema import _ensure_raw_facts_columns  # noqa: E402
from edinet_monitor.services.parser.raw_fact_mapper import to_raw_fact_rows  # noqa: E402
from edinet_monitor.services.parser.raw_fact_store_service import (  # noqa: E402
    RawFactInserter,
    delete_raw_facts_by_doc_ids,
    insert_raw_facts,
)
from edinet_pipeline.services.xbrl_parser import parse_xbrl_file_raw  # noqa: E402


SAMPLE_XBRL = b"""<?xml version="1.0" encoding="UTF-8"?>
<xbrli:xbrl
  xmlns:xbrli="http://www.xbrl.org/2003/instance"
  xmlns:xbrldi="http://xbrl.org/2006/xbrldi"
  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
  xmlns:iso4217="http://www.xbrl.org/2003/iso4217"
  xmlns:jppfs_cor="http://disclosure.edinet-fsa.go.jp/taxonomy/jppfs/2026-03-31/jppfs_cor"
  xmlns:jpcrp_cor="http://disclosure.edinet-fsa.go.jp/taxonomy/jpcrp/2026-03-31/jpcrp_cor">
  <xbrli:context id="CurrentYearDuration_ConsolidatedMember">
    <xbrli:entity>
      <xbrli:identifier scheme="http://disclosure.edinet-fsa.go.jp">E00001</xbrli:identifier>
      <xbrli:segment>
        <xbrldi:explicitMember dimension="jpcrp_cor:ConsolidatedOrNonConsolidatedAxis">jppfs_cor:ConsolidatedMember</xbrldi:explicitMember>
      </xbrli:segment>
    </xbrli:entity>
    <xbrli:period>
      <xbrli:startDate>2025-04-01</xbrli:startDate>
      <xbrli:endDate>2026-03-31</xbrli:endDate>
    </xbrli:period>
  </xbrli:context>
  <xbrli:unit id="JPY">
    <xbrli:measure>iso4217:JPY</xbrli:measure>
  </xbrli:unit>
  <jppfs_cor:NetSales contextRef="CurrentYearDuration_ConsolidatedMember" unitRef="JPY" decimals="-6">123000000</jppfs_cor:NetSales>
  <jppfs_cor:OperatingIncome contextRef="CurrentYearDuration_ConsolidatedMember" unitRef="JPY" xsi:nil="true"/>
</xbrli:xbrl>
"""


class CountingConnection(sqlite3.Connection):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.pragma_table_info_count = 0

    def execute(self, sql, parameters=(), /):
        if str(sql).strip().lower().startswith("pragma table_info(raw_facts)"):
            self.pragma_table_info_count += 1
        return super().execute(sql, parameters)


class RawFactMetadataTest(unittest.TestCase):
    def test_parser_and_mapper_preserve_tag_context_and_unit_metadata(self) -> None:
        parsed = parse_xbrl_file_raw(xbrl_bytes=SAMPLE_XBRL)

        net_sales = next(fact for fact in parsed["facts"] if fact["local"] == "NetSales")
        self.assertEqual(net_sales["qname"], "jppfs_cor:NetSales")
        self.assertEqual(net_sales["namespace_prefix"], "jppfs_cor")
        self.assertEqual(net_sales["taxonomy_kind"], "jp_standard")
        self.assertEqual(net_sales["decimals"], "-6")
        self.assertFalse(net_sales["is_nil"])

        nil_fact = next(fact for fact in parsed["facts"] if fact["local"] == "OperatingIncome")
        self.assertTrue(nil_fact["is_nil"])

        rows = to_raw_fact_rows(
            "S100TEST",
            parsed,
            xbrl_member_name="XBRL/PublicDoc/jpcrp030000-asr-001_E00001-000_2026-03-31_01_2026-06-28.xbrl",
        )
        row = next(item for item in rows if item["tag_name"] == "NetSales")
        dimensions = json.loads(row["context_dimensions_json"])
        unit = json.loads(row["unit_measures_json"])

        self.assertEqual(row["tag_qname"], "jppfs_cor:NetSales")
        self.assertEqual(row["namespace_uri"], "http://disclosure.edinet-fsa.go.jp/taxonomy/jppfs/2026-03-31/jppfs_cor")
        self.assertEqual(row["period_start"], "2025-04-01")
        self.assertEqual(row["period_end"], "2026-03-31")
        self.assertEqual(row["decimals"], "-6")
        self.assertEqual(row["is_nil"], 0)
        self.assertEqual(unit["measures"], ["iso4217:JPY"])
        self.assertEqual(
            dimensions["axis_members"]["jpcrp_cor:ConsolidatedOrNonConsolidatedAxis"],
            ["jppfs_cor:ConsolidatedMember"],
        )
        self.assertEqual(
            row["xbrl_member_name"],
            "XBRL/PublicDoc/jpcrp030000-asr-001_E00001-000_2026-03-31_01_2026-06-28.xbrl",
        )

    def test_insert_raw_facts_writes_metadata_columns_when_available(self) -> None:
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        self.addCleanup(conn.close)

        conn.execute(
            """
            CREATE TABLE raw_facts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                doc_id TEXT NOT NULL,
                tag_name TEXT NOT NULL,
                tag_qname TEXT,
                namespace_uri TEXT,
                namespace_prefix TEXT,
                taxonomy_kind TEXT,
                context_ref TEXT,
                unit_ref TEXT,
                decimals TEXT,
                period_type TEXT,
                period_start TEXT,
                period_end TEXT,
                instant_date TEXT,
                consolidation TEXT,
                is_nil INTEGER NOT NULL DEFAULT 0,
                context_dimensions_json TEXT,
                unit_measures_json TEXT,
                xbrl_member_name TEXT,
                value_text TEXT,
                created_at TEXT NOT NULL
            )
            """
        )
        parsed = parse_xbrl_file_raw(xbrl_bytes=SAMPLE_XBRL)
        rows = to_raw_fact_rows("S100TEST", parsed, xbrl_member_name="XBRL/PublicDoc/main.xbrl")

        saved_count = insert_raw_facts(conn, rows)

        self.assertEqual(saved_count, 2)
        row = conn.execute(
            """
            SELECT tag_qname, namespace_prefix, decimals, unit_measures_json, xbrl_member_name
            FROM raw_facts
            WHERE tag_name = 'NetSales'
            """
        ).fetchone()
        self.assertEqual(row["tag_qname"], "jppfs_cor:NetSales")
        self.assertEqual(row["namespace_prefix"], "jppfs_cor")
        self.assertEqual(row["decimals"], "-6")
        self.assertEqual(json.loads(row["unit_measures_json"])["measures"], ["iso4217:JPY"])
        self.assertEqual(row["xbrl_member_name"], "XBRL/PublicDoc/main.xbrl")

    def test_raw_fact_inserter_reuses_table_columns_and_insert_sql(self) -> None:
        conn = sqlite3.connect(":memory:", factory=CountingConnection)
        conn.row_factory = sqlite3.Row
        self.addCleanup(conn.close)

        conn.execute(
            """
            CREATE TABLE raw_facts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                doc_id TEXT NOT NULL,
                tag_name TEXT NOT NULL,
                tag_qname TEXT,
                value_text TEXT,
                created_at TEXT NOT NULL
            )
            """
        )
        inserter = RawFactInserter(conn)
        first_count = inserter.insert_many(
            [
                {
                    "doc_id": "DOC1",
                    "tag_name": "NetSales",
                    "tag_qname": "jppfs_cor:NetSales",
                    "value_text": "100",
                    "created_at": "2026-05-30 10:00:00",
                }
            ],
            chunk_size=1,
        )
        second_count = inserter.insert_many(
            [
                {
                    "doc_id": "DOC2",
                    "tag_name": "OperatingIncome",
                    "tag_qname": "jppfs_cor:OperatingIncome",
                    "value_text": "200",
                    "created_at": "2026-05-30 10:00:00",
                }
            ],
            chunk_size=1,
        )

        self.assertEqual(first_count, 1)
        self.assertEqual(second_count, 1)
        self.assertEqual(conn.pragma_table_info_count, 1)
        row_count = conn.execute("SELECT COUNT(*) FROM raw_facts").fetchone()[0]
        self.assertEqual(row_count, 2)

    def test_delete_raw_facts_by_doc_ids_chunks_targets_only(self) -> None:
        conn = sqlite3.connect(":memory:")
        self.addCleanup(conn.close)
        conn.execute(
            """
            CREATE TABLE raw_facts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                doc_id TEXT NOT NULL,
                tag_name TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )
        conn.executemany(
            """
            INSERT INTO raw_facts (doc_id, tag_name, created_at)
            VALUES (?, 'NetSales', '2026-05-30 10:00:00')
            """,
            [("DOC1",), ("DOC2",), ("DOC3",), ("KEEP",)],
        )

        deleted_count = delete_raw_facts_by_doc_ids(conn, ["DOC1", "DOC2", "DOC3"], chunk_size=2)

        self.assertEqual(deleted_count, 3)
        remaining = [
            str(row[0])
            for row in conn.execute("SELECT doc_id FROM raw_facts ORDER BY doc_id").fetchall()
        ]
        self.assertEqual(remaining, ["KEEP"])

    def test_schema_migration_adds_raw_fact_metadata_columns(self) -> None:
        conn = sqlite3.connect(":memory:")
        self.addCleanup(conn.close)

        conn.execute(
            """
            CREATE TABLE raw_facts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                doc_id TEXT NOT NULL,
                tag_name TEXT NOT NULL,
                context_ref TEXT,
                unit_ref TEXT,
                period_type TEXT,
                period_start TEXT,
                period_end TEXT,
                instant_date TEXT,
                consolidation TEXT,
                value_text TEXT,
                created_at TEXT NOT NULL
            )
            """
        )

        _ensure_raw_facts_columns(conn.cursor())

        columns = {
            str(row[1])
            for row in conn.execute("PRAGMA table_info(raw_facts)").fetchall()
        }
        self.assertIn("tag_qname", columns)
        self.assertIn("context_dimensions_json", columns)
        self.assertIn("unit_measures_json", columns)
        self.assertIn("xbrl_member_name", columns)


if __name__ == "__main__":
    unittest.main()
