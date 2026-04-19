from __future__ import annotations

import sqlite3
import sys
import unittest
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from edinet_monitor.services.metric_audit_service import (  # noqa: E402
    build_extension_candidate_report,
    build_metric_audit_report,
    discover_extension_tag_candidates,
    fetch_filing,
    fetch_raw_fact_audit_rows,
)


def create_tables(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE issuer_master (
            edinet_code TEXT PRIMARY KEY,
            security_code TEXT,
            company_name TEXT,
            industry_33 TEXT
        );
        CREATE TABLE filings (
            doc_id TEXT PRIMARY KEY,
            edinet_code TEXT NOT NULL,
            security_code TEXT,
            form_type TEXT NOT NULL,
            period_end TEXT,
            submit_date TEXT,
            xbrl_path TEXT,
            zip_path TEXT
        );
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
        );
        """
    )
    conn.commit()


def insert_sample_data(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        INSERT INTO issuer_master (edinet_code, security_code, company_name, industry_33)
        VALUES ('E00001', '12340', 'テスト株式会社', '化学')
        """
    )
    conn.execute(
        """
        INSERT INTO filings (
            doc_id, edinet_code, security_code, form_type, period_end, submit_date,
            xbrl_path, zip_path
        )
        VALUES (
            'S100TEST', 'E00001', '12340', '030000', '2026-03-31',
            '2026-06-28', '', ''
        )
        """
    )
    conn.execute(
        """
        INSERT INTO raw_facts (
            doc_id, tag_name, tag_qname, namespace_uri, namespace_prefix, taxonomy_kind,
            context_ref, unit_ref, decimals, period_type, period_start, period_end,
            instant_date, consolidation, is_nil, context_dimensions_json, unit_measures_json,
            xbrl_member_name, value_text, created_at
        )
        VALUES
        (
            'S100TEST', 'NetSales', 'jppfs_cor:NetSales',
            'http://example.com/jppfs', 'jppfs_cor', 'jp_standard',
            'CurrentYearDuration_ConsolidatedMember', 'JPY', '-6', 'duration',
            '2025-04-01', '2026-03-31', '', 'Consolidated', 0,
            '{"axis_members":{}}', '{"measures":["iso4217:JPY"]}',
            'XBRL/PublicDoc/main.xbrl', '1000', '2026-04-20 00:00:00'
        ),
        (
            'S100TEST', 'CustomRevenueExt', 'ext:CustomRevenueExt',
            'http://example.com/ext', 'ext', 'extension',
            'CurrentYearDuration_ConsolidatedMember', 'JPY', '-6', 'duration',
            '2025-04-01', '2026-03-31', '', 'Consolidated', 0,
            '{"axis_members":{}}', '{"measures":["iso4217:JPY"]}',
            'XBRL/PublicDoc/main.xbrl', '1100', '2026-04-20 00:00:00'
        )
        """
    )
    conn.commit()


class MetricAuditServiceTest(unittest.TestCase):
    def test_fetch_filing_and_raw_fact_audit_rows(self) -> None:
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        self.addCleanup(conn.close)
        create_tables(conn)
        insert_sample_data(conn)

        filing = fetch_filing(conn, security_code="1234", period_end="2026-03-31")
        raw_rows = fetch_raw_fact_audit_rows(conn, "S100TEST")

        self.assertIsNotNone(filing)
        self.assertEqual(filing["doc_id"], "S100TEST")
        self.assertEqual(len(raw_rows), 2)
        self.assertEqual(raw_rows[0]["tag_qname"], "ext:CustomRevenueExt")

    def test_build_metric_audit_report_contains_selected_tag_metadata(self) -> None:
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        self.addCleanup(conn.close)
        create_tables(conn)
        insert_sample_data(conn)
        filing = fetch_filing(conn, doc_id="S100TEST")
        raw_rows = fetch_raw_fact_audit_rows(conn, "S100TEST")

        candidates = [
            {
                "_selected": "YES",
                "_selection_reason": "selected_best_rank",
                "metric_key": "NetSalesCurrent",
                "source_tag": "NetSales",
                "value_num": 1000,
                "consolidation": "Consolidated",
                "_tag_priority": 0,
                "_structure_priority": 9999,
                "_manual_override_priority": 9999,
                "_consolidation_rank": 0,
                "_raw_context_ref": "CurrentYearDuration_ConsolidatedMember",
                "_raw_tag_qname": "jppfs_cor:NetSales",
                "_schema_type": "xbrli:monetaryItemType",
            }
        ]
        lines = build_metric_audit_report(
            filing=filing,
            candidates=candidates,
            selected=candidates,
            metric_base="NetSales",
            all_periods=False,
            target_metric_key="NetSalesCurrent",
        )
        text = "\n".join(lines)

        self.assertIn("metric_selection_audit", text)
        self.assertIn("NetSalesCurrent", text)
        self.assertIn("jppfs_cor:NetSales", text)

    def test_discover_extension_tag_candidates_returns_unmapped_extension_rows(self) -> None:
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        self.addCleanup(conn.close)
        create_tables(conn)
        insert_sample_data(conn)
        filing = fetch_filing(conn, doc_id="S100TEST")
        raw_rows = fetch_raw_fact_audit_rows(conn, "S100TEST")

        candidates = discover_extension_tag_candidates(
            filing=filing,
            raw_rows=raw_rows,
            metric_base="NetSales",
            include_mapped=False,
            current_only=True,
            limit=10,
        )
        report = build_extension_candidate_report(
            filing=filing,
            metric_base="NetSales",
            rows=candidates,
        )

        self.assertEqual(candidates[0]["tag_name"], "CustomRevenueExt")
        self.assertEqual(candidates[0]["taxonomy_kind"], "extension")
        self.assertIn("extension_tag_candidates", "\n".join(report))


if __name__ == "__main__":
    unittest.main()
