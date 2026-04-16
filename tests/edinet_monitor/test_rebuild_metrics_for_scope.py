from __future__ import annotations

import sqlite3
import unittest

from edinet_monitor.cli.rebuild_metrics_for_scope import fetch_scope_filings


def build_test_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
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
            edinet_code TEXT,
            security_code TEXT,
            form_type TEXT,
            period_end TEXT,
            submit_date TEXT,
            accounting_standard TEXT,
            document_display_unit TEXT,
            xbrl_path TEXT,
            zip_path TEXT
        );
        """
    )
    return conn


class RebuildMetricsForScopeTest(unittest.TestCase):
    def test_fetch_scope_filings_latest_only_returns_latest_annual_filing_per_issuer(self) -> None:
        conn = build_test_connection()
        try:
            conn.executemany(
                "INSERT INTO issuer_master (edinet_code, security_code, company_name, industry_33) VALUES (?, ?, ?, ?)",
                [
                    ("E1", "86040", "Nomura", "証券、商品先物取引業"),
                    ("E2", "71810", "Kanpo", "保険業"),
                ],
            )
            conn.executemany(
                """
                INSERT INTO filings (
                    doc_id, edinet_code, security_code, form_type, period_end, submit_date,
                    accounting_standard, document_display_unit, xbrl_path, zip_path
                ) VALUES (?, ?, ?, ?, ?, ?, '', '', '', '')
                """,
                [
                    ("D1", "E1", "86040", "030000", "2024-03-31", "2024-06-20"),
                    ("D2", "E1", "86040", "030000", "2025-03-31", "2025-06-20"),
                    ("D3", "E2", "71810", "030000", "2025-03-31", "2025-06-13"),
                ],
            )

            rows = fetch_scope_filings(
                conn,
                industry_33_list=["証券、商品先物取引業", "保険業"],
                security_codes=[],
                latest_only=True,
                limit=0,
            )
        finally:
            conn.close()

        self.assertEqual({row["doc_id"] for row in rows}, {"D2", "D3"})

    def test_fetch_scope_filings_accepts_4_digit_security_code_input(self) -> None:
        conn = build_test_connection()
        try:
            conn.execute(
                "INSERT INTO issuer_master (edinet_code, security_code, company_name, industry_33) VALUES (?, ?, ?, ?)",
                ("E1", "86040", "Nomura", "証券、商品先物取引業"),
            )
            conn.execute(
                """
                INSERT INTO filings (
                    doc_id, edinet_code, security_code, form_type, period_end, submit_date,
                    accounting_standard, document_display_unit, xbrl_path, zip_path
                ) VALUES (?, ?, ?, ?, ?, ?, '', '', '', '')
                """,
                ("D1", "E1", "86040", "030000", "2025-03-31", "2025-06-20"),
            )

            rows = fetch_scope_filings(
                conn,
                industry_33_list=[],
                security_codes=["8604"],
                latest_only=True,
                limit=0,
            )
        finally:
            conn.close()

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["doc_id"], "D1")


if __name__ == "__main__":
    unittest.main()
