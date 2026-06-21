from __future__ import annotations

import sqlite3
import unittest

from edinet_monitor.services.half_outstanding_shares_backfill_service import (
    backfill_2q_outstanding_shares,
)


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE filings (
            doc_id TEXT PRIMARY KEY,
            edinet_code TEXT,
            security_code TEXT,
            form_type TEXT,
            period_end TEXT,
            accounting_standard TEXT,
            document_display_unit TEXT
        );
        CREATE TABLE issuer_master (
            edinet_code TEXT PRIMARY KEY,
            security_code TEXT,
            company_name TEXT,
            industry_33 TEXT
        );
        CREATE TABLE raw_facts (
            doc_id TEXT,
            tag_name TEXT,
            context_ref TEXT,
            value_text TEXT,
            unit_ref TEXT,
            period_type TEXT,
            period_end TEXT,
            instant_date TEXT,
            consolidation TEXT,
            context_dimensions_json TEXT
        );
        CREATE TABLE normalized_metrics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            doc_id TEXT NOT NULL,
            edinet_code TEXT NOT NULL,
            security_code TEXT,
            metric_key TEXT NOT NULL,
            fiscal_year INTEGER,
            period_end TEXT,
            value_num REAL,
            source_tag TEXT,
            consolidation TEXT,
            rule_version TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE UNIQUE INDEX uq_normalized_metrics_doc_metric_period
            ON normalized_metrics(doc_id, metric_key, period_end);
        CREATE TABLE derived_metrics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            doc_id TEXT NOT NULL,
            edinet_code TEXT NOT NULL,
            security_code TEXT,
            metric_key TEXT NOT NULL,
            metric_base TEXT NOT NULL,
            metric_group TEXT NOT NULL,
            fiscal_year INTEGER,
            period_end TEXT,
            period_scope TEXT NOT NULL,
            period_key TEXT,
            quarter_type TEXT,
            period_offset INTEGER NOT NULL DEFAULT 0,
            consolidation TEXT,
            accounting_standard TEXT,
            document_display_unit TEXT,
            value_num REAL,
            value_unit TEXT NOT NULL,
            calc_status TEXT NOT NULL,
            formula_name TEXT NOT NULL,
            source_detail_json TEXT,
            rule_version TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE UNIQUE INDEX uq_derived_metrics_doc_metric_period
            ON derived_metrics(doc_id, metric_key, period_end, consolidation);
        """
    )
    return conn


def _insert_base_doc(conn: sqlite3.Connection) -> None:
    conn.execute(
        "INSERT INTO issuer_master VALUES ('E00001', '1111', 'テスト社', '化学')"
    )
    conn.execute(
        "INSERT INTO filings VALUES ('DOC1', 'E00001', '1111', '043A00', '2025-09-30', '', '百万円')"
    )
    conn.execute(
        """
        INSERT INTO derived_metrics (
            doc_id, edinet_code, security_code, metric_key, metric_base, metric_group,
            fiscal_year, period_end, period_scope, period_key, quarter_type, period_offset,
            consolidation, accounting_standard, document_display_unit, value_num, value_unit,
            calc_status, formula_name, source_detail_json, rule_version, created_at, updated_at
        )
        VALUES (
            'DOC1', 'E00001', '1111', 'OutstandingSharesCurrent', 'OutstandingShares', 'share',
            2025, '2025-09-30', 'quarter', 'actual:2Q', '2Q', 0,
            'Consolidated', '', '百万円', NULL, 'shares',
            'missing_input', 'outstanding_shares', '{}', 'test', 'now', 'now'
        )
        """
    )


class HalfOutstandingSharesBackfillServiceTest(unittest.TestCase):
    def test_backfills_from_filing_date_issued_and_direct_treasury(self) -> None:
        conn = _connect()
        _insert_base_doc(conn)
        conn.executemany(
            """
            INSERT INTO raw_facts VALUES (
                'DOC1', ?, ?, ?, 'shares', 'instant', '2025-09-30', '2025-09-30',
                'Consolidated', ?
            )
            """,
            [
                (
                    "NumberOfIssuedSharesAsOfFilingDateIssuedSharesTotalNumberOfSharesEtc",
                    "FilingDateInstant",
                    "1000000",
                    "",
                ),
                (
                    "TotalNumberOfSharesHeldTreasurySharesEtc",
                    "InterimInstant",
                    "50000",
                    "",
                ),
            ],
        )

        result = backfill_2q_outstanding_shares(conn, apply=True)

        self.assertEqual(result["ok_rows"], 1)
        issued = conn.execute(
            "SELECT value_num, source_tag FROM normalized_metrics WHERE metric_key='IssuedSharesCurrent'"
        ).fetchone()
        treasury = conn.execute(
            "SELECT value_num, source_tag FROM normalized_metrics WHERE metric_key='TreasurySharesCurrent'"
        ).fetchone()
        outstanding = conn.execute(
            "SELECT value_num, calc_status FROM derived_metrics WHERE metric_key='OutstandingSharesCurrent'"
        ).fetchone()
        self.assertEqual(issued["value_num"], 1_000_000)
        self.assertEqual(
            issued["source_tag"],
            "NumberOfIssuedSharesAsOfFilingDateIssuedSharesTotalNumberOfSharesEtc",
        )
        self.assertEqual(treasury["value_num"], 50_000)
        self.assertEqual(outstanding["value_num"], 950_000)
        self.assertEqual(outstanding["calc_status"], "ok")

    def test_sums_treasury_row_members_when_no_aggregate_exists(self) -> None:
        conn = _connect()
        _insert_base_doc(conn)
        conn.executemany(
            """
            INSERT INTO raw_facts VALUES (
                'DOC1', ?, ?, ?, 'shares', 'instant', '2025-09-30', '2025-09-30',
                'Consolidated', ?
            )
            """,
            [
                (
                    "NumberOfIssuedSharesAsOfFilingDateIssuedSharesTotalNumberOfSharesEtc",
                    "FilingDateInstant",
                    "1000000",
                    "",
                ),
                (
                    "TotalNumberOfSharesHeldTreasurySharesEtc",
                    "CurrentQuarterInstant_Row1Member",
                    "40000",
                    '{"explicit_members":[{"member":"jpcrp_cor:Row1Member"}],"has_scenario":true}',
                ),
                (
                    "TotalNumberOfSharesHeldTreasurySharesEtc",
                    "CurrentQuarterInstant_Row2Member",
                    "10000",
                    '{"explicit_members":[{"member":"jpcrp_cor:Row2Member"}],"has_scenario":true}',
                ),
            ],
        )

        result = backfill_2q_outstanding_shares(conn, apply=True)

        self.assertEqual(result["ok_rows"], 1)
        treasury = conn.execute(
            "SELECT value_num, source_tag FROM normalized_metrics WHERE metric_key='TreasurySharesCurrent'"
        ).fetchone()
        outstanding = conn.execute(
            "SELECT value_num, calc_status FROM derived_metrics WHERE metric_key='OutstandingSharesCurrent'"
        ).fetchone()
        self.assertEqual(treasury["value_num"], 50_000)
        self.assertEqual(outstanding["value_num"], 950_000)
        self.assertEqual(outstanding["calc_status"], "ok")

    def test_dry_run_does_not_write(self) -> None:
        conn = _connect()
        _insert_base_doc(conn)
        conn.execute(
            """
            INSERT INTO raw_facts VALUES (
                'DOC1', 'NumberOfIssuedSharesAsOfFilingDateIssuedSharesTotalNumberOfSharesEtc', 'FilingDateInstant',
                '1000000', 'shares', 'instant', '2025-09-30', '2025-09-30',
                'Consolidated', ''
            )
            """
        )

        result = backfill_2q_outstanding_shares(conn, apply=False)

        self.assertEqual(result["candidate_actions"], 1)
        count = conn.execute("SELECT COUNT(*) FROM normalized_metrics").fetchone()[0]
        status = conn.execute(
            "SELECT calc_status FROM derived_metrics WHERE metric_key='OutstandingSharesCurrent'"
        ).fetchone()[0]
        self.assertEqual(count, 0)
        self.assertEqual(status, "missing_input")

    def test_voting_rights_tag_is_not_used_as_issued_shares(self) -> None:
        conn = _connect()
        _insert_base_doc(conn)
        conn.execute(
            """
            INSERT INTO raw_facts VALUES (
                'DOC1', 'NumberOfSharesIssuedSharesVotingRights', 'CurrentQuarterInstant',
                '1000000', 'shares', 'instant', '2025-09-30', '2025-09-30',
                'Consolidated', ''
            )
            """
        )

        result = backfill_2q_outstanding_shares(conn, apply=False)

        self.assertEqual(result["candidate_actions"], 0)


if __name__ == "__main__":
    unittest.main()
