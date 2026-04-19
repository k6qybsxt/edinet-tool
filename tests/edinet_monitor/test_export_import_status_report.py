from __future__ import annotations

import sqlite3
import sys
import unittest
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from edinet_monitor.cli.export_import_status_report import (  # noqa: E402
    ReportFilters,
    expand_with_annual_gaps,
)


class ExportImportStatusReportTest(unittest.TestCase):
    def test_expand_with_annual_gaps_accepts_feb_29_for_feb_28_leap_year(self) -> None:
        conn = sqlite3.connect(":memory:")
        try:
            conn.row_factory = sqlite3.Row
            conn.execute(
                """
                CREATE TABLE filings (
                    doc_id TEXT,
                    edinet_code TEXT,
                    security_code TEXT,
                    form_type TEXT,
                    period_end TEXT,
                    submit_date TEXT
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE issuer_master (
                    edinet_code TEXT,
                    security_code TEXT,
                    company_name TEXT,
                    industry_33 TEXT
                )
                """
            )
            conn.execute(
                """
                INSERT INTO issuer_master (edinet_code, security_code, company_name, industry_33)
                VALUES ('E00001', '12340', 'Test Corp', '小売業')
                """
            )
            conn.executemany(
                """
                INSERT INTO filings (doc_id, edinet_code, security_code, form_type, period_end, submit_date)
                VALUES (?, 'E00001', '12340', '030000', ?, ?)
                """,
                [
                    ("S100AAAA", "2019-02-28", "2019-05-30 10:00"),
                    ("S100BBBB", "2020-02-29", "2020-05-29 10:00"),
                    ("S100CCCC", "2021-02-28", "2021-05-28 10:00"),
                ],
            )
            filters = ReportFilters(
                period_mode="all",
                period_from="",
                period_to="",
                industry_33_list=[],
                security_codes=[],
                status_list=["FILING_MISSING"],
                output_dir=Path("."),
                db_path=Path(":memory:"),
            )
            rows, annual_gap_detection = expand_with_annual_gaps(
                conn,
                filters,
                [
                    {
                        "doc_id": "S100AAAA",
                        "edinet_code": "E00001",
                        "security_code": "12340",
                        "company_name": "Test Corp",
                        "industry_33": "小売業",
                        "period_end": "2019-02-28",
                        "submit_date": "2019-05-30 10:00",
                        "download_status": "downloaded",
                        "parse_status": "derived_metrics_saved",
                        "zip_path": "",
                        "xbrl_path": "",
                        "normalized_count": 1,
                        "derived_count": 1,
                    },
                    {
                        "doc_id": "S100BBBB",
                        "edinet_code": "E00001",
                        "security_code": "12340",
                        "company_name": "Test Corp",
                        "industry_33": "小売業",
                        "period_end": "2020-02-29",
                        "submit_date": "2020-05-29 10:00",
                        "download_status": "downloaded",
                        "parse_status": "derived_metrics_saved",
                        "zip_path": "",
                        "xbrl_path": "",
                        "normalized_count": 1,
                        "derived_count": 1,
                    },
                    {
                        "doc_id": "S100CCCC",
                        "edinet_code": "E00001",
                        "security_code": "12340",
                        "company_name": "Test Corp",
                        "industry_33": "小売業",
                        "period_end": "2021-02-28",
                        "submit_date": "2021-05-28 10:00",
                        "download_status": "downloaded",
                        "parse_status": "derived_metrics_saved",
                        "zip_path": "",
                        "xbrl_path": "",
                        "normalized_count": 1,
                        "derived_count": 1,
                    },
                ],
            )
        finally:
            conn.close()

        self.assertTrue(annual_gap_detection)
        self.assertEqual([row["period_end"] for row in rows], ["2021-02-28", "2020-02-29", "2019-02-28"])
        self.assertNotIn("FILING_MISSING", {row["status"] for row in rows})

    def test_all_mode_does_not_backfill_latest_fiscal_month_day_before_it_exists(self) -> None:
        conn = sqlite3.connect(":memory:")
        try:
            conn.row_factory = sqlite3.Row
            conn.execute(
                """
                CREATE TABLE filings (
                    doc_id TEXT,
                    edinet_code TEXT,
                    security_code TEXT,
                    form_type TEXT,
                    period_end TEXT,
                    submit_date TEXT
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE issuer_master (
                    edinet_code TEXT,
                    security_code TEXT,
                    company_name TEXT,
                    industry_33 TEXT
                )
                """
            )
            conn.execute(
                """
                INSERT INTO issuer_master (edinet_code, security_code, company_name, industry_33)
                VALUES ('E00001', '12340', 'Test Corp', '小売業')
                """
            )
            actual_rows = []
            for doc_id, period_end in [
                ("S100AAAA", "2019-03-31"),
                ("S100BBBB", "2020-03-31"),
                ("S100CCCC", "2021-02-28"),
                ("S100DDDD", "2022-02-28"),
            ]:
                conn.execute(
                    """
                    INSERT INTO filings (doc_id, edinet_code, security_code, form_type, period_end, submit_date)
                    VALUES (?, 'E00001', '12340', '030000', ?, '2026-01-01 10:00')
                    """,
                    (doc_id, period_end),
                )
                actual_rows.append(
                    {
                        "doc_id": doc_id,
                        "edinet_code": "E00001",
                        "security_code": "12340",
                        "company_name": "Test Corp",
                        "industry_33": "小売業",
                        "period_end": period_end,
                        "submit_date": "2026-01-01 10:00",
                        "download_status": "downloaded",
                        "parse_status": "derived_metrics_saved",
                        "zip_path": "",
                        "xbrl_path": "",
                        "normalized_count": 1,
                        "derived_count": 1,
                    }
                )
            filters = ReportFilters(
                period_mode="all",
                period_from="",
                period_to="",
                industry_33_list=[],
                security_codes=[],
                status_list=["FILING_MISSING"],
                output_dir=Path("."),
                db_path=Path(":memory:"),
            )
            rows, _ = expand_with_annual_gaps(conn, filters, actual_rows)
        finally:
            conn.close()

        self.assertEqual({row["status"] for row in rows}, {"OK"})
        self.assertNotIn("2020-02-28", {row["period_end"] for row in rows})


if __name__ == "__main__":
    unittest.main()
