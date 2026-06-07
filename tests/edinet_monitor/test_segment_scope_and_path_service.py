from __future__ import annotations

import sqlite3
import shutil
import sys
import unittest
from pathlib import Path
from unittest.mock import patch


ROOT_DIR = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT_DIR / "src"
TMP_ROOT = ROOT_DIR / "tests" / "_tmp_segment_scope"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from edinet_monitor.services.edinet_storage_path_service import (  # noqa: E402
    repair_storage_paths,
    resolve_existing_path,
)
from edinet_monitor.services.segment_scope_service import (  # noqa: E402
    fetch_segment_scope_filings,
    parse_period_rank_specs,
)


def _create_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE issuer_master (
            edinet_code TEXT PRIMARY KEY,
            security_code TEXT,
            company_name TEXT,
            industry_33 TEXT,
            market TEXT,
            is_listed INTEGER NOT NULL DEFAULT 1,
            exchange TEXT
        );
        CREATE TABLE filings (
            doc_id TEXT PRIMARY KEY,
            edinet_code TEXT NOT NULL,
            security_code TEXT,
            form_type TEXT NOT NULL,
            period_end TEXT,
            submit_date TEXT,
            zip_path TEXT,
            xbrl_path TEXT,
            xbrl_member_name TEXT,
            download_status TEXT,
            parse_status TEXT
        );
        """
    )


class SegmentScopeAndPathServiceTest(unittest.TestCase):
    def setUp(self) -> None:
        if TMP_ROOT.exists():
            shutil.rmtree(TMP_ROOT)
        TMP_ROOT.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        _create_schema(self.conn)
        self.conn.execute(
            """
            INSERT INTO issuer_master (
                edinet_code, security_code, company_name, industry_33, market, is_listed, exchange
            ) VALUES ('E00001', '11110', 'A', '化学', 'Prime', 1, 'TSE')
            """
        )
        filings = []
        for index in range(1, 12):
            year = 2030 - index
            filings.append(
                (
                    f"DOC{index:02d}",
                    "E00001",
                    "11110",
                    "030000",
                    f"{year}-03-31",
                    f"{year}-06-30",
                    "",
                    "",
                    "",
                    "downloaded",
                    "derived_metrics_saved",
                )
            )
        self.conn.executemany(
            """
            INSERT INTO filings (
                doc_id, edinet_code, security_code, form_type, period_end, submit_date,
                zip_path, xbrl_path, xbrl_member_name, download_status, parse_status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            filings,
        )
        self.conn.commit()

    def tearDown(self) -> None:
        self.conn.close()
        if TMP_ROOT.exists():
            shutil.rmtree(TMP_ROOT)

    def test_parse_period_rank_specs_maps_business_terms(self) -> None:
        specs = parse_period_rank_specs("latest,5,10")
        self.assertEqual([(spec.label, spec.rank) for spec in specs], [("latest", 1), ("5_prior", 6), ("10_prior", 11)])

    def test_parse_period_rank_specs_maps_recent3(self) -> None:
        specs = parse_period_rank_specs("recent3")
        self.assertEqual([(spec.label, spec.rank) for spec in specs], [("latest", 1), ("1_prior", 2), ("2_prior", 3)])

    def test_fetch_segment_scope_filings_returns_latest_fifth_and_tenth_prior(self) -> None:
        rows = fetch_segment_scope_filings(
            self.conn,
            form_codes=["030000"],
            period_ranks="latest,5,10",
            codes=["1111"],
        )

        by_label = {row["period_rank_label"]: row for row in rows}
        self.assertEqual(by_label["latest"]["doc_id"], "DOC01")
        self.assertEqual(by_label["5_prior"]["doc_id"], "DOC06")
        self.assertEqual(by_label["10_prior"]["doc_id"], "DOC11")

    def test_fetch_segment_scope_filings_recent3_groups_half_form_types(self) -> None:
        half_filings = [
            ("HALF01", "E00001", "11110", "043A00", "2029-09-30", "2029-11-14", "", "", "", "downloaded", "derived_metrics_saved"),
            ("HALF02", "E00001", "11110", "043000", "2028-09-30", "2028-11-14", "", "", "", "downloaded", "derived_metrics_saved"),
            ("HALF03", "E00001", "11110", "043A00", "2027-09-30", "2027-11-14", "", "", "", "downloaded", "derived_metrics_saved"),
            ("HALF04", "E00001", "11110", "043000", "2026-09-30", "2026-11-14", "", "", "", "downloaded", "derived_metrics_saved"),
        ]
        self.conn.executemany(
            """
            INSERT INTO filings (
                doc_id, edinet_code, security_code, form_type, period_end, submit_date,
                zip_path, xbrl_path, xbrl_member_name, download_status, parse_status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            half_filings,
        )

        rows = fetch_segment_scope_filings(
            self.conn,
            form_codes=["043A00", "043000"],
            period_ranks="recent3",
            codes=["1111"],
        )

        by_label = {row["period_rank_label"]: row for row in rows}
        self.assertEqual([row["doc_id"] for row in rows], ["HALF01", "HALF02", "HALF03"])
        self.assertEqual(by_label["latest"]["form_type"], "043A00")
        self.assertEqual(by_label["1_prior"]["form_type"], "043000")
        self.assertEqual(by_label["2_prior"]["doc_id"], "HALF03")

    def test_resolve_existing_path_uses_replaced_storage_root(self) -> None:
        root = TMP_ROOT / "edinet_monitor"
        candidate = root / "raw" / "zip" / "2026-04-01" / "S100TEST.zip"
        candidate.parent.mkdir(parents=True)
        candidate.write_text("zip", encoding="utf-8")
        old_path = Path(r"D:\EDINET_Data\edinet_monitor\raw\zip\2026-04-01\S100TEST.zip")

        with patch("edinet_monitor.services.edinet_storage_path_service.MONITOR_STORAGE_ROOT", root):
            resolved = resolve_existing_path(current_path=old_path, expected_path=root / "missing.zip")

        self.assertEqual(resolved, candidate)

    def test_repair_storage_paths_updates_resolved_values(self) -> None:
        root = TMP_ROOT / "edinet_monitor"
        zip_path = root / "raw" / "zip" / "2029-06-30" / "DOC01.zip"
        xbrl_path = root / "raw" / "xbrl" / "2029-06-30" / "DOC01.xbrl"
        zip_path.parent.mkdir(parents=True)
        xbrl_path.parent.mkdir(parents=True)
        zip_path.write_text("zip", encoding="utf-8")
        xbrl_path.write_text("xbrl", encoding="utf-8")
        filing = {
            "doc_id": "DOC01",
            "submit_date": "2029-06-30",
            "zip_path": r"D:\EDINET_Data\edinet_monitor\raw\zip\2029-06-30\DOC01.zip",
            "xbrl_path": "",
        }

        with patch("edinet_monitor.services.edinet_storage_path_service.MONITOR_STORAGE_ROOT", root), patch(
            "edinet_monitor.services.edinet_storage_path_service.ZIP_ROOT", root / "raw" / "zip"
        ), patch("edinet_monitor.services.edinet_storage_path_service.XBRL_ROOT", root / "raw" / "xbrl"):
            actions = repair_storage_paths(self.conn, [filing], apply=True)

        self.assertEqual(len(actions), 1)
        self.assertTrue(actions[0].zip_resolved)
        self.assertTrue(actions[0].xbrl_resolved)


if __name__ == "__main__":
    unittest.main()
