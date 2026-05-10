from __future__ import annotations

import sqlite3
import shutil
import sys
import unittest
import uuid
import json
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT_DIR / "src"
TMP_ROOT = ROOT_DIR / "tests" / "_tmp_edinet_period_prune"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from edinet_monitor.services.edinet_period_prune_service import (
    fetch_old_edinet_period_candidates,
    prune_old_edinet_period_data,
)


class EdinetPeriodPruneServiceTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp_path = TMP_ROOT / uuid.uuid4().hex
        self.tmp_path.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        self._create_schema()
        self._insert_fixtures()

    def tearDown(self) -> None:
        self.conn.close()
        shutil.rmtree(self.tmp_path, ignore_errors=True)

    def _create_schema(self) -> None:
        self.conn.executescript(
            """
            CREATE TABLE issuer_master (
                edinet_code TEXT PRIMARY KEY,
                security_code TEXT,
                company_name TEXT,
                is_listed INTEGER,
                exchange TEXT
            );

            CREATE TABLE filings (
                doc_id TEXT PRIMARY KEY,
                edinet_code TEXT,
                security_code TEXT,
                form_type TEXT,
                period_end TEXT,
                submit_date TEXT,
                zip_path TEXT,
                xbrl_path TEXT
            );

            CREATE TABLE raw_facts (
                id INTEGER PRIMARY KEY,
                doc_id TEXT
            );

            CREATE TABLE normalized_metrics (
                id INTEGER PRIMARY KEY,
                doc_id TEXT
            );

            CREATE TABLE derived_metrics (
                id INTEGER PRIMARY KEY,
                doc_id TEXT
            );

            CREATE TABLE market_derived_metrics (
                id INTEGER PRIMARY KEY,
                source_type TEXT,
                source_id TEXT
            );
            """
        )

    def _insert_fixtures(self) -> None:
        self.conn.execute(
            """
            INSERT INTO issuer_master (
                edinet_code, security_code, company_name, is_listed, exchange
            ) VALUES ('E00001', '1111', 'A社', 1, 'TSE')
            """
        )
        zip_path = self.tmp_path / "old.zip"
        xbrl_path = self.tmp_path / "old.xbrl"
        zip_path.write_text("zip", encoding="utf-8")
        xbrl_path.write_text("xbrl", encoding="utf-8")

        for index in range(12):
            year = 2026 - index
            doc_id = f"DOC{index:02d}"
            self.conn.execute(
                """
                INSERT INTO filings (
                    doc_id, edinet_code, security_code, form_type, period_end,
                    submit_date, zip_path, xbrl_path
                ) VALUES (?, 'E00001', '1111', '030000', ?, ?, ?, ?)
                """,
                (
                    doc_id,
                    f"{year}-03-31",
                    f"{year}-06-30",
                    str(zip_path if index == 11 else ""),
                    str(xbrl_path if index == 11 else ""),
                ),
            )
            for table in ("raw_facts", "normalized_metrics", "derived_metrics"):
                self.conn.execute(f"INSERT INTO {table} (doc_id) VALUES (?)", (doc_id,))
            self.conn.execute(
                "INSERT INTO market_derived_metrics (source_type, source_id) VALUES ('edinet', ?)",
                (doc_id,),
            )
        self.conn.commit()

    def test_fetch_candidates_keeps_latest_eleven_annual_filings(self) -> None:
        candidates = fetch_old_edinet_period_candidates(self.conn, keep_latest=11)

        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0].doc_id, "DOC11")
        self.assertEqual(candidates[0].rank, 12)

    def test_fetch_candidates_can_prune_annual_and_2q_independently(self) -> None:
        for index in range(12):
            year = 2026 - index
            doc_id = f"HALF{index:02d}"
            self.conn.execute(
                """
                INSERT INTO filings (
                    doc_id, edinet_code, security_code, form_type, period_end,
                    submit_date, zip_path, xbrl_path
                ) VALUES (?, 'E00001', '1111', '043A00', ?, ?, '', '')
                """,
                (doc_id, f"{year}-09-30", f"{year}-11-14"),
            )
            for table in ("raw_facts", "normalized_metrics", "derived_metrics"):
                self.conn.execute(f"INSERT INTO {table} (doc_id) VALUES (?)", (doc_id,))
        self.conn.commit()

        candidates = fetch_old_edinet_period_candidates(
            self.conn,
            keep_latest=11,
            form_types=("030000", "043A00"),
        )

        self.assertEqual({candidate.doc_id for candidate in candidates}, {"DOC11", "HALF11"})
        self.assertEqual({candidate.form_type for candidate in candidates}, {"030000", "043A00"})

    def test_tenbagger_learning_security_is_excluded_from_prune_by_default(self) -> None:
        self.conn.execute(
            """
            INSERT INTO issuer_master (
                edinet_code, security_code, company_name, is_listed, exchange
            ) VALUES ('E06920', '69200', 'レーザーテック', 1, 'TSE')
            """
        )
        for index in range(12):
            year = 2026 - index
            doc_id = f"TEN{index:02d}"
            self.conn.execute(
                """
                INSERT INTO filings (
                    doc_id, edinet_code, security_code, form_type, period_end,
                    submit_date, zip_path, xbrl_path
                ) VALUES (?, 'E06920', '69200', '030000', ?, ?, '', '')
                """,
                (doc_id, f"{year}-03-31", f"{year}-06-30"),
            )
        self.conn.commit()

        candidates = fetch_old_edinet_period_candidates(self.conn, keep_latest=11)
        included = fetch_old_edinet_period_candidates(
            self.conn,
            keep_latest=11,
            exclude_security_codes=frozenset(),
        )

        self.assertNotIn("TEN11", {candidate.doc_id for candidate in candidates})
        self.assertIn("TEN11", {candidate.doc_id for candidate in included})

    def test_dry_run_reports_counts_without_deleting(self) -> None:
        result = prune_old_edinet_period_data(
            self.conn,
            keep_latest=11,
            apply=False,
            output_dir=self.tmp_path,
        )

        self.assertFalse(result.apply)
        self.assertEqual(result.candidate_count, 1)
        self.assertEqual(result.deleted_counts["filings"], 1)
        self.assertEqual(result.deleted_counts["raw_facts"], 1)
        self.assertEqual(result.deleted_counts["market_derived_metrics"], 1)
        self.assertIsNotNone(result.output_path)
        self.assertTrue(result.output_path.exists())
        self.assertEqual(
            self.conn.execute("SELECT COUNT(*) FROM filings").fetchone()[0],
            12,
        )

    def test_apply_deletes_only_db_rows_and_keeps_raw_files(self) -> None:
        result = prune_old_edinet_period_data(
            self.conn,
            keep_latest=11,
            apply=True,
            output_dir=self.tmp_path,
        )

        self.assertTrue(result.apply)
        self.assertEqual(result.candidate_count, 1)
        self.assertEqual(result.deleted_counts["filings"], 1)
        self.assertEqual(
            self.conn.execute("SELECT COUNT(*) FROM filings").fetchone()[0],
            11,
        )
        for table in ("raw_facts", "normalized_metrics", "derived_metrics"):
            self.assertEqual(
                self.conn.execute(
                    f"SELECT COUNT(*) FROM {table} WHERE doc_id = 'DOC11'"
                ).fetchone()[0],
                0,
            )
        self.assertEqual(
            self.conn.execute(
                """
                SELECT COUNT(*)
                FROM market_derived_metrics
                WHERE source_type = 'edinet'
                  AND source_id = 'DOC11'
                """
            ).fetchone()[0],
            0,
        )
        self.assertTrue((self.tmp_path / "old.zip").exists())
        self.assertTrue((self.tmp_path / "old.xbrl").exists())

    def test_apply_with_delete_files_removes_zip_xbrl_and_manifest_rows(self) -> None:
        manifest_dir = self.tmp_path / "manifests"
        manifest_dir.mkdir()
        manifest_path = manifest_dir / "document_manifest_2015-06-30.jsonl"
        manifest_path.write_text(
            "\n".join(
                [
                    json.dumps({"doc_id": "DOC11", "download_status": "downloaded"}),
                    json.dumps({"doc_id": "DOC00", "download_status": "downloaded"}),
                ]
            )
            + "\n",
            encoding="utf-8",
        )

        result = prune_old_edinet_period_data(
            self.conn,
            keep_latest=11,
            delete_files=True,
            apply=True,
            output_dir=self.tmp_path,
            zip_root=self.tmp_path,
            xbrl_root=self.tmp_path,
            manifest_root=manifest_dir,
        )

        self.assertEqual(result.file_counts["zip_files"], 1)
        self.assertEqual(result.file_counts["xbrl_files"], 1)
        self.assertEqual(result.file_counts["manifest_rows"], 1)
        self.assertFalse((self.tmp_path / "old.zip").exists())
        self.assertFalse((self.tmp_path / "old.xbrl").exists())
        self.assertIn("DOC00", manifest_path.read_text(encoding="utf-8"))
        self.assertNotIn("DOC11", manifest_path.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
