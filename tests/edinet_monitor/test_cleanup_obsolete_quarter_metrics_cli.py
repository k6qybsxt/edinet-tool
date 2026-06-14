from __future__ import annotations

import contextlib
import io
import shutil
import sqlite3
import sys
import unittest
import uuid
from pathlib import Path
from unittest import mock


ROOT_DIR = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT_DIR / "src"
TMP_ROOT = ROOT_DIR / "tests" / "_tmp_cleanup_obsolete_quarter_metrics"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from edinet_monitor.cli.cleanup_obsolete_quarter_metrics import main  # noqa: E402


class CleanupObsoleteQuarterMetricsCliTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp_path = TMP_ROOT / uuid.uuid4().hex
        self.tmp_path.mkdir(parents=True, exist_ok=True)
        self.db_path = self.tmp_path / "metrics.db"
        conn = sqlite3.connect(self.db_path)
        conn.executescript(
            """
            CREATE TABLE quarter_standalone_metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                quarter_type TEXT NOT NULL,
                metric_base TEXT NOT NULL
            );
            CREATE TABLE normalized_metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                metric_key TEXT NOT NULL
            );
            CREATE TABLE derived_metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                metric_base TEXT NOT NULL
            );
            CREATE TABLE jquants_financial_metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                metric_base TEXT NOT NULL
            );
            """
        )
        conn.executemany(
            "INSERT INTO quarter_standalone_metrics (quarter_type, metric_base) VALUES (?, ?)",
            [
                ("1Q", "NetSales"),
                ("1~2Q", "NetSales"),
                ("1Q", "OperatingCash"),
                ("1~2Q", "FCFGrowthRate"),
            ],
        )
        conn.executemany(
            "INSERT INTO normalized_metrics (metric_key) VALUES (?)",
            [("FCFGrowthRateCurrent",), ("NetSalesCurrent",)],
        )
        conn.executemany(
            "INSERT INTO derived_metrics (metric_base) VALUES (?)",
            [("FCFGrowthRate",), ("NetSalesGrowthRate",)],
        )
        conn.executemany(
            "INSERT INTO jquants_financial_metrics (metric_base) VALUES (?)",
            [("FCFGrowthRate",), ("NetSales",)],
        )
        conn.commit()
        conn.close()

    def tearDown(self) -> None:
        shutil.rmtree(self.tmp_path, ignore_errors=True)

    def _run_cli(self, *args: str) -> str:
        stdout = io.StringIO()
        argv = [
            "cleanup_obsolete_quarter_metrics",
            "--db-path",
            str(self.db_path),
            *args,
        ]
        with mock.patch.object(sys, "argv", argv), contextlib.redirect_stdout(stdout):
            main()
        return stdout.getvalue()

    def _count(self, table_name: str) -> int:
        conn = sqlite3.connect(self.db_path)
        try:
            return int(conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0])
        finally:
            conn.close()

    def test_dry_run_reports_targets_without_writing(self) -> None:
        output = self._run_cli()

        self.assertIn("mode=dry_run", output)
        self.assertIn("target_rows=5", output)
        self.assertIn("dry_run_only=1", output)
        self.assertEqual(self._count("quarter_standalone_metrics"), 4)
        self.assertEqual(self._count("normalized_metrics"), 2)
        self.assertEqual(self._count("derived_metrics"), 2)
        self.assertEqual(self._count("jquants_financial_metrics"), 2)

    def test_apply_deletes_only_obsolete_targets(self) -> None:
        output = self._run_cli("--apply")

        self.assertIn("mode=apply", output)
        self.assertIn("deleted_rows=5", output)
        self.assertIn("remaining_rows=0", output)
        conn = sqlite3.connect(self.db_path)
        try:
            quarter_rows = conn.execute(
                "SELECT quarter_type, metric_base FROM quarter_standalone_metrics ORDER BY id"
            ).fetchall()
            self.assertEqual(quarter_rows, [("1~2Q", "NetSales"), ("1Q", "OperatingCash")])
            normalized_rows = conn.execute(
                "SELECT metric_key FROM normalized_metrics ORDER BY id"
            ).fetchall()
            self.assertEqual(normalized_rows, [("NetSalesCurrent",)])
            derived_rows = conn.execute(
                "SELECT metric_base FROM derived_metrics ORDER BY id"
            ).fetchall()
            self.assertEqual(derived_rows, [("NetSalesGrowthRate",)])
            jquants_rows = conn.execute(
                "SELECT metric_base FROM jquants_financial_metrics ORDER BY id"
            ).fetchall()
            self.assertEqual(jquants_rows, [("NetSales",)])
        finally:
            conn.close()


if __name__ == "__main__":
    unittest.main()
