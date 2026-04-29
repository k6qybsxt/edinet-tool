from __future__ import annotations

import sqlite3
import sys
import unittest
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from edinet_monitor.cli.cleanup_obsolete_half_metrics import (  # noqa: E402
    count_obsolete_half_metrics,
    delete_obsolete_half_metrics,
)


class CleanupObsoleteHalfMetricsTest(unittest.TestCase):
    def test_counts_and_deletes_only_half_obsolete_metrics(self) -> None:
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.executescript(
            """
            CREATE TABLE filings (
                doc_id TEXT PRIMARY KEY,
                form_type TEXT
            );
            CREATE TABLE derived_metrics (
                doc_id TEXT,
                metric_key TEXT
            );
            """
        )
        conn.executemany(
            "INSERT INTO filings (doc_id, form_type) VALUES (?, ?)",
            [
                ("HALF1", "043A00"),
                ("ANNUAL1", "030000"),
            ],
        )
        conn.executemany(
            "INSERT INTO derived_metrics (doc_id, metric_key) VALUES (?, ?)",
            [
                ("HALF1", "NetSalesGrowthRate5YearCurrent"),
                ("HALF1", "NetSalesGrowthRate10YearCurrent"),
                ("HALF1", "NetSalesGrowthRateCurrent"),
                ("ANNUAL1", "NetSalesGrowthRate5YearCurrent"),
            ],
        )

        rows = count_obsolete_half_metrics(conn)
        self.assertEqual(sum(int(row["row_count"]) for row in rows), 2)

        deleted = delete_obsolete_half_metrics(conn)

        self.assertEqual(deleted, 2)
        remaining = conn.execute("SELECT doc_id, metric_key FROM derived_metrics").fetchall()
        self.assertEqual(len(remaining), 2)
        self.assertEqual(sum(int(row["row_count"]) for row in count_obsolete_half_metrics(conn)), 0)


if __name__ == "__main__":
    unittest.main()
