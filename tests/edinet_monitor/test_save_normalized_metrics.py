from __future__ import annotations

import sqlite3
import unittest
from unittest.mock import patch

from edinet_monitor.cli.save_normalized_metrics import run_save_normalized_metrics


class SaveNormalizedMetricsCliTest(unittest.TestCase):
    def test_run_save_normalized_metrics_accepts_sqlite_row_filings(self) -> None:
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        try:
            row = conn.execute(
                """
                SELECT
                    'S100TEST' AS doc_id,
                    'E00001' AS edinet_code,
                    '12340' AS security_code,
                    '化学' AS industry_33,
                    '2026-03-31' AS period_end,
                    'C:/tmp/test.xbrl' AS xbrl_path,
                    'C:/tmp/test.zip' AS zip_path
                """
            ).fetchone()
            assert row is not None

            with (
                patch("edinet_monitor.cli.save_normalized_metrics.create_tables"),
                patch("edinet_monitor.cli.save_normalized_metrics.get_connection", return_value=conn),
                patch(
                    "edinet_monitor.cli.save_normalized_metrics.fetch_raw_facts_saved_filings",
                    side_effect=[[row], []],
                ),
                patch(
                    "edinet_monitor.cli.save_normalized_metrics.fetch_raw_fact_rows",
                    return_value=[{"doc_id": "S100TEST"}],
                ),
                patch(
                    "edinet_monitor.cli.save_normalized_metrics.normalize_raw_fact_rows",
                    return_value=[{"doc_id": "S100TEST", "metric_key": "NetSalesCurrent"}],
                ),
                patch("edinet_monitor.cli.save_normalized_metrics.delete_normalized_metrics_by_doc_id"),
                patch(
                    "edinet_monitor.cli.save_normalized_metrics.insert_normalized_metrics",
                    return_value=1,
                ),
                patch("edinet_monitor.cli.save_normalized_metrics.mark_normalized_metrics_saved") as mark_saved,
                patch("edinet_monitor.cli.save_normalized_metrics.mark_normalized_metrics_error") as mark_error,
            ):
                result = run_save_normalized_metrics(
                    batch_size=10,
                    enable_period_fallback=True,
                    enforce_candidate_validation=True,
                )

            self.assertEqual(result["target_total"], 1)
            self.assertEqual(result["saved_docs_total"], 1)
            self.assertEqual(result["saved_rows_total"], 1)
            self.assertEqual(result["error_total"], 0)
            mark_saved.assert_called_once_with(conn, "S100TEST", commit=False)
            mark_error.assert_not_called()
        finally:
            conn.close()


if __name__ == "__main__":
    unittest.main()
