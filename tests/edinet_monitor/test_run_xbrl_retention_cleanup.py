from __future__ import annotations

import io
import sys
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import MagicMock, patch


ROOT_DIR = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))


class RunXbrlRetentionCleanupCliTest(unittest.TestCase):
    @patch("edinet_monitor.cli.run_xbrl_retention_cleanup.cleanup_old_xbrl_storage")
    @patch("edinet_monitor.cli.run_xbrl_retention_cleanup.get_connection")
    @patch("edinet_monitor.cli.run_xbrl_retention_cleanup.create_tables")
    def test_main_runs_cleanup_and_prints_summary(
        self,
        mock_create_tables: MagicMock,
        mock_get_connection: MagicMock,
        mock_cleanup_old_xbrl_storage: MagicMock,
    ) -> None:
        from edinet_monitor.cli.run_xbrl_retention_cleanup import main

        mock_conn = MagicMock()
        mock_get_connection.return_value = mock_conn
        mock_cleanup_old_xbrl_storage.return_value = {
            "status": "completed",
            "reason": "",
            "reference_month": "2026-04",
            "keep_from_month": "2026-02",
            "target_total": 10,
            "deleted_total": 8,
            "missing_file_total": 2,
            "error_total": 0,
            "sample_deleted_paths": [],
            "error": "",
        }

        stdout = io.StringIO()
        with patch.object(sys, "argv", ["run_xbrl_retention_cleanup", "--keep-months", "3"]):
            with redirect_stdout(stdout):
                main()

        output = stdout.getvalue()
        self.assertIn("xbrl_retention_status=completed", output)
        self.assertIn("xbrl_retention_deleted_total=8", output)
        mock_create_tables.assert_called_once()
        mock_cleanup_old_xbrl_storage.assert_called_once()
        self.assertEqual(mock_cleanup_old_xbrl_storage.call_args.kwargs["keep_months"], 3)
        mock_conn.close.assert_called_once()

    @patch("edinet_monitor.cli.run_xbrl_retention_cleanup.cleanup_old_xbrl_storage")
    @patch("edinet_monitor.cli.run_xbrl_retention_cleanup.get_connection")
    @patch("edinet_monitor.cli.run_xbrl_retention_cleanup.create_tables")
    def test_main_supports_disable_flag(
        self,
        mock_create_tables: MagicMock,
        mock_get_connection: MagicMock,
        mock_cleanup_old_xbrl_storage: MagicMock,
    ) -> None:
        from edinet_monitor.cli.run_xbrl_retention_cleanup import main

        mock_conn = MagicMock()
        mock_get_connection.return_value = mock_conn
        mock_cleanup_old_xbrl_storage.return_value = {
            "status": "skipped",
            "reason": "disabled",
            "reference_month": "",
            "keep_from_month": "",
            "target_total": 0,
            "deleted_total": 0,
            "missing_file_total": 0,
            "error_total": 0,
            "sample_deleted_paths": [],
            "error": "",
        }

        stdout = io.StringIO()
        with patch.object(sys, "argv", ["run_xbrl_retention_cleanup", "--disable"]):
            with redirect_stdout(stdout):
                main()

        output = stdout.getvalue()
        self.assertIn("xbrl_retention_enabled=0", output)
        self.assertIn("xbrl_retention_reason=disabled", output)
        self.assertEqual(mock_cleanup_old_xbrl_storage.call_args.kwargs["enabled"], False)
        mock_conn.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
