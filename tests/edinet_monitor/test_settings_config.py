from __future__ import annotations

import importlib
import os
import sys
import unittest
from pathlib import Path
from unittest.mock import patch


ROOT_DIR = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

import edinet_monitor.config.settings as monitor_settings  # noqa: E402


class MonitorSettingsConfigTest(unittest.TestCase):
    def tearDown(self) -> None:
        importlib.reload(monitor_settings)

    def test_monitor_defaults_point_to_e_drive(self) -> None:
        with patch.dict(
            os.environ,
            {
                "EDINET_MONITOR_DB_ROOT": "",
                "EDINET_MONITOR_STORAGE_ROOT": "",
                "EDINET_TSE_MASTER_CSV": "",
            },
            clear=False,
        ):
            reloaded = importlib.reload(monitor_settings)

        self.assertEqual(
            reloaded.MONITOR_DB_ROOT,
            Path(r"E:\EDINET_Data\edinet_monitor\db"),
        )
        self.assertEqual(
            reloaded.MONITOR_STORAGE_ROOT,
            Path(r"E:\EDINET_Data\edinet_monitor"),
        )
        self.assertEqual(
            reloaded.TSE_LISTING_MASTER_CSV_PATH,
            Path(r"E:\EDINET_Data\master\tse_issuer_master_latest.csv"),
        )

    def test_monitor_paths_support_env_overrides(self) -> None:
        with patch.dict(
            os.environ,
            {
                "EDINET_MONITOR_DB_ROOT": r"F:\Monitor\db",
                "EDINET_MONITOR_STORAGE_ROOT": r"F:\Monitor\storage",
                "EDINET_TSE_MASTER_CSV": r"F:\master\tse.csv",
            },
            clear=False,
        ):
            reloaded = importlib.reload(monitor_settings)

        self.assertEqual(reloaded.MONITOR_DB_ROOT, Path(r"F:\Monitor\db"))
        self.assertEqual(reloaded.MONITOR_STORAGE_ROOT, Path(r"F:\Monitor\storage"))
        self.assertEqual(reloaded.TSE_LISTING_MASTER_CSV_PATH, Path(r"F:\master\tse.csv"))


if __name__ == "__main__":
    unittest.main()
