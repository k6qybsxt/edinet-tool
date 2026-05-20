from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace
import unittest
from unittest.mock import patch
from pathlib import Path

from edinet_monitor.services.db_backup_service import backup_sqlite_db, build_backup_path


class DbBackupServiceTest(unittest.TestCase):
    def test_build_backup_path_adds_safe_label_and_timestamp(self) -> None:
        path = build_backup_path(
            source_path=Path("edinet_monitor.db"),
            output_dir=Path("D:/EDINET_Backup"),
            label="before/apply",
            timestamp=datetime(2026, 5, 20, 1, 2, 3),
        )

        self.assertEqual(
            path,
            Path("D:/EDINET_Backup/edinet_monitor_before_apply_20260520_010203.db"),
        )

    def test_backup_sqlite_db_copies_file_to_output_dir(self) -> None:
        source = Path("E:/EDINET_Data/edinet_monitor/db/edinet_monitor.db")
        output_dir = Path("D:/EDINET_Backup")

        with (
            patch("edinet_monitor.services.db_backup_service.Path.exists", return_value=True),
            patch("edinet_monitor.services.db_backup_service.Path.is_file", return_value=True),
            patch("edinet_monitor.services.db_backup_service.Path.mkdir") as mkdir,
            patch("edinet_monitor.services.db_backup_service.Path.stat") as stat,
            patch("edinet_monitor.services.db_backup_service.shutil.copy2") as copy2,
        ):
            stat.return_value = SimpleNamespace(st_size=7)
            result = backup_sqlite_db(source_path=source, output_dir=output_dir, label="unit")

        mkdir.assert_called_once_with(parents=True, exist_ok=True)
        copy2.assert_called_once_with(source, result.backup_path)
        self.assertEqual(result.source_size_bytes, 7)
        self.assertEqual(result.backup_size_bytes, 7)


if __name__ == "__main__":
    unittest.main()
