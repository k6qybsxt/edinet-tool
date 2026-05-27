from __future__ import annotations

import io
import shutil
import sys
import unittest
import uuid
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch


ROOT_DIR = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT_DIR / "src"
TMP_ROOT = ROOT_DIR / "tests" / "_tmp_db_reflection_items_cli"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from edinet_monitor.cli import db_reflection_items as cli  # noqa: E402


class DbReflectionItemsCliTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp_path = TMP_ROOT / uuid.uuid4().hex
        self.tmp_path.mkdir(parents=True, exist_ok=True)
        self.db_path = self.tmp_path / "monitor.db"

    def tearDown(self) -> None:
        shutil.rmtree(self.tmp_path, ignore_errors=True)

    def _run_cli(self, argv: list[str]) -> str:
        stdout = io.StringIO()
        with (
            patch.object(sys, "argv", ["db_reflection_items", *argv]),
            redirect_stdout(stdout),
        ):
            cli.main()
        return stdout.getvalue()

    def test_add_list_show_and_complete(self) -> None:
        add_output = self._run_cli(
            [
                "add",
                "--db-path",
                str(self.db_path),
                "--title",
                "Apply schema migration",
                "--category",
                "schema",
                "--description",
                "Apply pending schema migrations.",
                "--required-command",
                "python -m edinet_monitor.cli.apply_schema_migrations",
                "--verification-sql",
                "SELECT COUNT(*) FROM schema_migrations",
                "--related-migration-id",
                "003_add_db_reflection_items",
            ]
        )
        self.assertIn("item_id=1", add_output)

        list_output = self._run_cli(["list", "--db-path", str(self.db_path)])
        self.assertIn("1\tschema\tApply schema migration", list_output)

        show_output = self._run_cli(["show", "--db-path", str(self.db_path), "--item-id", "1"])
        self.assertIn("required_command_1=python -m edinet_monitor.cli.apply_schema_migrations", show_output)
        self.assertIn("verification_sql_1=SELECT COUNT(*) FROM schema_migrations", show_output)
        self.assertIn("related_migration_id_1=003_add_db_reflection_items", show_output)

        complete_output = self._run_cli(["complete", "--db-path", str(self.db_path), "--item-id", "1"])
        self.assertIn("completed_item_id=1", complete_output)

        list_after_complete = self._run_cli(["list", "--db-path", str(self.db_path)])
        self.assertIn("no_items=1", list_after_complete)

    def test_import_txt(self) -> None:
        txt_path = self.tmp_path / "reflection.txt"
        txt_path.write_text(
            """
================================================================================
[DB反映待ち] 有利子負債の過小計上修正
追加日: 2026-05-22
状態: 未反映

DB反映に必要な処理:
1. normalized_metrics 再保存
================================================================================
""".strip()
            + "\n",
            encoding="utf-8",
        )

        output = self._run_cli(["import-txt", "--db-path", str(self.db_path), "--path", str(txt_path)])
        self.assertIn("imported_count=1", output)
        self.assertIn("item_id=1", output)

        duplicate_output = self._run_cli(["import-txt", "--db-path", str(self.db_path), "--path", str(txt_path)])
        self.assertIn("imported_count=0", duplicate_output)
        self.assertIn("skipped_count=1", duplicate_output)


if __name__ == "__main__":
    unittest.main()
