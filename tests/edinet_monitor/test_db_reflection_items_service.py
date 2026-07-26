from __future__ import annotations

import shutil
import sqlite3
import sys
import unittest
import uuid
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT_DIR / "src"
TMP_ROOT = ROOT_DIR / "tests" / "_tmp_db_reflection_items_service"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from edinet_monitor.db.schema import create_tables, get_connection  # noqa: E402
from edinet_monitor.services.db_reflection_item_service import (  # noqa: E402
    add_db_reflection_item,
    complete_db_reflection_item,
    get_db_reflection_item,
    import_db_reflection_items_from_txt,
    list_db_reflection_items,
    update_db_reflection_item,
)


class DbReflectionItemsServiceTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp_path = TMP_ROOT / uuid.uuid4().hex
        self.tmp_path.mkdir(parents=True, exist_ok=True)
        self.db_path = self.tmp_path / "monitor.db"
        create_tables(self.db_path)
        self.conn = get_connection(self.db_path)

    def tearDown(self) -> None:
        self.conn.close()
        shutil.rmtree(self.tmp_path, ignore_errors=True)

    def test_add_list_show_and_complete_item(self) -> None:
        item = add_db_reflection_item(
            self.conn,
            title="Rebuild derived metrics",
            category="recalculation",
            description="Rebuild derived_metrics after formula change.",
            required_commands=["python -m edinet_monitor.cli.save_derived_metrics --run-all"],
            verification_sql=["SELECT COUNT(*) FROM derived_metrics"],
            related_migration_ids=["003_add_db_reflection_items"],
            notes="fixture",
        )

        items = list_db_reflection_items(self.conn)
        self.assertEqual([row.item_id for row in items], [item.item_id])
        loaded = get_db_reflection_item(self.conn, item.item_id)
        self.assertIsNotNone(loaded)
        self.assertEqual(loaded.required_commands, ["python -m edinet_monitor.cli.save_derived_metrics --run-all"])
        self.assertEqual(loaded.verification_sql, ["SELECT COUNT(*) FROM derived_metrics"])
        self.assertTrue(complete_db_reflection_item(self.conn, item.item_id))
        self.assertEqual(list_db_reflection_items(self.conn), [])

    def test_import_txt_registers_only_pending_blocks_and_is_idempotent(self) -> None:
        source_path = self.tmp_path / "db_reflection_ready_items.txt"
        source_path.write_text(
            """
================================================================================
[DB反映待ち] 反映済み項目
追加日: 2026-05-20
状態: 反映済み

内容:
- skip
================================================================================

================================================================================
[DB反映待ち] data_quality_report / schema_migrations 追加
追加日: 2026-05-25
状態: 未反映

DBスキーマ変更:
- 003_add_db_reflection_items
================================================================================
""".strip()
            + "\n",
            encoding="utf-8",
        )

        first = import_db_reflection_items_from_txt(self.conn, path=source_path)
        second = import_db_reflection_items_from_txt(self.conn, path=source_path)

        self.assertEqual(first.imported_count, 1)
        self.assertEqual(first.skipped_count, 0)
        self.assertEqual(second.imported_count, 0)
        self.assertEqual(second.skipped_count, 1)
        items = list_db_reflection_items(self.conn)
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0].title, "data_quality_report / schema_migrations 追加")
        self.assertEqual(items[0].related_migration_ids, ["003_add_db_reflection_items"])

    def test_update_replaces_pending_item_metadata(self) -> None:
        item = add_db_reflection_item(
            self.conn,
            title="Old title",
            category="recalculation",
            required_commands=["old command"],
            verification_sql=["SELECT 1"],
        )

        updated = update_db_reflection_item(
            self.conn,
            item.item_id,
            title="New title",
            required_commands=["new command"],
            verification_sql=["SELECT COUNT(*) AS target_count FROM derived_metrics"],
            notes="target_count=1",
        )

        self.assertIsNotNone(updated)
        assert updated is not None
        self.assertEqual(updated.title, "New title")
        self.assertEqual(updated.required_commands, ["new command"])
        self.assertEqual(updated.notes, "target_count=1")


if __name__ == "__main__":
    unittest.main()
