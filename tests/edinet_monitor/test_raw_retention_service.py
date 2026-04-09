from __future__ import annotations

import shutil
import sys
import unittest
import uuid
from datetime import date
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from edinet_monitor.services.storage.raw_retention_service import (  # noqa: E402
    cleanup_old_raw_storage,
    detect_latest_raw_month,
)


def make_tempdir() -> Path:
    base_dir = ROOT_DIR / "tests" / "_tmp_edinet_monitor"
    base_dir.mkdir(parents=True, exist_ok=True)
    temp_dir = base_dir / f"case_{uuid.uuid4().hex}"
    temp_dir.mkdir()
    return temp_dir


class RawRetentionServiceTest(unittest.TestCase):
    def test_detect_latest_raw_month_prefers_latest_available_month(self) -> None:
        tmpdir = make_tempdir()
        self.addCleanup(shutil.rmtree, tmpdir, True)
        zip_root = tmpdir / "zip"
        xbrl_root = tmpdir / "xbrl"
        manifest_root = tmpdir / "manifests"
        (zip_root / "2026-03-15").mkdir(parents=True)
        (xbrl_root / "2026-02-01").mkdir(parents=True)
        manifest_root.mkdir(parents=True)
        (manifest_root / "document_manifest_2026-04-01.jsonl").write_text("", encoding="utf-8")

        latest_month = detect_latest_raw_month(
            zip_root=zip_root,
            xbrl_root=xbrl_root,
            manifest_root=manifest_root,
        )

        self.assertEqual(latest_month, date(2026, 4, 1))

    def test_cleanup_old_raw_storage_deletes_only_months_older_than_keep_window(self) -> None:
        tmpdir = make_tempdir()
        self.addCleanup(shutil.rmtree, tmpdir, True)
        zip_root = tmpdir / "zip"
        xbrl_root = tmpdir / "xbrl"
        manifest_root = tmpdir / "manifests"

        (zip_root / "2016-03-31").mkdir(parents=True)
        (zip_root / "2016-04-01").mkdir(parents=True)
        (zip_root / "2026-03-15").mkdir(parents=True)
        (xbrl_root / "2016-03-31").mkdir(parents=True)
        (xbrl_root / "2016-04-01").mkdir(parents=True)
        manifest_root.mkdir(parents=True)
        (manifest_root / "document_manifest_2016-03.jsonl").write_text("", encoding="utf-8")
        (manifest_root / "document_manifest_2016-03-15.jsonl").write_text("", encoding="utf-8")
        (manifest_root / "document_manifest_2016-04.jsonl").write_text("", encoding="utf-8")
        (manifest_root / "document_manifest_2026-03-15.jsonl").write_text("", encoding="utf-8")

        summary = cleanup_old_raw_storage(
            latest_month=date(2026, 3, 1),
            keep_years=10,
            zip_root=zip_root,
            xbrl_root=xbrl_root,
            manifest_root=manifest_root,
        )

        self.assertEqual(summary["status"], "completed")
        self.assertEqual(summary["reference_month"], "2026-03")
        self.assertEqual(summary["keep_from_month"], "2016-04")
        self.assertEqual(summary["deleted_zip_dirs"], 1)
        self.assertEqual(summary["deleted_xbrl_dirs"], 1)
        self.assertEqual(summary["deleted_manifest_files"], 2)
        self.assertEqual(summary["deleted_total"], 4)
        self.assertFalse((zip_root / "2016-03-31").exists())
        self.assertFalse((xbrl_root / "2016-03-31").exists())
        self.assertFalse((manifest_root / "document_manifest_2016-03.jsonl").exists())
        self.assertFalse((manifest_root / "document_manifest_2016-03-15.jsonl").exists())
        self.assertTrue((zip_root / "2016-04-01").exists())
        self.assertTrue((xbrl_root / "2016-04-01").exists())
        self.assertTrue((manifest_root / "document_manifest_2016-04.jsonl").exists())

    def test_cleanup_old_raw_storage_skips_when_no_reference_month_found(self) -> None:
        tmpdir = make_tempdir()
        self.addCleanup(shutil.rmtree, tmpdir, True)

        summary = cleanup_old_raw_storage(
            zip_root=tmpdir / "zip",
            xbrl_root=tmpdir / "xbrl",
            manifest_root=tmpdir / "manifests",
        )

        self.assertEqual(summary["status"], "skipped")
        self.assertEqual(summary["reason"], "no_reference_month")
        self.assertEqual(summary["deleted_total"], 0)


if __name__ == "__main__":
    unittest.main()
