from __future__ import annotations

import shutil
import unittest
import uuid
from pathlib import Path

from edinet_monitor.cli.save_derived_metrics import _resolve_doc_ids as resolve_derived_doc_ids
from edinet_monitor.cli.save_normalized_metrics import _resolve_doc_ids as resolve_normalized_doc_ids
from edinet_monitor.cli.save_segment_metrics import _resolve_doc_ids as resolve_segment_doc_ids
from edinet_monitor.services.doc_id_file_service import load_doc_ids_file


ROOT_DIR = Path(__file__).resolve().parents[2]
TMP_ROOT = ROOT_DIR / "tests" / "_tmp_doc_id_file_service"


class DocIdFileServiceTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp_path = TMP_ROOT / uuid.uuid4().hex
        self.tmp_path.mkdir(parents=True, exist_ok=True)

    def tearDown(self) -> None:
        shutil.rmtree(self.tmp_path, ignore_errors=True)

    def test_load_doc_ids_file_ignores_comments_and_merges_with_cli_values(self) -> None:
        path = self.tmp_path / "doc_ids.txt"
        path.write_text("# target docs\nS100AAAA\n\nS100BBBB\nS100AAAA\n", encoding="utf-8")

        self.assertEqual(load_doc_ids_file(path), ("S100AAAA", "S100BBBB"))
        expected = ("S100CCCC", "S100AAAA", "S100BBBB")
        self.assertEqual(resolve_normalized_doc_ids("S100CCCC,S100AAAA", str(path)), expected)
        self.assertEqual(resolve_derived_doc_ids("S100CCCC,S100AAAA", str(path)), expected)
        self.assertEqual(resolve_segment_doc_ids("S100CCCC,S100AAAA", str(path)), expected)

    def test_load_doc_ids_file_raises_for_missing_file(self) -> None:
        with self.assertRaises(FileNotFoundError):
            load_doc_ids_file(Path("missing-doc-ids.txt"))
