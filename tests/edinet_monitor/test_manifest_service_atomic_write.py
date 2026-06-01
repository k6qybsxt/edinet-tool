from __future__ import annotations

import shutil
import sys
import unittest
import uuid
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from edinet_monitor.services.storage.manifest_service import write_manifest_rows  # noqa: E402


def make_tempdir() -> Path:
    base_dir = ROOT_DIR / "tests" / "_tmp_edinet_monitor"
    base_dir.mkdir(parents=True, exist_ok=True)
    temp_dir = base_dir / f"manifest_atomic_{uuid.uuid4().hex}"
    temp_dir.mkdir()
    return temp_dir


class ManifestServiceAtomicWriteTest(unittest.TestCase):
    def test_write_manifest_rows_preserves_existing_file_when_serialization_fails(self) -> None:
        tmpdir = make_tempdir()
        self.addCleanup(shutil.rmtree, tmpdir, True)
        manifest_path = tmpdir / "manifest.jsonl"
        manifest_path.write_text('{"doc_id":"old"}\n', encoding="utf-8")

        with self.assertRaises(TypeError):
            write_manifest_rows(manifest_path, [{"doc_id": object()}])

        self.assertEqual(manifest_path.read_text(encoding="utf-8"), '{"doc_id":"old"}\n')
        self.assertEqual(list(tmpdir.glob("*.tmp")), [])


if __name__ == "__main__":
    unittest.main()
