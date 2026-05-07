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

from edinet_monitor.services.edinet_download_progress_service import export_edinet_download_progress  # noqa: E402
from edinet_monitor.services.storage.manifest_service import write_manifest_rows  # noqa: E402


def make_tempdir() -> Path:
    base_dir = ROOT_DIR / "tests" / "_tmp_edinet_download_progress"
    base_dir.mkdir(parents=True, exist_ok=True)
    temp_dir = base_dir / f"case_{uuid.uuid4().hex}"
    temp_dir.mkdir()
    return temp_dir


class EdinetDownloadProgressServiceTest(unittest.TestCase):
    def test_export_edinet_download_progress_summarizes_manifest_chunks(self) -> None:
        tmpdir = make_tempdir()
        self.addCleanup(shutil.rmtree, tmpdir, True)
        manifest_dir = tmpdir / "manifests"
        output_dir = tmpdir / "out"
        manifest_dir.mkdir()
        write_manifest_rows(
            manifest_dir / "document_manifest_2026-04-01.jsonl",
            [
                {"doc_id": "S1", "download_status": "downloaded", "submit_date": "2026-04-01 09:00"},
                {"doc_id": "S2", "download_status": "pending", "submit_date": "2026-04-01 10:00"},
                {
                    "doc_id": "S3",
                    "download_status": "error",
                    "submit_date": "2026-04-01 11:00",
                    "download_error_type": "timeout",
                    "download_error_retryable": 1,
                },
            ],
        )

        result = export_edinet_download_progress(
            date_from="2026-04-01",
            date_to="2026-04-02",
            manifest_granularity="day",
            output_dir=output_dir,
            manifest_path_builder=lambda name: manifest_dir / f"{name}.jsonl",
        )

        self.assertEqual(len(result.chunks), 2)
        self.assertEqual(result.missing_manifest_chunks, 1)
        self.assertEqual(result.incomplete_chunks, 2)
        self.assertEqual(result.manifest_rows, 3)
        self.assertEqual(result.downloaded_rows, 1)
        self.assertEqual(result.pending_rows, 1)
        self.assertEqual(result.error_rows, 1)
        self.assertEqual(result.retryable_error_rows, 1)
        self.assertTrue(result.output_path and result.output_path.exists())
        report = result.output_path.read_text(encoding="utf-8-sig")
        self.assertIn("INCOMPLETE | 2026-04-01", report)
        self.assertIn("MANIFEST_MISSING | 2026-04-02", report)


if __name__ == "__main__":
    unittest.main()
