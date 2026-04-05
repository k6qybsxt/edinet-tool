from __future__ import annotations

import shutil
import sys
import unittest
import uuid
import zipfile
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from edinet_monitor.cli.download_manifest_zips import (  # noqa: E402
    resolve_manifest_path,
    run_download_manifest_zips,
)
from edinet_monitor.services.collector.manifest_download_service import (  # noqa: E402
    process_manifest_download_row,
    select_manifest_row_indexes,
)
from edinet_monitor.services.storage.manifest_service import (  # noqa: E402
    read_manifest_rows,
    write_manifest_rows,
)


def make_tempdir() -> Path:
    base_dir = ROOT_DIR / "tests" / "_tmp_edinet_monitor"
    base_dir.mkdir(parents=True, exist_ok=True)
    temp_dir = base_dir / f"case_{uuid.uuid4().hex}"
    temp_dir.mkdir()
    return temp_dir


def create_zip_file(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(path, "w") as zf:
        zf.writestr("sample.txt", "ok")


def build_manifest_row(doc_id: str, *, zip_path: str, download_status: str = "pending") -> dict[str, object]:
    return {
        "doc_id": doc_id,
        "edinet_code": "E00001",
        "security_code": "12340",
        "company_name": "テスト株式会社",
        "submit_date": "2026-04-01 09:00",
        "source_date": "2026-04-01",
        "zip_path": zip_path,
        "download_status": download_status,
        "download_attempts": 0,
    }


class DownloadManifestZipsTest(unittest.TestCase):
    def test_select_manifest_row_indexes_respects_retry_errors(self) -> None:
        rows = [
            build_manifest_row("S100AAAA", zip_path=r"D:\dummy\a.zip", download_status="pending"),
            build_manifest_row("S100BBBB", zip_path=r"D:\dummy\b.zip", download_status="error"),
            build_manifest_row("S100CCCC", zip_path=r"D:\dummy\c.zip", download_status="downloaded"),
        ]

        pending_only = select_manifest_row_indexes(rows, limit=10, retry_errors=False)
        with_retry = select_manifest_row_indexes(rows, limit=10, retry_errors=True)

        self.assertEqual(pending_only, [0])
        self.assertEqual(with_retry, [0, 1])

    def test_process_manifest_download_row_reuses_existing_zip(self) -> None:
        tmpdir = make_tempdir()
        self.addCleanup(shutil.rmtree, tmpdir, True)
        zip_path = tmpdir / "2026-04-01" / "S100AAAA.zip"
        create_zip_file(zip_path)

        row = build_manifest_row("S100AAAA", zip_path=str(zip_path))

        def fail_downloader(**_: object) -> Path:
            raise AssertionError("downloader should not be called when zip already exists")

        result = process_manifest_download_row(
            row,
            api_key="dummy-key",
            downloader=fail_downloader,
        )

        self.assertEqual(result["result"], "existing")
        self.assertEqual(row["download_status"], "downloaded")
        self.assertEqual(row["download_note"], "existing_file")
        self.assertEqual(row["download_attempts"], 1)

    def test_run_download_manifest_zips_updates_manifest_rows(self) -> None:
        tmpdir = make_tempdir()
        self.addCleanup(shutil.rmtree, tmpdir, True)
        manifest_path = tmpdir / "document_manifest.jsonl"
        zip_path = tmpdir / "raw" / "2026-04-01" / "S100AAAA.zip"

        rows = [build_manifest_row("S100AAAA", zip_path=str(zip_path))]
        write_manifest_rows(manifest_path, rows)

        def fake_downloader(*, doc_id: str, api_key: str, output_path: Path, timeout_sec: int = 30) -> Path:
            self.assertEqual(doc_id, "S100AAAA")
            self.assertEqual(api_key, "dummy-key")
            self.assertEqual(timeout_sec, 30)
            create_zip_file(output_path)
            return output_path

        summary = run_download_manifest_zips(
            api_key="dummy-key",
            manifest_path=manifest_path,
            batch_size=20,
            run_all=True,
            downloader=fake_downloader,
        )

        saved_rows = read_manifest_rows(manifest_path)

        self.assertEqual(summary["downloaded_total"], 1)
        self.assertEqual(summary["existing_total"], 0)
        self.assertEqual(summary["error_total"], 0)
        self.assertEqual(saved_rows[0]["download_status"], "downloaded")
        self.assertEqual(saved_rows[0]["download_note"], "downloaded")
        self.assertEqual(saved_rows[0]["download_attempts"], 1)
        self.assertTrue(Path(saved_rows[0]["zip_path"]).exists())

    def test_resolve_manifest_path_prefers_explicit_path(self) -> None:
        explicit = resolve_manifest_path(
            manifest_name="ignored_name",
            manifest_path_text=r"C:\tmp\manifest.jsonl",
        )

        self.assertEqual(str(explicit), r"C:\tmp\manifest.jsonl")


if __name__ == "__main__":
    unittest.main()
