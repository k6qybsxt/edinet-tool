from __future__ import annotations

from io import BytesIO
import shutil
import sys
import unittest
from unittest.mock import patch
import uuid
import zipfile
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from edinet_monitor.services.collector.document_download_service import (  # noqa: E402
    DownloadDocumentZipError,
    download_document_zip,
)


class FakeResponse:
    def __init__(self, content: bytes) -> None:
        self.content = content
        self.headers = {"Content-Type": "application/zip"}
        self.status_code = 200
        self.text = ""

    def raise_for_status(self) -> None:
        pass


def make_tempdir() -> Path:
    base_dir = ROOT_DIR / "tests" / "_tmp_edinet_monitor"
    base_dir.mkdir(parents=True, exist_ok=True)
    temp_dir = base_dir / f"document_download_{uuid.uuid4().hex}"
    temp_dir.mkdir()
    return temp_dir


def zip_bytes(content: str) -> bytes:
    buffer = BytesIO()
    with zipfile.ZipFile(buffer, "w") as zf:
        zf.writestr("sample.txt", content)
    return buffer.getvalue()


class DocumentDownloadServiceTest(unittest.TestCase):
    def test_download_document_zip_replaces_output_after_validating_temporary_zip(self) -> None:
        tmpdir = make_tempdir()
        self.addCleanup(shutil.rmtree, tmpdir, True)
        output_path = tmpdir / "sample.zip"

        with patch(
            "edinet_monitor.services.collector.document_download_service.requests.get",
            return_value=FakeResponse(zip_bytes("new")),
        ):
            saved_path = download_document_zip("S100AAAA", "dummy-key", output_path)

        self.assertEqual(saved_path, output_path)
        with zipfile.ZipFile(output_path) as zf:
            self.assertEqual(zf.read("sample.txt"), b"new")
        self.assertEqual(list(tmpdir.glob("*.tmp")), [])

    def test_download_document_zip_preserves_existing_output_when_new_zip_is_invalid(self) -> None:
        tmpdir = make_tempdir()
        self.addCleanup(shutil.rmtree, tmpdir, True)
        output_path = tmpdir / "sample.zip"
        output_path.write_bytes(zip_bytes("old"))

        with (
            patch(
                "edinet_monitor.services.collector.document_download_service.requests.get",
                return_value=FakeResponse(b"PK-invalid"),
            ),
            self.assertRaisesRegex(DownloadDocumentZipError, "saved_zip_invalid"),
        ):
            download_document_zip("S100AAAA", "dummy-key", output_path)

        with zipfile.ZipFile(output_path) as zf:
            self.assertEqual(zf.read("sample.txt"), b"old")
        self.assertEqual(list(tmpdir.glob("*.tmp")), [])


if __name__ == "__main__":
    unittest.main()
