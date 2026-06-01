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

from edinet_monitor.services.storage.zip_extract_service import (  # noqa: E402
    choose_preferred_xbrl_member,
    extract_period_end_from_xbrl_member_name,
    extract_first_xbrl,
    extract_preferred_xbrl,
)


def make_tempdir() -> Path:
    base_dir = ROOT_DIR / "tests" / "_tmp_edinet_monitor"
    base_dir.mkdir(parents=True, exist_ok=True)
    temp_dir = base_dir / f"zip_extract_{uuid.uuid4().hex}"
    temp_dir.mkdir()
    return temp_dir


class ZipExtractServiceTest(unittest.TestCase):
    def test_choose_preferred_xbrl_member_prefers_public_doc_annual_report(self) -> None:
        members = [
            "XBRL/AuditDoc/audit.xbrl",
            "XBRL/PublicDoc/jpcrp040300-q3r-001_E00001-000_2026-03-31_01_2026-05-01.xbrl",
            "XBRL/PublicDoc/jpcrp030000-asr-001_E00001-000_2026-03-31_01_2026-06-28.xbrl",
        ]

        selected = choose_preferred_xbrl_member(members)

        self.assertEqual(
            selected,
            "XBRL/PublicDoc/jpcrp030000-asr-001_E00001-000_2026-03-31_01_2026-06-28.xbrl",
        )

    def test_choose_preferred_xbrl_member_prefers_half_report_when_form_type_is_half(self) -> None:
        members = [
            "XBRL/PublicDoc/jpcrp030000-asr-001_E00001-000_2026-03-31_01_2026-06-28.xbrl",
            "XBRL/PublicDoc/jpcrp040300-q2r-001_E00001-000_2026-09-30_01_2026-11-14.xbrl",
        ]

        selected = choose_preferred_xbrl_member(members, form_type="043A00")

        self.assertEqual(
            selected,
            "XBRL/PublicDoc/jpcrp040300-q2r-001_E00001-000_2026-09-30_01_2026-11-14.xbrl",
        )

    def test_extract_period_end_from_xbrl_member_name_reads_half_period_end(self) -> None:
        period_end = extract_period_end_from_xbrl_member_name(
            "XBRL/PublicDoc/jpcrp040300-q2r-001_E00001-000_2026-09-30_01_2026-11-14.xbrl"
        )

        self.assertEqual(period_end, "2026-09-30")

    def test_extract_preferred_xbrl_returns_member_name_and_writes_selected_content(self) -> None:
        tmpdir = make_tempdir()
        self.addCleanup(shutil.rmtree, tmpdir, True)
        zip_path = tmpdir / "sample.zip"
        output_path = tmpdir / "out.xbrl"
        output_path.write_text("old", encoding="utf-8")

        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("XBRL/AuditDoc/audit.xbrl", "audit")
            zf.writestr(
                "XBRL/PublicDoc/jpcrp030000-asr-001_E00001-000_2026-03-31_01_2026-06-28.xbrl",
                "annual",
            )

        result = extract_preferred_xbrl(zip_path, output_path)

        self.assertEqual(output_path.read_text(encoding="utf-8"), "annual")
        self.assertEqual(result.output_path, output_path)
        self.assertEqual(
            result.member_name,
            "XBRL/PublicDoc/jpcrp030000-asr-001_E00001-000_2026-03-31_01_2026-06-28.xbrl",
        )
        self.assertEqual(len(result.member_names), 2)

    def test_extract_preferred_xbrl_removes_temporary_file_on_error(self) -> None:
        tmpdir = make_tempdir()
        self.addCleanup(shutil.rmtree, tmpdir, True)
        zip_path = tmpdir / "sample.zip"
        output_path = tmpdir / "out.xbrl"
        output_path.write_text("old", encoding="utf-8")

        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("readme.txt", "xbrl missing")

        with self.assertRaisesRegex(RuntimeError, "xbrl not found"):
            extract_preferred_xbrl(zip_path, output_path)

        self.assertEqual(output_path.read_text(encoding="utf-8"), "old")
        self.assertEqual(list(tmpdir.glob("*.tmp")), [])

    def test_extract_first_xbrl_keeps_backward_compatible_return_value(self) -> None:
        tmpdir = make_tempdir()
        self.addCleanup(shutil.rmtree, tmpdir, True)
        zip_path = tmpdir / "sample.zip"
        output_path = tmpdir / "out.xbrl"

        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("XBRL/PublicDoc/jpcrp030000-asr-001_E00001-000_2026-03-31_01_2026-06-28.xbrl", "annual")

        result = extract_first_xbrl(zip_path, output_path)

        self.assertEqual(result, output_path)
        self.assertEqual(output_path.read_text(encoding="utf-8"), "annual")


if __name__ == "__main__":
    unittest.main()
