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

from edinet_monitor.cli.run_zip_backfill import (  # noqa: E402
    build_month_manifest_name,
    iter_month_chunks,
    run_zip_backfill,
)
from edinet_monitor.services.storage.manifest_service import write_manifest_rows  # noqa: E402


def make_tempdir() -> Path:
    base_dir = ROOT_DIR / "tests" / "_tmp_edinet_monitor"
    base_dir.mkdir(parents=True, exist_ok=True)
    temp_dir = base_dir / f"case_{uuid.uuid4().hex}"
    temp_dir.mkdir()
    return temp_dir


class RunZipBackfillTest(unittest.TestCase):
    def test_iter_month_chunks_splits_range_by_calendar_month(self) -> None:
        chunks = iter_month_chunks(date(2026, 1, 30), date(2026, 3, 2))

        self.assertEqual([chunk.month_key for chunk in chunks], ["2026-01", "2026-02", "2026-03"])
        self.assertEqual(chunks[0].start_date, date(2026, 1, 30))
        self.assertEqual(chunks[0].end_date, date(2026, 1, 31))
        self.assertEqual(chunks[1].start_date, date(2026, 2, 1))
        self.assertEqual(chunks[1].end_date, date(2026, 2, 28))
        self.assertEqual(chunks[2].start_date, date(2026, 3, 1))
        self.assertEqual(chunks[2].end_date, date(2026, 3, 2))

    def test_build_month_manifest_name_appends_month_key(self) -> None:
        self.assertEqual(
            build_month_manifest_name("document_manifest", "2026-03"),
            "document_manifest_2026-03",
        )

    def test_run_zip_backfill_prepare_only_creates_monthly_manifests(self) -> None:
        tmpdir = make_tempdir()
        self.addCleanup(shutil.rmtree, tmpdir, True)
        manifest_calls: list[tuple[str, Path]] = []
        manifest_path_builder = lambda manifest_name: tmpdir / f"{manifest_name}.jsonl"

        def fake_collect(
            target_dates: list[date],
            *,
            api_key: str,
            allowed_edinet_codes: set[str],
            manifest_path: Path,
            append: bool = False,
            overwrite: bool = False,
            fetcher=None,
        ) -> dict[str, object]:
            manifest_calls.append((target_dates[0].isoformat(), manifest_path))
            write_manifest_rows(
                manifest_path,
                [
                    {
                        "doc_id": f"S100{target_dates[0].month:02d}",
                        "company_name": "テスト株式会社",
                        "submit_date": f"{target_dates[0].isoformat()} 09:00",
                        "download_status": "pending",
                    }
                ],
            )
            return {
                "manifest_path": str(manifest_path),
                "totals": {"incoming_manifest_rows": 1},
                "saved_manifest_rows": 1,
            }

        def fail_download(**_: object) -> dict[str, object]:
            raise AssertionError("prepare_only should skip download")

        summary = run_zip_backfill(
            api_key="dummy-key",
            start_date=date(2026, 1, 15),
            end_date=date(2026, 2, 5),
            manifest_prefix="document_manifest",
            master_csv_path=tmpdir / "issuer_master.csv",
            prepare_only=True,
            collect_func=fake_collect,
            download_func=fail_download,
            allowed_codes_loader=lambda _: {"E00001"},
            manifest_path_builder=manifest_path_builder,
            ensure_dirs_func=lambda: None,
        )

        self.assertEqual(summary["months"], 2)
        self.assertEqual(len(manifest_calls), 2)
        self.assertEqual(summary["downloaded_total"], 0)
        self.assertEqual(summary["error_total"], 0)

    def test_run_zip_backfill_reuses_existing_manifest_without_overwrite(self) -> None:
        tmpdir = make_tempdir()
        self.addCleanup(shutil.rmtree, tmpdir, True)
        manifest_path = tmpdir / "document_manifest_2026-03.jsonl"
        manifest_path_builder = lambda manifest_name: tmpdir / f"{manifest_name}.jsonl"
        write_manifest_rows(
            manifest_path,
            [
                {
                    "doc_id": "S100AAAA",
                    "company_name": "テスト株式会社",
                    "submit_date": "2026-03-10 09:00",
                    "download_status": "pending",
                }
            ],
        )

        collect_called = False

        def fake_collect(**_: object) -> dict[str, object]:
            nonlocal collect_called
            collect_called = True
            return {}

        def fake_download(**_: object) -> dict[str, object]:
            return {
                "downloaded_total": 0,
                "existing_total": 0,
                "error_total": 0,
                "manifest_rows": 1,
                "target_total": 0,
                "processed_total": 0,
            }

        summary = run_zip_backfill(
            api_key="dummy-key",
            start_date=date(2026, 3, 1),
            end_date=date(2026, 3, 31),
            manifest_prefix="document_manifest",
            master_csv_path=tmpdir / "issuer_master.csv",
            prepare_only=False,
            collect_func=fake_collect,
            download_func=fake_download,
            allowed_codes_loader=lambda _: {"E00001"},
            manifest_path_builder=manifest_path_builder,
            ensure_dirs_func=lambda: None,
        )

        self.assertFalse(collect_called)
        self.assertEqual(summary["months"], 1)
        self.assertEqual(summary["manifest_rows_total"], 1)


if __name__ == "__main__":
    unittest.main()
