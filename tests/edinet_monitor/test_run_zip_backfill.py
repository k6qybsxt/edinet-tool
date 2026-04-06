from __future__ import annotations

import json
import shutil
import sys
import unittest
import uuid
from datetime import date, datetime
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from edinet_monitor.cli.run_zip_backfill import (  # noqa: E402
    AUTO_DOWNLOAD_PEAK_THRESHOLD,
    build_month_manifest_name,
    iter_manifest_chunks,
    iter_month_chunks,
    resolve_effective_download_profile,
    resolve_manifest_granularity,
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

    def test_iter_manifest_chunks_supports_day_granularity(self) -> None:
        chunks = iter_manifest_chunks(date(2026, 4, 1), date(2026, 4, 3), granularity="day")

        self.assertEqual([chunk.chunk_key for chunk in chunks], ["2026-04-01", "2026-04-02", "2026-04-03"])
        self.assertTrue(all(chunk.granularity == "day" for chunk in chunks))

    def test_resolve_manifest_granularity_uses_profile_default_when_empty(self) -> None:
        self.assertEqual(resolve_manifest_granularity(manifest_granularity="", download_profile="peak"), "day")
        self.assertEqual(resolve_manifest_granularity(manifest_granularity="month", download_profile="peak"), "month")
        self.assertEqual(resolve_manifest_granularity(manifest_granularity="", download_profile="auto"), "month")

    def test_resolve_effective_download_profile_uses_threshold_for_auto(self) -> None:
        self.assertEqual(
            resolve_effective_download_profile(
                requested_profile="auto",
                manifest_rows=AUTO_DOWNLOAD_PEAK_THRESHOLD - 1,
                auto_peak_threshold=AUTO_DOWNLOAD_PEAK_THRESHOLD,
            ),
            "normal",
        )
        self.assertEqual(
            resolve_effective_download_profile(
                requested_profile="auto",
                manifest_rows=AUTO_DOWNLOAD_PEAK_THRESHOLD,
                auto_peak_threshold=AUTO_DOWNLOAD_PEAK_THRESHOLD,
            ),
            "peak",
        )
        self.assertEqual(
            resolve_effective_download_profile(
                requested_profile="normal",
                manifest_rows=999,
                auto_peak_threshold=AUTO_DOWNLOAD_PEAK_THRESHOLD,
            ),
            "normal",
        )

    def test_run_zip_backfill_prepare_only_creates_monthly_manifests(self) -> None:
        tmpdir = make_tempdir()
        self.addCleanup(shutil.rmtree, tmpdir, True)
        manifest_calls: list[tuple[str, Path]] = []
        manifest_path_builder = lambda manifest_name: tmpdir / f"{manifest_name}.jsonl"
        run_log_path = tmpdir / "zip_backfill_runs.jsonl"
        chunk_log_path = tmpdir / "zip_backfill_chunk_runs.jsonl"

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
            run_log_path=run_log_path,
            chunk_log_path=chunk_log_path,
            ensure_dirs_func=lambda: None,
        )

        self.assertEqual(summary["months"], 2)
        self.assertEqual(len(manifest_calls), 2)
        self.assertEqual(summary["downloaded_total"], 0)
        self.assertEqual(summary["error_total"], 0)
        self.assertTrue(run_log_path.exists())
        self.assertTrue(chunk_log_path.exists())

    def test_run_zip_backfill_writes_timing_logs(self) -> None:
        tmpdir = make_tempdir()
        self.addCleanup(shutil.rmtree, tmpdir, True)
        run_log_path = tmpdir / "zip_backfill_runs.jsonl"
        chunk_log_path = tmpdir / "zip_backfill_chunk_runs.jsonl"

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
            write_manifest_rows(
                manifest_path,
                [
                    {
                        "doc_id": "S100AAAA",
                        "company_name": "テスト株式会社",
                        "submit_date": f"{target_dates[0].isoformat()} 09:00",
                        "download_status": "pending",
                    }
                ],
            )
            return {"manifest_path": str(manifest_path), "saved_manifest_rows": 1, "totals": {"incoming_manifest_rows": 1}}

        def fake_download(**_: object) -> dict[str, object]:
            return {
                "downloaded_total": 1,
                "existing_total": 0,
                "error_total": 0,
                "manifest_rows": 1,
                "target_total": 1,
                "processed_total": 1,
                "cooldown_count": 0,
                "download_elapsed_seconds": 12.5,
                "retry_wait_elapsed_seconds": 1.25,
                "cooldown_elapsed_seconds": 0.0,
                "error_type_totals": {},
            }

        timestamp_values = iter(
            [
                datetime(2026, 4, 6, 12, 0, 0),
                datetime(2026, 4, 6, 12, 0, 5),
                datetime(2026, 4, 6, 12, 1, 0),
                datetime(2026, 4, 6, 12, 1, 30),
            ]
        )
        timer_values = iter([10.0, 20.0, 55.0, 100.0])

        summary = run_zip_backfill(
            api_key="dummy-key",
            start_date=date(2026, 4, 1),
            end_date=date(2026, 4, 1),
            manifest_prefix="document_manifest",
            master_csv_path=tmpdir / "issuer_master.csv",
            collect_func=fake_collect,
            download_func=fake_download,
            allowed_codes_loader=lambda _: {"E00001"},
            manifest_path_builder=lambda manifest_name: tmpdir / f"{manifest_name}.jsonl",
            run_log_path=run_log_path,
            chunk_log_path=chunk_log_path,
            timestamp_now_func=lambda: next(timestamp_values),
            timer_func=lambda: next(timer_values),
            ensure_dirs_func=lambda: None,
        )

        self.assertEqual(summary["started_at"], "2026-04-06 12:00:00")
        self.assertEqual(summary["finished_at"], "2026-04-06 12:01:30")
        self.assertEqual(summary["elapsed_seconds"], 90.0)
        self.assertEqual(summary["download_elapsed_seconds"], 12.5)
        self.assertEqual(summary["retry_wait_elapsed_seconds"], 1.25)
        self.assertEqual(summary["cooldown_elapsed_seconds"], 0.0)
        self.assertEqual(summary["run_id"].startswith("backfill_20260406_120000_"), True)
        self.assertTrue(run_log_path.exists())
        self.assertTrue(chunk_log_path.exists())

        with run_log_path.open("r", encoding="utf-8") as f:
            records = [json.loads(line) for line in f if line.strip()]

        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["started_at"], "2026-04-06 12:00:00")
        self.assertEqual(records[0]["finished_at"], "2026-04-06 12:01:30")
        self.assertEqual(records[0]["elapsed_seconds"], 90.0)
        self.assertEqual(records[0]["run_status"], "completed")
        self.assertEqual(records[0]["download_elapsed_seconds"], 12.5)
        self.assertEqual(records[0]["retry_wait_elapsed_seconds"], 1.25)
        self.assertEqual(records[0]["cooldown_elapsed_seconds"], 0.0)

        with chunk_log_path.open("r", encoding="utf-8") as f:
            chunk_records = [json.loads(line) for line in f if line.strip()]

        self.assertEqual(len(chunk_records), 1)
        self.assertEqual(chunk_records[0]["chunk_key"], "2026-04")
        self.assertEqual(chunk_records[0]["started_at"], "2026-04-06 12:00:05")
        self.assertEqual(chunk_records[0]["finished_at"], "2026-04-06 12:01:00")
        self.assertEqual(chunk_records[0]["elapsed_seconds"], 35.0)
        self.assertEqual(chunk_records[0]["chunk_status"], "completed")
        self.assertEqual(chunk_records[0]["manifest_rows"], 1)
        self.assertEqual(chunk_records[0]["run_id"], records[0]["run_id"])
        self.assertEqual(chunk_records[0]["download_elapsed_seconds"], 12.5)
        self.assertEqual(chunk_records[0]["retry_wait_elapsed_seconds"], 1.25)
        self.assertEqual(chunk_records[0]["cooldown_elapsed_seconds"], 0.0)
        self.assertEqual(summary["monthly_results"][0]["chunk_key"], "2026-04")
        self.assertEqual(summary["monthly_results"][0]["chunk_status"], "completed")

    def test_run_zip_backfill_reuses_existing_manifest_without_overwrite(self) -> None:
        tmpdir = make_tempdir()
        self.addCleanup(shutil.rmtree, tmpdir, True)
        manifest_path = tmpdir / "document_manifest_2026-03.jsonl"
        manifest_path_builder = lambda manifest_name: tmpdir / f"{manifest_name}.jsonl"
        run_log_path = tmpdir / "zip_backfill_runs.jsonl"
        chunk_log_path = tmpdir / "zip_backfill_chunk_runs.jsonl"
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
            run_log_path=run_log_path,
            chunk_log_path=chunk_log_path,
            ensure_dirs_func=lambda: None,
        )

        self.assertFalse(collect_called)
        self.assertEqual(summary["months"], 1)
        self.assertEqual(summary["manifest_rows_total"], 1)

    def test_run_zip_backfill_peak_profile_defaults_to_day_granularity(self) -> None:
        tmpdir = make_tempdir()
        self.addCleanup(shutil.rmtree, tmpdir, True)
        manifest_names: list[str] = []
        run_log_path = tmpdir / "zip_backfill_runs.jsonl"
        chunk_log_path = tmpdir / "zip_backfill_chunk_runs.jsonl"

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
            manifest_names.append(manifest_path.stem)
            write_manifest_rows(
                manifest_path,
                [
                    {
                        "doc_id": "S100AAAA",
                        "company_name": "テスト株式会社",
                        "submit_date": f"{target_dates[0].isoformat()} 15:30",
                        "download_status": "pending",
                    }
                ],
            )
            return {"manifest_path": str(manifest_path), "saved_manifest_rows": 1, "totals": {"incoming_manifest_rows": 1}}

        def fake_download(**_: object) -> dict[str, object]:
            return {
                "downloaded_total": 0,
                "existing_total": 0,
                "error_total": 0,
                "manifest_rows": 1,
                "target_total": 0,
                "processed_total": 0,
                "cooldown_count": 0,
                "error_type_totals": {},
            }

        run_zip_backfill(
            api_key="dummy-key",
            start_date=date(2026, 4, 1),
            end_date=date(2026, 4, 2),
            manifest_prefix="document_manifest",
            manifest_granularity="",
            master_csv_path=tmpdir / "issuer_master.csv",
            download_profile="peak",
            collect_func=fake_collect,
            download_func=fake_download,
            allowed_codes_loader=lambda _: {"E00001"},
            manifest_path_builder=lambda manifest_name: tmpdir / f"{manifest_name}.jsonl",
            run_log_path=run_log_path,
            chunk_log_path=chunk_log_path,
            ensure_dirs_func=lambda: None,
        )

        self.assertEqual(manifest_names, ["document_manifest_2026-04-01", "document_manifest_2026-04-02"])

    def test_run_zip_backfill_auto_profile_uses_peak_when_manifest_rows_hit_threshold(self) -> None:
        tmpdir = make_tempdir()
        self.addCleanup(shutil.rmtree, tmpdir, True)
        observed_profiles: list[tuple[int, int]] = []
        run_log_path = tmpdir / "zip_backfill_runs.jsonl"
        chunk_log_path = tmpdir / "zip_backfill_chunk_runs.jsonl"

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
            row_count = 3 if target_dates[0].month == 1 else 1
            rows = [
                {
                    "doc_id": f"S100{target_dates[0].month:02d}{index:02d}",
                    "company_name": "テスト株式会社",
                    "submit_date": f"{target_dates[0].isoformat()} 09:00",
                    "download_status": "pending",
                }
                for index in range(row_count)
            ]
            write_manifest_rows(manifest_path, rows)
            return {"manifest_path": str(manifest_path), "saved_manifest_rows": row_count, "totals": {"incoming_manifest_rows": row_count}}

        def fake_download(**kwargs: object) -> dict[str, object]:
            observed_profiles.append((int(kwargs["batch_size"]), int(kwargs["read_timeout_sec"])))
            return {
                "downloaded_total": 0,
                "existing_total": 0,
                "error_total": 0,
                "manifest_rows": 0,
                "target_total": 0,
                "processed_total": 0,
                "cooldown_count": 0,
                "error_type_totals": {},
            }

        summary = run_zip_backfill(
            api_key="dummy-key",
            start_date=date(2026, 1, 1),
            end_date=date(2026, 2, 1),
            manifest_prefix="document_manifest",
            master_csv_path=tmpdir / "issuer_master.csv",
            download_profile="auto",
            download_auto_peak_threshold=2,
            collect_func=fake_collect,
            download_func=fake_download,
            allowed_codes_loader=lambda _: {"E00001"},
            manifest_path_builder=lambda manifest_name: tmpdir / f"{manifest_name}.jsonl",
            run_log_path=run_log_path,
            chunk_log_path=chunk_log_path,
            ensure_dirs_func=lambda: None,
        )

        self.assertEqual(len(observed_profiles), 2)
        january_batch_size, january_read_timeout = observed_profiles[0]
        february_batch_size, february_read_timeout = observed_profiles[1]
        self.assertEqual(january_batch_size, 10)
        self.assertEqual(february_batch_size, 20)
        self.assertGreater(january_read_timeout, february_read_timeout)
        self.assertEqual(summary["effective_profile_totals"], {"normal": 1, "peak": 1})


if __name__ == "__main__":
    unittest.main()
