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
    resolve_download_runtime_settings,
    resolve_manifest_path,
    resolve_submit_filters,
    run_download_manifest_zips,
)
from edinet_monitor.services.collector.document_download_service import (  # noqa: E402
    DownloadDocumentZipError,
)
from edinet_monitor.services.collector.manifest_download_service import (  # noqa: E402
    matches_manifest_row_submit_filter,
    process_manifest_download_row,
    select_manifest_row_indexes,
)
from edinet_monitor.services.storage.manifest_service import (  # noqa: E402
    read_manifest_rows,
    summarize_manifest_rows,
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

    def test_select_manifest_row_indexes_respects_submit_date_filter(self) -> None:
        rows = [
            build_manifest_row("S100AAAA", zip_path=r"D:\dummy\a.zip"),
            build_manifest_row(
                "S100BBBB",
                zip_path=r"D:\dummy\b.zip",
            ),
        ]
        rows[1]["submit_date"] = "2026-04-02 09:00"

        filtered = select_manifest_row_indexes(
            rows,
            limit=10,
            target_date_text="2026-04-02",
        )

        self.assertEqual(filtered, [1])

    def test_select_manifest_row_indexes_respects_submit_time_filter(self) -> None:
        rows = [
            build_manifest_row("S100AAAA", zip_path=r"D:\dummy\a.zip"),
            build_manifest_row("S100BBBB", zip_path=r"D:\dummy\b.zip"),
        ]
        rows[0]["submit_date"] = "2026-04-01 14:59"
        rows[1]["submit_date"] = "2026-04-01 15:01"

        filtered = select_manifest_row_indexes(
            rows,
            limit=10,
            time_from_text="15:00",
            time_to_text="18:00",
        )

        self.assertEqual(filtered, [1])

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

    def test_process_manifest_download_row_retries_once_then_succeeds(self) -> None:
        tmpdir = make_tempdir()
        self.addCleanup(shutil.rmtree, tmpdir, True)
        zip_path = tmpdir / "2026-04-01" / "S100AAAA.zip"
        row = build_manifest_row("S100AAAA", zip_path=str(zip_path))
        call_count = {"value": 0}
        timer_values = iter([0.0, 1.0, 1.5, 3.0, 4.0, 6.0])

        def flaky_downloader(
            *,
            doc_id: str,
            api_key: str,
            output_path: Path,
            connect_timeout_sec: int,
            read_timeout_sec: int,
        ) -> Path:
            call_count["value"] += 1
            if call_count["value"] == 1:
                raise DownloadDocumentZipError("timeout", retryable=True)
            create_zip_file(output_path)
            return output_path

        result = process_manifest_download_row(
            row,
            api_key="dummy-key",
            downloader=flaky_downloader,
            max_retries=1,
            retry_wait_sec=1.0,
            sleep_func=lambda _: None,
            timer_func=lambda: next(timer_values),
        )

        self.assertEqual(result["result"], "downloaded")
        self.assertEqual(result["attempts_used"], 2)
        self.assertEqual(result["download_elapsed_seconds"], 3.0)
        self.assertEqual(result["retry_wait_elapsed_seconds"], 1.5)
        self.assertEqual(row["download_status"], "downloaded")
        self.assertEqual(row["download_attempts"], 2)
        self.assertEqual(call_count["value"], 2)

    def test_process_manifest_download_row_classifies_error(self) -> None:
        tmpdir = make_tempdir()
        self.addCleanup(shutil.rmtree, tmpdir, True)
        zip_path = tmpdir / "2026-04-01" / "S100AAAA.zip"
        row = build_manifest_row("S100AAAA", zip_path=str(zip_path))

        def failing_downloader(**_: object) -> Path:
            raise DownloadDocumentZipError("http_error", retryable=False, status_code=404)

        result = process_manifest_download_row(
            row,
            api_key="dummy-key",
            downloader=failing_downloader,
            max_retries=1,
            retry_wait_sec=0,
        )

        self.assertEqual(result["result"], "error")
        self.assertEqual(result["error_type"], "http_error")
        self.assertFalse(result["retryable"])
        self.assertEqual(result["status_code"], 404)
        self.assertEqual(row["download_error_type"], "http_error")
        self.assertEqual(row["download_http_status"], 404)
        self.assertFalse(result["cooldown_eligible"])

    def test_run_download_manifest_zips_updates_manifest_rows(self) -> None:
        tmpdir = make_tempdir()
        self.addCleanup(shutil.rmtree, tmpdir, True)
        manifest_path = tmpdir / "document_manifest.jsonl"
        zip_path = tmpdir / "raw" / "2026-04-01" / "S100AAAA.zip"

        rows = [build_manifest_row("S100AAAA", zip_path=str(zip_path))]
        write_manifest_rows(manifest_path, rows)

        def fake_downloader(
            *,
            doc_id: str,
            api_key: str,
            output_path: Path,
            connect_timeout_sec: int = 10,
            read_timeout_sec: int = 30,
        ) -> Path:
            self.assertEqual(doc_id, "S100AAAA")
            self.assertEqual(api_key, "dummy-key")
            self.assertEqual(connect_timeout_sec, 10)
            self.assertEqual(read_timeout_sec, 30)
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
        self.assertEqual(summary["initial_summary"]["pending_rows"], 1)
        self.assertEqual(summary["final_summary"]["downloaded_rows"], 1)

    def test_run_download_manifest_zips_filters_by_submit_date(self) -> None:
        tmpdir = make_tempdir()
        self.addCleanup(shutil.rmtree, tmpdir, True)
        manifest_path = tmpdir / "document_manifest.jsonl"
        zip_a = tmpdir / "raw" / "2026-04-01" / "S100AAAA.zip"
        zip_b = tmpdir / "raw" / "2026-04-02" / "S100BBBB.zip"

        rows = [
            build_manifest_row("S100AAAA", zip_path=str(zip_a)),
            build_manifest_row("S100BBBB", zip_path=str(zip_b)),
        ]
        rows[1]["submit_date"] = "2026-04-02 09:00"
        write_manifest_rows(manifest_path, rows)

        def fake_downloader(
            *,
            doc_id: str,
            api_key: str,
            output_path: Path,
            connect_timeout_sec: int = 10,
            read_timeout_sec: int = 30,
        ) -> Path:
            create_zip_file(output_path)
            return output_path

        summary = run_download_manifest_zips(
            api_key="dummy-key",
            manifest_path=manifest_path,
            batch_size=20,
            run_all=True,
            submit_date_text="2026-04-02",
            downloader=fake_downloader,
            progress_every=0,
        )

        saved_rows = read_manifest_rows(manifest_path)

        self.assertEqual(summary["processed_total"], 1)
        self.assertFalse(Path(saved_rows[0]["zip_path"]).exists())
        self.assertTrue(Path(saved_rows[1]["zip_path"]).exists())
        self.assertEqual(saved_rows[0]["download_status"], "pending")
        self.assertEqual(saved_rows[1]["download_status"], "downloaded")

    def test_run_download_manifest_zips_applies_cooldown_after_consecutive_retryable_errors(self) -> None:
        tmpdir = make_tempdir()
        self.addCleanup(shutil.rmtree, tmpdir, True)
        manifest_path = tmpdir / "document_manifest.jsonl"
        rows = [
            build_manifest_row("S100AAAA", zip_path=str(tmpdir / "raw" / "2026-04-01" / "S100AAAA.zip")),
            build_manifest_row("S100BBBB", zip_path=str(tmpdir / "raw" / "2026-04-01" / "S100BBBB.zip")),
        ]
        write_manifest_rows(manifest_path, rows)
        cooldown_calls: list[float] = []
        timer_values = iter([0.0, 2.0, 5.0, 8.0, 10.0, 14.0])

        def failing_downloader(**_: object) -> Path:
            raise DownloadDocumentZipError("timeout", retryable=True)

        summary = run_download_manifest_zips(
            api_key="dummy-key",
            manifest_path=manifest_path,
            batch_size=10,
            run_all=True,
            downloader=failing_downloader,
            max_retries=0,
            progress_every=0,
            cooldown_failure_streak=2,
            cooldown_sec=3.5,
            sleep_func=cooldown_calls.append,
            timer_func=lambda: next(timer_values),
        )

        self.assertEqual(summary["error_total"], 2)
        self.assertEqual(summary["cooldown_count"], 1)
        self.assertEqual(cooldown_calls[-1], 3.5)
        self.assertEqual(summary["error_type_totals"], {"timeout": 2})
        self.assertEqual(summary["download_elapsed_seconds"], 5.0)
        self.assertEqual(summary["retry_wait_elapsed_seconds"], 0.0)
        self.assertEqual(summary["cooldown_elapsed_seconds"], 4.0)

    def test_matches_manifest_row_submit_filter_combines_date_and_time(self) -> None:
        row = build_manifest_row("S100AAAA", zip_path=r"D:\dummy\a.zip")
        row["submit_date"] = "2026-04-04 15:30"

        self.assertTrue(
            matches_manifest_row_submit_filter(
                row,
                date_from_text="2026-04-01",
                date_to_text="2026-04-05",
                time_from_text="15:00",
                time_to_text="16:00",
            )
        )
        self.assertFalse(
            matches_manifest_row_submit_filter(
                row,
                date_from_text="2026-04-01",
                date_to_text="2026-04-05",
                time_from_text="16:00",
                time_to_text="17:00",
            )
        )

    def test_resolve_manifest_path_prefers_explicit_path(self) -> None:
        explicit = resolve_manifest_path(
            manifest_name="ignored_name",
            manifest_path_text=r"C:\tmp\manifest.jsonl",
        )

        self.assertEqual(str(explicit), r"C:\tmp\manifest.jsonl")

    def test_resolve_submit_filters_validates_inputs(self) -> None:
        self.assertEqual(
            resolve_submit_filters(
                submit_date_text="2026-04-01",
                submit_date_from_text="",
                submit_date_to_text="",
                submit_time_from_text="",
                submit_time_to_text="",
            ),
            ("2026-04-01", "", "", "", ""),
        )

        with self.assertRaises(ValueError):
            resolve_submit_filters(
                submit_date_text="2026-04-01",
                submit_date_from_text="2026-04-01",
                submit_date_to_text="2026-04-02",
                submit_time_from_text="",
                submit_time_to_text="",
            )

        with self.assertRaises(ValueError):
            resolve_submit_filters(
                submit_date_text="",
                submit_date_from_text="",
                submit_date_to_text="",
                submit_time_from_text="15:00",
                submit_time_to_text="",
            )

    def test_resolve_download_runtime_settings_uses_peak_defaults(self) -> None:
        settings = resolve_download_runtime_settings(
            profile_name="peak",
            batch_size=None,
            connect_timeout_sec=None,
            read_timeout_sec=None,
            max_retries=None,
            retry_wait_sec=None,
            progress_every=None,
            cooldown_failure_streak=None,
            cooldown_sec=None,
        )

        self.assertEqual(settings["profile_name"], "peak")
        self.assertEqual(settings["batch_size"], 10)
        self.assertEqual(settings["recommended_manifest_granularity"], "day")

    def test_summarize_manifest_rows_counts_error_types(self) -> None:
        summary = summarize_manifest_rows(
            [
                {"doc_id": "S1", "download_status": "error", "download_error_type": "timeout", "download_error_retryable": 1},
                {"doc_id": "S2", "download_status": "error", "download_error_type": "timeout", "download_error_retryable": 1},
                {"doc_id": "S3", "download_status": "error", "download_error_type": "http_error", "download_error_retryable": 0},
            ]
        )

        self.assertEqual(summary["error_rows"], 3)
        self.assertEqual(summary["retryable_error_rows"], 2)
        self.assertEqual(summary["error_type_counts"], {"http_error": 1, "timeout": 2})


if __name__ == "__main__":
    unittest.main()
