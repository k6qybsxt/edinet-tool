from __future__ import annotations

from contextlib import contextmanager
import shutil
import sqlite3
import sys
import threading
import time
import unittest
from unittest.mock import patch
import uuid
import zipfile
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from edinet_monitor.cli.download_filing_zips import (  # noqa: E402
    build_arg_parser,
    run_download_filing_zips,
)
from edinet_monitor.services.collector.document_download_service import DownloadDocumentZipError  # noqa: E402


class FakePerformanceLog:
    def __init__(self, **_: object) -> None:
        pass

    @contextmanager
    def measure(self, *_: object, **__: object):
        yield

    def finish(self, *_: object, **__: object) -> None:
        pass


def make_tempdir() -> Path:
    base_dir = ROOT_DIR / "tests" / "_tmp_edinet_monitor"
    base_dir.mkdir(parents=True, exist_ok=True)
    temp_dir = base_dir / f"download_parallel_{uuid.uuid4().hex}"
    temp_dir.mkdir()
    return temp_dir


def create_tables(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE issuer_master (
            edinet_code TEXT PRIMARY KEY,
            exchange TEXT,
            is_listed INTEGER NOT NULL
        );

        CREATE TABLE filings (
            doc_id TEXT PRIMARY KEY,
            edinet_code TEXT NOT NULL,
            security_code TEXT,
            form_type TEXT NOT NULL,
            submit_date TEXT,
            zip_path TEXT,
            download_status TEXT NOT NULL,
            parse_status TEXT NOT NULL
        );
        """
    )
    conn.execute(
        "INSERT INTO issuer_master (edinet_code, exchange, is_listed) VALUES ('E00001', 'TSE', 1)"
    )
    conn.commit()


def create_zip_file(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(path, "w") as zf:
        zf.writestr("sample.txt", "ok")


class DownloadFilingZipsParallelTest(unittest.TestCase):
    def test_workers_two_downloads_in_parallel_and_parent_updates_db(self) -> None:
        tmpdir = make_tempdir()
        self.addCleanup(shutil.rmtree, tmpdir, True)
        db_path = tmpdir / "case.db"
        setup_conn = sqlite3.connect(db_path)
        create_tables(setup_conn)
        setup_conn.executemany(
            """
            INSERT INTO filings (
                doc_id, edinet_code, security_code, form_type, submit_date,
                zip_path, download_status, parse_status
            )
            VALUES (?, 'E00001', '11110', '030000', '2026-06-01', '', 'pending', 'pending')
            """,
            [(f"S100{index:04d}",) for index in range(3)],
        )
        setup_conn.commit()
        setup_conn.close()
        lock = threading.Lock()
        active = 0
        max_active = 0

        def connection_factory() -> sqlite3.Connection:
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            return conn

        def fake_downloader(*, output_path: Path, **_: object) -> Path:
            nonlocal active
            nonlocal max_active
            with lock:
                active += 1
                max_active = max(max_active, active)
            time.sleep(0.02)
            create_zip_file(output_path)
            with lock:
                active -= 1
            return output_path

        with (
            patch("edinet_monitor.cli.download_filing_zips.get_connection", connection_factory),
            patch("edinet_monitor.cli.download_filing_zips.PerformanceLog", FakePerformanceLog),
            patch(
                "edinet_monitor.cli.download_filing_zips.build_zip_save_path",
                lambda _submit_date, doc_id: tmpdir / f"{doc_id}.zip",
            ),
        ):
            summary = run_download_filing_zips(
                api_key="dummy-key",
                batch_size=3,
                run_all=True,
                workers=2,
                downloader=fake_downloader,
            )

        verify_conn = connection_factory()
        self.addCleanup(verify_conn.close)
        statuses = [
            row["download_status"]
            for row in verify_conn.execute("SELECT download_status FROM filings ORDER BY doc_id").fetchall()
        ]
        self.assertEqual(statuses, ["downloaded", "downloaded", "downloaded"])
        self.assertEqual(summary["downloaded_total"], 3)
        self.assertEqual(summary["workers"], 2)
        self.assertEqual(summary["wave_count"], 2)
        self.assertEqual(max_active, 2)

    def test_retry_errors_run_all_attempts_failed_doc_once_per_run(self) -> None:
        tmpdir = make_tempdir()
        self.addCleanup(shutil.rmtree, tmpdir, True)
        db_path = tmpdir / "case.db"
        setup_conn = sqlite3.connect(db_path)
        create_tables(setup_conn)
        setup_conn.execute(
            """
            INSERT INTO filings (
                doc_id, edinet_code, security_code, form_type, submit_date,
                zip_path, download_status, parse_status
            )
            VALUES ('S100ERROR', 'E00001', '11110', '030000', '2026-06-01', '', 'error', 'pending')
            """
        )
        setup_conn.commit()
        setup_conn.close()
        calls: list[str] = []

        def connection_factory() -> sqlite3.Connection:
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            return conn

        def failing_downloader(*, doc_id: str, **_: object) -> Path:
            calls.append(doc_id)
            raise DownloadDocumentZipError("timeout", retryable=True)

        with (
            patch("edinet_monitor.cli.download_filing_zips.get_connection", connection_factory),
            patch("edinet_monitor.cli.download_filing_zips.PerformanceLog", FakePerformanceLog),
            patch(
                "edinet_monitor.cli.download_filing_zips.build_zip_save_path",
                lambda _submit_date, doc_id: tmpdir / f"{doc_id}.zip",
            ),
        ):
            summary = run_download_filing_zips(
                api_key="dummy-key",
                batch_size=1,
                run_all=True,
                retry_errors=True,
                max_retries=0,
                cooldown_failure_streak=0,
                downloader=failing_downloader,
            )

        self.assertEqual(calls, ["S100ERROR"])
        self.assertEqual(summary["target_total"], 1)
        self.assertEqual(summary["error_total"], 1)

    def test_parser_defaults_keep_serial_behavior(self) -> None:
        args = build_arg_parser().parse_args([])

        self.assertEqual(args.workers, 1)
        self.assertFalse(args.retry_errors)


if __name__ == "__main__":
    unittest.main()
