from __future__ import annotations

from contextlib import contextmanager
import shutil
import sqlite3
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

from edinet_monitor.cli.extract_xbrl_from_zips import (  # noqa: E402
    _run_extract_jobs,
    build_arg_parser,
    run_extract_xbrl_from_zips,
)


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
    temp_dir = base_dir / f"extract_parallel_{uuid.uuid4().hex}"
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
            form_type TEXT NOT NULL,
            submit_date TEXT,
            period_end TEXT,
            zip_path TEXT,
            xbrl_path TEXT,
            xbrl_member_name TEXT,
            download_status TEXT NOT NULL,
            parse_status TEXT NOT NULL
        );
        """
    )
    conn.commit()


def write_xbrl_zip(zip_path: Path, *, content: str = "annual") -> None:
    zip_path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr(
            "XBRL/PublicDoc/jpcrp030000-asr-001_E00001-000_2026-03-31_01_2026-06-28.xbrl",
            content,
        )


class ExtractXbrlFromZipsParallelTest(unittest.TestCase):
    def test_run_extract_jobs_workers_four_keeps_input_order(self) -> None:
        tmpdir = make_tempdir()
        self.addCleanup(shutil.rmtree, tmpdir, True)
        jobs = []
        for index in range(3):
            zip_path = tmpdir / f"sample_{index}.zip"
            write_xbrl_zip(zip_path, content=f"annual_{index}")
            jobs.append(
                {
                    "order_index": index,
                    "doc_id": f"S100A00{index}",
                    "form_type": "030000",
                    "zip_path": str(zip_path),
                    "xbrl_path": str(tmpdir / f"out_{index}.xbrl"),
                }
            )

        results, chunk_count, worker_elapsed = _run_extract_jobs(
            jobs,
            workers=4,
            extract_chunk_size=1,
        )

        self.assertEqual([row["doc_id"] for row in results], ["S100A000", "S100A001", "S100A002"])
        self.assertEqual(chunk_count, 3)
        self.assertGreaterEqual(worker_elapsed, 0.0)
        self.assertTrue(all(row["ok"] for row in results))
        self.assertEqual((tmpdir / "out_2.xbrl").read_text(encoding="utf-8"), "annual_2")

    def test_run_all_records_parent_db_updates_and_does_not_retry_failed_doc(self) -> None:
        tmpdir = make_tempdir()
        self.addCleanup(shutil.rmtree, tmpdir, True)
        db_path = tmpdir / "case.db"
        good_zip = tmpdir / "good.zip"
        bad_zip = tmpdir / "bad.zip"
        write_xbrl_zip(good_zip)
        with zipfile.ZipFile(bad_zip, "w") as zf:
            zf.writestr("readme.txt", "xbrl missing")

        setup_conn = sqlite3.connect(db_path)
        create_tables(setup_conn)
        setup_conn.execute(
            "INSERT INTO issuer_master (edinet_code, exchange, is_listed) VALUES ('E00001', 'TSE', 1)"
        )
        setup_conn.executemany(
            """
            INSERT INTO filings (
                doc_id, edinet_code, form_type, submit_date, period_end,
                zip_path, xbrl_path, xbrl_member_name, download_status, parse_status
            )
            VALUES (?, 'E00001', '030000', '2026-06-01', '', ?, '', '', 'downloaded', 'pending')
            """,
            [
                ("S100GOOD", str(good_zip)),
                ("S100BAD", str(bad_zip)),
            ],
        )
        setup_conn.commit()
        setup_conn.close()

        def connection_factory() -> sqlite3.Connection:
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            return conn

        with (
            patch("edinet_monitor.cli.extract_xbrl_from_zips.get_connection", connection_factory),
            patch("edinet_monitor.cli.extract_xbrl_from_zips.PerformanceLog", FakePerformanceLog),
            patch(
                "edinet_monitor.cli.extract_xbrl_from_zips.build_xbrl_save_path",
                lambda _submit_date, doc_id: tmpdir / f"{doc_id}.xbrl",
            ),
        ):
            summary = run_extract_xbrl_from_zips(batch_size=1, run_all=True, workers=4, extract_chunk_size=1)

        self.assertEqual(summary["target_total"], 2)
        self.assertEqual(summary["extracted_total"], 1)
        self.assertEqual(summary["error_total"], 1)
        verify_conn = connection_factory()
        self.addCleanup(verify_conn.close)
        rows = {
            row["doc_id"]: row
            for row in verify_conn.execute(
                "SELECT doc_id, parse_status, xbrl_path FROM filings ORDER BY doc_id"
            ).fetchall()
        }
        self.assertEqual(rows["S100GOOD"]["parse_status"], "xbrl_ready")
        self.assertEqual(rows["S100BAD"]["parse_status"], "xbrl_extract_error")
        self.assertTrue(str(rows["S100GOOD"]["xbrl_path"]).endswith("S100GOOD.xbrl"))

    def test_parser_defaults_keep_serial_behavior(self) -> None:
        args = build_arg_parser().parse_args([])

        self.assertEqual(args.workers, 1)
        self.assertEqual(args.extract_chunk_size, 5)

    def test_period_rank_scope_reuses_existing_xbrl_without_reextracting(self) -> None:
        tmpdir = make_tempdir()
        self.addCleanup(shutil.rmtree, tmpdir, True)
        db_path = tmpdir / "case.db"
        zip_path = tmpdir / "sample.zip"
        xbrl_path = tmpdir / "existing.xbrl"
        write_xbrl_zip(zip_path)
        xbrl_path.write_text("existing", encoding="utf-8")

        setup_conn = sqlite3.connect(db_path)
        create_tables(setup_conn)
        setup_conn.execute(
            "INSERT INTO issuer_master (edinet_code, exchange, is_listed) VALUES ('E00001', 'TSE', 1)"
        )
        setup_conn.execute(
            """
            INSERT INTO filings (
                doc_id, edinet_code, form_type, submit_date, period_end,
                zip_path, xbrl_path, xbrl_member_name, download_status, parse_status
            )
            VALUES ('S100EXIST', 'E00001', '030000', '2026-06-01', '', ?, ?, '', 'downloaded', 'xbrl_ready')
            """,
            (str(zip_path), str(xbrl_path)),
        )
        setup_conn.commit()
        setup_conn.close()

        def connection_factory() -> sqlite3.Connection:
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            return conn

        scope_row = {
            "doc_id": "S100EXIST",
            "form_type": "030000",
            "submit_date": "2026-06-01",
            "zip_path": str(zip_path),
            "xbrl_path": str(xbrl_path),
        }
        with (
            patch("edinet_monitor.cli.extract_xbrl_from_zips.get_connection", connection_factory),
            patch("edinet_monitor.cli.extract_xbrl_from_zips.PerformanceLog", FakePerformanceLog),
            patch("edinet_monitor.cli.extract_xbrl_from_zips.fetch_segment_scope_filings", return_value=[scope_row]),
        ):
            summary = run_extract_xbrl_from_zips(period_ranks="latest", workers=4)

        self.assertEqual(summary["target_total"], 1)
        self.assertEqual(summary["extracted_total"], 1)
        self.assertEqual(summary["extract_chunk_count"], 0)
        self.assertEqual(xbrl_path.read_text(encoding="utf-8"), "existing")


if __name__ == "__main__":
    unittest.main()
