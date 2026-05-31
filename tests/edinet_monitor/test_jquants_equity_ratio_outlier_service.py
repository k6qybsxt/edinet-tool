from __future__ import annotations

from concurrent.futures import Future
import shutil
import sqlite3
import sys
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch


ROOT_DIR = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT_DIR / "src"
TMP_ROOT = ROOT_DIR / "tests" / "_tmp_jquants_equity_ratio_outlier_service"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from edinet_monitor.db.schema import create_tables, get_connection  # noqa: E402
from edinet_monitor.cli.audit_jquants_equity_ratio_outliers import build_parser  # noqa: E402
from edinet_monitor.services.jquants_equity_ratio_outlier_service import (  # noqa: E402
    benchmark_jquants_equity_ratio_outlier_audit,
    run_jquants_equity_ratio_outlier_audit,
    write_jquants_equity_ratio_outlier_tsv,
)


class _ImmediateExecutor:
    def __init__(self, *, max_workers: int):
        self.max_workers = max_workers

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False

    def submit(self, func, *args, **kwargs):
        future = Future()
        try:
            future.set_result(func(*args, **kwargs))
        except Exception as exc:
            future.set_exception(exc)
        return future


def _insert_issuer(conn: sqlite3.Connection, *, edinet_code: str, security_code: str, company_name: str) -> None:
    conn.execute(
        """
        INSERT INTO issuer_master (
            edinet_code, security_code, company_name, market, industry_33,
            industry_17, is_listed, exchange, listing_category_raw,
            listing_source, updated_at
        ) VALUES (?, ?, ?, 'Prime', 'Test', 'Test', 1, 'TSE', 'Prime', 'fixture', '2026-05-31')
        """,
        (edinet_code, security_code, company_name),
    )


def _insert_listed_info(conn: sqlite3.Connection, *, security_code: str, company_name: str) -> None:
    conn.execute(
        """
        INSERT INTO jquants_listed_info_raw (
            listing_date, local_code, security_code, company_name, raw_json,
            created_at, updated_at
        ) VALUES ('2026-05-31', ?, ?, ?, '{}', '2026-05-31', '2026-05-31')
        """,
        (f"{security_code}0", security_code, company_name),
    )


def _insert_equity_ratio(
    conn: sqlite3.Connection,
    *,
    disclosure_number: str,
    edinet_code: str,
    security_code: str,
    fiscal_year: int,
    period_key: str,
    quarter_type: str,
    period_end: str,
    disclosed_date: str,
    value_num: float | None,
    calc_status: str = "ok",
) -> None:
    conn.execute(
        """
        INSERT INTO jquants_financial_metrics (
            disclosure_number, local_code, security_code, edinet_code, metric_kind,
            period_scope, period_key, quarter_type, forecast_target, forecast_stage,
            fiscal_year, period_start, period_end, disclosed_date, disclosed_time,
            metric_key, metric_base, metric_group, value_num, value_unit, calc_status,
            source_field, source_detail_json, rule_version, created_at, updated_at
        ) VALUES (
            ?, ?, ?, ?, 'actual',
            'quarter', ?, ?, NULL, '',
            ?, '', ?, ?, '15:00',
            'EquityRatioCurrent', 'EquityRatio', 'ratio', ?, 'ratio', ?,
            'fixture', '{}', 'v1', '2026-05-31', '2026-05-31'
        )
        """,
        (
            disclosure_number,
            f"{security_code}0",
            security_code,
            edinet_code,
            period_key,
            quarter_type,
            fiscal_year,
            period_end,
            disclosed_date,
            value_num,
            calc_status,
        ),
    )


class JQuantsEquityRatioOutlierServiceTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp_path = TMP_ROOT / uuid.uuid4().hex
        self.tmp_path.mkdir(parents=True, exist_ok=True)
        self.db_path = self.tmp_path / "monitor.db"
        create_tables(self.db_path)
        self.conn = get_connection(self.db_path)
        _insert_issuer(self.conn, edinet_code="E00001", security_code="1111", company_name="Negative Corp")
        _insert_listed_info(self.conn, security_code="2222", company_name="High Corp")
        _insert_issuer(self.conn, edinet_code="E00003", security_code="3333", company_name="Latest Corp")
        _insert_issuer(self.conn, edinet_code="E00004", security_code="4444", company_name="Normal Corp")
        _insert_equity_ratio(
            self.conn,
            disclosure_number="NEG",
            edinet_code="E00001",
            security_code="1111",
            fiscal_year=2024,
            period_key="actual:FY",
            quarter_type="FY",
            period_end="2025-03-31",
            disclosed_date="2025-05-01",
            value_num=-0.2,
        )
        _insert_equity_ratio(
            self.conn,
            disclosure_number="HIGH",
            edinet_code="E00002",
            security_code="2222",
            fiscal_year=2025,
            period_key="actual:3Q",
            quarter_type="3Q",
            period_end="2025-12-31",
            disclosed_date="2026-02-01",
            value_num=2.0,
        )
        _insert_equity_ratio(
            self.conn,
            disclosure_number="OLD",
            edinet_code="E00003",
            security_code="3333",
            fiscal_year=2025,
            period_key="actual:FY",
            quarter_type="FY",
            period_end="2026-03-31",
            disclosed_date="2026-04-01",
            value_num=3.0,
        )
        _insert_equity_ratio(
            self.conn,
            disclosure_number="LATEST",
            edinet_code="E00003",
            security_code="3333",
            fiscal_year=2025,
            period_key="actual:FY",
            quarter_type="FY",
            period_end="2026-03-31",
            disclosed_date="2026-04-02",
            value_num=0.5,
        )
        _insert_equity_ratio(
            self.conn,
            disclosure_number="NORMAL",
            edinet_code="E00004",
            security_code="4444",
            fiscal_year=2025,
            period_key="actual:1Q",
            quarter_type="1Q",
            period_end="2025-06-30",
            disclosed_date="2025-08-01",
            value_num=0.4,
        )
        self.conn.commit()

    def tearDown(self) -> None:
        self.conn.close()
        shutil.rmtree(self.tmp_path, ignore_errors=True)

    def test_audit_uses_latest_rows_bulk_join_and_classifies_outliers(self) -> None:
        result = run_jquants_equity_ratio_outlier_audit(
            db_path=self.db_path,
            date_from="2025-01-01",
            date_to="2026-12-31",
            workers=1,
        )

        self.assertEqual(result.checked_total, 4)
        self.assertEqual(result.anomaly_total, 2)
        self.assertEqual(result.negative_total, 1)
        self.assertEqual(result.over_150_percent_total, 1)
        self.assertEqual([row.security_code for row in result.outliers], ["2222", "1111"])
        self.assertEqual(result.outliers[0].company_name, "High Corp")

        logged = self.conn.execute(
            """
            SELECT workers, target_total, output_rows_total, summary_json
            FROM pipeline_performance_runs
            WHERE run_id = ?
            """,
            (result.run_id,),
        ).fetchone()
        self.assertEqual(logged["workers"], 1)
        self.assertEqual(logged["target_total"], 4)
        self.assertEqual(logged["output_rows_total"], 2)
        self.assertIn('"negative_total":1', logged["summary_json"])

    def test_serial_and_parallel_results_match(self) -> None:
        with patch(
            "edinet_monitor.services.jquants_equity_ratio_outlier_service.ProcessPoolExecutor",
            _ImmediateExecutor,
        ):
            benchmark = benchmark_jquants_equity_ratio_outlier_audit(
                db_path=self.db_path,
                date_from="2025-01-01",
                date_to="2026-12-31",
                parallel_workers=2,
            )

        self.assertTrue(benchmark.equivalent)
        self.assertEqual(benchmark.serial.checked_total, benchmark.parallel.checked_total)
        self.assertEqual(benchmark.serial.outliers, benchmark.parallel.outliers)
        rows = self.conn.execute(
            """
            SELECT workers
            FROM pipeline_performance_runs
            WHERE command_name = 'audit_jquants_equity_ratio_outliers'
            ORDER BY id
            """
        ).fetchall()
        self.assertEqual([row["workers"] for row in rows], [1, 2])

    def test_tsv_contains_company_name_and_percentage(self) -> None:
        result = run_jquants_equity_ratio_outlier_audit(
            db_path=self.db_path,
            date_from="2025-01-01",
            date_to="2026-12-31",
            workers=1,
        )
        output_path = write_jquants_equity_ratio_outlier_tsv(
            result=result,
            output_dir=self.tmp_path,
            date_from="2025-01-01",
            date_to="2026-12-31",
        )

        text = output_path.read_text(encoding="utf-8-sig")
        self.assertIn("company_name", text)
        self.assertIn("High Corp", text)
        self.assertIn("200.0", text)

    def test_cli_defaults_to_serial_after_parallel_benchmark(self) -> None:
        args = build_parser().parse_args(
            [
                "--date-from",
                "2025-01-01",
                "--date-to",
                "2026-12-31",
            ]
        )
        self.assertEqual(args.workers, 1)


if __name__ == "__main__":
    unittest.main()
