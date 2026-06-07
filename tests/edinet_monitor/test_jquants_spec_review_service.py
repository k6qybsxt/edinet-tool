from __future__ import annotations

import json
import shutil
import sqlite3
import subprocess
import sys
import unittest
import uuid
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT_DIR / "src"
TMP_ROOT = ROOT_DIR / "tests" / "_tmp_jquants_spec_review_service"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from edinet_monitor.services.jquants_spec_review_service import (  # noqa: E402
    JQuantsSpecReviewOptions,
    build_jquants_spec_review,
)


def _completed(command: list[str], payload, *, returncode: int = 0, stderr: str = ""):
    stdout = payload if isinstance(payload, str) else json.dumps(payload, ensure_ascii=False)
    return subprocess.CompletedProcess(args=command, returncode=returncode, stdout=stdout, stderr=stderr)


def _runner(
    *,
    schema_by_endpoint: dict[str, object],
    data_by_endpoint: dict[str, object] | None = None,
    fail_data_endpoint: str = "",
    fail_schema_endpoint: str = "",
):
    data_by_endpoint = data_by_endpoint or {}

    def run(command: list[str]):
        joined = " ".join(command)
        if "--version" in command:
            return _completed(command, "jquants 1.0.0\n")
        if "schema" in command:
            endpoint = command[-1]
            if endpoint == fail_schema_endpoint:
                return _completed(command, "schema failed", returncode=1, stderr="schema failed")
            return _completed(command, schema_by_endpoint.get(endpoint, {"fields": []}))
        if "fins" in command and "summary" in command:
            if fail_data_endpoint == "fins.summary":
                return _completed(command, "data failed", returncode=1, stderr="data failed")
            return _completed(command, data_by_endpoint.get("fins.summary", []))
        if "eq" in command and "daily" in command:
            if fail_data_endpoint == "eq.daily":
                return _completed(command, "data failed", returncode=1, stderr="data failed")
            return _completed(command, data_by_endpoint.get("eq.daily", []))
        raise AssertionError(f"Unexpected command: {joined}")

    return run


class JQuantsSpecReviewServiceTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp_path = TMP_ROOT / uuid.uuid4().hex
        self.tmp_path.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        self.conn.executescript(
            """
            CREATE TABLE jquants_statement_raw (
                disclosure_number TEXT PRIMARY KEY,
                disclosed_date TEXT,
                disclosed_time TEXT,
                local_code TEXT,
                security_code TEXT,
                raw_json TEXT NOT NULL
            );
            CREATE TABLE jquants_daily_quotes (
                local_code TEXT NOT NULL,
                security_code TEXT,
                trade_date TEXT NOT NULL,
                raw_json TEXT NOT NULL
            );
            """
        )

    def tearDown(self) -> None:
        self.conn.close()
        shutil.rmtree(self.tmp_path, ignore_errors=True)

    def _write_baseline(self, endpoint: str, fields: list[dict[str, str]]) -> None:
        path = self.tmp_path / "baseline" / f"{endpoint}.schema.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps({"endpoint": endpoint, "fields": fields}, ensure_ascii=False),
            encoding="utf-8",
        )

    def _insert_statement(self, row: dict) -> None:
        self.conn.execute(
            """
            INSERT INTO jquants_statement_raw (
                disclosure_number, disclosed_date, disclosed_time, local_code, security_code, raw_json
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                row.get("DiscNo"),
                row.get("DiscDate"),
                row.get("DiscTime", ""),
                row.get("Code"),
                str(row.get("Code", ""))[:4],
                json.dumps(row),
            ),
        )

    def _run_review(self, **kwargs):
        options = JQuantsSpecReviewOptions(
            endpoints=kwargs.pop("endpoints", ("fins.summary",)),
            date_value="2026-05-07",
            code="1111",
            baseline_dir=self.tmp_path / "baseline",
            output_dir=self.tmp_path / "reports",
            official_cli="jquants",
            **kwargs.pop("option_overrides", {}),
        )
        return build_jquants_spec_review(self.conn, options, runner=kwargs["runner"])

    def test_baseline_match_has_no_issue(self) -> None:
        row = {"DiscDate": "2026-05-07", "DiscTime": "15:00", "Code": "11110", "DiscNo": "DISC1", "Sales": 100}
        self._insert_statement(row)
        self._write_baseline("fins.summary", [{"name": "DiscNo", "type": "string"}, {"name": "Sales", "type": "number"}])

        result = self._run_review(
            runner=_runner(
                schema_by_endpoint={"fins.summary": {"fields": [{"name": "DiscNo", "type": "string"}, {"name": "Sales", "type": "number"}]}},
                data_by_endpoint={"fins.summary": [row]},
            )
        )

        self.assertEqual(result.issue_count, 0)
        self.assertTrue(result.json_path.exists())
        self.assertTrue(result.excel_path.exists())

    def test_added_field_is_warning(self) -> None:
        row = {"DiscDate": "2026-05-07", "Code": "11110", "DiscNo": "DISC1", "Sales": 100}
        self._insert_statement(row)
        self._write_baseline("fins.summary", [{"name": "Sales", "type": "number"}])

        result = self._run_review(
            runner=_runner(
                schema_by_endpoint={"fins.summary": {"fields": [{"name": "Sales", "type": "number"}, {"name": "NewField", "type": "string"}]}},
                data_by_endpoint={"fins.summary": [row]},
            )
        )

        self.assertEqual(result.counts_by_severity["warning"], 1)
        self.assertEqual(result.issues[0].check_name, "added_field")

    def test_removed_field_is_critical(self) -> None:
        row = {"DiscDate": "2026-05-07", "Code": "11110", "DiscNo": "DISC1", "Sales": 100}
        self._insert_statement(row)
        self._write_baseline("fins.summary", [{"name": "Sales", "type": "number"}, {"name": "Gone", "type": "string"}])

        result = self._run_review(
            runner=_runner(
                schema_by_endpoint={"fins.summary": {"fields": [{"name": "Sales", "type": "number"}]}},
                data_by_endpoint={"fins.summary": [row]},
            )
        )

        self.assertIn("removed_field", {issue.check_name for issue in result.issues})
        self.assertEqual(result.counts_by_severity["critical"], 1)

    def test_type_change_is_critical(self) -> None:
        row = {"DiscDate": "2026-05-07", "Code": "11110", "DiscNo": "DISC1", "Sales": 100}
        self._insert_statement(row)
        self._write_baseline("fins.summary", [{"name": "Sales", "type": "number"}])

        result = self._run_review(
            runner=_runner(
                schema_by_endpoint={"fins.summary": {"fields": [{"name": "Sales", "type": "string"}]}},
                data_by_endpoint={"fins.summary": [row]},
            )
        )

        self.assertIn("field_type_changed", {issue.check_name for issue in result.issues})
        self.assertEqual(result.counts_by_severity["critical"], 1)

    def test_missing_baseline_is_warning(self) -> None:
        row = {"DiscDate": "2026-05-07", "Code": "11110", "DiscNo": "DISC1", "Sales": 100}
        self._insert_statement(row)

        result = self._run_review(
            runner=_runner(
                schema_by_endpoint={"fins.summary": {"fields": [{"name": "Sales", "type": "number"}]}},
                data_by_endpoint={"fins.summary": [row]},
            )
        )

        self.assertIn("missing_baseline", {issue.check_name for issue in result.issues})
        self.assertEqual(result.counts_by_severity["warning"], 1)

    def test_fins_details_is_unsupported(self) -> None:
        result = self._run_review(
            endpoints=("fins.details",),
            runner=_runner(schema_by_endpoint={}, data_by_endpoint={}),
        )

        self.assertEqual(result.counts_by_severity["critical"], 1)
        self.assertEqual(result.issues[0].check_name, "unsupported_endpoint")

    def test_official_cli_failure_is_critical(self) -> None:
        self._write_baseline("fins.summary", [{"name": "Sales", "type": "number"}])

        result = self._run_review(
            runner=_runner(
                schema_by_endpoint={"fins.summary": {"fields": [{"name": "Sales", "type": "number"}]}},
                fail_data_endpoint="fins.summary",
            )
        )

        self.assertIn("official_cli_failed", {issue.check_name for issue in result.issues})
        self.assertEqual(result.counts_by_severity["critical"], 1)

    def test_raw_compare_missing_and_diff_are_issues(self) -> None:
        db_row = {"DiscDate": "2026-05-07", "Code": "11110", "DiscNo": "DISC1", "Sales": 99}
        official_rows = [
            {**db_row, "Sales": 100},
            {"DiscDate": "2026-05-07", "Code": "22220", "DiscNo": "DISC2", "Sales": 200},
        ]
        self._insert_statement(db_row)
        self._write_baseline("fins.summary", [{"name": "Sales", "type": "number"}])

        result = self._run_review(
            runner=_runner(
                schema_by_endpoint={"fins.summary": {"fields": [{"name": "Sales", "type": "number"}]}},
                data_by_endpoint={"fins.summary": official_rows},
            )
        )

        issue_names = {issue.check_name for issue in result.issues}
        self.assertIn("missing_in_db", issue_names)
        self.assertIn("field_value_diff", issue_names)
        self.assertEqual(result.counts_by_severity["critical"], 1)
        self.assertEqual(result.counts_by_severity["warning"], 1)


if __name__ == "__main__":
    unittest.main()
