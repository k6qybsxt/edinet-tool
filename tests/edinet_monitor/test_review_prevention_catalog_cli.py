from __future__ import annotations

import io
import json
import shutil
import sys
import unittest
import uuid
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch


ROOT_DIR = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT_DIR / "src"
TMP_ROOT = ROOT_DIR / "tests" / "_tmp_review_prevention_catalog_cli"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from edinet_monitor.cli import review_prevention_catalog as cli  # noqa: E402


def _item(item_id: str, *, status: str = "active", areas: list[str] | None = None) -> dict[str, object]:
    return {
        "id": item_id,
        "title": f"Title {item_id}",
        "status": status,
        "severity": "warning",
        "areas": areas or ["excel_export"],
        "triggers": ["pre_implementation_review"],
        "problem": "problem",
        "prevention": "prevention",
        "review_points": ["point"],
    }


class ReviewPreventionCatalogCliTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp_path = TMP_ROOT / uuid.uuid4().hex
        self.tmp_path.mkdir(parents=True, exist_ok=True)
        self.catalog_path = self.tmp_path / "catalog.json"
        self.output_dir = self.tmp_path / "reports"
        self.catalog_path.write_text(
            json.dumps(
                {
                    "version": 1,
                    "items": [
                        _item("excel_active"),
                        _item("db_active", areas=["db_reflection"]),
                        _item("excel_retired", status="retired"),
                    ],
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )

    def tearDown(self) -> None:
        shutil.rmtree(self.tmp_path, ignore_errors=True)

    def _run_cli(self, argv: list[str]) -> str:
        stdout = io.StringIO()
        with (
            patch.object(sys, "argv", ["review_prevention_catalog", *argv]),
            redirect_stdout(stdout),
        ):
            cli.main()
        return stdout.getvalue()

    def test_areas_filter_prints_summary_and_reports(self) -> None:
        output = self._run_cli(
            [
                "--catalog-path",
                str(self.catalog_path),
                "--areas",
                "excel_export,db_reflection",
                "--output-dir",
                str(self.output_dir),
            ]
        )

        self.assertIn("review_id=prevention_catalog_review_", output)
        self.assertIn("matched_count=2", output)
        self.assertIn("warning=2", output)
        self.assertIn("json_path=", output)
        self.assertIn("excel_path=", output)
        self.assertIn("item=warning|active|excel_active", output)
        self.assertNotIn("excel_retired", output)

    def test_include_retired_includes_retired_items(self) -> None:
        output = self._run_cli(
            [
                "--catalog-path",
                str(self.catalog_path),
                "--areas",
                "excel_export",
                "--include-retired",
                "--output-dir",
                str(self.output_dir),
            ]
        )

        self.assertIn("matched_count=2", output)
        self.assertIn("excel_retired", output)


if __name__ == "__main__":
    unittest.main()
