from __future__ import annotations

import json
import shutil
import sys
import unittest
import uuid
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT_DIR / "src"
TMP_ROOT = ROOT_DIR / "tests" / "_tmp_prevention_catalog_service"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from edinet_monitor.services.prevention_catalog_service import (  # noqa: E402
    PreventionCatalogReviewOptions,
    filter_prevention_catalog_items,
    load_prevention_catalog,
    review_prevention_catalog,
)


def _item(
    item_id: str,
    *,
    status: str = "active",
    severity: str = "warning",
    areas: list[str] | None = None,
    triggers: list[str] | None = None,
) -> dict[str, object]:
    return {
        "id": item_id,
        "title": f"Title {item_id}",
        "status": status,
        "severity": severity,
        "areas": areas or ["excel_export"],
        "triggers": triggers or ["pre_implementation_review"],
        "problem": "problem",
        "prevention": "prevention",
        "review_points": ["point"],
    }


def _write_catalog(path: Path, items: list[dict[str, object]]) -> None:
    path.write_text(json.dumps({"version": 1, "items": items}, ensure_ascii=False), encoding="utf-8")


class PreventionCatalogServiceTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp_path = TMP_ROOT / uuid.uuid4().hex
        self.tmp_path.mkdir(parents=True, exist_ok=True)
        self.catalog_path = self.tmp_path / "catalog.json"

    def tearDown(self) -> None:
        shutil.rmtree(self.tmp_path, ignore_errors=True)

    def test_load_filter_and_write_review_reports(self) -> None:
        _write_catalog(
            self.catalog_path,
            [
                _item("excel", severity="critical", areas=["excel_export"]),
                _item("db", severity="warning", areas=["db_reflection"], triggers=["pre_db_reflection"]),
                _item("retired", status="retired", areas=["excel_export"]),
            ],
        )

        items = load_prevention_catalog(self.catalog_path)
        filtered = filter_prevention_catalog_items(items, areas=("excel_export", "db_reflection"))
        self.assertEqual([item.item_id for item in filtered], ["excel", "db"])

        result = review_prevention_catalog(
            PreventionCatalogReviewOptions(
                catalog_path=self.catalog_path,
                areas=("excel_export", "db_reflection"),
                output_dir=self.tmp_path / "reports",
            )
        )

        self.assertEqual(result.status, "review_required")
        self.assertEqual(len(result.matched_items), 2)
        self.assertEqual(result.counts_by_severity["critical"], 1)
        self.assertTrue(result.json_path.exists())
        self.assertTrue(result.excel_path.exists())

    def test_validation_rejects_invalid_status_and_severity(self) -> None:
        _write_catalog(
            self.catalog_path,
            [
                _item("bad_status", status="unknown"),
                _item("bad_severity", severity="blocker"),
            ],
        )

        with self.assertRaises(ValueError) as context:
            load_prevention_catalog(self.catalog_path)

        message = str(context.exception)
        self.assertIn("status is invalid", message)
        self.assertIn("severity is invalid", message)

    def test_validation_rejects_duplicate_id_and_missing_required_field(self) -> None:
        missing_title = _item("missing_title")
        missing_title.pop("title")
        _write_catalog(
            self.catalog_path,
            [
                _item("duplicate"),
                _item("duplicate"),
                missing_title,
            ],
        )

        with self.assertRaises(ValueError) as context:
            load_prevention_catalog(self.catalog_path)

        message = str(context.exception)
        self.assertIn("duplicate item id: duplicate", message)
        self.assertIn("title must be a non-empty string", message)


if __name__ == "__main__":
    unittest.main()
