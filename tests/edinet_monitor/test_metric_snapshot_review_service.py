from __future__ import annotations

import csv
import json
import sqlite3
import shutil
import unittest
import uuid
from pathlib import Path

from edinet_monitor.services.metric_snapshot_review_service import (
    build_snapshot_comparison_review,
    enrich_comparison_rows,
    fetch_filing_metadata_by_doc_id,
    load_comparison_rows,
    load_comparison_summary,
)

TMP_ROOT = Path(__file__).resolve().parents[1] / "_tmp_edinet_monitor" / "metric_snapshot_review_service"


def temporary_workspace() -> Path:
    TMP_ROOT.mkdir(parents=True, exist_ok=True)
    path = TMP_ROOT / uuid.uuid4().hex
    path.mkdir(parents=True, exist_ok=False)
    return path


def write_tsv(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, delimiter="\t", lineterminator="\n")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


class MetricSnapshotReviewServiceTest(unittest.TestCase):
    def test_build_review_reports_ok_when_no_rows_exist(self) -> None:
        comparison_dir = temporary_workspace()
        try:
            summary = {
                "before_row_count": 10,
                "after_row_count": 10,
                "added_count": 0,
                "removed_count": 0,
                "value_changed_count": 0,
                "full_changed_same_value_count": 0,
            }
            (comparison_dir / "comparison_summary.json").write_text(
                json.dumps(summary, ensure_ascii=False),
                encoding="utf-8",
            )

            rows = load_comparison_rows(comparison_dir)
            lines = build_snapshot_comparison_review(
                comparison_dir=comparison_dir,
                summary=load_comparison_summary(comparison_dir),
                rows=rows,
            )
        finally:
            shutil.rmtree(comparison_dir, ignore_errors=True)

        self.assertEqual(rows, [])
        self.assertIn("result: OK", "\n".join(lines))

    def test_review_enriches_value_changes_with_company_and_metric_label(self) -> None:
        comparison_dir = temporary_workspace()
        try:
            (comparison_dir / "comparison_summary.json").write_text(
                json.dumps(
                    {
                        "added_count": 0,
                        "removed_count": 0,
                        "value_changed_count": 1,
                        "full_changed_same_value_count": 0,
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            write_tsv(
                comparison_dir / "value_changes.tsv",
                [
                    "row_key",
                    "source",
                    "doc_id",
                    "metric_key",
                    "period_end",
                    "period_scope",
                    "period_offset",
                    "consolidation",
                    "before_value_num",
                    "after_value_num",
                    "before_calc_status",
                    "after_calc_status",
                    "before_source_tag",
                    "after_source_tag",
                    "before_full_hash",
                    "after_full_hash",
                ],
                [
                    {
                        "row_key": "normalized_metrics|S100TEST|NetSalesCurrent|2026-03-31",
                        "source": "normalized_metrics",
                        "doc_id": "S100TEST",
                        "metric_key": "NetSalesCurrent",
                        "period_end": "2026-03-31",
                        "period_scope": "",
                        "period_offset": "",
                        "consolidation": "Consolidated",
                        "before_value_num": "100",
                        "after_value_num": "120",
                        "before_calc_status": "",
                        "after_calc_status": "",
                        "before_source_tag": "NetSales",
                        "after_source_tag": "Revenue",
                        "before_full_hash": "a",
                        "after_full_hash": "b",
                    }
                ],
            )

            db_path = comparison_dir / "test.db"
            with sqlite3.connect(db_path) as conn:
                conn.row_factory = sqlite3.Row
                conn.executescript(
                    """
                    CREATE TABLE issuer_master (
                        edinet_code TEXT PRIMARY KEY,
                        security_code TEXT,
                        company_name TEXT,
                        industry_33 TEXT
                    );
                    CREATE TABLE filings (
                        doc_id TEXT PRIMARY KEY,
                        edinet_code TEXT,
                        security_code TEXT,
                        period_end TEXT,
                        submit_date TEXT
                    );
                    """
                )
                conn.execute(
                    "INSERT INTO issuer_master VALUES (?, ?, ?, ?)",
                    ("E1", "12340", "Sample Co", "サービス業"),
                )
                conn.execute(
                    "INSERT INTO filings VALUES (?, ?, ?, ?, ?)",
                    ("S100TEST", "E1", "12340", "2026-03-31", "2026-06-28"),
                )
                metadata = fetch_filing_metadata_by_doc_id(conn, ["S100TEST"])

            rows = enrich_comparison_rows(load_comparison_rows(comparison_dir), metadata)
            lines = build_snapshot_comparison_review(
                comparison_dir=comparison_dir,
                summary=load_comparison_summary(comparison_dir),
                rows=rows,
            )
            text = "\n".join(lines)
        finally:
            shutil.rmtree(comparison_dir, ignore_errors=True)

        self.assertEqual(rows[0]["company_name"], "Sample Co")
        self.assertEqual(rows[0]["metric_label"], "売上高（当期）")
        self.assertIn("value_changed", text)
        self.assertIn("Sample Co", text)


if __name__ == "__main__":
    unittest.main()
