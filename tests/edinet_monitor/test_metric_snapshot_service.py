from __future__ import annotations

import sqlite3
import unittest
import uuid
from pathlib import Path

from edinet_monitor.services.metric_snapshot_service import (
    compare_metric_snapshots,
    export_metric_snapshot,
)


TMP_ROOT = Path(__file__).resolve().parents[1] / "_tmp_edinet_monitor" / "metric_snapshot_service"


def temporary_workspace() -> Path:
    TMP_ROOT.mkdir(parents=True, exist_ok=True)
    path = TMP_ROOT / uuid.uuid4().hex
    path.mkdir(parents=True, exist_ok=False)
    return path


def create_metric_tables(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE normalized_metrics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            doc_id TEXT NOT NULL,
            edinet_code TEXT NOT NULL,
            security_code TEXT,
            metric_key TEXT NOT NULL,
            fiscal_year INTEGER,
            period_end TEXT,
            value_num REAL,
            source_tag TEXT,
            consolidation TEXT,
            rule_version TEXT NOT NULL
        );

        CREATE TABLE derived_metrics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            doc_id TEXT NOT NULL,
            edinet_code TEXT NOT NULL,
            security_code TEXT,
            metric_key TEXT NOT NULL,
            metric_base TEXT NOT NULL,
            metric_group TEXT NOT NULL,
            fiscal_year INTEGER,
            period_end TEXT,
            period_scope TEXT NOT NULL,
            period_offset INTEGER NOT NULL DEFAULT 0,
            consolidation TEXT,
            accounting_standard TEXT,
            document_display_unit TEXT,
            value_num REAL,
            value_unit TEXT NOT NULL,
            calc_status TEXT NOT NULL,
            formula_name TEXT NOT NULL,
            source_detail_json TEXT,
            rule_version TEXT NOT NULL
        );
        """
    )
    conn.commit()


def insert_sample_rows(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        INSERT INTO normalized_metrics (
            doc_id, edinet_code, security_code, metric_key, fiscal_year, period_end,
            value_num, source_tag, consolidation, rule_version
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "S100TEST",
            "E00001",
            "12340",
            "NetSalesCurrent",
            2026,
            "2026-03-31",
            1000.0,
            "NetSales",
            "Consolidated",
            "v1",
        ),
    )
    conn.execute(
        """
        INSERT INTO derived_metrics (
            doc_id, edinet_code, security_code, metric_key, metric_base, metric_group,
            fiscal_year, period_end, period_scope, period_offset, consolidation,
            accounting_standard, document_display_unit, value_num, value_unit,
            calc_status, formula_name, source_detail_json, rule_version
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "S100TEST",
            "E00001",
            "12340",
            "ROACurrent",
            "ROA",
            "profitability",
            2026,
            "2026-03-31",
            "current",
            0,
            "Consolidated",
            "Japan GAAP",
            "百万円",
            0.05,
            "ratio",
            "ok",
            "roa",
            "{}",
            "v1",
        ),
    )
    conn.commit()


class MetricSnapshotServiceTest(unittest.TestCase):
    def test_export_and_compare_same_snapshot_has_no_diff(self) -> None:
        tmp_path = temporary_workspace()
        db_path = tmp_path / "metrics.db"
        with sqlite3.connect(db_path) as conn:
            create_metric_tables(conn)
            insert_sample_rows(conn)

        before = export_metric_snapshot(
            label="before_taxonomy_change",
            output_dir=tmp_path,
            db_path=db_path,
            timestamp="20260419_120000",
        )
        after = export_metric_snapshot(
            label="after_taxonomy_change",
            output_dir=tmp_path,
            db_path=db_path,
            timestamp="20260419_120100",
        )
        comparison = compare_metric_snapshots(
            before_dir=before.snapshot_dir,
            after_dir=after.snapshot_dir,
            output_dir=tmp_path,
            timestamp="20260419_120200",
        )

        self.assertEqual(before.normalized_rows, 1)
        self.assertEqual(before.derived_rows, 1)
        self.assertEqual(comparison.added_count, 0)
        self.assertEqual(comparison.removed_count, 0)
        self.assertEqual(comparison.value_changed_count, 0)
        self.assertEqual(comparison.full_changed_same_value_count, 0)
        self.assertTrue((before.snapshot_dir / "snapshot_manifest.json").exists())
        self.assertTrue((comparison.comparison_dir / "comparison_summary.json").exists())

    def test_compare_detects_value_change(self) -> None:
        tmp_path = temporary_workspace()
        db_path = tmp_path / "metrics.db"
        with sqlite3.connect(db_path) as conn:
            create_metric_tables(conn)
            insert_sample_rows(conn)

        before = export_metric_snapshot(
            label="before_taxonomy_change",
            output_dir=tmp_path,
            db_path=db_path,
            timestamp="20260419_121000",
        )

        with sqlite3.connect(db_path) as conn:
            conn.execute(
                "UPDATE normalized_metrics SET value_num = ? WHERE metric_key = ?",
                (1200.0, "NetSalesCurrent"),
            )
            conn.commit()

        after = export_metric_snapshot(
            label="after_taxonomy_change",
            output_dir=tmp_path,
            db_path=db_path,
            timestamp="20260419_121100",
        )
        comparison = compare_metric_snapshots(
            before_dir=before.snapshot_dir,
            after_dir=after.snapshot_dir,
            output_dir=tmp_path,
            timestamp="20260419_121200",
        )

        self.assertEqual(comparison.value_changed_count, 1)
        value_changes = (comparison.comparison_dir / "value_changes.tsv").read_text(encoding="utf-8")
        self.assertIn("NetSalesCurrent", value_changes)
        self.assertIn("1000", value_changes)
        self.assertIn("1200", value_changes)


if __name__ == "__main__":
    unittest.main()
