from __future__ import annotations

import unittest

from edinet_monitor.services.normalization_impact_service import (
    compare_normalized_rows,
    recalculate_derived_rows_for_preview,
    security_code_variants,
    summarize_impact_rows,
)


class NormalizationImpactServiceTest(unittest.TestCase):
    def test_compare_normalized_rows_reports_added_removed_and_changed(self) -> None:
        current_rows = [
            {
                "doc_id": "S100A",
                "metric_key": "NetSalesCurrent",
                "period_end": "2026-03-31",
                "value_num": 100,
                "source_tag": "NetSales",
                "consolidation": "Consolidated",
            },
            {
                "doc_id": "S100A",
                "metric_key": "OperatingIncomeCurrent",
                "period_end": "2026-03-31",
                "value_num": 20,
                "source_tag": "OperatingIncome",
                "consolidation": "Consolidated",
            },
            {
                "doc_id": "S100A",
                "metric_key": "OrdinaryIncomeCurrent",
                "period_end": "2026-03-31",
                "value_num": 10,
                "source_tag": "OrdinaryIncome",
                "consolidation": "Consolidated",
            },
        ]
        recalculated_rows = [
            {
                "doc_id": "S100A",
                "metric_key": "NetSalesCurrent",
                "period_end": "2026-03-31",
                "value_num": 100,
                "source_tag": "NetSales",
                "consolidation": "Consolidated",
            },
            {
                "doc_id": "S100A",
                "metric_key": "OperatingIncomeCurrent",
                "period_end": "2026-03-31",
                "value_num": 21,
                "source_tag": "OperatingIncome",
                "consolidation": "Consolidated",
                "_candidate_validation_status": "OK",
                "_period_source": "period_fallback",
                "_period_fallback_used": 1,
            },
            {
                "doc_id": "S100A",
                "metric_key": "ProfitCurrent",
                "period_end": "2026-03-31",
                "value_num": 7,
                "source_tag": "ProfitLoss",
                "consolidation": "Consolidated",
            },
        ]
        filings = {
            "S100A": {
                "doc_id": "S100A",
                "security_code": "12340",
                "company_name": "Sample Co",
                "industry_33": "Services",
                "period_end": "2026-03-31",
            }
        }

        rows = compare_normalized_rows(
            current_rows=current_rows,
            recalculated_rows=recalculated_rows,
            filing_by_doc_id=filings,
        )
        by_metric = {row["metric_key"]: row for row in rows}
        summary = summarize_impact_rows(rows)

        self.assertNotIn("NetSalesCurrent", by_metric)
        self.assertEqual(by_metric["OperatingIncomeCurrent"]["change_type"], "changed")
        self.assertEqual(by_metric["OperatingIncomeCurrent"]["before_value_num"], "20")
        self.assertEqual(by_metric["OperatingIncomeCurrent"]["after_value_num"], "21")
        self.assertEqual(by_metric["OperatingIncomeCurrent"]["candidate_validation_status"], "OK")
        self.assertEqual(by_metric["OperatingIncomeCurrent"]["period_fallback_used"], "1")
        self.assertEqual(by_metric["ProfitCurrent"]["change_type"], "added")
        self.assertEqual(by_metric["OrdinaryIncomeCurrent"]["change_type"], "removed")
        self.assertEqual(summary, {"added": 1, "removed": 1, "changed": 1, "unchanged": 0})

    def test_compare_normalized_rows_can_include_unchanged(self) -> None:
        row = {
            "doc_id": "S100A",
            "metric_key": "NetSalesCurrent",
            "period_end": "2026-03-31",
            "value_num": 100.0,
            "source_tag": "NetSales",
            "consolidation": "Consolidated",
        }

        rows = compare_normalized_rows(
            current_rows=[row],
            recalculated_rows=[dict(row)],
            filing_by_doc_id={"S100A": {"period_end": "2026-03-31"}},
            include_unchanged=True,
        )

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["change_type"], "unchanged")

    def test_compare_normalized_rows_detects_derived_value_changes(self) -> None:
        current_rows = [
            {
                "metric_source": "derived_metrics",
                "doc_id": "S100A",
                "metric_key": "OperatingMarginCurrent",
                "period_end": "2026-03-31",
                "consolidation": "Consolidated",
                "value_num": 0.2,
                "value_unit": "ratio",
                "calc_status": "ok",
                "formula_name": "ratio",
            }
        ]
        recalculated_rows = [
            {
                "metric_source": "derived_metrics",
                "doc_id": "S100A",
                "metric_key": "OperatingMarginCurrent",
                "period_end": "2026-03-31",
                "consolidation": "Consolidated",
                "value_num": 0.21,
                "value_unit": "ratio",
                "calc_status": "ok",
                "formula_name": "ratio",
            }
        ]

        rows = compare_normalized_rows(
            current_rows=current_rows,
            recalculated_rows=recalculated_rows,
            filing_by_doc_id={"S100A": {"period_end": "2026-03-31"}},
        )

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["metric_source"], "derived_metrics")
        self.assertEqual(rows[0]["change_type"], "changed")
        self.assertEqual(rows[0]["before_calc_status"], "ok")
        self.assertEqual(rows[0]["after_calc_status"], "ok")

    def test_recalculate_derived_rows_for_preview_marks_metric_source(self) -> None:
        filings = [
            {
                "doc_id": "S100A",
                "form_type": "030000",
                "accounting_standard": "",
                "document_display_unit": "",
            }
        ]
        normalized_rows = [
            {
                "doc_id": "S100A",
                "edinet_code": "E1",
                "security_code": "12340",
                "metric_key": "NetSalesCurrent",
                "fiscal_year": 2026,
                "period_end": "2026-03-31",
                "value_num": 100,
                "source_tag": "NetSales",
                "consolidation": "Consolidated",
                "rule_version": "test",
            },
            {
                "doc_id": "S100A",
                "edinet_code": "E1",
                "security_code": "12340",
                "metric_key": "OperatingIncomeCurrent",
                "fiscal_year": 2026,
                "period_end": "2026-03-31",
                "value_num": 20,
                "source_tag": "OperatingIncome",
                "consolidation": "Consolidated",
                "rule_version": "test",
            },
        ]

        rows = recalculate_derived_rows_for_preview(filings, normalized_rows)

        self.assertTrue(rows)
        self.assertEqual({row["metric_source"] for row in rows}, {"derived_metrics"})

    def test_security_code_variants_handles_four_and_five_digit_codes(self) -> None:
        self.assertEqual(security_code_variants("4613"), ["4613", "46130"])
        self.assertEqual(security_code_variants("46130"), ["4613", "46130"])
        self.assertEqual(security_code_variants(""), [])


if __name__ == "__main__":
    unittest.main()
