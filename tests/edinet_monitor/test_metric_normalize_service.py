from __future__ import annotations

import unittest
from pathlib import Path
import shutil

from edinet_monitor.services.normalizer.metric_normalize_service import (
    build_normalization_candidates,
    normalize_raw_fact_row,
    normalize_raw_fact_rows,
)


def build_raw_fact(*, doc_id: str = "DOC1", tag_name: str, value_text: str = "123", context_ref: str = "CurrentYearDuration_ConsolidatedMember", period_type: str = "duration", period_end: str = "2025-03-31", consolidation: str = "Consolidated") -> dict:
    return {
        "doc_id": doc_id,
        "tag_name": tag_name,
        "context_ref": context_ref,
        "unit_ref": "JPY",
        "period_type": period_type,
        "period_start": "2024-04-01",
        "period_end": period_end,
        "instant_date": None,
        "consolidation": consolidation,
        "value_text": value_text,
    }


LAB_XML_FOR_PRIORITY = """<?xml version="1.0" encoding="utf-8"?>
<link:linkbase xmlns:link="http://www.xbrl.org/2003/linkbase" xmlns:xlink="http://www.w3.org/1999/xlink">
  <link:labelLink xlink:type="extended" xlink:role="http://www.xbrl.org/2003/role/link">
    <link:loc xlink:type="locator" xlink:href="sample.xsd#jppfs_cor_GeneralAndAdministrativeExpensesSGA" xlink:label="GeneralAndAdministrativeExpensesSGA" />
    <link:loc xlink:type="locator" xlink:href="sample.xsd#jppfs_cor_GeneralAndAdministrativeExpenses" xlink:label="GeneralAndAdministrativeExpenses" />
    <link:label xlink:type="resource" xlink:label="label_sga" xlink:role="http://www.xbrl.org/2003/role/label" xml:lang="ja">販売費及び一般管理費</link:label>
    <link:label xlink:type="resource" xlink:label="label_ga" xlink:role="http://www.xbrl.org/2003/role/label" xml:lang="ja">一般管理費</link:label>
    <link:labelArc xlink:type="arc" xlink:from="GeneralAndAdministrativeExpensesSGA" xlink:to="label_sga" />
    <link:labelArc xlink:type="arc" xlink:from="GeneralAndAdministrativeExpenses" xlink:to="label_ga" />
  </link:labelLink>
</link:linkbase>
"""


class MetricNormalizeServiceTest(unittest.TestCase):
    def test_operating_gross_profit_maps_to_gross_profit(self) -> None:
        row = build_raw_fact(tag_name="OperatingGrossProfit")

        normalized = normalize_raw_fact_row(
            row,
            edinet_code="E00000",
            security_code="9501",
        )

        self.assertIsNotNone(normalized)
        assert normalized is not None
        self.assertEqual(normalized["metric_key"], "GrossProfitCurrent")
        self.assertEqual(normalized["source_tag"], "OperatingGrossProfit")

    def test_operating_cost_maps_to_cost_of_sales(self) -> None:
        row = build_raw_fact(tag_name="OperatingCost")

        normalized = normalize_raw_fact_row(
            row,
            edinet_code="E00000",
            security_code="9501",
        )

        self.assertIsNotNone(normalized)
        assert normalized is not None
        self.assertEqual(normalized["metric_key"], "CostOfSalesCurrent")
        self.assertEqual(normalized["source_tag"], "OperatingCost")

    def test_cost_of_raw_materials_maps_to_cost_of_sales(self) -> None:
        row = build_raw_fact(tag_name="CostOfRawMaterialsCOS")

        normalized = normalize_raw_fact_row(
            row,
            edinet_code="E00000",
            security_code="9708",
        )

        self.assertIsNotNone(normalized)
        assert normalized is not None
        self.assertEqual(normalized["metric_key"], "CostOfSalesCurrent")
        self.assertEqual(normalized["source_tag"], "CostOfRawMaterialsCOS")

    def test_cost_of_completed_work_maps_to_cost_of_sales(self) -> None:
        row = build_raw_fact(tag_name="CostOfCompletedWorkCOSExpOA")

        normalized = normalize_raw_fact_row(
            row,
            edinet_code="E00000",
            security_code="2153",
        )

        self.assertIsNotNone(normalized)
        assert normalized is not None
        self.assertEqual(normalized["metric_key"], "CostOfSalesCurrent")
        self.assertEqual(normalized["source_tag"], "CostOfCompletedWorkCOSExpOA")

    def test_cost_of_finished_goods_sold_maps_to_cost_of_sales(self) -> None:
        row = build_raw_fact(tag_name="CostOfFinishedGoodsSold")

        normalized = normalize_raw_fact_row(
            row,
            edinet_code="E00000",
            security_code="4888",
        )

        self.assertIsNotNone(normalized)
        assert normalized is not None
        self.assertEqual(normalized["metric_key"], "CostOfSalesCurrent")
        self.assertEqual(normalized["source_tag"], "CostOfFinishedGoodsSold")

    def test_goods_consignment_merchandise_cost_tag_maps_to_cost_of_sales(self) -> None:
        row = build_raw_fact(tag_name="GoodsConsignmentMerchandiseCostOfFinishedGoodsSoldCOS")

        normalized = normalize_raw_fact_row(
            row,
            edinet_code="E00000",
            security_code="4558",
        )

        self.assertIsNotNone(normalized)
        assert normalized is not None
        self.assertEqual(normalized["metric_key"], "CostOfSalesCurrent")
        self.assertEqual(
            normalized["source_tag"],
            "GoodsConsignmentMerchandiseCostOfFinishedGoodsSoldCOS",
        )

    def test_business_expenses_maps_to_combined_cost_and_sga(self) -> None:
        row = build_raw_fact(tag_name="BusinessExpenses")

        normalized = normalize_raw_fact_row(
            row,
            edinet_code="E00000",
            security_code="4579",
        )

        self.assertIsNotNone(normalized)
        assert normalized is not None
        self.assertEqual(
            normalized["metric_key"],
            "CostOfSalesAndSellingGeneralAndAdministrativeExpensesCurrent",
        )
        self.assertEqual(normalized["source_tag"], "BusinessExpenses")

    def test_banking_financing_expenses_maps_to_cost_of_sales(self) -> None:
        row = build_raw_fact(tag_name="FinancingExpensesOpeCFBNK")

        normalized = normalize_raw_fact_row(
            row,
            edinet_code="E00000",
            security_code="8306",
        )

        self.assertIsNotNone(normalized)
        assert normalized is not None
        self.assertEqual(normalized["metric_key"], "CostOfSalesCurrent")
        self.assertEqual(normalized["source_tag"], "FinancingExpensesOpeCFBNK")

    def test_banking_general_and_administrative_expenses_maps_to_selling_expenses(self) -> None:
        row = build_raw_fact(tag_name="GeneralAndAdministrativeExpensesOEBNK")

        normalized = normalize_raw_fact_row(
            row,
            edinet_code="E00000",
            security_code="8306",
        )

        self.assertIsNotNone(normalized)
        assert normalized is not None
        self.assertEqual(normalized["metric_key"], "SellingExpensesCurrent")
        self.assertEqual(normalized["source_tag"], "GeneralAndAdministrativeExpensesOEBNK")

    def test_banking_ordinary_expenses_maps_to_combined_cost_and_sga(self) -> None:
        row = build_raw_fact(tag_name="OrdinaryExpensesBNK")

        normalized = normalize_raw_fact_row(
            row,
            edinet_code="E00000",
            security_code="8306",
        )

        self.assertIsNotNone(normalized)
        assert normalized is not None
        self.assertEqual(
            normalized["metric_key"],
            "CostOfSalesAndSellingGeneralAndAdministrativeExpensesCurrent",
        )
        self.assertEqual(normalized["source_tag"], "OrdinaryExpensesBNK")

    def test_banking_income_tags_map_to_bank_specific_metrics(self) -> None:
        funding_row = build_raw_fact(tag_name="InterestIncomeOIBNK")
        fees_row = build_raw_fact(tag_name="FeesAndCommissionsOIBNK")

        funding = normalize_raw_fact_row(
            funding_row,
            edinet_code="E00000",
            security_code="8306",
        )
        fees = normalize_raw_fact_row(
            fees_row,
            edinet_code="E00000",
            security_code="8306",
        )

        self.assertIsNotNone(funding)
        self.assertIsNotNone(fees)
        assert funding is not None
        assert fees is not None
        self.assertEqual(funding["metric_key"], "FundingIncomeCurrent")
        self.assertEqual(fees["metric_key"], "FeesAndCommissionsIncomeCurrent")

    def test_insurance_specific_tags_map_to_expected_metrics(self) -> None:
        cases = [
            ("InsuranceClaimsAndOthersSummaryOfBusinessResults", "InsuranceClaimsPaymentsCurrent"),
            ("ProvisionOfPolicyReserveAndOtherOEINS", "PolicyReserveProvisionCurrent"),
            ("InvestmentExpensesOEINS", "InvestmentExpensesCurrent"),
            ("ProjectExpensesINS", "ProjectExpensesCurrent"),
            (
                "OperatingExpensesINS",
                "CostOfSalesAndSellingGeneralAndAdministrativeExpensesCurrent",
            ),
        ]

        for tag_name, expected_key in cases:
            with self.subTest(tag_name=tag_name):
                row = build_raw_fact(tag_name=tag_name)
                normalized = normalize_raw_fact_row(
                    row,
                    edinet_code="E00000",
                    security_code="7181",
                )
                self.assertIsNotNone(normalized)
                assert normalized is not None
                self.assertEqual(normalized["metric_key"], expected_key)

    def test_securities_financial_expenses_maps_to_cost_of_sales(self) -> None:
        row = build_raw_fact(tag_name="FinancialExpensesSEC")

        normalized = normalize_raw_fact_row(
            row,
            edinet_code="E00000",
            security_code="8604",
        )

        self.assertIsNotNone(normalized)
        assert normalized is not None
        self.assertEqual(normalized["metric_key"], "CostOfSalesCurrent")
        self.assertEqual(normalized["source_tag"], "FinancialExpensesSEC")

    def test_securities_expense_ifrs_maps_to_combined_cost_and_sga(self) -> None:
        row = build_raw_fact(tag_name="ExpenseIFRS")

        normalized = normalize_raw_fact_row(
            row,
            edinet_code="E00000",
            security_code="8473",
        )

        self.assertIsNotNone(normalized)
        assert normalized is not None
        self.assertEqual(
            normalized["metric_key"],
            "CostOfSalesAndSellingGeneralAndAdministrativeExpensesCurrent",
        )
        self.assertEqual(normalized["source_tag"], "ExpenseIFRS")

    def test_securities_net_revenue_maps_to_gross_profit(self) -> None:
        row = build_raw_fact(tag_name="NetRevenueSummaryOfBusinessResults")

        normalized = normalize_raw_fact_row(
            row,
            edinet_code="E00000",
            security_code="8604",
        )

        self.assertIsNotNone(normalized)
        assert normalized is not None
        self.assertEqual(normalized["metric_key"], "GrossProfitCurrent")
        self.assertEqual(normalized["source_tag"], "NetRevenueSummaryOfBusinessResults")

    def test_combined_cost_and_sga_prefers_total_operating_expenses_tag(self) -> None:
        rows = [
            build_raw_fact(
                doc_id="DOC2",
                tag_name="OperatingExpenses",
                value_text="100",
            ),
            build_raw_fact(
                doc_id="DOC2",
                tag_name="OperatingExpensesIFRS",
                value_text="120",
            ),
        ]

        normalized_rows = normalize_raw_fact_rows(
            rows,
            edinet_code="E00000",
            security_code="9432",
        )

        self.assertEqual(len(normalized_rows), 1)
        self.assertEqual(
            normalized_rows[0]["metric_key"],
            "CostOfSalesAndSellingGeneralAndAdministrativeExpensesCurrent",
        )
        self.assertEqual(normalized_rows[0]["source_tag"], "OperatingExpensesIFRS")
        self.assertEqual(normalized_rows[0]["value_num"], 120.0)

    def test_structure_priority_prefers_sga_heading_over_general_expense_when_tag_priority_ties(self) -> None:
        rows = [
            build_raw_fact(
                doc_id="DOC3",
                tag_name="GeneralAndAdministrativeExpenses",
                value_text="100",
            ),
            build_raw_fact(
                doc_id="DOC3",
                tag_name="GeneralAndAdministrativeExpensesSGA",
                value_text="120",
            ),
        ]

        tmp_dir = Path("tests") / "_tmp_metric_normalize_structure_priority"
        shutil.rmtree(tmp_dir, ignore_errors=True)
        tmp_dir.mkdir(parents=True, exist_ok=True)
        try:
            xbrl_path = tmp_dir / "sample.xbrl"
            xbrl_path.write_text("<xbrli:xbrl/>", encoding="utf-8")
            (tmp_dir / "sample_lab.xml").write_text(LAB_XML_FOR_PRIORITY, encoding="utf-8")

            normalized_rows = normalize_raw_fact_rows(
                rows,
                edinet_code="E00000",
                security_code="0000",
                xbrl_path=str(xbrl_path),
            )
        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)

        self.assertEqual(len(normalized_rows), 1)
        self.assertEqual(normalized_rows[0]["metric_key"], "SellingExpensesCurrent")
        self.assertEqual(normalized_rows[0]["source_tag"], "GeneralAndAdministrativeExpensesSGA")
        self.assertEqual(normalized_rows[0]["value_num"], 120.0)

    def test_build_normalization_candidates_exposes_structure_details(self) -> None:
        rows = [
            build_raw_fact(
                doc_id="DOC4",
                tag_name="GeneralAndAdministrativeExpenses",
                value_text="100",
            ),
            build_raw_fact(
                doc_id="DOC4",
                tag_name="GeneralAndAdministrativeExpensesSGA",
                value_text="120",
            ),
        ]

        tmp_dir = Path("tests") / "_tmp_metric_normalize_candidates"
        shutil.rmtree(tmp_dir, ignore_errors=True)
        tmp_dir.mkdir(parents=True, exist_ok=True)
        try:
            xbrl_path = tmp_dir / "sample.xbrl"
            xbrl_path.write_text("<xbrli:xbrl/>", encoding="utf-8")
            (tmp_dir / "sample_lab.xml").write_text(LAB_XML_FOR_PRIORITY, encoding="utf-8")

            candidates = build_normalization_candidates(
                rows,
                edinet_code="E00000",
                security_code="0000",
                xbrl_path=str(xbrl_path),
            )
        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)

        self.assertEqual(len(candidates), 2)
        sga_row = next(row for row in candidates if row["source_tag"] == "GeneralAndAdministrativeExpensesSGA")
        ga_row = next(row for row in candidates if row["source_tag"] == "GeneralAndAdministrativeExpenses")
        self.assertEqual(sga_row["_structure_role"], "expense")
        self.assertEqual(sga_row["_structure_confidence"], "high")
        self.assertLess(sga_row["_structure_priority"], ga_row["_structure_priority"])

    def test_usgaap_operating_cash_maps_to_operating_cash(self) -> None:
        row = build_raw_fact(
            tag_name="CashFlowsFromUsedInOperatingActivitiesUSGAAPSummaryOfBusinessResults",
        )

        normalized = normalize_raw_fact_row(
            row,
            edinet_code="E00000",
            security_code="7751",
        )

        self.assertIsNotNone(normalized)
        assert normalized is not None
        self.assertEqual(normalized["metric_key"], "OperatingCashCurrent")
        self.assertEqual(
            normalized["source_tag"],
            "CashFlowsFromUsedInOperatingActivitiesUSGAAPSummaryOfBusinessResults",
        )

    def test_usgaap_cash_and_cash_equivalents_maps_to_cash_and_cash_equivalents(self) -> None:
        row = build_raw_fact(
            tag_name="CashAndCashEquivalentsUSGAAPSummaryOfBusinessResults",
            context_ref="CurrentYearInstant",
            period_type="instant",
        )

        normalized = normalize_raw_fact_row(
            row,
            edinet_code="E00000",
            security_code="7751",
        )

        self.assertIsNotNone(normalized)
        assert normalized is not None
        self.assertEqual(normalized["metric_key"], "CashAndCashEquivalentsCurrent")
        self.assertEqual(
            normalized["source_tag"],
            "CashAndCashEquivalentsUSGAAPSummaryOfBusinessResults",
        )


if __name__ == "__main__":
    unittest.main()
