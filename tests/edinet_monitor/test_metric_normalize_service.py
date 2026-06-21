from __future__ import annotations

import unittest
from pathlib import Path
import shutil

from edinet_monitor.services.normalizer.metric_normalize_service import (
    build_normalization_candidates,
    normalize_raw_fact_row,
    normalize_raw_fact_rows,
)


def build_raw_fact(*, doc_id: str = "DOC1", tag_name: str, value_text: str = "123", context_ref: str = "CurrentYearDuration_ConsolidatedMember", period_type: str = "duration", period_start: str | None = "2024-04-01", period_end: str | None = "2025-03-31", instant_date: str | None = None, consolidation: str = "Consolidated", unit_ref: str = "JPY", context_dimensions_json: str = "", unit_measures_json: str = "") -> dict:
    return {
        "doc_id": doc_id,
        "tag_name": tag_name,
        "context_ref": context_ref,
        "unit_ref": unit_ref,
        "period_type": period_type,
        "period_start": period_start,
        "period_end": period_end,
        "instant_date": instant_date,
        "consolidation": consolidation,
        "context_dimensions_json": context_dimensions_json,
        "unit_measures_json": unit_measures_json,
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

    def test_negative_zero_is_stored_as_plain_zero(self) -> None:
        row = build_raw_fact(
            tag_name="NetCashProvidedByUsedInFinancingActivitiesSummaryOfBusinessResults",
            value_text="-0",
        )

        normalized = normalize_raw_fact_row(
            row,
            edinet_code="E00000",
            security_code="9999",
        )

        self.assertIsNotNone(normalized)
        assert normalized is not None
        self.assertEqual(normalized["value_num"], 0.0)

    def test_equity_ratio_percent_tag_is_normalized_to_ratio(self) -> None:
        row = build_raw_fact(
            tag_name="EquityToAssetRatioSummaryOfBusinessResults",
            value_text="42.5",
            context_ref="CurrentYearInstant_ConsolidatedMember",
            period_type="instant",
            period_start=None,
            period_end=None,
            instant_date="2025-03-31",
            unit_ref="pure",
        )

        normalized = normalize_raw_fact_row(
            row,
            edinet_code="E00000",
            security_code="9501",
        )

        self.assertIsNotNone(normalized)
        assert normalized is not None
        self.assertEqual(normalized["metric_key"], "EquityRatioCurrent")
        self.assertEqual(normalized["value_num"], 0.425)

    def test_equity_ratio_ratio_tag_is_kept_as_ratio(self) -> None:
        row = build_raw_fact(
            tag_name="EquityToAssetRatioSummaryOfBusinessResults",
            value_text="0.43",
            context_ref="CurrentYearInstant_ConsolidatedMember",
            period_type="instant",
            period_start=None,
            period_end=None,
            instant_date="2025-03-31",
            unit_ref="pure",
        )

        normalized = normalize_raw_fact_row(
            row,
            edinet_code="E00000",
            security_code="9501",
        )

        self.assertIsNotNone(normalized)
        assert normalized is not None
        self.assertEqual(normalized["metric_key"], "EquityRatioCurrent")
        self.assertEqual(normalized["value_num"], 0.43)

    def test_equity_ratio_tag_is_rejected_when_still_out_of_range_after_percent_conversion(self) -> None:
        row = build_raw_fact(
            tag_name="EquityToAssetRatioIFRSSummaryOfBusinessResults",
            value_text="1357.63",
            context_ref="CurrentYearInstant_ConsolidatedMember",
            period_type="instant",
            period_start=None,
            period_end=None,
            instant_date="2025-03-31",
            unit_ref="pure",
        )

        normalized = normalize_raw_fact_row(
            row,
            edinet_code="E00000",
            security_code="6758",
        )

        self.assertIsNone(normalized)

    def test_usgaap_parent_equity_summary_maps_to_net_assets(self) -> None:
        row = build_raw_fact(
            tag_name="EquityAttributableToOwnersOfParentUSGAAPSummaryOfBusinessResults",
            value_text="3380273000000",
            context_ref="CurrentYearInstant",
            period_type="instant",
            period_start=None,
            period_end=None,
            instant_date="2024-12-31",
            unit_ref="JPY",
        )

        normalized = normalize_raw_fact_row(
            row,
            edinet_code="E00000",
            security_code="7751",
        )

        self.assertIsNotNone(normalized)
        assert normalized is not None
        self.assertEqual(normalized["metric_key"], "NetAssetsCurrent")
        self.assertEqual(normalized["value_num"], 3380273000000.0)

    def test_issued_shares_prefers_filing_date_tag_over_voting_rights_candidate(self) -> None:
        rows = [
            build_raw_fact(
                tag_name="NumberOfSharesIssuedSharesVotingRights",
                value_text="39783000",
                context_ref="CurrentYearInstant_ConsolidatedMember",
                period_type="instant",
                period_start=None,
                period_end=None,
                instant_date="2024-03-31",
                consolidation="Consolidated",
                unit_ref="shares",
            ),
            build_raw_fact(
                tag_name="NumberOfIssuedSharesAsOfFiscalYearEndIssuedSharesTotalNumberOfSharesEtc",
                value_text="1261232000",
                context_ref="FilingDateInstant",
                period_type="instant",
                period_start=None,
                period_end=None,
                instant_date="2024-06-25",
                consolidation="NonConsolidated",
                unit_ref="shares",
            ),
        ]

        normalized_rows = normalize_raw_fact_rows(
            rows,
            edinet_code="E00000",
            security_code="6758",
            filing_period_end="2024-03-31",
        )

        issued = [row for row in normalized_rows if row["metric_key"] == "IssuedSharesCurrent"]
        self.assertEqual(len(issued), 1)
        self.assertEqual(issued[0]["value_num"], 1261232000.0)
        self.assertEqual(
            issued[0]["source_tag"],
            "NumberOfIssuedSharesAsOfFiscalYearEndIssuedSharesTotalNumberOfSharesEtc",
        )
        self.assertEqual(issued[0]["period_end"], "2024-03-31")

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

    def test_general_and_administrative_expenses_maps_to_separate_metric(self) -> None:
        row = build_raw_fact(tag_name="GeneralAndAdministrativeExpensesSGA")

        normalized = normalize_raw_fact_row(
            row,
            edinet_code="E00000",
            security_code="9534",
        )

        self.assertIsNotNone(normalized)
        assert normalized is not None
        self.assertEqual(normalized["metric_key"], "GeneralAndAdministrativeExpensesCurrent")
        self.assertEqual(normalized["source_tag"], "GeneralAndAdministrativeExpensesSGA")

    def test_gas_sga_tags_are_limited_to_electric_and_gas_industry(self) -> None:
        row = build_raw_fact(tag_name="SellingGeneralAndAdministrativeExpensesGAS")

        blocked = normalize_raw_fact_row(
            row,
            edinet_code="E00000",
            security_code="9534",
            industry_33="サービス業",
        )
        normalized = normalize_raw_fact_row(
            row,
            edinet_code="E00000",
            security_code="9534",
            industry_33="電気・ガス業",
        )

        self.assertIsNone(blocked)
        self.assertIsNotNone(normalized)
        assert normalized is not None
        self.assertEqual(normalized["metric_key"], "SellingExpensesCurrent")
        self.assertEqual(normalized["source_tag"], "SellingGeneralAndAdministrativeExpensesGAS")

    def test_profit_before_tax_tag_is_saved_as_profit_before_tax_only(self) -> None:
        row = build_raw_fact(tag_name="IncomeBeforeIncomeTaxes", value_text="1000")

        candidates = build_normalization_candidates(
            [row],
            edinet_code="E00000",
            security_code="1234",
        )
        by_key = {candidate["metric_key"]: candidate for candidate in candidates}

        self.assertIn("ProfitBeforeTaxCurrent", by_key)
        self.assertNotIn("OrdinaryIncomeCurrent", by_key)
        self.assertEqual(by_key["ProfitBeforeTaxCurrent"]["value_num"], 1000.0)

    def test_average_age_months_are_converted_to_years(self) -> None:
        row = build_raw_fact(
            tag_name="AverageAgeMonthsInformationAboutReportingCompanyInformationAboutEmployees",
            value_text="420",
            context_ref="CurrentYearInstant_ConsolidatedMember",
            period_type="instant",
            period_start=None,
            period_end=None,
            instant_date="2025-03-31",
            unit_ref="pure",
        )

        normalized = normalize_raw_fact_row(
            row,
            edinet_code="E00000",
            security_code="1234",
        )

        self.assertIsNotNone(normalized)
        assert normalized is not None
        self.assertEqual(normalized["metric_key"], "AverageAgeCurrent")
        self.assertEqual(normalized["value_num"], 35.0)

    def test_employee_metrics_accept_pure_unit_and_nonconsolidated_profile_values(self) -> None:
        rows = [
            build_raw_fact(
                tag_name="NumberOfEmployees",
                value_text="17414",
                context_ref="CurrentYearInstant",
                period_type="instant",
                period_start=None,
                period_end=None,
                instant_date="2025-03-31",
                consolidation="Consolidated",
                unit_ref="pure",
            ),
            build_raw_fact(
                tag_name="AverageAgeYearsInformationAboutReportingCompanyInformationAboutEmployees",
                value_text="42.5",
                context_ref="CurrentYearInstant_NonConsolidatedMember",
                period_type="instant",
                period_start=None,
                period_end=None,
                instant_date="2025-03-31",
                consolidation="NonConsolidated",
                unit_ref="pure",
            ),
            build_raw_fact(
                tag_name="AverageLengthOfServiceYearsInformationAboutReportingCompanyInformationAboutEmployees",
                value_text="18.7",
                context_ref="CurrentYearInstant_NonConsolidatedMember",
                period_type="instant",
                period_start=None,
                period_end=None,
                instant_date="2025-03-31",
                consolidation="NonConsolidated",
                unit_ref="pure",
            ),
            build_raw_fact(
                tag_name="AverageAnnualSalaryInformationAboutReportingCompanyInformationAboutEmployees",
                value_text="8448000",
                context_ref="CurrentYearInstant_NonConsolidatedMember",
                period_type="instant",
                period_start=None,
                period_end=None,
                instant_date="2025-03-31",
                consolidation="NonConsolidated",
                unit_ref="JPY",
            ),
        ]

        normalized_rows = normalize_raw_fact_rows(
            rows,
            edinet_code="E00893",
            security_code="4613",
            enforce_candidate_validation=True,
        )
        by_key = {row["metric_key"]: row for row in normalized_rows}

        self.assertEqual(by_key["NumberOfEmployeesCurrent"]["value_num"], 17414.0)
        self.assertEqual(by_key["AverageAgeCurrent"]["value_num"], 42.5)
        self.assertEqual(by_key["AverageLengthOfServiceCurrent"]["value_num"], 18.7)
        self.assertEqual(by_key["AverageAnnualSalaryCurrent"]["value_num"], 8448000.0)

    def test_half_report_skips_disabled_employee_metrics(self) -> None:
        rows = [
            build_raw_fact(
                tag_name="NumberOfEmployees",
                value_text="17414",
                context_ref="CurrentYearInstant",
                period_type="instant",
                period_start=None,
                period_end=None,
                instant_date="2025-09-30",
                consolidation="Consolidated",
                unit_ref="pure",
            ),
            build_raw_fact(
                tag_name="AverageAgeYearsInformationAboutReportingCompanyInformationAboutEmployees",
                value_text="42.5",
                context_ref="CurrentYearInstant_NonConsolidatedMember",
                period_type="instant",
                period_start=None,
                period_end=None,
                instant_date="2025-09-30",
                consolidation="NonConsolidated",
                unit_ref="pure",
            ),
            build_raw_fact(
                tag_name="AverageLengthOfServiceYearsInformationAboutReportingCompanyInformationAboutEmployees",
                value_text="18.7",
                context_ref="CurrentYearInstant_NonConsolidatedMember",
                period_type="instant",
                period_start=None,
                period_end=None,
                instant_date="2025-09-30",
                consolidation="NonConsolidated",
                unit_ref="pure",
            ),
            build_raw_fact(
                tag_name="AverageAnnualSalaryInformationAboutReportingCompanyInformationAboutEmployees",
                value_text="8448000",
                context_ref="CurrentYearInstant_NonConsolidatedMember",
                period_type="instant",
                period_start=None,
                period_end=None,
                instant_date="2025-09-30",
                consolidation="NonConsolidated",
                unit_ref="JPY",
            ),
        ]

        normalized_rows = normalize_raw_fact_rows(
            rows,
            edinet_code="E00893",
            security_code="4613",
            filing_period_end="2025-09-30",
            form_type="043A00",
            enforce_candidate_validation=True,
        )
        keys = {row["metric_key"] for row in normalized_rows}

        self.assertNotIn("NumberOfEmployeesCurrent", keys)
        self.assertNotIn("AverageAgeCurrent", keys)
        self.assertNotIn("AverageLengthOfServiceCurrent", keys)
        self.assertNotIn("AverageAnnualSalaryCurrent", keys)

    def test_gas_supply_and_sales_expenses_maps_to_selling_expenses_only(self) -> None:
        row = build_raw_fact(tag_name="SupplyAndSalesExpensesGAS")

        normalized = normalize_raw_fact_row(
            row,
            edinet_code="E00000",
            security_code="9534",
            industry_33="電気・ガス業",
        )

        self.assertIsNotNone(normalized)
        assert normalized is not None
        self.assertEqual(normalized["metric_key"], "SellingExpensesOnlyCurrent")
        self.assertEqual(normalized["source_tag"], "SupplyAndSalesExpensesGAS")

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
        self.assertEqual(normalized_rows[0]["metric_key"], "GeneralAndAdministrativeExpensesCurrent")
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

    def test_consolidated_reporting_guard_excludes_nonconsolidated_pl_candidates(self) -> None:
        rows = [
            build_raw_fact(
                doc_id="DOC5",
                tag_name="NetSales",
                value_text="1000",
            ),
            build_raw_fact(
                doc_id="DOC5",
                tag_name="GeneralAndAdministrativeExpensesSGA",
                value_text="120",
                context_ref="CurrentYearDuration_NonConsolidatedMember",
                consolidation="NonConsolidated",
            ),
        ]

        normalized_rows = normalize_raw_fact_rows(
            rows,
            edinet_code="E00000",
            security_code="7199",
            enforce_candidate_validation=True,
        )

        self.assertEqual([row["metric_key"] for row in normalized_rows], ["NetSalesCurrent"])

    def test_consolidated_reporting_guard_keeps_standalone_nonconsolidated_candidate(self) -> None:
        rows = [
            build_raw_fact(
                doc_id="DOC6",
                tag_name="GeneralAndAdministrativeExpensesSGA",
                value_text="120",
                context_ref="CurrentYearDuration_NonConsolidatedMember",
                consolidation="NonConsolidated",
            ),
        ]

        normalized_rows = normalize_raw_fact_rows(
            rows,
            edinet_code="E00000",
            security_code="9636",
            enforce_candidate_validation=True,
        )

        self.assertEqual(len(normalized_rows), 1)
        self.assertEqual(normalized_rows[0]["metric_key"], "GeneralAndAdministrativeExpensesCurrent")
        self.assertEqual(normalized_rows[0]["consolidation"], "NonConsolidated")

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

    def test_period_fallback_is_disabled_by_default(self) -> None:
        row = build_raw_fact(
            tag_name="NetSales",
            context_ref="CustomAnnualDuration",
            period_start="2024-04-01",
            period_end="2025-03-31",
        )

        normalized = normalize_raw_fact_row(
            row,
            edinet_code="E00000",
            security_code="9501",
            filing_period_end="2025-03-31",
        )

        self.assertIsNone(normalized)

    def test_period_fallback_maps_current_and_prior_duration_rows_when_enabled(self) -> None:
        rows = [
            build_raw_fact(
                doc_id="DOC5",
                tag_name="NetSales",
                value_text="200",
                context_ref="CustomAnnualDurationCurrent",
                period_start="2024-04-01",
                period_end="2025-03-31",
            ),
            build_raw_fact(
                doc_id="DOC5",
                tag_name="NetSales",
                value_text="180",
                context_ref="CustomAnnualDurationPrior",
                period_start="2023-04-01",
                period_end="2024-03-31",
            ),
        ]

        normalized_rows = normalize_raw_fact_rows(
            rows,
            edinet_code="E00000",
            security_code="9501",
            filing_period_end="2025-03-31",
            enable_period_fallback=True,
        )

        by_key = {row["metric_key"]: row for row in normalized_rows}
        self.assertEqual(by_key["NetSalesCurrent"]["value_num"], 200.0)
        self.assertEqual(by_key["NetSalesPrior1"]["value_num"], 180.0)

    def test_period_fallback_maps_instant_rows_when_enabled(self) -> None:
        row = build_raw_fact(
            tag_name="CashAndCashEquivalents",
            context_ref="CustomInstantContext",
            period_type="instant",
            period_start=None,
            period_end=None,
            instant_date="2024-03-31",
        )

        normalized = normalize_raw_fact_row(
            row,
            edinet_code="E00000",
            security_code="9501",
            filing_period_end="2025-03-31",
            enable_period_fallback=True,
        )

        self.assertIsNotNone(normalized)
        assert normalized is not None
        self.assertEqual(normalized["metric_key"], "CashAndCashEquivalentsPrior1")

    def test_period_fallback_ignores_short_duration_rows(self) -> None:
        row = build_raw_fact(
            tag_name="NetSales",
            context_ref="CustomQuarterDuration",
            period_start="2025-01-01",
            period_end="2025-03-31",
        )

        normalized = normalize_raw_fact_row(
            row,
            edinet_code="E00000",
            security_code="9501",
            filing_period_end="2025-03-31",
            enable_period_fallback=True,
        )

        self.assertIsNone(normalized)

    def test_half_period_fallback_accepts_half_year_duration_rows(self) -> None:
        row = build_raw_fact(
            tag_name="NetSales",
            context_ref="CustomHalfDuration",
            period_start="2025-04-01",
            period_end="2025-09-30",
        )

        normalized = normalize_raw_fact_row(
            row,
            edinet_code="E00000",
            security_code="9501",
            filing_period_end="2025-09-30",
            form_type="043A00",
            enable_period_fallback=True,
        )

        self.assertIsNotNone(normalized)
        assert normalized is not None
        self.assertEqual(normalized["metric_key"], "NetSalesCurrent")

    def test_half_period_fallback_maps_prior_year_same_half_end_to_prior1(self) -> None:
        row = build_raw_fact(
            tag_name="NetSales",
            context_ref="CustomHalfDuration",
            period_start="2024-04-01",
            period_end="2024-09-30",
        )

        normalized = normalize_raw_fact_row(
            row,
            edinet_code="E00000",
            security_code="9501",
            filing_period_end="2025-09-30",
            form_type="043A00",
            enable_period_fallback=True,
        )

        self.assertIsNotNone(normalized)
        assert normalized is not None
        self.assertEqual(normalized["metric_key"], "NetSalesPrior1")

    def test_candidate_validation_is_audit_only_by_default(self) -> None:
        row = build_raw_fact(
            tag_name="NetSales",
            context_dimensions_json=(
                '{"axis_members":{"jpcrp_cor:OperatingSegmentsAxis":["ext:PaintBusinessMember"]}}'
            ),
        )

        normalized = normalize_raw_fact_row(
            row,
            edinet_code="E00000",
            security_code="9501",
        )

        self.assertIsNotNone(normalized)
        assert normalized is not None
        self.assertEqual(normalized["metric_key"], "NetSalesCurrent")
        self.assertEqual(normalized["_candidate_validation_status"], "EXCLUDE")
        self.assertIn("detail_dimension_candidate", normalized["_candidate_validation_issues"])

    def test_candidate_validation_can_exclude_detail_dimensions_when_enforced(self) -> None:
        rows = [
            build_raw_fact(
                tag_name="NetSales",
                value_text="100",
                context_dimensions_json=(
                    '{"axis_members":{"jpcrp_cor:OperatingSegmentsAxis":["ext:PaintBusinessMember"]}}'
                ),
            ),
            build_raw_fact(tag_name="NetSales", value_text="200"),
        ]

        normalized_rows = normalize_raw_fact_rows(
            rows,
            edinet_code="E00000",
            security_code="9501",
            enforce_candidate_validation=True,
        )

        self.assertEqual(len(normalized_rows), 1)
        self.assertEqual(normalized_rows[0]["value_num"], 200.0)

    def test_candidate_validation_flags_schema_period_type_mismatch(self) -> None:
        row = build_raw_fact(tag_name="NetSales")

        normalized = normalize_raw_fact_row(
            row,
            edinet_code="E00000",
            security_code="9501",
            structure_map={
                "NetSales": {
                    "schema": {
                        "type": "xbrli:monetaryItemType",
                        "period_type": "instant",
                    }
                }
            },
        )

        self.assertIsNotNone(normalized)
        assert normalized is not None
        self.assertEqual(normalized["_candidate_validation_status"], "EXCLUDE")
        self.assertIn("schema_period_type_mismatch", normalized["_candidate_validation_issues"])

    def test_candidate_validation_can_exclude_wrong_unit_when_enforced(self) -> None:
        row = build_raw_fact(
            tag_name="IssuedShares",
            period_type="instant",
            context_ref="CurrentYearInstant",
            period_start=None,
            instant_date="2025-03-31",
            unit_ref="JPY",
            unit_measures_json='{"measures":["iso4217:JPY"]}',
        )

        normalized = normalize_raw_fact_row(
            row,
            edinet_code="E00000",
            security_code="9501",
            structure_map={"IssuedShares": {"schema": {"type": "xbrli:sharesItemType"}}},
            enforce_candidate_validation=True,
        )

        self.assertIsNone(normalized)

    def test_enforced_validation_prefers_consolidated_jpy_translation_over_nonconsolidated_jpy(self) -> None:
        rows = [
            build_raw_fact(
                tag_name="RevenueIFRSSummaryOfBusinessResults",
                value_text="4581232000",
                unit_ref="USD",
            ),
            build_raw_fact(
                tag_name="RevenueKeyFinancialData",
                value_text="717100000000",
                unit_ref="JPY",
            ),
            build_raw_fact(
                tag_name="NetSalesSummaryOfBusinessResults",
                value_text="502737000000",
                context_ref="CurrentYearDuration_NonConsolidatedMember",
                consolidation="NonConsolidated",
                unit_ref="JPY",
            ),
        ]

        normalized_rows = normalize_raw_fact_rows(
            rows,
            edinet_code="E01725",
            security_code="6269",
            enforce_candidate_validation=True,
        )

        self.assertEqual(len(normalized_rows), 1)
        self.assertEqual(normalized_rows[0]["metric_key"], "NetSalesCurrent")
        self.assertEqual(normalized_rows[0]["source_tag"], "RevenueKeyFinancialData")
        self.assertEqual(normalized_rows[0]["value_num"], 717100000000.0)
        self.assertEqual(normalized_rows[0]["consolidation"], "Consolidated")

    def test_enforced_validation_does_not_fall_back_to_nonconsolidated_jpy_when_consolidated_is_foreign_currency(self) -> None:
        rows = [
            build_raw_fact(
                tag_name="CostOfSalesIFRS",
                value_text="4022553000",
                unit_ref="USD",
            ),
            build_raw_fact(
                tag_name="CostOfSales",
                value_text="473162000000",
                context_ref="CurrentYearDuration_NonConsolidatedMember",
                consolidation="NonConsolidated",
                unit_ref="JPY",
            ),
        ]

        normalized_rows = normalize_raw_fact_rows(
            rows,
            edinet_code="E01725",
            security_code="6269",
            enforce_candidate_validation=True,
        )

        self.assertEqual(normalized_rows, [])


if __name__ == "__main__":
    unittest.main()
