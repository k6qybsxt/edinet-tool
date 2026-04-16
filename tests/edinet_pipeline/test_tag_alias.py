import unittest

from edinet_pipeline.domain.tag_alias import normalize_tag_to_metric


class TagAliasTest(unittest.TestCase):
    def test_sector_topline_tags_map_to_net_sales(self) -> None:
        cases = [
            "NetSalesOfCompletedConstructionContractsCNS",
            "OperatingRevenuesSummaryOfBusinessResults",
            "BusinessRevenueSummaryOfBusinessResults",
            "RevenueIFRSSummaryOfBusinessResults",
            "OperatingRevenueSummaryOfBusinessResults",
            "OrdinaryIncomeBNK",
            "InsurancePremiumsAndOtherOIINS",
        ]

        for tag_name in cases:
            with self.subTest(tag_name=tag_name):
                self.assertEqual(normalize_tag_to_metric(tag_name), "NetSales")

    def test_sector_cost_tags_map_to_cost_of_sales(self) -> None:
        cases = [
            "CostOfSalesOfCompletedConstructionContractsCNS",
            "OperatingCost",
            "OperatingCost2",
            "OperatingExpensesAndCostOfSalesOfTransportationRWY",
            "TotalBusinessExpensesCOSExpOA",
            "CostOfBusinessRevenue",
            "CostOfBusinessRevenueBusinessExpenses",
            "CostOfBusinessRevenueCOSExpOA",
            "CostOfRawMaterialsCOS",
            "CostOfCompletedWorkCOSExpOA",
            "CostOfFinishedGoodsSold",
            "GoodsConsignmentMerchandiseCostOfFinishedGoodsSoldCOS",
            "FinancingExpensesOpeCFBNK",
            "FinancialExpensesSEC",
        ]

        for tag_name in cases:
            with self.subTest(tag_name=tag_name):
                self.assertEqual(normalize_tag_to_metric(tag_name), "CostOfSales")

    def test_sector_gross_profit_tags_map_to_gross_profit(self) -> None:
        cases = [
            "GrossProfit",
            "OperatingGrossProfit",
            "OperatingGrossProfitIFRS",
            "OperatingGrossProfitWAT",
            "OperatingGrossProfitNetGP",
            "GrossProfitOnCompletedConstructionContractsCNS",
            "BusinessGrossProfitOrLoss",
            "NetRevenueSummaryOfBusinessResults",
        ]

        for tag_name in cases:
            with self.subTest(tag_name=tag_name):
                self.assertEqual(normalize_tag_to_metric(tag_name), "GrossProfit")

    def test_combined_cost_and_sga_tags_map_to_combined_metric(self) -> None:
        cases = [
            "CostOfSalesAndSellingGeneralAndAdministrativeExpensesIFRS",
            "OperatingExpensesIFRS",
            "OperatingExpenses",
            "OperatingExpensesOILTelecommunications",
            "ElectricUtilityOperatingExpensesELE",
            "ElectricUtilityOperatingExpenses",
            "BusinessExpenses",
            "OperatingExpensesINS",
            "OperatingCostsAndExpensesCOSExpOA",
            "OrdinaryExpensesBNK",
            "ExpenseIFRS",
        ]

        for tag_name in cases:
            with self.subTest(tag_name=tag_name):
                self.assertEqual(
                    normalize_tag_to_metric(tag_name),
                    "CostOfSalesAndSellingGeneralAndAdministrativeExpenses",
                )

    def test_banking_tags_map_to_expected_metrics(self) -> None:
        cases = {
            "GeneralAndAdministrativeExpensesOEBNK": "SellingExpenses",
            "InterestIncomeOIBNK": "FundingIncome",
            "FeesAndCommissionsOIBNK": "FeesAndCommissionsIncome",
        }

        for tag_name, metric_name in cases.items():
            with self.subTest(tag_name=tag_name):
                self.assertEqual(normalize_tag_to_metric(tag_name), metric_name)

    def test_insurance_tags_map_to_expected_metrics(self) -> None:
        cases = {
            "InsuranceClaimsAndOthersSummaryOfBusinessResults": "InsuranceClaimsPayments",
            "ProvisionOfPolicyReserveAndOtherOEINS": "PolicyReserveProvision",
            "InvestmentExpensesOEINS": "InvestmentExpenses",
            "ProjectExpensesINS": "ProjectExpenses",
            "OperatingExpensesINS": "CostOfSalesAndSellingGeneralAndAdministrativeExpenses",
        }

        for tag_name, metric_name in cases.items():
            with self.subTest(tag_name=tag_name):
                self.assertEqual(normalize_tag_to_metric(tag_name), metric_name)

    def test_usgaap_cashflow_tags_map_to_cashflow_metrics(self) -> None:
        cases = {
            "CashFlowsFromUsedInOperatingActivitiesUSGAAPSummaryOfBusinessResults": "OperatingCash",
            "CashFlowsFromUsedInInvestingActivitiesUSGAAPSummaryOfBusinessResults": "InvestmentCash",
            "CashFlowsFromUsedInFinancingActivitiesUSGAAPSummaryOfBusinessResults": "FinancingCash",
            "CashAndCashEquivalentsUSGAAPSummaryOfBusinessResults": "CashAndCashEquivalents",
        }

        for tag_name, metric_name in cases.items():
            with self.subTest(tag_name=tag_name):
                self.assertEqual(normalize_tag_to_metric(tag_name), metric_name)


if __name__ == "__main__":
    unittest.main()
