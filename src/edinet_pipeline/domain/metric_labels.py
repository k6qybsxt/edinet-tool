from __future__ import annotations

from edinet_pipeline.domain.tag_alias import normalize_tag_to_metric


METRIC_BASE_LABELS = {
    "NetSales": "\u58f2\u4e0a\u9ad8",
    "CostOfSales": "\u58f2\u4e0a\u539f\u4fa1",
    "CostOfSalesAndSellingGeneralAndAdministrativeExpenses": "\u58f2\u4e0a\u539f\u4fa1\u4e26\u3073\u306b\u8ca9\u58f2\u8cbb\u53ca\u3073\u4e00\u822c\u7ba1\u7406\u8cbb",
    "SellingExpenses": "\u8ca9\u7ba1\u8cbb",
    "OperatingIncome": "\u55b6\u696d\u5229\u76ca",
    "OrdinaryIncome": "\u7d4c\u5e38\u5229\u76ca",
    "ProfitLoss": "\u7d14\u5229\u76ca",
    "OperatingCash": "\u55b6\u696dCF",
    "InvestmentCash": "\u6295\u8cc7CF",
    "FinancingCash": "\u8ca1\u52d9CF",
    "TotalAssets": "\u7dcf\u8cc7\u7523",
    "NetAssets": "\u7d14\u8cc7\u7523",
    "CashAndCashEquivalents": "\u671f\u672b\u6b8b",
    "IssuedShares": "\u767a\u884c\u6e08\u682a\u5f0f\u6570",
    "TreasuryShares": "\u81ea\u5df1\u682a\u5f0f\u6570",
    "OutstandingShares": "\u767a\u884c\u682a\u6570",
    "NetSalesGrowthRate": "\u58f2\u4e0a\u9ad8\u6210\u9577\u7387",
    "OrdinaryIncomeGrowthRate": "\u7d4c\u5e38\u5229\u76ca\u6210\u9577\u7387",
    "CashBalanceGrowthRate": "\u73fe\u91d1\u6b8b\u9ad8\u6210\u9577\u7387",
    "GrossProfit": "\u58f2\u4e0a\u7dcf\u5229\u76ca",
    "CostOfSalesRatio": "\u58f2\u4e0a\u539f\u4fa1\u7387",
    "GrossProfitMargin": "\u58f2\u4e0a\u7dcf\u5229\u76ca\u7387",
    "SellingExpensesRatio": "\u8ca9\u7ba1\u8cbb\u7387",
    "OperatingMargin": "\u55b6\u696d\u5229\u76ca\u7387",
    "OrdinaryIncomeMargin": "\u7d4c\u5e38\u5229\u76ca\u7387",
    "EstimatedNetIncome": "\u63a8\u5b9a\u7d14\u5229\u76ca(\u7d4c\u5e38\u5229\u76ca*0.7)",
    "EstimatedNetMargin": "\u7d14\u5229\u76ca\u7387",
    "ROA": "ROA",
    "ROE": "ROE",
    "EquityRatio": "\u81ea\u5df1\u8cc7\u672c\u6bd4\u7387",
    "FCF": "FCF",
}

METRIC_GROUP_LABELS = {
    "growth": "\u6210\u9577",
    "profitability": "\u53ce\u76ca\u6027",
    "safety": "\u5b89\u5168\u6027",
    "cashflow": "\u30ad\u30e3\u30c3\u30b7\u30e5\u30d5\u30ed\u30fc",
    "efficiency": "\u52b9\u7387\u6027",
    "estimated": "\u63a8\u5b9a",
    "return": "\u53ce\u76ca\u6307\u6a19",
    "share": "\u682a\u5f0f",
    "dummy": "\u30c6\u30b9\u30c8",
}

METRIC_SUFFIX_LABELS = {
    "Current": "\u5f53\u671f",
    "Prior1": "\u524d\u671f",
    "Prior2": "\u524d\u3005\u671f",
    "Prior3": "3\u671f\u524d",
    "Prior4": "4\u671f\u524d",
}

_SORTED_SUFFIXES = sorted(METRIC_SUFFIX_LABELS.keys(), key=len, reverse=True)


def metric_base_to_display_name(metric_base: str | None) -> str:
    text = str(metric_base or "").strip()
    if not text:
        return ""
    return METRIC_BASE_LABELS.get(text, text)


def metric_group_to_display_name(metric_group: str | None) -> str:
    text = str(metric_group or "").strip()
    if not text:
        return ""
    return METRIC_GROUP_LABELS.get(text, text)


def split_metric_key(metric_key: str | None) -> tuple[str, str | None]:
    text = str(metric_key or "").strip()
    if not text:
        return "", None

    for suffix in _SORTED_SUFFIXES:
        if text.endswith(suffix) and len(text) > len(suffix):
            return text[: -len(suffix)], suffix

    return text, None


def metric_key_to_display_name(metric_key: str | None) -> str:
    base_key, suffix = split_metric_key(metric_key)
    base_label = metric_base_to_display_name(base_key)
    if not suffix:
        return base_label
    suffix_label = METRIC_SUFFIX_LABELS.get(suffix, suffix)
    return f"{base_label}\uff08{suffix_label}\uff09"


def tag_name_to_display_name(tag_name: str | None) -> str:
    text = str(tag_name or "").strip()
    if not text:
        return ""

    normalized = normalize_tag_to_metric(text)
    if normalized:
        return metric_base_to_display_name(normalized)
    return text
