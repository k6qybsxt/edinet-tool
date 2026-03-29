from __future__ import annotations


RULE_NAME = "minimum_viable_value_check"
RULE_VERSION = "2026-03-29-v1"


def evaluate_minimum_viable_value_check(metrics: dict[str, dict]) -> dict:
    required_keys = [
        "NetSalesCurrent",
        "OperatingIncomeCurrent",
        "NetAssetsCurrent",
        "CashAndCashEquivalentsCurrent",
    ]

    missing_keys = [key for key in required_keys if key not in metrics]

    net_sales = metrics.get("NetSalesCurrent", {}).get("value_num")
    operating_income = metrics.get("OperatingIncomeCurrent", {}).get("value_num")
    net_assets = metrics.get("NetAssetsCurrent", {}).get("value_num")
    cash = metrics.get("CashAndCashEquivalentsCurrent", {}).get("value_num")

    result_flag = 1 if len(missing_keys) == 0 else 0

    detail = {
        "missing_keys": missing_keys,
        "checked_keys": required_keys,
        "values": {
            "NetSalesCurrent": net_sales,
            "OperatingIncomeCurrent": operating_income,
            "NetAssetsCurrent": net_assets,
            "CashAndCashEquivalentsCurrent": cash,
        },
    }

    return {
        "rule_name": RULE_NAME,
        "rule_version": RULE_VERSION,
        "result_flag": result_flag,
        "score": float(100 - len(missing_keys) * 25),
        "detail": detail,
    }