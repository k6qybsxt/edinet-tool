from __future__ import annotations


IFRS_US_GAAP_KEYWORDS = (
    "ifrs",
    "international",
    "usgaap",
    "us-gaap",
    "us gaap",
    "米国",
)


def is_ifrs_or_us_gaap(accounting_standard: str | None) -> bool:
    text = str(accounting_standard or "").strip().casefold()
    if not text:
        return False
    return any(keyword.casefold() in text for keyword in IFRS_US_GAAP_KEYWORDS)
