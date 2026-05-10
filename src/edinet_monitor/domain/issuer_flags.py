from __future__ import annotations

from typing import Any


TENBAGGER_LEARNING_SECURITY_CODES: frozenset[str] = frozenset(
    {
        "1514",
        "2334",
        "2413",
        "2931",
        "3038",
        "3350",
        "3778",
        "3923",
        "3936",
        "4107",
        "4613",
        "5801",
        "5803",
        "6016",
        "6035",
        "6085",
        "6227",
        "6405",
        "6507",
        "6573",
        "6574",
        "6777",
        "6834",
        "6920",
        "6946",
        "7003",
        "7011",
        "7014",
        "7564",
        "7692",
        "7936",
        "8105",
        "9560",
        "9666",
    }
)


def normalize_security_code_for_flag(value: Any) -> str:
    text = str(value or "").strip()
    digits = "".join(ch for ch in text if ch.isdigit())
    if len(digits) >= 5 and digits.endswith("0"):
        return digits[:4]
    if len(digits) >= 4:
        return digits[:4]
    return digits


def is_tenbagger_learning_security(value: Any) -> bool:
    return normalize_security_code_for_flag(value) in TENBAGGER_LEARNING_SECURITY_CODES


def tenbagger_learning_mark(value: Any) -> str:
    return "〇" if is_tenbagger_learning_security(value) else ""
