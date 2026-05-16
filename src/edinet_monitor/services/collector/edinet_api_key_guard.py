from __future__ import annotations


PLACEHOLDER_EDINET_API_KEYS = {
    "あなたのEDINET_APIキー",
    "あなたのEDINET APIキー",
    "YOUR_EDINET_API_KEY",
    "your_edinet_api_key",
    "<EDINET_API_KEY>",
}


def is_placeholder_edinet_api_key(api_key: str | None) -> bool:
    text = str(api_key or "").strip()
    if not text:
        return False
    if text in PLACEHOLDER_EDINET_API_KEYS:
        return True
    normalized = text.lower().replace(" ", "").replace("-", "_")
    return normalized in {key.lower().replace(" ", "").replace("-", "_") for key in PLACEHOLDER_EDINET_API_KEYS}


def validate_edinet_api_key(api_key: str | None, *, allow_empty: bool = False) -> str:
    text = str(api_key or "").strip()
    if not text:
        if allow_empty:
            return ""
        raise RuntimeError("Set EDINET_API_KEY before running.")
    if is_placeholder_edinet_api_key(text):
        raise RuntimeError(
            "EDINET_API_KEY がプレースホルダーのままです。"
            "実際のEDINET APIキーを設定してから再実行してください。"
        )
    return text
