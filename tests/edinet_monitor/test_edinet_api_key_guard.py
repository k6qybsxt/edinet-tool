from __future__ import annotations

import sys
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import patch


ROOT_DIR = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from edinet_monitor.services.collector.document_list_service import fetch_document_list  # noqa: E402
from edinet_monitor.services.collector.edinet_api_key_guard import (  # noqa: E402
    is_placeholder_edinet_api_key,
    validate_edinet_api_key,
)


class EdinetApiKeyGuardTest(unittest.TestCase):
    def test_placeholder_key_is_rejected(self) -> None:
        with self.assertRaises(RuntimeError) as context:
            validate_edinet_api_key("あなたのEDINET_APIキー")

        self.assertIn("プレースホルダー", str(context.exception))

    def test_realistic_key_is_returned_trimmed(self) -> None:
        self.assertEqual(validate_edinet_api_key("  real-api-key  "), "real-api-key")

    def test_placeholder_variants_are_detected(self) -> None:
        self.assertTrue(is_placeholder_edinet_api_key("YOUR_EDINET_API_KEY"))
        self.assertTrue(is_placeholder_edinet_api_key("あなたのEDINET APIキー"))

    def test_document_list_stops_before_http_request_for_placeholder(self) -> None:
        with patch("edinet_monitor.services.collector.document_list_service.requests.get") as request_get:
            with self.assertRaises(RuntimeError):
                fetch_document_list(target_date=date(2026, 4, 1), api_key="あなたのEDINET_APIキー")

        request_get.assert_not_called()


if __name__ == "__main__":
    unittest.main()
