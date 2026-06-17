from __future__ import annotations

import sys
import unittest
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from edinet_monitor.services.quarter_source_policy_service import (  # noqa: E402
    actual_financial_source_for_quarter,
    uses_edinet_segment,
    uses_jquants_forecast,
)


class QuarterSourcePolicyServiceTest(unittest.TestCase):
    def test_actual_financial_source_is_fixed_by_quarter(self) -> None:
        self.assertEqual(actual_financial_source_for_quarter("1Q"), "jquants")
        self.assertEqual(actual_financial_source_for_quarter("3Q"), "jquants")
        self.assertEqual(actual_financial_source_for_quarter("2Q"), "edinet")
        self.assertEqual(actual_financial_source_for_quarter("4Q"), "edinet")
        self.assertIsNone(actual_financial_source_for_quarter("FY"))

    def test_segment_is_edinet_only_for_2q_and_4q(self) -> None:
        self.assertFalse(uses_edinet_segment("1Q"))
        self.assertTrue(uses_edinet_segment("2Q"))
        self.assertFalse(uses_edinet_segment("3Q"))
        self.assertTrue(uses_edinet_segment("4Q"))

    def test_forecast_stages_are_jquants(self) -> None:
        for stage in ("initial", "1Q", "2Q", "3Q"):
            self.assertTrue(uses_jquants_forecast(stage))
        self.assertFalse(uses_jquants_forecast("4Q"))


if __name__ == "__main__":
    unittest.main()
