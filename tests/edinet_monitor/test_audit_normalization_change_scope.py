from __future__ import annotations

import argparse
import unittest

from edinet_monitor.cli.audit_normalization_change_scope import (
    metric_bases_from_preview_rows,
    validate_args,
)


def build_args(**overrides: object) -> argparse.Namespace:
    values = {
        "doc_ids": [],
        "security_codes": [],
        "industry_33_list": [],
        "allow_all": False,
        "limit": 5,
    }
    values.update(overrides)
    return argparse.Namespace(**values)


class AuditNormalizationChangeScopeTest(unittest.TestCase):
    def test_validate_args_requires_scope_or_guarded_allow_all(self) -> None:
        with self.assertRaises(SystemExit):
            validate_args(build_args())

        validate_args(build_args(security_codes=["4613"]))
        validate_args(build_args(allow_all=True, limit=1))

        with self.assertRaises(SystemExit):
            validate_args(build_args(allow_all=True, limit=0))

    def test_metric_bases_from_preview_rows_uses_normalized_diffs_only(self) -> None:
        rows = [
            {
                "metric_source": "normalized_metrics",
                "change_type": "changed",
                "metric_key": "NetSalesCurrent",
            },
            {
                "metric_source": "normalized_metrics",
                "change_type": "added",
                "metric_key": "NetSalesPrior1",
            },
            {
                "metric_source": "derived_metrics",
                "change_type": "changed",
                "metric_key": "ROACurrent",
            },
            {
                "metric_source": "normalized_metrics",
                "change_type": "unchanged",
                "metric_key": "OperatingIncomeCurrent",
            },
        ]

        self.assertEqual(metric_bases_from_preview_rows(rows), ["NetSales"])


if __name__ == "__main__":
    unittest.main()
