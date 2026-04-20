from __future__ import annotations

import argparse
import unittest

from edinet_monitor.cli.run_normalization_change_workflow import validate_args


def build_args(**overrides: object) -> argparse.Namespace:
    values = {
        "doc_ids": [],
        "security_codes": [],
        "industry_33_list": [],
        "limit": 20,
        "allow_all": False,
        "apply": False,
        "before_snapshot": "",
    }
    values.update(overrides)
    return argparse.Namespace(**values)


class RunNormalizationChangeWorkflowTest(unittest.TestCase):
    def test_validate_args_requires_scope_or_guarded_allow_all(self) -> None:
        with self.assertRaises(SystemExit):
            validate_args(build_args())

        validate_args(build_args(allow_all=True, limit=1))

        with self.assertRaises(SystemExit):
            validate_args(build_args(allow_all=True, limit=0))

    def test_validate_args_requires_before_snapshot_when_applying(self) -> None:
        with self.assertRaises(SystemExit):
            validate_args(build_args(doc_ids=["S100TEST"], apply=True))

        validate_args(
            build_args(
                doc_ids=["S100TEST"],
                apply=True,
                before_snapshot="D:/work/before_taxonomy_change_20260420_000000",
            )
        )


if __name__ == "__main__":
    unittest.main()
