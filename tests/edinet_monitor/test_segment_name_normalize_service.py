from __future__ import annotations

import unittest

from edinet_monitor.services.segment_name_normalize_service import (
    SegmentNameCandidate,
    canonical_segment_key,
    preferred_segment_name_map,
)


class SegmentNameNormalizeServiceTest(unittest.TestCase):
    def test_canonical_segment_key_matches_old_and_namespaced_member_qnames(self) -> None:
        old_key = canonical_segment_key(
            "E01777-000GameAndNetworkServicesReportableSegmentMember",
            "E01777-000Game And Network Services",
        )
        current_key = canonical_segment_key(
            "jpcrp030000-asr_E01777-000:GameAndNetworkServicesReportableSegmentMember",
            "ゲーム＆ネットワークサービス",
        )

        self.assertEqual(old_key, "gameandnetworkservices")
        self.assertEqual(current_key, old_key)

    def test_preferred_segment_name_map_prefers_japanese_label_for_same_member(self) -> None:
        preferred = preferred_segment_name_map(
            [
                SegmentNameCandidate(
                    edinet_code="E01777",
                    segment_kind="business",
                    member_qname="E01777-000GameAndNetworkServicesReportableSegmentMember",
                    segment_name="E01777-000Game And Network Services",
                    period_end="2022-03-31",
                ),
                SegmentNameCandidate(
                    edinet_code="E01777",
                    segment_kind="business",
                    member_qname="jpcrp030000-asr_E01777-000:GameAndNetworkServicesReportableSegmentMember",
                    segment_name="ゲーム＆ネットワークサービス",
                    period_end="2025-03-31",
                ),
            ]
        )

        self.assertEqual(
            preferred[("E01777", "business", "gameandnetworkservices")],
            "ゲーム＆ネットワークサービス",
        )


if __name__ == "__main__":
    unittest.main()
