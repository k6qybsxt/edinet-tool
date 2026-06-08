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

    def test_canonical_segment_key_aliases_sony_renamed_entertainment_segment(self) -> None:
        old_key = canonical_segment_key(
            "E01777-000ElectronicsProductsAndSolutionsReportableSegmentMember",
            "E01777-000Electronics Products And Solutions",
        )
        current_key = canonical_segment_key(
            "jpcrp030000-asr_E01777-000:EntertainmentTechnologyAndServicesReportableSegmentMember",
            "\u30a8\u30f3\u30bf\u30c6\u30a4\u30f3\u30e1\u30f3\u30c8\u30fb"
            "\u30c6\u30af\u30ce\u30ed\u30b8\u30fc\uff06\u30b5\u30fc\u30d3\u30b9",
        )

        self.assertEqual(old_key, "entertainmenttechnologyandservices")
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


    def test_preferred_segment_name_map_prefers_japanese_label_for_sony_renamed_segment(self) -> None:
        preferred = preferred_segment_name_map(
            [
                SegmentNameCandidate(
                    edinet_code="E01777",
                    segment_kind="business",
                    member_qname="E01777-000ElectronicsProductsAndSolutionsReportableSegmentMember",
                    segment_name="E01777-000Electronics Products And Solutions",
                    period_end="2022-03-31",
                ),
                SegmentNameCandidate(
                    edinet_code="E01777",
                    segment_kind="business",
                    member_qname=(
                        "jpcrp030000-asr_E01777-000:"
                        "EntertainmentTechnologyAndServicesReportableSegmentMember"
                    ),
                    segment_name=(
                        "\u30a8\u30f3\u30bf\u30c6\u30a4\u30f3\u30e1\u30f3\u30c8\u30fb"
                        "\u30c6\u30af\u30ce\u30ed\u30b8\u30fc\uff06\u30b5\u30fc\u30d3\u30b9"
                    ),
                    period_end="2025-03-31",
                ),
            ]
        )

        self.assertEqual(
            preferred[("E01777", "business", "entertainmenttechnologyandservices")],
            "\u30a8\u30f3\u30bf\u30c6\u30a4\u30f3\u30e1\u30f3\u30c8\u30fb"
            "\u30c6\u30af\u30ce\u30ed\u30b8\u30fc\uff06\u30b5\u30fc\u30d3\u30b9",
        )


if __name__ == "__main__":
    unittest.main()
