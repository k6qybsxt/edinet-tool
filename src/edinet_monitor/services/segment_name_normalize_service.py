from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import re
from typing import Iterable


SEGMENT_MEMBER_SUFFIXES = (
    "ReportableSegmentsMember",
    "ReportableSegmentMember",
    "OperatingSegmentsMember",
    "OperatingSegmentMember",
    "BusinessUnitReportableSegmentsMember",
    "BusinessUnitReportableSegmentMember",
    "BusinessMember",
    "Member",
)
SEGMENT_KEY_ALIASES = {
    "electronicsproductsandsolutions": "entertainmenttechnologyandservices",
}


@dataclass(frozen=True)
class SegmentNameCandidate:
    edinet_code: str
    segment_kind: str
    member_qname: str
    segment_name: str
    period_end: str = ""


def _local_name(qname: str) -> str:
    text = str(qname or "").strip()
    if ":" in text:
        return text.rsplit(":", 1)[-1]
    if "}" in text:
        return text.rsplit("}", 1)[-1]
    return text


def _normalize_key_text(text: str) -> str:
    return re.sub(r"[^0-9A-Za-z]+", "", text).lower()


def _normalize_unicode_key_text(text: str) -> str:
    return re.sub(r"\W+", "", text, flags=re.UNICODE).lower()


def canonical_segment_key(member_qname: str, segment_name: str = "") -> str:
    member = str(member_qname or "").strip()
    if member.lower().startswith("textblock:"):
        return f"textblock:{_normalize_unicode_key_text(segment_name or member)}"

    local = _local_name(member)
    local = re.sub(r"^[A-Z]\d{5}-\d{3}", "", local)
    for suffix in SEGMENT_MEMBER_SUFFIXES:
        if local.endswith(suffix):
            local = local[: -len(suffix)]
            break

    key = _normalize_key_text(local)
    if key:
        return SEGMENT_KEY_ALIASES.get(key, key)
    fallback_key = _normalize_key_text(segment_name or member)
    return SEGMENT_KEY_ALIASES.get(fallback_key, fallback_key)


def _has_japanese(text: str) -> bool:
    return re.search(r"[\u3040-\u30ff\u3400-\u9fff]", text) is not None


def _is_ascii_text(text: str) -> bool:
    return bool(text) and all(ord(ch) < 128 for ch in text)


def _looks_like_member_fallback(text: str) -> bool:
    normalized = str(text or "").strip()
    return bool(re.match(r"^[A-Z]\d{5}-\d{3}", normalized))


def _period_rank(period_end: str) -> date:
    text = str(period_end or "").strip()
    try:
        return date.fromisoformat(text[:10])
    except Exception:
        return date.min


def segment_name_quality(name: str) -> int:
    text = str(name or "").strip()
    if not text:
        return 99
    if _has_japanese(text):
        return 0
    if not _is_ascii_text(text):
        return 1
    if _looks_like_member_fallback(text):
        return 4
    return 3


def preferred_segment_name_map(
    candidates: Iterable[SegmentNameCandidate],
) -> dict[tuple[str, str, str], str]:
    best: dict[tuple[str, str, str], SegmentNameCandidate] = {}
    for candidate in candidates:
        key = (
            str(candidate.edinet_code or ""),
            str(candidate.segment_kind or ""),
            canonical_segment_key(candidate.member_qname, candidate.segment_name),
        )
        if not key[2]:
            continue
        current = best.get(key)
        if current is None:
            best[key] = candidate
            continue
        candidate_score = (
            segment_name_quality(candidate.segment_name),
            -_period_rank(candidate.period_end).toordinal(),
            len(str(candidate.segment_name or "")),
        )
        current_score = (
            segment_name_quality(current.segment_name),
            -_period_rank(current.period_end).toordinal(),
            len(str(current.segment_name or "")),
        )
        if candidate_score < current_score:
            best[key] = candidate
    return {key: candidate.segment_name for key, candidate in best.items()}
