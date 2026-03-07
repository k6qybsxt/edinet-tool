from dataclasses import dataclass, asdict
from enum import Enum


class SkipCode(str, Enum):
    EXCEL_NOT_FOUND = "EXCEL_NOT_FOUND"
    FILE1_PARSE_ERROR = "FILE1_PARSE_ERROR"
    FILE2_NOT_FOUND = "FILE2_NOT_FOUND"
    FILE2_ERROR = "FILE2_ERROR"
    FILE3_YEAR_MISS = "FILE3_YEAR_MISS"
    FILE3_ERROR = "FILE3_ERROR"
    HALF_WRITE_ERROR = "HALF_WRITE_ERROR"
    NO_SECURITY_CODE = "NO_SECURITY_CODE"
    RENAME_ERROR = "RENAME_ERROR"
    UNKNOWN = "UNKNOWN"


@dataclass
class SkipItem:
    code: str
    phase: str
    slot: int | None
    excel: str | None
    xbrl: str | None
    message: str
    exc_type: str | None = None
    exc_msg: str | None = None


def add_skip(skipped_files: list, *, code: SkipCode, phase: str, loop: dict | None,
             excel: str | None, xbrl: str | None, message: str, exc: Exception | None = None):
    item = SkipItem(
        code=code.value if isinstance(code, SkipCode) else str(code),
        phase=phase,
        slot=(loop.get("slot") if loop else None),
        excel=excel,
        xbrl=xbrl,
        message=message,
        exc_type=(type(exc).__name__ if exc else None),
        exc_msg=(str(exc) if exc else None),
    )
    skipped_files.append(asdict(item))


def log_skip_summary(logger, skipped_files: list):
    logger.info("--- skipped summary ---")
    if not skipped_files:
        logger.info("skipped=0")
        return

    counts: dict[str, int] = {}
    for s in skipped_files:
        c = s.get("code", "UNKNOWN")
        counts[c] = counts.get(c, 0) + 1

    parts = [f"{k}={counts[k]}" for k in sorted(counts.keys())]
    logger.warning("[skipped summary] " + " ".join(parts))

    logger.info("--- skipped details (first 30) ---")
    for s in skipped_files[:30]:
        logger.warning(
            "skip "
            f"code={s.get('code')} phase={s.get('phase')} slot={s.get('slot')} "
            f"excel={s.get('excel')} xbrl={s.get('xbrl')} msg={s.get('message')} "
            f"exc={s.get('exc_type')}:{s.get('exc_msg')}"
        )