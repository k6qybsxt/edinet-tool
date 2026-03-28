import os
import re


_PATTERN = re.compile(r"^(\d+)-(2|4|5|6).+\.xbrl$", re.IGNORECASE)


def _parse_xbrl_filename(filename):
    m = _PATTERN.match(filename)
    if not m:
        return None, None

    slot = int(m.group(1))
    doc_kind = m.group(2)
    return slot, doc_kind


def build_xbrl_file_index(base_dir, max_n=50, logger=None):
    index = {
        n: {"2": [], "4": [], "5": [], "6": []}
        for n in range(1, max_n + 1)
    }

    for entry in os.scandir(base_dir):
        if not entry.is_file():
            continue

        if not entry.name.lower().endswith(".xbrl"):
            continue

        slot, doc_kind = _parse_xbrl_filename(entry.name)
        if slot is None or doc_kind is None:
            continue

        if slot < 1 or slot > max_n:
            continue

        index[slot][doc_kind].append(entry.path)

    for slot_map in index.values():
        for doc_kind in ("2", "4", "5", "6"):
            slot_map[doc_kind].sort()

    if logger is not None:
        hit_slots = 0
        total_files = 0

        for n in range(1, max_n + 1):
            c2 = len(index[n]["2"])
            c4 = len(index[n]["4"])
            c5 = len(index[n]["5"])
            c6 = len(index[n]["6"])

            if c2 or c4 or c5 or c6:
                hit_slots += 1
                total_files += c2 + c4 + c5 + c6
                logger.debug(
                    f"[xbrl index] slot={n} 2={c2} 4={c4} 5={c5} 6={c6}"
                )

        logger.info(f"[xbrl index] hit_slots={hit_slots} total_files={total_files}")

    return index