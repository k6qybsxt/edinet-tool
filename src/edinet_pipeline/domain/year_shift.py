import re


def get_fy_end_year(out: dict):
    if not isinstance(out, dict):
        return None

    for k, v in out.items():
        if not isinstance(k, str):
            continue
        if "Current" in k and v not in (None, ""):
            m = re.search(r"(19|20)\d{2}", str(v))
            if m:
                return int(m.group(0))

    return None


def shift_suffixes_by_yeargap(key: str, year_gap: int):
    if not isinstance(key, str):
        return key

    suffixes = ["Current", "Prior1", "Prior2", "Prior3", "Prior4"]
    if year_gap <= 0:
        return key

    for i, suffix in enumerate(suffixes):
        if key.endswith(suffix):
            new_index = i + year_gap
            if new_index >= len(suffixes):
                return None
            return key[:-len(suffix)] + suffixes[new_index]

    return key


def shift_with_keep(out: dict, year_gap: int):
    if not isinstance(out, dict):
        return {}

    shifted = {}
    for k, v in out.items():
        nk = shift_suffixes_by_yeargap(k, year_gap)
        if nk is None:
            continue
        shifted[nk] = v

    return shifted


def shift_out_meta_by_yeargap(out_meta: dict, year_gap: int):
    if not isinstance(out_meta, dict):
        return {}

    shifted = {}
    for k, meta in out_meta.items():
        nk = shift_suffixes_by_yeargap(k, year_gap)
        if nk is None:
            continue
        shifted[nk] = meta

    return shifted