def filter_for_annual(out: dict, use_half: bool = False):
    if not isinstance(out, dict):
        return {}

    result = {}
    for k, v in out.items():
        if v in (None, ""):
            continue

        if use_half and k.endswith("Current"):
            continue

        result[k] = v

    return result


def filter_for_annual_old(out: dict):
    if not isinstance(out, dict):
        return {}

    result = {}
    for k, v in out.items():
        if v in (None, ""):
            continue
        result[k] = v

    return result


def filter_for_half(out: dict):
    if not isinstance(out, dict):
        return {}

    result = {}
    for k, v in out.items():
        if v in (None, ""):
            continue
        result[k] = v

    return result