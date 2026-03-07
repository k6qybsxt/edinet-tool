def _expand_dei_date_parts(result: dict):
    v = result.get("CurrentFiscalYearEndDateDEI")
    if isinstance(v, str):
        parts = v.split("-")
        if len(parts) == 3:
            yyyy, mm, _dd = parts
            result["CurrentFiscalYearEndDateDEIyear"] = yyyy
            result["CurrentFiscalYearEndDateDEImonth"] = mm.lstrip("0") or "0"

def filter_for_annual(out: dict, use_half: bool = False):
    if not isinstance(out, dict):
        return {}

    result = {}

    for k, v in out.items():
        if v in (None, ""):
            continue

        if not use_half:
            result[k] = v
            continue

        # 半期ありのときは、最新有報(file2)を全体時系列に合わせて1年後ろへずらす
        # Current -> Prior1
        # Prior1  -> Prior2
        # Prior2  -> Prior3
        # Prior3  -> Prior4
        # Prior4  -> 破棄
        if k.endswith("Current"):
            nk = k[:-len("Current")] + "Prior1"
        elif k.endswith("Prior1"):
            nk = k[:-len("Prior1")] + "Prior2"
        elif k.endswith("Prior2"):
            nk = k[:-len("Prior2")] + "Prior3"
        elif k.endswith("Prior3"):
            nk = k[:-len("Prior3")] + "Prior4"
        elif k.endswith("Prior4"):
            continue
        else:
            nk = k

        result[nk] = v

    result["UseHalfModeFlag"] = 0
    _expand_dei_date_parts(result)
    return result

def filter_for_annual_old(out: dict):
    if not isinstance(out, dict):
        return {}

    result = {}
    for k, v in out.items():
        if v in (None, ""):
            continue
        result[k] = v

    result["UseHalfModeFlag"] = 0
    _expand_dei_date_parts(result)
    return result


def filter_for_half(out: dict):
    if not isinstance(out, dict):
        return {}

    result = {}
    for k, v in out.items():
        if v in (None, ""):
            continue

        # 半期ありでは Current 群には書かない
        if k.endswith("Current"):
            continue

        result[k] = v
        
    result["UseHalfModeFlag"] = 1
    _expand_dei_date_parts(result)
    return result