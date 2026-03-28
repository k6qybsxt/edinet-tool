import re


def normalize_security_code(code: str | None) -> str | None:
    if code is None:
        return None
    s = str(code).strip()
    m = re.search(r"(\d{4})", s)
    return m.group(1) if m else None


def pick_security_code(*candidates) -> str | None:
    for c in candidates:
        n = normalize_security_code(c)
        if n:
            return n
    return None


def ensure_security_code(out_meta: dict | None, *fallbacks) -> str | None:
    meta_code = None
    if isinstance(out_meta, dict):
        meta_code = out_meta.get("security_code")
    return pick_security_code(meta_code, *fallbacks)