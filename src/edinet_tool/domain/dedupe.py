from collections import Counter
import datetime as _dt


def raw_key(row: dict):
    return (
        row.get("company_code", ""),
        row.get("doc_id", ""),
        row.get("doc_type", ""),
        row.get("consolidation", ""),
        row.get("metric_key", ""),
        row.get("time_slot", ""),
        row.get("period_start", ""),
        row.get("period_end", ""),
        row.get("period_kind", ""),
        row.get("unit", ""),
    )


def raw_key_for_template(row: dict):
    return (
        row.get("company_code", ""),
        row.get("doc_type", ""),
        row.get("consolidation", ""),
        row.get("metric_key", ""),
        row.get("time_slot", ""),
        row.get("period_kind", ""),
    )


def _to_date(x):
    if isinstance(x, _dt.datetime):
        return x.date()
    if isinstance(x, _dt.date):
        return x
    return None


def _score_raw_row(r: dict):
    status = (r.get("status") or "").upper()
    status_score = {"OK": 3, "MISSING": 2, "ERROR": 1}.get(status, 0)

    tr = r.get("tag_rank")
    try:
        tag_rank_score = -int(tr)
    except Exception:
        tag_rank_score = -999999

    has_value = 1 if (r.get("value") not in (None, "")) else 0
    has_unit = 1 if (r.get("unit") not in (None, "")) else 0

    pe = _to_date(r.get("period_end"))
    period_score = pe.toordinal() if pe else -1

    return (status_score, has_value, has_unit, tag_rank_score, period_score)


def dedupe_raw_rows_keep_best(rows: list[dict]) -> tuple[list[dict], int]:
    best = {}
    dup_count = 0

    for row in rows:
        k = raw_key_for_template(row)
        if k not in best:
            best[k] = row
        else:
            dup_count += 1
            if _score_raw_row(row) > _score_raw_row(best[k]):
                best[k] = row

    seen = set()
    out = []
    for row in rows:
        k = raw_key_for_template(row)
        if k in seen:
            continue
        out.append(best[k])
        seen.add(k)

    return out, dup_count


def find_duplicate_template_keys(rows: list[dict]):
    cnt = Counter(raw_key_for_template(r) for r in rows)
    return [(k, v) for k, v in cnt.items() if v > 1]