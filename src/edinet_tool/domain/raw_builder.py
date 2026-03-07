RAW_COLS = [
    "company_code",
    "doc_id",
    "doc_type",
    "consolidation",
    "metric_key",
    "time_slot",
    "period_start",
    "period_end",
    "period_kind",
    "value",
    "unit",
    "tag_used",
    "tag_rank",
    "status",
    "run_id",
    "source_file",
]


_SUFFIXES = ("Current", "Prior1", "Prior2", "Prior3", "Prior4")


def _split_key(key: str):
    for s in _SUFFIXES:
        if key.endswith(s):
            return key[:-len(s)], s
    return key, None


def split_metric_timeslot(metric_key: str):
    if "_" not in metric_key:
        return metric_key, None
    base, tail = metric_key.rsplit("_", 1)
    if tail in ("YTD", "Quarter"):
        return base, tail
    return metric_key, None


def build_raw_rows_from_out(company_code, doc_id, doc_type, out, out_meta):
    rows = []

    if not isinstance(out_meta, dict):
        return rows

    for key, meta in out_meta.items():
        metric_with_suffix = meta.get("metric_key") or key
        metric_base_with_slot, suffix = _split_key(metric_with_suffix)
        metric_base, time_slot = split_metric_timeslot(metric_base_with_slot)

        row = {
            "company_code": company_code or "",
            "doc_id": doc_id or "",
            "doc_type": doc_type or "",
            "consolidation": meta.get("consolidation") or "",
            "metric_key": metric_base or "",
            "time_slot": time_slot or suffix or "",
            "period_start": meta.get("period_start"),
            "period_end": meta.get("period_end"),
            "period_kind": meta.get("period_kind") or "",
            "value": out.get(key) if isinstance(out, dict) else None,
            "unit": meta.get("unit") or "",
            "tag_used": meta.get("tag_used") or "",
            "tag_rank": meta.get("tag_rank"),
            "status": meta.get("status") or ("OK" if isinstance(out, dict) and key in out else "MISSING"),
        }
        rows.append(row)

    return rows


def append_missing_annual_ytd_rows(raw_rows, company_code, doc_id, out_meta, duration_metric_keys):
    if not isinstance(out_meta, dict):
        return

    existing = {
        (
            r.get("company_code", ""),
            r.get("doc_id", ""),
            r.get("doc_type", ""),
            r.get("metric_key", ""),
            r.get("time_slot", ""),
            r.get("period_kind", ""),
        )
        for r in raw_rows
    }

    for metric_key in duration_metric_keys:
        k = (
            company_code or "",
            doc_id or "",
            "annual",
            metric_key,
            "YTD",
            "duration",
        )
        if k in existing:
            continue

        raw_rows.append({
            "company_code": company_code or "",
            "doc_id": doc_id or "",
            "doc_type": "annual",
            "consolidation": "",
            "metric_key": metric_key,
            "time_slot": "YTD",
            "period_start": None,
            "period_end": None,
            "period_kind": "duration",
            "value": None,
            "unit": "",
            "tag_used": "",
            "tag_rank": None,
            "status": "MISSING",
            "run_id": None,
            "source_file": None,
        })


def attach_run_info(raw_rows, run_id):
    for r in raw_rows:
        r["run_id"] = run_id
        r["source_file"] = r.get("doc_id")