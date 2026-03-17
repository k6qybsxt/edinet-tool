# src/xbrl_parser.py
from __future__ import annotations

from pathlib import Path
import os
import re
import json
import traceback
from typing import Any
from datetime import datetime, timedelta
from lxml import etree
from collections import defaultdict, deque
from io import BytesIO

# ここに、main.py から移す「解析専用定数」を置く
# 例:
# RAW_COLS = [...]
# TAG_PRIORITY = {...}
# =========================
# 1) METRICS（あなたの現行をそのまま）
# =========================
METRICS = {
    # ---------- PL / CF（duration） ----------
    "NetSales": {
        "tags": [
            # ★半期サマリー（最優先）
            "jpcrp_cor:NetSalesSummaryOfBusinessResults",
            # 通常PL
            "jppfs_cor:NetSales",
            # 保険（売上相当）
            "jpcrp_cor:RevenuesFromExternalCustomers",
            # IFRS サマリー / KeyFinancialData
            "jpcrp_cor:OperatingRevenuesIFRSKeyFinancialData",
            # IFRS
            "jpigp_cor:RevenueIFRS",
            "jpigp_cor:NetSalesIFRS",
        ],
        "kind": "duration",
        "unit": "millions",
    },
    "CostOfSales": {
        "tags": [
            "jppfs_cor:CostOfSales",
            "jpcrp_cor:CostOfRevenueIFRSSummaryOfBusinessResults",
            "jpcrp_cor:CostOfOperatingRevenueIFRSSummaryOfBusinessResults",
            "jpigp_cor:CostOfSalesIFRS",
            "jpigp_cor:CostOfRevenueIFRS",
            "jpigp_cor:CostOfOperatingRevenueIFRS",
            "jpigp_cor:OperatingCostsIFRS",
        ],
        "kind": "duration",
        "unit": "millions",
    },
    "GrossProfit": {
        "tags": [
            "jppfs_cor:GrossProfit",
            "jpcrp_cor:GrossProfitIFRSSummaryOfBusinessResults",
            "jpigp_cor:GrossProfitIFRS",
        ],
        "kind": "duration",
        "unit": "millions",
    },
    "SellingExpenses": {
        "tags": [
            "jppfs_cor:SellingGeneralAndAdministrativeExpenses",
            "jpcrp_cor:SellingGeneralAndAdministrativeExpensesIFRSSummaryOfBusinessResults",
            "jpcrp_cor:AdministrativeExpensesIFRSSummaryOfBusinessResults",
            "jpcrp_cor:DistributionCostsIFRSSummaryOfBusinessResults",
            "jpigp_cor:SellingGeneralAndAdministrativeExpensesIFRS",
            "jpigp_cor:SellingExpensesAndGeneralAdministrativeExpensesIFRS",
            "jpigp_cor:AdministrativeExpensesIFRS",
            "jpigp_cor:DistributionCostsIFRS",
            "jpigp_cor:SalesAndMarketingExpensesIFRS",
        ],
        "kind": "duration",
        "unit": "millions",
    },
    "FinancialBusinessCost": {
        "tags": [
            "jpigp_cor:CostOfFinancingOperationsIFRS",
            "jpigp_cor:FinanceCostsIFRS",
        ],
        "kind": "duration",
        "unit": "millions",
    },
        "OperatingIncome": {
        "tags": [
            "jppfs_cor:OperatingIncome",
            "jpcrp_cor:OperatingIncomeIFRSSummaryOfBusinessResults",
            "jpcrp_cor:OperatingProfitIFRSSummaryOfBusinessResults",
            "jpigp_cor:OperatingProfitLossIFRS",
            "jpigp_cor:OperatingProfitIFRS",
            "jpigp_cor:OperatingLossIFRS",
            "jpigp_cor:OperatingIncomeIFRS",
        ],
        "kind": "duration",
        "unit": "millions",
    },
    "OrdinaryIncome": {
        "tags": [
            # ★半期サマリー（最優先）
            "jpcrp_cor:OrdinaryIncomeLossSummaryOfBusinessResults",

            # J-GAAP
            "jppfs_cor:OrdinaryIncome",

            # IFRS サマリー / KeyFinancialData
            "jpcrp_cor:ProfitLossBeforeTaxIFRSSummaryOfBusinessResults",
            "jpcrp_cor:ProfitBeforeIncomeTaxesFromContinuingIFRSKeyFinancialData",

            # IFRS / 別名対策
            "jpigp_cor:ProfitBeforeTaxIFRS",
            "jpigp_cor:ProfitLossBeforeTaxIFRS",
            "jpigp_cor:ProfitLossBeforeIncomeTaxesIFRS",
            "jpigp_cor:IncomeBeforeIncomeTaxesIFRS",
            "jpigp_cor:ProfitBeforeIncomeTaxesIFRS",
        ],
        "kind": "duration",
        "unit": "millions",
    },
    "ProfitLoss": {
        "tags": [
            # ★半期サマリー（最優先）
            "jpcrp_cor:ProfitLossAttributableToOwnersOfParentSummaryOfBusinessResults",
            # 日本基準（半期）
            "jppfs_cor:ProfitLossAttributableToOwnersOfParent",
            # 日本基準（通期）
            "jppfs_cor:ProfitLoss",
            # IFRS
            "jpigp_cor:ProfitLossAttributableToOwnersOfParentIFRS",
            "jpigp_cor:ProfitLossIFRS",
        ],
        "kind": "duration",
        "unit": "millions",
    },
    #営業CF
    "OperatingCash": {
        "tags": [
            # ★半期サマリー（最優先）
            "jpcrp_cor:NetCashProvidedByUsedInOperatingActivitiesSummaryOfBusinessResults",
            # 通常CF
            "jppfs_cor:NetCashProvidedByUsedInOperatingActivities",
            # IFRS
            "jpigp_cor:NetCashProvidedByUsedInOperatingActivitiesIFRS",
        ],
        "kind": "duration",
        "unit": "millions",
    },
    #投資CF
    "InvestmentCash": {
        "tags": [
            # ★半期サマリー（最優先）
            "jpcrp_cor:NetCashProvidedByUsedInInvestingActivitiesSummaryOfBusinessResults",
            # 通常CF（表記ゆれ対策で2つ）
            "jppfs_cor:NetCashProvidedByUsedInInvestmentActivities",
            "jppfs_cor:NetCashProvidedByUsedInInvestingActivities",
            # IFRS
            "jpigp_cor:NetCashProvidedByUsedInInvestingActivitiesIFRS",
        ],
        "kind": "duration",
        "unit": "millions",
    },
    #財務CF
    "FinancingCash": {
        "tags": [
            # ★半期サマリー（最優先）
            "jpcrp_cor:NetCashProvidedByUsedInFinancingActivitiesSummaryOfBusinessResults",
            # 通常CF
            "jppfs_cor:NetCashProvidedByUsedInFinancingActivities",
            # IFRS
            "jpigp_cor:NetCashProvidedByUsedInFinancingActivitiesIFRS",
        ],
        "kind": "duration",
        "unit": "millions",
    },

    # ---------- BS（instant） ----------
    "TotalAssets": {
        "tags": [
            "jpcrp_cor:TotalAssetsSummaryOfBusinessResults",
            "jpcrp_cor:TotalAssetsIFRSSummaryOfBusinessResults",
            "jpigp_cor:AssetsIFRS",
        ],
        "kind": "instant_num",
        "unit": "millions",
    },
    "NetAssets": {
        "tags": [
            "jpcrp_cor:NetAssetsSummaryOfBusinessResults",
            "jpcrp_cor:EquityAttributableToOwnersOfParentIFRSSummaryOfBusinessResults",
            "jpigp_cor:EquityAttributableToOwnersOfParentIFRS",
        ],
        "kind": "instant_num",
        "unit": "millions",
    },
    "CashAndCashEquivalents": {
        "tags": [
            "jppfs_cor:CashAndCashEquivalents",
            "jpcrp_cor:CashAndCashEquivalentsSummaryOfBusinessResults",
            "jpcrp_cor:CashAndCashEquivalentsIFRSSummaryOfBusinessResults",
            "jpigp_cor:CashAndCashEquivalentsIFRS",
        ],
        "kind": "instant_num",
        "unit": "millions",
    },

    # ---------- 株数（instant）：「材料」を拾って差分でTotalNumberに統一 ----------
    # 発行済株式総数（優先順を整理）
    "IssuedShares": {
        "tags": [
            # ★期末ベース（最優先：最も安定しやすい）
            "jpcrp_cor:NumberOfIssuedSharesAsOfFiscalYearEndIssuedSharesTotalNumberOfSharesEtc",
            # ★提出日ベース（次点）
            "jpcrp_cor:NumberOfIssuedSharesAsOfFilingDateIssuedSharesTotalNumberOfSharesEtc",

            # 代表的（株式等の状況）
            "jpcrp_cor:TotalNumberOfIssuedSharesIssuedSharesTotalNumberOfSharesEtc",
            # サマリー系（会社により出る）
            "jpcrp_cor:TotalNumberOfIssuedSharesSummaryOfBusinessResults",
            # 普通株/普通株式名義系（会社により出る）
            "jpcrp_cor:TotalNumberOfIssuedSharesCommonStockIssuedSharesTotalNumberOfSharesEtc",
            "jpcrp_cor:TotalNumberOfIssuedSharesOrdinaryShareIssuedSharesTotalNumberOfSharesEtc",
            "jpcrp_cor:NumberOfIssuedSharesAsOfFiscalYearEndIssuedSharesTotalNumberOfSharesEtc",
        ],
        "kind": "instant_num",
        "unit": "ones",
    },

    # 自己株式数（優先順を整理）
    "TreasuryShares": {
        "tags": [
            # ★自己株式数（合計）が最優先
            "jpcrp_cor:TotalNumberOfSharesHeldTreasurySharesEtc",
            # ★自己名義（合計が取れない場合の次点）
            "jpcrp_cor:NumberOfSharesHeldInOwnNameTreasurySharesEtc",

            # 表記ゆれ・補助（会社により出る）
            "jpcrp_cor:TotalNumberOfSharesHeldInTheNameOfOthersTreasurySharesEtc",
            "jpcrp_cor:TotalNumberOfSharesHeldInOwnNameTreasurySharesEtc",
            "jpcrp_cor:TotalNumberOfTreasurySharesSummaryOfBusinessResults",
            "jpcrp_cor:NumberOfTreasurySharesAsOfFiscalYearEndIssuedSharesTotalNumberOfSharesEtc",
            "jpcrp_cor:TreasurySharesAtTheEndOfFiscalYearIssuedSharesTotalNumberOfSharesEtc",
        ],
        "kind": "instant_num",
        "unit": "ones",
    },
    }



# XBRLデータの解析（完成版）
def parse_xbrl_file_legacy(xbrl_file, mode="full", logger=None, pre_parsed=None):

    if logger is None:
        class _DummyLogger:
            def debug(self, *args, **kwargs): pass
            def info(self, *args, **kwargs): pass
            def warning(self, *args, **kwargs): pass
            def error(self, *args, **kwargs): pass
        logger = _DummyLogger()

    if pre_parsed:
        contexts = pre_parsed.get("contexts", {})
        nsmap = pre_parsed.get("nsmap", {})
        dei_data = pre_parsed.get("dei_data", {})
    else:
        contexts = {}
        nsmap = {}
        dei_data = {}

    def _attr_any(el, *names):
        """
        属性を name候補から探して返す。
        lxmlはQName属性になることがあるので、末尾一致でも拾う。
        """
        # 1) まず通常の get
        for n in names:
            v = el.get(n)
            if v is not None:
                return v

        # 2) QName属性対策： {uri}contextRef のようなキーが来る
        if el.attrib:
            for k, v in el.attrib.items():
                kk = k.split("}")[-1]  # localname
                if kk in names:
                    return v
        return None

    # ===== util =====
    def parse_ymd(s):
        try:
            return datetime.strptime(s, "%Y-%m-%d")
        except Exception:
            return None

    def months_diff(s, e):
        if not s or not e:
            return None
        m = (e.year - s.year) * 12 + (e.month - s.month)
        if e.day < s.day:
            m -= 1
        return m

    def duration_bucket_months(start_ymd, end_ymd):
        s = parse_ymd(start_ymd)
        e = parse_ymd(end_ymd)
        md = months_diff(s, e)
        if md is None:
            return None
        if 5 <= md <= 7:
            return 6
        if 11 <= md <= 13:
            return 12
        return None

    def dim_rank(dim):
        return 0 if dim == "Consolidated" else 1

    def best_update(store, key, cand):
        cur = store.get(key)
        if cur is None:
            store[key] = cand
            return
        if (dim_rank(cand["dim"]), cand["tag_priority"]) < (dim_rank(cur["dim"]), cur["tag_priority"]):
            store[key] = cand

    # ===== outputs =====
    out = {}
    out_meta = {}

    # ===== nsmap -> QName helper =====
    def _qname(tag_prefixed: str) -> str:
        pref, local = tag_prefixed.split(":", 1)
        uri = nsmap.get(pref)
        return f"{{{uri}}}{local}" if uri else f"{{}}{local}"

    # === PATCH: METRIC判定を QName ではなく localname で行う（prefix差を吸収）===
    METRIC_L = {}  # localname -> (metric, tag_priority, kind, unit, tag_used)

    for metric, meta in METRICS.items():
        for tag_priority, tag in enumerate(meta["tags"]):
            local = tag.split(":", 1)[1] if ":" in tag else tag
            # 同じlocalが複数出る場合は、先に入った(=優先度高い)を残す
            METRIC_L.setdefault(local, (metric, tag_priority, meta["kind"], meta["unit"], tag))

    DEI_TAGS = {
        "jpdei_cor:CurrentFiscalYearStartDateDEI": "CurrentFiscalYearStartDateDEI",
        "jpdei_cor:CurrentPeriodEndDateDEI": "CurrentPeriodEndDateDEI",
        "jpdei_cor:TypeOfCurrentPeriodDEI": "TypeOfCurrentPeriodDEI",
        "jpdei_cor:CurrentFiscalYearEndDateDEI": "CurrentFiscalYearEndDateDEI",
        "jpdei_cor:SecurityCodeDEI": "SecurityCodeDEI",
        "jpdei_cor:FilerNameInJapaneseDEI": "CompanyNameCoverPage",
        "jpdei_cor:FilerNameDEI": "CompanyNameCoverPage",
    }
    DEI_Q = {_qname(k): v for k, v in DEI_TAGS.items()}

    if pre_parsed and dei_data:
        for raw_tag, v in dei_data.items():
            local_tag = str(raw_tag).split("}")[-1]

            if local_tag == "CurrentFiscalYearStartDateDEI":
                out["CurrentFiscalYearStartDateDEI"] = v
                fy_start_dei = v

            elif local_tag == "CurrentPeriodEndDateDEI":
                out["CurrentPeriodEndDateDEI"] = v
                period_end_dei = v

            elif local_tag == "TypeOfCurrentPeriodDEI":
                out["TypeOfCurrentPeriodDEI"] = v
                period_type = v

            elif local_tag == "CurrentFiscalYearEndDateDEI":
                out["CurrentFiscalYearEndDateDEI"] = v

            elif local_tag == "SecurityCodeDEI":
                if str(v).isdigit() and len(str(v)) >= 2:
                    security_code = str(v)[:-1]
                    out["SecurityCodeDEI"] = security_code

            elif local_tag in ("FilerNameInJapaneseDEI", "FilerNameDEI"):
                if "CompanyNameCoverPage" not in out:
                    out["CompanyNameCoverPage"] = v

            elif local_tag == "DocumentDisplayUnitDEI":
                out["DocumentDisplayUnit"] = v

    fy_start_dei = out.get("CurrentFiscalYearStartDateDEI")
    period_end_dei = out.get("CurrentPeriodEndDateDEI")
    period_type = out.get("TypeOfCurrentPeriodDEI")
    security_code = out.get("SecurityCodeDEI")

    # ===== state =====
    ctxref_missing = 0
    ctxref_notfound = 0
    ctxref_found = 0
    ctxref_samples = []

    # factがcontextより先に来たときの保険（ctxref -> list[factinfo]）
    pending = defaultdict(list)

    # best stores
    dur_best = {}
    inst_best = {}
    fy_end_candidates = set()

    # index（後段を高速に）
    dur_ends_by_metric_months = defaultdict(set)  # (metric, months) -> set(end)
    inst_ends_by_metric = defaultdict(set)        # metric -> set(end)

    metric_hit = 0
    metric_hit_nonempty = 0
    metric_hit_sample = []
    display_unit_votes = {"百万円": 0, "千円": 0}

    seen_locals = set()
    seen_local_sample = []



    # ===== 1-pass iterparse =====
    if not pre_parsed:
        for event, el in etree.iterparse(xbrl_file, events=("end",), recover=True, huge_tree=True):
            local = el.tag.split("}")[-1]

            # context の子要素は parent(context) を読むまで clear しない
            if local in {
                "startDate",
                "endDate",
                "instant",
                "explicitMember",
                "entity",
                "identifier",
                "period",
                "scenario",
                "segment",
            }:
                continue

            if len(seen_local_sample) < 10 and local not in seen_locals:
                seen_locals.add(local)
                seen_local_sample.append(local)

            # --- context ---
            if local == "context":
                ctx_id = (el.get("id") or "").strip() or None
                if ctx_id:
                    start_s = end_s = inst_s = None

                    for p in el.iter():
                        pl = p.tag.split("}")[-1]
                        if pl == "startDate":
                            start_s = (p.text or "").strip() or None
                        elif pl == "endDate":
                            end_s = (p.text or "").strip() or None
                        elif pl == "instant":
                            inst_s = (p.text or "").strip() or None

                    members = []
                    for m in el.iter():
                        if m.tag.split("}")[-1] == "explicitMember":
                            t = (m.text or "").strip()
                            if t:
                                members.append(t)

                    is_noncon = any("NonConsolidatedMember" in t for t in members)
                    dim = "NonConsolidated" if is_noncon else "Consolidated"

                    ctx = {"start": start_s, "end": end_s, "instant": inst_s, "dim": dim}
                    contexts[ctx_id] = ctx

                    if ctx_id in pending:
                        for finfo in pending.pop(ctx_id):
                            metric, tag_priority, kind, unit, tag_used, txt = finfo

                            if kind == "duration":
                                if ctx["start"] and ctx["end"]:
                                    months = duration_bucket_months(ctx["start"], ctx["end"])
                                    if months in (6, 12):
                                        end_date = ctx["end"]

                                        cand = {
                                            "value": txt,
                                            "start": ctx["start"],
                                            "end": end_date,
                                            "months": months,
                                            "dim": ctx["dim"],
                                            "tag_priority": tag_priority,
                                            "tag_used": tag_used,
                                        }

                                        best_update(dur_best, (metric, end_date, months), cand)
                                        dur_ends_by_metric_months[(metric, months)].add(end_date)

                                        if months == 12 and parse_ymd(end_date):
                                            fy_end_candidates.add(end_date)

                            else:
                                if ctx["instant"]:
                                    end_date = ctx["instant"]

                                    cand = {
                                        "value": txt,
                                        "start": None,
                                        "end": end_date,
                                        "dim": ctx["dim"],
                                        "tag_priority": tag_priority,
                                        "tag_used": tag_used,
                                    }

                                    best_update(inst_best, (metric, end_date), cand)
                                    inst_ends_by_metric[metric].add(end_date)

                el.clear()
                continue

            # --- DEI ---
            k = DEI_Q.get(el.tag)

            if k:
                v = (el.text or "").strip()

                if v:
                    if k == "CompanyNameCoverPage":
                        if "CompanyNameCoverPage" not in out:
                            out["CompanyNameCoverPage"] = v
                    else:
                        out[k] = v

                    if k == "CurrentFiscalYearStartDateDEI":
                        fy_start_dei = v

                    elif k == "CurrentPeriodEndDateDEI":
                        period_end_dei = v

                    elif k == "TypeOfCurrentPeriodDEI":
                        period_type = v

                    elif k == "SecurityCodeDEI":
                        if v.isdigit() and len(v) >= 2:
                            security_code = v[:-1]
                            out["SecurityCodeDEI"] = security_code

                el.clear()
                continue

            # --- FACT ---
            info = METRIC_L.get(local)

            if info:

                metric, tag_priority, kind, unit, tag_used = info

                metric_hit += 1

                txt = (el.text or "").strip()

                unit_ref = (el.get("unitRef") or "").strip()
                decimals = (el.get("decimals") or "").strip()

                if unit_ref == "JPY":
                    if decimals == "-6":
                        display_unit_votes["百万円"] += 1
                    elif decimals == "-3":
                        display_unit_votes["千円"] += 1

                if txt:
                    metric_hit_nonempty += 1

                if len(metric_hit_sample) < 8:
                    metric_hit_sample.append((local, tag_used))

                ctxref = (_attr_any(el, "contextRef") or "").strip() or None

                # --- ctxrefメーター（最初の10件だけサンプル）---
                if len(ctxref_samples) < 10:
                    ctxref_samples.append((local, ctxref, ctxref in contexts if ctxref else None))

                if not ctxref:
                    ctxref_missing += 1
                else:
                    if ctxref in contexts:
                        ctxref_found += 1
                    else:
                        ctxref_notfound += 1

                if metric_hit <= 5:
                    logger.debug(
                        "[ctxref sample] local=%s ctxref=%s has_ctx=%s",
                        local,
                        ctxref,
                        (ctxref in contexts) if ctxref else None
                    )

                if ctxref:

                    txt = (el.text or "").strip()

                    if txt:

                        ctx = contexts.get(ctxref)

                        if ctx is None:
                            pending[ctxref].append(
                                (metric, tag_priority, kind, unit, tag_used, txt)
                            )

                        else:

                            if kind == "duration":

                                if ctx["start"] and ctx["end"]:

                                    months = duration_bucket_months(ctx["start"], ctx["end"])

                                    if months in (6, 12):

                                        end_date = ctx["end"]

                                        cand = {
                                            "value": txt,
                                            "start": ctx["start"],
                                            "end": end_date,
                                            "months": months,
                                            "dim": ctx["dim"],
                                            "tag_priority": tag_priority,
                                            "tag_used": tag_used,
                                        }

                                        best_update(dur_best, (metric, end_date, months), cand)

                                        dur_ends_by_metric_months[(metric, months)].add(end_date)

                                        if months == 12 and parse_ymd(end_date):
                                            fy_end_candidates.add(end_date)

                            else:

                                if ctx["instant"]:

                                    end_date = ctx["instant"]

                                    cand = {
                                        "value": txt,
                                        "start": None,
                                        "end": end_date,
                                        "dim": ctx["dim"],
                                        "tag_priority": tag_priority,
                                        "tag_used": tag_used,
                                    }

                                    best_update(inst_best, (metric, end_date), cand)

                                    inst_ends_by_metric[metric].add(end_date)
            el.clear()

    if pre_parsed:
        for el in pre_parsed.get("facts", []):
            local = el.get("local") or str(el.get("tag", "")).split("}")[-1]

            if len(seen_local_sample) < 10 and local not in seen_locals:
                seen_locals.add(local)
                seen_local_sample.append(local)

            info = METRIC_L.get(local)
            if not info:
                continue

            metric, tag_priority, kind, unit, tag_used = info
            metric_hit += 1

            txt = (el.get("text") or "").strip()
            unit_ref = (el.get("unitRef") or "").strip()
            decimals = (el.get("decimals") or "").strip()

            if unit_ref == "JPY":
                if decimals == "-6":
                    display_unit_votes["百万円"] += 1
                elif decimals == "-3":
                    display_unit_votes["千円"] += 1

            if txt:
                metric_hit_nonempty += 1

            if len(metric_hit_sample) < 8:
                metric_hit_sample.append((local, tag_used))

            ctxref = (el.get("contextRef") or "").strip() or None

            if len(ctxref_samples) < 10:
                ctxref_samples.append((local, ctxref, ctxref in contexts if ctxref else None))

            if not ctxref:
                ctxref_missing += 1
                continue

            if ctxref in contexts:
                ctxref_found += 1
            else:
                ctxref_notfound += 1
                continue

            ctx = contexts.get(ctxref)
            if not ctx or not txt:
                continue

            if kind == "duration":
                if ctx["start"] and ctx["end"]:
                    months = duration_bucket_months(ctx["start"], ctx["end"])
                    if months in (6, 12):
                        end_date = ctx["end"]

                        cand = {
                            "value": txt,
                            "start": ctx["start"],
                            "end": end_date,
                            "months": months,
                            "dim": ctx["dim"],
                            "tag_priority": tag_priority,
                            "tag_used": tag_used,
                        }

                        best_update(dur_best, (metric, end_date, months), cand)
                        dur_ends_by_metric_months[(metric, months)].add(end_date)

                        if months == 12 and parse_ymd(end_date):
                            fy_end_candidates.add(end_date)
            else:
                if ctx["instant"]:
                    end_date = ctx["instant"]

                    cand = {
                        "value": txt,
                        "start": None,
                        "end": end_date,
                        "dim": ctx["dim"],
                        "tag_priority": tag_priority,
                        "tag_used": tag_used,
                    }

                    best_update(inst_best, (metric, end_date), cand)
                    inst_ends_by_metric[metric].add(end_date)  

    sample_ctx = list(contexts.items())[:5]
    logger.debug("[context sample] mode=%s %s", mode, sample_ctx)

    logger.debug(
        "[ctxref meter] mode=%s missing=%d notfound=%d found=%d samples=%s",
        mode, ctxref_missing, ctxref_notfound, ctxref_found, ctxref_samples
    )

    # ===== DEBUG METER (temporary) =====
    try:

        dur_n = len(dur_best)
        inst_n = len(inst_best)

        dur_6 = sum(1 for (m, end, months) in dur_best.keys() if months == 6)
        dur_12 = sum(1 for (m, end, months) in dur_best.keys() if months == 12)

        logger.debug(
            "[parse debug] mode=%s contexts=%d dur_best=%d(inst=%d) dur6=%d dur12=%d ns_has_jppfs=%s ns_has_jpcrp=%s",
            mode,
            len(contexts),
            dur_n,
            inst_n,
            dur_6,
            dur_12,
            ("jppfs_cor" in nsmap),
            ("jpcrp_cor" in nsmap),
        )

    except Exception:
        pass


    logger.debug("[local sample] mode=%s %s", mode, seen_local_sample)

    logger.debug(
        "[fact meter] mode=%s metric_hit=%d metric_nonempty=%d sample=%s",
        mode,
        metric_hit,
        metric_hit_nonempty,
        metric_hit_sample
    )

    # ===== DEI half key =====
    if mode == "half" and period_end_dei:
        out["HalfPeriodEndDateDEI"] = period_end_dei

    # ===== base_year / fy_start =====
    fy_ends = sorted({d for d in fy_end_candidates if parse_ymd(d)}, reverse=True)
    base_fy_end = fy_ends[0] if fy_ends else None
    base_dt = parse_ymd(base_fy_end) if base_fy_end else None
    base_year = base_dt.year if base_dt else None

    fy_start = None
    if fy_start_dei and parse_ymd(fy_start_dei):
        fy_start = fy_start_dei
    if fy_start is None:
        fy_end_dei = out.get("CurrentFiscalYearEndDateDEI")
        if fy_end_dei:
            fy_end_dt = parse_ymd(fy_end_dei)
            if fy_end_dt:
                prev_fy_end = fy_end_dt.replace(year=fy_end_dt.year - 1)
                fy_start = (prev_fy_end + timedelta(days=1)).strftime("%Y-%m-%d")
    if fy_start is None and base_fy_end:
        # base_fy_endに一致する12ヶ月 cand の start を拾う
        starts = []
        for (m, end, months), cand in dur_best.items():
            if months == 12 and end == base_fy_end and cand.get("start") and parse_ymd(cand["start"]):
                starts.append(cand["start"])
        starts = sorted(set(starts), key=lambda s: parse_ymd(s))
        if starts:
            fy_start = starts[0]

    period_end = period_end_dei

    # ===== OUTPUT: duration =====
    for metric, meta in METRICS.items():
        if meta["kind"] != "duration":
            continue

        # YTD(6)
        best = None
        if mode == "half" and period_end and parse_ymd(period_end):
            cand = dur_best.get((metric, period_end, 6))
            if cand and ((fy_start is None) or (cand.get("start") == fy_start)):
                best = cand

        if best is None and fy_start:
            ends = sorted({e for e in dur_ends_by_metric_months.get((metric, 6), set())
                           if parse_ymd(e) and dur_best.get((metric, e, 6), {}).get("start") == fy_start},
                          reverse=True)
            if ends:
                best = dur_best.get((metric, ends[0], 6))

        if best is None:
            ends = sorted({e for e in dur_ends_by_metric_months.get((metric, 6), set()) if parse_ymd(e)}, reverse=True)
            if ends:
                best = dur_best.get((metric, ends[0], 6))

        key = f"{metric}YTD"
        if best:
            out[key] = trim_value(best["value"], meta["unit"])
            out_meta[key] = {
                "period_start": best.get("start"),
                "period_end": best.get("end"),
                "period_kind": "duration",
                "unit": meta["unit"],
                "consolidation": "C" if best.get("dim") == "Consolidated" else "N",
                "tag_used": best.get("tag_used"),
                "tag_rank": (best.get("tag_priority") or 0) + 1,
                "status": "OK",
            }
        else:
            out[key] = None
            out_meta[key] = {
                "period_start": None,
                "period_end": None,
                "period_kind": "duration",
                "unit": meta["unit"],
                "consolidation": None,
                "tag_used": None,
                "tag_rank": None,
                "status": "MISSING",
            }

        # 12ヶ月 Current/Prior（full） / Prior1（half）
        ends_12 = sorted(
            {e for e in dur_ends_by_metric_months.get((metric, 12), set()) if parse_ymd(e)},
            reverse=True
        )

        if mode == "half":
            # 半期ファイルでは「直近の12ヶ月実績」を Prior1 として採用
            if ends_12:
                end_date = ends_12[0]
                best12 = dur_best.get((metric, end_date, 12))
                if best12:
                    k2 = f"{metric}Prior1"
                    out[k2] = trim_value(best12["value"], meta["unit"])
                    out_meta[k2] = {
                        "period_start": best12.get("start"),
                        "period_end": best12.get("end"),
                        "period_kind": "duration",
                        "unit": meta["unit"],
                        "consolidation": "C" if best12.get("dim") == "Consolidated" else "N",
                        "tag_used": best12.get("tag_used"),
                        "tag_rank": (best12.get("tag_priority") or 0) + 1,
                        "status": "OK",
                    }
        else:
            for end_date in ends_12:
                if base_year is None:
                    continue

                dt = parse_ymd(end_date)
                if not dt:
                    continue

                diff = base_year - dt.year
                if diff < 0 or diff > 4:
                    continue

                suffix = "Current" if diff == 0 else f"Prior{diff}"

                best12 = dur_best.get((metric, end_date, 12))
                if best12:
                    k2 = f"{metric}{suffix}"
                    out[k2] = trim_value(best12["value"], meta["unit"])
                    out_meta[k2] = {
                        "period_start": best12.get("start"),
                        "period_end": best12.get("end"),
                        "period_kind": "duration",
                        "unit": meta["unit"],
                        "consolidation": "C" if best12.get("dim") == "Consolidated" else "N",
                        "tag_used": best12.get("tag_used"),
                        "tag_rank": (best12.get("tag_priority") or 0) + 1,
                        "status": "OK",
                    }

    # ===== OUTPUT: instant =====
    for metric, meta in METRICS.items():
        if meta["kind"] == "duration":
            continue

        inst_ends = sorted({e for e in inst_ends_by_metric.get(metric, set()) if parse_ymd(e)}, reverse=True)

        # Quarter
        if inst_ends:
            chosen_end = inst_ends[0]
            target_dt = parse_ymd(period_end) if period_end else None
            if target_dt:
                if period_end in inst_ends:
                    chosen_end = period_end
                else:
                    fy_start_dt = parse_ymd(fy_start) if fy_start else None
                    inst_dts = []
                    for e in inst_ends:
                        dt = parse_ymd(e)
                        if not dt:
                            continue
                        if fy_start_dt and dt < fy_start_dt:
                            continue
                        inst_dts.append(dt)
                    if not inst_dts:
                        inst_dts = [parse_ymd(e) for e in inst_ends if parse_ymd(e)]
                    if inst_dts:
                        inst_dts.sort(key=lambda dt: abs((dt - target_dt).days))
                        chosen_end = inst_dts[0].strftime("%Y-%m-%d")

            best_q = inst_best.get((metric, chosen_end))
            if best_q:
                key = f"{metric}Quarter"
                out[key] = trim_value(best_q["value"], meta["unit"])
                out_meta[key] = {
                    "period_start": None,
                    "period_end": best_q.get("end"),
                    "period_kind": "instant",
                    "unit": meta["unit"],
                    "consolidation": "C" if best_q.get("dim") == "Consolidated" else "N",
                    "tag_used": best_q.get("tag_used"),
                    "tag_rank": (best_q.get("tag_priority") or 0) + 1,
                    "status": "OK",
                }

        # Current/Prior（full） / Prior1（half）
        if mode == "half":
            # 半期ファイルでは、四半期末(Quarter)とは別に、
            # 直近の期末instantを Prior1 として採用する
            prior1_candidates = []

            target_q_end = parse_ymd(period_end) if period_end else None
            for end_date in inst_ends:
                dt = parse_ymd(end_date)
                if not dt:
                    continue
                if target_q_end and dt >= target_q_end:
                    continue
                prior1_candidates.append(end_date)

            if prior1_candidates:
                end_date = sorted(prior1_candidates, reverse=True)[0]
                best_i = inst_best.get((metric, end_date))
                if best_i:
                    key = f"{metric}Prior1"
                    out[key] = trim_value(best_i["value"], meta["unit"])
                    out_meta[key] = {
                        "period_start": None,
                        "period_end": best_i.get("end"),
                        "period_kind": "instant",
                        "unit": meta["unit"],
                        "consolidation": "C" if best_i.get("dim") == "Consolidated" else "N",
                        "tag_used": best_i.get("tag_used"),
                        "tag_rank": (best_i.get("tag_priority") or 0) + 1,
                        "status": "OK",
                    }
        else:
            for end_date in inst_ends:
                if base_year is None:
                    continue

                dt = parse_ymd(end_date)
                if not dt:
                    continue

                diff = base_year - dt.year
                if diff < 0 or diff > 4:
                    continue

                suffix = "Current" if diff == 0 else f"Prior{diff}"

                best_i = inst_best.get((metric, end_date))
                if best_i:
                    key = f"{metric}{suffix}"
                    out[key] = trim_value(best_i["value"], meta["unit"])
                    out_meta[key] = {
                        "period_start": None,
                        "period_end": best_i.get("end"),
                        "period_kind": "instant",
                        "unit": meta["unit"],
                        "consolidation": "C" if best_i.get("dim") == "Consolidated" else "N",
                        "tag_used": best_i.get("tag_used"),
                        "tag_rank": (best_i.get("tag_priority") or 0) + 1,
                        "status": "OK",
                    }

    # === GrossProfit 計算（常に NetSales - CostOfSales を優先） ===
    gross_profit_suffixes = ["YTD", "Current", "Prior1", "Prior2", "Prior3", "Prior4"]

    for suffix in gross_profit_suffixes:
        gp_key = f"GrossProfit{suffix}"

        net_sales = out.get(f"NetSales{suffix}")
        cost_of_sales = out.get(f"CostOfSales{suffix}")

        if net_sales in (None, "") or cost_of_sales in (None, ""):
            continue

        try:
            gross_profit_value = int(net_sales) - int(cost_of_sales)
        except Exception:
            continue

        out[gp_key] = gross_profit_value

        net_meta = out_meta.get(f"NetSales{suffix}", {}) or {}
        cost_meta = out_meta.get(f"CostOfSales{suffix}", {}) or {}

        out_meta[gp_key] = {
            "period_start": net_meta.get("period_start") or cost_meta.get("period_start"),
            "period_end": net_meta.get("period_end") or cost_meta.get("period_end"),
            "period_kind": net_meta.get("period_kind") or cost_meta.get("period_kind") or "duration",
            "unit": "millions",
            "consolidation": net_meta.get("consolidation") or cost_meta.get("consolidation"),
            "tag_used": "CALC(NetSales-CostOfSales)",
            "tag_rank": 0,
            "status": "OK",
        }

    # === SellingExpenses 計算（金融事業に係る金融費用 + 販売費及び一般管理費） ===
    selling_exp_suffixes = ["YTD", "Current", "Prior1", "Prior2", "Prior3", "Prior4"]

    for suffix in selling_exp_suffixes:
        se_key = f"SellingExpenses{suffix}"

        sga_value = out.get(se_key)
        finance_cost_value = out.get(f"FinancialBusinessCost{suffix}")

        if sga_value in (None, "") or finance_cost_value in (None, ""):
            continue

        try:
            selling_exp_value = int(sga_value) + int(finance_cost_value)
        except Exception:
            continue

        out[se_key] = selling_exp_value

        sga_meta = out_meta.get(se_key, {}) or {}
        finance_meta = out_meta.get(f"FinancialBusinessCost{suffix}", {}) or {}

        out_meta[se_key] = {
            "period_start": sga_meta.get("period_start") or finance_meta.get("period_start"),
            "period_end": sga_meta.get("period_end") or finance_meta.get("period_end"),
            "period_kind": sga_meta.get("period_kind") or finance_meta.get("period_kind") or "duration",
            "unit": "millions",
            "consolidation": sga_meta.get("consolidation") or finance_meta.get("consolidation"),
            "tag_used": "CALC(FinancialBusinessCost+SellingExpenses)",
            "tag_rank": 0,
            "status": "OK",
        }

    # === TotalNumber 計算 ===
    suffixes = [
        "Current",
        "Prior1",
        "Prior2",
        "Prior3",
        "Prior4",
        "Quarter"
    ]

    for suffix in suffixes:

        issued = out.get(f"IssuedShares{suffix}")
        treasury = out.get(f"TreasuryShares{suffix}")

        if issued is None or treasury is None:
            continue

        key = f"TotalNumber{suffix}"

        try:
            value = int(issued) - int(treasury)
        except Exception:
            continue

        out[key] = value

        out_meta[key] = {
            "period_start": None,
            "period_end": (out_meta.get(f"IssuedShares{suffix}", {}) or {}).get("period_end"),
            "period_kind": "instant",
            "unit": "shares",
            "consolidation": (out_meta.get(f"IssuedShares{suffix}", {}) or {}).get("consolidation"),
            "tag_used": "CALC(IssuedShares-TreasuryShares)",
            "tag_rank": 0,
            "status": "OK",
        }

    if display_unit_votes["百万円"] == 0 and display_unit_votes["千円"] == 0:
        out["DocumentDisplayUnit"] = "百万円"
    elif display_unit_votes["百万円"] >= display_unit_votes["千円"]:
        out["DocumentDisplayUnit"] = "百万円"
    else:
        out["DocumentDisplayUnit"] = "千円"

    return out, security_code, out_meta

def _detect_accounting_standard(nsmap):

    if "ifrs-full" in nsmap:
        return "ifrs"

    if "jpigp_cor" in nsmap:
        return "ifrs"

    return "jpgaap"

def _extract_dei_data_from_out(out):
    keys = [
        "CurrentFiscalYearStartDateDEI",
        "CurrentPeriodEndDateDEI",
        "TypeOfCurrentPeriodDEI",
        "CurrentFiscalYearEndDateDEI",
        "SecurityCodeDEI",
        "CompanyNameCoverPage",
        "HalfPeriodEndDateDEI",
        "DocumentDisplayUnit",
    ]
    return {k: out.get(k) for k in keys if k in out}

def _safe_local(tag):
    return str(tag).split("}")[-1]


def _safe_text(v):
    if v is None:
        return None
    s = str(v).strip()
    return s or None


def _safe_num(v):
    if v is None:
        return None
    try:
        return float(str(v).replace(",", ""))
    except Exception:
        return None

def trim_value(v, unit):
    if v is None:
        return None
    try:
        if unit == "millions":
            return int(float(v))
        if unit == "ones":
            return int(float(v))
        return v
    except Exception:
        return None

def parse_xbrl_file_raw(path=None, xbrl_bytes=None, mode="full", logger=None):
    facts = []
    contexts = {}
    units = {}
    nsmap = {}
    dei_data = {}

    security_code = None
    accounting_standard = "jpgaap"
    document_display_unit = None

    source = BytesIO(xbrl_bytes) if xbrl_bytes else path

    for event, elem in etree.iterparse(source, events=("start", "end"), recover=True, huge_tree=True):
        tag = elem.tag
        local = str(tag).split("}")[-1]

        if event == "start" and elem.nsmap:
            nsmap.update({k: v for k, v in elem.nsmap.items() if k})
            continue

        if event != "end":
            continue

        # context / unit の親要素を読む前に子を clear しない
        if local in {
            "startDate",
            "endDate",
            "instant",
            "explicitMember",
            "entity",
            "identifier",
            "period",
            "scenario",
            "segment",
            "measure",
        }:
            continue

        if local == "context":
            cid = (elem.get("id") or "").strip()
            if cid:
                start_s = end_s = inst_s = None
                members = []

                for p in elem.iter():
                    pl = str(p.tag).split("}")[-1]
                    if pl == "startDate":
                        start_s = (p.text or "").strip() or None
                    elif pl == "endDate":
                        end_s = (p.text or "").strip() or None
                    elif pl == "instant":
                        inst_s = (p.text or "").strip() or None
                    elif pl == "explicitMember":
                        t = (p.text or "").strip()
                        if t:
                            members.append(t)

                is_noncon = any("NonConsolidatedMember" in t for t in members)
                dim = "NonConsolidated" if is_noncon else "Consolidated"

                contexts[cid] = {
                    "start": start_s,
                    "end": end_s,
                    "instant": inst_s,
                    "dim": dim,
                }

        elif local == "unit":
            uid = (elem.get("id") or "").strip()
            if uid:
                measures = []
                for p in elem.iter():
                    pl = str(p.tag).split("}")[-1]
                    if pl == "measure":
                        t = (p.text or "").strip()
                        if t:
                            measures.append(t)
                units[uid] = {"measures": measures}

        else:
            context_ref = None
            for n in ("contextRef",):
                v = elem.get(n)
                if v is not None:
                    context_ref = v
                    break
            if context_ref is None and elem.attrib:
                for k, v in elem.attrib.items():
                    if k.split("}")[-1] == "contextRef":
                        context_ref = v
                        break

            text = (elem.text or "").strip()

            if context_ref:
                facts.append({
                    "tag": str(tag),
                    "local": local,
                    "text": text,
                    "contextRef": context_ref,
                    "unitRef": elem.get("unitRef"),
                    "decimals": elem.get("decimals"),
                })

            if "dei" in str(tag).lower() and text:
                dei_data[str(tag)] = text

                if "securitycode" in str(tag).lower():
                    if text.isdigit() and len(text) >= 2:
                        security_code = text[:-1]
                    else:
                        security_code = text

                if "accountingstandard" in str(tag).lower():
                    accounting_standard = text

                if "documentdisplayunit" in str(tag).lower():
                    document_display_unit = text

        elem.clear()

    out, security_code2, out_meta = parse_xbrl_file_legacy(
        source,
        mode=mode,
        logger=logger,
        pre_parsed={
            "facts": facts,
            "contexts": contexts,
            "units": units,
            "nsmap": nsmap,
            "dei_data": dei_data,
        },
    )

    return {
        "facts": facts,
        "contexts": contexts,
        "units": units,
        "nsmap": nsmap,
        "dei_data": dei_data,
        "meta": {
            "accounting_standard": accounting_standard,
            "document_display_unit": document_display_unit,
        },
        "out": out,
        "out_meta": out_meta,
        "security_code": security_code or security_code2,
    }

def parse_xbrl_file(xbrl_file, mode="full", logger=None):
    parsed = parse_xbrl_file_raw(
        xbrl_file,
        mode=mode,
        logger=logger,
    )
    return parsed["out"], parsed["security_code"], parsed["out_meta"]