import os
import openpyxl
from bs4 import BeautifulSoup
import yfinance as yf
import re
from datetime import datetime, timedelta
import glob
from tkinter import Tk, filedialog
import pandas as pd
import json

# スクリプトのあるフォルダに作業ディレクトリを変更
script_dir = os.path.dirname(os.path.abspath(__file__))  # スクリプトのあるフォルダを取得
os.chdir(script_dir)  # 作業ディレクトリをスクリプトのフォルダに変更

# 作業ディレクトリを表示
print("作業ディレクトリ:", os.getcwd())

# フォルダを選択する関数
def choose_directory():
    root = Tk()
    root.withdraw()              # メインウィンドウ非表示
    root.update()                # これが効く環境が多い
    folder_path = filedialog.askdirectory(title="XBRLフォルダを選択してください")
    root.destroy()               # ★これ重要：Tkを閉じる
    return folder_path

# メイン処理
base_dir = r"C:\Users\silve\OneDrive\PC\開発\test\Python\XBRL"
print("XBRLフォルダ（固定）:", base_dir)

if not os.path.isdir(base_dir):
    print("base_dir が存在しません。パスを確認してください:", base_dir)
    exit()

def trim_value(value, unit='millions'):
    """
    EDINETのfactは文字列で来ることが多いので、まず int 化。
    株数(unit='ones')は割らずにそのまま返す。
    """
    try:
        s = str(value).replace(",", "").strip()
        v = int(s)

        if unit == "ones":
            return v

        factor = {'millions': 1_000_000, 'thousands': 1_000, 'ten': 10}[unit]
        return v // factor
    except Exception:
        return 'データなし'


# XBRLデータの解析（完成版）
def parse_xbrl_data(xbrl_file, mode="full"):
    # mode:
    #   "full" : YTD + Current/Prior + Quarter + DEI（有報向け）
    #   "half" : YTD + Quarter + DEI だけ（半期向け：Current/Prior作らない）
    with open(xbrl_file, "r", encoding="utf-8") as f:
        soup = BeautifulSoup(f, "lxml-xml")

    out = {}

    # ========= 1) context 読み取り =========
    contexts = {}
    for ctx in soup.find_all("context"):
        ctx_id = ctx.get("id")
        if not ctx_id:
            continue

        period = ctx.find("period")
        if not period:
            continue

        start = period.find("startDate")
        end = period.find("endDate")
        instant = period.find("instant")

        start_s = start.get_text(strip=True) if start else None
        end_s = end.get_text(strip=True) if end else None
        inst_s = instant.get_text(strip=True) if instant else None

        members = [m.get_text(strip=True) for m in ctx.find_all("xbrldi:explicitMember")]
        is_noncon = any("NonConsolidatedMember" in t for t in members)
        dim = "NonConsolidated" if is_noncon else "Consolidated"

        contexts[ctx_id] = {"start": start_s, "end": end_s, "instant": inst_s, "dim": dim}

    def parse_ymd(s):
        """日付っぽい文字列だけYYYY-MM-DDで処理。失敗したら None。"""
        try:
            return datetime.strptime(s, "%Y-%m-%d")
        except Exception:
            return None

    def months_diff(s, e):
        """日付 s->e のだいたいの月差。日が足りない分は1か月減らす。"""
        if not s or not e:
            return None
        m = (e.year - s.year) * 12 + (e.month - s.month)
        # 例：4/1→9/30 は 5か月差に見えるが、上期として拾いたい
        # なので「日が小さくても減らしすぎない」ように調整は緩めにする
        if e.day < s.day:
            m -= 1
        return m

    def duration_bucket_months(start_ymd, end_ymd):
        """duration期間を「上期(6)」「通期(12)」に分類（6/12以外は無視）"""
        s = parse_ymd(start_ymd)
        e = parse_ymd(end_ymd)
        md = months_diff(s, e)
        if md is None:
            return None

        # 上期：5〜7か月程度を6扱い（会社・暦のズレ吸収）
        if 5 <= md <= 7:
            return 6

        # 通期：11〜13か月程度を12扱い
        if 11 <= md <= 13:
            return 12

        return None

    # ========= 2) メトリクス定義（ここに増やす） =========
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
                # IFRS
                "jpigp_cor:RevenueIFRS",
                "jpigp_cor:NetSalesIFRS",
            ],
            "kind": "duration",
            "unit": "millions",
        },
        "CostOfSales": {
            "tags": ["jppfs_cor:CostOfSales", "jpigp_cor:CostOfSalesIFRS"],
            "kind": "duration",
            "unit": "millions",
        },
        "GrossProfit": {
            "tags": ["jppfs_cor:GrossProfit", "jpigp_cor:GrossProfitIFRS"],
            "kind": "duration",
            "unit": "millions",
        },
        "SellingExpenses": {
            "tags": [
                "jppfs_cor:SellingGeneralAndAdministrativeExpenses",
                "jpigp_cor:SellingGeneralAndAdministrativeExpensesIFRS",
            ],
            "kind": "duration",
            "unit": "millions",
        },
        "OperatingIncome": {
            "tags": ["jppfs_cor:OperatingIncome", "jpigp_cor:OperatingProfitLossIFRS"],
            "kind": "duration",
            "unit": "millions",
        },
        "OrdinaryIncome": {
            "tags": [
                # ★半期サマリー（最優先）
                "jpcrp_cor:OrdinaryIncomeLossSummaryOfBusinessResults",
                # J-GAAP
                "jppfs_cor:OrdinaryIncome",
                # IFRS（経常相当）
                "jpigp_cor:ProfitLossBeforeTaxIFRS",
                "jpigp_cor:ProfitLossBeforeIncomeTaxesIFRS",
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
                "jpigp_cor:CashAndCashEquivalentsIFRS",
                "jpcrp_cor:CashAndCashEquivalentsSummaryOfBusinessResults",
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

                # ★議決権表など“表系”は最後（会社によっては出るが優先しない）
                "jpcrp_cor:NumberOfSharesIssuedSharesVotingRights",
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

    # ========= 2.2) DEI（PeriodEnd / FY start）を先に取得（file1/2/3 共通） =========
    period_end = None
    fy_start_dei = None
    period_type = None

    fy_start_el = soup.find("jpdei_cor:CurrentFiscalYearStartDateDEI")
    if fy_start_el:
        fy_start_dei = fy_start_el.get_text(strip=True)
        out["CurrentFiscalYearStartDateDEI"] = fy_start_dei

    period_end_el = soup.find("jpdei_cor:CurrentPeriodEndDateDEI")
    if period_end_el:
        period_end = period_end_el.get_text(strip=True)
        out["CurrentPeriodEndDateDEI"] = period_end

    type_el = soup.find("jpdei_cor:TypeOfCurrentPeriodDEI")
    if type_el:
        period_type = type_el.get_text(strip=True)
        out["TypeOfCurrentPeriodDEI"] = period_type

    # ★ halfモードなら「上期末」を専用キーでも保持（リネーム等で使いやすくする）
    if mode == "half" and period_end:
        out["HalfPeriodEndDateDEI"] = period_end

    # ========= 3) fact収集 =========
    facts = []  # {metric, value, end, months, dim, tag_priority, kind, unit}
    for metric, meta in METRICS.items():
        for tag_priority, tag in enumerate(meta["tags"]):
            for el in soup.find_all(tag):
                ctxref = el.get("contextRef")
                if not ctxref or ctxref not in contexts:
                    continue
                info = contexts[ctxref]

                if meta["kind"] == "duration":
                    if not info["start"] or not info["end"]:
                        continue
                    months = duration_bucket_months(info["start"], info["end"])
                    if months not in (6, 12):
                        continue
                    end_date = info["end"]
                else:
                    if not info["instant"]:
                        continue
                    months = None
                    end_date = info["instant"]

                facts.append({
                    "metric": metric,
                    "value": el.get_text(strip=True),
                    "start": info["start"],        # ★追加
                    "end": end_date,
                    "months": months,
                    "dim": info["dim"],
                    "tag_priority": tag_priority,
                    "kind": meta["kind"],
                    "unit": meta["unit"],
                })

    # ========= 4) 「基準年」を決める（最新の通期duration end） =========
    fy_ends = sorted(
        {f["end"] for f in facts if f["kind"] == "duration" and f["months"] == 12 and parse_ymd(f["end"])},
        reverse=True
    )
    base_fy_end = fy_ends[0] if fy_ends else None
    base_dt = parse_ymd(base_fy_end) if base_fy_end else None
    base_year = base_dt.year if base_dt else None

    # ========= 4.2) 当期の期首日（FY start）を推定 =========
    fy_start = None

    # (A) DEIの CurrentFiscalYearStartDateDEI があれば最優先
    if fy_start_dei and parse_ymd(fy_start_dei):
        fy_start = fy_start_dei

    # (B) 無ければ CurrentFiscalYearEndDateDEI から逆算（保険）
    if fy_start is None:
        fy_el = soup.find("jpdei_cor:CurrentFiscalYearEndDateDEI")
        if fy_el:
            try:
                fy_end_dei = parse_ymd(fy_el.get_text(strip=True))
                if fy_end_dei:
                    prev_fy_end = fy_end_dei.replace(year=fy_end_dei.year - 1)
                    fy_start = (prev_fy_end + timedelta(days=1)).strftime("%Y-%m-%d")
            except Exception:
                fy_start = None

    # (C) それでも取れなければ従来ロジック（最終保険）
    if fy_start is None and base_fy_end:
        starts = sorted(
            {f["start"] for f in facts
            if f["kind"] == "duration"
            and f["months"] == 12
            and f["end"] == base_fy_end
            and f.get("start")
            and parse_ymd(f["start"])},
            key=lambda s: parse_ymd(s)
        )
        if starts:
            fy_start = starts[0]
    # ========= 5) 連結優先 + タグ優先で 1つ選ぶ =========
    def pick_best(metric, *, end_date, months=None):
        cands = [f for f in facts if f["metric"] == metric and f["end"] == end_date]

        if months is not None:
            cands = [f for f in cands if f["months"] == months]

        if not cands:
            return None

        def dim_rank(d):
            return 0 if d == "Consolidated" else 1

        cands.sort(key=lambda f: (dim_rank(f["dim"]), f["tag_priority"]))
        return cands[0]

    # ========= 6) 出力（自動割り当て） =========
    # (A) duration：上期YTD（当期だけ） + 通期5年（Current/Prior1..4）
    for metric, meta in METRICS.items():
        if meta["kind"] != "duration":
            continue

        ## 上期（当期だけ） -> xxxYTD
        # 0) (halfなら) DEIの period_end があれば「end==period_end & months==6」を最優先
        # 1) 期首(fy_start)が推定できるなら「start==fy_start & months==6」で次に優先
        # 2) ダメなら「months==6 の中で最も新しいend」を拾う（保険）
        best = None

        # (0) half：period_end一致を最優先（※ここでbestを確定させる）
        if mode == "half" and period_end and parse_ymd(period_end):
            cand = pick_best(metric, end_date=period_end, months=6)
            if cand:
                # fy_start が取れているなら start も一致しているものだけ採用（前年YTD誤採用の防止）
                if (fy_start is None) or (cand.get("start") == fy_start):
                    best = cand

        # (1) 期首一致で探す（次に優先）
        if best is None and fy_start:
            h1_ends = sorted(
                {f["end"] for f in facts
                 if f["metric"] == metric
                 and f["months"] == 6
                 and f.get("start") == fy_start
                 and parse_ymd(f["end"])},
                reverse=True
            )
            if h1_ends:
                best = pick_best(metric, end_date=h1_ends[0], months=6)

        # (2) フォールバック：months==6の最新を拾う（上期YTDを必ず作るため）
        if best is None:
            h1_ends_fb = sorted(
                {f["end"] for f in facts
                 if f["metric"] == metric
                 and f["months"] == 6
                 and parse_ymd(f["end"])},
                reverse=True
            )
            if h1_ends_fb:
                best = pick_best(metric, end_date=h1_ends_fb[0], months=6)

        if best:
            out[f"{metric}YTD"] = trim_value(best["value"], meta["unit"])
        else:
            # ★上期YTDは絶対欲しい要望なので、見つからない場合は明示的に埋める
            out[f"{metric}YTD"] = "データなし"

        # ★通期 5年 -> Current/Prior1..4（halfモードでは作らない）
        if mode != "half":
            fy_ends_metric = sorted(
                {f["end"] for f in facts
                 if f["metric"] == metric and f["months"] == 12 and parse_ymd(f["end"])},
                reverse=True
            )
            for end_date in fy_ends_metric:
                if base_year is None:
                    break
                dt = parse_ymd(end_date)
                if not dt:
                    continue
                diff = base_year - dt.year
                if diff < 0 or diff > 4:
                    continue

                suffix = "Current" if diff == 0 else f"Prior{diff}"
                best = pick_best(metric, end_date=end_date, months=12)
                if best:
                    out[f"{metric}{suffix}"] = trim_value(best["value"], meta["unit"])

    # (B) instant：通期5年（Current/Prior1..4） + 上期末（Quarter）
    # ルール：
    # - Quarterは「最新のinstant」を採用（＝上期末が最新になりやすい）
    # - 通期5年は「年」で割り当て（base_yearとの差分）だが halfモードでは作らない
    for metric, meta in METRICS.items():
        if meta["kind"] == "duration":
            continue

        inst_ends = sorted(
            {f["end"] for f in facts if f["metric"] == metric and parse_ymd(f["end"])},
            reverse=True
        )

        # Quarter：DEIのCurrentPeriodEndDateDEIを最優先（無ければ最も近いinstant、最後に最新）
        if inst_ends:
            chosen_end = inst_ends[0]  # 従来フォールバック（最新）

            target_dt = parse_ymd(period_end) if period_end else None
            if target_dt:
                # 1) 完全一致があればそれ
                if period_end in inst_ends:
                    chosen_end = period_end
                else:
                    # 2) 最も近い日付を選ぶ（ただしFY startより前は除外して飛びを防止）
                    fy_start_dt = parse_ymd(fy_start) if fy_start else None

                    inst_dts = []
                    for e in inst_ends:
                        dt = parse_ymd(e)
                        if not dt:
                            continue
                        # FY start が取れている場合は、それ以前を候補から除外
                        if fy_start_dt and dt < fy_start_dt:
                            continue
                        inst_dts.append(dt)

                    # FY start 制限で候補が空なら、制限なしで再収集（保険）
                    if not inst_dts:
                        for e in inst_ends:
                            dt = parse_ymd(e)
                            if dt:
                                inst_dts.append(dt)

                    if inst_dts:
                        inst_dts.sort(key=lambda dt: abs((dt - target_dt).days))
                        chosen_end = inst_dts[0].strftime("%Y-%m-%d")

            best_q = pick_best(metric, end_date=chosen_end)
            if best_q:
                out[f"{metric}Quarter"] = trim_value(best_q["value"], meta["unit"])
        # ★通期5年：年で Current/Prior1..4 を作る（halfモードでは作らない）
        if mode != "half":
            for end_date in inst_ends:
                if base_year is None:
                    break
                dt = parse_ymd(end_date)
                if not dt:
                    continue
                diff = base_year - dt.year
                if diff < 0 or diff > 4:
                    continue

                suffix = "Current" if diff == 0 else f"Prior{diff}"
                best = pick_best(metric, end_date=end_date)
                if best:
                    out[f"{metric}{suffix}"] = trim_value(best["value"], meta["unit"])

    # ========= 6.5) TotalNumber を「自己株式控除後」に統一して生成 =========
    # TotalNumber = IssuedShares - TreasuryShares
    # halfモードでは Quarter だけ作る（Current/Prior は作らない）
    if mode == "half":
        suffixes = ["Quarter"]
    else:
        suffixes = ["Current", "Prior1", "Prior2", "Prior3", "Prior4", "Quarter"]

    for suffix in suffixes:
        issued = out.get(f"IssuedShares{suffix}")
        treasury = out.get(f"TreasuryShares{suffix}")

        if isinstance(issued, int) and isinstance(treasury, int):
            out[f"TotalNumber{suffix}"] = issued - treasury

    # ========= 7) 証券コード等 =========
    security_code = None
    sc_el = soup.find("jpdei_cor:SecurityCodeDEI")
    if sc_el:
        sc = sc_el.get_text(strip=True)
        if sc.isdigit() and len(sc) >= 2:
            security_code = sc[:-1]
            out["SecurityCodeDEI"] = security_code

    # ========= 会社名（DEI） =========
    name_tags = [
        "jpdei_cor:FilerNameInJapaneseDEI",     # 通常これ
        "jpdei_cor:FilerNameDEI",               # 会社によってはこれ
    ]

    for tag in name_tags:
        name_el = soup.find(tag)
        if name_el:
            out["CompanyNameCoverPage"] = name_el.get_text(strip=True)
            break

    # FY end date（年/月分解用に保持）
    fy_el = soup.find("jpdei_cor:CurrentFiscalYearEndDateDEI")
    if fy_el:
        out["CurrentFiscalYearEndDateDEI"] = fy_el.get_text(strip=True)

    # デバッグ（必要なければ消してOK）
    print("\n=== OUT KEYS ===")
    for k in sorted(out.keys()):
        print(k)

    return out, security_code

# Excelへのデータ書き込み
def write_data_to_excel(excel_file, data, cell_map):
    workbook = openpyxl.load_workbook(excel_file)
    sheet = workbook['決算入力']

    for key, cell in cell_map.items():
        if key not in data:
            continue

        value = data[key]

        # None や 「データなし」は書かない
        if value is None:
            continue
        if isinstance(value, str) and value.strip() in ("", "データなし"):
            continue

        targets = cell if isinstance(cell, list) else [cell]

        for c in targets:
            current_value = sheet[c].value

            # ==========================
            # ★ 数式セルは上書きしない
            # ==========================
            if isinstance(current_value, str) and current_value.startswith("="):
                print(f"数式セル {c} はスキップしました。")
                continue

            sheet[c] = value

    workbook.save(excel_file)       

# 株価データの取得　調整前株価：auto_adjust=False　調整後株価：auto_adjust=True　←株価同じ。今後変わるかも(2025.02.22現在)
def get_stock_price(stock_code, target_date, backup_date, buffer_days=3):
    start_date = (datetime.strptime(backup_date, '%Y-%m-%d') - timedelta(days=buffer_days)).strftime('%Y-%m-%d')
    end_date   = (datetime.strptime(target_date, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')

    hist = yf.download(tickers=stock_code, start=start_date, end=end_date, auto_adjust=False)

    # tz-aware の時だけ外す（naiveなら何もしない）
    if getattr(hist.index, "tz", None) is not None:
        hist.index = hist.index.tz_localize(None)

    check_date = datetime.strptime(target_date, '%Y-%m-%d')
    start_dt   = datetime.strptime(start_date, '%Y-%m-%d')

    while check_date >= start_dt:
        ts = pd.Timestamp(check_date.date())
        if ts in hist.index:
            close = hist.loc[ts, 'Close']
            return float(close)  # scalar想定
        check_date -= timedelta(days=1)

    return None

# 株価データをExcelに書き込む
def write_stock_data_to_excel(excel_file, stock_code, date_pairs):
    workbook = openpyxl.load_workbook(excel_file)
    sheet = workbook['決算入力']
    
    for dates in date_pairs:
        price = get_stock_price(stock_code, dates['target_date'], dates['backup_date'])
        if price is not None:
            sheet[dates['cell']] = float(price.iloc[0]) if isinstance(price, pd.Series) else float(price)

            print(f"{dates['target_date']}の株価がセル{dates['cell']}に保存されました。")
        else:
            print(f"{dates['target_date']}のデータが取得できませんでした。")
    
    workbook.save(excel_file)

# Excelファイルのリネーム（安全版）
def safe_filename(s: str) -> str:
    """
    Windowsでファイル名に使えない文字を置換して安全にする
    """
    if s is None:
        return ""
    s = str(s).strip()
    # Windows NG: \ / : * ? " < > |
    s = re.sub(r'[\\/:*?"<>|]', '_', s)
    # 末尾のドット/スペースもNG
    s = s.rstrip(". ").strip()
    return s

def rename_excel_file(original_path, security_code, company_name, period_end_date):
    """
    original_path を「<code>_<name>_<date>.xlsx」にリネームし、パスを返す。
    同名があれば _1, _2... を付ける。
    """
    dir_path = os.path.dirname(original_path)

    code = safe_filename(security_code)
    name = safe_filename(company_name)
    date = safe_filename(period_end_date)

    base_name = f"{code}_{name}_{date}".strip("_")
    if not base_name:
        raise ValueError("リネーム用の情報が不足しています（code/name/date が空です）。")

    new_file_path = os.path.join(dir_path, f"{base_name}.xlsx")

    counter = 1
    while os.path.exists(new_file_path):
        new_file_path = os.path.join(dir_path, f"{base_name}_{counter}.xlsx")
        counter += 1

    try:
        os.rename(original_path, new_file_path)
    except PermissionError as e:
        # Excelで開いている、または別プロセスが掴んでいる可能性が高い
        raise PermissionError(
            f"リネームできません（ファイルが開かれている可能性があります）。\n"
            f"対象: {original_path}\n"
            f"対策: Excelで該当ファイルを閉じてから再実行してください。"
        ) from e

    print(f"Excelファイルがリネームされました: {new_file_path}")
    return new_file_path

cell_map_annual = {
    # 売上
    'NetSalesPrior4': 'D5',
    'NetSalesPrior3': 'G5',
    'NetSalesPrior2': 'J5',
    'NetSalesPrior1': 'M5',
    'NetSalesCurrent': 'P36',   # 半期なしのときだけ使う

    # 売上原価〜営業利益（当期P6〜P9 は半期なしのときだけ）
    'CostOfSalesPrior4': 'D6', 'CostOfSalesPrior3': 'G6', 'CostOfSalesPrior2': 'J6', 'CostOfSalesPrior1': 'M6', 'CostOfSalesCurrent': 'P6',
    'GrossProfitPrior4': 'D7', 'GrossProfitPrior3': 'G7', 'GrossProfitPrior2': 'J7', 'GrossProfitPrior1': 'M7', 'GrossProfitCurrent': 'P7',
    'SellingExpensesPrior4': 'D8','SellingExpensesPrior3': 'G8','SellingExpensesPrior2': 'J8','SellingExpensesPrior1': 'M8','SellingExpensesCurrent': 'P8',
    'OperatingIncomePrior4': 'D9','OperatingIncomePrior3': 'G9','OperatingIncomePrior2': 'J9','OperatingIncomePrior1': 'M9','OperatingIncomeCurrent': 'P9',

    # 経常・純利益
    'OrdinaryIncomePrior4': 'D10',
    'OrdinaryIncomePrior3': 'G10',
    'OrdinaryIncomePrior2': 'J10',
    'OrdinaryIncomePrior1': 'M10',
    'OrdinaryIncomeCurrent': 'P37',  # 半期なしのみ

    'ProfitLossPrior4': 'D11',
    'ProfitLossPrior3': 'G11',
    'ProfitLossPrior2': 'J11',
    'ProfitLossPrior1': 'M11',
    'ProfitLossCurrent': 'P38',      # 半期なしのみ

    # 発行株式数（自己株控除後）
    'TotalNumberPrior4': 'D13',
    'TotalNumberPrior3': 'G13',
    'TotalNumberPrior2': 'J13',
    'TotalNumberPrior1': 'M13',
    'TotalNumberCurrent': 'P40',     # 半期なしのみ

    # BS
    'TotalAssetsPrior4': 'C17',
    'TotalAssetsPrior3': 'F17',
    'TotalAssetsPrior2': 'I17',
    'TotalAssetsPrior1': 'L17',
    'TotalAssetsCurrent': 'O44',     # 半期なしのみ（あなたの運用に合わせたセル）

    'NetAssetsPrior4': 'D17',
    'NetAssetsPrior3': 'G17',
    'NetAssetsPrior2': 'J17',
    'NetAssetsPrior1': 'M17',
    'NetAssetsCurrent': 'P44',       # 半期なしのみ

    # CF
    'OperatingCashPrior4': 'C21',
    'OperatingCashPrior3': 'F21',
    'OperatingCashPrior2': 'I21',
    'OperatingCashPrior1': 'L21',
    'OperatingCashCurrent': 'O48',   # 半期なしのみ

    'InvestmentCashPrior4': 'D21',
    'InvestmentCashPrior3': 'G21',
    'InvestmentCashPrior2': 'J21',
    'InvestmentCashPrior1': 'M21',
    'InvestmentCashCurrent': 'P48',  # 半期なしのみ

    'FinancingCashPrior4': 'E21',
    'FinancingCashPrior3': 'H21',
    'FinancingCashPrior2': 'K21',
    'FinancingCashPrior1': 'N21',
    'FinancingCashCurrent': 'Q48',   # 半期なしのみ

    'CashAndCashEquivalentsPrior4': 'D22',
    'CashAndCashEquivalentsPrior3': 'G22',
    'CashAndCashEquivalentsPrior2': 'J22',
    'CashAndCashEquivalentsPrior1': 'M22',
    'CashAndCashEquivalentsCurrent': 'P49',  # 半期なしのみ

    # 表紙
    'SecurityCodeDEI': 'K2',
    'CompanyNameCoverPage': 'L2',
    'CurrentFiscalYearEndDateDEIyear': 'N2',
    'CurrentFiscalYearEndDateDEImonth': 'O2',
}

cell_map_half = {
    'NetSalesYTD': 'J36',
    'OrdinaryIncomeYTD': 'J37',
    'ProfitLossYTD': 'J38',

    'TotalNumberQuarter': 'J40',
    'TotalAssetsQuarter': 'I44',
    'NetAssetsQuarter': 'J44',

    'OperatingCashYTD': 'I48',
    'InvestmentCashYTD': 'J48',
    'FinancingCashYTD': 'K48',

    'CashAndCashEquivalentsQuarter': 'J49',
}

# ファイルパスを順番に確認する関数
def find_available_excel_file(base_path, file_name, max_copies=10):
    for i in range(max_copies):
        if i == 0:
            file_path = os.path.join(base_path, f"{file_name} - コピー.xlsx")
        else:
            file_path = os.path.join(base_path, f"{file_name} - コピー ({i}).xlsx")
        
        if os.path.exists(file_path):
            return file_path
    
    print(f"{file_name}のいずれのバージョンも見つかりませんでした。")
    return None

# 各ループで処理するファイルパスをリスト化
loops = [
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '1-2*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '1-4*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '1-5*.xbrl'))
        },
        'excel_file_path': r'C:\Users\silve\OneDrive\PC\EDINET\決算分析シート\決算分析シート1.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '2-2*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '2-4*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '2-5*.xbrl'))
        },
        'excel_file_path': r'C:\Users\silve\OneDrive\PC\EDINET\決算分析シート\決算分析シート2.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '3-2*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '3-4*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '3-5*.xbrl'))
        },
        'excel_file_path': r'C:\Users\silve\OneDrive\PC\EDINET\決算分析シート\決算分析シート3.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '4-2*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '4-4*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '4-5*.xbrl'))
        },
        'excel_file_path': r'C:\Users\silve\OneDrive\PC\EDINET\決算分析シート\決算分析シート4.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '5-2*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '5-4*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '5-5*.xbrl'))
        },
        'excel_file_path': r'C:\Users\silve\OneDrive\PC\EDINET\決算分析シート\決算分析シート5.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '6-2*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '6-4*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '6-5*.xbrl'))
        },
        'excel_file_path': r'C:\Users\silve\OneDrive\PC\EDINET\決算分析シート\決算分析シート6.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '7-2*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '7-4*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '7-5*.xbrl'))
        },
        'excel_file_path': r'C:\Users\silve\OneDrive\PC\EDINET\決算分析シート\決算分析シート7.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '8-2*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '8-4*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '8-5*.xbrl'))
        },
        'excel_file_path': r'C:\Users\silve\OneDrive\PC\EDINET\決算分析シート\決算分析シート8.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '9-2*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '9-4*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '9-5*.xbrl'))
        },
        'excel_file_path': r'C:\Users\silve\OneDrive\PC\EDINET\決算分析シート\決算分析シート9.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '10-2*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '10-4*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '10-5*.xbrl'))
        },
        'excel_file_path': r'C:\Users\silve\OneDrive\PC\EDINET\決算分析シート\決算分析シート10.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '11-2*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '11-4*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '11-5*.xbrl'))
        },
        'excel_file_path': r'C:\Users\silve\OneDrive\PC\EDINET\決算分析シート\決算分析シート11.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '12-2*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '12-4*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '12-5*.xbrl'))
        },
        'excel_file_path': r'C:\Users\silve\OneDrive\PC\EDINET\決算分析シート\決算分析シート12.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '13-2*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '13-4*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '13-5*.xbrl'))
        },
        'excel_file_path': r'C:\Users\silve\OneDrive\PC\EDINET\決算分析シート\決算分析シート13.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '14-2*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '14-4*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '14-5*.xbrl'))
        },
        'excel_file_path': r'C:\Users\silve\OneDrive\PC\EDINET\決算分析シート\決算分析シート14.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '15-2*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '15-4*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '15-5*.xbrl'))
        },
        'excel_file_path': r'C:\Users\silve\OneDrive\PC\EDINET\決算分析シート\決算分析シート15.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '16-2*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '16-4*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '16-5*.xbrl'))
        },
        'excel_file_path': r'C:\Users\silve\OneDrive\PC\EDINET\決算分析シート\決算分析シート16.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '17-2*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '17-4*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '17-5*.xbrl'))
        },
        'excel_file_path': r'C:\Users\silve\OneDrive\PC\EDINET\決算分析シート\決算分析シート17.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '18-2*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '18-4*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '18-5*.xbrl'))
        },
        'excel_file_path': r'C:\Users\silve\OneDrive\PC\EDINET\決算分析シート\決算分析シート18.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '19-2*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '19-4*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '19-5*.xbrl'))
        },
        'excel_file_path': r'C:\Users\silve\OneDrive\PC\EDINET\決算分析シート\決算分析シート19.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '20-2*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '20-4*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '20-5*.xbrl'))
        },
        'excel_file_path': r'C:\Users\silve\OneDrive\PC\EDINET\決算分析シート\決算分析シート20.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '21-2*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '21-4*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '21-5*.xbrl'))
        },
        'excel_file_path': r'C:\Users\silve\OneDrive\PC\EDINET\決算分析シート\決算分析シート21.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '22-2*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '22-4*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '22-5*.xbrl'))
        },
        'excel_file_path': r'C:\Users\silve\OneDrive\PC\EDINET\決算分析シート\決算分析シート22.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '23-2*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '23-4*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '23-5*.xbrl'))
        },
        'excel_file_path': r'C:\Users\silve\OneDrive\PC\EDINET\決算分析シート\決算分析シート23.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '24-2*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '24-4*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '24-5*.xbrl'))
        },
        'excel_file_path': r'C:\Users\silve\OneDrive\PC\EDINET\決算分析シート\決算分析シート24.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '25-2*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '25-4*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '25-5*.xbrl'))
        },
        'excel_file_path': r'C:\Users\silve\OneDrive\PC\EDINET\決算分析シート\決算分析シート25.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '26-2*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '26-4*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '26-5*.xbrl'))
        },
        'excel_file_path': r'C:\Users\silve\OneDrive\PC\EDINET\決算分析シート\決算分析シート26.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '27-2*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '27-4*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '27-5*.xbrl'))
        },
        'excel_file_path': r'C:\Users\silve\OneDrive\PC\EDINET\決算分析シート\決算分析シート27.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '28-2*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '28-4*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '28-5*.xbrl'))
        },
        'excel_file_path': r'C:\Users\silve\OneDrive\PC\EDINET\決算分析シート\決算分析シート28.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '29-2*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '29-4*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '29-5*.xbrl'))
        },
        'excel_file_path': r'C:\Users\silve\OneDrive\PC\EDINET\決算分析シート\決算分析シート29.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '30-2*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '30-4*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '30-5*.xbrl'))
        },
        'excel_file_path': r'C:\Users\silve\OneDrive\PC\EDINET\決算分析シート\決算分析シート30.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '31-2*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '31-4*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '31-5*.xbrl'))
        },
        'excel_file_path': r'C:\Users\silve\OneDrive\PC\EDINET\決算分析シート\決算分析シート31.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '32-2*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '32-4*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '32-5*.xbrl'))
        },
        'excel_file_path': r'C:\Users\silve\OneDrive\PC\EDINET\決算分析シート\決算分析シート32.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '33-2*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '33-4*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '33-5*.xbrl'))
        },
        'excel_file_path': r'C:\Users\silve\OneDrive\PC\EDINET\決算分析シート\決算分析シート33.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '34-2*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '34-4*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '34-5*.xbrl'))
        },
        'excel_file_path': r'C:\Users\silve\OneDrive\PC\EDINET\決算分析シート\決算分析シート34.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '35-2*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '35-4*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '35-5*.xbrl'))
        },
        'excel_file_path': r'C:\Users\silve\OneDrive\PC\EDINET\決算分析シート\決算分析シート35.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '36-2*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '36-4*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '36-5*.xbrl'))
        },
        'excel_file_path': r'C:\Users\silve\OneDrive\PC\EDINET\決算分析シート\決算分析シート36.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '37-2*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '37-4*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '37-5*.xbrl'))
        },
        'excel_file_path': r'C:\Users\silve\OneDrive\PC\EDINET\決算分析シート\決算分析シート37.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '38-2*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '38-4*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '38-5*.xbrl'))
        },
        'excel_file_path': r'C:\Users\silve\OneDrive\PC\EDINET\決算分析シート\決算分析シート38.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '39-2*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '39-4*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '39-5*.xbrl'))
        },
        'excel_file_path': r'C:\Users\silve\OneDrive\PC\EDINET\決算分析シート\決算分析シート39.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '40-2*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '40-4*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '40-5*.xbrl'))
        },
        'excel_file_path': r'C:\Users\silve\OneDrive\PC\EDINET\決算分析シート\決算分析シート40.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '41-2*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '41-4*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '41-5*.xbrl'))
        },
        'excel_file_path': r'C:\Users\silve\OneDrive\PC\EDINET\決算分析シート\決算分析シート41.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '42-2*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '42-4*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '42-5*.xbrl'))
        },
        'excel_file_path': r'C:\Users\silve\OneDrive\PC\EDINET\決算分析シート\決算分析シート42.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '43-2*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '43-4*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '43-5*.xbrl'))
        },
        'excel_file_path': r'C:\Users\silve\OneDrive\PC\EDINET\決算分析シート\決算分析シート43.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '44-2*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '44-4*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '44-5*.xbrl'))
        },
        'excel_file_path': r'C:\Users\silve\OneDrive\PC\EDINET\決算分析シート\決算分析シート44.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '45-2*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '45-4*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '45-5*.xbrl'))
        },
        'excel_file_path': r'C:\Users\silve\OneDrive\PC\EDINET\決算分析シート\決算分析シート45.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '46-2*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '46-4*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '46-5*.xbrl'))
        },
        'excel_file_path': r'C:\Users\silve\OneDrive\PC\EDINET\決算分析シート\決算分析シート46.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '47-2*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '47-4*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '47-5*.xbrl'))
        },
        'excel_file_path': r'C:\Users\silve\OneDrive\PC\EDINET\決算分析シート\決算分析シート47.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '48-2*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '48-4*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '48-5*.xbrl'))
        },
        'excel_file_path': r'C:\Users\silve\OneDrive\PC\EDINET\決算分析シート\決算分析シート48.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '49-2*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '49-4*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '49-5*.xbrl'))
        },
        'excel_file_path': r'C:\Users\silve\OneDrive\PC\EDINET\決算分析シート\決算分析シート49.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '50-2*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '50-4*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '50-5*.xbrl'))
        },
        'excel_file_path': r'C:\Users\silve\OneDrive\PC\EDINET\決算分析シート\決算分析シート50.xlsx'
    }
]

print("DEBUG file1:", glob.glob(os.path.join(base_dir, '1-2*.xbrl')))
print("DEBUG file2:", glob.glob(os.path.join(base_dir, '1-4*.xbrl')))
print("DEBUG file3:", glob.glob(os.path.join(base_dir, '1-5*.xbrl')))
print("DEBUG all files:", os.listdir(base_dir))

#「どのキーをどの書類が書くか」を分けるフィルタ
def filter_keys_for_file(data: dict, file_kind: str) -> dict:
    """
    file_kind:
      - "half"  : file1（半期最新）→ YTD と Quarter と DEIだけ
      - "annual": file2（最新有報）→ Current/Prior* と DEIだけ（YTD/Quarterは触らない）
      - "annual_old": file3（過去有報）→ Prior* だけ（Currentは触らない）
    """
    out = {}

    for k, v in data.items():
        if file_kind == "half":
            if k.endswith("YTD") or k.endswith("Quarter") or k in ("SecurityCodeDEI", "CurrentFiscalYearEndDateDEI"):
                out[k] = v

        elif file_kind == "annual":
            if (k.endswith("Current") or k.endswith(("Prior1","Prior2","Prior3","Prior4"))
                or k in ("SecurityCodeDEI", "CurrentFiscalYearEndDateDEI")):
                # YTD/Quarterは annual 側では書かない
                if not (k.endswith("YTD") or k.endswith("Quarter")):
                    out[k] = v

        elif file_kind == "annual_old":
            # 古い有報はPriorだけ埋めたい
            if k.endswith(("Prior1", "Prior2", "Prior3", "Prior4")):
                out[k] = v
            # DEIは触らなくてOK（必要なら入れても良いが基本不要）

    return out

# ===============================
# 有報（XBRL）の期末日から「期末年（西暦）」を取得する関数
# 例：
#   "2025-03-31" → 2025
# file2（最新有報）の基準年を決めるために使用する
# ===============================
def get_fy_end_year(xbrl_data: dict) -> int | None:
    """
    CurrentFiscalYearEndDateDEI (YYYY-MM-DD) から期末年(西暦)を取り出す
    """
    s = xbrl_data.get("CurrentFiscalYearEndDateDEI")
    if isinstance(s, str):
        try:
            return datetime.strptime(s, "%Y-%m-%d").year
        except Exception:
            return None
    return None

def shift_suffixes_by_yeargap(data: dict, year_gap: int) -> dict:
    """
    file2の期末年を基準に、古い有報(file3)の Current/Prior を Prior側へずらす。

    year_gap = base_year(file2) - year(file3)
    例：file2=2025, file3=2023 → year_gap=2
        file3の NetSalesCurrent → NetSalesPrior2
        file3の NetSalesPrior1 → NetSalesPrior3
    """
    if year_gap <= 0:
        return data

    out = {}
    for k, v in data.items():
        m = re.match(r"^(.*?)(Current|Prior(\d+))$", k)
        if not m:
            out[k] = v
            continue

        prefix = m.group(1)
        suf = m.group(2)

        if suf == "Current":
            n = year_gap
        else:
            n = year_gap + int(m.group(3))

        # Prior1..4 の範囲だけ採用（あなたのExcelが5年枠の想定のため）
        if 1 <= n <= 4:
            out[f"{prefix}Prior{n}"] = v

    return out

def shift_with_keep(data: dict, year_gap: int,
                    keep_keys=("SecurityCodeDEI","CurrentFiscalYearEndDateDEI","CompanyNameCoverPage")) -> dict:
    """
    shift_suffixes_by_yeargap は Current/Prior 系しか返さないため、
    DEIなど「残したいキー」を shift後に戻すラッパー
    """
    shifted = shift_suffixes_by_yeargap(data, year_gap)
    for k in keep_keys:
        if k in data:
            shifted[k] = data[k]
    return shifted


def filter_for_annual(data: dict) -> dict:
    """通期比較（Current/Prior* + DEI）。YTD/Quarterは除外。"""
    out = {}
    for k, v in data.items():
        if k.endswith("YTD") or k.endswith("Quarter"):
            continue
        if k.endswith("Current") or k.endswith(("Prior1","Prior2","Prior3","Prior4")):
            out[k] = v
        if k in ("SecurityCodeDEI", "CurrentFiscalYearEndDateDEI", "CompanyNameCoverPage",
                 "CurrentFiscalYearEndDateDEIyear","CurrentFiscalYearEndDateDEImonth"):
            out[k] = v
    return out


def filter_for_annual_old(data: dict) -> dict:
    """古い有報はPrior側だけ（Currentは触らない）"""
    out = {}
    for k, v in data.items():
        if k.endswith(("Prior1","Prior2","Prior3","Prior4")):
            out[k] = v
    return out


def filter_for_half(data: dict) -> dict:
    """半期はYTD/Quarterだけ + DEI必要分"""
    out = {}
    for k, v in data.items():
        if k.endswith("YTD") or k.endswith("Quarter"):
            out[k] = v
        if k in ("SecurityCodeDEI", "CurrentFiscalYearEndDateDEI",
                 "CurrentFiscalYearEndDateDEIyear","CurrentFiscalYearEndDateDEImonth"):
            out[k] = v
    return out


def make_annual_map_for_use_half(use_half: bool, base_map: dict) -> dict:
    """
    半期ありの場合：
      - 通期Current（P列やO44/P44/O48…）はテンプレ仕様上「書かない」
      - Prior側（D/G/J/M）は埋める
    """
    m = dict(base_map)
    if use_half:
        for k in list(m.keys()):
            if k.endswith("Current") and k not in ("SecurityCodeDEI","CompanyNameCoverPage",
                                                  "CurrentFiscalYearEndDateDEIyear","CurrentFiscalYearEndDateDEImonth"):
                m.pop(k, None)
    return m

# 処理するファイル数を選択
def choose_file_count():
    while True:
        try:
            count = int(input("処理するファイルの数を選択してください（1～50）: "))
            if 1 <= count <= 50:
                return count
            else:
                print("1から50の範囲で入力してください。")
        except ValueError:
            print("数字を入力してください。")

# ファイル数の選択
file_count = choose_file_count()

# スキップされたファイルを記録するリスト
skipped_files = []

# 決算期を最初に1回だけ選択
try:
    def load_config(file_path):
        with open(file_path, 'r') as f:
            return json.load(f)

    config = load_config('決算期_KANPE.json')
    chosen_period = input("決算期を選択してください（例 25-1）: ")

    if chosen_period in config:
        date_pairs = config[chosen_period]
        print(f"選択された決算期: {chosen_period}")
        print("決算期データ:", date_pairs)
    else:
        print("無効な選択です。プログラムを終了します。")
        exit()  # 無効な選択の場合、プログラム終了
except Exception as e:
    print(f"設定ファイルの読み込み中にエラー: {e}")
    exit()

# XBRLデータの取得、証券コードの取得、Excelへの書き込み、株価データ取得までをループ処理に含める
for i in range(file_count):
    loop = loops[i]
    excel_base_name = os.path.basename(loop['excel_file_path']).replace('.xlsx', '')
    excel_directory = os.path.dirname(loop['excel_file_path'])

    selected_file = find_available_excel_file(excel_directory, excel_base_name)

    if selected_file:
        loop['excel_file_path'] = selected_file
        print(f"ーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーー\n使用するExcelファイル: {selected_file}")
    else:
        skipped_files.append({'reason': "Excelファイルが見つからない", 'file': excel_base_name})
        print("使用するファイルが見つかりませんでした。次のループを実行します。")
        continue

    rename_info = None
    xbrl_file_paths = loop['xbrl_file_paths']
    excel_file_path = loop['excel_file_path']
    security_code = None
    base_year = None

    # -------------------------
    # 0) file1（半期）があれば先に読む（base_year決定）
    # -------------------------
    x1 = None
    use_half = bool(xbrl_file_paths.get("file1") and xbrl_file_paths["file1"])

    if use_half:
        try:
            path1 = xbrl_file_paths["file1"][0]          # ★追加：path1 を定義
            x1, _ = parse_xbrl_data(path1, mode="half")  # ★ここで使う
            base_year = get_fy_end_year(x1)              # 半期の期末年（例：2025）
        except Exception as e:
            skipped_files.append({'reason': f"file1(半期) 解析エラー: {e}", 'file': excel_file_path})
            x1 = None
            use_half = False

    # 半期あり/なしで「通期書き込みマップ」を確定
    annual_map = make_annual_map_for_use_half(use_half, cell_map_annual)

    # -------------------------
    # 1) file2（最新有報）→ 通期比較を埋める
    # -------------------------
    x2 = None
    if xbrl_file_paths.get("file2") and xbrl_file_paths["file2"]:
        try:
            path2 = xbrl_file_paths["file2"][0]
            x2, security_code = parse_xbrl_data(path2, mode="full")

            # --- base_year を決める（半期が無いなら有報が基準） ---
            y2 = get_fy_end_year(x2)
            if base_year is None and y2 is not None:
                base_year = y2

            # --- file2 が base_year より古い年なら Prior側へずらす（保険） ---
            if base_year is not None and y2 is not None:
                gap2 = base_year - y2
                if gap2 > 0:
                    x2 = shift_with_keep(x2, gap2)

            # --- 表紙（N2/O2）は「半期ありなら file1 のFY end」「半期なしなら file2 のFY end」 ---
            fy_source = None
            if use_half and x1 and x1.get("CurrentFiscalYearEndDateDEI"):
                fy_source = x1["CurrentFiscalYearEndDateDEI"]
            elif x2.get("CurrentFiscalYearEndDateDEI"):
                fy_source = x2["CurrentFiscalYearEndDateDEI"]

            if fy_source:
                dt = datetime.strptime(fy_source, "%Y-%m-%d")
                x2["CurrentFiscalYearEndDateDEIyear"] = dt.year
                x2["CurrentFiscalYearEndDateDEImonth"] = dt.month

            # --- 通期比較を書き込み（annual_map は use_half により Current が入ったり消えたりする） ---
            write_data_to_excel(excel_file_path, filter_for_annual(x2), annual_map)

            # --- リネーム用：半期ありなら period_end（上期末）を優先、半期なしならFY end ---
            period_for_name = None
            if use_half and x1 and x1.get("CurrentPeriodEndDateDEI"):
                period_for_name = x1["CurrentPeriodEndDateDEI"]
            elif x2.get("CurrentFiscalYearEndDateDEI"):
                period_for_name = x2["CurrentFiscalYearEndDateDEI"]

            if x2.get("SecurityCodeDEI") and period_for_name:
                cname = x2.get("CompanyNameCoverPage", "")
                rename_info = (x2["SecurityCodeDEI"], cname, period_for_name)

        except Exception as e:
            skipped_files.append({'reason': f"file2(最新有報) 解析/書込エラー: {e}", 'file': excel_file_path})
    else:
        skipped_files.append({'reason': "file2(最新有報) が見つからない", 'file': excel_file_path})

    # -------------------------
    # 2) file3（過去有報）→ Prior補完（base_year必須）
    # -------------------------
    if base_year is not None and xbrl_file_paths.get("file3") and xbrl_file_paths["file3"]:
        try:
            path3 = xbrl_file_paths["file3"][0]          # ★追加：path3 を定義
            x3, _ = parse_xbrl_data(path3, mode="full")

            y3 = get_fy_end_year(x3)
            if y3 is not None:
                gap3 = base_year - y3
                if gap3 > 0:
                    x3 = shift_with_keep(x3, gap3)

                write_data_to_excel(excel_file_path, filter_for_annual_old(x3), annual_map)
            else:
                skipped_files.append({'reason': "file3 期末年が取れない", 'file': excel_file_path})

        except Exception as e:
            skipped_files.append({'reason': f"file3(過去有報) 解析/書込エラー: {e}", 'file': excel_file_path})

    # -------------------------
    # 3) 半期ありなら最後に YTD/Quarter を確定（最優先）
    # -------------------------
    if use_half and x1 is not None:
        if 'CurrentFiscalYearEndDateDEI' in x1:
            try:
                dt = datetime.strptime(x1['CurrentFiscalYearEndDateDEI'], "%Y-%m-%d")
                x1['CurrentFiscalYearEndDateDEIyear'] = dt.year
                x1['CurrentFiscalYearEndDateDEImonth'] = dt.month
            except Exception:
                pass

        write_data_to_excel(excel_file_path, filter_for_half(x1), cell_map_half)

    # 証券コードを表示（デバッグ用）
    if security_code:
        print(f"取得した証券コード: {security_code}")
        stock_code = f"{security_code}.T"

        try:
            write_stock_data_to_excel(excel_file_path, stock_code, date_pairs)
        except Exception as e:
            skipped_files.append({'reason': f"株価データの書き込み中にエラー: {e}", 'file': excel_file_path})
            print(f"株価データの書き込み中にエラーが発生しました。スキップします。")
        # ★ ここに追加
        if rename_info:
            try:
                excel_file_path = rename_excel_file(
                    excel_file_path,
                    rename_info[0],  # security_code
                    rename_info[1],  # company_name
                    rename_info[2],  # period_end_date
                )
            except Exception as e:
                print(f"リネーム中にエラー: {e}")

    else:
        skipped_files.append({'reason': "証券コードが取得できない", 'file': excel_file_path})
        print("証券コードが取得できませんでした。")

# スキップされたファイルの一覧を表示
print("\n--- スキップされたファイル一覧 ---")
if skipped_files:
    for skipped in skipped_files:
        print(f"理由: {skipped['reason']}, ファイル: {skipped['file']}")
else:
    print("スキップされたファイルはありません。")