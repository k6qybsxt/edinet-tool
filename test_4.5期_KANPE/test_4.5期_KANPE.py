import os
import openpyxl
from bs4 import BeautifulSoup
import yfinance as yf
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
    root.withdraw()  # Tkinterのウィンドウを非表示にする
    folder_path = filedialog.askdirectory(title="XBRLフォルダを選択してください")
    return folder_path

# メイン処理
print("XBRLフォルダを選択してください。")
base_dir = choose_directory()

if not base_dir:
    print("XBRLフォルダが選択されませんでした。プログラムを終了します。")
    exit()

# 数値を指定された単位で切り捨て
def trim_value(value, unit='millions'):
    try:
        factor = {'millions': 1_000_000, 'thousands': 1_000, 'ten': 10}[unit]
        return int(value) // factor
    except ValueError:
        return 'データなし'

# XBRLデータの解析
def parse_xbrl_data(xbrl_file):
    with open(xbrl_file, "r", encoding="utf-8") as f:
        soup = BeautifulSoup(f, "lxml-xml")

    # -------------------------
    # 1) context 情報を構造で保持
    # -------------------------
    contexts = {}
    for ctx in soup.find_all("context"):
        ctx_id = ctx.get("id")

        period = ctx.find("period")
        if not period:
            continue

        start = period.find("startDate")
        end = period.find("endDate")
        instant = period.find("instant")

        # dimension（連結/単体）判定：ConsolidatedOrNonConsolidatedAxis を見る
        # ※明示が無い場合は連結扱い（EDINETでは連結が基本）
        dim_member_texts = [m.get_text(strip=True) for m in ctx.find_all("xbrldi:explicitMember")]
        is_noncon = any("NonConsolidatedMember" in t for t in dim_member_texts)
        dim = "NonConsolidated" if is_noncon else "Consolidated"

        contexts[ctx_id] = {
            "start": start.get_text(strip=True) if start else None,
            "end": end.get_text(strip=True) if end else None,
            "instant": instant.get_text(strip=True) if instant else None,
            "dim": dim,
        }

    def months_between(start_ymd: str, end_ymd: str) -> int:
        s = datetime.strptime(start_ymd, "%Y-%m-%d")
        e = datetime.strptime(end_ymd, "%Y-%m-%d")
        return (e.year - s.year) * 12 + (e.month - s.month)

    # -------------------------
    # 2) 期間（通期=12 / 上期=6）ごとの「対象end日」を決める
    #    連結優先で endDate が一番新しいものを current とする
    # -------------------------
    def pick_end_dates(target_months: int):
        # candidates: (end_date, dim)
        cands = []
        for info in contexts.values():
            if info["start"] and info["end"]:
                m = months_between(info["start"], info["end"])
                if m == target_months:
                    cands.append((info["end"], info["dim"]))

        # end_date で降順、同日なら 連結優先
        # dim の優先度: Consolidated(0) < NonConsolidated(1)
        def dim_rank(d):
            return 0 if d == "Consolidated" else 1

        cands.sort(key=lambda x: (x[0], -1 if x[1] == "Consolidated" else -2), reverse=True)

        # current_end: 最上位（連結優先）
        # prior_end: その次（年次比較用）
        # ※同じ end が複数dimで出るので、end日だけ抽出してユニーク化
        end_dates = []
        for end_date, _dim in cands:
            if end_date not in end_dates:
                end_dates.append(end_date)

        current_end = end_dates[0] if len(end_dates) >= 1 else None
        prior_end = end_dates[1] if len(end_dates) >= 2 else None
        return current_end, prior_end

    fy_end_cur, fy_end_pri = pick_end_dates(12)  # 通期
    h1_end_cur, h1_end_pri = pick_end_dates(6)   # 上期累計

    # -------------------------
    # 3) タグ候補（J-GAAP/IFRS）定義
    #    ※あなたの画像の表をベースに。増やしたいタグはここに足すだけ
    # -------------------------
    METRICS = {
        # ---- 表紙/DEI ----
        "CompanyNameCoverPage": {
            "tags": ["jpcrp_cor:CompanyNameCoverPage"],
            "kind": "instant_text",  # 文字列
            "unit": None,
        },
        "CurrentFiscalYearEndDateDEI": {
            "tags": ["jpdei_cor:CurrentFiscalYearEndDateDEI"],
            "kind": "instant_date",  # 日付
            "unit": "date",
        },
        "SecurityCodeDEI": {
            "tags": ["jpdei_cor:SecurityCodeDEI"],
            "kind": "instant_text",
            "unit": None,
        },

        # ---- PL（通期/上期）----
        "NetSales": {
            "tags": [
                "jppfs_cor:NetSales",
                "jpigp_cor:RevenueIFRS",
                "jpigp_cor:NetSalesIFRS",
                "jpcrp030000-asr_E02144-000:TotalNetRevenuesIFRS",
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
            "tags": ["jppfs_cor:OrdinaryIncome", "jpigp_cor:ProfitLossBeforeTaxIFRS"],
            "kind": "duration",
            "unit": "millions",
        },
        "ProfitLoss": {
            "tags": ["jppfs_cor:ProfitLoss", "jpigp_cor:ProfitLossIFRS"],
            "kind": "duration",
            "unit": "millions",
        },

        # ---- BS（期末instant：通期末/上期末）----
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

        # ---- CF（通期/上期）----
        "OperatingCash": {
            "tags": [
                "jppfs_cor:NetCashProvidedByUsedInOperatingActivities",
                "jpigp_cor:NetCashProvidedByUsedInOperatingActivitiesIFRS",
            ],
            "kind": "duration",
            "unit": "millions",
        },
        "InvestmentCash": {
            "tags": [
                "jppfs_cor:NetCashProvidedByUsedInInvestmentActivities",
                "jpigp_cor:NetCashProvidedByUsedInInvestingActivitiesIFRS",
            ],
            "kind": "duration",
            "unit": "millions",
        },
        "FinancingCash": {
            "tags": [
                "jppfs_cor:NetCashProvidedByUsedInFinancingActivities",
                "jpigp_cor:NetCashProvidedByUsedInFinancingActivitiesIFRS",
            ],
            "kind": "duration",
            "unit": "millions",
        },
        "CashAndCashEquivalents": {
            "tags": [
                "jppfs_cor:CashAndCashEquivalents",
                "jpigp_cor:CashAndCashEquivalentsIFRS",
            ],
            "kind": "instant_num",
            "unit": "millions",
        },

        # ---- そのほか（あなたの表の項目に合わせて追加してOK）----
        "ShortTermBorrowings": {
            "tags": ["jppfs_cor:NetIncreaseDecreaseInShortTermLoansPayableFinCF"],
            "kind": "duration",
            "unit": "millions",
        },
        "LongTermBorrowings": {
            "tags": ["jppfs_cor:ProceedsFromLongTermLoansPayableFinCF"],
            "kind": "duration",
            "unit": "millions",
        },
        "Bonds": {
            "tags": ["jppfs_cor:ProceedsFromIssuanceOfBondsFinCF"],
            "kind": "duration",
            "unit": "millions",
        },
        "TreasuryStock": {
            "tags": ["jppfs_cor:PurchaseOfTreasuryStockFinCF"],
            "kind": "duration",
            "unit": "millions",
        },
        "Dividends": {
            "tags": ["jppfs_cor:CashDividendsPaidFinCF"],
            "kind": "duration",
            "unit": "millions",
        },
        "SalariesAndWages": {
            "tags": ["jppfs_cor:SalariesAndWagesSGA"],
            "kind": "duration",
            "unit": "millions",
        },
        "Bonuses": {
            "tags": ["jppfs_cor:BonusesAndAllowanceSGA"],
            "kind": "duration",
            "unit": "millions",
        },
        "ProvisionForBonuses": {
            "tags": ["jppfs_cor:ProvisionForBonusesSGA"],
            "kind": "duration",
            "unit": "millions",
        },
        "RetirementBenefitExpenses": {
            "tags": ["jppfs_cor:RetirementBenefitExpensesSGA"],
            "kind": "duration",
            "unit": "millions",
        },
        "DepreciationAndAmortization": {
            "tags": ["jppfs_cor:DepreciationAndAmortizationOpeCF"],
            "kind": "duration",
            "unit": "millions",
        },
    }

    # -------------------------
    # 4) fact 探索（連結優先＋endDate一致＋タグ優先）
    # -------------------------
    def pick_fact(tags, *, kind, end_date, prefer_dim="Consolidated"):
        if end_date is None:
            return None

        # スコア：タグ順 + 連結優先
        def dim_score(d):
            # 連結を最優先、無ければ単体
            return 1 if d == prefer_dim else 0

        best = None
        best_score = None

        for tag_priority, tag in enumerate(tags):
            for el in soup.find_all(tag):
                ctxref = el.get("contextRef")
                if not ctxref or ctxref not in contexts:
                    continue
                info = contexts[ctxref]

                # kind に応じて end_date を合わせる
                if kind in ("duration",):
                    if not (info["start"] and info["end"]):
                        continue
                    if info["end"] != end_date:
                        continue
                elif kind in ("instant_num", "instant_text", "instant_date"):
                    if not info["instant"]:
                        continue
                    if info["instant"] != end_date:
                        continue
                else:
                    continue

                score = (dim_score(info["dim"]), -tag_priority)
                if (best_score is None) or (score > best_score):
                    best_score = score
                    best = el

        return best

    # -------------------------
    # 5) 返すキーを「あなたのcell_mapに合わせる」
    #    通期: xxxPrior / xxxCurrent
    #    上期: xxxYTD（=上期累計 current）/（必要なら Prior も追加可）
    # -------------------------
    out = {}

    def put_metric(metric_key, meta, *, end_date, out_key):
        el = pick_fact(meta["tags"], kind=meta["kind"], end_date=end_date, prefer_dim="Consolidated")
        if not el:
            return
        txt = el.get_text(strip=True)

        if meta["kind"] == "instant_text":
            out[out_key] = txt
            return

        if meta["kind"] == "instant_date":
            # YYYY-MM-DD をそのまま返す（必要なら年/月分解は後段で）
            out[out_key] = txt
            return

        # 数値
        unit = meta["unit"]
        out[out_key] = trim_value(txt, unit) if unit else txt

    # --- 通期（Current / Prior） ---
    for k, meta in METRICS.items():
        if meta["kind"] in ("duration", "instant_num"):
            put_metric(k, meta, end_date=fy_end_pri, out_key=f"{k}Prior")
            put_metric(k, meta, end_date=fy_end_cur, out_key=f"{k}Current")

    # --- 上期累計（Current） ---
    # ※あなたの既存cell_mapは YTD が「上期累計」用途なので、それに合わせる
    for k, meta in METRICS.items():
        if meta["kind"] == "duration":
            put_metric(k, meta, end_date=h1_end_cur, out_key=f"{k}YTD")
        elif meta["kind"] == "instant_num":
            put_metric(k, meta, end_date=h1_end_cur, out_key=f"{k}Quarter")

    # --- 表紙系（instant_text/date）は FilingDateInstant が多いので別取り（揺れに強い）
    # CompanyName / SecurityCode / FY end date はまず最初に見つかったものでOK
    # （もしここも厳密にするなら、FilingDateInstant context を拾う実装にできます）
    sc_el = soup.find("jpdei_cor:SecurityCodeDEI")
    if sc_el:
        sc = sc_el.get_text(strip=True)
        if sc.isdigit():
            out["SecurityCodeDEI"] = sc[:-1]
    cn_el = soup.find("jpcrp_cor:CompanyNameCoverPage")
    if cn_el:
        out["CompanyNameCoverPage"] = cn_el.get_text(strip=True)
    fy_el = soup.find("jpdei_cor:CurrentFiscalYearEndDateDEI")
    if fy_el:
        out["CurrentFiscalYearEndDateDEI"] = fy_el.get_text(strip=True)

    security_code = out.get("SecurityCodeDEI")
    return out, security_code

# Excelへのデータ書き込み
def write_data_to_excel(excel_file, data, cell_map):
    workbook = openpyxl.load_workbook(excel_file)
    sheet = workbook['決算入力']
    
    for key, cell in cell_map.items():
        if key in data:
            if isinstance(cell, list):
                for c in cell:
                    sheet[c] = data[key]
            else:
                sheet[cell] = data[key]
    
    workbook.save(excel_file)       

# 株価データの取得　調整前株価：auto_adjust=False　調整後株価：auto_adjust=True　←株価同じ。今後変わるかも(2025.02.22現在)
def get_stock_price(stock_code, target_date, backup_date, buffer_days=3):
    start_date = (datetime.strptime(backup_date, '%Y-%m-%d') - timedelta(days=buffer_days)).strftime('%Y-%m-%d')
    end_date = (datetime.strptime(target_date, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')
    hist = yf.download(tickers=stock_code, start=start_date, end=end_date, auto_adjust=False)
    hist.index = hist.index.tz_localize(None)
    
    check_date = datetime.strptime(target_date, '%Y-%m-%d')
    while check_date >= datetime.strptime(start_date, '%Y-%m-%d'):
        if check_date.strftime('%Y-%m-%d') in hist.index:
            return hist.loc[check_date.strftime('%Y-%m-%d'), 'Close']
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

# Excelファイルのリネーム
def rename_excel_file(original_path, security_code, company_name, period_end_date):
    base_name = f"{security_code}_{company_name}_{period_end_date}"
    new_file_path = os.path.join(os.path.dirname(original_path), f"{base_name}.xlsx")
    
    counter = 1
    while os.path.exists(new_file_path):
        new_file_path = os.path.join(os.path.dirname(original_path), f"{base_name}_{counter}.xlsx")
        counter += 1
    
    os.rename(original_path, new_file_path)
    print(f"Excelファイルがリネームされました: {new_file_path}")
    return new_file_path

# 条件に応じたマッピングの選択
tags_contexts = {
    #売上高
    'NetSalesPrior': ('jppfs_cor:NetSales', "Prior1YearDuration_NonConsolidatedMember", True, 'millions'),
    'NetSalesCurrent': ('jppfs_cor:NetSales', "CurrentYearDuration_NonConsolidatedMember", True, 'millions'),
    'NetSalesYTD': ('jppfs_cor:NetSales', "InterimDuration_NonConsolidatedMember", True, 'millions'),
    #売上原価
    'CostOfSalesPrior': ('jppfs_cor:CostOfSales', "Prior1YearDuration_NonConsolidatedMember", True, 'millions'),
    'CostOfSalesCurrent': ('jppfs_cor:CostOfSales', "CurrentYearDuration_NonConsolidatedMember", True, 'millions'),
    #売上総利益
    'GrossProfitPrior': ('jppfs_cor:GrossProfit', "Prior1YearDuration_NonConsolidatedMember", True, 'millions'),
    'GrossProfitCurrent': ('jppfs_cor:GrossProfit', "CurrentYearDuration_NonConsolidatedMember", True, 'millions'),
    #販管費
    'SellingExpensesPrior': ('jppfs_cor:SellingGeneralAndAdministrativeExpenses', "Prior1YearDuration_NonConsolidatedMember", True, 'millions'),
    'SellingExpensesCurrent': ('jppfs_cor:SellingGeneralAndAdministrativeExpenses', "CurrentYearDuration_NonConsolidatedMember", True, 'millions'),
    #営業利益
    'OperatingIncomePrior': ('jppfs_cor:OperatingIncome', "Prior1YearDuration_NonConsolidatedMember", True, 'millions'),
    'OperatingIncomeCurrent': ('jppfs_cor:OperatingIncome', "CurrentYearDuration_NonConsolidatedMember", True, 'millions'),
    #経常利益
    'OrdinaryIncomePrior': ('jppfs_cor:OrdinaryIncome', "Prior1YearDuration_NonConsolidatedMember", True, 'millions'),
    'OrdinaryIncomeCurrent': ('jppfs_cor:OrdinaryIncome', "CurrentYearDuration_NonConsolidatedMember", True, 'millions'),
    'OrdinaryIncomeYTD': ('jppfs_cor:OrdinaryIncome', "InterimDuration_NonConsolidatedMember", True, 'millions'),
    #純利益
    'ProfitLossPrior': ('jppfs_cor:ProfitLoss', "Prior1YearDuration_NonConsolidatedMember", True, 'millions'),
    'ProfitLossCurrent': ('jppfs_cor:ProfitLoss', "CurrentYearDuration_NonConsolidatedMember", True, 'millions'),
    'ProfitLossYTD': ('jppfs_cor:ProfitLoss', "InterimDuration_NonConsolidatedMember", True, 'millions'),
    #発行株数
    'TotalNumberPrior3': ('jpcrp_cor:TotalNumberOfIssuedSharesSummaryOfBusinessResults', "Prior3YearInstant_NonConsolidatedMember", True, 'thousands'),
    'TotalNumberPrior2': ('jpcrp_cor:TotalNumberOfIssuedSharesSummaryOfBusinessResults', "Prior2YearInstant_NonConsolidatedMember", True, 'thousands'),
    'TotalNumberPrior1': ('jpcrp_cor:TotalNumberOfIssuedSharesSummaryOfBusinessResults', "Prior1YearInstant_NonConsolidatedMember", True, 'thousands'),
    'TotalNumberCurrent': ('jpcrp_cor:TotalNumberOfIssuedSharesSummaryOfBusinessResults', "CurrentYearInstant_NonConsolidatedMember", True, 'thousands'),
    'TotalNumberQuarter': ('jpcrp_cor:TotalNumberOfIssuedSharesSummaryOfBusinessResults', "CurrentQuarterInstant_NonConsolidatedMember", True, 'thousands'),
    #資産
    'TotalAssetsPrior3': ('jpcrp_cor:TotalAssetsSummaryOfBusinessResults', "Prior3YearInstant_NonConsolidatedMember", True, 'millions'),
    'TotalAssetsPrior2': ('jpcrp_cor:TotalAssetsSummaryOfBusinessResults', "Prior2YearInstant_NonConsolidatedMember", True, 'millions'),
    'TotalAssetsPrior1': ('jpcrp_cor:TotalAssetsSummaryOfBusinessResults', "Prior1YearInstant_NonConsolidatedMember", True, 'millions'),
    'TotalAssetsCurrent': ('jpcrp_cor:TotalAssetsSummaryOfBusinessResults', "CurrentYearInstant_NonConsolidatedMember", True, 'millions'),
    'TotalAssetsQuarter': ('jpcrp_cor:TotalAssetsSummaryOfBusinessResults', "CurrentQuarterInstant_NonConsolidatedMember", True, 'millions'),
    #資本
    'NetAssetsPrior3': ('jpcrp_cor:NetAssetsSummaryOfBusinessResults', "Prior3YearInstant_NonConsolidatedMember", True, 'millions'),
    'NetAssetsPrior2': ('jpcrp_cor:NetAssetsSummaryOfBusinessResults', "Prior2YearInstant_NonConsolidatedMember", True, 'millions'),
    'NetAssetsPrior1': ('jpcrp_cor:NetAssetsSummaryOfBusinessResults', "Prior1YearInstant_NonConsolidatedMember", True, 'millions'),
    'NetAssetsCurrent': ('jpcrp_cor:NetAssetsSummaryOfBusinessResults', "CurrentYearInstant_NonConsolidatedMember", True, 'millions'),
    'NetAssetsQuarter': ('jpcrp_cor:NetAssetsSummaryOfBusinessResults', "InterimInstant_NonConsolidatedMember", True, 'millions'),
    #営業CF
    'OperatingCashPrior': ('jppfs_cor:NetCashProvidedByUsedInOperatingActivities', "Prior1YearDuration_NonConsolidatedMember", True, 'millions'),
    'OperatingCashCurrent': ('jppfs_cor:NetCashProvidedByUsedInOperatingActivities', "CurrentYearDuration_NonConsolidatedMember", True, 'millions'),
    'OperatingCashYTD': ('jppfs_cor:NetCashProvidedByUsedInOperatingActivities', "InterimDuration_NonConsolidatedMember", True, 'millions'),
    #投資CF
    'InvestmentCashPrior': ('jppfs_cor:NetCashProvidedByUsedInInvestmentActivities', "Prior1YearDuration_NonConsolidatedMember", True, 'millions'),
    'InvestmentCashCurrent': ('jppfs_cor:NetCashProvidedByUsedInInvestmentActivities', "CurrentYearDuration_NonConsolidatedMember", True, 'millions'),
    'InvestmentCashYTD': ('jppfs_cor:NetCashProvidedByUsedInInvestmentActivities', "InterimDuration_NonConsolidatedMember", True, 'millions'),
    #財務CF
    'FinancingCashPrior': ('jppfs_cor:NetCashProvidedByUsedInFinancingActivities', "Prior1YearDuration_NonConsolidatedMember", True, 'millions'),
    'FinancingCashCurrent': ('jppfs_cor:NetCashProvidedByUsedInFinancingActivities', "CurrentYearDuration_NonConsolidatedMember", True, 'millions'),
    'FinancingCashYTD': ('jppfs_cor:NetCashProvidedByUsedInFinancingActivities', "InterimDuration_NonConsolidatedMember", True, 'millions'),
    #期末残
    'CashAndCashEquivalentsPrior': ('jppfs_cor:CashAndCashEquivalents', "Prior1YearInstant_NonConsolidatedMember", True, 'millions'),
    'CashAndCashEquivalentsCurrent': ('jppfs_cor:CashAndCashEquivalents', "CurrentYearInstant_NonConsolidatedMember", True, 'millions'),
    'CashAndCashEquivalentsQuarter': ('jppfs_cor:CashAndCashEquivalents', "InterimInstant_NonConsolidatedMember", True, 'millions'),
    #短期借
    'ShortTermBorrowingsPrior': ('jppfs_cor:NetIncreaseDecreaseInShortTermLoansPayableFinCF', "Prior1YearDuration_NonConsolidatedMember", True, 'millions'),
    'ShortTermBorrowingsCurrent': ('jppfs_cor:NetIncreaseDecreaseInShortTermLoansPayableFinCF', "CurrentYearDuration_NonConsolidatedMember", True, 'millions'),
    'ShortTermBorrowingsQuarter': ('jppfs_cor:NetIncreaseDecreaseInShortTermLoansPayableFinCF', "InterimDuration_NonConsolidatedMember", True, 'millions'),
    #長期借
    'LongTermBorrowingsPrior': ('jppfs_cor:ProceedsFromLongTermLoansPayableFinCF', "Prior1YearDuration_NonConsolidatedMember", True, 'millions'),
    'LongTermBorrowingsCurrent': ('jppfs_cor:ProceedsFromLongTermLoansPayableFinCF', "CurrentYearDuration_NonConsolidatedMember", True, 'millions'),
    'LongTermBorrowingsQuarter': ('jppfs_cor:ProceedsFromLongTermLoansPayableFinCF', "InterimDuration_NonConsolidatedMember", True, 'millions'),
    #社債
    'BondsPrior': ('jppfs_cor:ProceedsFromIssuanceOfBondsFinCF', "Prior1YearDuration_NonConsolidatedMember", True, 'millions'),
    'BondsCurrent': ('jppfs_cor:ProceedsFromIssuanceOfBondsFinCF', "CurrentYearDuration_NonConsolidatedMember", True, 'millions'),
    'BondsQuarter': ('jppfs_cor:ProceedsFromIssuanceOfBondsFinCF', "InterimDuration_NonConsolidatedMember", True, 'millions'),
    #自己株式取得
    'TreasuryStockPrior': ('jppfs_cor:PurchaseOfTreasuryStockFinCF', "Prior1YearDuration_NonConsolidatedMember", True, 'millions'),
    'TreasuryStockCurrent': ('jppfs_cor:PurchaseOfTreasuryStockFinCF', "CurrentYearDuration_NonConsolidatedMember", True, 'millions'),
    'TreasuryStockQuarter': ('jppfs_cor:PurchaseOfTreasuryStockFinCF', "InterimDuration_NonConsolidatedMember", True, 'millions'),
    #配当金
    'DividendsPrior': ('jppfs_cor:CashDividendsPaidFinCF', "Prior1YearDuration_NonConsolidatedMember", True, 'millions'),
    'DividendsCurrent': ('jppfs_cor:CashDividendsPaidFinCF', "CurrentYearDuration_NonConsolidatedMember", True, 'millions'),
    'DividendsQuarter': ('jppfs_cor:CashDividendsPaidFinCF', "InterimDuration_NonConsolidatedMember", True, 'millions'),
    #給料及び賃金
    'SalariesAndWagesPrior': ('jppfs_cor:SalariesAndWagesSGA', "Prior1YearDuration_NonConsolidatedMember", True, 'millions'),
    'SalariesAndWagesCurrent': ('jppfs_cor:SalariesAndWagesSGA', "CurrentYearDuration_NonConsolidatedMember", True, 'millions'),
    #賞与
    'BonusesPrior': ('jppfs_cor:BonusesAndAllowanceSGA', "Prior1YearDuration_NonConsolidatedMember", True, 'millions'),
    'BonusesCurrent': ('jppfs_cor:BonusesAndAllowanceSGA', "CurrentYearDuration_NonConsolidatedMember", True, 'millions'),
    #賞与引当金
    'ProvisionForBonusesPrior': ('jppfs_cor:ProvisionForBonusesSGA', "Prior1YearDuration_NonConsolidatedMember", True, 'millions'),
    'ProvisionForBonusesCurrent': ('jppfs_cor:ProvisionForBonusesSGA', "CurrentYearDuration_NonConsolidatedMember", True, 'millions'),
    #退職給付費用
    'RetirementBenefitExpensesPrior': ('jppfs_cor:RetirementBenefitExpensesSGA', "Prior1YearDuration_NonConsolidatedMember", True, 'millions'),
    'RetirementBenefitExpensesCurrent': ('jppfs_cor:RetirementBenefitExpensesSGA', "CurrentYearDuration_NonConsolidatedMember", True, 'millions'),
    #CF減価償却費
    'DepreciationAndAmortizationPrior': ('jppfs_cor:DepreciationAndAmortizationOpeCF', "Prior1YearDuration_NonConsolidatedMember", True, 'millions'),
    'DepreciationAndAmortizationCurrent': ('jppfs_cor:DepreciationAndAmortizationOpeCF', "CurrentYearDuration_NonConsolidatedMember", True, 'millions'),
    #証券コード
    'SecurityCodeDEI': ('jpdei_cor:SecurityCodeDEI', "FilingDateInstant", True, 'ten'),
    #会社名
    'CompanyNameCoverPage': ('jpcrp_cor:CompanyNameCoverPage', "FilingDateInstant", False, 'millions'),
    #当会計期間終了日
    'CurrentPeriodEndDateDEIdate': ('jpdei_cor:CurrentPeriodEndDateDEI', "FilingDateInstant", False, 'date'),
    #当事業年度終了日
    'CurrentFiscalYearEndDateDEIyear': ('jpdei_cor:CurrentFiscalYearEndDateDEI', "FilingDateInstant", False, 'year'),
    'CurrentFiscalYearEndDateDEImonth': ('jpdei_cor:CurrentFiscalYearEndDateDEI', "FilingDateInstant", False, 'month')
            },
cell_map_file1 = {
    #売上高
    'NetSalesPrior': 'D5', 'NetSalesCurrent': 'G5',
    #売上原価
    'CostOfSalesPrior': 'D6', 'CostOfSalesCurrent': 'G6',
    #売上総利益
    'GrossProfitPrior': 'D7', 'GrossProfitCurrent': 'G7',
    #販管費
    'SellingExpensesPrior': 'D8', 'SellingExpensesCurrent': 'G8',
    #営業利益
    'OperatingIncomePrior': 'D9', 'OperatingIncomeCurrent': 'G9',
    #経常利益
    'OrdinaryIncomePrior': 'D10', 'OrdinaryIncomeCurrent': 'G10',
    #純利益
    'ProfitLossPrior': 'D11', 'ProfitLossCurrent': 'G11',
    #営業CF
    'OperatingCashPrior': 'C21', 'OperatingCashCurrent': 'F21',
    #投資CF
    'InvestmentCashPrior': 'D21', 'InvestmentCashCurrent': 'G21',
    #財務CF
    'FinancingCashPrior': 'E21', 'FinancingCashCurrent': 'H21',
    #期末残
    'CashAndCashEquivalentsPrior': 'D22', 'CashAndCashEquivalentsCurrent': 'G22',
    #短期借
    'ShortTermBorrowingsPrior': 'C57', 'ShortTermBorrowingsCurrent': 'F57',
    #長期借
    'LongTermBorrowingsPrior': 'D57', 'LongTermBorrowingsCurrent': 'G57',
    #社債
    'BondsPrior': 'E57', 'BondsCurrent': 'H57',
    #自己株式取得
    'TreasuryStockPrior': 'D60', 'TreasuryStockCurrent': 'G60',
    #配当金
    'DividendsPrior': 'D61', 'DividendsCurrent': 'G61',
    #給料及び賃金
    'SalariesAndWagesPrior': 'D63', 'SalariesAndWagesCurrent': 'G63',
    #賞与
    'BonusesPrior': 'D64', 'BonusesCurrent': 'G64',
    #賞与引当金
    'ProvisionForBonusesPrior': 'D65', 'ProvisionForBonusesCurrent': 'G65',
    #退職給付費用
    'RetirementBenefitExpensesPrior': 'D66', 'RetirementBenefitExpensesCurrent': 'G66',
    #CF減価償却費
    'DepreciationAndAmortizationPrior': 'D67', 'DepreciationAndAmortizationCurrent': 'G67'

            },
cell_map_file2 = {
    #売上高
    'NetSalesPrior': 'J5', 'NetSalesCurrent': 'M5',
    #売上原価
    'CostOfSalesPrior': 'J6', 'CostOfSalesCurrent': 'M6',
    #売上総利益
    'GrossProfitPrior': 'J7', 'GrossProfitCurrent': 'M7',
    #販管費
    'SellingExpensesPrior': 'J8', 'SellingExpensesCurrent': 'M8',
    #営業利益
    'OperatingIncomePrior': 'J9', 'OperatingIncomeCurrent': 'M9',
    #経常利益
    'OrdinaryIncomePrior': 'J10', 'OrdinaryIncomeCurrent': 'M10',
    #純利益
    'ProfitLossPrior': 'J11', 'ProfitLossCurrent': 'M11',
    #発行株数
    'TotalNumberPrior3': 'D13', 'TotalNumberPrior2': 'G13', 
    'TotalNumberPrior1': 'J13', 'TotalNumberCurrent': 'M13',
    #資産
    'TotalAssetsPrior3': 'C17', 'TotalAssetsPrior2': 'F17',
    'TotalAssetsPrior1': 'I17', 'TotalAssetsCurrent': 'L17', 
    #資本
    'NetAssetsPrior3': 'D17', 'NetAssetsPrior2': 'G17',
    'NetAssetsPrior1': 'J17', 'NetAssetsCurrent': 'M17',
    #営業CF
    'OperatingCashPrior': 'I21', 'OperatingCashCurrent': 'L21',
    #投資CF
    'InvestmentCashPrior': 'J21', 'InvestmentCashCurrent': 'M21',
    #財務CF
    'FinancingCashPrior': 'K21', 'FinancingCashCurrent': 'N21',
    #期末残
    'CashAndCashEquivalentsPrior': 'J22', 'CashAndCashEquivalentsCurrent': 'M22',
    #短期借
    'ShortTermBorrowingsPrior': 'I57', 'ShortTermBorrowingsCurrent': 'L57',
    #長期借
    'LongTermBorrowingsPrior': 'J57', 'LongTermBorrowingsCurrent': 'M57',
    #社債
    'BondsPrior': 'K57', 'BondsCurrent': 'N57',
    #自己株式取得
    'TreasuryStockPrior': 'J60', 'TreasuryStockCurrent': 'M60',
    #配当金
    'DividendsPrior': 'J61', 'DividendsCurrent': 'M61',
    #給料及び賃金
    'SalariesAndWagesPrior': 'J63', 'SalariesAndWagesCurrent': 'M63',
    #賞与
    'BonusesPrior': 'J64', 'BonusesCurrent': 'M64',
    #賞与引当金
    'ProvisionForBonusesPrior': 'J65', 'ProvisionForBonusesCurrent': 'M65',
    #退職給付費用
    'RetirementBenefitExpensesPrior': 'J66', 'RetirementBenefitExpensesCurrent': 'M66',
    #CF減価償却費
    'DepreciationAndAmortizationPrior': 'J67', 'DepreciationAndAmortizationCurrent': 'M67'
    },

#半期
cell_map_file3 = {
    #売上高
    'NetSalesYTD': 'J36',
    #経常利益
    'OrdinaryIncomeYTD': 'J37',
    #純利益
    'ProfitLossYTD': 'J38',
    #発行株数
    'TotalNumberQuarter': 'J40',
    #資産
    'TotalAssetsQuarter': 'I44',
    #資本
    'NetAssetsQuarter': 'J44',
    #営業CF
    'OperatingCashYTD': 'I48',
    #投資CF
    'InvestmentCashYTD': 'J48',
    #財務CF
    'FinancingCashYTD': 'K48',
    #期末残
    'CashAndCashEquivalentsQuarter': 'J49', 
    #短期借
    'ShortTermBorrowingsQuarter': 'O57',
    #長期借
    'LongTermBorrowingsQuarter': 'P57',
    #社債
    'BondsQuarter': 'Q57',
    #自己株式取得
    'TreasuryStockQuarter': 'P60',
    #配当金
    'DividendsQuarter': 'P61',
    #給料及び賃金
    'SalariesAndWagesPrior': 'P63',
    #賞与
    'BonusesPrior': 'P64',
    #賞与引当金
    'ProvisionForBonusesPrior': 'P65',
    #退職給付費用
    'RetirementBenefitExpensesPrior': 'P66',
    #CF減価償却費
    'DepreciationAndAmortizationPrior': 'P67',   
    #証券コード
    'SecurityCodeDEI': 'K2',
    #会社名
    'CompanyNameCoverPage': 'L2',
    #当会計期間終了日
    'CurrentFiscalYearEndDateDEIyear': 'N2',
    'CurrentFiscalYearEndDateDEImonth': 'O2'
            }
tags_contexts={
#連結決算の場合
    #売上高
    'NetSalesPrior': ('jppfs_cor:NetSales', "Prior1YearDuration", True, 'millions'),
    'NetSalesCurrent': ('jppfs_cor:NetSales', "CurrentYearDuration", True, 'millions'),
    'NetSalesYTD': ('jppfs_cor:NetSales', "InterimDuration", True, 'millions'),
    #売上原価
    'CostOfSalesPrior': ('jppfs_cor:CostOfSales', "Prior1YearDuration", True, 'millions'),
    'CostOfSalesCurrent': ('jppfs_cor:CostOfSales', "CurrentYearDuration", True, 'millions'),
    #売上総利益
    'GrossProfitPrior': ('jppfs_cor:GrossProfit', "Prior1YearDuration", True, 'millions'),
    'GrossProfitCurrent': ('jppfs_cor:GrossProfit', "CurrentYearDuration", True, 'millions'),
    #販管費
    'SellingExpensesPrior': ('jppfs_cor:SellingGeneralAndAdministrativeExpenses', "Prior1YearDuration", True, 'millions'),
    'SellingExpensesCurrent': ('jppfs_cor:SellingGeneralAndAdministrativeExpenses', "CurrentYearDuration", True, 'millions'),
    #営業利益
    'OperatingIncomePrior': ('jppfs_cor:OperatingIncome', "Prior1YearDuration", True, 'millions'),
    'OperatingIncomeCurrent': ('jppfs_cor:OperatingIncome', "CurrentYearDuration", True, 'millions'),
    #経常利益
    'OrdinaryIncomePrior': ('jppfs_cor:OrdinaryIncome', "Prior1YearDuration", True, 'millions'),
    'OrdinaryIncomeCurrent': ('jppfs_cor:OrdinaryIncome', "CurrentYearDuration", True, 'millions'),
    'OrdinaryIncomeYTD': ('jppfs_cor:OrdinaryIncome', "InterimDuration", True, 'millions'),
    #純利益
    'ProfitLossPrior': ('jppfs_cor:ProfitLoss', "Prior1YearDuration", True, 'millions'),
    'ProfitLossCurrent': ('jppfs_cor:ProfitLoss', "CurrentYearDuration", True, 'millions'),
    'ProfitLossYTD': ('jppfs_cor:ProfitLoss', "InterimDuration", True, 'millions'),
    #発行株数
    'TotalNumberPrior3': ('jpcrp_cor:TotalNumberOfIssuedSharesSummaryOfBusinessResults', "Prior3YearInstant_NonConsolidatedMember", True, 'thousands'),
    'TotalNumberPrior2': ('jpcrp_cor:TotalNumberOfIssuedSharesSummaryOfBusinessResults', "Prior2YearInstant_NonConsolidatedMember", True, 'thousands'),
    'TotalNumberPrior1': ('jpcrp_cor:TotalNumberOfIssuedSharesSummaryOfBusinessResults', "Prior1YearInstant_NonConsolidatedMember", True, 'thousands'),
    'TotalNumberCurrent': ('jpcrp_cor:TotalNumberOfIssuedSharesSummaryOfBusinessResults', "CurrentYearInstant_NonConsolidatedMember", True, 'thousands'),
    'TotalNumberFiling': ('jpcrp_cor:NumberOfIssuedSharesAsOfFiscalYearEndIssuedSharesTotalNumberOfSharesEtc', "FilingDateInstant_OrdinaryShareMember", True, 'thousands'),
    #資産
    'TotalAssetsPrior3': ('jpcrp_cor:TotalAssetsSummaryOfBusinessResults', "Prior3YearInstant", True, 'millions'),
    'TotalAssetsPrior2': ('jpcrp_cor:TotalAssetsSummaryOfBusinessResults', "Prior2YearInstant", True, 'millions'),
    'TotalAssetsPrior1': ('jpcrp_cor:TotalAssetsSummaryOfBusinessResults', "Prior1YearInstant", True, 'millions'),
    'TotalAssetsCurrent': ('jpcrp_cor:TotalAssetsSummaryOfBusinessResults', "CurrentYearInstant", True, 'millions'),
    'TotalAssetsQuarter': ('jpcrp_cor:TotalAssetsSummaryOfBusinessResults', "InterimInstant", True, 'millions'),
    #資本
    'NetAssetsPrior3': ('jpcrp_cor:NetAssetsSummaryOfBusinessResults', "Prior3YearInstant", True, 'millions'),
    'NetAssetsPrior2': ('jpcrp_cor:NetAssetsSummaryOfBusinessResults', "Prior2YearInstant", True, 'millions'),
    'NetAssetsPrior1': ('jpcrp_cor:NetAssetsSummaryOfBusinessResults', "Prior1YearInstant", True, 'millions'),
    'NetAssetsCurrent': ('jpcrp_cor:NetAssetsSummaryOfBusinessResults', "CurrentYearInstant", True, 'millions'),
    'NetAssetsQuarter': ('jpcrp_cor:NetAssetsSummaryOfBusinessResults', "InterimInstant", True, 'millions'),
    #営業CF
    'OperatingCashPrior': ('jppfs_cor:NetCashProvidedByUsedInOperatingActivities', "Prior1YearDuration", True, 'millions'),
    'OperatingCashCurrent': ('jppfs_cor:NetCashProvidedByUsedInOperatingActivities', "CurrentYearDuration", True, 'millions'),
    'OperatingCashYTD': ('jppfs_cor:NetCashProvidedByUsedInOperatingActivities', "InterimDuration", True, 'millions'),
    #投資CF
    'InvestmentCashPrior': ('jppfs_cor:NetCashProvidedByUsedInInvestmentActivities', "Prior1YearDuration", True, 'millions'),
    'InvestmentCashCurrent': ('jppfs_cor:NetCashProvidedByUsedInInvestmentActivities', "CurrentYearDuration", True, 'millions'),
    'InvestmentCashYTD': ('jppfs_cor:NetCashProvidedByUsedInInvestmentActivities', "InterimDuration", True, 'millions'),
    #財務CF
    'FinancingCashPrior': ('jppfs_cor:NetCashProvidedByUsedInFinancingActivities', "Prior1YearDuration", True, 'millions'),
    'FinancingCashCurrent': ('jppfs_cor:NetCashProvidedByUsedInFinancingActivities', "CurrentYearDuration", True, 'millions'),
    'FinancingCashYTD': ('jppfs_cor:NetCashProvidedByUsedInFinancingActivities', "InterimDuration", True, 'millions'),
    #期末残
    'CashAndCashEquivalentsPrior': ('jpcrp_cor:CashAndCashEquivalentsSummaryOfBusinessResults', "Prior1YearInstant", True, 'millions'),
    'CashAndCashEquivalentsCurrent': ('jpcrp_cor:CashAndCashEquivalentsSummaryOfBusinessResults', "CurrentYearInstant", True, 'millions'),
    'CashAndCashEquivalentsQuarter': ('jppfs_cor:CashAndCashEquivalents', "InterimInstant", True, 'millions'),
    #短期借
    'ShortTermBorrowingsPrior': ('jppfs_cor:NetIncreaseDecreaseInShortTermLoansPayableFinCF', "Prior1YearDuration", True, 'millions'),
    'ShortTermBorrowingsCurrent': ('jppfs_cor:NetIncreaseDecreaseInShortTermLoansPayableFinCF', "CurrentYearDuration", True, 'millions'),
    'ShortTermBorrowingsQuarter': ('jppfs_cor:NetIncreaseDecreaseInShortTermLoansPayableFinCF', "InterimDuration", True, 'millions'),
    #長期借
    'LongTermBorrowingsPrior': ('jppfs_cor:ProceedsFromLongTermLoansPayableFinCF', "Prior1YearDuration", True, 'millions'),
    'LongTermBorrowingsCurrent': ('jppfs_cor:ProceedsFromLongTermLoansPayableFinCF', "CurrentYearDuration", True, 'millions'),
    'LongTermBorrowingsQuarter': ('jppfs_cor:ProceedsFromLongTermLoansPayableFinCF', "InterimDuration", True, 'millions'),
    #社債
    'BondsPrior': ('jppfs_cor:ProceedsFromIssuanceOfBondsFinCF', "Prior1YearDuration", True, 'millions'),
    'BondsCurrent': ('jppfs_cor:ProceedsFromIssuanceOfBondsFinCF', "CurrentYearDuration", True, 'millions'),
    'BondsQuarter': ('jppfs_cor:ProceedsFromIssuanceOfBondsFinCF', "InterimDuration", True, 'millions'),
    #自己株式取得
    'TreasuryStockPrior': ('jppfs_cor:PurchaseOfTreasuryStockFinCF', "Prior1YearDuration", True, 'millions'),
    'TreasuryStockCurrent': ('jppfs_cor:PurchaseOfTreasuryStockFinCF', "CurrentYearDuration", True, 'millions'),
    'TreasuryStockQuarter': ('jppfs_cor:PurchaseOfTreasuryStockFinCF', "InterimDuration", True, 'millions'),
    #配当金
    'DividendsPrior': ('jppfs_cor:CashDividendsPaidFinCF', "Prior1YearDuration", True, 'millions'),
    'DividendsCurrent': ('jppfs_cor:CashDividendsPaidFinCF', "CurrentYearDuration", True, 'millions'),
    'DividendsQuarter': ('jppfs_cor:CashDividendsPaidFinCF', "InterimDuration", True, 'millions'),
    #給料及び賃金
    'SalariesAndWagesPrior': ('jppfs_cor:SalariesAndWagesSGA', "Prior1YearDuration", True, 'millions'),
    'SalariesAndWagesCurrent': ('jppfs_cor:SalariesAndWagesSGA', "CurrentYearDuration", True, 'millions'),
    #賞与
    'BonusesPrior': ('jppfs_cor:BonusesAndAllowanceSGA', "Prior1YearDuration", True, 'millions'),
    'BonusesCurrent': ('jppfs_cor:BonusesAndAllowanceSGA', "CurrentYearDuration", True, 'millions'),
    #賞与引当金
    'ProvisionForBonusesPrior': ('jppfs_cor:ProvisionForBonusesSGA', "Prior1YearDuration", True, 'millions'),
    'ProvisionForBonusesCurrent': ('jppfs_cor:ProvisionForBonusesSGA', "CurrentYearDuration", True, 'millions'),
    #退職給付費用
    'RetirementBenefitExpensesPrior': ('jppfs_cor:RetirementBenefitExpensesSGA', "Prior1YearDuration", True, 'millions'),
    'RetirementBenefitExpensesCurrent': ('jppfs_cor:RetirementBenefitExpensesSGA', "CurrentYearDuration", True, 'millions'),
    #CF減価償却費
    'DepreciationAndAmortizationPrior': ('jppfs_cor:DepreciationAndAmortizationOpeCF', "Prior1YearDuration", True, 'millions'),
    'DepreciationAndAmortizationCurrent': ('jppfs_cor:DepreciationAndAmortizationOpeCF', "CurrentYearDuration", True, 'millions'),
    #証券コード
    'SecurityCodeDEI': ('jpdei_cor:SecurityCodeDEI', "FilingDateInstant", True, 'ten'),
    #会社名
    'CompanyNameCoverPage': ('jpcrp_cor:CompanyNameCoverPage', "FilingDateInstant", False, 'millions'),
    #当会計期間終了日
    'CurrentPeriodEndDateDEIdate': ('jpdei_cor:CurrentPeriodEndDateDEI', "FilingDateInstant", False, 'date'),
    #当事業年度終了日
    'CurrentFiscalYearEndDateDEIyear': ('jpdei_cor:CurrentFiscalYearEndDateDEI', "FilingDateInstant", False, 'year'),
    'CurrentFiscalYearEndDateDEImonth': ('jpdei_cor:CurrentFiscalYearEndDateDEI', "FilingDateInstant", False, 'month')
            },
cell_map_file1 ={
    #売上高
    'NetSalesPrior': 'D5', 'NetSalesCurrent': 'G5',
    #売上原価
    'CostOfSalesPrior': 'D6', 'CostOfSalesCurrent': 'G6',
    #売上総利益
    'GrossProfitPrior': 'D7', 'GrossProfitCurrent': 'G7',
    #販管費
    'SellingExpensesPrior': 'D8', 'SellingExpensesCurrent': 'G8',
    #営業利益
    'OperatingIncomePrior': 'D9', 'OperatingIncomeCurrent': 'G9',
    #経常利益
    'OrdinaryIncomePrior': 'D10', 'OrdinaryIncomeCurrent': 'G10',
    #経常利益
    'ProfitLossPrior': 'D11', 'ProfitLossCurrent': 'G11',
    #営業CF
    'OperatingCashPrior': 'C21', 'OperatingCashCurrent': 'F21',
    #投資CF
    'InvestmentCashPrior': 'D21', 'InvestmentCashCurrent': 'G21',
    #財務CF
    'FinancingCashPrior': 'E21', 'FinancingCashCurrent': 'H21',
    #期末残
    'CashAndCashEquivalentsPrior': 'D22', 'CashAndCashEquivalentsCurrent': 'G22',
    #短期借
    'ShortTermBorrowingsPrior': 'C57', 'ShortTermBorrowingsCurrent': 'F57',
    #長期借
    'LongTermBorrowingsPrior': 'D57', 'LongTermBorrowingsCurrent': 'G57',
    #社債
    'BondsPrior': 'E57', 'BondsCurrent': 'H57',
    #自己株式取得
    'TreasuryStockPrior': 'D60', 'TreasuryStockCurrent': 'G60',
    #配当金
    'DividendsPrior': 'D61', 'DividendsCurrent': 'G61',
    #給料及び賃金
    'SalariesAndWagesPrior': 'D63', 'SalariesAndWagesCurrent': 'G63',
    #賞与
    'BonusesPrior': 'D64', 'BonusesCurrent': 'G64',
    #賞与引当金
    'ProvisionForBonusesPrior': 'D65', 'ProvisionForBonusesCurrent': 'G65',
    #退職給付費用
    'RetirementBenefitExpensesPrior': 'D66', 'RetirementBenefitExpensesCurrent': 'G66',
    #CF減価償却費
    'DepreciationAndAmortizationPrior': 'D67', 'DepreciationAndAmortizationCurrent': 'G67'
            },
cell_map_file2 ={
    #売上高
    'NetSalesPrior': 'J5', 'NetSalesCurrent': 'M5',
    #売上原価
    'CostOfSalesPrior': 'J6', 'CostOfSalesCurrent': 'M6',
    #売上総利益
    'GrossProfitPrior': 'J7', 'GrossProfitCurrent': 'M7',
    #販管費
    'SellingExpensesPrior': 'J8', 'SellingExpensesCurrent': 'M8',
    #営業利益
    'OperatingIncomePrior': 'J9', 'OperatingIncomeCurrent': 'M9',
    #経常利益
    'OrdinaryIncomePrior': 'J10', 'OrdinaryIncomeCurrent': 'M10',
    #純利益
    'ProfitLossPrior': 'J11', 'ProfitLossCurrent': 'M11',
    #発行株数
    'TotalNumberPrior3': 'D13', 'TotalNumberPrior2': 'G13', 
    'TotalNumberPrior1': 'J13', 'TotalNumberCurrent': 'M13',
    #資産
    'TotalAssetsPrior3': 'C17', 'TotalAssetsPrior2': 'F17',
    'TotalAssetsPrior1': 'I17', 'TotalAssetsCurrent': 'L17', 
    #資本
    'NetAssetsPrior3': 'D17', 'NetAssetsPrior2': 'G17',
    'NetAssetsPrior1': 'J17', 'NetAssetsCurrent': 'M17',
    #営業CF
    'OperatingCashPrior': 'I21', 'OperatingCashCurrent': 'L21',
    #投資CF
    'InvestmentCashPrior': 'J21', 'InvestmentCashCurrent': 'M21',
    #財務CF
    'FinancingCashPrior': 'K21', 'FinancingCashCurrent': 'N21',
    #期末残
    'CashAndCashEquivalentsPrior': 'J22', 'CashAndCashEquivalentsCurrent': 'M22',
    #期末残
    'CashAndCashEquivalentsPrior': 'J22', 'CashAndCashEquivalentsCurrent': 'M22',
    #短期借
    'ShortTermBorrowingsPrior': 'I57', 'ShortTermBorrowingsCurrent': 'L57',
    #長期借
    'LongTermBorrowingsPrior': 'J57', 'LongTermBorrowingsCurrent': 'M57',
    #社債
    'BondsPrior': 'K57', 'BondsCurrent': 'N57',
    #自己株式取得
    'TreasuryStockPrior': 'J60', 'TreasuryStockCurrent': 'M60',
    #配当金
    'DividendsPrior': 'J61', 'DividendsCurrent': 'M61',
    #給料及び賃金
    'SalariesAndWagesPrior': 'J63', 'SalariesAndWagesCurrent': 'M63',
    #賞与
    'BonusesPrior': 'J64', 'BonusesCurrent': 'M64',
    #賞与引当金
    'ProvisionForBonusesPrior': 'J65', 'ProvisionForBonusesCurrent': 'M65',
    #退職給付費用
    'RetirementBenefitExpensesPrior': 'J66', 'RetirementBenefitExpensesCurrent': 'M66',
    #CF減価償却費
    'DepreciationAndAmortizationPrior': 'J67', 'DepreciationAndAmortizationCurrent': 'M67'
            },
cell_map_file3 ={
    #売上高
    'NetSalesYTD': 'J36',
    #経常利益
    'OrdinaryIncomeYTD': 'J37',
    #純利益
    'ProfitLossYTD': 'J38',
    #発行株数
    'TotalNumberFiling': 'J40',
    #資産
    'TotalAssetsQuarter': 'I44',
    #資本
    'NetAssetsQuarter': 'J44',
    #営業CF
    'OperatingCashYTD': 'I48',
    #投資CF
    'InvestmentCashYTD': 'J48',
    #財務CF
    'FinancingCashYTD': 'K48',
    #期末残
    'CashAndCashEquivalentsQuarter': 'J49',
    #短期借
    'ShortTermBorrowingsQuarter': 'O57',
    #長期借
    'LongTermBorrowingsQuarter': 'P57',
    #社債
    'BondsQuarter': 'Q57',
    #自己株式取得
    'TreasuryStockQuarter': 'P60',
    #配当金
    'DividendsQuarter': 'P61', 
    #証券コード
    'SecurityCodeDEI': 'K2',
    #会社名
    'CompanyNameCoverPage': 'L2',
    #当事業年度終了日
    'CurrentFiscalYearEndDateDEIyear': 'N2',
    'CurrentFiscalYearEndDateDEImonth': 'O2'
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

    config = load_config('決算期.json')
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
    # Excelファイル名を解析
    excel_base_name = os.path.basename(loop['excel_file_path']).replace('.xlsx', '')
    excel_directory = os.path.dirname(loop['excel_file_path'])
    
    # 実際に使用するExcelファイルを見つける
    selected_file = find_available_excel_file(excel_directory, excel_base_name)
    
    if selected_file:
        loop['excel_file_path'] = selected_file  
        print(f"ーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーー\n使用するExcelファイル: {selected_file}")
    else:
        skipped_files.append({
            'reason': "Excelファイルが見つからない",
            'file': excel_base_name
        })
        print("使用するファイルが見つかりませんでした。次のループを実行します。")
        continue

    rename_info = None
    xbrl_file_paths = loop['xbrl_file_paths']
    excel_file_path = loop['excel_file_path']
    security_code = None  # 初期値

    for key, paths in xbrl_file_paths.items():
        if not paths:
            skipped_files.append({
                'reason': f"{key} に対応するXBRLファイルが見つからない",
                'file': excel_file_path
            })
            print(f"{key} に対応するファイルが見つかりません。スキップします。")
            continue

        path = paths[0]
        
        # XBRL解析
        try:
            xbrl_data, security_code = parse_xbrl_data(path)
        except Exception as e:  
            skipped_files.append({
                'reason': f"XBRLデータの解析中にエラー: {e}",
                'file': path
            })
            print(f"{key} のファイル {path} を解析中にエラーが発生しました。スキップします。")
            continue

        cell_map = cell_map_file1 if key == 'file1' else cell_map_file2 if key == 'file2' else cell_map_file3

        if 'CurrentFiscalYearEndDateDEI' in xbrl_data:
            date_str = xbrl_data['CurrentFiscalYearEndDateDEI']
            dt = datetime.strptime(date_str, "%Y-%m-%d")
            xbrl_data['CurrentFiscalYearEndDateDEIyear'] = dt.year
            xbrl_data['CurrentFiscalYearEndDateDEImonth'] = dt.month

        try:
            write_data_to_excel(excel_file_path, xbrl_data, cell_map)
        except Exception as e:
            skipped_files.append({
                'reason': f"Excelデータの書き込み中にエラー: {e}",
                'file': excel_file_path
            })
            print(f"Excelデータの書き込み中にエラーが発生しました。スキップします。")

        # リネーム情報を取得
        if key == 'file3' and \
            'SecurityCodeDEI' in xbrl_data and \
            'CompanyNameCoverPage' in xbrl_data and \
            'CurrentFiscalYearEndDateDEI' in xbrl_data:
            rename_info = (
                xbrl_data['SecurityCodeDEI'],
                xbrl_data['CompanyNameCoverPage'],
                xbrl_data['CurrentFiscalYearEndDateDEI']
            )

    # Excelファイルのリネーム
    if rename_info:
        try:
            excel_file_path = rename_excel_file(excel_file_path, *rename_info)
        except Exception as e:
            skipped_files.append({
                'reason': f"Excelファイルのリネーム中にエラー: {e}",
                'file': excel_file_path
            })
            print(f"Excelファイルのリネーム中にエラーが発生しました。")
    else:
        skipped_files.append({
            'reason': "リネーム用のデータが不足している",
            'file': excel_file_path
        })
        print("リネーム用のデータが不足しています。")

    # 証券コードを表示（デバッグ用）
    if security_code:
        print(f"取得した証券コード: {security_code}")
        stock_code = f"{security_code}.T"

        try:
            write_stock_data_to_excel(excel_file_path, stock_code, date_pairs)  # ループ内では `date_pairs` を使用
        except Exception as e:
            skipped_files.append({
                'reason': f"株価データの書き込み中にエラー: {e}",
                'file': excel_file_path
            })
            print(f"株価データの書き込み中にエラーが発生しました。スキップします。")
    if not security_code:
        skipped_files.append({
            'reason': "証券コードが取得できない",
            'file': path
        })
        print("証券コードが取得できませんでした。")

# スキップされたファイルの一覧を表示
print("\n--- スキップされたファイル一覧 ---")
if skipped_files:
    for skipped in skipped_files:
        print(f"理由: {skipped['reason']}, ファイル: {skipped['file']}")
else:
    print("スキップされたファイルはありません。")