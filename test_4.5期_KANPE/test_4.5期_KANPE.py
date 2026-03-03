import os
import requests
from io import StringIO
import logging
from logging.handlers import RotatingFileHandler
import certifi

os.environ["SSL_CERT_FILE"] = certifi.where()

import openpyxl
from bs4 import BeautifulSoup
import yfinance as yf
import re
from datetime import datetime, timedelta
import glob
from tkinter import Tk, filedialog
import pandas as pd
import json
from openpyxl.cell.cell import Cell

# ===== skipped_files: 構造化（原因コード化）=====
from dataclasses import dataclass, asdict
from enum import Enum
import traceback

class SkipCode(str, Enum):
    EXCEL_NOT_FOUND   = "EXCEL_NOT_FOUND"      # 作業Excelが見つからない
    FILE1_PARSE_ERROR = "FILE1_PARSE_ERROR"    # 半期XBRL解析失敗
    FILE2_NOT_FOUND   = "FILE2_NOT_FOUND"      # 最新有報XBRLなし
    FILE2_ERROR       = "FILE2_ERROR"          # 最新有報 解析/書込失敗
    FILE3_YEAR_MISS   = "FILE3_YEAR_MISS"      # 過去有報 期末年取れず
    FILE3_ERROR       = "FILE3_ERROR"          # 過去有報 解析/書込失敗
    HALF_WRITE_ERROR  = "HALF_WRITE_ERROR"     # 半期 書込失敗
    NO_SECURITY_CODE  = "NO_SECURITY_CODE"     # 証券コードが取れない（株価等で必要）
    RENAME_ERROR      = "RENAME_ERROR"         # リネーム失敗（非致命）
    UNKNOWN           = "UNKNOWN"

@dataclass
class SkipItem:
    code: str
    phase: str               # "excel_select" / "file1" / "file2" / "file3" / "half" / "stock" / "rename"
    slot: int | None         # loop["slot"] を入れる（将来トレースしやすい）
    excel: str | None
    xbrl: str | None
    message: str
    exc_type: str | None = None
    exc_msg: str | None = None

def add_skip(skipped_files: list, *, code: SkipCode, phase: str, loop: dict | None,
             excel: str | None, xbrl: str | None, message: str, exc: Exception | None = None):
    item = SkipItem(
        code=code.value if isinstance(code, SkipCode) else str(code),
        phase=phase,
        slot=(loop.get("slot") if loop else None),
        excel=excel,
        xbrl=xbrl,
        message=message,
        exc_type=(type(exc).__name__ if exc else None),
        exc_msg=(str(exc) if exc else None),
    )
    skipped_files.append(asdict(item))

def log_skip_summary(logger: logging.Logger, skipped_files: list):
    logger.info("--- skipped summary ---")
    if not skipped_files:
        logger.info("skipped=0")
        return

    # code別件数
    counts: dict[str, int] = {}
    for s in skipped_files:
        c = s.get("code", "UNKNOWN")
        counts[c] = counts.get(c, 0) + 1

    # 1行サマリ（運用向け）
    parts = [f"{k}={counts[k]}" for k in sorted(counts.keys())]
    logger.warning("[skipped summary] " + " ".join(parts))

    # 詳細（最大30件だけ表示：ログ肥大化防止）
    logger.info("--- skipped details (first 30) ---")
    for s in skipped_files[:30]:
        logger.warning(
            "skip "
            f"code={s.get('code')} phase={s.get('phase')} slot={s.get('slot')} "
            f"excel={s.get('excel')} xbrl={s.get('xbrl')} msg={s.get('message')} "
            f"exc={s.get('exc_type')}:{s.get('exc_msg')}"
        )

def normalize_security_code(raw: object) -> str | None:
    """
    4桁の証券コード(数字)に正規化して返す。取れなければNone。
    例: "2206", "2206.0", "2206-T", "2206.T", "2206 " -> "2206"
    """
    if raw is None:
        return None

    s = str(raw).strip()
    if not s:
        return None

    # 小数っぽい "2206.0" 対策
    if re.fullmatch(r"\d+\.0+", s):
        s = s.split(".", 1)[0]

    # "2206.T" / "2206-T" / "2206（東証）" みたいな混在を数字だけに寄せる
    m = re.search(r"(\d{4})", s)
    if not m:
        return None

    code = m.group(1)
    return code

def pick_security_code(*candidates: object) -> str | None:
    """
    候補を左から順に見て、最初に正規化できた4桁コードを返す。
    """
    for c in candidates:
        code = normalize_security_code(c)
        if code:
            return code
    return None

def ensure_security_code(x2: dict | None, parsed_code: object, x1: dict | None = None) -> str | None:
    """
    file2成功時の security_code を必ず確定させる（優先順位固定）
    """
    if x2 is None:
        return None

    # 優先順位（固定）
    # 1) parse_xbrl_data の戻り値
    # 2) DEI の SecurityCode
    # 3) よくある別名キー（念のため）
    # 4) file1側（半期）があるなら最後の保険
    return pick_security_code(
        parsed_code,
        x2.get("SecurityCodeDEI"),
        x2.get("SecurityCode"),
        x2.get("SecurityCodeCoverPage"),
        (x1.get("SecurityCodeDEI") if x1 else None),
        (x1.get("SecurityCode") if x1 else None),
    )

# ===== Logging Policy (固定ルール) =====
# CRITICAL: 続行不可。設定不備・テンプレ汚染防止ロック等。→ SystemExit で終了
# ERROR   : 想定外例外。続行はするが必ず調査対象（logger.exception含む）
# WARNING : 非致命だが結果に影響する可能性（欠損/スキップ/見つからない等）
# INFO    : 進捗と結果サマリ（開始/終了、選択した決算期、各フェーズwritten/missing 等）
# DEBUG   : 詳細（XBRLファイル一覧、キー一覧、内部データの中身など）
def setup_logger(debug: bool = False, log_dir: str | None = None) -> logging.Logger:
    """
    - 入口で1回だけ呼ぶ
    - console: INFO以上（debug=TrueならDEBUG）
    - file   : DEBUG以上を全部保存（運用証跡）
    - 時刻は秒まで（ミリ秒無し）
    - ハンドラ重複を確実に防ぐ
    """
    logger = logging.getLogger("edinet")
    logger.setLevel(logging.DEBUG)  # ロガー本体は常にDEBUG、出力はハンドラ側で制御
    logger.propagate = False

    # 既存ハンドラがあれば全削除（再実行/REPLでも二重出力しない）
    if logger.handlers:
        for h in list(logger.handlers):
            logger.removeHandler(h)

    datefmt = "%Y-%m-%d %H:%M:%S"
    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s",
        datefmt=datefmt
    )

    # --- Console handler ---
    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    ch.setLevel(logging.DEBUG if debug else logging.INFO)
    logger.addHandler(ch)

    # --- File handler (証跡) ---
    if log_dir is None:
        log_dir = os.path.join(os.getcwd(), "logs")
    os.makedirs(log_dir, exist_ok=True)

    log_path = os.path.join(log_dir, "run.log")
    fh = RotatingFileHandler(
        log_path,
        maxBytes=2_000_000,  # 2MB
        backupCount=5,
        encoding="utf-8"
    )
    fh.setFormatter(fmt)
    fh.setLevel(logging.DEBUG)  # ファイルは常に全部残す
    logger.addHandler(fh)

    logger.debug(f"logger initialized: debug={debug}, log_path={log_path}")
    return logger

logger = None


# ===== DEBUG設定 =====
DEBUG = False  # True にすると詳細ログ

def log(*args, **kwargs):
    if DEBUG:
        logger.debug(" ".join(map(str, args)))
        
# スクリプトのあるフォルダに作業ディレクトリを変更
script_dir = os.path.dirname(os.path.abspath(__file__))  # スクリプトのあるフォルダを取得
os.chdir(script_dir)  # 作業ディレクトリをスクリプトのフォルダに変更

# フォルダを選択する関数
def choose_directory():
    root = Tk()
    root.withdraw()              # メインウィンドウ非表示
    root.update()                # これが効く環境が多い
    folder_path = filedialog.askdirectory(title="XBRLフォルダを選択してください")
    root.destroy()               # ★これ重要：Tkを閉じる
    return folder_path

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
    out_meta = {}

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
            key = f"{metric}YTD"
            out[key] = trim_value(best["value"], meta["unit"])
            out_meta[key] = {
                "period_start": best.get("start"),
                "period_end": best.get("end"),
                "period_kind": "duration",
                "unit": meta["unit"],
                "consolidation": "C" if best.get("dim") == "Consolidated" else "N",
                "tag_used": meta["tags"][best["tag_priority"]],
                "tag_rank": best["tag_priority"] + 1,
                "status": "OK",
            }

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
                    key = f"{metric}{suffix}"
                    out[key] = trim_value(best["value"], meta["unit"])
                    out_meta[key] = {
                        "period_start": best.get("start"),
                        "period_end": best.get("end"),
                        "period_kind": "duration",
                        "unit": meta["unit"],
                        "consolidation": "C" if best.get("dim") == "Consolidated" else "N",
                        "tag_used": meta["tags"][best["tag_priority"]],
                        "tag_rank": best["tag_priority"] + 1,
                        "status": "OK",
                    }


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
                key = f"{metric}Quarter"
                out[key] = trim_value(best_q["value"], meta["unit"])
                out_meta[key] = {
                    "period_start": None,
                    "period_end": best_q.get("end"),
                    "period_kind": "instant",
                    "unit": meta["unit"],
                    "consolidation": "C" if best_q.get("dim") == "Consolidated" else "N",
                    "tag_used": meta["tags"][best_q["tag_priority"]],
                    "tag_rank": best_q["tag_priority"] + 1,
                    "status": "OK",
                }

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
                    key = f"{metric}{suffix}"
                    out[key] = trim_value(best["value"], meta["unit"])
                    out_meta[key] = {
                        "period_start": None,
                        "period_end": best.get("end"),
                        "period_kind": "instant",
                        "unit": meta["unit"],
                        "consolidation": "C" if best.get("dim") == "Consolidated" else "N",
                        "tag_used": meta["tags"][best["tag_priority"]],
                        "tag_rank": best["tag_priority"] + 1,
                        "status": "OK",
                    }

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
            key = f"TotalNumber{suffix}"
            out[key] = issued - treasury
            out_meta[key] = {
                "period_start": None,
                "period_end": (out_meta.get(f"IssuedShares{suffix}", {}) or {}).get("period_end"),
                "period_kind": "instant",
                "unit": "ones",
                "consolidation": (out_meta.get(f"IssuedShares{suffix}", {}) or {}).get("consolidation"),
                "tag_used": "CALC(IssuedShares-TreasuryShares)",
                "tag_rank": 0,
                "status": "OK",
            }

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
    if DEBUG:
        logger.debug("=== OUT KEYS ===")
        for k in sorted(out.keys()):
            logger.debug(k)

    # 返り値を増やす
    return out, security_code, out_meta

def _candidate_names(base_key: str):
    """
    base_key: 'NetSales_YTD' のような「Scope無し」のキーを想定
    返り値: ['NetSales_C_YTD', 'NetSales_N_YTD'] のように優先順で返す
    """
    parts = base_key.split("_")
    if len(parts) >= 3:
        # すでに NetSales_C_YTD のようにScope入りならそのまま
        return [base_key]

    # 例: NetSales_YTD → NetSales_C_YTD / NetSales_N_YTD
    if len(parts) == 2:
        metric, period = parts
        return [f"{metric}_C_{period}", f"{metric}_N_{period}"]

    # 例外：変換できない形はそのまま
    return [base_key]

# requests session を1回だけ作って使い回す（毎回作るより安定＆高速）
_YF_SESSION = None

def _get_yf_session():
    global _YF_SESSION
    if _YF_SESSION is None:
        s = requests.Session()
        s.verify = certifi.where()  # ← CA を固定
        _YF_SESSION = s
    return _YF_SESSION

# 株価データの取得
def _to_stooq_symbol(stock_code: str) -> str:
    """
    '2206.T' / '2206' どちらが来ても stooq用に変換する
    stooqの日本株は '2206.JP' 形式
    """
    code = stock_code.strip().upper()
    if code.endswith(".T"):
        code = code[:-2]
    # 数字4桁だけ抽出したい場合はここで調整してOK
    return f"{code}.JP"

def get_stock_price(stock_code, target_date, backup_date, buffer_days=3):
    """
    stooq から日足CSVを取得して、target_date→見つからなければ過去に遡ってCloseを返す
    """
    # 取得期間（余裕を持たせる）
    start_date = (datetime.strptime(backup_date, "%Y-%m-%d") - timedelta(days=buffer_days)).date()
    end_date   = (datetime.strptime(target_date, "%Y-%m-%d") + timedelta(days=1)).date()

    symbol = _to_stooq_symbol(stock_code)
    url = f"https://stooq.com/q/d/l/?s={symbol}&i=d"

    r = requests.get(url, timeout=20)
    r.raise_for_status()

    df = pd.read_csv(StringIO(r.text))
    if df.empty or "Date" not in df.columns or "Close" not in df.columns:
        return None

    df["Date"] = pd.to_datetime(df["Date"]).dt.date
    # 期間で絞る
    df = df[(df["Date"] >= start_date) & (df["Date"] <= end_date)]
    if df.empty:
        return None

    # target_dateから遡って探す
    check = datetime.strptime(target_date, "%Y-%m-%d").date()
    start = start_date

    close_map = dict(zip(df["Date"], df["Close"]))

    while check >= start:
        if check in close_map and pd.notna(close_map[check]):
            return float(close_map[check])
        check -= timedelta(days=1)

    return None

def validate_stock_date_pairs(date_pairs):
    """
    JSONのdate_pairsが「name前提」になっているか検証する。
    不整合があれば早期に気づけるように ValueError を投げる。
    """
    bad = []
    for i, item in enumerate(date_pairs):
        # cell が残っていたらNG（完全NamedRange化の契約違反）
        if "cell" in item:
            bad.append((i, "cell_is_not_allowed", item))
            continue
        # name / target_date / backup_date は必須
        if not item.get("name") or not item.get("target_date") or not item.get("backup_date"):
            bad.append((i, "missing_required_keys", item))

    if bad:
        msg_lines = ["株価date_pairsが name前提になっていません（JSONを修正してください）。"]
        for i, reason, item in bad[:10]:  # 長くなりすぎないように先頭だけ
            msg_lines.append(f"  - index={i}, reason={reason}, item={item}")
        raise ValueError("\n".join(msg_lines))

#NamedRangeに書き込むユーティリティ
def _set_value_to_namedrange(workbook, range_name: str, value) -> bool:
    """
    NamedRangeが存在すれば、その参照先セル(群)に value を書く。成功なら True。
    1セル想定だが、範囲でも全セルに同じ値を書ける。
    """
    dn = workbook.defined_names.get(range_name)
    if dn is None:
        return False

    for sheet_name, ref in dn.destinations:
        if sheet_name not in workbook.sheetnames:
            continue
        ws = workbook[sheet_name]

        obj = ws[ref]
        # ws["A1"] は Cell、ws["A1:B2"] は tuple(tuple(Cell))
        if isinstance(obj, openpyxl.cell.cell.Cell):
            obj.value = value
        else:
            for row in obj:
                for cell in row:
                    cell.value = value
    return True

# 株価データをExcelに書き込む
def write_stock_data_to_excel(excel_file, stock_code, date_pairs):
    """
    株価は NamedRange 専用（必須）。
    date_pairs の各要素は必ず:
      {"name": "...", "target_date": "YYYY-MM-DD", "backup_date": "YYYY-MM-DD"}
    を満たすこと。
    """
    workbook = openpyxl.load_workbook(excel_file)

    result = {
        "written": 0,
        "miss": 0,          # 株価データが取得できなかった（非致命）
        "errors": 0,        # 例外など（非致命だが要調査）
        "missing_name": 0,  # NamedRange が見つからない（テンプレ不整合）
        "bad_input": 0,     # JSON要素の不足（設計ミス）
    }

    for item in date_pairs:
        name = item.get("name")
        target_date = item.get("target_date")
        backup_date = item.get("backup_date")

        # 入力仕様違反（name前提）
        if not name or not target_date or not backup_date:
            logger.warning(f"[WARNING] 株価date_pairsの要素が不完全なのでスキップ: {item}")
            result["bad_input"] += 1
            continue

        # 取得
        try:
            price = get_stock_price(stock_code, target_date, backup_date)
        except Exception as e:
            logger.exception(
                f"株価取得で想定外エラー（続行） "
                f"code={stock_code} target={target_date} backup={backup_date}"
            )
            result["errors"] += 1
            continue

        if price is None:
            logger.warning(f"{target_date} の株価が取得できませんでした（続行）: name={name} code={stock_code}")
            result["miss"] += 1
            continue

        v = float(price)

        # 書き込み（NamedRangeのみ）
        wrote = _set_value_to_namedrange(workbook, name, v)
        if not wrote:
            logger.warning(f"NamedRangeが見つからず書けませんでした: {name} ({target_date})")
            result["missing_name"] += 1
            continue

        result["written"] += 1
        logger.debug(f"{target_date} の株価を書き込みました: {name}")

    # --- 株価処理サマリ（必ず1回出す：運用の核） ---
    logger.info(
        f"[stock summary] "
        f"written={result['written']} "
        f"miss={result['miss']} "
        f"errors={result['errors']} "
        f"missing_name={result['missing_name']} "
        f"bad_input={result['bad_input']} "
        f"(code={stock_code})"
    )

    # --- 問題があれば警告（要調査だが続行） ---
    if result["errors"] > 0 or result["missing_name"] > 0 or result["bad_input"] > 0:
        logger.warning(
            f"[stock warning] issues detected "
            f"errors={result['errors']} "
            f"missing_name={result['missing_name']} "
            f"bad_input={result['bad_input']} "
            f"(code={stock_code})"
        )

    workbook.save(excel_file)
    return result

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

    logger.info(f"Excelファイルがリネームされました: {new_file_path}")
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
def find_available_excel_file(base_path, file_name, max_copies=30):
    """
    方針：
      1) まずコピー系（- コピー / - コピー (n)）を探し、あれば「更新日時が最新」を返す
      2) コピーが無ければ、オリジナル（file_name.xlsx/.xlsm）を探す
      3) オリジナルが見つかったら「新しいコピーを作って」そのパスを返す
      4) それも無ければワイルドカードで最後の救済
    """
    import shutil

    exts = ["xlsx", "xlsm"]

    def newest(paths):
        paths = [p for p in paths if os.path.exists(p)]
        if not paths:
            return None
        paths.sort(key=lambda p: os.path.getmtime(p), reverse=True)
        return paths[0]

    def make_next_copy(original_path):
        # 例：決算分析シート-1.xlsx → 決算分析シート-1 - コピー.xlsx / (2) / (3)...
        root, ext = os.path.splitext(original_path)
        base = root  # 既に拡張子無しのフルパス

        # まず「 - コピー.ext」が空いていればそこへ
        candidate0 = f"{base} - コピー{ext}"
        if not os.path.exists(candidate0):
            shutil.copy2(original_path, candidate0)
            return candidate0

        # 次に (2)〜 を探す
        for i in range(2, max_copies + 2):
            cand = f"{base} - コピー ({i}){ext}"
            if not os.path.exists(cand):
                shutil.copy2(original_path, cand)
                return cand

        raise RuntimeError("コピー上限に達しました（max_copies を増やしてください）")

    # --- 0) 名前ゆれ吸収（_ と - の両方を試す） ---
    variants = {file_name}
    variants.add(file_name.replace("決算分析シート-", "決算分析シート_"))
    variants.add(file_name.replace("決算分析シート_", "決算分析シート-"))

    # --- 1) まずコピーを探す（厳密） ---
    copy_hits = []
    for v in variants:
        for ext in exts:
            copy_hits.append(os.path.join(base_path, f"{v} - コピー.{ext}"))
            for i in range(2, max_copies + 2):
                copy_hits.append(os.path.join(base_path, f"{v} - コピー ({i}).{ext}"))

    hit = newest(copy_hits)
    if hit:
        return hit

    # --- 2) コピーが無い → オリジナルを探す ---
    originals = []
    for v in variants:
        for ext in exts:
            originals.append(os.path.join(base_path, f"{v}.{ext}"))

    orig = newest(originals)
    if orig:
        # ★重要：オリジナルを返さず、必ずコピーを作って返す
        try:
            new_copy = make_next_copy(orig)
            logger.info(f"テンプレから作業用コピーを作成しました: {new_copy}")
            return new_copy
        except Exception:
            logger.exception("作業用コピー作成に失敗しました")
            raise

    # --- 3) 最終フォールバック：ワイルドカード（コピー表記のゆれ吸収） ---
    patterns = []
    for v in variants:
        patterns += [
            os.path.join(base_path, f"{v}*コピー*.xlsx"),
            os.path.join(base_path, f"{v}*コピー*.xlsm"),
        ]
    hits = []
    for pat in patterns:
        hits.extend(glob.glob(pat))

    hit2 = newest(hits)
    if hit2:
        return hit2

    logger.warning(f"{file_name} のいずれのバージョンも見つかりませんでした。")
    return None

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

def filter_for_annual(data: dict, use_half: bool = False) -> dict:
    """
    通期テンプレ（旧 cell_map_annual 相当）に存在するキーだけ返す。
    use_half=True（半期あり）のときは *_Current を書かない運用を維持。
    """
    allow = {
        # 売上
        "NetSalesPrior4", "NetSalesPrior3", "NetSalesPrior2", "NetSalesPrior1", "NetSalesCurrent",

        # 売上原価〜営業利益
        "CostOfSalesPrior4", "CostOfSalesPrior3", "CostOfSalesPrior2", "CostOfSalesPrior1", "CostOfSalesCurrent",
        "GrossProfitPrior4", "GrossProfitPrior3", "GrossProfitPrior2", "GrossProfitPrior1", "GrossProfitCurrent",
        "SellingExpensesPrior4", "SellingExpensesPrior3", "SellingExpensesPrior2", "SellingExpensesPrior1", "SellingExpensesCurrent",
        "OperatingIncomePrior4", "OperatingIncomePrior3", "OperatingIncomePrior2", "OperatingIncomePrior1", "OperatingIncomeCurrent",

        # 経常・純利益
        "OrdinaryIncomePrior4", "OrdinaryIncomePrior3", "OrdinaryIncomePrior2", "OrdinaryIncomePrior1", "OrdinaryIncomeCurrent",
        "ProfitLossPrior4", "ProfitLossPrior3", "ProfitLossPrior2", "ProfitLossPrior1", "ProfitLossCurrent",

        # 発行株式数（自己株控除後）
        "TotalNumberPrior4", "TotalNumberPrior3", "TotalNumberPrior2", "TotalNumberPrior1", "TotalNumberCurrent",

        # BS
        "TotalAssetsPrior4", "TotalAssetsPrior3", "TotalAssetsPrior2", "TotalAssetsPrior1", "TotalAssetsCurrent",
        "NetAssetsPrior4", "NetAssetsPrior3", "NetAssetsPrior2", "NetAssetsPrior1", "NetAssetsCurrent",

        # CF
        "OperatingCashPrior4", "OperatingCashPrior3", "OperatingCashPrior2", "OperatingCashPrior1", "OperatingCashCurrent",
        "InvestmentCashPrior4", "InvestmentCashPrior3", "InvestmentCashPrior2", "InvestmentCashPrior1", "InvestmentCashCurrent",
        "FinancingCashPrior4", "FinancingCashPrior3", "FinancingCashPrior2", "FinancingCashPrior1", "FinancingCashCurrent",

        # 現金等
        "CashAndCashEquivalentsPrior4", "CashAndCashEquivalentsPrior3", "CashAndCashEquivalentsPrior2",
        "CashAndCashEquivalentsPrior1", "CashAndCashEquivalentsCurrent",

        # 表紙系（必要なら）
        "SecurityCodeDEI",
        "CompanyNameCoverPage",
    }

    out = {k: v for k, v in data.items() if k in allow}

    # 半期あり運用：通期の *_Current は使わない（今まで通り）
    if use_half:
        out = {k: v for k, v in out.items() if not k.endswith("Current")}

    return out

def filter_for_annual_old(data: dict) -> dict:
    """
    過去有報（file3）は Prior 側だけを使う。
    さらにテンプレに入力欄がある指標だけに絞る。
    """
    allow = {
        # 売上
        "NetSalesPrior4", "NetSalesPrior3", "NetSalesPrior2", "NetSalesPrior1",

        # 売上原価〜営業利益
        "CostOfSalesPrior4", "CostOfSalesPrior3", "CostOfSalesPrior2", "CostOfSalesPrior1",
        "GrossProfitPrior4", "GrossProfitPrior3", "GrossProfitPrior2", "GrossProfitPrior1",
        "SellingExpensesPrior4", "SellingExpensesPrior3", "SellingExpensesPrior2", "SellingExpensesPrior1",
        "OperatingIncomePrior4", "OperatingIncomePrior3", "OperatingIncomePrior2", "OperatingIncomePrior1",

        # 経常・純利益
        "OrdinaryIncomePrior4", "OrdinaryIncomePrior3", "OrdinaryIncomePrior2", "OrdinaryIncomePrior1",
        "ProfitLossPrior4", "ProfitLossPrior3", "ProfitLossPrior2", "ProfitLossPrior1",

        # 発行株式数（自己株控除後）
        "TotalNumberPrior4", "TotalNumberPrior3", "TotalNumberPrior2", "TotalNumberPrior1",

        # BS
        "TotalAssetsPrior4", "TotalAssetsPrior3", "TotalAssetsPrior2", "TotalAssetsPrior1",
        "NetAssetsPrior4", "NetAssetsPrior3", "NetAssetsPrior2", "NetAssetsPrior1",

        # CF
        "OperatingCashPrior4", "OperatingCashPrior3", "OperatingCashPrior2", "OperatingCashPrior1",
        "InvestmentCashPrior4", "InvestmentCashPrior3", "InvestmentCashPrior2", "InvestmentCashPrior1",
        "FinancingCashPrior4", "FinancingCashPrior3", "FinancingCashPrior2", "FinancingCashPrior1",

        # 現金等
        "CashAndCashEquivalentsPrior4", "CashAndCashEquivalentsPrior3",
        "CashAndCashEquivalentsPrior2", "CashAndCashEquivalentsPrior1",
    }

    return {k: v for k, v in data.items() if k in allow}

def filter_for_half(data: dict) -> dict:
    """
    半期テンプレ（旧 cell_map_half 相当）に存在するキーだけ返す。
    NamedRange移行後も、テンプレに無い項目は出力しない。
    """
    allow = {
        # PL（YTD）
        "NetSalesYTD",
        "OrdinaryIncomeYTD",
        "ProfitLossYTD",

        # 株式・BS（Quarter）
        "TotalNumberQuarter",
        "TotalAssetsQuarter",
        "NetAssetsQuarter",

        # CF（YTD）
        "OperatingCashYTD",
        "InvestmentCashYTD",
        "FinancingCashYTD",

        # 現金等（Quarter）
        "CashAndCashEquivalentsQuarter",

        # 表紙（必要なら）
        "SecurityCodeDEI",
        "CompanyNameCoverPage",
        "CurrentFiscalYearEndDateDEIyear",
        "CurrentFiscalYearEndDateDEImonth",
    }

    out = {}
    for k, v in data.items():
        if k in allow:
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

# =========================
# NamedRange Writer + キー変換（Scopeなし版）
# =========================
# 変換対象の「語尾」一覧（あなたの out キー体系に合わせる）
_SUFFIXES = [
    "YTD",
    "Quarter",
    "Current",
    "Prior1", "Prior2", "Prior3", "Prior4",
]

_suffix_pat = re.compile(rf"^(.+?)({'|'.join(_SUFFIXES)})$")

def to_namedrange_key(key: str) -> str:
    """
    例：
      NetSalesYTD        -> NetSales_YTD
      TotalAssetsQuarter -> TotalAssets_Quarter
      ProfitLossPrior2   -> ProfitLoss_Prior2
      SecurityCodeDEI    -> SecurityCodeDEI（そのまま）
    """
    if not isinstance(key, str) or not key:
        return key

    # DEI系や既に '_' を含むものは基本そのまま（必要なら後で個別対応）
    if "_" in key:
        return key
    if key.endswith("DEI"):
        return key

    m = _suffix_pat.match(key)
    if not m:
        return key

    metric = m.group(1)
    suffix = m.group(2)
    return f"{metric}_{suffix}"


def transform_keys_for_namedranges(data: dict) -> dict:
    """
    data のキーを NamedRange 名に合う形へ変換して返す
    """
    out = {}
    for k, v in data.items():
        nk = to_namedrange_key(k)
        out[nk] = v
    return out


def _iter_namedrange_cells(workbook: openpyxl.Workbook, range_name: str):
    """
    NamedRangeが指すセル（1セル/複数セル）を返す
    - ref が "A1" だと ws[ref] は Cell を返す
    - ref が "A1:B2" だと ws[ref] は 2次元タプルを返す
    """
    dn = workbook.defined_names.get(range_name)
    if dn is None:
        return

    for sheet_name, ref in dn.destinations:
        if sheet_name not in workbook.sheetnames:
            continue
        ws = workbook[sheet_name]

        obj = ws[ref]

        # 1セル（Cell）ケース
        if isinstance(obj, Cell):
            yield obj
            continue

        # 範囲（2次元のタプル）ケース
        # obj は ((Cell, Cell, ...), (Cell, Cell, ...), ...)
        for row in obj:
            for cell in row:
                yield cell

def write_data_to_excel_namedranges(excel_file: str, data: dict, *,
                                   transform_keys: bool = True,
                                   skip_if_formula: bool = True,
                                   skip_values=("データなし", "", None),
                                   dry_run: bool = False) -> dict:
    """
    data の key と同名の NamedRange に書き込む（cell_map不要）
    - transform_keys=True のとき NetSalesYTD -> NetSales_YTD のように変換してから書く
    - 数式セルは上書きしない（あなたの既存挙動を踏襲）
    戻り値:
      {
        "written": [(name, "Sheet!A1"), ...],
        "skipped": [(name, reason), ...],
        "missing":  [name, ...]
      }
    """
    wb = openpyxl.load_workbook(excel_file, keep_vba=excel_file.lower().endswith(".xlsm"))
    result = {"written": [], "skipped": [], "missing": []}

    payload = transform_keys_for_namedranges(data) if transform_keys else dict(data)

    for name, value in payload.items():
        # None / 空 / データなし は書かない
        if value in skip_values or (isinstance(value, str) and value.strip() in skip_values):
            result["skipped"].append((name, "empty"))
            continue

        cells = list(_iter_namedrange_cells(wb, name))
        if not cells:
            result["missing"].append(name)
            continue

        for cell in cells:
            if skip_if_formula and isinstance(cell.value, str) and cell.value.startswith("="):
                result["skipped"].append((name, f"formula@{cell.coordinate}"))
                continue
            cell.value = value
            result["written"].append((name, f"{cell.parent.title}!{cell.coordinate}"))

    if not dry_run:
        wb.save(excel_file)

    return result

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

def load_config(file_path):
            with open(file_path, "r", encoding="utf-8") as f:
                return json.load(f)

# 各ループで処理するファイルパスをリスト化
def build_loops(base_dir, template_dir, max_n=50):
    """
    現在：決算分析シート-{n}.xlsx を対象に loops を作る
    将来：会社コード_会社名 方式に切替予定（この関数だけ差し替えればOK）
    """
    loops = []
    for n in range(1, max_n + 1):
        file1 = glob.glob(os.path.join(base_dir, f"{n}-2*.xbrl"))
        file2 = glob.glob(os.path.join(base_dir, f"{n}-4*.xbrl"))
        file3 = glob.glob(os.path.join(base_dir, f"{n}-5*.xbrl"))

        # デバッグ（必要な時だけ）
        logger.debug(f"[{n}] file1: {file1}")
        logger.debug(f"[{n}] file2: {file2}")
        logger.debug(f"[{n}] file3: {file3}")

        excel_file_path = os.path.join(template_dir, f"決算分析シート_{n}.xlsx")

        loops.append({
            "xbrl_file_paths": {"file1": file1, "file2": file2, "file3": file3},
            "excel_file_path": excel_file_path,
            "slot": n,  # 将来の拡張用（ログ/追跡用）
        })

    return loops

# XBRLデータの取得、証券コードの取得、Excelへの書き込み、株価データ取得までをループ処理に含める
def process_one_loop(loop, date_pairs, skipped_files):
    """
    1社（=1ループ）分の処理。
    - skipped_files は main から渡される（global禁止）
    - 例外は基本ここで握らず、main側でcatchして次へ進む
    """
    annual_reported = False

    excel_base_name = os.path.basename(loop["excel_file_path"]).replace(".xlsx", "")
    excel_directory = os.path.dirname(loop["excel_file_path"])

    # --- Excel選択失敗 ---
    selected_file = find_available_excel_file(excel_directory, excel_base_name)
    if not selected_file:
        add_skip(
            skipped_files,
            code=SkipCode.EXCEL_NOT_FOUND,
            phase="excel_select",
            loop=loop,
            excel=excel_base_name,
            xbrl=None,
            message="使用するExcelが見つからない"
        )
        logger.warning("使用するファイルが見つかりませんでした。次のループを実行します。")
        return

    loop["excel_file_path"] = selected_file
    logger.info(f"使用するExcelファイル: {selected_file}")

    # ★安全ロック：テンプレ（コピー無し）に書き込ませない
    base_no_ext = os.path.splitext(selected_file)[0]
    if " - コピー" not in base_no_ext:
        logger.critical(f"安全ロック発動：コピーではないExcelに書き込もうとしました: {selected_file}")
        raise SystemExit(1)

    rename_info = None
    xbrl_file_paths = loop["xbrl_file_paths"]
    excel_file_path = loop["excel_file_path"]
    security_code = None
    base_year = None

    # -------------------------
    # 0) file1（半期）があれば先に読む（base_year決定）
    # -------------------------
    x1 = None
    use_half = bool(xbrl_file_paths.get("file1") and xbrl_file_paths["file1"])

    # --- file1（半期）解析失敗 ---
    if use_half:
        try:
            path1 = xbrl_file_paths["file1"][0]
            x1, _ = parse_xbrl_data(path1, mode="half")
            base_year = get_fy_end_year(x1)
        except Exception as e:
            add_skip(
                skipped_files,
                code=SkipCode.FILE1_PARSE_ERROR,
                phase="file1",
                loop=loop,
                excel=excel_file_path,
                xbrl=(xbrl_file_paths["file1"][0] if xbrl_file_paths.get("file1") else None),
                message="file1(半期) 解析エラー",
                exc=e
            )
            x1 = None
            use_half = False

    # -------------------------
    # 1) file2（最新有報）
    # -------------------------
    x2 = None

    if xbrl_file_paths.get("file2") and xbrl_file_paths["file2"]:
        try:
            path2 = xbrl_file_paths["file2"][0]
            x2, parsed_security_code = parse_xbrl_data(path2, mode="full")

            # ★ここで必ず確定（優先順位固定）
            security_code = ensure_security_code(x2, parsed_security_code, x1)
            logger.debug(f"[code check] parsed={parsed_security_code} fixed={security_code}")

            # （ここに：あなたの既存の file2 書込処理）

            # リネーム用：半期ありなら period_end（上期末）を優先
            period_for_name = None
            if use_half and x1 and x1.get("CurrentPeriodEndDateDEI"):
                period_for_name = x1["CurrentPeriodEndDateDEI"]
            elif x2.get("CurrentFiscalYearEndDateDEI"):
                period_for_name = x2["CurrentFiscalYearEndDateDEI"]

            # ★rename_infoは SecurityCodeDEI を直接参照せず、確定済み security_code を使う
            if security_code and period_for_name:
                cname = (
                    x2.get("CompanyNameCoverPage")
                    or (x1.get("CompanyNameCoverPage") if x1 else "")
                    or ""
                )
                rename_info = (security_code, cname, period_for_name)
                logger.debug(f"[rename check] rename_info={rename_info}")

        except Exception as e:
            add_skip(
                skipped_files,
                code=SkipCode.FILE2_ERROR,
                phase="file2",
                loop=loop,
                excel=excel_file_path,
                xbrl=path2,
                message="file2(最新有報) 解析/書込エラー",
                exc=e
            )
    else:
        add_skip(
            skipped_files,
            code=SkipCode.FILE2_NOT_FOUND,
            phase="file2",
            loop=loop,
            excel=excel_file_path,
            xbrl=None,
            message="file2(最新有報) が見つからない"
        )

    # -------------------------
    # 2) file3（過去有報）→ Prior補完
    # -------------------------
    # --- file3（過去有報）年取れず / 失敗 ---
    if base_year is not None and xbrl_file_paths.get("file3") and xbrl_file_paths["file3"]:
        try:
            path3 = xbrl_file_paths["file3"][0]
            x3, _ = parse_xbrl_data(path3, mode="full")
            y3 = get_fy_end_year(x3)

            if y3 is None:
                add_skip(
                    skipped_files,
                    code=SkipCode.FILE3_YEAR_MISS,
                    phase="file3",
                    loop=loop,
                    excel=excel_file_path,
                    xbrl=path3,
                    message="file3 期末年が取れない"
                )
            else:
                # （中略：あなたの既存処理）
                pass

        except Exception as e:
            add_skip(
                skipped_files,
                code=SkipCode.FILE3_ERROR,
                phase="file3",
                loop=loop,
                excel=excel_file_path,
                xbrl=(xbrl_file_paths["file3"][0] if xbrl_file_paths.get("file3") else None),
                message="file3(過去有報) 解析/書込エラー",
                exc=e
            )

    # -------------------------
    # 3) 半期ありなら最後にYTD/Quarter確定
    # -------------------------
    # --- half（半期）書込失敗 ---
    if use_half and x1 is not None:
        try:
            r1 = write_data_to_excel_namedranges(excel_file_path, filter_for_half(x1))
            # （中略：あなたの既存処理）
        except Exception as e:
            add_skip(
                skipped_files,
                code=SkipCode.HALF_WRITE_ERROR,
                phase="half",
                loop=loop,
                excel=excel_file_path,
                xbrl=(xbrl_file_paths["file1"][0] if xbrl_file_paths.get("file1") else None),
                message="half(半期) 書込エラー",
                exc=e
            )

    # -------------------------
    # 4) 株価（非致命）
    # -------------------------
    if security_code:
        logger.info(f"取得した証券コード: {security_code}")
        stock_code = f"{security_code}.T"
        logger.debug(f"[stock check] security_code={security_code} stock_code={stock_code}")
        try:
            stock_result = write_stock_data_to_excel(excel_file_path, stock_code, date_pairs)
            if stock_result:
                logger.debug(
                    f"[stock] written={stock_result.get('written',0)} "
                    f"miss={stock_result.get('miss',0)} "
                    f"errors={stock_result.get('errors',0)} "
                    f"missing_name={stock_result.get('missing_name',0)} "
                    f"bad_input={stock_result.get('bad_input',0)}"
                )

        except Exception:
            logger.exception("株価データの書き込みで想定外エラー（続行）")
    # --- 証券コード無し（株価の前提） ---
    else:
        add_skip(
            skipped_files,
            code=SkipCode.NO_SECURITY_CODE,
            phase="stock",
            loop=loop,
            excel=excel_file_path,
            xbrl=None,
            message="証券コードが取得できない"
        )
        logger.warning("証券コードが取得できませんでした。")

    # -------------------------
    # 5) リネーム（非致命）
    # -------------------------
    if rename_info:
        try:
            rename_excel_file(
                excel_file_path,
                rename_info[0],  # security_code
                rename_info[1],  # company_name
                rename_info[2],  # period_end_date
            )
        except Exception:
            logger.exception("リネーム中にエラーが発生しました（続行）")


def main():
    # 作業ディレクトリを表示
    logger.info(f"作業ディレクトリ: {os.getcwd()}")

    # === XBRLフォルダ設定 ===
    base_dir = r"C:\Users\silve\OneDrive\PC\開発\test\Python\XBRL"
    template_dir = r"C:\Users\silve\OneDrive\PC\EDINET\決算分析シート"

    logger.info(f"XBRLフォルダ（固定）: {base_dir}")
    if not os.path.isdir(base_dir):
        logger.critical(f"base_dir が存在しません。パスを確認してください: {base_dir}")
        raise SystemExit(1)

    # === 件数入力 ===
    file_count = choose_file_count()

    # === スキップ一覧（ローカル）===
    skipped_files = []

    # === loops生成 ===
    loops = build_loops(base_dir, template_dir, max_n=50)

    # === 決算期を最初に1回だけ選択 ===
    try:
        config = load_config("決算期_KANPE.json")
        chosen_period = input("決算期を選択してください（例 25-1）: ")

        if chosen_period not in config:
            logger.critical("無効な選択です。プログラムを終了します。")
            raise SystemExit(1)

        date_pairs = config[chosen_period]
        validate_stock_date_pairs(date_pairs)
        logger.info(f"選択された決算期: {chosen_period}")
        logger.debug(f"決算期データ: {date_pairs}")
    except Exception:
        logger.exception("決算期設定の読み込み/選択に失敗しました")
        raise SystemExit(1)

    # === ★ここが必要：1件ずつ処理する（呼び出し）===
    for i in range(min(file_count, len(loops))):
        try:
            process_one_loop(loops[i], date_pairs, skipped_files)
        except SystemExit:
            raise
        except Exception:
            logger.exception(f"1件処理で想定外エラー（続行）: index={i}")

    # === スキップ一覧表示（最後）===
    log_skip_summary(logger, skipped_files)

if __name__ == "__main__":
    logger = setup_logger(debug=DEBUG)
    try:
        logger.info("===== プログラム開始 =====")
        main()
        logger.info("===== 正常終了 =====")
    except SystemExit:
        raise
    except Exception:
        logger.exception("致命的エラーで終了しました")
        raise