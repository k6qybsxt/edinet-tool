AI_README.md（更新版）
1. プロジェクト目的

EDINETのXBRLから決算データを抽出し、
Excel決算分析テンプレートへ自動入力するPythonツール。 

AI_README

目的

決算データ取得自動化

Excel分析テンプレ更新自動化

企業分析効率化

2. 入力データ

EDINET XBRL

対象書類

有価証券報告書

四半期報告書

入力ファイル構成

file1 = 四半期報告書 (存在する場合)
file2 = 最新有価証券報告書
file3 = 過去有価証券報告書
3. モード
halfモード

四半期報告書が存在する場合

mode = half

取得対象

YTD
Quarter
Prior1
Prior2
Prior3
Prior4

特徴

Current列は四半期データを優先

通期Currentは書き込まない

fullモード

四半期報告書が存在しない場合

mode = full

取得対象

Current
Prior1
Prior2
Prior3
Prior4
4. 取得メトリクス
Duration
NetSales
CostOfSales
GrossProfit
SellingExpenses
OperatingIncome
OrdinaryIncome
ProfitLoss
OperatingCash
InvestmentCash
FinancingCash
Instant
TotalAssets
NetAssets
CashAndCashEquivalents
IssuedShares
TreasuryShares
TotalNumber
5. TotalNumber計算
TotalNumber = IssuedShares - TreasuryShares

suffix別に計算

例

IssuedSharesPrior1
TreasurySharesPrior1
→ TotalNumberPrior1
6. Excel出力

NamedRange方式

例

NetSales_Current
OperatingIncome_Prior2
TotalAssets_Prior3
TotalNumber_Quarter

Excel書き込みは

excel_service.py

で実行。

7. Excelテンプレート

テンプレート形式

.xlsm

理由

VBAマクロ使用

更新ボタンによる列更新

マクロ例

Sub 更新()

Pythonは

値のみ書き込み

を行う。

8. 数値単位変換

XBRL unitRefを解析し変換

XBRL	Excel
JPY	百万円
shares	千株

例

331129000000 → 331129
63664400 → 63664

四捨五入。

9. rawデータ

rawデータは

sheet = raw_edinet

へ出力。

用途

XBRL解析検証

デバッグ

データ監査

10. プログラム構造
src/edinet_tool/

parser/
    xbrl_parser.py

services/
    parse_service.py
    loop_processor.py
    excel_service.py
    raw_service.py
    stock_service.py

domain/
    raw_builder.py
    dedupe.py
    output_buffer.py

config/
    settings.py

logging/
    logger.py
11. 処理フロー
XBRL
 ↓
xbrl_parser
 ↓
parse_service
 ↓
loop_processor
 ↓
output_buffer
 ↓
excel_service
 ↓
Excel

raw処理

raw_builder
 ↓
raw_service
 ↓
dedupe
12. 株価取得

株価取得は

stock_service.py

で実行。

取得方法

yfinance

取得データ

Prior4
Prior3
Prior2
Prior1
Q1
Q2
Q3
Q4

高速化設計

株価キャッシュ方式

一度取得したデータは

cache dict

に保存。

同一銘柄の複数日取得を

1回のAPI取得

にまとめる。

13. 重複処理

rawデータは

raw_key

で重複判定

キー

company_code
doc_type
consolidation
metric_key
time_slot
period_kind

重複処理

dedupe_raw_rows_keep_best()
14. ログ

主要ログ

[excel write]
[raw]
[loop summary]
[stock summary]

デバッグログ

[parse debug]
[buffer debug]

DEBUGレベルのみ表示

15. ZipFile警告

Python3.14 + openpyxl + xlsm環境で

ZipFile.__del__ warning

が発生する場合がある。

例

Exception ignored while calling deallocator ZipFile.__del__

現状

出力成功

Excel書き込み成功

のため

致命エラーではない

運用上は

警告は無視可能
16. 開発ルール

AIが修正する場合

必ず

関数単位で修正
副作用を出さない
ログ形式を維持
NamedRange仕様を壊さない
17. AIへ相談する方法

必ず提示

問題
ログ
関係ファイル

例

raw dup still が発生

ログ
...

raw_service.py を確認
18. 今後の設計

予定

EXCELファイルリネーム
XBRL読み込み方法見直し
IFRS対応
50社一括解析
XBRL parse cache
並列処理
19. 次の開発タスク

現在の優先

file1〜50探索ログ削減

理由

現在

[1] file1
[2] file1
...
[50] file1

が DEBUGログに大量出力される。

目的

ログ可読性改善
ログサイズ削減
目標
EDINET決算分析ツール完全自動化