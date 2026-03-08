1. プロジェクト目的

EDINETのXBRLから決算データを抽出し、
Excel決算分析テンプレートへ自動入力する。

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

で行う。

7. 数値単位変換

XBRL unitRefを解析し変換。

XBRL unit	Excel
JPY	百万円
shares	千株

例

331129000000 → 331129
63664400 → 63664

四捨五入処理。

8. rawデータ

rawデータは

sheet = raw_edinet

へ出力。

raw目的

XBRL解析検証

デバッグ

データ監査

9. プログラム構造
src/edinet_tool/

parser/
    xbrl_parser.py

services/
    parse_service.py
    loop_processor.py
    excel_service.py
    raw_service.py

domain/
    raw_builder.py
    dedupe.py
    output_buffer.py

config/
    settings.py

logging/
    logger.py
10. 処理フロー
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
11. 重複処理

rawデータは

raw_key

で重複判定。

キー

company_code
doc_type
consolidation
metric_key
time_slot
period_kind

重複処理

dedupe_raw_rows_keep_best()
12. ログ

主要ログ

[excel write]
[raw]
[loop summary]
[stock summary]

デバッグログ

[parse debug]
[buffer debug]

DEBUGレベルのみ表示。

13. パフォーマンス設計

将来目標

10〜50倍高速化

予定機能

XBRL解析キャッシュ
14. 開発ルール

AIが修正する場合

必ず

関数単位で修正

副作用を出さない

ログ形式を維持

NamedRange仕様を壊さない

15. 相談ログの提示方法

AIへ相談する際は

問題
ログ
関係ファイル

を提示。

例

raw dup still が発生
ログ
...
raw_service.py を確認
16. 今後の設計

予定
EXCELファイルリネーム
XBRL読み込み方法の見直し
IFRS対応
50社一括解析
XBRL parse cache
並列処理


目標

決算分析ツール完全自動化