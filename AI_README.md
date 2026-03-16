# AI_README.md

# EDINETツール 開発引き継ぎドキュメント（ChatGPT用）

## 1. プロジェクト概要

### プロジェクト名
edinet-tool

### GitHub
https://github.com/k6qybsxt/edinet-tool

### 目的
EDINETのXBRLを解析し、決算分析Excelテンプレートへ自動入力するツール。

### 対象
- 有価証券報告書
- 半期報告書

### 解析対象
- 売上
- 利益
- CF
- BS
- 株式数
- DEI
- 株価

---

## 2. 現在の開発フェーズ

現在は **IFRS対応と指標正規化の実装フェーズ**

### 大目標
- EDINETツールを10〜50倍高速化する
- 日本基準 / IFRS の両対応を安定させる
- Excelテンプレへ投資家目線で自然な値を書き込む

---

## 3. 完了した設計

### 3-1. XBRL読み込み方法見直し（完了）

旧設計

```text
parse_xbrl_file
  ↓
out
  ↓
Excel

問題

XBRL構造を破棄

再解析が必要

IFRS対応困難

拡張性が低い

新設計

XBRL
 ↓
parse_xbrl_file_raw
 ↓
ParsedXbrlDocument
 ├ facts
 ├ contexts
 ├ units
 ├ nsmap
 ├ dei_data
 └ meta
 ↓
parse_cache
 ↓
各処理
3-2. ParsedXbrlDocument構造
ParsedXbrlDocument
 ├ facts
 ├ contexts
 ├ units
 ├ nsmap
 ├ dei_data
 ├ accounting_standard
 └ document_display_unit
3-3. facts構造
fact
 ├ tag
 ├ qname
 ├ value
 ├ context_ref
 ├ unit_ref
 ├ decimals
 ├ precision
 ├ period_kind
 ├ start_date
 ├ end_date
 ├ instant_date
 ├ is_consolidated
 └ members
3-4. contexts構造
context
 ├ period_kind
 ├ start_date
 ├ end_date
 ├ instant_date
 ├ is_consolidated
 └ members
3-5. units構造
unit
 ├ measures
 ├ numerator
 └ denominator
4. parse cache

XBRLは1回だけ解析される。

parse_cache

[xbrl cache hit]

[xbrl cache miss]

5. raw生成

現在は

facts
↓
raw_service
↓
raw_rows
tags制限

normalize_tag_to_metric(tag) ベースで raw 化

raw 側で metric ごとの絞り込みあり

現在のraw最適化

ProfitLoss: member付きfact除外

IssuedShares / TreasuryShares:

member付きfact除外

instant以外除外

rawログ

[raw metric map]

[raw optimize] skipped_doc_overlap ...

6. 現在のパフォーマンス
1社解析

約 3.8〜4.0 秒
（株価取得込み）

XBRL解析部

0.03〜0.16 秒程度 / 1ファイル

高速化目標

1社 0.2〜0.5 秒

7. IFRS対応の進捗
当初予定

IFRS対応 1/4

現在

IFRS対応 実装かなり進行済み

対応済み内容

accounting_standard 判定

IFRSタグ追加

tag mapping 拡張

raw_builder 対応

Excel出力対応

半期IFRS確認

指標計算の投資家目線調整

8. 現在の重要仕様
8-1. NetAssets

NetAssets_〇〇 には
親会社の所有者に帰属する持ち分合計 を入れる方針。

理由

ProfitLoss が親会社帰属利益ベースのため整合性が高い

投資家目線で自然

8-2. GrossProfit

常に以下で計算する。

GrossProfit = NetSales - CostOfSales

IFRSで売上総利益タグが無くても、この計算値を GrossProfit_〇〇 系へ入れる。

8-3. SellingExpenses

常に以下で扱う。

SellingExpenses
= 販売費及び一般管理費
+ 金融事業に係る金融費用

金融事業に係る金融費用が無い場合は、
販売費及び一般管理費の値をそのまま使う。

8-4. TotalNumber

従来どおり以下で計算する。

TotalNumber = IssuedShares - TreasuryShares
9. 現在追加済みの主要IFRSタグ
9-1. NetSales系

jpcrp_cor:OperatingRevenuesIFRSKeyFinancialData

9-2. OrdinaryIncome系

jpcrp_cor:ProfitLossBeforeTaxIFRSSummaryOfBusinessResults
などを利用中

9-3. NetAssets系

jpcrp_cor:EquityAttributableToOwnersOfParentIFRSSummaryOfBusinessResults

9-4. TotalAssets系

jpcrp_cor:TotalAssetsIFRSSummaryOfBusinessResults

9-5. 金融事業に係る金融費用

FinancialBusinessCost として保持

現在の tags:

jpigp_cor:CostOfFinancingOperationsIFRS

jpigp_cor:FinanceCostsIFRS

10. 直近で実装した重要改善
10-1. CostOfSales 別名タグ対策

実装済み

10-2. SellingExpenses 別名タグ対策

実装済み

10-3. OperatingIncome 別名タグ対策

実装済み

10-4. OrdinaryIncome 別名タグ強化

実装済み

10-5. ProfitLoss の絞り込み

member付きfact除外を実装済み

10-6. IssuedShares / TreasuryShares の絞り込み

member付きfact除外

instant以外除外

10-7. 金融事業に係る金融費用対応

FinancialBusinessCost を追加し、
SellingExpenses = SG&A + FinancialBusinessCost を実装済み

10-8. 半期IFRS確認

トヨタの半期報告書で確認済み
Excel目視でも問題なし

11. 現在のログで確認できていること
年間IFRS

FinancialBusinessCostCurrent

FinancialBusinessCostPrior1

FinancialBusinessCostPrior2

FinancialBusinessCostPrior3

FinancialBusinessCostPrior4

が取得できている。

半期IFRS

FinancialBusinessCostYTD

SellingExpensesYTD

GrossProfitYTD

OperatingIncomeYTD

OrdinaryIncomeYTD

ProfitLossYTD

が取得できている。

12. 現在の主要処理フロー
main
 ↓
loop_builder
 ↓
loop_processor
 ↓
parse_service
 ↓
parse_cache
 ↓
xbrl_parser
 ↓
raw_service
 ↓
excel_service
13. 重要ディレクトリ
src/edinet_tool

cli

config

domain

services

logging_utils

重要ファイル

src/edinet_tool/services/xbrl_parser.py

src/edinet_tool/services/raw_service.py

src/edinet_tool/services/parse_service.py

src/edinet_tool/services/parse_cache.py

src/edinet_tool/services/excel_service.py

src/edinet_tool/services/loop_processor.py

14. 現在の課題
14-1. ZipFile.del 警告

ログに以下が出る。

ZipFile.__del__
ValueError: I/O operation on closed file

これは openpyxl 由来の警告。
現在の主タスクでは触らない。

14-2. 株価取得の未来日

未来日の株価は取得できない。
例: 2026-03-31 の株価。
不具合ではない。

15. 次の優先タスク候補

優先候補

IFRSタグの追加整理

OperatingIncome のIFRS別名タグさらに整理

raw側の不要タグ除外をさらに強化

README / AI_README を都度更新

50社一括解析に向けた安定化

その後、高速化フェーズに戻る

16. 今後のロードマップ
高速化ロードマップ

XBRL parse cache

Excel rename高速化

XBRL読み込み見直し ← 完了

IFRS対応 ← 進行中だがかなり進んだ

50社一括解析

XBRL parse cache強化

並列処理

17. 開発ルール（重要）

ユーザーはPython初心者。

回答ルール

解説不要

指示のみ

必要な時だけ区切る

無理に10回に分けない

プロのプログラマーとして「ここでログを見たい」と思うタイミングで止める

コード修正指示方法

〇行目〜〇行目を置換してください

関数を丸ごと置換してください

注意

中身を把握していないファイルがある場合は回答しない

必要ならアップロードを促す

把握済みファイルに基づいて修正案を出す

18. 開発環境
OS

Windows

Python

3.14

主要ライブラリ

lxml

pandas

openpyxl

yfinance

19. 現在の判断

現在のIFRS対応は、
年間IFRS / 半期IFRS ともに、Excel目視で大きな問題なし。

特に以下は通っている。

GrossProfit

SellingExpenses

NetAssets

TotalNumber

ProfitLoss 絞り込み

IssuedShares / TreasuryShares 絞り込み

FinancialBusinessCost 対応

20. 次チャット開始時の指示

次のAIは以下から開始する。

開始ポイント

AI_README.md を踏まえて、次の優先タスクを1つ選んで進める。

有力候補

OperatingIncome のIFRS別名タグ整理

IFRSタグ追加の残件整理

raw最適化の追加

21. 最重要目標

最終的に実現すること

EDINETツール

50社解析

10〜50倍高速化

日本基準 / IFRS 両対応の安定運用

END