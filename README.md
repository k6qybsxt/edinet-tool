EDINET XBRL Parser & Excel Analyzer
概要

EDINETで公開されている XBRL形式の有価証券報告書・四半期報告書 を解析し、
Excelの決算分析テンプレートへ 自動入力するPythonツール。

目的は

決算データ取得の自動化

Excel分析テンプレの自動更新

企業分析の高速化

主な機能
1. XBRL解析

EDINET XBRLから以下の財務データを取得

カテゴリ	項目
売上系	NetSales / CostOfSales / GrossProfit
利益系	OperatingIncome / OrdinaryIncome / ProfitLoss
BS	TotalAssets / NetAssets / CashAndCashEquivalents
CF	OperatingCash / InvestmentCash / FinancingCash
株式	IssuedShares / TreasuryShares / TotalNumber
2. 取得期間
期間	説明
Current	当期
Prior1	1期前
Prior2	2期前
Prior3	3期前
Prior4	4期前
Quarter	四半期
3. 解析モード
halfモード

四半期報告書が存在する場合

file1 = 四半期報告書
file2 = 最新有価証券報告書
file3 = 過去有価証券報告書

取得データ

YTD
Quarter
Prior
fullモード

四半期報告書が存在しない場合

file1 = 最新有価証券報告書
file2 = 過去有価証券報告書
file3 = さらに過去
Excel出力

Excelテンプレートの NamedRange に書き込み。

例

NetSales_Current
OperatingIncome_Prior2
TotalAssets_Prior4
TotalNumber_Quarter
数値単位処理

XBRL unitRef を解析し自動変換

XBRL単位	Excel出力
JPY	百万円
shares	千株

例

331129000000 → 331129
63664400 → 63664

四捨五入で変換。

rawデータ

すべての取得データを

raw_edinet

シートへ出力。

用途

デバッグ

データ検証

XBRL解析確認

プロジェクト構造
src/
 └ edinet_tool

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
処理フロー
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
ログ例
[excel write] ranges=87
[raw] written rows=121
[loop summary] slot=1 code=2206
開発環境
Python 3.x
VSCode
Git

推奨拡張

Python
Pylance
GitHub Copilot
GitHub運用

基本フロー

git add .
git commit -m "update"
git push

ChatGPTへ相談する際は

GitHub URL
ログ
問題点

を提示。

今後の開発予定

第１優先：EXCELファイルリネーム
第２優先：XBRL読み込み方法の見直し
第３優先：IFRS対応
第４優先：1~50社一括処理
第５優先：高速化
第６優先：XBRL解析キャッシュ

目的

10〜50倍高速化
一括処理
1〜50社

同時処理。

開発者

EDINET決算分析ツール
Python自動化プロジェクト