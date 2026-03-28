AI_README — EDINET XBRL 決算自動分析システム
目的

EDINETのXBRLを解析し
決算分析Excelテンプレートへ自動入力するPythonバッチシステム。

対応機能

EDINET ZIP投入

XBRL自動抽出

半期あり / 半期なし自動判定

5年分データ取得

Excelテンプレート自動入力

株価取得

複数社一括処理

並列処理（ProcessPool）

現在
50社運用可能な基礎システム完成

回答ルール（重要）

ユーザーはPython初心者のため

回答は 必ず修正指示のみ

解説不要

修正指示は次の形式

○行目〜○行目を置き換え

または

関数○○を丸ごと置き換え

必要な情報があれば質問すること。

現在の処理フロー
ZIP投入
↓
ZIPからXBRL抽出
↓
company_jobs生成
↓
ProcessPool並列処理
↓
company_runner_worker
↓
company_runner
↓
loop_processor
↓
parse_service
↓
excel_service
↓
stock_service
↓
Excel出力
↓
batch_summary.csv生成
入力
data/input/zip

EDINETからダウンロードしたZIP

例

121_Xbrl_Search_xxxx.zip
122ki_half_Xbrl_Search_xxxx.zip
出力
data/output/{timestamp}/

excel/
reports/

company_jobs.csv
batch_summary.csv
failed_jobs.csv
現在のフォルダ構造
src/

main.py

edinet_tool/

 cli/

 config/

 domain/

 logging_utils/

 services/

  batch_input_service.py
  company_runner.py
  company_runner_worker.py
  excel_service.py
  file_indexer.py
  loop_builder.py
  loop_processor.py
  parse_cache.py
  parse_service.py
  raw_service.py
  stock_service.py
  stock_write_service.py
  summary_service.py
  workbook_service.py
  xbrl_parser.py
  xbrl_zip_reader.py
  zip_loader.py

runtime.py
並列処理
ProcessPoolExecutor

worker

company_runner_worker.py

並列設定

runtime.py
use_process_pool = True
max_workers = auto

CPU数から自動決定

min(8, cpu_count-1)
パフォーマンス

現在

4社
約14秒

推定

50社
約4〜5分
parse_cache

XBRL解析キャッシュ

XbrlParseCache

現在

max_items = 16
解決済み問題
1 ZIP削除エラー
PermissionError
_zip_extracted

原因

ZipFileハンドル残存

解決

with ZipFile()
2 並列処理 Pathエラー
TypeError
unsupported operand type(s) for /

原因

ProcessPoolでPathがstr化

解決

template_dir = Path(template_dir)
output_root = Path(output_root)
3 workerログ爆発

解決

SilentLogger
現在のログ
[process pool] workers=6
[company start]
[batch summary]

のみ

出力確認
batch_summary.csv

例

slot company_code company_name status stock_status
1 2206 success success
2 4613 success success
3 6857 success success
4 7203 success success
Git運用

ユーザーは以下で管理

git add .
git commit -m "message"
git push
次の開発フェーズ

高速化

予定

① XBRLストリーム解析

現在

lxml parse

予定

iterparse

効果

10〜20倍高速化
② ZIPメモリ展開

現在

ZIP → disk展開

予定

ZIP → memory
③ Excel書き込み最適化

現在

セル単位書き込み

予定

range batch write
最終目標
50社
30〜40秒処理
AIへの依頼

このシステムを

10〜50倍高速化

する設計を提示してほしい。

ただし

回答は

修正指示のみ

でお願いします。