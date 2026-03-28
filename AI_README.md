# AI_README.md

## このファイルの目的
このファイルは、将来 AI にこのプロジェクトの修正や調査を依頼するときに、  
**短時間で全体像・運用ルール・重要仕様・確認ポイントを理解させるための引継ぎファイル**です。

---

# 1. プロジェクト概要

## プロジェクト名
EDINET Pipeline

## 目的
EDINET の XBRL（有価証券報告書・半期報告書）を解析し、  
決算分析テンプレート Excel に自動入力する Python バッチシステム。

## 主な処理
1. ZIP から対象 XBRL を抽出
2. 会社ごとに file1 / file2 / file3 を自動選定
3. XBRL を解析して指標を抽出
4. Excel テンプレートへ NamedRange ベースで書き込み
5. raw_edinet シートへ監査用データを書き込み
6. 株価データを書き込み
7. 実行結果を reports 配下に CSV 出力

---

# 2. 現在の開発・運用場所

## 開発場所
- `C:\Users\silve\EDINET_Pipeline`

## 実データ
- `D:\EDINET_Data`

## バックアップ先
- `D:\EDINET_Backup`

## 入力ZIPフォルダ
- `D:\EDINET_Data\input\zip`

## 出力先
- `D:\EDINET_Data\output\実行日時\`

---

# 3. Git / ブランチ運用ルール

## 現在の基本ルール
- `main` では直接作業しない
- 普段の作業継続元ブランチは `move-to-users-silve`
- 新しい作業は `move-to-users-silve` から新ブランチを切る
- 修正後はコミットして push する

## 重要
AI は**main で直接作業させない前提**で指示すること。

---

# 4. ユーザーへの回答ルール

## 最重要
- 指示のみでよい
- 解説は原則不要
- 必要な情報が欲しければ言う
- 修正指示は初心者向けに具体的に出す
- 「○行目〜○行目を置き換えてください」または「○○関数を丸ごと置き換えてください」の形で指示する
- ファイル名を必ず明示する
- あいまいな指示をしない
- 必要なファイル内容が足りないときは、先にそのファイルを貼ってもらう

## ログ調査ルール
- 通常は `NORMAL`
- 原因調査時だけ `DEBUG`
- **NORMAL ログだけで原因が分からない場合は、DEBUG に切り替えるよう指示する**

---

# 5. 現在のフォルダ構成

```text
EDINET_Pipeline/
├─ docs/
├─ scripts/
│  ├─ backup_checkpoint.bat
│  └─ run_KANPE.bat
├─ src/
│  ├─ edinet_pipeline/
│  │  ├─ cli/
│  │  ├─ config/
│  │  │  ├─ runtime.py
│  │  │  └─ settings.py
│  │  ├─ domain/
│  │  │  ├─ dedupe.py
│  │  │  ├─ filters.py
│  │  │  ├─ output_buffer.py
│  │  │  ├─ raw_builder.py
│  │  │  ├─ run_checks.py
│  │  │  ├─ security_code.py
│  │  │  ├─ skip.py
│  │  │  ├─ tag_alias.py
│  │  │  └─ year_shift.py
│  │  ├─ logging_utils/
│  │  │  └─ logger.py
│  │  └─ services/
│  │     ├─ batch_input_service.py
│  │     ├─ cleanup_service.py
│  │     ├─ company_execution_service.py
│  │     ├─ company_runner.py
│  │     ├─ company_runner_worker.py
│  │     ├─ company_task_result.py
│  │     ├─ excel_service.py
│  │     ├─ file_indexer.py
│  │     ├─ loop_builder.py
│  │     ├─ loop_processor.py
│  │     ├─ main_setup_service.py
│  │     ├─ parse_cache.py
│  │     ├─ parse_service.py
│  │     ├─ raw_service.py
│  │     ├─ stock_service.py
│  │     ├─ stock_write_service.py
│  │     ├─ summary_service.py
│  │     ├─ workbook_service.py
│  │     ├─ xbrl_parser.py
│  │     ├─ xbrl_zip_reader.py
│  │     └─ zip_loader.py
│  └─ main.py
├─ templates/
│  └─ 決算分析シート_1.xlsm
├─ tests/
├─ .gitignore
├─ AI_README.md
├─ print_tree.py
└─ requirements.txt
```

---

# 6. このプログラムの処理の流れ

## 全体フロー
1. `main.py` 実行
2. `main_setup_service.py` で出力先や展開先を準備
3. `batch_input_service.py` で ZIP 内 XBRL を会社ごとに整理
4. `build_all_company_jobs()` で会社単位の job を作る
5. `run_company_jobs()` で会社ごとに処理
6. `loop_processor.py` で 1社分の主処理を実行
7. Excel 書込 / raw_edinet 書込 / 株価書込
8. `summary_service.py` で CSV レポート出力
9. 一時展開フォルダ削除

## 1社分の主処理
`loop_processor.py` の `process_one_loop()` が中心。

中で主に以下を実行する。
- `prepare_workbook()`
- `parse_half_doc()`
- `parse_latest_annual_doc()`
- `parse_old_annual_doc()`
- `finalize_half_buffer()`
- `build_raw_rows_all_docs()`
- `write_data_to_workbook_namedranges()`
- `write_rows_to_raw_sheet_workbook()`
- `write_stock_data_to_workbook()`
- 最終 Excel を `output\...\excel\` へ移動

---

# 7. file1 / file2 / file3 の仕様

## 基本
会社ごとに `file1 / file2 / file3` を自動選定する。

## 半期あり会社
- `file1` = 半期報告書の最新1本
- `file2` = 有価証券報告書の最新1本
- `file3` = 有価証券報告書の次に新しい1本

## 半期なし会社
- `file1` = 有価証券報告書の最新1本
- `file2` = 有価証券報告書の次に新しい1本
- `file3` = 有価証券報告書のその次に新しい1本

## 注意
- 必ずしも「1期前」「2期前」とは限らない
- 入力 ZIP 内の対象 XBRL を新しい順に並べて選ぶ
- 必要本数が足りない会社は job が作られない

## 見分け方
- 半期報告書: `jpcrp040300`
- 有価証券報告書: `jpcrp030000-asr`

---

# 8. 出力物の意味

## Excel
- 出力先: `D:\EDINET_Data\output\実行日時\excel\`
- ファイル名: `証券コード_会社名_期末日.xlsm`

## reports
- `batch_summary.csv`
  - 全会社の結果一覧
- `company_jobs.csv`
  - file1 / file2 / file3 の割当確認
- `failed_jobs.csv`
  - failed / partial_success / skipped のみ

## work
- テンプレートの作業コピーを置く場所
- `_work_` 付きファイル
- 最終成果物ではない

## _zip_extracted
- ZIP 内 XBRL の一時展開先
- 処理後に削除される

## logs
- `run.log`
  - 実行全体の流れ
- `loop_summary.jsonl`
  - 1社ごとの詳細サマリ

---

# 9. raw_edinet シートの意味

## 役割
raw_edinet は、  
**何のタグが採用されたか / 何が missing か / どの期間の値か** を確認するための監査用シート。

## 主な列
- `company_code`
- `doc_id`
- `doc_type`
- `consolidation`
- `metric_key`
- `time_slot`
- `period_start`
- `period_end`
- `period_kind`
- `value`
- `unit`
- `tag_used`
- `tag_rank`
- `status`
- `run_id`
- `source_file`

## 調査の基本
数値が想定どおり入らないときは、まず raw_edinet を見る。

## 注意
- `annual` の `YTD / MISSING` は監査行として残ることがある
- これだけで不具合とは限らない

---

# 10. 重要仕様（修正時に壊してはいけない）

## IFRS営業利益
- IFRS企業で連結営業利益タグが取れない場合でも、個別 `jppfs_cor:OperatingIncome` で補完しない
- 空欄のままを正とする

## SellingExpenses
- すでに SellingExpenses が取れている場合、FinancialBusinessCost を足して上書きしない
- 二重加算は不具合

## annual YTD MISSING
- raw_edinet の annual の YTD MISSING は監査行が中心
- 実害がなければ不具合扱いしない

## Excel書込
- セル番地固定ではなく NamedRange ベース
- テンプレート本体には直接書き込まない
- `work` フォルダの作業コピー経由で書き込む

## 数値変換
- 財務数値は表示単位に応じて変換される
- 株数は 1,000 株単位で丸める

---

# 11. ログ設定

## 設定箇所
- `src/edinet_pipeline/config/settings.py`

## 使い分け
- `LOG_MODE = "NORMAL"`  
  通常運用
- `LOG_MODE = "DEBUG"`  
  原因調査

## AIへの指示ルール
不具合時に NORMAL ログだけで原因が分からない場合は、  
**DEBUG に切り替えるように指示すること。**

---

# 12. 主要ファイルの役割

## `src/main.py`
エントリーポイント。全体実行。

## `src/edinet_pipeline/services/main_setup_service.py`
出力先、展開先、テンプレート場所、parse cache を準備。

## `src/edinet_pipeline/services/batch_input_service.py`
ZIP 内 XBRL の走査、会社単位の job 作成、file1 / file2 / file3 の決定。

## `src/edinet_pipeline/services/loop_processor.py`
1社分の主処理。

## `src/edinet_pipeline/services/parse_service.py`
half / annual の解析制御。

## `src/edinet_pipeline/services/raw_service.py`
raw_edinet 用行データ生成。

## `src/edinet_pipeline/domain/raw_builder.py`
raw_edinet 用列定義、行組立、MISSING 行追加、run_id 付与。

## `src/edinet_pipeline/services/excel_service.py`
NamedRange 書込、raw_edinet シート書込、ファイル名調整。

## `src/edinet_pipeline/services/workbook_service.py`
テンプレートから安全な作業コピー作成。

## `src/edinet_pipeline/services/summary_service.py`
batch_summary.csv / company_jobs.csv / failed_jobs.csv の出力。

---

# 13. 回帰確認用の固定会社

## J-GAAP
- 2206 江崎グリコ株式会社
- 4613 関西ペイント株式会社

## IFRS
- 7203 トヨタ自動車株式会社
- 6857 株式会社アドバンテスト
- 8001 伊藤忠商事株式会社
- 8473 ＳＢＩホールディングス株式会社
- 9983 株式会社ファーストリテイリング

---

# 14. AI が問題調査するときの基本順序

1. `batch_summary.csv` を確認
2. `failed_jobs.csv` を確認
3. `company_jobs.csv` を確認
4. 出力Excelの `raw_edinet` を確認
5. `run.log` を確認
6. それでも原因不明なら `DEBUG` に切り替える
7. 必要なファイル内容をユーザーに貼ってもらう
8. 修正指示は、ファイル名つき・置換単位で具体的に出す

---

# 15. ユーザー環境の前提

## Python
- ユーザーは Python 初心者
- AI の提案コードをそのままコピペして使う前提

## 指示の出し方
以下の形を優先すること。
- `○○.py の ○行目〜○行目を置き換えてください`
- `○○関数を丸ごと置き換えてください`
- `○行目の下に追加してください`

## 避けること
- 抽象的な説明だけで終わること
- 「for文の前あたり」のような曖昧指示
- main で直接作業させること

---

# 16. 現在の到達点

- `C:\Users\silve\EDINET_Pipeline` への移行完了
- `.venv` 再構築完了
- `run_KANPE.bat` 起動確認完了
- GitHub追跡開始完了
- `backup_checkpoint.bat` 作成・実行確認完了
- `AI_README.md` に安全運用ルール追記済み
- `main` 整理完了
- `origin/main` と同期済み
- `docs/初心者向け運用ガイド.md` 作成済み

---

# 17. AI への最後の注意

このプロジェクトで AI がやるべきことは、  
**原因調査 → 必要ファイル取得 → 具体的な修正指示** です。

このプロジェクトで AI がやってはいけないことは、  
**曖昧な説明だけして終わること / main で直接作業させること / DEBUG 切替判断を忘れること** です。