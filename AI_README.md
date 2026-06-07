# AI_README.md

最終更新: 2026-06-07

このファイルは、人間向けの通常READMEではなく、将来のAIまたは作業者がこの環境を復旧し、`edinet_pipeline` / `edinet_monitor` / DB / J-Quants / Excel出力 / 運用ルールを短時間で理解して、安全に調査・改修を再開するためのランブックです。

バックアップ対象:

- `C:\Users\silve\EDINET_Pipeline`
- `E:\EDINET_Data`

重要: APIキー、Windows環境変数、J-Quants公式CLIのログイン資格情報、`.venv` の完全な実行環境、`D:\作業用` の一時Excel、`D:\EDINET_Backup`、`C:\Users\silve\.codex` は上記バックアップだけでは復元できない可能性があります。復旧時は「バックアップ対象外」と「復旧後チェックリスト」を先に確認してください。

---

## 1. 全体像

このプロジェクトは、EDINETとJ-Quantsから取得した企業データをSQLite DBへ保存し、正規化・派生指標計算・Excel出力・監査・スクリーニングを行うPythonプロジェクトです。

主な役割:

- `edinet_monitor`: 現在の中心。DBファーストの取得、保存、正規化、派生指標、J-Quants、Excel出力、Excel監査、Golden Master比較、daily review、スクリーニングを担当。
- `edinet_pipeline`: 旧来または補助的なExcel/XBRL処理系。XBRL解析、タグ別名、テンプレートExcel出力、DBから分析ワークブックを作る処理などを持つ。

基本データフロー:

```text
EDINET API
  -> document list / filing ZIP
  -> XBRL展開
  -> raw_facts
  -> normalized_metrics
  -> derived_metrics / quarter_standalone_metrics / segment_metrics
  -> market_derived_metrics / industry_aggregate_metrics
  -> Excel出力 / Excel監査 / screening

J-Quants API
  -> jquants_statement_raw / jquants_daily_quotes / jquants_listed_info_raw
  -> jquants_financial_metrics
  -> market_derived_metrics
  -> Excel出力 / data_quality_report / daily review
```

運用の原則:

- DB本体は正の台帳です。ExcelはDBからの出力結果であり、Excelだけで原因確定しない。
- ユーザーが明示しない限り、本番DBの更新、再計算、削除は行わない。
- DB反映が必要な作業は、先に `db_reflection_items` へ pending として登録し、目的・変更内容・必要コマンド・確認SQL・関連migrationを残す。

---

## 2. 重要パス

リポジトリ:

- `C:\Users\silve\EDINET_Pipeline`
- 主要設定: `src\edinet_monitor\config\settings.py`

主DB:

- `E:\EDINET_Data\edinet_monitor\db\edinet_monitor.db`
- DB root上書き: `EDINET_MONITOR_DB_ROOT`
- DBバックアップroot既定値: `D:\EDINET_Backup`

edinet_monitor保存領域:

- root: `E:\EDINET_Data\edinet_monitor`
- ZIP: `E:\EDINET_Data\edinet_monitor\raw\zip`
- XBRL: `E:\EDINET_Data\edinet_monitor\raw\xbrl`
- manifest: `E:\EDINET_Data\edinet_monitor\raw\manifests`
- monitor logs: `E:\EDINET_Data\edinet_monitor\logs`
- J-Quants保存領域: `E:\EDINET_Data\edinet_monitor\jquants`
- storage root上書き: `EDINET_MONITOR_STORAGE_ROOT`
- operation logs既定値: `C:\Users\silve\EDINET_Pipeline\logs\operation`
- operation logs上書き: `EDINET_OPERATION_LOG_ROOT`

マスタ・設定:

- 東証銘柄マスタ: `E:\EDINET_Data\master\tse_issuer_master_latest.csv`
- 東証銘柄マスタ上書き: `EDINET_TSE_MASTER_CSV`
- Excel抽出条件: `config\excel\DB抽出条件.xlsx`
- Excel監査代表銘柄: `config\excel\metric_excel_audit_targets.json`
- Golden Master: `config\excel\golden_master\`
- Golden Master基準Excel:
  - `config\excel\golden_master\normal_audit_set.xlsx`
  - `config\excel\golden_master\known_issue_audit_set.xlsx`
- Golden Master正規化JSON:
  - `config\excel\golden_master\normal_audit_set.normalized.json`
  - `config\excel\golden_master\known_issue_audit_set.normalized.json`

J-Quants:

- API base既定値: `https://api.jquants.com/v2`
- API key環境変数: `JQUANTS_API_KEY`
- 公式CLI候補: `tools\jquants-cli\jquants.exe`
- J-Quants Skill: `C:\Users\silve\.codex\skills\jquants-cli-usage\SKILL.md`

運用メモ:

- `docs\troubleshooting.md`
- このファイル: `AI_README.md`

---

## 3. 復旧手順

SSD故障などから復旧する場合は、この順番で確認します。

1. リポジトリとデータを復元する。

```powershell
Test-Path "C:\Users\silve\EDINET_Pipeline"
Test-Path "E:\EDINET_Data\edinet_monitor\db\edinet_monitor.db"
```

2. Python仮想環境を作り直す。`.venv` はバックアップされていても、復旧先では作り直す方が安全です。

```powershell
cd C:\Users\silve\EDINET_Pipeline
py -m venv .venv
.\.venv\Scripts\python.exe -m pip install --upgrade pip
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

3. PowerShellで `PYTHONPATH` を設定する。

```powershell
$env:PYTHONPATH='C:\Users\silve\EDINET_Pipeline\src'
```

4. 必要なWindows環境変数を再設定する。キー本体はこのファイルやGit管理ファイルへ書かない。

- `EDINET_API_KEY`
- `JQUANTS_API_KEY`
- 必要に応じて `EDINET_MONITOR_DB_ROOT`
- 必要に応じて `EDINET_MONITOR_STORAGE_ROOT`
- 必要に応じて `EDINET_OPERATION_LOG_ROOT`
- 必要に応じて `JQUANTS_API_BASE_URL`
- 必要に応じて `JQUANTS_STORAGE_ROOT`

5. DB疎通を読み取り中心で確認する。

```powershell
.\.venv\Scripts\python.exe -c "from edinet_monitor.config.settings import DB_PATH; print(DB_PATH)"
.\.venv\Scripts\python.exe -c "import sqlite3; from edinet_monitor.config.settings import DB_PATH; c=sqlite3.connect(DB_PATH); print(c.execute('select count(*) from sqlite_master').fetchone()[0]); c.close()"
```

6. schema migration台帳を確認する。最初は `--dry-run` だけでよい。

```powershell
.\.venv\Scripts\python.exe -m edinet_monitor.cli.apply_schema_migrations --dry-run --all
```

7. DB反映待ちを確認する。

```powershell
.\.venv\Scripts\python.exe -m edinet_monitor.cli.db_reflection_items list
```

8. 最新のDB品質レポートとdaily reviewを確認する。daily reviewはDBを読み取り専用で開く。

```powershell
.\.venv\Scripts\python.exe -m edinet_monitor.cli.daily_review `
  --normal-excel "D:\作業用\出力_通常監査セット.xlsx" `
  --known-issue-excel "D:\作業用\出力_既知異常検知セット.xlsx"
```

9. 代表テストを実行する。

```powershell
$env:PYTHONPATH='C:\Users\silve\EDINET_Pipeline\src'
.\.venv\Scripts\python.exe -m unittest `
  tests.edinet_monitor.test_derived_metric_service `
  tests.edinet_monitor.test_quarter_standalone_metric_service `
  tests.edinet_monitor.test_market_derived_metric_service `
  tests.edinet_monitor.test_segment_scope_and_path_service `
  tests.edinet_monitor.test_metric_excel_export_service `
  tests.edinet_monitor.test_metric_excel_audit_service `
  tests.edinet_monitor.test_metric_excel_golden_master_service `
  tests.edinet_monitor.test_daily_review_service `
  tests.edinet_monitor.test_jquants_services `
  tests.edinet_monitor.test_jquants_audit_services `
  tests.edinet_monitor.test_jquants_official_cli_compare_service `
  tests.edinet_pipeline.test_tag_alias `
  tests.edinet_pipeline.test_db_excel_export_service
```

---

## 4. バックアップ対象外

次のものは、`C:\Users\silve\EDINET_Pipeline` と `E:\EDINET_Data` のバックアップだけでは漏れやすいです。

- Windowsユーザー環境変数。
- EDINET APIキー。
- J-Quants APIキー。
- J-Quants公式CLIの資格情報。例: `%USERPROFILE%\.config\jquants\credentials.json`。
- `C:\Users\silve\.codex\skills` とCodexプラグイン/スキル本体。
- `.venv` の完全な実行環境。
- `D:\作業用` の一時Excel、比較用ファイル、手動メモ。
- `D:\EDINET_Backup`。
- `EDINET_OPERATION_LOG_ROOT` を外部パスに変えている場合のoperation log。
- ブラウザログイン状態や外部ツールの個人設定。

SQLite DBをコピーする時は、処理中のDB更新を止めてからコピーします。DB稼働中のコピーでは、`edinet_monitor.db-wal` / `edinet_monitor.db-shm` が存在する場合に整合性を崩す可能性があります。安全に取るならSQLiteの `.backup` 相当の方法か、パイプライン停止後のファイルコピーを使います。

---

## 5. DB構造

主要テーブル:

- `schema_migrations`: DB構造の適用済み台帳。適用済み行は削除しない。
- `db_reflection_items`: DB反映待ちキュー。反映完了後は `complete` で削除する。
- `data_quality_report_runs`, `data_quality_report_items`: DB品質レポート履歴。最新20件だけ保持する。
- `issuer_master`: 銘柄・会社マスタ。
- `filings`: EDINET提出書類一覧。
- `raw_facts`: XBRLから抽出した生データ。
- `normalized_metrics`: EDINET由来の正規化済み指標。
- `derived_metrics`: EDINET由来の派生指標。
- `quarter_standalone_metrics`: 四半期単独値、四半期単独成長率。
- `segment_metrics`: セグメント、地域別などの部門別指標。
- `industry_aggregate_metrics`: 業種集計。
- `jquants_statement_raw`: J-Quants財務サマリーraw JSON。
- `jquants_financial_metrics`: J-Quants財務値を正規化した指標。
- `jquants_daily_quotes`: J-Quants株価日次raw JSONと正規化値。
- `jquants_listed_info_raw`: J-Quants上場銘柄マスタraw。
- `jquants_ingest_runs`, `jquants_ingest_progress`: J-Quants取得履歴。
- `market_derived_metrics`: 株価、時価総額、PBR、PER、株価上昇率などの市場由来派生指標。
- `pipeline_runs`, `pipeline_run_chunks`: 日次パイプライン実行履歴。
- `pipeline_performance_runs`, `pipeline_performance_spans`: CLI性能ログ。
- `screening_runs`, `screening_results`: スクリーニング結果。

主要ビュー:

- `active_latest_jquants_metrics`: J-Quants財務指標について、Excel採用ロジックと同じく最新開示1件を返すビュー。最新が `missing` でも古い値には戻らない。

DB構造定義:

- `src\edinet_monitor\db\schema.py`: 通常テーブル、インデックス、ビュー作成。
- `src\edinet_monitor\db\migrations.py`: 軽量migration台帳。
- 現在コード上のmigration:
  - `001_baseline_current_schema`
  - `002_add_data_quality_report_tables`
  - `003_add_db_reflection_items`
  - `004_add_pipeline_performance_logs`

---

## 6. DB運用ルール

DB更新を行う場合は、原則として以下を満たします。

- 対象テーブル、対象銘柄、対象期間、対象form codeを明記する。
- dry-runがあるCLIでは先にdry-runする。
- 更新後に件数確認または代表銘柄確認を行う。
- market系指標は、EDINET/J-Quants財務値を入れ直した後に再計算する。
- ユーザーが「DBへ反映」「再計算を実行」などを明示するまでは、本番DBを更新しない。

`schema_migrations`:

- DB構造の反映済み状態を見る台帳。
- 適用済み行は削除しない。
- 確認は `apply_schema_migrations --dry-run --all` を使う。
- 未適用migrationがある場合だけ、ユーザー確認後にmigration適用を検討する。

`db_reflection_items`:

- DB反映待ちを見るキュー。
- DBスキーマ変更がなくても、既存DB値の再計算・再取得・再構築が必要なら pending として登録する。
- 登録内容には、目的、変更内容、必要な反映コマンド、確認SQL、関連migrationがあれば含める。
- 反映完了後は `complete` で削除する。完了前に勝手に削除しない。

代表コマンド:

```powershell
.\.venv\Scripts\python.exe -m edinet_monitor.cli.db_reflection_items list
.\.venv\Scripts\python.exe -m edinet_monitor.cli.db_reflection_items show --item-id ITEM_ID
.\.venv\Scripts\python.exe -m edinet_monitor.cli.db_reflection_items complete --item-id ITEM_ID
```

`data_quality_report`:

- DB品質、欠損、異常値、J-Quants品質、前回差分を見るレポート。
- DBへrun/itemを保存し、履歴は最新20件だけ保持する。
- レポート作成はDBに履歴を書き込むため、実行前に目的を確認する。

`daily_review`:

- schema migration、DB反映待ち、最新data quality、Excel監査、Golden Master差分をまとめる読み取りレビュー。
- DB接続は `mode=ro` + `PRAGMA query_only = ON`。
- pipeline failure policyは現在 `report_only`。critical/warningがあってもdaily pipelineを失敗扱いにはしない。
- 出力先既定値は `logs\operation\daily_review\`、保持件数は20件。

---

## 7. 指標計算ルール

全般:

- EDINET値は `raw_facts -> normalized_metrics -> derived_metrics` の順に確認する。
- Excel出力の期待行は `build_metric_excel_rows()` を基準にする。監査側で単位変換、IFRS表示、J-Quants四半期処理、rank/平均/中央値を再実装しない。
- Excelの値セルが空欄の場合は、監査では値不一致として扱わない。ただし行そのものがない、単位・分類が欠ける、通期/半期/四半期が混ざる場合はissueにする。

成長率:

- 1Qから4Qの前期比成長率は、同一doc内の `Prior1` よりも前年度docの `Current` を優先参照する。
- J-Quants 1Q/3QのExcel表示は、当期同Q累計を前年度同Q累計で割って計算する。
- 入力値が1つでも `MISSING` または欠損なら、Excel表示は空欄でよい。

EPS/BPS/1株指標:

- 独自EPS: `推定純利益(経常利益 * 0.7) / 発行株数`。
- 独自BPS: `純資産 / 発行株数`。
- 1株資産: `総資産 / 発行済株式数`。
- 1株負債: `(総資産 - 純資産) / 発行済株式数`。
- `AssetsPerShare` / `LiabilitiesPerShare` は通期だけでなく2Qでも計算対象。
- J-Quants actualの公式EPS/BPSは検算用指標として扱えるが、予想EPSは保存しない方針。
- FEPS, FEPS2Q, NxFEPS, NxFEPS2Q などの予想EPSは保存対象にしない。

自己資本比率:

- 直接タグ値が `58.3` のような百分率の場合は `0.583` に補正する。
- 補正後も `abs(value) > 1` の値は異常値としてnormalized採用しない。
- 異常な直接タグを除外した後は、既存の派生計算で `純資産 / 総資産` にフォールバックする。

ProfitBeforeTax:

- IFRS/US GAAPは税引前利益タグを優先する。
- J-GAAPの1Q/3Qで `ProfitBeforeTax` がなく `OrdinaryIncome` がある場合のみ、`OrdinaryIncome` を税引前利益相当として四半期単独に出す。
- 直接 `ProfitBeforeTax` がある場合は直接値を優先する。

market_derived_metrics:

- 時価総額、PBR、PER、株価、株価上昇率などは `market_derived_metrics` に保存する。
- J-Quants財務値やEDINET派生値を再投入した後は、別途 `save_market_derived_metrics --apply` で再計算する。
- 旧2Q書類 `043000` と新2Q書類 `043A00` は、どちらも `period_scope='quarter'`, `quarter_type='2Q'` として扱う。

Excel表示単位:

- 時価総額は「億円」表示。千万の位は四捨五入する。
- 発行株数は「千株」表示。百の位は四捨五入する。

削除済み・非表示として扱う2Q指標:

- `2Q 株価上昇率(５年)`
- `2Q 株価上昇率(10年)`
- `2Q 従業員数`
- `2Q 平均年齢`
- `2Q 平均年間給与`

---

## 8. EDINET取得・正規化の考え方

EDINET側は、XBRLタグを単純に1対1で採用しない。タグ候補、業種、連結/非連結、期間、構造、手動優先度、会計基準を使って正規化する。

対象form code:

- `030000`: 有価証券報告書。
- `043A00`: 新様式の半期報告書。
- `043000`: 旧様式の2Q/半期報告書。

重要ファイル:

- `src\edinet_monitor\services\collector\document_filter_service.py`
- `src\edinet_monitor\services\normalizer\metric_normalize_service.py`
- `src\edinet_monitor\services\normalizer\metric_catalog.py`
- `src\edinet_monitor\services\normalizer\structure_classifier.py`
- `src\edinet_monitor\services\derived_metrics\derived_metric_service.py`
- `src\edinet_monitor\services\quarter_standalone_metric_service.py`
- `src\edinet_monitor\services\segment_metric_service.py`
- `src\edinet_monitor\services\segment_scope_service.py`
- `src\edinet_pipeline\domain\tag_alias.py`

有報やXBRLとDB/Excelが違う疑いがある場合は、この順に切り分ける。

1. 有報PDFまたはXBRL上の該当箇所。
2. `raw_facts`
3. `normalized_metrics`
4. `derived_metrics` / `quarter_standalone_metrics` / `segment_metrics` / `market_derived_metrics`
5. Excel出力ロジック
6. Excel監査またはGolden Master差分

タグ別名の注意:

- `tag_alias.py` は、営業収益、売上高、IFRS/US GAAP表記など、表記が違うが同じ指標として扱う候補を判断する時に確認する。
- キヤノンなどUS GAAP銘柄では、`ProfitLossBeforeTaxUSGAAPSummaryOfBusinessResults`、`NetIncomeLossAttributableToOwnersOfParentUSGAAPSummaryOfBusinessResults`、`TotalAssetsUSGAAPSummaryOfBusinessResults` をそれぞれ税引前利益、純利益、総資産系に解決する。

---

## 9. J-Quants運用ルール

現在の運用前提:

- 利用プランは Standard として扱う。
- DBへ取り込む主対象は `fins.summary` と `eq.daily`。
- `/v2/fins/details` は使わない。Standard対象外として扱い、DB反映・監査・daily運用に組み込まない。
- `compare_jquants_official_cli` の引数には `fins.details` が残っているが、サービス側でPremium-onlyとしてエラーにする。Standard運用では使わない。

この方針の確認点:

- 公式J-Quants API clientのREADMEでは、`get_fin_summary` と株価日足はFree以上、Standard以上は空売り・信用・デリバティブ系、`get_fin_details` はPremium以上に分類されている。
- ローカル実装 `src\edinet_monitor\services\jquants_official_cli_compare_service.py` は `SUPPORTED_ENDPOINTS = {"fins.summary", "eq.daily"}` で、`fins.details` を明示的に拒否する。

公式CLIでの切り分け:

```powershell
jquants --output json fins summary --date 2026-05-07
jquants --output json eq daily --date 2026-05-07 --code 72030
```

DB raw値との比較:

```powershell
.\.venv\Scripts\python.exe -m edinet_monitor.cli.compare_jquants_official_cli `
  --endpoint fins.summary `
  --date 2026-05-07 `
  --output-dir "logs\operation\jquants_compare"

.\.venv\Scripts\python.exe -m edinet_monitor.cli.compare_jquants_official_cli `
  --endpoint eq.daily `
  --date 2026-05-07 `
  --code 72030 `
  --output-dir "logs\operation\jquants_compare"
```

切り分け基準:

- 公式CLIも失敗する: APIキー、プラン、提供期間、J-Quants側仕様を疑う。
- 公式CLIは成功し、DB rawが違う: 取得・保存処理を疑う。
- DB rawは一致し、metricが違う: mapperまたは派生計算を疑う。
- DB値は一致し、Excelだけ違う: Excel出力ロジックを疑う。

仕様変更時:

```powershell
jquants schema
jquants schema fins.summary
jquants schema eq.daily
```

J-Quants秘密情報:

- `JQUANTS_API_KEY` の値はGit管理ファイル、ログ、READMEへ書かない。
- 公式CLIのログイン資格情報もバックアップ対象外になりやすい。復旧後に再ログインまたは環境変数設定を行う。

---

## 10. Excel出力・監査・daily review

Excel出力:

```powershell
.\.venv\Scripts\python.exe -m edinet_monitor.cli.export_metric_excel `
  --condition-xlsx "D:\作業用\条件.xlsx" `
  --output-dir "D:\作業用"
```

Excel監査:

- 既存Excelを読み、DB由来の期待行と突合する。
- 代表銘柄セットは `normal` / `known_issue` / `all`。
- ExcelのSummaryから `periods` と `segment_mode` を読み、期待値生成へ反映する。
- DB接続は読み取り専用。
- 値セルが空欄の場合は値不一致にしない。
- 行欠落、単位不一致、分類不一致、通期/半期/四半期混入、非空値の不一致をissueにする。

```powershell
.\.venv\Scripts\python.exe -m edinet_monitor.cli.audit_metric_excel `
  --excel-path "D:\作業用\出力_通常監査セット.xlsx" `
  --target-set normal

.\.venv\Scripts\python.exe -m edinet_monitor.cli.audit_metric_excel `
  --excel-path "D:\作業用\出力_既知異常検知セット.xlsx" `
  --target-set known_issue
```

Golden Master:

- 基準Excelは `config\excel\golden_master\` に保存する。
- 比較はExcelバイナリではなく、正規化JSON同士で行う。
- `generated_at`、空欄値、スタイル差分などは比較対象外。
- 基準更新は、不具合でない差分だと確認してから行う。

```powershell
.\.venv\Scripts\python.exe -m edinet_monitor.cli.normalize_metric_excel_golden_master `
  --excel-path "config\excel\golden_master\normal_audit_set.xlsx" `
  --output-json "config\excel\golden_master\normal_audit_set.normalized.json"

.\.venv\Scripts\python.exe -m edinet_monitor.cli.compare_metric_excel_golden_master `
  --golden-json "config\excel\golden_master\normal_audit_set.normalized.json" `
  --actual-excel "D:\作業用\出力_通常監査セット.xlsx"
```

daily review:

- schema migration、DB反映待ち、最新data quality、Excel監査、Golden Master差分を1つにまとめる。
- 最初はpipelineを失敗扱いにしない `report_only` 運用。
- 出力先は `logs\operation\daily_review\`。
- JSONとXLSXを出し、最新20件程度を保持する。

```powershell
.\.venv\Scripts\python.exe -m edinet_monitor.cli.daily_review `
  --normal-excel "D:\作業用\出力_通常監査セット.xlsx" `
  --known-issue-excel "D:\作業用\出力_既知異常検知セット.xlsx"
```

---

## 11. 代表CLI

日次パイプライン:

```powershell
.\.venv\Scripts\python.exe -m edinet_monitor.cli.run_daily_pipeline --target-date YYYY-MM-DD
```

EDINET ZIPバックフィル:

```powershell
.\.venv\Scripts\python.exe -m edinet_monitor.cli.run_zip_backfill `
  --date-from YYYY-MM-DD `
  --date-to YYYY-MM-DD `
  --download-profile auto `
  --download-run-all
```

J-Quants財務・株価バックフィル:

```powershell
.\.venv\Scripts\python.exe -m edinet_monitor.cli.run_jquants_backfill `
  --date-from YYYY-MM-DD `
  --date-to YYYY-MM-DD `
  --codes all `
  --statement-periods FY,1Q,2Q,3Q `
  --output-dir "logs\operation\jquants_backfill"
```

J-Quants rawからmetrics再構築:

```powershell
.\.venv\Scripts\python.exe -m edinet_monitor.cli.rebuild_jquants_metrics_from_raw `
  --date-from YYYY-MM-DD `
  --date-to YYYY-MM-DD `
  --periods FY,1Q,2Q,3Q `
  --codes all `
  --apply `
  --output-dir "logs\operation\db_reflection"
```

EDINET normalized/derived再構築:

```powershell
.\.venv\Scripts\python.exe -m edinet_monitor.cli.rebuild_metrics_for_scope `
  --security-code 7203 `
  --form-codes 030000,043A00,043000 `
  --limit 0
```

market_derived_metrics再計算:

```powershell
.\.venv\Scripts\python.exe -m edinet_monitor.cli.save_market_derived_metrics `
  --codes all `
  --period-scopes all `
  --apply `
  --output-dir "logs\operation\db_reflection"
```

セグメント再構築:

```powershell
.\.venv\Scripts\python.exe -m edinet_monitor.cli.rebuild_segment_raw_facts `
  --codes 4613,6758,6857,7203,7751,8001,9983 `
  --form-codes 030000,043A00,043000 `
  --period-ranks recent3 `
  --force-extract `
  --apply `
  --output-dir "logs\operation\db_reflection"

.\.venv\Scripts\python.exe -m edinet_monitor.cli.save_segment_metrics `
  --codes 4613,6758,6857,7203,7751,8001,9983 `
  --form-codes 030000,043A00,043000 `
  --period-ranks recent3 `
  --apply `
  --output-dir "logs\operation\db_reflection"
```

四半期単独指標保存:

```powershell
.\.venv\Scripts\python.exe -m edinet_monitor.cli.save_quarter_standalone_metrics `
  --codes 4613,6758,6857,7203,7751,8001,9983 `
  --apply `
  --output-dir "logs\operation\db_reflection"
```

data quality report:

```powershell
.\.venv\Scripts\python.exe -m edinet_monitor.cli.data_quality_report `
  --date-from YYYY-MM-DD `
  --date-to YYYY-MM-DD `
  --codes all `
  --output-dir "logs\operation\data_quality"
```

J-Quants品質監査:

```powershell
.\.venv\Scripts\python.exe -m edinet_monitor.cli.audit_jquants_quality `
  --date-from YYYY-MM-DD `
  --date-to YYYY-MM-DD `
  --codes all `
  --output-dir "logs\operation\jquants_quality"
```

---

## 12. テスト

代表的な回帰テスト:

```powershell
$env:PYTHONPATH='C:\Users\silve\EDINET_Pipeline\src'
.\.venv\Scripts\python.exe -m unittest `
  tests.edinet_monitor.test_derived_metric_service `
  tests.edinet_monitor.test_quarter_standalone_metric_service `
  tests.edinet_monitor.test_market_derived_metric_service `
  tests.edinet_monitor.test_segment_scope_and_path_service `
  tests.edinet_monitor.test_metric_excel_export_service `
  tests.edinet_monitor.test_metric_excel_audit_service `
  tests.edinet_monitor.test_metric_excel_golden_master_service `
  tests.edinet_monitor.test_daily_review_service `
  tests.edinet_monitor.test_data_quality_report_service `
  tests.edinet_monitor.test_jquants_services `
  tests.edinet_monitor.test_jquants_audit_services `
  tests.edinet_monitor.test_jquants_official_cli_compare_service `
  tests.edinet_pipeline.test_tag_alias `
  tests.edinet_pipeline.test_db_excel_export_service
```

編集後の軽い確認:

```powershell
git diff --check
```

全体確認が必要な場合:

```powershell
.\.venv\Scripts\python.exe -m unittest discover tests
```

---

## 13. 直近の修正状況・DB反映状況

2026-06-07時点で把握している重要変更:

- Excel監査CLI `audit_metric_excel` を追加済み。
- 代表銘柄セット `config\excel\metric_excel_audit_targets.json` を追加済み。
- Golden Master正規化CLIと差分比較CLIを追加済み。
- daily review CLI `daily_review` を追加済み。現時点では `report_only` で、pipeline失敗扱いにはしない。
- data quality report履歴はDB保存し、最新20件だけ保持する。
- `db_reflection_items` キューをDB内に追加済み。旧 `D:\作業用\DB反映まち` テキストファイル運用は最新の正ではない。

Excel不具合対応の直近修正:

- 自己資本比率の直接タグ値について、百分率補正と異常値除外を追加。
- 異常な自己資本比率は `純資産 / 総資産` フォールバックに回す。
- キヤノンなどUS GAAPの税引前利益、純利益、総資産タグを `tag_alias.py` に追加。
- `2Q 1株資産` / `2Q 1株負債` を半期でも計算対象に追加。
- 旧2Q書類 `043000` でも2Q株価を生成するように追加。
- セグメント対象期間指定 `recent3` を追加し、最新、1期前、2期前を対象にできるようにした。
- J-GAAPの1Q/3Qで `ProfitBeforeTax` がない場合、`OrdinaryIncome` を税引前利益相当として四半期単独に出す。

DB反映状況:

- 上記のうちDB再計算が必要なものは、`db_reflection_items` に pending 登録してから実行する運用。
- 直近作業では、優先対策1-3用と優先対策4-6用のDB再計算項目を別々に登録した。復旧後は `db_reflection_items list` で残っているか確認する。
- DBスキーマ変更が不要な修正でも、既存DB値の再計算が必要ならExcelにはすぐ反映されない。対象のpending itemを確認し、ユーザー承認後に必要コマンドを実行する。

確認SQLの考え方:

- 自己資本比率は `ABS(value_num) > 1` の残存を確認する。
- キヤノンUS GAAPは該当docの `normalized_metrics` に税引前利益、純利益、総資産が入るか確認する。
- 2Q 1株指標は `derived_metrics` の `period_scope='quarter'`、`quarter_type='2Q'` で確認する。
- 2Q株価は `market_derived_metrics` と `filings.form_type IN ('043A00','043000')` で確認する。
- 2Qセグメントは `segment_metrics` の `period_scope='quarter'`、`quarter_type='2Q'` で確認する。
- 1Q/3Q税引前利益相当は `quarter_standalone_metrics.metric_base='ProfitBeforeTax'` で確認する。

---

## 14. AI作業ルール

AIがこのプロジェクトを触る時のルール:

- ユーザーが明示しない限り、DB本体を更新しない。
- 実装とDB反映を分ける。実装後にDB再計算が必要なら、`db_reflection_items` に pending として登録する。
- `schema_migrations` の適用済み行は削除しない。
- `db_reflection_items` は反映完了後に `complete` で削除する。完了前に消さない。
- `data_quality_report` は最新20件だけ保存する前提で扱う。
- APIキーや資格情報の値をファイル、ログ、チャットに書かない。
- 既存のユーザー変更を勝手に戻さない。
- Excel不具合は、Excelだけを見て判断せず、DB raw、normalized、derived、export、auditの順で切り分ける。
- J-Quants不具合は、まず公式CLIで同じ日付・同じ銘柄を叩き、DB rawと比較する。
- `/v2/fins/details` を使う提案はしない。Standard運用では対象外。
- EDINET値が有報と違う疑いがある場合、タグ候補、連結/非連結、期間、業種ルール、`tag_alias.py` を確認する。
- テストなしで広範囲のDB再構築や削除を提案しない。
- destructiveなGit操作、DB削除、ファイル一括削除はユーザー確認なしで行わない。

---

## 15. 復旧後チェックリスト

復旧直後は以下を確認する。

- `C:\Users\silve\EDINET_Pipeline` が存在する。
- `E:\EDINET_Data\edinet_monitor\db\edinet_monitor.db` が存在する。
- `requirements.txt` から `.venv` を再作成済み。
- `PYTHONPATH` が `C:\Users\silve\EDINET_Pipeline\src` を指している。
- `EDINET_API_KEY` が設定済み。
- `JQUANTS_API_KEY` が設定済み。
- `apply_schema_migrations --dry-run --all` で未適用migrationの有無を把握した。
- `db_reflection_items list` でDB反映待ちを把握した。
- 最新の `data_quality_report` のcritical/warningを確認した。
- `daily_review` がJSON/XLSXを出力できる。
- `audit_metric_excel` が代表銘柄セットExcelで起動する。
- Golden Master JSONが `config\excel\golden_master\` に存在する。
- `jquants --output json fins summary --date YYYY-MM-DD` が実行できる。
- `compare_jquants_official_cli --endpoint fins.summary --help` 相当が起動する。
- `/v2/fins/details` を使わない方針を再確認した。
- 代表テストが通る。
- `D:\作業用` の一時Excelが必要な作業では、別途ファイルを復元または再出力した。

---

## 16. 最後に

このプロジェクトで一番重要なのは、コードそのものよりも「どの値を正として扱うか」「DBをいつ更新してよいか」「J-QuantsとEDINETをどう切り分けるか」です。

復旧後に迷った場合は、すぐに実装へ進まず、まずこの順で確認してください。

1. `AI_README.md`
2. `docs\troubleshooting.md`
3. `src\edinet_monitor\config\settings.py`
4. `src\edinet_monitor\db\schema.py`
5. `src\edinet_monitor\db\migrations.py`
6. `db_reflection_items list`
7. 関連テスト
