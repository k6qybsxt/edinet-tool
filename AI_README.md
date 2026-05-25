# AI_README.md

最終更新: 2026-05-25

このファイルは、人間向けの通常READMEではなく、将来のAIまたは作業者がこの環境を復旧し、`edinet_pipeline` / `edinet_monitor` / DB運用を安全に再開するためのランブックです。

バックアップ対象:

- `C:\Users\silve\EDINET_Pipeline`
- `E:\EDINET_Data`

重要: APIキー、Windows環境変数、仮想環境 `.venv` の完全な再現、`D:\作業用\DB反映まち`、`D:\EDINET_Backup` は上記バックアップに含まれない可能性があります。復旧時はこのファイルの「復旧手順」と「バックアップ対象外」を先に確認してください。

---

## 1. 全体像

このプロジェクトは、EDINETとJ-Quantsから取得した企業データをSQLite DBに保存し、正規化・派生指標計算・Excel出力・スクリーニングを行うPythonプロジェクトです。

大きく2系統があります。

- `edinet_monitor`: 現在の中心。DBファーストの取得、保存、正規化、派生指標、J-Quants、Excel出力、スクリーニングを担当。
- `edinet_pipeline`: 旧来または補助的なExcel/XBRL処理系。XBRL解析、テンプレートExcel出力、DBから分析ワークブックを作る処理などを持つ。

基本データフロー:

```text
EDINET API
  -> document list
  -> filing ZIP
  -> XBRL展開
  -> raw_facts
  -> normalized_metrics
  -> derived_metrics / quarter_standalone_metrics / segment_metrics
  -> Excel出力 / screening

J-Quants API
  -> jquants_statement_raw / jquants_daily_quotes / jquants_listed_info_raw
  -> jquants_financial_metrics
  -> market_derived_metrics
  -> Excel出力 / audit
```

---

## 2. 重要パス

リポジトリ:

- `C:\Users\silve\EDINET_Pipeline`

主DB:

- `E:\EDINET_Data\edinet_monitor\db\edinet_monitor.db`
- 設定元: `src\edinet_monitor\config\settings.py`
- 環境変数で上書き可能: `EDINET_MONITOR_DB_ROOT`

edinet_monitor保存領域:

- root: `E:\EDINET_Data\edinet_monitor`
- ZIP: `E:\EDINET_Data\edinet_monitor\raw\zip`
- XBRL: `E:\EDINET_Data\edinet_monitor\raw\xbrl`
- manifest: `E:\EDINET_Data\edinet_monitor\raw\manifests`
- logs: `E:\EDINET_Data\edinet_monitor\logs`
- J-Quants保存領域: `E:\EDINET_Data\edinet_monitor\jquants`

マスタ:

- `E:\EDINET_Data\master\tse_issuer_master_latest.csv`
- 設定元: `EDINET_TSE_MASTER_CSV`

反映待ちメモ:

- `D:\作業用\DB反映まち\db_reflection_ready_items_20260430_212147.txt`
- 注意: `C:\Users\silve\EDINET_Pipeline` と `E:\EDINET_Data` だけをバックアップする場合、このファイルは含まれない。

J-Quants公式CLI:

- `C:\Users\silve\EDINET_Pipeline\tools\jquants-cli\jquants.exe`
- Skill: `C:\Users\silve\.codex\skills\jquants-cli-usage\SKILL.md`

運用メモ:

- `docs\troubleshooting.md`

---

## 3. 復旧手順

SSD故障などから復旧する場合は、この順番で確認します。

1. `C:\Users\silve\EDINET_Pipeline` を復元する。
2. `E:\EDINET_Data` を復元する。
3. DBが存在することを確認する。

```powershell
Test-Path "E:\EDINET_Data\edinet_monitor\db\edinet_monitor.db"
```

4. Python仮想環境を作り直す。`.venv` はバックアップされていても、復旧先では作り直す方が安全です。

```powershell
cd C:\Users\silve\EDINET_Pipeline
py -m venv .venv
.\.venv\Scripts\python.exe -m pip install --upgrade pip
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

5. PowerShellで `PYTHONPATH` を設定する。

```powershell
$env:PYTHONPATH='C:\Users\silve\EDINET_Pipeline\src'
```

6. 必要なWindows環境変数を再設定する。キー本体はこのファイルに書かない。

- `EDINET_API_KEY`
- `JQUANTS_API_KEY`
- 必要に応じて `EDINET_MONITOR_DB_ROOT`
- 必要に応じて `EDINET_OPERATION_LOG_ROOT`
- 必要に応じて `JQUANTS_API_BASE_URL`

7. DB疎通を確認する。

```powershell
.\.venv\Scripts\python.exe -c "from edinet_monitor.config.settings import DB_PATH; print(DB_PATH)"
.\.venv\Scripts\python.exe -c "import sqlite3; from edinet_monitor.config.settings import DB_PATH; c=sqlite3.connect(DB_PATH); print(c.execute('select count(*) from sqlite_master').fetchone()[0]); c.close()"
```

8. 代表テストを実行する。

```powershell
$env:PYTHONPATH='C:\Users\silve\EDINET_Pipeline\src'
.\.venv\Scripts\python.exe -m unittest `
  tests.edinet_monitor.test_jquants_services `
  tests.edinet_monitor.test_jquants_audit_services `
  tests.edinet_monitor.test_jquants_official_cli_compare_service `
  tests.edinet_monitor.test_market_derived_metric_service `
  tests.edinet_monitor.test_metric_excel_export_service `
  tests.edinet_pipeline.test_db_excel_export_service
```

---

## 4. バックアップ対象外

次のものは、復旧時に失われやすいです。

- Windowsユーザー環境変数
- EDINET APIキー
- J-Quants APIキー
- `D:\作業用\DB反映まち`
- `D:\EDINET_Backup`
- `.venv` の完全な実行環境
- Codex Skill本体: `C:\Users\silve\.codex\skills`
- PowerShellの現在セッション設定

特に `D:\作業用\DB反映まち\db_reflection_ready_items_20260430_212147.txt` は、DB反映履歴と保留事項を含むため、必要なら別途バックアップ対象に追加してください。

---

## 5. DB構造

主要テーブル:

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
- `market_derived_metrics`: 株価、時価総額、PBR、PER、株価上昇率などの市場由来派生指標。
- `pipeline_runs`, `pipeline_run_chunks`: 日次パイプライン実行履歴。
- `screening_runs`, `screening_results`: スクリーニング結果。

ビュー:

- `active_latest_jquants_metrics`: J-Quants財務指標について、Excel採用ロジックと同じく最新開示1件を返すビュー。最新が `missing` でも古い値には戻らない。

DB初期化:

- `src\edinet_monitor\db\schema.py`
- `create_tables()` がテーブル、インデックス、ビューを作成する。
- 既存DBを破壊する処理ではないが、DB反映前には必ず目的を確認する。

---

## 6. DB運用ルール

DBへの反映前に作業内容が明確でない場合は、まず次のファイルへ追記する。

```text
D:\作業用\DB反映まち\db_reflection_ready_items_20260430_212147.txt
```

DB更新を行う場合は、原則として以下を満たす。

- 対象テーブルと対象期間を明記する。
- dry-runがあるCLIでは先にdry-runする。
- 更新後に件数確認または代表銘柄確認を行う。
- market系指標は、J-Quants財務値を入れ直した後に再計算する。
- ユーザーが「DBへ反映」と明示するまでは、本番DBを更新しない。

本番DB更新に関わる代表CLI:

```powershell
.\.venv\Scripts\python.exe -m edinet_monitor.cli.run_daily_pipeline --target-date YYYY-MM-DD

.\.venv\Scripts\python.exe -m edinet_monitor.cli.rebuild_jquants_metrics_from_raw `
  --date-from YYYY-MM-DD `
  --date-to YYYY-MM-DD `
  --periods FY,1Q,2Q,3Q `
  --codes all `
  --apply

.\.venv\Scripts\python.exe -m edinet_monitor.cli.save_market_derived_metrics `
  --date-from YYYY-MM-DD `
  --date-to YYYY-MM-DD `
  --codes all `
  --period-scopes all `
  --apply
```

---

## 7. 指標計算ルール

成長率:

- 1Qから4Qの前期比成長率は、同一doc内の `Prior1` よりも「前年度docのCurrent値」を優先参照する。
- J-Quants 1Q/3QのExcel表示は、当期同Q累計を前年度同Q累計で割って計算する。
- 入力値が1つでも `MISSING` または欠損なら、Excel表示は空欄でよい。

EPS/BPS:

- 独自EPS: `推定純利益(経常利益 * 0.7) / 発行株数`
- 独自BPS: `純資産 / 発行株数`
- J-Quants actualの公式EPS/BPSは検算用指標として扱えるが、予想EPSは保存しない方針。
- FEPS, FEPS2Q, NxFEPS, NxFEPS2Q などの予想EPSは保存対象にしない。

推定純利益:

- `推定純利益(経常利益*0.7)` を使う。

market_derived_metrics:

- 時価総額、PBR、PER、株価上昇率などは `market_derived_metrics` に保存する。
- J-Quants財務値を再投入した後は、別途 `save_market_derived_metrics --apply` で再計算する。

Excel表示単位:

- 時価総額は「億円」表示。千万の位は四捨五入する。
- 発行株数は「千株」表示。百の位は四捨五入する。

削除済み・削除対象として扱った指標:

- `2Q 株価上昇率(５年)`
- `2Q 株価上昇率(10年)`
- `2Q 従業員数`
- `2Q 平均年齢`
- `2Q 平均年間給与`

---

## 8. EDINET取得・正規化の考え方

EDINET側は、XBRLタグを単純に1対1で採用しない。タグ候補、業種、連結/非連結、期間、構造、手動優先度を使って正規化する。

重要ファイル:

- `src\edinet_monitor\services\normalizer\metric_normalize_service.py`
- `src\edinet_monitor\services\normalizer\metric_catalog.py`
- `src\edinet_monitor\services\normalizer\structure_classifier.py`
- `src\edinet_pipeline\domain\tag_alias.py`

`tag_alias.py` は、営業収益や売上高など、表記が違うが同じ指標として扱う候補を判断する時に確認する。

ExcelやDB値が有報と違う疑いがある場合は、この順に切り分ける。

1. 有報PDFまたはXBRL上の該当箇所。
2. `raw_facts`
3. `normalized_metrics`
4. `derived_metrics` / `quarter_standalone_metrics` / `market_derived_metrics`
5. Excel出力ロジック

---

## 9. J-Quants運用ルール

現在の加入プラン:

- Standard

重要な制約:

- `/v2/fins/details` はStandard対象外として扱う。
- `fins details` のDB反映は行わない。
- `/v2/fins/details` 用に実装した取得・保存・監査コードは削除済み。

J-Quantsで怪しい挙動が出たら、まず公式 `jquants` CLIで同じ条件を叩く。

```powershell
jquants --output json fins summary --date 2026-05-07
jquants --output json eq daily --date 2026-05-07 --code 72030
```

DB raw値との比較は専用CLIを使う。

```powershell
.\.venv\Scripts\python.exe -m edinet_monitor.cli.compare_jquants_official_cli `
  --endpoint fins.summary `
  --date 2026-05-07 `
  --output-dir "D:\作業用\DB反映まち"

.\.venv\Scripts\python.exe -m edinet_monitor.cli.compare_jquants_official_cli `
  --endpoint eq.daily `
  --date 2026-05-07 `
  --code 72030 `
  --output-dir "D:\作業用\DB反映まち"
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

参照:

- `docs\troubleshooting.md`
- `C:\Users\silve\.codex\skills\jquants-cli-usage\SKILL.md`
- `C:\Users\silve\.codex\skills\jquants-cli-usage\references\plans.md`
- `C:\Users\silve\.codex\skills\jquants-cli-usage\references\commands-fins.md`
- `C:\Users\silve\.codex\skills\jquants-cli-usage\references\commands-eq.md`

---

## 10. 代表CLI

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
  --statement-periods 1Q,3Q `
  --output-dir "D:\作業用\DB反映まち"
```

J-Quants rawからmetrics再構築:

```powershell
.\.venv\Scripts\python.exe -m edinet_monitor.cli.rebuild_jquants_metrics_from_raw `
  --date-from YYYY-MM-DD `
  --date-to YYYY-MM-DD `
  --periods FY,1Q,2Q,3Q `
  --codes all `
  --apply `
  --output-dir "D:\作業用\DB反映まち"
```

market_derived_metrics再計算:

```powershell
.\.venv\Scripts\python.exe -m edinet_monitor.cli.save_market_derived_metrics `
  --date-from YYYY-MM-DD `
  --date-to YYYY-MM-DD `
  --codes all `
  --period-scopes all `
  --apply `
  --output-dir "D:\作業用\DB反映まち"
```

Excel出力:

```powershell
.\.venv\Scripts\python.exe -m edinet_monitor.cli.export_metric_excel `
  --condition-xlsx "D:\作業用\条件.xlsx" `
  --output-dir "D:\作業用"
```

J-Quants品質監査:

```powershell
.\.venv\Scripts\python.exe -m edinet_monitor.cli.audit_jquants_quality `
  --date-from YYYY-MM-DD `
  --date-to YYYY-MM-DD `
  --codes all `
  --output-dir "D:\作業用\DB反映まち"
```

---

## 11. テスト

代表的な回帰テスト:

```powershell
$env:PYTHONPATH='C:\Users\silve\EDINET_Pipeline\src'
.\.venv\Scripts\python.exe -m unittest `
  tests.edinet_monitor.test_derived_metric_service `
  tests.edinet_monitor.test_metric_excel_export_service `
  tests.edinet_monitor.test_jquants_services `
  tests.edinet_monitor.test_jquants_audit_services `
  tests.edinet_monitor.test_jquants_official_cli_compare_service `
  tests.edinet_monitor.test_quarter_standalone_metric_service `
  tests.edinet_monitor.test_market_derived_metric_service `
  tests.edinet_pipeline.test_db_excel_export_service
```

編集後の軽い確認:

```powershell
git diff --check
```

---

## 12. 直近の修正状況

2026-04末から2026-05にかけての重要変更:

- 1Qから4Qの成長率は、同一docの `Prior1` ではなく前年度docの `Current` を優先参照するよう修正。
- J-Quants 1Q/3Qの成長率は、当期同Q累計を前年度同Q累計で割る。
- J-Quants EPS/BPSは、独自算出ルールを維持。
- 予想EPSは保存しない方針を維持。
- 1Q/3QのEPS増加率、BPS増加率、推定純利益、売上高増収率、営業利益増益率、経常利益増益率、純利益増益率、推定純利益増益率、株価上昇率などを追加。
- 2Q/4Qの一部指標名に `(前期比)` を付与。
- 時価総額のExcel表示を「億円」に変更。
- 発行株数のExcel表示を「千株」に変更。
- 4Qの5年/10年成長率系指標を、通期・四半期含め全期間表示できるよう変更。欠損があれば空欄。
- 2Qの不要指標をDB・Excel出力対象から削除。
- `active_latest_jquants_metrics` ビューを追加。
- `jquants_listed_info_raw` 保存とissuer master照合を追加。
- J-Quants異常値検知CLI `audit_jquants_quality` を追加。
- Standardプランでは `/v2/fins/details` が使えないため、fs_detailsのDB反映を断念。
- `/v2/fins/details` 用の取得、保存、DB作成、監査コードを削除。
- 公式 `jquants` CLIとの比較用CLI `compare_jquants_official_cli` を追加。
- J-Quantsトラブル時の運用メモ `docs\troubleshooting.md` を追加。

---

## 13. AI作業ルール

AIがこのプロジェクトを触る時のルール:

- ユーザーが明示しない限り、DB本体を更新しない。
- DB反映前の内容は `D:\作業用\DB反映まち\db_reflection_ready_items_20260430_212147.txt` に記録する。
- 既存のユーザー変更を勝手に戻さない。
- 文字化けしている既存ファイルを見つけた場合、内容を推測して雑に修正しない。関連コードとテストで確認する。
- Excelの不具合は、Excelだけを見て判断せず、DB raw、normalized、derived、exportの順で切り分ける。
- J-Quantsの不具合は、まず公式CLIで同じ日付・同じ銘柄を叩く。
- EDINET値が有報と違う疑いがある場合、タグ候補、連結/非連結、期間、業種ルール、`tag_alias.py` を確認する。
- テストなしで広範囲のDB再構築や削除を提案しない。
- destructiveなGit操作、DB削除、ファイル一括削除はユーザー確認なしで行わない。

---

## 14. 復旧後チェックリスト

復旧直後は以下を確認する。

- `C:\Users\silve\EDINET_Pipeline` が存在する。
- `E:\EDINET_Data\edinet_monitor\db\edinet_monitor.db` が存在する。
- `requirements.txt` から `.venv` を再作成済み。
- `EDINET_API_KEY` が設定済み。
- `JQUANTS_API_KEY` が設定済み。
- `PYTHONPATH` が `C:\Users\silve\EDINET_Pipeline\src` を指している。
- `python -m unittest ...` の代表テストが通る。
- `jquants --output json fins summary --date 2026-05-07` が実行できる。
- `compare_jquants_official_cli` が `--help` で起動する。
- `D:\作業用\DB反映まち` が必要なら別途復元されている。

---

## 15. 最後に

このプロジェクトで一番重要なのは、コードそのものよりも「どの値を正として扱うか」「DBをいつ更新してよいか」「J-QuantsとEDINETをどう切り分けるか」です。

復旧後に迷った場合は、すぐに実装へ進まず、まずこの順で確認してください。

1. `AI_README.md`
2. `docs\troubleshooting.md`
3. `src\edinet_monitor\config\settings.py`
4. `src\edinet_monitor\db\schema.py`
5. 関連テスト
6. DB反映待ちファイル
