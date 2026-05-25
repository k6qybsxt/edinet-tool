# troubleshooting.md

## J-Quantsで怪しい挙動が出たとき

J-Quants周りの問題は、独自CLIやDB保存処理を疑う前に、公式 `jquants` CLIで同じ日付・同じ銘柄を叩いて切り分けます。

公式CLIはこの環境では通常 `C:\Users\silve\EDINET_Pipeline\tools\jquants-cli\jquants.exe` を使います。PATHに入っていれば `jquants` だけで実行できます。

### まず確認すること

- 公式CLIも失敗する: APIキー、加入プラン、J-Quants側の仕様・提供期間・障害を疑う。
- 公式CLIは成功し、DB raw値が違う: 取得・保存処理を疑う。
- DB raw値は一致し、normalized metricが違う: mapperや派生指標計算を疑う。
- DB値は一致し、Excelだけ違う: Excel出力ロジックを疑う。

### 専用CLIで公式CLIとDB rawを比較する

財務サマリー:

```powershell
.\.venv\Scripts\python.exe -m edinet_monitor.cli.compare_jquants_official_cli `
  --endpoint fins.summary `
  --date 2026-05-07 `
  --output-dir "D:\作業用\DB反映まち"
```

株価日次:

```powershell
.\.venv\Scripts\python.exe -m edinet_monitor.cli.compare_jquants_official_cli `
  --endpoint eq.daily `
  --date 2026-05-07 `
  --code 72030 `
  --output-dir "D:\作業用\DB反映まち"
```

出力されるTXTには件数サマリー、TSVにはフィールド単位の差分が出ます。

### /v2/fins/details の扱い

Standardプランでは公式CLIのプラン表上、`fins details` はPremium対象です。Standard環境で403になる場合は、原則として契約プラン制限として扱います。

このプロジェクトでは `/v2/fins/details` のDB反映は行わず、専用CLIでも `--endpoint fins.details` は明示的に拒否します。

確認だけ行う場合:

```powershell
jquants --output json fins details --date 2026-05-07
```

Standardで403が返る場合、独自コード側の不具合調査へ進む前にプラン制限として記録します。

### J-Quants仕様変更時の確認

仕様変更が疑われる場合は、公式CLIのschemaとSkill参照を先に確認します。

```powershell
jquants schema
jquants schema fins.summary
jquants schema eq.daily
```

参照先:

- `C:\Users\silve\.codex\skills\jquants-cli-usage\SKILL.md`
- `C:\Users\silve\.codex\skills\jquants-cli-usage\references\plans.md`
- `C:\Users\silve\.codex\skills\jquants-cli-usage\references\commands-fins.md`
- `C:\Users\silve\.codex\skills\jquants-cli-usage\references\commands-eq.md`

仕様上のフィールド名が変わった場合は、まず公式CLIのJSON出力と `jquants_statement_raw.raw_json` または `jquants_daily_quotes.raw_json` を比較し、その後にmapperや派生計算を修正します。
