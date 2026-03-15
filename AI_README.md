# AI_README.md

EDINETツール 開発引き継ぎドキュメント（ChatGPT用）

---

# 1. プロジェクト概要

## プロジェクト名

edinet-tool
GitHub: https://github.com/k6qybsxt/edinet-tool

## 目的

EDINETのXBRLを解析し、決算分析Excelテンプレートへ自動入力するツール。

対象

* 有価証券報告書
* 四半期報告書

解析対象

* 売上
* 利益
* CF
* BS
* 株式数
* DEI
* 株価

---

# 2. 現在の開発フェーズ

現在は **高速化のための設計刷新フェーズ**

大目標

```
EDINETツールを10〜50倍高速化する
```

そのための設計変更を段階的に実施している。

---

# 3. 完了した設計

## XBRL読み込み方法見直し　10／10（完了）

旧設計

```
parse_xbrl_file
  ↓
out
  ↓
Excel
```

問題

* XBRL構造を破棄
* 再解析が必要
* IFRS対応困難
* 拡張性が低い

---

## 新設計

```
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
```

---

## ParsedXbrlDocument構造

```
ParsedXbrlDocument
 ├ facts
 ├ contexts
 ├ units
 ├ nsmap
 ├ dei_data
 ├ accounting_standard
 └ document_display_unit
```

---

## facts構造

```
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
```

---

## contexts構造

```
context
 ├ period_kind
 ├ start_date
 ├ end_date
 ├ instant_date
 ├ is_consolidated
 └ members
```

---

## units構造

```
unit
 ├ measures
 ├ numerator
 └ denominator
```

---

# 4. parse cache

XBRLは1回だけ解析される。

```
parse_cache
```

ログ

```
[xbrl cache hit]
[xbrl cache miss]
```

---

# 5. raw生成

現在は

```
facts
↓
raw_service
↓
raw_rows
```

タグ制限

```
ALLOWED_RAW_FACT_TAGS
```

raw rows

```
raw_rows ≈ 27
```

---

# 6. 現在のパフォーマンス

1社解析

```
約7秒
```

高速化予定

```
1社 0.2〜0.5秒
```

---

# 7. 次の開発フェーズ

次は

```
IFRS対応　1／10
```

---

# 8. IFRS対応の目的

現在

```
jpcrp_cor
jppfs_cor
```

のみ。

IFRS企業

```
ifrs-full
```

が必要。

---

## IFRS対応で実装する内容

1

```
accounting_standard判定
```

2

```
IFRSタグ追加
```

3

```
タグマッピング
```

4

```
raw_builder対応
```

5

```
Excel出力対応
```

---

# 9. 今後のロードマップ

高速化ロードマップ

```
1 XBRL parse cache
2 Excel rename高速化
3 XBRL読み込み見直し   ← 完了
4 IFRS対応              ← 次
5 50社一括解析
6 XBRL parse cache強化
7 並列処理
```

---

# 10. 開発ルール（重要）

ユーザーはPython初心者。

回答ルール

```
解説不要
指示のみ
```

コード修正指示

```
〇行目〜〇行目を置換
関数を丸ごと置換
```

の形式。

---

# 11. 開発環境

OS

```
Windows
```

Python

```
3.14
```

主要ライブラリ

```
lxml
pandas
openpyxl
yfinance
```

---

# 12. プロジェクト構造

```
src/edinet_tool

cli
config
domain
services
logging_utils
```

重要ディレクトリ

```
services
```

---

## services

```
file_indexer.py
loop_builder.py
loop_processor.py
parse_service.py
parse_cache.py
xbrl_parser.py
raw_service.py
excel_service.py
```

---

# 13. 現在の主要処理フロー

```
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
```

---

# 14. 注意事項

次の開発では

```
parse_xbrl_file
```

の依存を

```
parse_xbrl_file_raw
```

へ徐々に移行する。

---

# 15. 現在の課題

ログに以下が出る

```
ZipFile.__del__
ValueError: I/O operation on closed file
```

これは

```
openpyxl
```

由来の警告であり、
現在の開発タスクには含めない。

---

# 16. 次のチャット開始時の指示

次のAIは以下から開始する。

```
IFRS対応　1／10
```

作業対象

```
xbrl_parser
raw_service
tag mapping
```

---

# 17. 最重要目標

最終的に実現すること

```
EDINETツール
50社解析
10〜50倍高速化
```

---

END
