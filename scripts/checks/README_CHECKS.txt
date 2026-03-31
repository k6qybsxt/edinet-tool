【scripts/checks の使い方】

■ 目的
日常運用で使う確認用スクリプトをここに集約する。

■ 日常で使う順番
1.
python scripts\checks\check_pending_tse_filings.py

2.
python scripts\checks\check_download_status.py

3.
python scripts\checks\check_xbrl_status.py

4.
python scripts\checks\check_raw_facts_status.py

5.
python scripts\checks\check_normalized_metrics_status.py

6.
python scripts\checks\check_screening_results.py

7.
python scripts\checks\check_low_metric_docs.py

■ 各スクリプトの役割
check_pending_tse_filings.py
- 未処理件数の確認

check_download_status.py
- ZIPダウンロード状況の確認

check_xbrl_status.py
- XBRL抽出状況の確認

check_raw_facts_status.py
- raw_facts 保存状況の確認

check_normalized_metrics_status.py
- normalized_metrics 保存状況の確認
- 件数確認
- 重複確認

check_screening_results.py
- screening 実行結果の確認

check_low_metric_docs.py
- normalized_metrics 件数が少ない doc の確認

■ investigation フォルダについて
scripts\checks\investigation\ は原因調査用。
通常運用では使わない。
不具合調査が必要なときだけ使う。

■ 現在の基準
- pending_count=0
- low metric docs が空
- screening_runs 最新行が target_count=72, hit_count=72

■ ログで原因が分からないとき
NORMALログだけで原因が分からない場合は、
対象に応じて以下の LOG_MODE を DEBUG に変更して再実行する。

- edinet_monitor:
  src\edinet_monitor\config\settings.py

- edinet_pipeline:
  src\edinet_pipeline\config\settings.py