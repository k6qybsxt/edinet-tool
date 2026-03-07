import certifi
import os

os.environ["SSL_CERT_FILE"] = certifi.where()

from edinet_tool.config.settings import BASE_DIR, load_config
from edinet_tool.services.stock_service import validate_stock_date_pairs
from edinet_tool.domain.skip import log_skip_summary
from edinet_tool.services.loop_processor import process_one_loop
from edinet_tool.services.loop_builder import build_loops
from edinet_tool.logging_utils.logger import setup_logger
import edinet_tool.logging_utils.logger as logger_module
from edinet_tool.cli.prompts import choose_file_count

logger = None

def main():
    # 作業ディレクトリを表示
    logger.info(f"プロジェクトルート: {BASE_DIR}")

    # === XBRLフォルダ設定 ===
    base_dir = str(BASE_DIR / "data" / "input" / "XBRL")
    template_dir = str(BASE_DIR / "templates")

    logger.info(f"XBRLフォルダ（固定）: {base_dir}")
    if not os.path.isdir(base_dir):
        logger.critical(f"base_dir が存在しません。パスを確認してください: {base_dir}")
        raise SystemExit(1)

    # === 件数入力 ===
    file_count = choose_file_count()

    # === スキップ一覧（ローカル）===
    skipped_files = []

    # === loops生成 ===
    loops = build_loops(base_dir, template_dir, max_n=50, logger=logger)

    # === 決算期を最初に1回だけ選択 ===
    try:
        config = load_config(BASE_DIR / "config" / "決算期_KANPE.json")
        chosen_period = input("決算期を選択してください（例 25-1）: ")

        if chosen_period not in config:
            logger.critical("無効な選択です。プログラムを終了します。")
            raise SystemExit(1)

        date_pairs = config[chosen_period]
        validate_stock_date_pairs(date_pairs)
        logger.info(f"選択された決算期: {chosen_period}")
        logger.debug(f"決算期データ: {date_pairs}")
    except Exception:
        logger.exception("決算期設定の読み込み/選択に失敗しました")
        raise SystemExit(1)

    # === ★ここが必要：1件ずつ処理する（呼び出し）===
    for i in range(min(file_count, len(loops))):
        try:
            process_one_loop(loops[i], date_pairs, skipped_files, logger)
        except SystemExit:
            raise
        except Exception:
            logger.exception(f"1件処理で想定外エラー（続行）: index={i}")

    # === スキップ一覧表示（最後）===
    log_skip_summary(logger, skipped_files)

if __name__ == "__main__":
    logger = setup_logger(debug=logger_module.DEBUG)
    logger_module.logger = logger
    try:
        logger.info("===== プログラム開始 =====")
        main()
        logger.info("===== 正常終了 =====")
    except SystemExit:
        raise
    except Exception:
        logger.exception("致命的エラーで終了しました")
        raise