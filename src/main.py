import certifi
import os
import gc

os.environ["SSL_CERT_FILE"] = certifi.where()

from edinet_tool.config.settings import BASE_DIR, load_config, LOG_LEVEL
from edinet_tool.services.stock_service import validate_stock_date_pairs, clear_stock_price_cache
from edinet_tool.domain.skip import log_skip_summary
from edinet_tool.services.loop_processor import process_one_loop
from edinet_tool.services.loop_builder import build_loops
from edinet_tool.logging_utils.logger import setup_logger
from edinet_tool.cli.prompts import choose_file_count
from edinet_tool.services.parse_cache import XbrlParseCache

logger = None

def main():

    # 株価キャッシュ初期化
    clear_stock_price_cache()

    # 作業ディレクトリを表示
    logger.info(f"プロジェクトルート: {BASE_DIR}")

    # === ZIPフォルダ設定 ===
    base_dir = str(BASE_DIR / "data" / "input" / "zip")
    template_dir = str(BASE_DIR / "templates")

    logger.info(f"ZIPフォルダ（固定）: {base_dir}")
    if not os.path.isdir(base_dir):
        logger.critical(f"base_dir が存在しません。パスを確認してください: {base_dir}")
        raise SystemExit(1)

    # === 件数入力 ===
    file_count = choose_file_count()

    # === スキップ一覧（ローカル）===
    skipped_files = []

    parse_cache = XbrlParseCache(logger=logger)

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
        loop = loops[i]
        try:
            process_one_loop(
                loop,
                date_pairs,
                skipped_files,
                logger,
                parse_cache=parse_cache,
            )
        except SystemExit:
            raise
        except Exception:
            logger.exception(f"1件処理で想定外エラー（続行）: index={i}")

    # === スキップ一覧表示（最後）===
    log_skip_summary(logger, skipped_files)

    # ZipFile 後始末を終了前に前倒し
    gc.collect()

if __name__ == "__main__":
    logger = setup_logger(log_level=LOG_LEVEL)
    try:
        logger.info("===== プログラム開始 =====")
        logger.info(f"[log config] level={LOG_LEVEL}")
        main()
        logger.info("===== 正常終了 =====")
    except SystemExit:
        raise
    except Exception:
        logger.exception("致命的エラーで終了しました")
        raise