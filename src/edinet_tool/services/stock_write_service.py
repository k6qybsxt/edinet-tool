from edinet_tool.services.stock_service import write_stock_data_to_excel
from edinet_tool.domain.skip import SkipCode, add_skip


def write_stock_if_possible(excel_file_path, security_code, date_pairs, skipped_files, loop, logger):
    if security_code:
        logger.info(f"取得した証券コード: {security_code}")
        stock_code = f"{security_code}.T"
        logger.debug(f"[stock check] security_code={security_code} stock_code={stock_code}")
        try:
            stock_result = write_stock_data_to_excel(excel_file_path, stock_code, date_pairs, logger)
            if stock_result:
                logger.debug(
                    f"[stock] written={stock_result.get('written',0)} "
                    f"miss={stock_result.get('miss',0)} "
                    f"errors={stock_result.get('errors',0)} "
                    f"missing_name={stock_result.get('missing_name',0)} "
                    f"bad_input={stock_result.get('bad_input',0)}"
                )
        except Exception:
            logger.exception("株価データの書き込みで想定外エラー（続行）")
    else:
        add_skip(
            skipped_files,
            code=SkipCode.NO_SECURITY_CODE,
            phase="stock",
            loop=loop,
            excel=excel_file_path,
            xbrl=None,
            message="証券コードが取得できない"
        )
        logger.warning("証券コードが取得できませんでした。")