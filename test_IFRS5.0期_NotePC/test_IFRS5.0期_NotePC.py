import os
import openpyxl
from bs4 import BeautifulSoup
import yfinance as yf
from datetime import datetime, timedelta
import glob
from tkinter import Tk, filedialog
import pandas as pd
import json

# スクリプトのあるフォルダに作業ディレクトリを変更
script_dir = os.path.dirname(os.path.abspath(__file__))  # スクリプトのあるフォルダを取得
os.chdir(script_dir)  # 作業ディレクトリをスクリプトのフォルダに変更

# 作業ディレクトリを表示
print("作業ディレクトリ:", os.getcwd())

# フォルダを選択する関数
def choose_directory():
    root = Tk()
    root.withdraw()  # Tkinterのウィンドウを非表示にする
    folder_path = filedialog.askdirectory(title="XBRLフォルダを選択してください")
    return folder_path

# メイン処理
print("XBRLフォルダを選択してください。")
base_dir = choose_directory()

if not base_dir:
    print("XBRLフォルダが選択されませんでした。プログラムを終了します。")
    exit()

# 数値を指定された単位で切り捨て
def trim_value(value, unit='millions'):
    try:
        factor = {'millions': 1_000_000, 'thousands': 1_000, 'ten': 10}[unit]
        return int(value) // factor
    except ValueError:
        return 'データなし'

# XBRLデータのパース
def parse_xbrl_data(xbrl_file, tags_contexts):
    with open(xbrl_file, 'r', encoding='utf-8') as file:
        soup = BeautifulSoup(file, 'lxml-xml')
        
        def get_data(tags, context_base, trim=True, unit_or_part='millions'):
            contexts = [context_base]
            for context in contexts:
                for tag in tags:  # タグをリストとして渡し、順番に検索
                    element = soup.find(tag, contextRef=context)
                    if element:
                        value = element.text
                        try:
                            # 日付形式として解析
                            date_obj = datetime.strptime(value, '%Y-%m-%d')
                            if unit_or_part == 'date':
                                return date_obj.strftime('%Y-%m-%d')
                            elif unit_or_part == 'year':
                                return date_obj.year
                            elif unit_or_part == 'month':
                                return date_obj.month
                            elif unit_or_part == 'day':
                                return date_obj.day
                        except ValueError:
                            return trim_value(value, unit_or_part) if trim else value
            return 'データなし'
        
        # 証券コードを取得
        security_code_element = soup.find('jpdei_cor:SecurityCodeDEI', contextRef="FilingDateInstant")
        security_code = security_code_element.text.strip() if security_code_element else None

        # 一の位を除外
        if security_code and security_code.isdigit():
            security_code = security_code[:-1] 
        return {
    key: get_data(tag, context, trim, unit) for key, (tag, context, trim, unit) in tags_contexts.items()
},security_code


# Excelへのデータ書き込み
def write_data_to_excel(excel_file, data, cell_map):
    workbook = openpyxl.load_workbook(excel_file)
    sheet = workbook['決算入力']
    
    for key, cell in cell_map.items():
        if key in data:
            if isinstance(cell, list):
                for c in cell:
                    sheet[c] = data[key]
            else:
                sheet[cell] = data[key]
    
    workbook.save(excel_file)       

# 株価データの取得　調整前株価：auto_adjust=False　調整後株価：auto_adjust=True　←株価同じ。今後変わるかも(2025.02.22現在)
def get_stock_price(stock_code, target_date, backup_date, buffer_days=3):
    start_date = (datetime.strptime(backup_date, '%Y-%m-%d') - timedelta(days=buffer_days)).strftime('%Y-%m-%d')
    end_date = (datetime.strptime(target_date, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')
    hist = yf.download(tickers=stock_code, start=start_date, end=end_date, auto_adjust=False)
    hist.index = hist.index.tz_localize(None)
    
    check_date = datetime.strptime(target_date, '%Y-%m-%d')
    while check_date >= datetime.strptime(start_date, '%Y-%m-%d'):
        if check_date.strftime('%Y-%m-%d') in hist.index:
            return hist.loc[check_date.strftime('%Y-%m-%d'), 'Close']
        check_date -= timedelta(days=1)
    return None

# 株価データをExcelに書き込む
def write_stock_data_to_excel(excel_file, stock_code, date_pairs):
    workbook = openpyxl.load_workbook(excel_file)
    sheet = workbook['決算入力']
    
    for dates in date_pairs:
        price = get_stock_price(stock_code, dates['target_date'], dates['backup_date'])
        if price is not None:
            sheet[dates['cell']] = float(price.iloc[0]) if isinstance(price, pd.Series) else float(price)

            print(f"{dates['target_date']}の株価がセル{dates['cell']}に保存されました。")
        else:
            print(f"{dates['target_date']}のデータが取得できませんでした。")
    
    workbook.save(excel_file)

# Excelファイルのリネーム
def rename_excel_file(original_path, security_code, company_name, period_end_date):
    base_name = f"{security_code}_{company_name}_{period_end_date}"
    new_file_path = os.path.join(os.path.dirname(original_path), f"{base_name}.xlsx")
    
    counter = 1
    while os.path.exists(new_file_path):
        new_file_path = os.path.join(os.path.dirname(original_path), f"{base_name}_{counter}.xlsx")
        counter += 1
    
    os.rename(original_path, new_file_path)
    print(f"Excelファイルがリネームされました: {new_file_path}")
    return new_file_path

# 条件に応じたマッピングの選択
def select_mapping():
        return {
            'tags_contexts': {
                #売上高（売上収益、営業収益）
                'NetSalesPrior': (['jpigp_cor:NetSalesIFRS', 'jpigp_cor:RevenueIFRS'], "Prior1YearDuration", True, 'millions'),
                'NetSalesCurrent': (['jpigp_cor:NetSalesIFRS', 'jpigp_cor:RevenueIFRS'], "CurrentYearDuration", True, 'millions'),
                #売上原価
                'CostOfSalesPrior': (['jpigp_cor:CostOfSalesIFRS'], "Prior1YearDuration", True, 'millions'),
                'CostOfSalesCurrent': (['jpigp_cor:CostOfSalesIFRS'], "CurrentYearDuration", True, 'millions'),
                #売上総利益
                'GrossProfitPrior': (['jpigp_cor:GrossProfitIFRS'], "Prior1YearDuration", True, 'millions'),
                'GrossProfitCurrent': (['jpigp_cor:GrossProfitIFRS'], "CurrentYearDuration", True, 'millions'),
                #販管費
                'SellingGeneralPrior': (['jpigp_cor:SellingGeneralAndAdministrativeExpensesIFRS'], "Prior1YearDuration", True, 'millions'),
                'SellingGeneralCurrent': (['jpigp_cor:SellingGeneralAndAdministrativeExpensesIFRS'], "CurrentYearDuration", True, 'millions'),
                #営業利益
                'OperatingProfitLossPrior': (['jpigp_cor:OperatingProfitLossIFRS'], "Prior1YearDuration", True, 'millions'),
                'OperatingProfitLossCurrent': (['jpigp_cor:OperatingProfitLossIFRS'], "CurrentYearDuration", True, 'millions'),
                #経常利益（税引前利益）
                'ProfitLossBeforePrior': (['jpigp_cor:ProfitLossBeforeTaxIFRS'], "Prior1YearDuration", True, 'millions'),
                'ProfitLossBeforeCurrent': (['jpigp_cor:ProfitLossBeforeTaxIFRS'], "CurrentYearDuration", True, 'millions'),
                #純利益（当期利益）
                'ProfitLossPrior': (['jpigp_cor:ProfitLossIFRS'], "Prior1YearDuration", True, 'millions'),
                'ProfitLossCurrent': (['jpigp_cor:ProfitLossIFRS'], "CurrentYearDuration", True, 'millions'),
                #発行株数
                'TotalNumberPrior4': (['jpcrp_cor:TotalNumberOfIssuedSharesSummaryOfBusinessResults'], "Prior4YearInstant_NonConsolidatedMember", True, 'thousands'),
                'TotalNumberPrior3': (['jpcrp_cor:TotalNumberOfIssuedSharesSummaryOfBusinessResults'], "Prior3YearInstant_NonConsolidatedMember", True, 'thousands'),
                'TotalNumberPrior2': (['jpcrp_cor:TotalNumberOfIssuedSharesSummaryOfBusinessResults'], "Prior2YearInstant_NonConsolidatedMember", True, 'thousands'),
                'TotalNumberPrior1': (['jpcrp_cor:TotalNumberOfIssuedSharesSummaryOfBusinessResults'], "Prior1YearInstant_NonConsolidatedMember", True, 'thousands'),
                'TotalNumberCurrent': (['jpcrp_cor:TotalNumberOfIssuedSharesSummaryOfBusinessResults'], "CurrentYearInstant_NonConsolidatedMember", True, 'thousands'),
                #資産
                'TotalAssetsPrior4': (['jpcrp_cor:TotalAssetsIFRSSummaryOfBusinessResults'], "Prior4YearInstant", True, 'millions'),
                'TotalAssetsPrior3': (['jpcrp_cor:TotalAssetsIFRSSummaryOfBusinessResults'], "Prior3YearInstant", True, 'millions'),
                'TotalAssetsPrior2': (['jpcrp_cor:TotalAssetsIFRSSummaryOfBusinessResults'], "Prior2YearInstant", True, 'millions'),
                'TotalAssetsPrior1': (['jpcrp_cor:TotalAssetsIFRSSummaryOfBusinessResults'], "Prior1YearInstant", True, 'millions'),
                'TotalAssetsCurrent': (['jpcrp_cor:TotalAssetsIFRSSummaryOfBusinessResults'], "CurrentYearInstant", True, 'millions'),
                #資本
                'EquityAttributablePrior4': (['jpcrp_cor:EquityAttributableToOwnersOfParentIFRSSummaryOfBusinessResults'], "Prior4YearInstant", True, 'millions'),
                'EquityAttributablePrior3': (['jpcrp_cor:EquityAttributableToOwnersOfParentIFRSSummaryOfBusinessResults'], "Prior3YearInstant", True, 'millions'),
                'EquityAttributablePrior2': (['jpcrp_cor:EquityAttributableToOwnersOfParentIFRSSummaryOfBusinessResults'], "Prior2YearInstant", True, 'millions'),
                'EquityAttributablePrior1': (['jpcrp_cor:EquityAttributableToOwnersOfParentIFRSSummaryOfBusinessResults'], "Prior1YearInstant", True, 'millions'),
                'EquityAttributableCurrent': (['jpcrp_cor:EquityAttributableToOwnersOfParentIFRSSummaryOfBusinessResults'], "CurrentYearInstant", True, 'millions'),
                #営業CF
                'OperatingPrior': (['jpigp_cor:NetCashProvidedByUsedInOperatingActivitiesIFRS'], "Prior1YearDuration", True, 'millions'),
                'OperatingCurrent': (['jpigp_cor:NetCashProvidedByUsedInOperatingActivitiesIFRS'], "CurrentYearDuration", True, 'millions'),
                #投資CF
                'InvestingPrior': (['jpigp_cor:NetCashProvidedByUsedInInvestingActivitiesIFRS'], "Prior1YearDuration", True, 'millions'),
                'InvestingCurrent': (['jpigp_cor:NetCashProvidedByUsedInInvestingActivitiesIFRS'], "CurrentYearDuration", True, 'millions'),
                #財務CF
                'FinancingPrior': (['jpigp_cor:NetCashProvidedByUsedInFinancingActivitiesIFRS'], "Prior1YearDuration", True, 'millions'),
                'FinancingCurrent': (['jpigp_cor:NetCashProvidedByUsedInFinancingActivitiesIFRS'], "CurrentYearDuration", True, 'millions'),
                #FCF
                'CashAndCashPrior': (['jpigp_cor:CashAndCashEquivalentsIFRS'], "Prior1YearInstant", True, 'millions'),
                'CashAndCashCurrent': (['jpigp_cor:CashAndCashEquivalentsIFRS'], "CurrentYearInstant", True, 'millions'),
                #証券コード
                'SecurityCodeDEI': (['jpdei_cor:SecurityCodeDEI'], "FilingDateInstant", True, 'ten'),
                #会社名
                'CompanyNameCoverPage': (['jpcrp_cor:CompanyNameCoverPage'], "FilingDateInstant", False, 'millions'),
                #当会計期間終了日
                'CurrentPeriodEndDateDEIdate': (['jpdei_cor:CurrentPeriodEndDateDEI'], "FilingDateInstant", False, 'date'),
                #当事業年度終了日
                'CurrentFiscalYearEndDateDEIyear': (['jpdei_cor:CurrentFiscalYearEndDateDEI'], "FilingDateInstant", False, 'year'),
                'CurrentFiscalYearEndDateDEImonth': (['jpdei_cor:CurrentFiscalYearEndDateDEI'], "FilingDateInstant", False, 'month')
            },
            'cell_map_file1': {
                #売上高（売上収益、営業収益）
                'NetSalesPrior': 'D5', 'NetSalesCurrent': 'G5',
                #売上原価
                'CostOfSalesPrior': 'D6', 'CostOfSalesCurrent': 'G6',
                #売上総利益
                'GrossProfitPrior': 'D7', 'GrossProfitCurrent': 'G7',
                #販管費
                'SellingGeneralPrior': 'D8', 'SellingGeneralCurrent': 'G8',
                #営業利益
                'OperatingProfitLossPrior': 'D9', 'OperatingProfitLossCurrent': 'G9',
                #経常利益（税引前利益）
                'ProfitLossBeforePrior': 'D10', 'ProfitLossBeforeCurrent': 'G10',
                #純利益（当期利益）
                'ProfitLossPrior': 'D11', 'ProfitLossCurrent': 'G11',
                #営業CF
                'OperatingPrior': 'C21', 'OperatingCurrent': 'F21',
                #投資CF
                'InvestingPrior': 'D21', 'InvestingCurrent': 'G21',
                #財務CF
                'FinancingPrior': 'E21', 'FinancingCurrent': 'H21',
                #FCF
                'CashAndCashPrior': 'D22', 'CashAndCashCurrent': 'G22'
            },
            'cell_map_file2': {
                #売上高（売上収益、営業収益）
                'NetSalesPrior': 'J5', 'NetSalesCurrent': 'M5',
                #売上原価
                'CostOfSalesPrior': 'J6', 'CostOfSalesCurrent': 'M6',
                #売上総利益
                'GrossProfitPrior': 'J7', 'GrossProfitCurrent': 'M7',
                #販管費
                'SellingGeneralPrior': 'J8', 'SellingGeneralCurrent': 'M8',
                #営業利益
                'OperatingProfitLossPrior': 'J9', 'OperatingProfitLossCurrent': 'M9',
                #経常利益（税引前利益）
                'ProfitLossBeforePrior': 'J10', 'ProfitLossBeforeCurrent': 'M10',
                #純利益（当期利益）
                'ProfitLossPrior': 'J11', 'ProfitLossCurrent': 'M11',
                #営業CF
                'OperatingPrior': 'I21', 'OperatingCurrent': 'L21',
                #投資CF
                'InvestingPrior': 'J21', 'InvestingCurrent': 'M21',
                #財務CF
                'FinancingPrior': 'K21', 'FinancingCurrent': 'N21',
                #FCF
                'CashAndCashPrior': 'J22', 'CashAndCashCurrent': 'M22'
            },
            'cell_map_file3': {
                #売上高（売上収益、営業収益）
                'NetSalesCurrent': ['P30', 'P36'],
                #売上原価
                'CostOfSalesCurrent': 'P6',
                #売上総利益
                'GrossProfitCurrent': 'P7',
                #販管費
                'SellingGeneralCurrent': 'P8',
                #営業利益
                'OperatingProfitLossCurrent': 'P9',
                #経常利益（税引前利益）
                'ProfitLossBeforeCurrent': ['P31', 'P37'],
                #純利益（当期利益）
                'ProfitLossCurrent': ['P32', 'P38'],
                #発行株数
                'TotalNumberPrior4': 'D13', 'TotalNumberPrior3': 'G13', 
                'TotalNumberPrior2': 'J13', 'TotalNumberPrior1': 'M13',
                'TotalNumberCurrent': 'P40',
                #資産
                'TotalAssetsPrior4': 'C17', 'TotalAssetsPrior3': 'F17',
                'TotalAssetsPrior2': 'I17', 'TotalAssetsPrior1': 'L17', 
                'TotalAssetsCurrent': 'O44',
                #資本
                'EquityAttributablePrior4': 'D17', 'EquityAttributablePrior3': 'G17',
                'EquityAttributablePrior2': 'J17', 'EquityAttributablePrior1': 'M17',
                'EquityAttributableCurrent': 'P44',
                #営業CF
                'OperatingCurrent': 'O48',
                #投資CF
                'InvestingCurrent': 'P48',
                #財務CF
                'FinancingCurrent': 'Q48',
                #FCF
                'CashAndCashCurrent': 'P49',    
                #証券コード
                'SecurityCodeDEI': 'K2',
                #会社名
                'CompanyNameCoverPage': 'L2',
                #当事業年度終了日
                'CurrentFiscalYearEndDateDEIyear': 'N2',
                'CurrentFiscalYearEndDateDEImonth': 'O2'
            }
        }

# ファイルパスを順番に確認する関数
def find_available_excel_file(base_path, file_name, max_copies=10):
    for i in range(max_copies):
        if i == 0:
            file_path = os.path.join(base_path, f"{file_name} - コピー.xlsx")
        else:
            file_path = os.path.join(base_path, f"{file_name} - コピー ({i}).xlsx")
        
        if os.path.exists(file_path):
            return file_path
    
    print(f"{file_name}のいずれのバージョンも見つかりませんでした。")
    return None

# 各ループで処理するファイルパスをリスト化
loops = [
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '1-1*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '1-3*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '1-4*.xbrl'))
        },
        'excel_file_path': r'C:\Users\users\OneDrive\PC\EDINET\決算分析シート\決算分析シート1.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '2-1*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '2-3*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '2-4*.xbrl'))
        },
        'excel_file_path': r'C:\Users\users\OneDrive\PC\EDINET\決算分析シート\決算分析シート2.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '3-1*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '3-3*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '3-4*.xbrl'))
        },
        'excel_file_path': r'C:\Users\users\OneDrive\PC\EDINET\決算分析シート\決算分析シート3.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '4-1*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '4-3*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '4-4*.xbrl'))
        },
        'excel_file_path': r'C:\Users\users\OneDrive\PC\EDINET\決算分析シート\決算分析シート4.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '5-1*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '5-3*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '5-4*.xbrl'))
        },
        'excel_file_path': r'C:\Users\users\OneDrive\PC\EDINET\決算分析シート\決算分析シート5.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '6-1*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '6-3*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '6-4*.xbrl'))
        },
        'excel_file_path': r'C:\Users\users\OneDrive\PC\EDINET\決算分析シート\決算分析シート6.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '7-1*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '7-3*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '7-4*.xbrl'))
        },
        'excel_file_path': r'C:\Users\users\OneDrive\PC\EDINET\決算分析シート\決算分析シート7.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '8-1*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '8-3*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '8-4*.xbrl'))
        },
        'excel_file_path': r'C:\Users\users\OneDrive\PC\EDINET\決算分析シート\決算分析シート8.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '9-1*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '9-3*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '9-4*.xbrl'))
        },
        'excel_file_path': r'C:\Users\users\OneDrive\PC\EDINET\決算分析シート\決算分析シート9.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '10-1*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '10-3*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '10-4*.xbrl'))
        },
        'excel_file_path': r'C:\Users\users\OneDrive\PC\EDINET\決算分析シート\決算分析シート10.xlsx'
        },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '11-1*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '11-3*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '11-4*.xbrl'))
        },
        'excel_file_path': r'C:\Users\users\OneDrive\PC\EDINET\決算分析シート\決算分析シート11.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '12-1*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '12-3*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '12-4*.xbrl'))
        },
        'excel_file_path': r'C:\Users\users\OneDrive\PC\EDINET\決算分析シート\決算分析シート12.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '13-1*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '13-3*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '13-4*.xbrl'))
        },
        'excel_file_path': r'C:\Users\users\OneDrive\PC\EDINET\決算分析シート\決算分析シート13.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '14-1*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '14-3*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '14-4*.xbrl'))
        },
        'excel_file_path': r'C:\Users\users\OneDrive\PC\EDINET\決算分析シート\決算分析シート14.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '15-1*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '15-3*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '15-4*.xbrl'))
        },
        'excel_file_path': r'C:\Users\users\OneDrive\PC\EDINET\決算分析シート\決算分析シート15.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '16-1*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '16-3*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '16-4*.xbrl'))
        },
        'excel_file_path': r'C:\Users\users\OneDrive\PC\EDINET\決算分析シート\決算分析シート16.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '17-1*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '17-3*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '17-4*.xbrl'))
        },
        'excel_file_path': r'C:\Users\users\OneDrive\PC\EDINET\決算分析シート\決算分析シート17.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '18-1*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '18-3*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '18-4*.xbrl'))
        },
        'excel_file_path': r'C:\Users\users\OneDrive\PC\EDINET\決算分析シート\決算分析シート18.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '19-1*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '19-3*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '19-4*.xbrl'))
        },
        'excel_file_path': r'C:\Users\users\OneDrive\PC\EDINET\決算分析シート\決算分析シート19.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '20-1*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '20-3*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '20-4*.xbrl'))
        },
        'excel_file_path': r'C:\Users\users\OneDrive\PC\EDINET\決算分析シート\決算分析シート20.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '21-1*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '21-3*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '21-4*.xbrl'))
        },
        'excel_file_path': r'C:\Users\users\OneDrive\PC\EDINET\決算分析シート\決算分析シート21.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '22-1*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '22-3*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '22-4*.xbrl'))
        },
        'excel_file_path': r'C:\Users\users\OneDrive\PC\EDINET\決算分析シート\決算分析シート22.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '23-1*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '23-3*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '23-4*.xbrl'))
        },
        'excel_file_path': r'C:\Users\users\OneDrive\PC\EDINET\決算分析シート\決算分析シート23.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '24-1*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '24-3*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '24-4*.xbrl'))
        },
        'excel_file_path': r'C:\Users\users\OneDrive\PC\EDINET\決算分析シート\決算分析シート24.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '25-1*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '25-3*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '25-4*.xbrl'))
        },
        'excel_file_path': r'C:\Users\users\OneDrive\PC\EDINET\決算分析シート\決算分析シート25.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '26-1*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '26-3*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '26-4*.xbrl'))
        },
        'excel_file_path': r'C:\Users\users\OneDrive\PC\EDINET\決算分析シート\決算分析シート26.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '27-1*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '27-3*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '27-4*.xbrl'))
        },
        'excel_file_path': r'C:\Users\users\OneDrive\PC\EDINET\決算分析シート\決算分析シート27.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '28-1*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '28-3*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '28-4*.xbrl'))
        },
        'excel_file_path': r'C:\Users\users\OneDrive\PC\EDINET\決算分析シート\決算分析シート28.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '29-1*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '29-3*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '29-4*.xbrl'))
        },
        'excel_file_path': r'C:\Users\users\OneDrive\PC\EDINET\決算分析シート\決算分析シート29.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '30-1*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '30-3*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '30-4*.xbrl'))
        },
        'excel_file_path': r'C:\Users\users\OneDrive\PC\EDINET\決算分析シート\決算分析シート30.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '31-1*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '31-3*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '31-4*.xbrl'))
        },
        'excel_file_path': r'C:\Users\users\OneDrive\PC\EDINET\決算分析シート\決算分析シート31.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '32-1*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '32-3*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '32-4*.xbrl'))
        },
        'excel_file_path': r'C:\Users\users\OneDrive\PC\EDINET\決算分析シート\決算分析シート32.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '33-1*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '33-3*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '33-4*.xbrl'))
        },
        'excel_file_path': r'C:\Users\users\OneDrive\PC\EDINET\決算分析シート\決算分析シート33.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '34-1*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '34-3*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '34-4*.xbrl'))
        },
        'excel_file_path': r'C:\Users\users\OneDrive\PC\EDINET\決算分析シート\決算分析シート34.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '35-1*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '35-3*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '35-4*.xbrl'))
        },
        'excel_file_path': r'C:\Users\users\OneDrive\PC\EDINET\決算分析シート\決算分析シート35.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '36-1*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '36-3*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '36-4*.xbrl'))
        },
        'excel_file_path': r'C:\Users\users\OneDrive\PC\EDINET\決算分析シート\決算分析シート36.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '37-1*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '37-3*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '37-4*.xbrl'))
        },
        'excel_file_path': r'C:\Users\users\OneDrive\PC\EDINET\決算分析シート\決算分析シート37.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '38-1*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '38-3*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '38-4*.xbrl'))
        },
        'excel_file_path': r'C:\Users\users\OneDrive\PC\EDINET\決算分析シート\決算分析シート38.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '39-1*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '39-3*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '39-4*.xbrl'))
        },
        'excel_file_path': r'C:\Users\users\OneDrive\PC\EDINET\決算分析シート\決算分析シート39.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '40-1*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '40-3*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '40-4*.xbrl'))
        },
        'excel_file_path': r'C:\Users\users\OneDrive\PC\EDINET\決算分析シート\決算分析シート40.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '41-1*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '41-3*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '41-4*.xbrl'))
        },
        'excel_file_path': r'C:\Users\users\OneDrive\PC\EDINET\決算分析シート\決算分析シート41.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '42-1*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '42-3*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '42-4*.xbrl'))
        },
        'excel_file_path': r'C:\Users\users\OneDrive\PC\EDINET\決算分析シート\決算分析シート42.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '43-1*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '43-3*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '43-4*.xbrl'))
        },
        'excel_file_path': r'C:\Users\users\OneDrive\PC\EDINET\決算分析シート\決算分析シート43.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '44-1*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '44-3*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '44-4*.xbrl'))
        },
        'excel_file_path': r'C:\Users\users\OneDrive\PC\EDINET\決算分析シート\決算分析シート44.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '45-1*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '45-3*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '45-4*.xbrl'))
        },
        'excel_file_path': r'C:\Users\users\OneDrive\PC\EDINET\決算分析シート\決算分析シート45.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '46-1*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '46-3*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '46-4*.xbrl'))
        },
        'excel_file_path': r'C:\Users\users\OneDrive\PC\EDINET\決算分析シート\決算分析シート46.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '47-1*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '47-3*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '47-4*.xbrl'))
        },
        'excel_file_path': r'C:\Users\users\OneDrive\PC\EDINET\決算分析シート\決算分析シート47.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '48-1*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '48-3*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '48-4*.xbrl'))
        },
        'excel_file_path': r'C:\Users\users\OneDrive\PC\EDINET\決算分析シート\決算分析シート48.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '49-1*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '49-3*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '49-4*.xbrl'))
        },
        'excel_file_path': r'C:\Users\users\OneDrive\PC\EDINET\決算分析シート\決算分析シート49.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '50-1*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '50-3*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '50-4*.xbrl'))
        },
        'excel_file_path': r'C:\Users\users\OneDrive\PC\EDINET\決算分析シート\決算分析シート50.xlsx'
    }
]

# 処理するファイル数を選択
def choose_file_count():
    while True:
        try:
            count = int(input("処理するファイルの数を選択してください（1～50）: "))
            if 1 <= count <= 50:
                return count
            else:
                print("1から50の範囲で入力してください。")
        except ValueError:
            print("数字を入力してください。")

# ファイル数の選択
file_count = choose_file_count()

# スキップされたファイルを記録するリスト
skipped_files = []

# 決算期を最初に1回だけ選択
try:
    def load_config(file_path):
        with open(file_path, 'r') as f:
            return json.load(f)

    config = load_config('決算期.json')
    chosen_period = input("決算期を選択してください（例 25-1）: ")

    if chosen_period in config:
        date_pairs = config[chosen_period]
        print(f"選択された決算期: {chosen_period}")
        print("決算期データ:", date_pairs)
    else:
        print("無効な選択です。プログラムを終了します。")
        exit()  # 無効な選択の場合、プログラム終了
except Exception as e:
    print(f"設定ファイルの読み込み中にエラー: {e}")
    exit()

# XBRLデータの取得、証券コードの取得、Excelへの書き込み、株価データ取得までをループ処理に含める
for i in range(file_count):
    loop = loops[i]
    # Excelファイル名を解析
    excel_base_name = os.path.basename(loop['excel_file_path']).replace('.xlsx', '')
    excel_directory = os.path.dirname(loop['excel_file_path'])
    
    # 実際に使用するExcelファイルを見つける
    selected_file = find_available_excel_file(excel_directory, excel_base_name)
    
    if selected_file:
        loop['excel_file_path'] = selected_file  
        print(f"使用するExcelファイル: {selected_file}")
    else:
        skipped_files.append({
            'reason': "Excelファイルが見つからない",
            'file': excel_base_name
        })
        print("使用するファイルが見つかりませんでした。次のループを実行します。")
        continue

    rename_info = None
    xbrl_file_paths = loop['xbrl_file_paths']
    excel_file_path = loop['excel_file_path']
    security_code = None  # 初期値

    for key, paths in xbrl_file_paths.items():
        if not paths:
            skipped_files.append({
                'reason': f"{key} に対応するXBRLファイルが見つからない",
                'file': excel_file_path
            })
            print(f"{key} に対応するファイルが見つかりません。スキップします。")
            continue

        path = paths[0]
        
        # マッピングを選択
        mappings = select_mapping() 
        tags_contexts = mappings['tags_contexts']
        # XBRLファイルからデータを解析
        try:
            xbrl_data, security_code = parse_xbrl_data(path, tags_contexts)
        except Exception as e:
            skipped_files.append({
                'reason': f"XBRLデータの解析中にエラー: {e}",
                'file': path
            })
            print(f"{key} のファイル {path} を解析中にエラーが発生しました。スキップします。")
            continue

        # マッピングを選択
        mappings = select_mapping()

        # マッピングから各変数を取得
        tags_contexts = mappings['tags_contexts']
        cell_map_file1 = mappings['cell_map_file1']
        cell_map_file2 = mappings['cell_map_file2']
        cell_map_file3 = mappings['cell_map_file3']

        cell_map = cell_map_file1 if key == 'file1' else cell_map_file2 if key == 'file2' else cell_map_file3

        try:
            write_data_to_excel(excel_file_path, xbrl_data, cell_map)
        except Exception as e:
            skipped_files.append({
                'reason': f"Excelデータの書き込み中にエラー: {e}",
                'file': excel_file_path
            })
            print(f"Excelデータの書き込み中にエラーが発生しました。スキップします。")

        # リネーム情報を取得
        if key == 'file3' and 'SecurityCodeDEI' in xbrl_data and 'CompanyNameCoverPage' in xbrl_data and 'CurrentPeriodEndDateDEIdate' in xbrl_data:
            rename_info = (xbrl_data['SecurityCodeDEI'], xbrl_data['CompanyNameCoverPage'], xbrl_data['CurrentPeriodEndDateDEIdate'])

    # Excelファイルのリネーム
    if rename_info:
        try:
            excel_file_path = rename_excel_file(excel_file_path, *rename_info)
        except Exception as e:
            skipped_files.append({
                'reason': f"Excelファイルのリネーム中にエラー: {e}",
                'file': excel_file_path
            })
            print(f"Excelファイルのリネーム中にエラーが発生しました。")
    else:
        skipped_files.append({
            'reason': "リネーム用のデータが不足している",
            'file': excel_file_path
        })
        print("リネーム用のデータが不足しています。")

    # 証券コードを表示（デバッグ用）
    if security_code:
        print(f"取得した証券コード: {security_code}")
        stock_code = f"{security_code}.T"

        try:
            write_stock_data_to_excel(excel_file_path, stock_code, date_pairs)  # ループ内では `date_pairs` を使用
        except Exception as e:
            skipped_files.append({
                'reason': f"株価データの書き込み中にエラー: {e}",
                'file': excel_file_path
            })
            print(f"株価データの書き込み中にエラーが発生しました。スキップします。")
    if not security_code:
        skipped_files.append({
            'reason': "証券コードが取得できない",
            'file': path
        })
        print("証券コードが取得できませんでした。")

# スキップされたファイルの一覧を表示
print("\n--- スキップされたファイル一覧 ---")
if skipped_files:
    for skipped in skipped_files:
        print(f"理由: {skipped['reason']}, ファイル: {skipped['file']}")
else:
    print("スキップされたファイルはありません。")