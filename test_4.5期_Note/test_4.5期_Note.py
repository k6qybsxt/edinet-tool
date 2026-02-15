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
        
        # 特定のタグとコンテキストに対してのみNonConsolidatedMemberの存在をチェック
        def has_non_consolidated_member_for_tags(tags_contexts):
            for tag, (context_base, _, _, _) in tags_contexts.items():
                if tag in ['jppfs_cor:NetSales', 'jppfs_cor:CostOfSales']:
                    contexts = [context_base, f"{context_base}_NonConsolidatedMember"]
                    for context in contexts:
                        element = soup.find(tag, contextRef=context)
                        if element and "_NonConsolidatedMember" in context:
                            print(f"Found element with context: {context}, Element: {element}")
                            return True
            return False
        
        # jppfs_cor:NetSalesとjppfs_cor:CostOfSalesに対してチェック
        has_non_consolidated_member = has_non_consolidated_member_for_tags(tags_contexts)

        def get_data(tag, context_base, trim=True, unit_or_part='millions'):
            contexts = [context_base, f"{context_base}_NonConsolidatedMember"]
            for context in contexts:
                element = soup.find(tag, contextRef=context)
                if element:
                    value = element.text
                    try:
                        # 日付形式として解析
                        date_obj = datetime.strptime(value, '%Y-%m-%d')
                        # unit_or_partが指定されていないか、デフォルトで日付全体を返す
                        if unit_or_part == 'date':
                            return date_obj.strftime('%Y-%m-%d')
                        elif unit_or_part == 'year':
                            return date_obj.year
                        elif unit_or_part == 'month':
                            return date_obj.month
                        elif unit_or_part == 'day':
                            return date_obj.day
                    except ValueError:
                        # 日付として解析できない場合の処理
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
}, has_non_consolidated_member, security_code


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
def select_mapping(has_non_consolidated_member):
    if has_non_consolidated_member:
        return {
            'tags_contexts': {
            #個別決算の場合
                #売上高
                'NetSalesPrior': ('jppfs_cor:NetSales', "Prior1YearDuration_NonConsolidatedMember", True, 'millions'),
                'NetSalesCurrent': ('jppfs_cor:NetSales', "CurrentYearDuration_NonConsolidatedMember", True, 'millions'),
                'NetSalesYTD': ('jppfs_cor:NetSales', "CurrentYTDDuration_NonConsolidatedMember", True, 'millions'),
                #売上原価
                'CostOfSalesPrior': ('jppfs_cor:CostOfSales', "Prior1YearDuration_NonConsolidatedMember", True, 'millions'),
                'CostOfSalesCurrent': ('jppfs_cor:CostOfSales', "CurrentYearDuration_NonConsolidatedMember", True, 'millions'),
                #売上総利益
                'GrossProfitPrior': ('jppfs_cor:GrossProfit', "Prior1YearDuration_NonConsolidatedMember", True, 'millions'),
                'GrossProfitCurrent': ('jppfs_cor:GrossProfit', "CurrentYearDuration_NonConsolidatedMember", True, 'millions'),
                #販管費
                'SellingExpensesPrior': ('jppfs_cor:SellingGeneralAndAdministrativeExpenses', "Prior1YearDuration_NonConsolidatedMember", True, 'millions'),
                'SellingExpensesCurrent': ('jppfs_cor:SellingGeneralAndAdministrativeExpenses', "CurrentYearDuration_NonConsolidatedMember", True, 'millions'),
                #営業利益
                'OperatingIncomePrior': ('jppfs_cor:OperatingIncome', "Prior1YearDuration_NonConsolidatedMember", True, 'millions'),
                'OperatingIncomeCurrent': ('jppfs_cor:OperatingIncome', "CurrentYearDuration_NonConsolidatedMember", True, 'millions'),
                #経常利益
                'OrdinaryIncomePrior': ('jppfs_cor:OrdinaryIncome', "Prior1YearDuration_NonConsolidatedMember", True, 'millions'),
                'OrdinaryIncomeCurrent': ('jppfs_cor:OrdinaryIncome', "CurrentYearDuration_NonConsolidatedMember", True, 'millions'),
                'OrdinaryIncomeYTD': ('jppfs_cor:OrdinaryIncome', "CurrentYTDDuration_NonConsolidatedMember", True, 'millions'),
                #純利益
                'ProfitLossPrior': ('jppfs_cor:ProfitLoss', "Prior1YearDuration_NonConsolidatedMember", True, 'millions'),
                'ProfitLossCurrent': ('jppfs_cor:ProfitLoss', "CurrentYearDuration_NonConsolidatedMember", True, 'millions'),
                'ProfitLossYTD': ('jppfs_cor:ProfitLoss', "CurrentYTDDuration_NonConsolidatedMember", True, 'millions'),
                #発行株数
                'TotalNumberPrior3': ('jpcrp_cor:TotalNumberOfIssuedSharesSummaryOfBusinessResults', "Prior3YearInstant_NonConsolidatedMember", True, 'thousands'),
                'TotalNumberPrior2': ('jpcrp_cor:TotalNumberOfIssuedSharesSummaryOfBusinessResults', "Prior2YearInstant_NonConsolidatedMember", True, 'thousands'),
                'TotalNumberPrior1': ('jpcrp_cor:TotalNumberOfIssuedSharesSummaryOfBusinessResults', "Prior1YearInstant_NonConsolidatedMember", True, 'thousands'),
                'TotalNumberCurrent': ('jpcrp_cor:TotalNumberOfIssuedSharesSummaryOfBusinessResults', "CurrentYearInstant_NonConsolidatedMember", True, 'thousands'),
                'TotalNumberQuarter': ('jpcrp_cor:TotalNumberOfIssuedSharesSummaryOfBusinessResults', "CurrentQuarterInstant_NonConsolidatedMember", True, 'thousands'),
                #資産
                'TotalAssetsPrior3': ('jpcrp_cor:TotalAssetsSummaryOfBusinessResults', "Prior3YearInstant_NonConsolidatedMember", True, 'millions'),
                'TotalAssetsPrior2': ('jpcrp_cor:TotalAssetsSummaryOfBusinessResults', "Prior2YearInstant_NonConsolidatedMember", True, 'millions'),
                'TotalAssetsPrior1': ('jpcrp_cor:TotalAssetsSummaryOfBusinessResults', "Prior1YearInstant_NonConsolidatedMember", True, 'millions'),
                'TotalAssetsCurrent': ('jpcrp_cor:TotalAssetsSummaryOfBusinessResults', "CurrentYearInstant_NonConsolidatedMember", True, 'millions'),
                'TotalAssetsQuarter': ('jpcrp_cor:TotalAssetsSummaryOfBusinessResults', "CurrentQuarterInstant_NonConsolidatedMember", True, 'millions'),
                #資本
                'NetAssetsPrior3': ('jpcrp_cor:NetAssetsSummaryOfBusinessResults', "Prior3YearInstant_NonConsolidatedMember", True, 'millions'),
                'NetAssetsPrior2': ('jpcrp_cor:NetAssetsSummaryOfBusinessResults', "Prior2YearInstant_NonConsolidatedMember", True, 'millions'),
                'NetAssetsPrior1': ('jpcrp_cor:NetAssetsSummaryOfBusinessResults', "Prior1YearInstant_NonConsolidatedMember", True, 'millions'),
                'NetAssetsCurrent': ('jpcrp_cor:NetAssetsSummaryOfBusinessResults', "CurrentYearInstant_NonConsolidatedMember", True, 'millions'),
                'NetAssetsQuarter': ('jpcrp_cor:NetAssetsSummaryOfBusinessResults', "CurrentQuarterInstant_NonConsolidatedMember", True, 'millions'),
                #営業CF
                'OperatingCashPrior': ('jppfs_cor:NetCashProvidedByUsedInOperatingActivities', "Prior1YearDuration_NonConsolidatedMember", True, 'millions'),
                'OperatingCashCurrent': ('jppfs_cor:NetCashProvidedByUsedInOperatingActivities', "CurrentYearDuration_NonConsolidatedMember", True, 'millions'),
                'OperatingCashYTD': ('jppfs_cor:NetCashProvidedByUsedInOperatingActivities', "CurrentYTDDuration_NonConsolidatedMember", True, 'millions'),
                #投資CF
                'InvestmentCashPrior': ('jppfs_cor:NetCashProvidedByUsedInInvestmentActivities', "Prior1YearDuration_NonConsolidatedMember", True, 'millions'),
                'InvestmentCashCurrent': ('jppfs_cor:NetCashProvidedByUsedInInvestmentActivities', "CurrentYearDuration_NonConsolidatedMember", True, 'millions'),
                'InvestmentCashYTD': ('jppfs_cor:NetCashProvidedByUsedInInvestmentActivities', "CurrentYTDDuration_NonConsolidatedMember", True, 'millions'),
                #財務CF
                'FinancingCashPrior': ('jppfs_cor:NetCashProvidedByUsedInFinancingActivities', "Prior1YearDuration_NonConsolidatedMember", True, 'millions'),
                'FinancingCashCurrent': ('jppfs_cor:NetCashProvidedByUsedInFinancingActivities', "CurrentYearDuration_NonConsolidatedMember", True, 'millions'),
                'FinancingCashYTD': ('jppfs_cor:NetCashProvidedByUsedInFinancingActivities', "CurrentYTDDuration_NonConsolidatedMember", True, 'millions'),
                #FCF
                'CashAndCashEquivalentsPrior': ('jppfs_cor:CashAndCashEquivalents', "Prior1YearInstant_NonConsolidatedMember", True, 'millions'),
                'CashAndCashEquivalentsCurrent': ('jppfs_cor:CashAndCashEquivalents', "CurrentYearInstant_NonConsolidatedMember", True, 'millions'),
                'CashAndCashEquivalentsQuarter': ('jppfs_cor:CashAndCashEquivalents', "CurrentQuarterInstant_NonConsolidatedMember", True, 'millions'),
                #証券コード
                'SecurityCodeDEI': ('jpdei_cor:SecurityCodeDEI', "FilingDateInstant", True, 'ten'),
                #会社名
                'CompanyNameCoverPage': ('jpcrp_cor:CompanyNameCoverPage', "FilingDateInstant", False, 'millions'),
                #当会計期間終了日
                'CurrentPeriodEndDateDEIdate': ('jpdei_cor:CurrentPeriodEndDateDEI', "FilingDateInstant", False, 'date'),
                #当事業年度終了日
                'CurrentFiscalYearEndDateDEIyear': ('jpdei_cor:CurrentFiscalYearEndDateDEI', "FilingDateInstant", False, 'year'),
                'CurrentFiscalYearEndDateDEImonth': ('jpdei_cor:CurrentFiscalYearEndDateDEI', "FilingDateInstant", False, 'month')
            },
            'cell_map_file1': {
                #売上高
                'NetSalesPrior': 'D5', 'NetSalesCurrent': 'G5',
                #売上原価
                'CostOfSalesPrior': 'D6', 'CostOfSalesCurrent': 'G6',
                #売上総利益
                'GrossProfitPrior': 'D7', 'GrossProfitCurrent': 'G7',
                #販管費
                'SellingExpensesPrior': 'D8', 'SellingExpensesCurrent': 'G8',
                #営業利益
                'OperatingIncomePrior': 'D9', 'OperatingIncomeCurrent': 'G9',
                #経常利益
                'OrdinaryIncomePrior': 'D10', 'OrdinaryIncomeCurrent': 'G10',
                #純利益
                'NetIncomePrior': 'D11', 'NetIncomeCurrent': 'G11',
                #営業CF
                'OperatingCashPrior': 'C21', 'OperatingCashCurrent': 'F21',
                #投資CF
                'InvestmentCashPrior': 'D21', 'InvestmentCashCurrent': 'G21',
                #財務CF
                'FinancingCashPrior': 'E21', 'FinancingCashCurrent': 'H21',
                #FCF
                'CashAndCashEquivalentsPrior': 'D22', 'CashAndCashEquivalentsCurrent': 'G22'
            },
            'cell_map_file2': {
                #売上高
                'NetSalesPrior': 'J5', 'NetSalesCurrent': 'M5',
                #売上原価
                'CostOfSalesPrior': 'J6', 'CostOfSalesCurrent': 'M6',
                #売上総利益
                'GrossProfitPrior': 'J7', 'GrossProfitCurrent': 'M7',
                #販管費
                'SellingExpensesPrior': 'J8', 'SellingExpensesCurrent': 'M8',
                #営業利益
                'OperatingIncomePrior': 'J9', 'OperatingIncomeCurrent': 'M9',
                #経常利益
                'OrdinaryIncomePrior': 'J10', 'OrdinaryIncomeCurrent': 'M10',
                #純利益
                'ProfitLossPrior': 'J11', 'ProfitLossCurrent': 'M11',
                #発行株数
                'TotalNumberPrior3': 'D13', 'TotalNumberPrior2': 'G13', 
                'TotalNumberPrior1': 'J13', 'TotalNumberCurrent': 'M13',
                #資産
                'TotalAssetsPrior3': 'C17', 'TotalAssetsPrior2': 'F17',
                'TotalAssetsPrior1': 'I17', 'TotalAssetsCurrent': 'L17', 
                #資本
                'NetAssetsPrior3': 'D17', 'NetAssetsPrior2': 'G17',
                'NetAssetsPrior1': 'J17', 'NetAssetsCurrent': 'M17',
                #営業CF
                'OperatingCashPrior': 'I21', 'OperatingCashCurrent': 'L21',
                #投資CF
                'InvestmentCashPrior': 'J21', 'InvestmentCashCurrent': 'M21',
                #財務CF
                'FinancingCashPrior': 'K21', 'FinancingCashCurrent': 'N21',
                #FCF
                'CashAndCashEquivalentsPrior': 'J22', 'CashAndCashEquivalentsCurrent': 'M22'
            },
            'cell_map_file3': {
                #売上高（半期）
                'NetSalesYTD': 'J36',
                #経常利益（半期）
                'OrdinaryIncomeYTD': 'J37',
                #純利益（半期）
                'ProfitLossYTD': 'J38',
                #発行株数
                'TotalNumberQuarter': 'J40',
                #資産
                'TotalAssetsQuarter': 'I44',
                #資本
                'NetAssetsQuarter': 'J44',
                #営業CF（半期）
                'OperatingCashYTD': 'I48',
                #投資CF（半期）
                'InvestmentCashYTD': 'J48',
                #財務CF（半期）
                'FinancingCashYTD': 'K48',
                #FCF（半期）
                'CashAndCashEquivalentsQuarter': 'J49',    
                #証券コード
                'SecurityCodeDEI': 'K2',
                #会社名
                'CompanyNameCoverPage': 'L2',
                #当会計期間終了日
                'CurrentFiscalYearEndDateDEIyear': 'N2',
                'CurrentFiscalYearEndDateDEImonth': 'O2'
            }
        }
    else:
        return {
            'tags_contexts': {
            #連結決算の場合
                #売上高
                'NetSalesPrior': ('jppfs_cor:NetSales', "Prior1YearDuration", True, 'millions'),
                'NetSalesCurrent': ('jppfs_cor:NetSales', "CurrentYearDuration", True, 'millions'),
                'NetSalesYTD': ('jppfs_cor:NetSales', "CurrentYTDDuration", True, 'millions'),
                #売上原価
                'CostOfSalesPrior': ('jppfs_cor:CostOfSales', "Prior1YearDuration", True, 'millions'),
                'CostOfSalesCurrent': ('jppfs_cor:CostOfSales', "CurrentYearDuration", True, 'millions'),
                #売上総利益
                'GrossProfitPrior': ('jppfs_cor:GrossProfit', "Prior1YearDuration", True, 'millions'),
                'GrossProfitCurrent': ('jppfs_cor:GrossProfit', "CurrentYearDuration", True, 'millions'),
                #販管費
                'SellingExpensesPrior': ('jppfs_cor:SellingGeneralAndAdministrativeExpenses', "Prior1YearDuration", True, 'millions'),
                'SellingExpensesCurrent': ('jppfs_cor:SellingGeneralAndAdministrativeExpenses', "CurrentYearDuration", True, 'millions'),
                #営業利益
                'OperatingIncomePrior': ('jppfs_cor:OperatingIncome', "Prior1YearDuration", True, 'millions'),
                'OperatingIncomeCurrent': ('jppfs_cor:OperatingIncome', "CurrentYearDuration", True, 'millions'),
                #経常利益
                'OrdinaryIncomePrior': ('jppfs_cor:OrdinaryIncome', "Prior1YearDuration", True, 'millions'),
                'OrdinaryIncomeCurrent': ('jppfs_cor:OrdinaryIncome', "CurrentYearDuration", True, 'millions'),
                'OrdinaryIncomeYTD': ('jppfs_cor:OrdinaryIncome', "CurrentYTDDuration", True, 'millions'),
                #純利益
                'ProfitLossPrior': ('jppfs_cor:ProfitLoss', "Prior1YearDuration", True, 'millions'),
                'ProfitLossCurrent': ('jppfs_cor:ProfitLoss', "CurrentYearDuration", True, 'millions'),
                'ProfitLossYTD': ('jppfs_cor:ProfitLoss', "CurrentYTDDuration", True, 'millions'),
                #発行株数
                'TotalNumberPrior3': ('jpcrp_cor:TotalNumberOfIssuedSharesSummaryOfBusinessResults', "Prior3YearInstant_NonConsolidatedMember", True, 'thousands'),
                'TotalNumberPrior2': ('jpcrp_cor:TotalNumberOfIssuedSharesSummaryOfBusinessResults', "Prior2YearInstant_NonConsolidatedMember", True, 'thousands'),
                'TotalNumberPrior1': ('jpcrp_cor:TotalNumberOfIssuedSharesSummaryOfBusinessResults', "Prior1YearInstant_NonConsolidatedMember", True, 'thousands'),
                'TotalNumberCurrent': ('jpcrp_cor:TotalNumberOfIssuedSharesSummaryOfBusinessResults', "CurrentYearInstant_NonConsolidatedMember", True, 'thousands'),
                'TotalNumberFiling': ('jpcrp_cor:NumberOfIssuedSharesAsOfFiscalYearEndIssuedSharesTotalNumberOfSharesEtc', "FilingDateInstant_OrdinaryShareMember", True, 'thousands'),
                #資産
                'TotalAssetsPrior3': ('jpcrp_cor:TotalAssetsSummaryOfBusinessResults', "Prior3YearInstant", True, 'millions'),
                'TotalAssetsPrior2': ('jpcrp_cor:TotalAssetsSummaryOfBusinessResults', "Prior2YearInstant", True, 'millions'),
                'TotalAssetsPrior1': ('jpcrp_cor:TotalAssetsSummaryOfBusinessResults', "Prior1YearInstant", True, 'millions'),
                'TotalAssetsCurrent': ('jpcrp_cor:TotalAssetsSummaryOfBusinessResults', "CurrentYearInstant", True, 'millions'),
                'TotalAssetsQuarter': ('jpcrp_cor:TotalAssetsSummaryOfBusinessResults', "CurrentQuarterInstant", True, 'millions'),
                #資本
                'NetAssetsPrior3': ('jpcrp_cor:NetAssetsSummaryOfBusinessResults', "Prior3YearInstant", True, 'millions'),
                'NetAssetsPrior2': ('jpcrp_cor:NetAssetsSummaryOfBusinessResults', "Prior2YearInstant", True, 'millions'),
                'NetAssetsPrior1': ('jpcrp_cor:NetAssetsSummaryOfBusinessResults', "Prior1YearInstant", True, 'millions'),
                'NetAssetsCurrent': ('jpcrp_cor:NetAssetsSummaryOfBusinessResults', "CurrentYearInstant", True, 'millions'),
                'NetAssetsQuarter': ('jpcrp_cor:NetAssetsSummaryOfBusinessResults', "CurrentQuarterInstant", True, 'millions'),
                #営業CF
                'OperatingCashPrior': ('jppfs_cor:NetCashProvidedByUsedInOperatingActivities', "Prior1YearDuration", True, 'millions'),
                'OperatingCashCurrent': ('jppfs_cor:NetCashProvidedByUsedInOperatingActivities', "CurrentYearDuration", True, 'millions'),
                'OperatingCashYTD': ('jppfs_cor:NetCashProvidedByUsedInOperatingActivities', "CurrentYTDDuration", True, 'millions'),
                #投資CF
                'InvestmentCashPrior': ('jppfs_cor:NetCashProvidedByUsedInInvestmentActivities', "Prior1YearDuration", True, 'millions'),
                'InvestmentCashCurrent': ('jppfs_cor:NetCashProvidedByUsedInInvestmentActivities', "CurrentYearDuration", True, 'millions'),
                'InvestmentCashYTD': ('jppfs_cor:NetCashProvidedByUsedInInvestmentActivities', "CurrentYTDDuration", True, 'millions'),
                #財務CF
                'FinancingCashPrior': ('jppfs_cor:NetCashProvidedByUsedInFinancingActivities', "Prior1YearDuration", True, 'millions'),
                'FinancingCashCurrent': ('jppfs_cor:NetCashProvidedByUsedInFinancingActivities', "CurrentYearDuration", True, 'millions'),
                'FinancingCashYTD': ('jppfs_cor:NetCashProvidedByUsedInFinancingActivities', "CurrentYTDDuration", True, 'millions'),
                #FCF
                'CashAndCashEquivalentsPrior': ('jpcrp_cor:CashAndCashEquivalentsSummaryOfBusinessResults', "Prior1YearInstant", True, 'millions'),
                'CashAndCashEquivalentsCurrent': ('jpcrp_cor:CashAndCashEquivalentsSummaryOfBusinessResults', "CurrentYearInstant", True, 'millions'),
                'CashAndCashEquivalentsQuarter': ('jppfs_cor:CashAndCashEquivalents', "CurrentQuarterInstant", True, 'millions'),
                #証券コード
                'SecurityCodeDEI': ('jpdei_cor:SecurityCodeDEI', "FilingDateInstant", True, 'ten'),
                #会社名
                'CompanyNameCoverPage': ('jpcrp_cor:CompanyNameCoverPage', "FilingDateInstant", False, 'millions'),
                #当会計期間終了日
                'CurrentPeriodEndDateDEIdate': ('jpdei_cor:CurrentPeriodEndDateDEI', "FilingDateInstant", False, 'date'),
                #当事業年度終了日
                'CurrentFiscalYearEndDateDEIyear': ('jpdei_cor:CurrentFiscalYearEndDateDEI', "FilingDateInstant", False, 'year'),
                'CurrentFiscalYearEndDateDEImonth': ('jpdei_cor:CurrentFiscalYearEndDateDEI', "FilingDateInstant", False, 'month')
            },
            'cell_map_file1': {
                #売上高
                'NetSalesPrior': 'D5', 'NetSalesCurrent': 'G5',
                #売上原価
                'CostOfSalesPrior': 'D6', 'CostOfSalesCurrent': 'G6',
                #売上総利益
                'GrossProfitPrior': 'D7', 'GrossProfitCurrent': 'G7',
                #販管費
                'SellingExpensesPrior': 'D8', 'SellingExpensesCurrent': 'G8',
                #営業利益
                'OperatingIncomePrior': 'D9', 'OperatingIncomeCurrent': 'G9',
                #経常利益
                'OrdinaryIncomePrior': 'D10', 'OrdinaryIncomeCurrent': 'G10',
                #経常利益
                'ProfitLossPrior': 'D11', 'ProfitLossCurrent': 'G11',
                #営業CF
                'OperatingCashPrior': 'C21', 'OperatingCashCurrent': 'F21',
                #投資CF
                'InvestmentCashPrior': 'D21', 'InvestmentCashCurrent': 'G21',
                #財務CF
                'FinancingCashPrior': 'E21', 'FinancingCashCurrent': 'H21',
                #FCF
                'CashAndCashEquivalentsPrior': 'D22', 'CashAndCashEquivalentsCurrent': 'G22'
            },
            'cell_map_file2': {
                #売上高
                'NetSalesPrior': 'J5', 'NetSalesCurrent': 'M5',
                #売上原価
                'CostOfSalesPrior': 'J6', 'CostOfSalesCurrent': 'M6',
                #売上総利益
                'GrossProfitPrior': 'J7', 'GrossProfitCurrent': 'M7',
                #販管費
                'SellingExpensesPrior': 'J8', 'SellingExpensesCurrent': 'M8',
                #営業利益
                'OperatingIncomePrior': 'J9', 'OperatingIncomeCurrent': 'M9',
                #経常利益
                'OrdinaryIncomePrior': 'J10', 'OrdinaryIncomeCurrent': 'M10',
                #純利益
                'ProfitLossPrior': 'J11', 'ProfitLossCurrent': 'M11',
                #発行株数
                'TotalNumberPrior3': 'D13', 'TotalNumberPrior2': 'G13', 
                'TotalNumberPrior1': 'J13', 'TotalNumberCurrent': 'M13',
                #資産
                'TotalAssetsPrior3': 'C17', 'TotalAssetsPrior2': 'F17',
                'TotalAssetsPrior1': 'I17', 'TotalAssetsCurrent': 'L17', 
                #資本
                'NetAssetsPrior3': 'D17', 'NetAssetsPrior2': 'G17',
                'NetAssetsPrior1': 'J17', 'NetAssetsCurrent': 'M17',
                #営業CF
                'OperatingCashPrior': 'I21', 'OperatingCashCurrent': 'L21',
                #投資CF
                'InvestmentCashPrior': 'J21', 'InvestmentCashCurrent': 'M21',
                #財務CF
                'FinancingCashPrior': 'K21', 'FinancingCashCurrent': 'N21',
                #FCF
                'CashAndCashEquivalentsPrior': 'J22', 'CashAndCashEquivalentsCurrent': 'M22'
            },
            'cell_map_file3': {
                #売上高（半期）
                'NetSalesYTD': 'J36',
                #経常利益（半期）
                'OrdinaryIncomeYTD': 'J37',
                #純利益（半期）
                'ProfitLossYTD': 'J38',
                #発行株数
                'TotalNumberFiling': 'J40',
                #資産
                'TotalAssetsQuarter': 'I44',
                #資本
                'NetAssetsQuarter': 'J44',
                #営業CF（半期）
                'OperatingCashYTD': 'I48',
                #投資CF（半期）
                'InvestmentCashYTD': 'J48',
                #財務CF（半期）
                'FinancingCashYTD': 'K48',
                #FCF（半期）
                'CashAndCashEquivalentsQuarter': 'J49',
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
            'file1': glob.glob(os.path.join(base_dir, '1-2*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '1-4*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '1-5*.xbrl'))
        },
        'excel_file_path': r'C:\Users\users\OneDrive\PC\EDINET\決算分析シート\決算分析シート1.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '2-2*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '2-4*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '2-5*.xbrl'))
        },
        'excel_file_path': r'C:\Users\users\OneDrive\PC\EDINET\決算分析シート\決算分析シート2.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '3-2*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '3-4*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '3-5*.xbrl'))
        },
        'excel_file_path': r'C:\Users\users\OneDrive\PC\EDINET\決算分析シート\決算分析シート3.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '4-2*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '4-4*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '4-5*.xbrl'))
        },
        'excel_file_path': r'C:\Users\users\OneDrive\PC\EDINET\決算分析シート\決算分析シート4.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '5-2*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '5-4*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '5-5*.xbrl'))
        },
        'excel_file_path': r'C:\Users\users\OneDrive\PC\EDINET\決算分析シート\決算分析シート5.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '6-2*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '6-4*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '6-5*.xbrl'))
        },
        'excel_file_path': r'C:\Users\users\OneDrive\PC\EDINET\決算分析シート\決算分析シート6.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '7-2*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '7-4*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '7-5*.xbrl'))
        },
        'excel_file_path': r'C:\Users\users\OneDrive\PC\EDINET\決算分析シート\決算分析シート7.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '8-2*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '8-4*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '8-5*.xbrl'))
        },
        'excel_file_path': r'C:\Users\users\OneDrive\PC\EDINET\決算分析シート\決算分析シート8.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '9-2*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '9-4*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '9-5*.xbrl'))
        },
        'excel_file_path': r'C:\Users\users\OneDrive\PC\EDINET\決算分析シート\決算分析シート9.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '10-2*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '10-4*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '10-5*.xbrl'))
        },
        'excel_file_path': r'C:\Users\users\OneDrive\PC\EDINET\決算分析シート\決算分析シート10.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '11-2*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '11-4*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '11-5*.xbrl'))
        },
        'excel_file_path': r'C:\Users\users\OneDrive\PC\EDINET\決算分析シート\決算分析シート11.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '12-2*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '12-4*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '12-5*.xbrl'))
        },
        'excel_file_path': r'C:\Users\users\OneDrive\PC\EDINET\決算分析シート\決算分析シート12.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '13-2*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '13-4*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '13-5*.xbrl'))
        },
        'excel_file_path': r'C:\Users\users\OneDrive\PC\EDINET\決算分析シート\決算分析シート13.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '14-2*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '14-4*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '14-5*.xbrl'))
        },
        'excel_file_path': r'C:\Users\users\OneDrive\PC\EDINET\決算分析シート\決算分析シート14.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '15-2*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '15-4*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '15-5*.xbrl'))
        },
        'excel_file_path': r'C:\Users\users\OneDrive\PC\EDINET\決算分析シート\決算分析シート15.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '16-2*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '16-4*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '16-5*.xbrl'))
        },
        'excel_file_path': r'C:\Users\users\OneDrive\PC\EDINET\決算分析シート\決算分析シート16.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '17-2*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '17-4*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '17-5*.xbrl'))
        },
        'excel_file_path': r'C:\Users\users\OneDrive\PC\EDINET\決算分析シート\決算分析シート17.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '18-2*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '18-4*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '18-5*.xbrl'))
        },
        'excel_file_path': r'C:\Users\users\OneDrive\PC\EDINET\決算分析シート\決算分析シート18.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '19-2*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '19-4*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '19-5*.xbrl'))
        },
        'excel_file_path': r'C:\Users\users\OneDrive\PC\EDINET\決算分析シート\決算分析シート19.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '20-2*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '20-4*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '20-5*.xbrl'))
        },
        'excel_file_path': r'C:\Users\users\OneDrive\PC\EDINET\決算分析シート\決算分析シート20.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '21-2*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '21-4*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '21-5*.xbrl'))
        },
        'excel_file_path': r'C:\Users\users\OneDrive\PC\EDINET\決算分析シート\決算分析シート21.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '22-2*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '22-4*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '22-5*.xbrl'))
        },
        'excel_file_path': r'C:\Users\users\OneDrive\PC\EDINET\決算分析シート\決算分析シート22.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '23-2*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '23-4*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '23-5*.xbrl'))
        },
        'excel_file_path': r'C:\Users\users\OneDrive\PC\EDINET\決算分析シート\決算分析シート23.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '24-2*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '24-4*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '24-5*.xbrl'))
        },
        'excel_file_path': r'C:\Users\users\OneDrive\PC\EDINET\決算分析シート\決算分析シート24.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '25-2*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '25-4*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '25-5*.xbrl'))
        },
        'excel_file_path': r'C:\Users\users\OneDrive\PC\EDINET\決算分析シート\決算分析シート25.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '26-2*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '26-4*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '26-5*.xbrl'))
        },
        'excel_file_path': r'C:\Users\users\OneDrive\PC\EDINET\決算分析シート\決算分析シート26.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '27-2*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '27-4*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '27-5*.xbrl'))
        },
        'excel_file_path': r'C:\Users\users\OneDrive\PC\EDINET\決算分析シート\決算分析シート27.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '28-2*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '28-4*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '28-5*.xbrl'))
        },
        'excel_file_path': r'C:\Users\users\OneDrive\PC\EDINET\決算分析シート\決算分析シート28.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '29-2*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '29-4*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '29-5*.xbrl'))
        },
        'excel_file_path': r'C:\Users\users\OneDrive\PC\EDINET\決算分析シート\決算分析シート29.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '30-2*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '30-4*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '30-5*.xbrl'))
        },
        'excel_file_path': r'C:\Users\users\OneDrive\PC\EDINET\決算分析シート\決算分析シート30.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '31-2*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '31-4*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '31-5*.xbrl'))
        },
        'excel_file_path': r'C:\Users\users\OneDrive\PC\EDINET\決算分析シート\決算分析シート31.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '32-2*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '32-4*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '32-5*.xbrl'))
        },
        'excel_file_path': r'C:\Users\users\OneDrive\PC\EDINET\決算分析シート\決算分析シート32.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '33-2*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '33-4*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '33-5*.xbrl'))
        },
        'excel_file_path': r'C:\Users\users\OneDrive\PC\EDINET\決算分析シート\決算分析シート33.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '34-2*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '34-4*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '34-5*.xbrl'))
        },
        'excel_file_path': r'C:\Users\users\OneDrive\PC\EDINET\決算分析シート\決算分析シート34.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '35-2*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '35-4*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '35-5*.xbrl'))
        },
        'excel_file_path': r'C:\Users\users\OneDrive\PC\EDINET\決算分析シート\決算分析シート35.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '36-2*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '36-4*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '36-5*.xbrl'))
        },
        'excel_file_path': r'C:\Users\users\OneDrive\PC\EDINET\決算分析シート\決算分析シート36.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '37-2*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '37-4*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '37-5*.xbrl'))
        },
        'excel_file_path': r'C:\Users\users\OneDrive\PC\EDINET\決算分析シート\決算分析シート37.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '38-2*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '38-4*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '38-5*.xbrl'))
        },
        'excel_file_path': r'C:\Users\users\OneDrive\PC\EDINET\決算分析シート\決算分析シート38.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '39-2*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '39-4*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '39-5*.xbrl'))
        },
        'excel_file_path': r'C:\Users\users\OneDrive\PC\EDINET\決算分析シート\決算分析シート39.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '40-2*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '40-4*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '40-5*.xbrl'))
        },
        'excel_file_path': r'C:\Users\users\OneDrive\PC\EDINET\決算分析シート\決算分析シート40.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '41-2*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '41-4*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '41-5*.xbrl'))
        },
        'excel_file_path': r'C:\Users\users\OneDrive\PC\EDINET\決算分析シート\決算分析シート41.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '42-2*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '42-4*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '42-5*.xbrl'))
        },
        'excel_file_path': r'C:\Users\users\OneDrive\PC\EDINET\決算分析シート\決算分析シート42.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '43-2*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '43-4*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '43-5*.xbrl'))
        },
        'excel_file_path': r'C:\Users\users\OneDrive\PC\EDINET\決算分析シート\決算分析シート43.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '44-2*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '44-4*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '44-5*.xbrl'))
        },
        'excel_file_path': r'C:\Users\users\OneDrive\PC\EDINET\決算分析シート\決算分析シート44.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '45-2*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '45-4*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '45-5*.xbrl'))
        },
        'excel_file_path': r'C:\Users\users\OneDrive\PC\EDINET\決算分析シート\決算分析シート45.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '46-2*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '46-4*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '46-5*.xbrl'))
        },
        'excel_file_path': r'C:\Users\users\OneDrive\PC\EDINET\決算分析シート\決算分析シート46.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '47-2*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '47-4*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '47-5*.xbrl'))
        },
        'excel_file_path': r'C:\Users\users\OneDrive\PC\EDINET\決算分析シート\決算分析シート47.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '48-2*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '48-4*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '48-5*.xbrl'))
        },
        'excel_file_path': r'C:\Users\users\OneDrive\PC\EDINET\決算分析シート\決算分析シート48.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '49-2*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '49-4*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '49-5*.xbrl'))
        },
        'excel_file_path': r'C:\Users\users\OneDrive\PC\EDINET\決算分析シート\決算分析シート49.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '50-2*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '50-4*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '50-5*.xbrl'))
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
        print(f"ーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーー\n使用するExcelファイル: {selected_file}")
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
        mappings = select_mapping(False)  
        tags_contexts = mappings['tags_contexts']
        # XBRLファイルからデータを解析
        try:
            xbrl_data, has_non_consolidated_member, security_code = parse_xbrl_data(path, tags_contexts)
        except Exception as e:
            skipped_files.append({
                'reason': f"XBRLデータの解析中にエラー: {e}",
                'file': path
            })
            print(f"{key} のファイル {path} を解析中にエラーが発生しました。スキップします。")
            continue

        mappings = select_mapping(has_non_consolidated_member)
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