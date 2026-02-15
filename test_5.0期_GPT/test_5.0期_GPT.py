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
                #決算報告書より
                'NetSalesPrior': ('jppfs_cor:NetSales', "Prior1YearDuration_NonConsolidatedMember", True, 'millions'),
                'NetSalesCurrent': ('jppfs_cor:NetSales', "CurrentYearDuration_NonConsolidatedMember", True, 'millions'),
                'CostOfSalesPrior': ('jppfs_cor:CostOfSales', "Prior1YearDuration_NonConsolidatedMember", True, 'millions'),
                'CostOfSalesCurrent': ('jppfs_cor:CostOfSales', "CurrentYearDuration_NonConsolidatedMember", True, 'millions'),
                'SecurityCodeDEI': ('jpdei_cor:SecurityCodeDEI', "FilingDateInstant", True, 'ten'),
                'CompanyNameCoverPage': ('jpcrp_cor:CompanyNameCoverPage', "FilingDateInstant", False, 'millions'),
                'CurrentPeriodEndDateDEIdate': ('jpdei_cor:CurrentPeriodEndDateDEI', "FilingDateInstant", False, 'date'),
                'CurrentFiscalYearEndDateDEIyear': ('jpdei_cor:CurrentFiscalYearEndDateDEI', "FilingDateInstant", False, 'year'),
                'CurrentFiscalYearEndDateDEImonth': ('jpdei_cor:CurrentFiscalYearEndDateDEI', "FilingDateInstant", False, 'month')
            },
            'cell_map_file1': {
                'NetSalesPrior': 'D5', 'NetSalesCurrent': 'G5',
                'CostOfSalesPrior': 'D6', 'CostOfSalesCurrent': 'G6'
            },
            'cell_map_file2': {
                'NetSalesPrior': 'J5', 'NetSalesCurrent': 'M5',
                'CostOfSalesPrior': 'J6', 'CostOfSalesCurrent': 'M6'
            },
            'cell_map_file3': {
                'NetSalesCurrent': ['P30', 'P36'],
                'CostOfSalesCurrent': 'P6',    
                'SecurityCodeDEI': 'K2',
                'CompanyNameCoverPage': 'L2',
                'CurrentFiscalYearEndDateDEIyear': 'N2',
                'CurrentFiscalYearEndDateDEImonth': 'O2'
            }
        }
    else:
        return {
            'tags_contexts': {
                'NetSalesPrior': ('jppfs_cor:NetSales', "Prior1YearDuration", True, 'millions'),
                'NetSalesCurrent': ('jppfs_cor:NetSales', "CurrentYearDuration", True, 'millions'),
                'CostOfSalesPrior': ('jppfs_cor:CostOfSales', "Prior1YearDuration", True, 'millions'),
                'CostOfSalesCurrent': ('jppfs_cor:CostOfSales', "CurrentYearDuration", True, 'millions'),
                'SecurityCodeDEI': ('jpdei_cor:SecurityCodeDEI', "FilingDateInstant", True, 'ten'),
                'CompanyNameCoverPage': ('jpcrp_cor:CompanyNameCoverPage', "FilingDateInstant", False, 'millions'),
                'CurrentPeriodEndDateDEIdate': ('jpdei_cor:CurrentPeriodEndDateDEI', "FilingDateInstant", False, 'date'),
                'CurrentFiscalYearEndDateDEIyear': ('jpdei_cor:CurrentFiscalYearEndDateDEI', "FilingDateInstant", False, 'year'),
                'CurrentFiscalYearEndDateDEImonth': ('jpdei_cor:CurrentFiscalYearEndDateDEI', "FilingDateInstant", False, 'month')
            },
            'cell_map_file1': {
                'NetSalesPrior': 'D5', 'NetSalesCurrent': 'G5',
                'CostOfSalesPrior': 'D6', 'CostOfSalesCurrent': 'G6'
            },
            'cell_map_file2': {
                'NetSalesPrior': 'J5', 'NetSalesCurrent': 'M5',
                'CostOfSalesPrior': 'J6', 'CostOfSalesCurrent': 'M6'
            },
            'cell_map_file3': {
                'NetSalesCurrent': ['P30', 'P36'],
                'CostOfSalesCurrent': 'P6',    
                'SecurityCodeDEI': 'K2',
                'CompanyNameCoverPage': 'L2',
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
        'excel_file_path': r'C:\Users\Owner\OneDrive\PC\EDINET\決算分析シート\決算分析シート1.xlsx'
    },
    {
        'xbrl_file_paths': {
            'file1': glob.glob(os.path.join(base_dir, '2-1*.xbrl')),
            'file2': glob.glob(os.path.join(base_dir, '2-3*.xbrl')),
            'file3': glob.glob(os.path.join(base_dir, '2-4*.xbrl'))
        },
        'excel_file_path': r'C:\Users\Owner\OneDrive\PC\EDINET\決算分析シート\決算分析シート2.xlsx'
    }
]

# 処理するファイル数を選択
def choose_file_count():
    while True:
        try:
            count = int(input("処理するファイルの数を選択してください（1～2）: "))
            if 1 <= count <= 2:
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