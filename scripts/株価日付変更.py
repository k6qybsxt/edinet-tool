import json
from datetime import datetime
from dateutil.relativedelta import relativedelta

data = {
    "1": [
        {"target_date": "2020-01-31", "backup_date": "2020-01-30", "cell": "D26"},
        {"target_date": "2021-01-31", "backup_date": "2021-01-30", "cell": "G26"},
        {"target_date": "2022-01-31", "backup_date": "2022-01-30", "cell": "J26"},
        {"target_date": "2023-01-31", "backup_date": "2023-01-30", "cell": "M26"},
        {"target_date": "2023-04-30", "backup_date": "2023-04-29", "cell": "G53"},
        {"target_date": "2023-07-31", "backup_date": "2023-07-30", "cell": "J53"},
        {"target_date": "2023-10-31", "backup_date": "2023-10-30", "cell": "M53"},
        {"target_date": "2024-01-31", "backup_date": "2024-01-30", "cell": "P53"}
    ],
    "2": [
        {"target_date": "2020-02-28", "backup_date": "2020-02-27", "cell": "D26"},
        {"target_date": "2021-02-28", "backup_date": "2021-02-27", "cell": "G26"},
        {"target_date": "2022-02-28", "backup_date": "2022-02-27", "cell": "J26"},
        {"target_date": "2023-02-28", "backup_date": "2023-02-27", "cell": "M26"},
        {"target_date": "2023-05-31", "backup_date": "2023-05-30", "cell": "G53"},
        {"target_date": "2023-08-31", "backup_date": "2023-08-30", "cell": "J53"},
        {"target_date": "2023-11-30", "backup_date": "2023-11-29", "cell": "M53"},
        {"target_date": "2024-02-28", "backup_date": "2024-02-27", "cell": "P53"}
    ],
    "3": [
        {"target_date": "2020-03-31", "backup_date": "2020-03-30", "cell": "D26"},
        {"target_date": "2021-03-31", "backup_date": "2021-03-30", "cell": "G26"},
        {"target_date": "2022-03-31", "backup_date": "2022-03-30", "cell": "J26"},
        {"target_date": "2023-03-31", "backup_date": "2023-03-30", "cell": "M26"},
        {"target_date": "2023-06-30", "backup_date": "2023-06-29", "cell": "G53"},
        {"target_date": "2023-09-30", "backup_date": "2023-09-29", "cell": "J53"},
        {"target_date": "2023-12-31", "backup_date": "2023-12-30", "cell": "M53"},
        {"target_date": "2024-03-31", "backup_date": "2024-03-30", "cell": "P53"}
    ],
    "4": [
        {"target_date": "2020-04-30", "backup_date": "2020-04-29", "cell": "D26"},
        {"target_date": "2022-04-30", "backup_date": "2022-04-29", "cell": "J26"},
        {"target_date": "2021-04-30", "backup_date": "2021-04-29", "cell": "G26"},
        {"target_date": "2023-04-30", "backup_date": "2023-04-29", "cell": "M26"},
        {"target_date": "2023-07-31", "backup_date": "2023-07-30", "cell": "G53"},
        {"target_date": "2023-10-31", "backup_date": "2023-10-30", "cell": "J53"},
        {"target_date": "2024-01-31", "backup_date": "2024-01-30", "cell": "M53"},
        {"target_date": "2024-04-30", "backup_date": "2024-04-29", "cell": "P53"}
    ],
    "5": [
        {"target_date": "2020-05-31", "backup_date": "2020-05-30", "cell": "D26"},
        {"target_date": "2021-05-31", "backup_date": "2021-05-30", "cell": "G26"},
        {"target_date": "2022-05-31", "backup_date": "2022-05-30", "cell": "J26"},
        {"target_date": "2023-05-31", "backup_date": "2023-05-30", "cell": "M26"},
        {"target_date": "2023-08-31", "backup_date": "2023-08-30", "cell": "G53"},
        {"target_date": "2023-11-30", "backup_date": "2023-11-29", "cell": "J53"},
        {"target_date": "2024-02-28", "backup_date": "2024-02-27", "cell": "M53"},
        {"target_date": "2024-05-31", "backup_date": "2024-05-30", "cell": "P53"}
    ],
    "6": [
        {"target_date": "2020-06-30", "backup_date": "2020-06-29", "cell": "D26"},
        {"target_date": "2021-06-30", "backup_date": "2021-06-29", "cell": "G26"},
        {"target_date": "2022-06-30", "backup_date": "2022-06-29", "cell": "J26"},
        {"target_date": "2023-06-30", "backup_date": "2023-06-29", "cell": "M26"},
        {"target_date": "2023-09-30", "backup_date": "2023-09-29", "cell": "G53"},
        {"target_date": "2023-12-31", "backup_date": "2023-12-30", "cell": "J53"},
        {"target_date": "2024-03-31", "backup_date": "2024-03-30", "cell": "M53"},
        {"target_date": "2024-06-30", "backup_date": "2024-06-29", "cell": "P53"}
    ],
    "7": [
        {"target_date": "2020-07-31", "backup_date": "2020-07-30", "cell": "D26"},
        {"target_date": "2021-07-31", "backup_date": "2021-07-30", "cell": "G26"},
        {"target_date": "2022-07-31", "backup_date": "2022-07-30", "cell": "J26"},
        {"target_date": "2023-07-31", "backup_date": "2023-07-30", "cell": "M26"},
        {"target_date": "2023-10-31", "backup_date": "2023-10-30", "cell": "G53"},
        {"target_date": "2024-01-31", "backup_date": "2024-01-30", "cell": "J53"},
        {"target_date": "2024-04-30", "backup_date": "2024-04-29", "cell": "M53"},
        {"target_date": "2024-07-31", "backup_date": "2024-07-30", "cell": "P53"}
    ],
    "8": [
        {"target_date": "2020-08-31", "backup_date": "2020-08-30", "cell": "D26"},
        {"target_date": "2021-08-31", "backup_date": "2021-08-30", "cell": "G26"},
        {"target_date": "2022-08-31", "backup_date": "2022-08-30", "cell": "J26"},
        {"target_date": "2023-08-31", "backup_date": "2023-08-30", "cell": "M26"},
        {"target_date": "2023-11-30", "backup_date": "2023-11-29", "cell": "G53"},
        {"target_date": "2024-02-28", "backup_date": "2024-02-27", "cell": "J53"},
        {"target_date": "2024-05-31", "backup_date": "2024-05-30", "cell": "M53"},
        {"target_date": "2024-08-31", "backup_date": "2024-08-30", "cell": "P53"}
    ],
    "9": [
        {"target_date": "2020-09-30", "backup_date": "2020-09-29", "cell": "D26"},
        {"target_date": "2021-09-30", "backup_date": "2021-09-29", "cell": "G26"},
        {"target_date": "2022-09-30", "backup_date": "2022-09-29", "cell": "J26"},
        {"target_date": "2023-09-30", "backup_date": "2023-09-29", "cell": "M26"},
        {"target_date": "2023-12-31", "backup_date": "2023-12-30", "cell": "G53"},
        {"target_date": "2024-03-31", "backup_date": "2024-03-30", "cell": "J53"},
        {"target_date": "2024-06-30", "backup_date": "2024-06-29", "cell": "M53"},
        {"target_date": "2024-09-30", "backup_date": "2024-09-29", "cell": "P53"}
    ],
    "10": [
        {"target_date": "2020-10-31", "backup_date": "2020-10-30", "cell": "D26"},
        {"target_date": "2021-10-31", "backup_date": "2021-10-30", "cell": "G26"},
        {"target_date": "2022-10-31", "backup_date": "2022-10-30", "cell": "J26"},
        {"target_date": "2023-10-31", "backup_date": "2023-10-30", "cell": "M26"},
        {"target_date": "2024-01-31", "backup_date": "2024-01-30", "cell": "G53"},
        {"target_date": "2024-04-30", "backup_date": "2024-04-29", "cell": "J53"},
        {"target_date": "2024-07-31", "backup_date": "2024-07-30", "cell": "M53"},
        {"target_date": "2024-10-31", "backup_date": "2024-10-30", "cell": "P53"}
    ],
    "11": [
        {"target_date": "2020-11-30", "backup_date": "2020-11-29", "cell": "D26"},
        {"target_date": "2021-11-30", "backup_date": "2021-11-29", "cell": "G26"},
        {"target_date": "2022-11-30", "backup_date": "2022-11-29", "cell": "J26"},
        {"target_date": "2023-11-30", "backup_date": "2023-11-29", "cell": "M26"},
        {"target_date": "2024-02-28", "backup_date": "2024-02-27", "cell": "G53"},
        {"target_date": "2024-05-31", "backup_date": "2024-05-30", "cell": "J53"},
        {"target_date": "2024-08-31", "backup_date": "2024-08-30", "cell": "M53"},
        {"target_date": "2024-11-30", "backup_date": "2024-11-29", "cell": "P53"}
    ],
    "12": [
        {"target_date": "2020-12-31", "backup_date": "2020-12-30", "cell": "D26"},
        {"target_date": "2021-12-31", "backup_date": "2021-12-30", "cell": "G26"},
        {"target_date": "2022-12-31", "backup_date": "2022-12-30", "cell": "J26"},
        {"target_date": "2023-12-31", "backup_date": "2023-12-30", "cell": "M26"},
        {"target_date": "2024-03-31", "backup_date": "2024-03-30", "cell": "G53"},
        {"target_date": "2024-06-30", "backup_date": "2024-06-29", "cell": "J53"},
        {"target_date": "2024-09-30", "backup_date": "2024-09-29", "cell": "M53"},
        {"target_date": "2024-12-31", "backup_date": "2024-12-30", "cell": "P53"}
    ],

    #上記日付より-1年
    "-1": [
        {"target_date": "2019-01-31", "backup_date": "2019-01-30", "cell": "D26"},
        {"target_date": "2020-01-31", "backup_date": "2020-01-30", "cell": "G26"},
        {"target_date": "2021-01-31", "backup_date": "2021-01-30", "cell": "J26"},
        {"target_date": "2022-01-31", "backup_date": "2022-01-30", "cell": "M26"},
        {"target_date": "2022-04-30", "backup_date": "2022-04-29", "cell": "G53"},
        {"target_date": "2022-07-31", "backup_date": "2022-07-30", "cell": "J53"},
        {"target_date": "2022-10-31", "backup_date": "2022-10-30", "cell": "M53"},
        {"target_date": "2023-01-31", "backup_date": "2023-01-30", "cell": "P53"},
    ],
    "-2": [
        {"target_date": "2019-02-28", "backup_date": "2019-02-27", "cell": "D26"},
        {"target_date": "2020-02-28", "backup_date": "2020-02-27", "cell": "G26"},
        {"target_date": "2021-02-28", "backup_date": "2021-02-27", "cell": "J26"},
        {"target_date": "2022-02-28", "backup_date": "2022-02-27", "cell": "M26"},
        {"target_date": "2022-05-31", "backup_date": "2022-05-30", "cell": "G53"},
        {"target_date": "2022-08-31", "backup_date": "2022-08-30", "cell": "J53"},
        {"target_date": "2022-11-30", "backup_date": "2022-11-29", "cell": "M53"},
        {"target_date": "2023-02-28", "backup_date": "2023-02-27", "cell": "P53"},
    ],
    "-3": [
        {"target_date": "2019-03-31", "backup_date": "2019-03-30", "cell": "D26"},
        {"target_date": "2020-03-31", "backup_date": "2020-03-30", "cell": "G26"},
        {"target_date": "2021-03-31", "backup_date": "2021-03-30", "cell": "J26"},
        {"target_date": "2022-03-31", "backup_date": "2022-03-30", "cell": "M26"},
        {"target_date": "2022-06-30", "backup_date": "2022-06-29", "cell": "G53"},
        {"target_date": "2022-09-30", "backup_date": "2022-09-29", "cell": "J53"},
        {"target_date": "2022-12-31", "backup_date": "2022-12-30", "cell": "M53"},
        {"target_date": "2023-03-31", "backup_date": "2023-03-30", "cell": "P53"},
    ],
    "-4": [
        {"target_date": "2019-04-30", "backup_date": "2019-04-29", "cell": "D26"},
        {"target_date": "2021-04-30", "backup_date": "2021-04-29", "cell": "J26"},
        {"target_date": "2020-04-30", "backup_date": "2020-04-29", "cell": "G26"},
        {"target_date": "2022-04-30", "backup_date": "2022-04-29", "cell": "M26"},
        {"target_date": "2022-07-31", "backup_date": "2022-07-30", "cell": "G53"},
        {"target_date": "2022-10-31", "backup_date": "2022-10-30", "cell": "J53"},
        {"target_date": "2023-01-31", "backup_date": "2023-01-30", "cell": "M53"},
        {"target_date": "2023-04-30", "backup_date": "2023-04-29", "cell": "P53"},
    ],
    "-5": [
        {"target_date": "2019-05-31", "backup_date": "2019-05-30", "cell": "D26"},
        {"target_date": "2020-05-31", "backup_date": "2020-05-30", "cell": "G26"},
        {"target_date": "2021-05-31", "backup_date": "2021-05-30", "cell": "J26"},
        {"target_date": "2022-05-31", "backup_date": "2022-05-30", "cell": "M26"},
        {"target_date": "2022-08-31", "backup_date": "2022-08-30", "cell": "G53"},
        {"target_date": "2022-11-30", "backup_date": "2022-11-29", "cell": "J53"},
        {"target_date": "2023-02-28", "backup_date": "2023-02-27", "cell": "M53"},
        {"target_date": "2023-05-31", "backup_date": "2023-05-30", "cell": "P53"},
    ],
    "-6": [
        {"target_date": "2019-06-30", "backup_date": "2019-06-29", "cell": "D26"},
        {"target_date": "2020-06-30", "backup_date": "2020-06-29", "cell": "G26"},
        {"target_date": "2021-06-30", "backup_date": "2021-06-29", "cell": "J26"},
        {"target_date": "2022-06-30", "backup_date": "2022-06-29", "cell": "M26"},
        {"target_date": "2022-09-30", "backup_date": "2022-09-29", "cell": "G53"},
        {"target_date": "2022-12-31", "backup_date": "2022-12-30", "cell": "J53"},
        {"target_date": "2023-03-31", "backup_date": "2023-03-30", "cell": "M53"},
        {"target_date": "2023-06-30", "backup_date": "2023-06-29", "cell": "P53"},
    ],
    "-7": [
        {"target_date": "2019-07-31", "backup_date": "2019-07-30", "cell": "D26"},
        {"target_date": "2020-07-31", "backup_date": "2020-07-30", "cell": "G26"},
        {"target_date": "2021-07-31", "backup_date": "2021-07-30", "cell": "J26"},
        {"target_date": "2022-07-31", "backup_date": "2022-07-30", "cell": "M26"},
        {"target_date": "2022-10-31", "backup_date": "2022-10-30", "cell": "G53"},
        {"target_date": "2023-01-31", "backup_date": "2023-01-30", "cell": "J53"},
        {"target_date": "2023-04-30", "backup_date": "2023-04-29", "cell": "M53"},
        {"target_date": "2023-07-31", "backup_date": "2023-07-30", "cell": "P53"},
    ],
    "-8": [
        {"target_date": "2019-08-31", "backup_date": "2019-08-30", "cell": "D26"},
        {"target_date": "2020-08-31", "backup_date": "2020-08-30", "cell": "G26"},
        {"target_date": "2021-08-31", "backup_date": "2021-08-30", "cell": "J26"},
        {"target_date": "2022-08-31", "backup_date": "2022-08-30", "cell": "M26"},
        {"target_date": "2022-11-30", "backup_date": "2022-11-29", "cell": "G53"},
        {"target_date": "2023-02-28", "backup_date": "2023-02-27", "cell": "J53"},
        {"target_date": "2023-05-31", "backup_date": "2023-05-30", "cell": "M53"},
        {"target_date": "2023-08-31", "backup_date": "2023-08-30", "cell": "P53"},
    ],
    "-9": [
        {"target_date": "2019-09-30", "backup_date": "2019-09-29", "cell": "D26"},
        {"target_date": "2020-09-30", "backup_date": "2020-09-29", "cell": "G26"},
        {"target_date": "2021-09-30", "backup_date": "2021-09-29", "cell": "J26"},
        {"target_date": "2022-09-30", "backup_date": "2022-09-29", "cell": "M26"},
        {"target_date": "2022-12-31", "backup_date": "2022-12-30", "cell": "G53"},
        {"target_date": "2023-03-31", "backup_date": "2023-03-30", "cell": "J53"},
        {"target_date": "2023-06-30", "backup_date": "2023-06-29", "cell": "M53"},
        {"target_date": "2023-09-30", "backup_date": "2023-09-29", "cell": "P53"},
    ],
    "-10": [
        {"target_date": "2019-10-31", "backup_date": "2019-10-30", "cell": "D26"},
        {"target_date": "2020-10-31", "backup_date": "2020-10-30", "cell": "G26"},
        {"target_date": "2021-10-31", "backup_date": "2021-10-30", "cell": "J26"},
        {"target_date": "2022-10-31", "backup_date": "2022-10-30", "cell": "M26"},
        {"target_date": "2023-01-31", "backup_date": "2023-01-30", "cell": "G53"},
        {"target_date": "2023-04-30", "backup_date": "2023-04-29", "cell": "J53"},
        {"target_date": "2023-07-31", "backup_date": "2023-07-30", "cell": "M53"},
        {"target_date": "2023-10-31", "backup_date": "2023-10-30", "cell": "P53"},
    ],
    "-11": [
        {"target_date": "2019-11-30", "backup_date": "2019-11-29", "cell": "D26"},
        {"target_date": "2020-11-30", "backup_date": "2020-11-29", "cell": "G26"},
        {"target_date": "2021-11-30", "backup_date": "2021-11-29", "cell": "J26"},
        {"target_date": "2022-11-30", "backup_date": "2022-11-29", "cell": "M26"},
        {"target_date": "2023-02-28", "backup_date": "2023-02-27", "cell": "G53"},
        {"target_date": "2023-05-31", "backup_date": "2023-05-30", "cell": "J53"},
        {"target_date": "2023-08-31", "backup_date": "2023-08-30", "cell": "M53"},
        {"target_date": "2023-11-30", "backup_date": "2023-11-29", "cell": "P53"},
    ],
    "-12": [
        {"target_date": "2019-12-31", "backup_date": "2019-12-30", "cell": "D26"},
        {"target_date": "2020-12-31", "backup_date": "2020-12-30", "cell": "G26"},
        {"target_date": "2021-12-31", "backup_date": "2021-12-30", "cell": "J26"},
        {"target_date": "2022-12-31", "backup_date": "2022-12-30", "cell": "M26"},
        {"target_date": "2023-03-31", "backup_date": "2023-03-30", "cell": "G53"},
        {"target_date": "2023-06-30", "backup_date": "2023-06-29", "cell": "J53"},
        {"target_date": "2023-09-30", "backup_date": "2023-09-29", "cell": "M53"},
        {"target_date": "2023-12-31", "backup_date": "2023-12-30", "cell": "P53"}
    ]
}

# 日付を調整する関数
def adjust_dates(data, year_offset):
    adjusted_data = {}
    for month, records in data.items():
        adjusted_records = []
        for record in records:
            target_date = datetime.strptime(record["target_date"], "%Y-%m-%d")
            backup_date = datetime.strptime(record["backup_date"], "%Y-%m-%d")
            adjusted_record = {
                "target_date": (target_date + relativedelta(years=year_offset)).strftime("%Y-%m-%d"),
                "backup_date": (backup_date + relativedelta(years=year_offset)).strftime("%Y-%m-%d"),
                "cell": record["cell"]
            }
            adjusted_records.append(adjusted_record)
        adjusted_data[month] = adjusted_records
    return adjusted_data

# データを表示する関数
def display_data(key, data_to_display):
    print(f'    "{key}": [')
    for entry in data_to_display:
        print(f"        {json.dumps(entry)},")
    print("    ],")

# メイン処理
def main():
    try:
        # 年数の増減を取得
        year_offset = int(input("年数の増減を選択してください（-10～5）: "))
        if not -10 <= year_offset <= 5:
            raise ValueError("範囲外の値です。")
        
        # 日付を調整
        adjusted_data = adjust_dates(data, year_offset)
        
        # 結果を表示
        print("以下のデータが表示される:")
        print("{")
        for key, records in adjusted_data.items():
            display_data(key, records)
        print("}")
    except ValueError as e:
        print(f"入力エラー: {e}")

if __name__ == "__main__":
    main()
