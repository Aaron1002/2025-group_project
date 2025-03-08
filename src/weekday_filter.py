import pandas as pd
from datetime import datetime, timedelta

# 產生 2024 年所有平日的日期
def get_weekdays_of_2024():
    start_date = datetime(2024, 1, 1)
    end_date = datetime(2024, 12, 31)
    weekdays = []
    
    current_date = start_date
    while current_date <= end_date:
        if current_date.weekday() < 5:  # 0=Monday, 4=Friday
            weekdays.append(current_date.strftime('%Y-%m-%d'))
        current_date += timedelta(days=1)
    
    return weekdays

# 讀取 CSV 並過濾指定日期的資料
def filter_csv_by_weekdays(csv_file, output_file):
    weekdays = get_weekdays_of_2024()
    print("確認這些是 2024 年的所有平日:")
    print(weekdays)
    
    df = pd.read_csv(csv_file)
    df['Date'] = pd.to_datetime(df['DataCollectTime']).dt.strftime('%Y-%m-%d')
    
    filtered_df = df[df['Date'].isin(weekdays)]
    filtered_df.drop(columns=['Date'], inplace=True)
    
    filtered_df.to_csv(output_file, index=False)
    print(f"已篩選並輸出新的 CSV: {output_file}")

# 使用範例
input_csv = r'input_file'  # 你的原始 CSV 檔案
output_csv = 'filtered_output.csv'  # 產生的 CSV 檔案
filter_csv_by_weekdays(input_csv, output_csv)
