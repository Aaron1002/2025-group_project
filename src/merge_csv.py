import os
import pandas as pd
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

# 設定資料夾路徑
folder_path = r'.\data\unzip_各旅次路徑原始資料 (M06A) 202404~06\M06A'

# 設定合併後的 CSV 檔案路徑
merged_csv_path = '各旅次路徑原始資料 (M06A) 2024年4~6月.csv'

# 先清空舊的合併檔案（如果存在）
if os.path.exists(merged_csv_path):
    os.remove(merged_csv_path)

# 用來儲存所有 CSV 檔案路徑的列表
csv_files = []
for root, dirs, files in os.walk(folder_path):
    for filename in files:
        if filename.endswith('.csv'):
            csv_files.append(os.path.join(root, filename))

# 定義讀取 CSV 並返回資料的函數
def read_csv(file_path):
    try:
        data = pd.read_csv(file_path)
        return data, file_path  # 返回讀取的資料和檔案路徑
    except Exception as e:
        print(f"無法讀取檔案 {file_path}: {e}")
        return None, file_path

# 開始多執行緒處理
with ThreadPoolExecutor() as executor:
    futures = [executor.submit(read_csv, file_path) for file_path in csv_files]
    
    # 監控進度
    for future in tqdm(as_completed(futures), total=len(futures), desc="合併進度", unit="檔案"):
        data, file_path = future.result()
        if data is not None:
            # 若是第一個檔案，寫入檔案的標題
            if not os.path.exists(merged_csv_path):
                data.to_csv(merged_csv_path, index=False, mode='w', header=True)
            else:
                # 不是第一個檔案，將資料附加至檔案後
                data.to_csv(merged_csv_path, index=False, mode='a', header=False)

print("所有 CSV 檔案（包含子資料夾中的檔案）已成功合併！")
