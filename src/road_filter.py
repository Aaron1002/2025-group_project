import pandas as pd
from tqdm import tqdm

# 設定輸入與輸出檔案路徑
input_csv_path = '各旅次路徑原始資料 (M06A) 2024年4~6月.csv'  # 替換成實際檔案名稱
output_csv_path = '各旅次路徑原始資料 (M06A) 2024年4~6月(五股~內湖).csv'

# 指定要篩選的路段
filter_segments = {
    "01F0213N", "01F0233N", "01F0256N", "01F0293N",
    "01F0182S", "01F0248S", "01F0264S", "01F0293S"
}

# 設定 chunksize 來逐批讀取大檔案
chunksize = 100000

with tqdm(desc="處理進度", unit="行") as pbar:
    for chunk in pd.read_csv(input_csv_path, chunksize=chunksize, header=None):
        filtered_chunk = chunk[chunk[2].astype(str).isin(filter_segments) | chunk[4].astype(str).isin(filter_segments)]
        
        if not filtered_chunk.empty:
            filtered_chunk.to_csv(output_csv_path, mode='a', index=False, header=not pbar.n)
        
        pbar.update(len(chunk))

print("篩選完成，結果已儲存到", output_csv_path)