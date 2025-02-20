"""
import pandas as pd

# 讀取 CSV 文件
csv_file_path = r'檔案路徑'
df = pd.read_csv(csv_file_path)

# 顯示讀取到的資料
print(df)

# 將 DataFrame 保存為 PKL 文件
pkl_file_path = 'path_to_save_pkl_file.pkl'
df.to_pickle(pkl_file_path)

print("CSV 文件已成功讀取並保存為 PKL 文件")
"""

import pandas as pd

# 讀取 PKL 文件
pkl_file_path = r'檔案路徑'
df = pd.read_pickle(pkl_file_path)

# 顯示讀取到的資料
print(df)

print("PKL 文件已成功讀取")