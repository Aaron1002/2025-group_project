import os
import pandas as pd

def collect_filtered_data(root_folder, output_file):
    all_data = []
    
    # 遍歷主資料夾內的所有子資料夾
    for subdir in os.listdir(root_folder):
        subdir_path = os.path.join(root_folder, subdir)
        
        if os.path.isdir(subdir_path):  # 確保是子資料夾
            for file in os.listdir(subdir_path):
                if file.endswith(".csv"):  # 確保是 CSV 檔案
                    file_path = os.path.join(subdir_path, file)
                    
                    # 嘗試不同的編碼讀取 CSV 以避免亂碼
                    try:
                        df = pd.read_csv(file_path, dtype={"LinkID": str}, encoding="utf-8")
                    except UnicodeDecodeError:
                        try:
                            df = pd.read_csv(file_path, dtype={"LinkID": str}, encoding="latin1")
                        except UnicodeDecodeError:
                            try:
                                df = pd.read_csv(file_path, dtype={"LinkID": str}, encoding="gb18030")
                            except Exception as e:
                                print(f"無法讀取 {file_path}: {e}")
                                continue
                    
                    # 篩選符合條件的資料
                    filtered_df = df[df["LinkID"].between("00001000017", "00001000032")]
                    
                    if not filtered_df.empty:
                        all_data.append(filtered_df)
    
    # 合併所有符合條件的資料
    if all_data:
        result_df = pd.concat(all_data, ignore_index=True)
        result_df.to_csv(output_file, index=False, encoding="utf-8-sig")  # 使用 utf-8-sig 避免亂碼
        print(f"篩選後的資料已儲存至 {output_file}")
    else:
        print("沒有找到符合條件的資料。")

# 設定你的主資料夾路徑與輸出檔案名稱
root_folder = r"root"
output_file = "filtered_data.csv"

collect_filtered_data(root_folder, output_file)