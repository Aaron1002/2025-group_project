'''
將資料夾下符合條件的所有xml檔案(包含子資料夾)合併成一個`csv`檔案
:usage: 
1. merge_xml_to_df("{欲被合併的資料夾路徑}", "{該資料集檔案名稱開頭}", {處理函數(將xml轉為DataFrame)})
，將會有`temp_no_mul_{欲被合併的資料夾名稱}`資料夾，內含數個csv檔案。
2. simple_merge_csv("{欲被合併的資料夾路徑}")，將會有一個csv檔案。

ex: 解壓縮`eTag 配對路徑動態資訊(v2.0)`後舉例。
1. merge_xml_to_df(r"D:/test_data/unzip_eTag 配對路徑動態資訊(v2.0)", 'ETagPairLive_', xml2df)
2. simple_merge_csv(r"D:\test_data\temp_no_mul_unzip_eTag 配對路徑動態資訊(v2.0)")
最後會有一個`csv_temp_unzip_路段即時路況動態資訊(v2.0).csv`檔案

警告：資料夾須符合以下結構，否則請先整理
root(資料集名稱)
├─ YYYYMMDD
|  ├─ {任何固定字串}_hhmm.xml
'''


import gc
import logging
import os
import pandas as pd
from tqdm import tqdm
from tqdm import tqdm
import datetime
import xml.etree.ElementTree as ET
import pandas as pd



logging.basicConfig(level=logging.ERROR, filename = f'log_{datetime.datetime.now().strftime("%Y%m%d_%H%M%S")}.txt')
def xml2df(file_path):
    '''
    將xml檔案轉換為DataFrame
    需要依不同檔案修改
    '''

    # 載入 XML 檔案（請依實際檔案路徑修改）
    tree = ET.parse(file_path)
    root = tree.getroot()

    # 因為 XML 定義了命名空間，所以必須設定命名空間對應字典
    ns = {'ns': 'http://traffic.transportdata.tw/standard/traffic/schema/'}

    rows = []
    # 迭代每個 ETagPairLive 元素
    for etag_live in root.findall('.//ns:ETagPairLive', ns):
        etag_id = etag_live.find('ns:ETagPairID', ns).text
        start_status = etag_live.find('ns:StartETagStatus', ns).text
        end_status = etag_live.find('ns:EndETagStatus', ns).text
        start_time = etag_live.find('ns:StartTime', ns).text
        end_time = etag_live.find('ns:EndTime', ns).text
        collect_time = etag_live.find('ns:DataCollectTime', ns).text
        
        # 取得 Flows 節點內所有 Flow 子節點
        flows = etag_live.find('ns:Flows', ns)
        if flows is not None:
            for flow in flows.findall('ns:Flow', ns):
                vehicle_type = flow.find('ns:VehicleType', ns).text
                travel_time = flow.find('ns:TravelTime', ns).text
                std_dev = flow.find('ns:StandardDeviation', ns).text
                mean_speed = flow.find('ns:SpaceMeanSpeed', ns).text
                vehicle_count = flow.find('ns:VehicleCount', ns).text

                # 將每筆資料存成字典
                rows.append({
                    'ETagPairID': etag_id,
                    'StartETagStatus': start_status,
                    'EndETagStatus': end_status,
                    'StartTime': start_time,
                    'EndTime': end_time,
                    'DataCollectTime': collect_time,
                    'VehicleType': vehicle_type,
                    'TravelTime': travel_time,
                    'StandardDeviation': std_dev,
                    'SpaceMeanSpeed': mean_speed,
                    'VehicleCount': vehicle_count
                })

    # 轉換為 DataFrame
    df = pd.DataFrame(rows)
        
    return df

def merge_xml_to_df(root_dir: str, file_pre_name: str, file_handler) -> None:
    '''
    將資料夾內的xml合併成一個csv檔，每個資料夾(第一層)一個
    '''
    def merge_for_single_folder(folder_dir : str, save_path):
        '''
        將資料夾內的所有xml合併為df並轉檔儲存
        '''
        os.makedirs(os.path.dirname(save_path), exist_ok=True)
        first, df_first = 1, pd.DataFrame([])
        dfs = []
        for root, _, files in os.walk(folder_dir):
            for f in files:
                if f.startswith(file_pre_name):
                    file_path = os.path.join(root, f)
                    file_name = os.path.basename(file_path)
                    parent_folder = os.path.basename(os.path.dirname(file_path))
                    date_ = parent_folder
                    time_ = file_name.split('_')[-1].split('.')[0]
                    timestamps = f"{date_} {time_}"
                    timestamps = pd.to_datetime(timestamps, format='%Y%m%d %H%M')
                    
                    df = file_handler(file_path)
                    if df.empty: 
                        continue
                    df.insert(0, 'timestamp', timestamps)
                    
                    if first:
                        dfs.append(df)
                        df_first = df
                        first = 0
                    else:
                        dfs.append(df)
        
        if dfs and (df_first is not None):
            df_first = pd.concat(dfs, ignore_index=True)
            df_first.to_csv(f"{save_path}.csv")
            
            del dfs
            del df_first
            gc.collect()
            return save_path
        else:
            logging.error(f"Error: {folder_dir} 沒有合併任何資料")
            return None

    for folder_name in tqdm(os.listdir(root_dir)):
        name = os.path.basename(root_dir.rstrip('/\\'))
        parent_dir = os.path.dirname(root_dir)
        output_dir = os.path.join(parent_dir, f'temp_no_mul_{name}', folder_name)
        merge_for_single_folder(os.path.join(root_dir, folder_name), output_dir)

    return

def simple_merge_csv(target_folder_path: str):
    '''
    將資料夾內的所有csv(目前資料夾)合併為一個csv
    '''
    # target_folder_path = r"D:\test_data\csv_temp_unzip_路段即時路況動態資訊(v2.0)"
    name = os.path.basename(target_folder_path.rstrip('/\\'))
    parent_dir = os.path.dirname(target_folder_path)
    output_dir = os.path.join(parent_dir, f'{name}.csv')
    # print(output_dir)
    # 先處理第一個檔案，寫入標題與資料
    first = True
    for file_name in tqdm(os.listdir(target_folder_path)):
        file_path = os.path.join(target_folder_path, file_name)
        # print(os.path.getsize(file_path))
        if os.path.getsize(file_path) <= 2:
            continue
        for chunk in pd.read_csv(file_path, chunksize=100000):
            if first:
                chunk.to_csv(output_dir, index=False, mode='w', header=True)
                first = False
            else:
                chunk.to_csv(output_dir, index=False, mode='a', header=False)

# merge_xml_to_df(r"D:\test_data\unzip_eTag 配對路徑動態資訊(v2.0)", 'ETagPairLive_', xml2df)
# simple_merge_csv(r"D:\test_data\temp_no_mul_unzip_eTag 配對路徑動態資訊(v2.0)")