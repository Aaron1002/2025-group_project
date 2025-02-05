import os
import concurrent.futures
import requests
import gzip
import shutil
import xml.etree.ElementTree as ET

def parallel_download(url_list, save_dirs, extracted_dirs):
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        futures = [
                executor.submit(thread_1, url_list[0], save_dirs[0], extracted_dirs[0]),
                executor.submit(thread_2, url_list[1], save_dirs[1], extracted_dirs[1]),
                executor.submit(thread_3, url_list[2], save_dirs[2], extracted_dirs[2]),
        ]
        #futures = executor.submit(download_file, url_list[0], save_directory)
        concurrent.futures.wait(futures)  # 等待所有 thread 完成
        
        print("所有檔案下載完成")

def thread_1(pre_url, save_dir, extracted_dir):
    for i in range(0, 2359):
        if (2358-i) >= 1000:    
            url = pre_url + str(2358-i) + ".xml.gz"  
        if (2358-i) < 1000:
            url = pre_url + "0" + str(2358-i) + ".xml.gz"
        if (2358-i) < 100:
            url = pre_url + "00" + str(2358-i) + ".xml.gz"
        if (2358-i) < 10:
            url = pre_url + "000" + str(2358-i) + ".xml.gz"
        file_path = download_file(url, save_dir) # 下載 .xml.gz 檔案
        if file_path:
            extracted_file = extract_xml_gz(file_path, extracted_dir) # 解壓縮 .xml.gz    

def thread_2(pre_url, save_dir, extracted_dir):
    for i in range(0, 2359):
        if (2358-i) >= 1000:    
            url = pre_url + str(2358-i) + ".xml.gz"  
        if (2358-i) < 1000:
            url = pre_url + "0" + str(2358-i) + ".xml.gz"
        if (2358-i) < 100:
            url = pre_url + "00" + str(2358-i) + ".xml.gz"
        if (2358-i) < 10:
            url = pre_url + "000" + str(2358-i) + ".xml.gz"
        file_path = download_file(url, save_dir) # 下載 .xml.gz 檔案
        if file_path:
            extracted_file = extract_xml_gz(file_path, extracted_dir) # 解壓縮 .xml.gz    

def thread_3(pre_url, save_dir, extracted_dir):
    for i in range(0, 2359):
        if (2358-i) >= 1000:    
            url = pre_url + str(2358-i) + ".xml.gz"  
        if (2358-i) < 1000:
            url = pre_url + "0" + str(2358-i) + ".xml.gz"
        if (2358-i) < 100:
            url = pre_url + "00" + str(2358-i) + ".xml.gz"
        if (2358-i) < 10:
            url = pre_url + "000" + str(2358-i) + ".xml.gz"
        file_path = download_file(url, save_dir) # 下載 .xml.gz 檔案
        if file_path:
            extracted_file = extract_xml_gz(file_path, extracted_dir) # 解壓縮 .xml.gz    

def download_file(url, save_dir, filename=None):    # 下載檔案

    os.makedirs(save_dir, exist_ok=True)  # 確保目錄存在
    if filename is None:
        filename = url.split('/')[-1]
    save_path = os.path.join(save_dir, filename)

    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()
        with open(save_path, 'wb') as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)
        print(f"檔案已下載到: {save_path}")
        return save_path
    except requests.RequestException as e:
        print(f"下載失敗: {e}")
        return None

def extract_xml_gz(file_path, extract_dir): # 解壓縮先前所下載過的所有檔案

    os.makedirs(extract_dir, exist_ok=True)  # 確保目錄存在
    if not file_path.endswith('.xml.gz'):
        print(f"檔案格式不支援解壓縮: {file_path}")
        return None

    try:
        extracted_file = os.path.join(extract_dir, os.path.basename(file_path).replace('.gz', ''))
        with gzip.open(file_path, 'rb') as gz_file:
            with open(extracted_file, 'wb') as extracted:
                shutil.copyfileobj(gz_file, extracted)
        print(f".xml.gz 檔案已解壓縮到: {extracted_file}")
        return extracted_file
    except Exception as e:
        print(f".xml.gz 解壓縮失敗: {e}")
        return None
    
def single_thread_download():
    file_path = download_file(single_url, save_directory) # 下載 .xml.gz 檔案
    if file_path:
        extracted_file = extract_xml_gz(file_path, extract_directory) # 解壓縮 .xml.gz

def use_thread():
    save_dirs = [] # thread分別要存入的資料夾
    extracted_dirs = [] # thread解壓縮後分別要存入的資料夾
    date_list = [] # thread分別要下載的日期

    print("輸入三個分別存入的資料夾路徑: ")
    for i in range(0, 3):
        add_dir = str(input(""))
        save_dirs.append(add_dir)
    
    print("輸入三個解壓縮後分別存入的資料夾路徑: ")
    for i in range(0, 3):
        add_dir = str(input(""))
        extracted_dirs.append(add_dir)

    print("輸入三個分別要下載的日期: ")    
    for i in range(0, 3):
        add_dir = str(input(""))
        date_list.append(add_dir)

    url_list = ["https://tisvcloud.freeway.gov.tw/history/motc20/VD/" + date_list[0] + "/VDLive_",
                "https://tisvcloud.freeway.gov.tw/history/motc20/VD/" + date_list[1] + "/VDLive_",
                "https://tisvcloud.freeway.gov.tw/history/motc20/VD/" + date_list[2] + "/VDLive_"
            ]    
    
    parallel_download(url_list, save_dirs, extracted_dirs)


thread_or_not = int(input("是否使用多執行緒下載? (0/1): "))

if thread_or_not:
    use_thread()
else:   
    date = str(input("請輸入欲下載的日期 (格式: YYYYMMDD): "))
    save_directory = str(input("請輸入欲存入之資料夾路徑: "))
    extract_directory = str(input("請輸入解壓縮後欲存入之資料夾路徑: "))
    single_url = "https://tisvcloud.freeway.gov.tw/history/motc20/VD/" + date + "/VD_0000.xml.gz" 
    
    single_thread_download()


"""
需動態修改的地方: 
    1. use_thread() 的 "url_list[]", 
    2. 外面的 "single_url"
"""