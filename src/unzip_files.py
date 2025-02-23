'''
解壓縮資料夾下的所有檔案(包含子資料夾內的檔案)
:usage: unzip_files("{欲被解壓縮的資料夾路徑}")
ex: unzip_files("./test_zip_folder")，會將`test_zip_folder`內的所有檔案解壓縮到新的資料夾`unzip_test_zip_folder`
'''

from operator import mul
import os
from tracemalloc import start
from turtle import up
import zipfile
import logging
from datetime import datetime
import tarfile
import shutil
import threading
from tqdm import tqdm
import gzip
import time



def extract_file(file_path, extract_path):
    if zipfile.is_zipfile(file_path):
        # logging.info(f'開始解壓縮: {file_path}')
        with zipfile.ZipFile(file_path, 'r') as zip_ref:
            zip_ref.extractall(extract_path)
    elif tarfile.is_tarfile(file_path):
        # logging.info(f'開始解壓縮: {file_path}')
        with tarfile.open(file_path, 'r:*') as tar_ref:
            tar_ref.extractall(extract_path)
    elif file_path.endswith('.gz'):
        # logging.info(f'開始解壓縮: {file_path}')
        os.makedirs(extract_path, exist_ok=True)
        with gzip.open(file_path, 'rb') as f_in:
            with open(os.path.join(extract_path, os.path.basename(file_path)[:-3]), 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out) # type: ignore
    else:
        logging.warning(f'不支援的壓縮檔格式: {file_path}')
        
def unzip_files_normal(folder_path, output_folder):
    # 取得資料夾名稱
    folder_name = os.path.basename(folder_path.rstrip('/\\'))
    print(f"output_folder: {output_folder}")
    os.makedirs(output_folder, exist_ok=True)

    # 走訪資料夾內所有檔案
    total_files = sum(len(files) for _, _, files in os.walk(folder_path))
    with tqdm(total=total_files, desc="解壓縮進度") as pbar:
        for root, _, files in os.walk(folder_path):
            for file in files:
                file_path = os.path.join(root, file)
                # logging.info(f'開始檢查檔案: {file_path}')
                relative_path = os.path.relpath(root, folder_path)
                extract_path = os.path.join(output_folder, relative_path)
                if file_path.endswith(('.gz', '.zip', '.tar')):
                    extract_file(file_path, extract_path)
                    # logging.info(f'解壓縮完成: {file_path}')
                else:
                    # 複製不需要解壓縮的檔案
                    os.makedirs(extract_path, exist_ok=True)
                    shutil.copy2(file_path, extract_path)
                    # logging.info(f'複製檔案完成: {file_path} 到 {extract_path}')
                pbar.update(1)

def unzip_files_multithreaded(folder_path, output_folder):
    # 取得資料夾名稱
    # folder_name = os.path.basename(folder_path.rstrip('/\\'))
    print(f"output_folder: {output_folder}")
    os.makedirs(output_folder, exist_ok=True)

    def process_file(file_path, extract_path):
        # logging.info(f'開始檢查檔案: {file_path}')
        if file_path.endswith(('.gz', '.zip', '.tar')):
            extract_file(file_path, extract_path)
            # logging.info(f'解壓縮完成: {file_path}')
        else:
            # 複製不需要解壓縮的檔案
            os.makedirs(extract_path, exist_ok=True)
            shutil.copy2(file_path, extract_path)
            # logging.info(f'複製檔案完成: {file_path} 到 {extract_path}')
        pbar.update(1)

    threads = []
    max_threads = 30
    worker_per_thread = 10000
    active_based_count = threading.active_count()
    
    waiting_list = []
    total_files = sum(len(files) for _, _, files in os.walk(folder_path))
    
    def thread_worker(waiting_eles):
        for file_path, extract_path in waiting_eles:
            process_file(file_path, extract_path)
        
        return
    
    with tqdm(total=total_files, desc="解壓縮進度") as pbar:
        # 走訪資料夾內所有檔案
        for root, _, files in os.walk(folder_path):
            for file in files:
                file_path = os.path.join(root, file)
                relative_path = os.path.relpath(root, folder_path)
                extract_path = os.path.join(output_folder, relative_path)
                waiting_list.append([file_path, extract_path])
        
        for i in range(0, len(waiting_list), worker_per_thread):
            t = threading.Thread(target=thread_worker, args=([waiting_list[i:i+worker_per_thread]]))
            threads.append(t)
        
        
        for t in threads[:max_threads]:
            t.start()
            time.sleep(5)
        
        for t in threads[max_threads:]:
            t.start()
            while threading.active_count() - active_based_count >= max_threads:
                time.sleep(30)
                    
        for t in threads:
            t.join()

def check_for_remaining_archives(folder_path):
    remaining_archives = []
    total_files = sum(len(files) for _, _, files in os.walk(folder_path))
    pbar = tqdm(total=total_files, desc="檢查進度")
    for root, _, files in os.walk(folder_path):
        for file in files:
            file_path = os.path.join(root, file)
            if file_path.endswith(('.gz', '.zip', '.tar')):
                remaining_archives.append(file_path)
            pbar.update(1)
    return remaining_archives

def unzip_files(target_folder_path, output_folder=None):
    log_folder = os.path.join(os.path.dirname(__file__), '.log')
    os.makedirs(log_folder, exist_ok=True)
    target_folder_name = os.path.basename(target_folder_path.rstrip('/\\'))
    
    if output_folder == None: output_folder = os.path.dirname(target_folder_path) #預設為原路徑
    output_folder = os.path.join(output_folder, f'unzip_{target_folder_name}')
    
    print(f"from: {target_folder_path}\nto: {output_folder}")
    
    current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = os.path.join(log_folder, f'{target_folder_name}_{current_time}.log')

    print(f"Log file: {log_filename}")
    # logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s', filename=log_filename, filemode='w')

    # start = time.time()
    # unzip_files_normal(target_folder_path, output_folder)
    # end = time.time()
    # single = end - start

    start_mt = time.time()
    unzip_files_multithreaded(target_folder_path, output_folder)
    end_mt = time.time()
    multi = end_mt - start_mt

    # print(f"Single thread: {single}")
    print(f"Multi thread: {multi}")
    
    print("正在檢查是否有未解壓縮的檔案")
    remaining_zip_file = check_for_remaining_archives(output_folder)
    if len(remaining_zip_file) == 0:
        print("所有檔案已解壓縮")
    else:
        print(f"有未解壓縮的檔案{remaining_zip_file}")

# target_folder_path = r"C:\Users\user\OneDrive\documents\code\Python\Projects\project-2025\src\data\eTag 配對路徑動態資訊(v2.0)"
# output_folder = r"D:\data"
# unzip_files(target_folder_path, output_folder)

# target_folder_path = r"C:\Users\user\OneDrive\documents\code\Python\Projects\project-2025\src\data\eTag 配對路徑靜態資訊(v2.0)"
# unzip_files(target_folder_path, output_folder)

# target_folder_path = r"C:\Users\user\OneDrive\documents\code\Python\Projects\project-2025\src\data\路段即時路況動態資訊(v2.0)"
# unzip_files(target_folder_path, output_folder)