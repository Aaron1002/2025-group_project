'''
自動下載`https://tisvcloud.freeway.gov.tw/history-list.php`中某個資料夾下的所有檔案
:usage: download_whole_folder({檔案在網頁上的全名})
ex: download_whole_folder("各類車種旅次數量 (M08A)")
'''
from datetime import datetime
import os
import threading
import time
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm



'''讓之後的所有requests失敗(遇到RequestException)時都會自動重試，避免網路斷線時報錯'''
# 定義重試參數
MAX_RETRIES = 5   # 最大重試次數
RETRY_DELAY = 5   # 每次重試的間隔秒數
RETRY_DELAY_MULTIPLIER = 3  # 間隔秒數的乘數

if not getattr(requests.Session, "_is_patched", False):
    # 保存原始的 requests 方法
    _original_request = requests.Session.request

    def retry_request(self, method, url, *args, **kwargs):
        current_retry_delay = RETRY_DELAY
        for attempt in range(MAX_RETRIES):
            try:
                response = _original_request(self, method, url, *args, **kwargs)
                response.raise_for_status()  # 檢查 HTTP 狀態碼
                return response
            except requests.exceptions.RequestException as e:
                # print(f"請求失敗（第 {attempt + 1} 次）：{e}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(current_retry_delay := current_retry_delay * RETRY_DELAY_MULTIPLIER)  # 每次重試之前增加延遲
                    # print(current_retry_delay)
                else:
                    raise  Exception(f"請求失敗（第 {attempt + 1} 次），放棄重試：{e}") #達到最大重試次數後拋出異常

    # 替換 requests 的 request 方法
    requests.Session.request = retry_request # type: ignore
    requests.Session._is_patched = True # type: ignore
    print('注意：已修改reqquests，現在會自動重試請求')
else:
    print('requests已經被修改過了，將會自動重試請求')
''''''

root = './data/'
class Url:
    def __init__(self, url, name, path = '', last_modified = '2099-01-25 07:04'):
        self.url : str = url
        self.name : str = name
        self.path : str = path
        self.last_modified : datetime = datetime.strptime(last_modified.strip(), "%Y-%m-%d %H:%M")
        
    def __str__(self):
        return f"[{self.url}, \n{self.name}, \n{self.path}, \n{self.last_modified}]"
    
    def __repr__(self):
        return f"[{self.url}, \n{self.name}, \n{self.path}, \n{self.last_modified}]"

def get_url_type(url : str) -> str:
    '''
    檢查目標網址是下載連結還是網頁連結，避免不必要的流量消耗
    
    :param url(str): 欲檢查網址
    :return : `site`: 網站連結, `file`: 檔案下載連結
    '''
    response_header = requests.head(url).headers.get("Content-Type")
    if 'html' in str(response_header): return "site"
    else: return "file"
    
def find_path_with_name(name : str) -> str:
    '''
    尋找指定資料名稱的下載連結
    
    :param name(str): 資料夾名稱(網頁上顯示的檔案全名)
    :return: 下載連結
    '''
    url = "https://tisvcloud.freeway.gov.tw/history-list.php"
    res = requests.get(url)
    soup = BeautifulSoup(res.text, "lxml")
    table = soup.find("table", {'id': 'opendataTable'}).tbody # type: ignore
    target_url = table.find("a", {'title': name}).get("href") # type: ignore
    assert type(target_url) is str #target_url應該要是string
    print(target_url)
    if name == "各類車種通行量統計各類車種 (M03A)":
        target_url += "/"
    
    return target_url

def download_single_file(url : Url, bar : tqdm | None = None) -> bool:
    res = requests.get(url.url)
    os.makedirs(root + url.path, exist_ok=True)
    with open(root + url.path + url.name, 'wb') as f:
        f.write(res.content)
    if bar: bar.update(1)
    return True

def list_url_current(url : Url) -> list[Url]:
    '''
    列出當前所有資料夾連結
    '''
    url_type = get_url_type(url.url)
    if url_type == "file":
        # download_single_file(url)
        return []
    elif url_type == "site":
        res = requests.get(url.url)
        soup = BeautifulSoup(res.text, "lxml")
        target_urls : list[Url] = []
        ele = soup.find_all("td", {'class': 'indexcolname'})
        for td in ele:
            for a in td.find_all("a", href=True):
                last_modified = a.find_parent('tr').find('td', class_='indexcollastmod').text
                target_urls.append(
                    Url(url.url + a["href"], a.text, f"{url.path}/{url.name}/", last_modified)
                )
        if len(target_urls) == 0: print(url, 'has no items')
        return target_urls
    
    return []

def list_url_walk(url : Url, first_level : bool = False, bar : tqdm | None = None) -> list[Url]:
    '''
    列出所有子資料夾連結
    '''
    target_urls : list[Url] = []
    # print(urls)
    if get_url_type(url.url) == "file":
        return [url]
    else:
        if first_level: #use threading
            urls = list_url_current(url)
            if bar is None:
                bar = tqdm(total=len(urls))
            threads = []
            results = []  # 用來儲存各執行緒回傳的結果
            lock = threading.Lock()  # 保護 results 的鎖
            max_threads = 20
            active_based_count = threading.active_count()
            # 定義每個執行緒要執行的工作
            def worker(u: Url):
                res = list_url_walk(u, False, bar)
                with lock:
                    results.extend(res)
                bar.update(1)

            # 為每個 URL 建立執行緒
            for u in urls:
                t = threading.Thread(target=worker, args=(u,))
                threads.append(t)
                
            for t in threads:
                t.start()
                while threading.active_count() - active_based_count >= max_threads:
                    time.sleep(0.2)

            # 等待所有執行緒完成
            for t in threads:
                t.join()

            target_urls.extend(results)
        else:
            urls = list_url_current(url)
            for url in urls:
                target_urls.extend(list_url_walk(url))

    return target_urls

def download_whole_folder(name : str):
    url = find_path_with_name(name)
    print("正在蒐集檔案下載連結")
    files = list_url_walk(Url(url, name), first_level=True)
    # url = name
    # files = list_url_walk(Url(url, ""))
    in_date_files = []
    print("正在篩選符合日期的檔案")
    for file in tqdm(files):
        if file.last_modified >= datetime(2024, 1, 1, 0, 0):
            in_date_files.append(file)
    print("開始下載")
    threads = []
    bar = tqdm(total=len(in_date_files))
    max_threads = 10
    active_based_count = threading.active_count()
    for file in in_date_files:
        t = threading.Thread(target=download_single_file, args=(file, bar, ))
        threads.append(t)
    for t in threads:
        t.start()
        while threading.active_count() - active_based_count >= max_threads:
            time.sleep(0.5)
        
    for t in threads:
        t.join()

    return files
