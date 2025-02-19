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



root = './'
header_content = []
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
# date = datetime(2024, 1, 1, 0, 0)  
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
