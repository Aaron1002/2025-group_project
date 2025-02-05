import os
import requests
import gzip
import shutil
import xml.etree.ElementTree as ET


def download_file(url, save_dir, filename=None):

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


def extract_xml_gz(file_path, extract_dir):

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


def parse_xml(file_path):
    """
    解析 XML 檔案並列出其根標籤和子標籤。

    :param file_path: XML 檔案路徑
    """
    try:
        tree = ET.parse(file_path)
        root = tree.getroot()
        print(f"根標籤: {root.tag}")
        print("子標籤:")
        for child in root:
            print(f"  - {child.tag}")
    except Exception as e:
        print(f"XML 解析失敗: {e}")


# 測試下載並解壓縮和解析
save_directory = "data"
extract_directory = "data"

for i in range(0, 1042):
    url = "https://tisvcloud.freeway.gov.tw/history/motc20/VD/20250116/VDLive_" + str(1041-i) + ".xml.gz"  # 替換為實際 XML.GZ 檔案 URL
    if (1042-i) < 1000:
        url = "https://tisvcloud.freeway.gov.tw/history/motc20/VD/20250116/VDLive_0" + str(1041-i) + ".xml.gz"
    if (1042-i) < 100:
        url = "https://tisvcloud.freeway.gov.tw/history/motc20/VD/20250116/VDLive_00" + str(1041-i) + ".xml.gz"
    if (1042-i) < 10:
        url = "https://tisvcloud.freeway.gov.tw/history/motc20/VD/20250116/VDLive_000" + str(1041-i) + ".xml.gz"

    # 下載 .xml.gz 檔案
    file_path = download_file(url, save_directory)

    if file_path:
        # 解壓縮 .xml.gz
        extracted_file = extract_xml_gz(file_path, extract_directory)

if extracted_file:
    # 解析 XML 檔案
    parse_xml(extracted_file)
