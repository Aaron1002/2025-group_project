import os
import concurrent.futures
import requests
import gzip
import shutil
import glob
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta

def generate_dates(start_date, end_date):
    date_list = []
    current_date = datetime.strptime(start_date, "%Y%m%d")
    end_date = datetime.strptime(end_date, "%Y%m%d")
    while current_date <= end_date:
        date_list.append(current_date.strftime("%Y%m%d"))
        current_date += timedelta(days=1)
    return date_list

def parallel_download(url_list, save_base_dir, extracted_base_dir, csv_base_dir, date):
    save_dir = os.path.join(save_base_dir, date)
    extracted_dir = os.path.join(extracted_base_dir, date)
    csv_dir = os.path.join(csv_base_dir, date)
    os.makedirs(save_dir, exist_ok=True)
    os.makedirs(extracted_dir, exist_ok=True)
    os.makedirs(csv_dir, exist_ok=True)
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        futures = [executor.submit(download_and_extract, url, save_dir, extracted_dir) for url in url_list]
        concurrent.futures.wait(futures)
        print(f"{date} 所有檔案下載完成")
    
    convert_to_csv(extracted_dir, csv_dir)

def download_and_extract(url, save_dir, extracted_dir):
    file_path = download_file(url, save_dir)
    if file_path:
        extract_xml_gz(file_path, extracted_dir)
        os.remove(file_path)

def download_file(url, save_dir):
    os.makedirs(save_dir, exist_ok=True)
    filename = url.split('/')[-1]
    save_path = os.path.join(save_dir, filename)

    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()
        with open(save_path, 'wb') as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)
        print(f"下載成功: {save_path}")
        return save_path
    except requests.RequestException as e:
        print(f"下載失敗: {e}")
        return None

def extract_xml_gz(file_path, extract_dir):
    os.makedirs(extract_dir, exist_ok=True)
    if not file_path.endswith('.xml.gz'):
        print(f"無法解壓縮: {file_path}")
        return None

    try:
        extracted_file = os.path.join(extract_dir, os.path.basename(file_path).replace('.gz', ''))
        with gzip.open(file_path, 'rb') as gz_file:
            with open(extracted_file, 'wb') as extracted:
                shutil.copyfileobj(gz_file, extracted)
        print(f"解壓縮完成: {extracted_file}")
        return extracted_file
    except Exception as e:
        print(f"解壓縮失敗: {e}")
        return None

def parse_xml(file_path):
    tree = ET.parse(file_path)
    root = tree.getroot()
    namespace = {'ns': 'http://traffic.transportdata.tw/standard/traffic/schema/'}

    data = []
    for vdlive in root.findall('.//ns:VDLive', namespace):
        vdid = vdlive.findtext("ns:VDID", default="", namespaces=namespace)
        data_collect_time = vdlive.findtext("ns:DataCollectTime", default="", namespaces=namespace)

        for link_flow in vdlive.findall(".//ns:LinkFlow", namespace):
            link_id = link_flow.findtext("ns:LinkID", default="", namespaces=namespace)

            for lane in link_flow.findall(".//ns:Lane", namespace):
                lane_id = lane.findtext("ns:LaneID", default="", namespaces=namespace)
                lane_type = lane.findtext("ns:LaneType", default="", namespaces=namespace)
                speed = lane.findtext("ns:Speed", default="", namespaces=namespace)
                occupancy = lane.findtext("ns:Occupancy", default="", namespaces=namespace)

                for vehicle in lane.findall(".//ns:Vehicle", namespace):
                    vehicle_type = vehicle.findtext("ns:VehicleType", default="", namespaces=namespace)
                    volume = vehicle.findtext("ns:Volume", default="", namespaces=namespace)
                    vehicle_speed = vehicle.findtext("ns:Speed", default="", namespaces=namespace)

                    data.append({
                        "VDID": vdid,
                        "DataCollectTime": data_collect_time,
                        "LinkID": link_id,
                        "LaneID": lane_id,
                        "LaneType": lane_type,
                        "Speed": speed,
                        "Occupancy": occupancy,
                        "VehicleType": vehicle_type,
                        "Volume": volume,
                        "VehicleSpeed": vehicle_speed
                    })
    return data

def convert_to_csv(input_folder, output_folder):
    all_data = []
    for xml_file in glob.glob(os.path.join(input_folder, "*.xml")):
        all_data.extend(parse_xml(xml_file))
    if all_data:
        output_file = os.path.join(output_folder, "output.csv")
        df = pd.DataFrame(all_data)
        df.to_csv(output_file, index=False, encoding="utf-8-sig")
        print(f"CSV 轉換完成: {output_file}")

def download_full_year():
    save_base_dir = input("請輸入下載檔案存放的根目錄: ")
    extracted_base_dir = input("請輸入解壓縮後的根目錄: ")
    csv_base_dir = input("請輸入 CSV 檔案存放的根目錄: ")
    
    date_list = generate_dates("20240101", "20241231")
    for date in date_list:
        url_list = [f"https://tisvcloud.freeway.gov.tw/history/motc20/VD/{date}/VDLive_{hour:04d}.xml.gz" 
                    for hour in range(0, 2400, 5)]
        parallel_download(url_list, save_base_dir, extracted_base_dir, csv_base_dir, date)

if __name__ == "__main__":
    download_full_year()
