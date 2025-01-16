import xml.etree.ElementTree as ET
import pandas as pd

# 定義 XML 命名空間
namespace = {"ns": "http://traffic.transportdata.tw/standard/traffic/schema/"}

# 讀取 XML 檔案
file_path = "欲讀取檔案的位置"
tree = ET.parse(file_path)
root = tree.getroot()

data = []

# 解析 VDLive 監測點資訊
for vdlive in root.findall(".//ns:VDLive", namespace):
    vdid = vdlive.find("ns:VDID", namespace).text
    data_time = vdlive.find("ns:DataCollectTime", namespace).text

    for link_flow in vdlive.findall(".//ns:LinkFlow", namespace):
        link_id = link_flow.find("ns:LinkID", namespace).text

        for lane in link_flow.findall(".//ns:Lane", namespace):
            lane_id = lane.find("ns:LaneID", namespace).text
            speed = lane.find("ns:Speed", namespace).text
            occupancy = lane.find("ns:Occupancy", namespace).text

            for vehicle in lane.findall(".//ns:Vehicle", namespace):
                vehicle_type = vehicle.find("ns:VehicleType", namespace).text
                volume = vehicle.find("ns:Volume", namespace).text
                vehicle_speed = vehicle.find("ns:Speed", namespace).text

                data.append([vdid, data_time, link_id, lane_id, speed, occupancy, vehicle_type, volume, vehicle_speed])

# 轉換為 DataFrame
df = pd.DataFrame(data, columns=["VDID", "DataCollectTime", "LinkID", "LaneID", "Speed", "Occupancy", "VehicleType", "Volume", "VehicleSpeed"])

# 存為 CSV 檔案
csv_path = "欲儲存csv的位置"
df.to_csv(csv_path, index=False)

print(f"CSV 檔案已成功儲存到 {csv_path}")
