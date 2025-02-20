import os
import glob
import pandas as pd
import xml.etree.ElementTree as ET

# 設定 XML 檔案所在的資料夾和輸出的 CSV 檔案
input_folder = r"D:\Old.D_\大學\專題\競賽資料\Data\test_data"  # 修改為你的 XML 資料夾
output_file = "output.csv"

# 解析 XML 並提取數據
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

# 遍歷資料夾中的所有 XML 檔案
all_data = []
for xml_file in glob.glob(os.path.join(input_folder, "*.xml")):
    all_data.extend(parse_xml(xml_file))

# 將數據寫入 CSV
df = pd.DataFrame(all_data)
df.to_csv(output_file, index=False, encoding="utf-8-sig")

print(f"CSV 檔案已生成: {output_file}")
