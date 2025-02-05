import os
import csv
import xml.etree.ElementTree as ET

def get_text(element, default="N/A"):
    """安全地提取 XML 元素的文字內容"""
    return element.text if element is not None else default

def parse_and_extract_xml(file_path):
    """
    解析 XML 檔案並提取所需資訊。
    """
    try:
        tree = ET.parse(file_path)
        root = tree.getroot()
        
        namespace = "{http://traffic.transportdata.tw/standard/traffic/schema/}"  # XML 命名空間
        
        update_time = get_text(root.find(f"{namespace}UpdateTime"))
        update_interval = get_text(root.find(f"{namespace}UpdateInterval"))
        authority_code = get_text(root.find(f"{namespace}AuthorityCode"))

        data = []
        for vd in root.findall(f"{namespace}VDs/{namespace}VD"):
            vdid = get_text(vd.find(f"{namespace}VDID"))
            sub_authority_code = get_text(vd.find(f"{namespace}SubAuthorityCode"))
            bi_directional = get_text(vd.find(f"{namespace}BiDirectional"))
            vd_type = get_text(vd.find(f"{namespace}VDType"))
            location_type = get_text(vd.find(f"{namespace}LocationType"))
            detection_type = get_text(vd.find(f"{namespace}DetectionType"))
            position_lon = get_text(vd.find(f"{namespace}PositionLon"))
            position_lat = get_text(vd.find(f"{namespace}PositionLat"))
            road_id = get_text(vd.find(f"{namespace}RoadID"))
            road_name = get_text(vd.find(f"{namespace}RoadName"))
            road_class = get_text(vd.find(f"{namespace}RoadClass"))
            location_mile = get_text(vd.find(f"{namespace}LocationMile"))

            detection_link = vd.find(f"{namespace}DetectionLinks/{namespace}DetectionLink")
            link_id = get_text(detection_link.find(f"{namespace}LinkID")) if detection_link is not None else "N/A"
            bearing = get_text(detection_link.find(f"{namespace}Bearing")) if detection_link is not None else "N/A"
            road_direction = get_text(detection_link.find(f"{namespace}RoadDirection")) if detection_link is not None else "N/A"
            lane_num = get_text(detection_link.find(f"{namespace}LaneNum")) if detection_link is not None else "N/A"
            actual_lane_num = get_text(detection_link.find(f"{namespace}ActualLaneNum")) if detection_link is not None else "N/A"

            road_section = vd.find(f"{namespace}RoadSection")
            start = get_text(road_section.find(f"{namespace}Start")) if road_section is not None else "N/A"
            end = get_text(road_section.find(f"{namespace}End")) if road_section is not None else "N/A"

            data.append([
                update_time, update_interval, authority_code, vdid, sub_authority_code,
                bi_directional, link_id, bearing, road_direction, lane_num, actual_lane_num,
                vd_type, location_type, detection_type, position_lon, position_lat,
                road_id, road_name, road_class, start, end, location_mile
            ])
        return data
    except Exception as e:
        print(f"XML 解析失敗: {e}")
        return []

def convert_folder_xml_to_csv(folder_path, output_folder):
    """
    解析資料夾中的所有 XML 檔案並轉換為 CSV。
    """
    os.makedirs(output_folder, exist_ok=True)
    output_csv = os.path.join(output_folder, "output.csv")
    
    headers = [
        "UpdateTime", "UpdateInterval", "AuthorityCode", "VDID", "SubAuthorityCode",
        "BiDirectional", "LinkID", "Bearing", "RoadDirection", "LaneNum", "ActualLaneNum",
        "VDType", "LocationType", "DetectionType", "PositionLon", "PositionLat",
        "RoadID", "RoadName", "RoadClass", "Start", "End", "LocationMile"
    ]
    
    data = []
    for filename in os.listdir(folder_path):
        if filename.endswith(".xml"):
            file_path = os.path.join(folder_path, filename)
            data.extend(parse_and_extract_xml(file_path))
    
    with open(output_csv, "w", newline="", encoding="utf-8-sig") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(headers)
        writer.writerows(data)
    
    print(f"CSV 檔案已生成: {output_csv}")

if __name__ == "__main__":
    folder_path = input("請輸入 XML 檔案資料夾路徑: ")
    output_folder = input("請輸入 CSV 存放資料夾: ")
    convert_folder_xml_to_csv(folder_path, output_folder)
