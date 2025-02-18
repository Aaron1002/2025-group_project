import os
import glob
import pandas as pd
import xml.etree.ElementTree as ET

# 設定 XML 檔案所在的資料夾和輸出的 CSV 檔案
input_folder = r"D:\Old.D_\大學\專題\競賽資料\Code\data_xml\unextracted"  # 修改為你的 XML 檔案資料夾路徑
output_file = "output.csv"

# 解析 XML 並提取數據
def parse_xml(file_path):
    tree = ET.parse(file_path)
    root = tree.getroot()

    namespace = {'ns': 'http://traffic.transportdata.tw/standard/traffic/schema/'}

    news_data = []
    for news in root.findall('.//ns:News', namespace):
        news_info = {
            "NewsID": news.findtext("ns:NewsID", default="", namespaces=namespace),
            "Language": news.findtext("ns:Language", default="", namespaces=namespace),
            "Title": news.findtext("ns:Title", default="", namespaces=namespace),
            "NewsCategory": news.findtext("ns:NewsCategory", default="", namespaces=namespace),
            "NewsURL": news.findtext("ns:NewsURL", default="", namespaces=namespace),
            "PublishTime": news.findtext("ns:PublishTime", default="", namespaces=namespace),
            "UpdateTime": news.findtext("ns:UpdateTime", default="", namespaces=namespace),
        }
        news_data.append(news_info)
    
    return news_data

# 遍歷資料夾中的所有 XML 檔案
all_news = []
for xml_file in glob.glob(os.path.join(input_folder, "*.xml")):
    all_news.extend(parse_xml(xml_file))

# 將數據寫入 CSV
df = pd.DataFrame(all_news)
df.to_csv(output_file, index=False, encoding="utf-8-sig")

print(f"CSV 檔案已生成: {output_file}")
