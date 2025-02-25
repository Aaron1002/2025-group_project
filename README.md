# 2025-group_project

## Usage

1. 下載：執行`src\auto_download_whole_folder.py`中的`download_whole_folder`。
ex: `download_whole_folder("各類車種旅次數量 (M08A)")`，將會自動下載`https://tisvcloud.freeway.gov.tw/history-list.php`中`各類車種旅次數量 (M08A)`資料夾下的所有檔案。
2. 解壓縮：執行`src\unzip_files.py`中的`unzip_files`。
ex: `unzip_files("./test_zip_folder")`，會將`test_zip_folder`內的所有檔案解壓縮到新的資料夾`unzip_test_zip_folder`。
3. 合併為同一檔案：按照`src\merge_xml.py`開頭的說明，注意檔案結構符合條件與函式手動修改。
4. 分析大(直接讀RAM會不夠的)檔案：參考`src\dask_template.ipynb`。
