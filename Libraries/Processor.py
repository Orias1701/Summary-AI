import os
import re
import json
import pandas as pd

# === CÁC HÀM XỬ LÝ FILE ===

def load_json(file_path):
    if not os.path.exists(file_path):
        return []
    with open(file_path, 'r', encoding='utf-8') as f:
        return json.load(f)
    
def replace_json(data, file_path):
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

def save_json(data, file_path):
    with open(file_path, 'a', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

def load_jsonl(file_path):
    data = []
    if os.path.exists(file_path):
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                data.append(json.loads(line))
    return data

def replace_jsonl(data, file_path):
    with open(file_path, 'w', encoding='utf-8') as f:
        for item in data:
            f.write(json.dumps(item, ensure_ascii=False) + '\n')

def save_jsonl(data, file_path):
    with open(file_path, 'a', encoding='utf-8') as f:
        for item in data:
            f.write(json.dumps(item, ensure_ascii=False) + '\n')


# === CÁC HÀM HỖ TRỢ ===

def get_urls_from_url_file(file_path):
    """Lấy set các URL đã có từ file URLS.json."""
    urls = set()
    # Hàm load_json trả về một list các dictionary
    url_info_list = load_json(file_path) 
    for item in url_info_list:
        if 'url' in item:
            urls.add(item['url'])
    return urls

def get_existing_article_urls(file_path):
    """Lấy set các URL bài viết đã có từ file JSONL."""
    urls = set()
    if os.path.exists(file_path):
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    urls.add(json.loads(line)['url'])
                except (json.JSONDecodeError, KeyError):
                    continue
    return urls

def convert_to_xlsx(json_path, xlsx_path):
    """Chuyển file JSON (dạng list các object) hoặc JSONL sang XLSX."""
    try:
        # Tự động phát hiện định dạng file
        if json_path.endswith('.jsonl'):
            df = pd.read_json(json_path, lines=True)
        else:
            df = pd.read_json(json_path) # Đọc file JSON chuẩn
            
        column_order = ["category", "sub_category", "url", "title", "description", "content", "date", "words"]
        df = df[[col for col in column_order if col in df.columns]]
        df.to_excel(xlsx_path, index=False, engine='openpyxl')
        print(f"-> Đã xuất thành công file Excel tại {xlsx_path}")
    except (FileNotFoundError, ValueError) as e:
        print(f"-> Không có dữ liệu hoặc lỗi khi chuyển sang Excel: {e}")

def get_url_key(item):
    match = re.search(r'-(\d+)\.html', item['url'])
    return int(match.group(1)) if match else 0

def heapify(arr, n, i, key_func):
    largest = i
    l = 2 * i + 1
    r = 2 * i + 2
    if l < n and key_func(arr[l]) > key_func(arr[largest]): largest = l
    if r < n and key_func(arr[r]) > key_func(arr[largest]): largest = r
    if largest != i:
        arr[i], arr[largest] = arr[largest], arr[i]
        heapify(arr, n, largest, key_func)

def heapSort(arr, key_func):
    n = len(arr)
    for i in range(n // 2 - 1, -1, -1):
        heapify(arr, n, i, key_func)
    for i in range(n - 1, 0, -1):
        arr[i], arr[0] = arr[0], arr[i]
        heapify(arr, i, 0, key_func)
    return arr