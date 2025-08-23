import pandas as pd
import requests
import json
import os
import re
import tqdm
from bs4 import BeautifulSoup
from datetime import datetime
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- CONFIGURATION ---
BASE_URL = "https://vnexpress.net"
DATA_DIR = "../Database"
JSON_DIR = os.path.join(DATA_DIR, "JSON")
XLSX_DIR = os.path.join(DATA_DIR, "XLSX")
JSON_FILE = os.path.join(JSON_DIR, "vnexpress_articles.jsonl")
XLSX_FILE = os.path.join(XLSX_DIR, "vnexpress_articles.xlsx")
MIN_YEAR = 2020
MIN_WORDS = 300
MAX_WORDS = 1000
TARGET_ARTICLES_PER_SUBTYPE = 33
MAX_CONCURRENT_WORKERS = 10
REQUEST_TIMEOUT = 30

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}
TYPE_DICT = {
    'thoi-su': ['chinh-tri', 'dan-sinh', 'giao-thong'],
    'the-gioi': ['tu-lieu', 'phan-tich', 'quan-su'],
    'kinh-doanh': ['quoc-te', 'doanh-nghiep', 'chung-khoan'],
    'khoa-hoc': ['tin-tuc', 'phat-minh', 'the-gioi-tu-nhien'],
    'giai-tri': ['gioi-sao', 'sach', 'phim', 'nhac'],
    'the-thao': ['bong-da', 'tennis', 'cac-mon-khac'],
    'giao-duc': ['tin-tuc', 'tuyen-sinh', 'du-hoc'],
    'suc-khoe': ['tin-tuc', 'cac-benh', 'song-khoe'],
}
VIETNAMESE_DAYS = {
    "Chủ nhật": "Sunday", "Thứ hai": "Monday", "Thứ ba": "Tuesday",
    "Thứ tư": "Wednesday", "Thứ năm": "Thursday", "Thứ sáu": "Friday",
    "Thứ bảy": "Saturday"
}

# --- CORE FUNCTIONS ---

def create_session():
    """Tạo session với cơ chế tự động thử lại."""
    session = requests.Session()
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

def fetch_page_content(session, url):
    """Lấy nội dung HTML của một URL an toàn."""
    try:
        response = session.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        return response.content
    except requests.exceptions.RequestException:
        print(f"Lỗi URL {url} sau nhiều lần thử.")
        return None

def convert_vietnamese_date(date_str):
    """Chuyển đổi định dạng ngày tháng tiếng Việt."""
    for vn_day, en_day in VIETNAMESE_DAYS.items():
        if vn_day in date_str:
            date_str = date_str.replace(vn_day, en_day)
            break
    date_str = re.sub(r"\s\(GMT[+-]\d{1,2}\)", "", date_str)
    try:
        return datetime.strptime(date_str, "%A, %d/%m/%Y, %H:%M")
    except ValueError:
        return None

def get_article_urls(session, category, sub_category):
    """Lấy list URLs từ một chuyên mục con."""
    urls = []
    page_num = 1
    while len(urls) < TARGET_ARTICLES_PER_SUBTYPE:
        list_url = f"{BASE_URL}/{category}/{sub_category}-p{page_num}"
        content = fetch_page_content(session, list_url)
        if not content: break
        soup = BeautifulSoup(content, "html.parser")
        title_tags = soup.find_all(class_="title-news")
        if not title_tags: break
        for title in title_tags:
            link_tag = title.find("a")
            if link_tag and link_tag.get("href"):
                urls.append(link_tag.get("href"))
                if len(urls) >= TARGET_ARTICLES_PER_SUBTYPE: break
        page_num += 1
    return urls

def scrape_article_details(session, article_url, category, sub_category):
    """Lấy chi tiết nội dung của một bài báo."""
    content = fetch_page_content(session, article_url)
    if not content: return None
    soup = BeautifulSoup(content, "html.parser")
    
    title_tag = soup.find("h1", class_="title-detail")
    title = title_tag.get_text(strip=True) if title_tag else ""
    description_tag = soup.find("p", class_="description")
    description = description_tag.get_text(strip=True) if description_tag else ""
    date_tag = soup.find("span", class_="date")
    date_obj = convert_vietnamese_date(date_tag.text.strip()) if date_tag else None
    
    if not all([title, description, date_obj]) or date_obj.year < MIN_YEAR:
        return None

    article_body = " ".join(p.get_text(" ", strip=True) for p in soup.find_all("p", class_="Normal"))
    word_count = len(article_body.split())

    if MIN_WORDS < word_count < MAX_WORDS:
        return {
            "category": category,
            "sub_category": sub_category,
            "url": article_url,
            "title": title,
            "description": description,
            "content": article_body,
            "date": date_obj.strftime("%Y-%m-%d %H:%M:%S"),
            "words": word_count,
        }
    return None

def load_existing_urls(file_path):
    """Đọc file jsonl và trả về một set các URL đã tồn tại."""
    existing_urls = set()
    if not os.path.exists(file_path):
        return existing_urls
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            try:
                existing_urls.add(json.loads(line)['url'])
            except (json.JSONDecodeError, KeyError):
                continue
    return existing_urls

def json_export(new_articles):
    """Ghi nối tiếp các bài báo mới vào file JSONL."""
    with open(JSON_FILE, "a", encoding="utf-8") as f:
        for article in new_articles:
            f.write(json.dumps(article, ensure_ascii=False) + "\n")
            
    print(f"Hoàn thành! Đã thêm {len(new_articles)} bài báo mới.")

def xlsx_convert(jsonl_path, xlsx_path):
    """Đọc toàn bộ file JSON Lines và xuất ra file Excel."""
    print(f"Đang đọc dữ liệu từ file: {jsonl_path}")
    try:
        df = pd.read_json(jsonl_path, lines=True)
    except FileNotFoundError:
        print(f"Lỗi: Không tìm thấy file {jsonl_path}")
        return

    print(f"Đang ghi dữ liệu ra file Excel: {xlsx_path}")
    df.to_excel(xlsx_path, index=False, engine='openpyxl')
    print(f"Hoàn thành! Đã lưu file tại: {xlsx_path}")

def main():
    """Hàm chính điều phối việc crawl dữ liệu."""
    os.makedirs(JSON_DIR, exist_ok=True)
    os.makedirs(XLSX_DIR, exist_ok=True)
    
    existing_urls = load_existing_urls(JSON_FILE)
    print(f"Đã tìm thấy {len(existing_urls)} URL đã được crawl.")

    with create_session() as session:
        print("Bước 1: Thu thập tất cả URLs...")
        url_info_list = []
        for category, sub_categories in tqdm.tqdm(TYPE_DICT.items(), desc="Categories"):
            for sub_cat in sub_categories:
                urls = get_article_urls(session, category, sub_cat)
                for url in urls:
                    url_info_list.append({'url': url, 'cat': category, 'sub': sub_cat})
        
        new_urls_to_scrape = [info for info in url_info_list if info['url'] not in existing_urls]
        
        print(f"Tổng cộng {len(url_info_list)} URL, trong đó có {len(new_urls_to_scrape)} URL mới.")
        if not new_urls_to_scrape:
            print("Không có bài báo mới nào để crawl.")
            return

        print("\nBước 2: Bắt đầu crawl chi tiết bài báo mới...")
        new_articles = []
        with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_WORKERS) as executor:
            future_to_url = {
                executor.submit(scrape_article_details, session, info['url'], info['cat'], info['sub']): info 
                for info in new_urls_to_scrape
            }
            progress_bar = tqdm.tqdm(as_completed(future_to_url), total=len(new_urls_to_scrape), desc="Crawling Articles")
            for future in progress_bar:
                if result := future.result():
                    new_articles.append(result)

    if not new_articles:
        print("Không crawl được bài báo hợp lệ nào.")
        return

    print(f"\nBước 3: Lưu {len(new_articles)} bài báo mới vào file...")
    json_export(new_articles)
    xlsx_convert(JSON_FILE, XLSX_FILE)

if __name__ == "__main__":
    main()