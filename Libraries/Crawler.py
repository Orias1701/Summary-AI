# Tên file: crawler_module.py

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

class VnExpressCrawler:
    """
    Một lớp để crawl dữ liệu từ VnExpress với các cấu hình tùy chỉnh.
    """
    def __init__(self, config):
        """
        Khởi tạo crawler với một dictionary cấu hình.
        """
        # --- THIẾT LẬP CẤU HÌNH ---
        self.config = config
        self.base_url = config.get("BASE_URL", "https://vnexpress.net")
        self.data_dir = config.get("DATA_DIR", "../Database")
        self.min_year = config.get("MIN_YEAR", 2020)
        self.min_words = config.get("MIN_WORDS", 200)
        self.max_words = config.get("MAX_WORDS", 1000)
        self.target_articles = config.get("TARGET_ARTICLES_PER_SUBTYPE", 25)
        self.max_workers = config.get("MAX_CONCURRENT_WORKERS", 10)
        self.validation_count = config.get("VALIDATION_ARTICLES_COUNT", 10)
        self.type_dict = config.get("TYPE_DICT", {})

        # --- Thiết lập đường dẫn file ---
        self.json_dir = os.path.join(self.data_dir, "JSON")
        self.xlsx_dir = os.path.join(self.data_dir, "XLSX")
        self.json_file = os.path.join(self.json_dir, "vnexpress.jsonl")
        self.xlsx_file = os.path.join(self.xlsx_dir, "vnexpress.xlsx")

        # --- Hằng số nội bộ ---
        self.HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
        self.VIETNAMESE_DAYS = {"Chủ nhật": "Sunday", "Thứ hai": "Monday", "Thứ ba": "Tuesday", "Thứ tư": "Wednesday", "Thứ năm": "Thursday", "Thứ sáu": "Friday", "Thứ bảy": "Saturday"}
        
        os.makedirs(self.json_dir, exist_ok=True)
        os.makedirs(self.xlsx_dir, exist_ok=True)

    def _create_session(self):
        """Tạo session với cơ chế tự động thử lại."""
        session = requests.Session()
        retries = Retry(total=5, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
        adapter = HTTPAdapter(max_retries=retries)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    def _fetch_page_content(self, session, url):
        """Lấy nội dung HTML của một URL an toàn."""
        try:
            response = session.get(url, headers=self.HEADERS, timeout=10)
            response.raise_for_status()
            return response.content
        except requests.exceptions.RequestException:
            return None

    def _convert_vietnamese_date(self, date_str):
        """Chuyển đổi định dạng ngày tháng tiếng Việt."""
        for vn_day, en_day in self.VIETNAMESE_DAYS.items():
            if vn_day in date_str:
                date_str = date_str.replace(vn_day, en_day)
                break
        date_str = re.sub(r"\s\(GMT[+-]\d{1,2}\)", "", date_str)
        try:
            return datetime.strptime(date_str, "%A, %d/%m/%Y, %H:%M")
        except ValueError:
            return None

    def _scrape_article_details(self, session, article_url, category, sub_category):
        """Lấy chi tiết nội dung của một bài báo."""
        content = self._fetch_page_content(session, article_url)
        if not content: return None
        soup = BeautifulSoup(content, "html.parser")
        
        title = soup.find("h1", class_="title-detail").get_text(strip=True) if soup.find("h1", class_="title-detail") else ""
        description = soup.find("p", class_="description").get_text(strip=True) if soup.find("p", class_="description") else ""
        date_obj = self._convert_vietnamese_date(soup.find("span", class_="date").text.strip()) if soup.find("span", class_="date") else None
        
        if not all([title, description, date_obj]) or date_obj.year < self.min_year:
            return None

        article_body = " ".join(p.get_text(" ", strip=True) for p in soup.find_all("p", class_="Normal"))
        word_count = len(article_body.split())

        if self.min_words < word_count < self.max_words:
            return {
                "category": category, "sub_category": sub_category, "url": article_url,
                "title": title, "description": description, "content": article_body,
                "date": date_obj.strftime("%Y-%m-%d %H:%M:%S"), "words": word_count,
            }
        return None

    def _is_subcategory_valid(self, session, category, sub_category):
        """Kiểm tra nhanh một sub-category có dữ liệu chất lượng không."""
        list_url = f"{self.base_url}/{category}/{sub_category}-p1"
        content = self._fetch_page_content(session, list_url)
        if not content: return False
        
        soup = BeautifulSoup(content, "html.parser")
        title_tags = soup.find_all(class_="title-news")
        if not title_tags: return False

        urls_to_check = [tag.find("a").get("href") for tag in title_tags if tag.find("a")]
        
        for url in urls_to_check[:self.validation_count]:
            if self._scrape_article_details(session, url, category, sub_category):
                return True
        return False

    def _get_all_urls_from_valid_subcategory(self, session, category, sub_category):
        """Lấy tất cả URL từ một sub-category hợp lệ."""
        urls = []
        page_num = 1
        while len(urls) < self.target_articles:
            list_url = f"{self.base_url}/{category}/{sub_category}-p{page_num}"
            content = self._fetch_page_content(session, list_url)
            if not content: break
            soup = BeautifulSoup(content, "html.parser")
            title_tags = soup.find_all(class_="title-news")
            if not title_tags: break
            for title in title_tags:
                link_tag = title.find("a")
                if link_tag and link_tag.get("href"):
                    urls.append({'url': link_tag.get("href"), 'cat': category, 'sub': sub_category})
                    if len(urls) >= self.target_articles: break
            page_num += 1
        return urls

    def _load_existing_urls(self):
        """Đọc file jsonl và trả về set các URL đã tồn tại."""
        existing_urls = set()
        if os.path.exists(self.json_file):
            with open(self.json_file, 'r', encoding='utf-8') as f:
                for line in f:
                    try: existing_urls.add(json.loads(line)['url'])
                    except (json.JSONDecodeError, KeyError): continue
        return existing_urls

    def _export_to_files(self, new_articles):
        """Lưu dữ liệu mới vào file JSONL và cập nhật file XLSX."""
        if new_articles:
            with open(self.json_file, "a", encoding="utf-8") as f:
                for article in new_articles:
                    f.write(json.dumps(article, ensure_ascii=False) + "\n")
        
        print(f"\nĐang cập nhật file Excel tại: {self.xlsx_file}")
        try:
            df = pd.read_json(self.json_file, lines=True)
            df.to_excel(self.xlsx_file, index=False, engine='openpyxl')
            print("Cập nhật file Excel thành công.")
        except FileNotFoundError:
            print("Chưa có file JSON để tạo Excel.")
        except ValueError:
            print("File JSON rỗng, không tạo được file Excel.")

    def run(self):
        """
        Thực thi toàn bộ quy trình crawl dữ liệu.
        Trả về danh sách các bài báo mới đã crawl được.
        """
        with self._create_session() as session:
            # Giai đoạn 1: Do thám tuần tự
            print("--- Giai đoạn 1: Do thám các sub-category ---")
            valid_subcategories = []
            pbar = tqdm.tqdm(total=sum(len(v) for v in self.type_dict.values()), desc="Scanning")
            for category, sub_categories in self.type_dict.items():
                for sub_cat in sub_categories:
                    if self._is_subcategory_valid(session, category, sub_cat):
                        valid_subcategories.append((category, sub_cat))
                        tqdm.tqdm.write(f"[HỢP LỆ] Sub-category: {category}/{sub_cat}")
                    else:
                        tqdm.tqdm.write(f"[BỎ QUA] Sub-category: {category}/{sub_cat}")
                    pbar.update(1)
            pbar.close()
            
            if not valid_subcategories:
                print("--- Hoàn thành. Không có sub-category nào hợp lệ. ---")
                return []

            # Giai đoạn 2: Crawl đa luồng
            print(f"\n--- Giai đoạn 2: Crawl đa luồng từ {len(valid_subcategories)} sub-category hợp lệ ---")
            existing_urls = self._load_existing_urls()
            
            all_urls_info = []
            for category, sub_cat in tqdm.tqdm(valid_subcategories, desc="Collecting URLs"):
                all_urls_info.extend(self._get_all_urls_from_valid_subcategory(session, category, sub_cat))
            
            new_urls_to_scrape = [info for info in all_urls_info if info['url'] not in existing_urls]
            
            print(f"Tìm thấy {len(new_urls_to_scrape)} URL mới để crawl.")
            if not new_urls_to_scrape:
                self._export_to_files([])
                return []

            new_articles = []
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                future_to_url = {
                    executor.submit(self._scrape_article_details, session, info['url'], info['cat'], info['sub']): info 
                    for info in new_urls_to_scrape
                }
                progress_bar = tqdm.tqdm(as_completed(future_to_url), total=len(new_urls_to_scrape), desc="Crawling")
                for future in progress_bar:
                    if result := future.result():
                        new_articles.append(result)

        print(f"\n--- Hoàn thành. Crawl được {len(new_articles)} bài báo mới. ---")
        self._export_to_files(new_articles)
        
        return new_articles