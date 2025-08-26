# Tên file: Libraries/Crawler.py

import requests
import re
import tqdm
import time
import random
from bs4 import BeautifulSoup
from datetime import datetime
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- LỚP CƠ SỞ (BASE CLASS) ---
class BaseCrawler:
    def __init__(self, config):
        self.base_url = config.get("BASE_URL", "https://vnexpress.net")
        self.min_year = config.get("MIN_YEAR", 2020)
        self.min_words = config.get("MIN_WORDS", 200)
        self.max_words = config.get("MAX_WORDS", 1000)
        self.USER_AGENTS = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
        ]
        self.VIETNAMESE_DAYS = {"Chủ nhật": "Sunday", "Thứ hai": "Monday", "Thứ ba": "Tuesday", "Thứ tư": "Wednesday", "Thứ năm": "Thursday", "Thứ sáu": "Friday", "Thứ bảy": "Saturday"}

    def createSession(self):
        session = requests.Session()
        retries = Retry(total=3, backoff_factor=0.5, status_forcelist=[500, 502, 503, 504])
        adapter = HTTPAdapter(max_retries=retries)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    def fetchPageContent(self, session, url, timeout):
        try:
            time.sleep(random.uniform(0.5, 1.5))
            headers = {
                'User-Agent': random.choice(self.USER_AGENTS),
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
            }
            response = session.get(url, headers=headers, timeout=timeout)
            response.raise_for_status()
            return response.content
        except requests.exceptions.RequestException:
            return None

    def convertVietnameseDate(self, date_str):
        for vn_day, en_day in self.VIETNAMESE_DAYS.items():
            if vn_day in date_str: date_str = date_str.replace(vn_day, en_day)
        date_str = re.sub(r"\s\(GMT[+-]\d{1,2}\)", "", date_str)
        try: return datetime.strptime(date_str, "%A, %d/%m/%Y, %H:%M")
        except ValueError: return None

# --- GIAI ĐOẠN 1: KIỂM TRA CHUYÊN MỤC ---
class CategoryValidator(BaseCrawler):
    def __init__(self, config):
        super().__init__(config)
        self.validation_count = config.get("VALIDATION_ARTICLES_COUNT", 5)
        self.type_dict = config.get("TYPE_DICT", {})

    def scrapeTemporaryArticle(self, session, url):
        
        # Scrape một bài báo tạm thời chỉ để kiểm tra tính hợp lệ.

        content = self.fetchPageContent(session, url)
        if not content: return False
        soup = BeautifulSoup(content, "lxml")
        
        description = soup.find("p", class_="description")
        date_obj = self.convertVietnameseDate(soup.find("span", class_="date").text.strip()) if soup.find("span", class_="date") else None
        
        if not all([description, date_obj]) or date_obj.year < self.min_year:
            return False

        article_body = " ".join(p.get_text(" ", strip=True) for p in soup.find_all("p", class_="Normal"))
        word_count = len(article_body.split())

        return self.min_words < word_count < self.max_words

    def run(self):

        # Duyệt tuần tự để lấy danh sách category/subcategory hợp lệ.
        # Trả về: list các tuple (category, sub_category)

        valid_subcategories = []
        with self.createSession() as session:
            print("--- Giai đoạn 1: Bắt đầu kiểm tra các sub-category ---")
            pbar_scan = tqdm.tqdm(total=sum(len(v) for v in self.type_dict.values()), desc="Scanning")
            for category, sub_categories in self.type_dict.items():
                for sub_cat in sub_categories:
                    list_url = f"{self.base_url}/{category}/{sub_cat}-p1"
                    content = self.fetchPageContent(session, list_url)
                    if content:
                        soup = BeautifulSoup(content, "lxml")
                        title_tags = soup.find_all(class_="title-news")
                        urls_to_check = [tag.find("a").get("href") for tag in title_tags if tag.find("a")]
                        
                        validated = 0
                        for url in urls_to_check[:self.validation_count]:
                            if self.scrapeTemporaryArticle(session, url):
                                validated += 1
                                if validated >= self.validation_count / 5:
                                    valid_subcategories.append({'cat': category, 'sub': sub_cat})
                                    tqdm.tqdm.write(f"[HỢP LỆ] Sub-category: {category}/{sub_cat}")
                                    break
                    pbar_scan.update(1)
            pbar_scan.close()
        print(f"--- Hoàn thành Giai đoạn 1. Tìm thấy {len(valid_subcategories)} sub-category hợp lệ. ---")
        return valid_subcategories


# --- GIAI ĐOẠN 2: THU THẬP URL (ĐÃ CẬP NHẬT) ---
class UrlCollector(BaseCrawler):
    def __init__(self, config):
        super().__init__(config)
        self.target_articles = config.get("TARGET_ARTICLES_PER_SUBTYPE", 30)
        self.page_timeout = config.get("URL_PAGE_TIMEOUT", 10)
    
    def _collectSubcategoryUrls(self, session, category, sub_category):
        urls = []
        page_num = 1
        pbar = tqdm.tqdm(total=self.target_articles, desc=f"URL: {category}/{sub_category}")
        time_of_last_progress = time.time()
        
        while len(urls) < self.target_articles:
            if time.time() - time_of_last_progress > self.page_timeout:
                tqdm.tqdm.write(f"[!] Timeout: Bỏ qua sub-category '{category}/{sub_category}'.")
                break

            list_url = f"{self.base_url}/{category}/{sub_category}-p{page_num}"
            content = self.fetchPageContent(session, list_url, timeout=self.page_timeout)
            
            if not content:
                page_num += 1
                continue

            soup = BeautifulSoup(content, "lxml")
            title_tags = soup.find_all(class_="title-news")
            if not title_tags: break

            found_new = False
            for title in title_tags:
                if link := title.find("a", href=True):
                    # Thay đổi dữ liệu trả về ở đây
                    urls.append({'url': link['href'], 'sub': sub_category})
                    pbar.update(1)
                    found_new = True
                    if len(urls) >= self.target_articles: break
            
            if found_new: time_of_last_progress = time.time()
            page_num += 1
        
        pbar.close()
        return urls

    def run(self, valid_subcategories, categories_to_process: list):
        all_urls_info = []
        subcategories_to_process = [
            sub for sub in valid_subcategories if sub['cat'] in categories_to_process
        ]
        
        if not subcategories_to_process:
            print(f"Không tìm thấy sub-category hợp lệ nào cho các category: {categories_to_process}")
            return []

        print(f"\n--- Bắt đầu thu thập URL từ {len(subcategories_to_process)} sub-category được chọn ---")
        try:
            with self.createSession() as session:
                for sub_info in subcategories_to_process:
                    all_urls_info.extend(self._collectSubcategoryUrls(session, sub_info['cat'], sub_info['sub']))
        except KeyboardInterrupt:
            print("\n[DỪNG] Đã nhận lệnh dừng từ người dùng.")
        finally:
            print(f"--- Giai đoạn 2 Kết thúc. Thu thập được {len(all_urls_info)} URL. ---")
            return all_urls_info

# --- GIAI ĐOẠN 3: CRAWL NỘI DUNG (ĐÃ CẬP NHẬT) ---
class ArticleCrawler(BaseCrawler):
    def __init__(self, config):
        super().__init__(config)
        self.max_workers = config.get("MAX_CONCURRENT_WORKERS", 6)
        self.article_timeout = config.get("ARTICLE_TIMEOUT", 5)

    def scrapeArticleDetails(self, session, url_info, default_category: str):
        """Nhận category mặc định từ tham số."""
        url = url_info['url']
        sub_category = url_info['sub']
        
        content = self.fetchPageContent(session, url, timeout=self.article_timeout)
        if not content: return None
        soup = BeautifulSoup(content, "lxml")
        
        title = soup.find("h1", class_="title-detail").get_text(strip=True) if soup.find("h1", class_="title-detail") else ""
        description = soup.find("p", class_="description").get_text(strip=True) if soup.find("p", class_="description") else ""
        date_obj = self.convertVietnameseDate(soup.find("span", class_="date").text.strip()) if soup.find("span", class_="date") else None
        
        if not all([title, description, date_obj]) or date_obj.year < self.min_year: return None

        article_body = " ".join(p.get_text(" ", strip=True) for p in soup.find_all("p", class_="Normal"))
        word_count = len(article_body.split())

        if self.min_words < word_count < self.max_words:
            return {"category": default_category, "sub_category": sub_category, "url": url, "title": title, "description": description, "content": article_body, "date": date_obj.strftime("%Y-%m-%d %H:%M:%S"), "words": word_count}
        return None
    
    def run(self, urls_to_crawl, category: str, existing_article_urls=set()):
        """
        Crawl và trả về danh sách bài viết mới và danh sách URL đã crawl thành công.
        """
        final_urls_to_scrape = [info for info in urls_to_crawl if info['url'] not in existing_article_urls]
        
        print(f"\n--- Bắt đầu crawl {len(final_urls_to_scrape)} URL cho category '{category}' ---")
        if not final_urls_to_scrape: 
            # Vẫn trả về 2 giá trị để đảm bảo tính nhất quán
            return [], []

        new_articles = []
        crawled_urls = [] # Danh sách mới để lưu các URL đã crawl thành công
        try:
            with self.createSession() as session:
                with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                    future_to_info = {executor.submit(self.scrapeArticleDetails, session, info, category): info for info in final_urls_to_scrape}
                    progress_bar = tqdm.tqdm(as_completed(future_to_info), total=len(final_urls_to_scrape), desc=f"Crawling {category}")
                    
                    for future in progress_bar:
                        try:
                            if result := future.result():
                                new_articles.append(result)
                                # Thêm URL vào danh sách đã crawl thành công
                                crawled_urls.append(result['url']) 
                        except Exception: 
                            continue
        except KeyboardInterrupt:
            print("\n[DỪNG] Đã nhận lệnh dừng từ người dùng.")
            print(f"--- Kết thúc crawl. Thu được {len(new_articles)} bài báo mới. ---")
            return new_articles, crawled_urls
        finally:
            print(f"--- Kết thúc crawl. Thu được {len(new_articles)} bài báo mới. ---")
            # Trả về cả hai danh sách
            return new_articles, crawled_urls