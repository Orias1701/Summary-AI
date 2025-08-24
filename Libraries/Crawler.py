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

class BaseCrawler:

    # Lớp cơ sở chứa các hàm và thuộc tính chung cho các crawler con.

    def __init__(self, config):
        self.base_url = config.get("BASE_URL", "https://vnexpress.net")
        self.min_year = config.get("MIN_YEAR", 2020)
        self.min_words = config.get("MIN_WORDS", 200)
        self.max_words = config.get("MAX_WORDS", 1000)
        self.HEADERS = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Cache-Control': 'max-age=0',
        }
        self.VIETNAMESE_DAYS = {"Chủ nhật": "Sunday", "Thứ hai": "Monday", "Thứ ba": "Tuesday", "Thứ tư": "Wednesday", "Thứ năm": "Thursday", "Thứ sáu": "Friday", "Thứ bảy": "Saturday"}

    def createSession(self):
        session = requests.Session()
        retries = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"]
        )

        adapter = HTTPAdapter(max_retries=retries)
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        return session

    def fetchPageContent(self, session, url):
        try:
            time.sleep(random.uniform(0.5, 1.5))
            response = session.get(url, headers=self.HEADERS, timeout=10)
            response.raise_for_status()
            print(f"[OK] Fetched: {url}")
            return response.content

        except requests.exceptions.Timeout:
            print(f"[TIMEOUT] Request to {url} took too long.")
            return None

        except requests.exceptions.HTTPError as e:
            print(f"[HTTP ERROR] {url} returned status {e.response.status_code}.")
            return None

        except requests.exceptions.ConnectionError:
            print(f"[CONNECTION ERROR] Failed to connect to {url}.")
            return None

        except requests.exceptions.RequestException as e:
            print(f"[REQUEST ERROR] {url} failed with error: {e}")
            return None

    def convertVietnameseDate(self, date_str):
        for vn_day, en_day in self.VIETNAMESE_DAYS.items():
            if vn_day in date_str:
                date_str = date_str.replace(vn_day, en_day)
                break
        date_str = re.sub(r"\s\(GMT[+-]\d{1,2}\)", "", date_str)
        try:
            return datetime.strptime(date_str, "%A, %d/%m/%Y, %H:%M")
        except ValueError:
            return None

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


# --- GIAI ĐOẠN 2: THU THẬP URL ---
class UrlCollector(BaseCrawler):
    def __init__(self, config):
        super().__init__(config)
        self.target_articles = config.get("TARGET_ARTICLES_PER_SUBTYPE", 30)
        self.progress_timeout = config.get("PROGRESS_TIMEOUT", 10)

    def collectSubcategoryUrls(self, session, category, sub_category):

        # Lấy tất cả URL từ một sub-category.
        
        urls = []
        page_num = 1
        pbar = tqdm.tqdm(total=self.target_articles, desc=f"Lấy URL: {category}/{sub_category}")
        
        time_of_last_progress = time.time()
        while len(urls) < self.target_articles:
            # 1. Kiểm tra timeout ở đầu mỗi vòng lặp
            if time.time() - time_of_last_progress > self.progress_timeout:
                tqdm.tqdm.write(f"\n[!] Timeout: Không tìm thấy URL mới trong {self.progress_timeout} giây cho mục {category}/{sub_category}. Bỏ qua.")
                break

            list_url = f"{self.base_url}/{category}/{sub_category}-p{page_num}"
            content = self.fetchPageContent(session, list_url)
            if not content: break
            
            soup = BeautifulSoup(content, "lxml")
            title_tags = soup.find_all(class_="title-news")
            if not title_tags: break
            
            urls_found_on_this_page = 0
            for title in title_tags:
                link_tag = title.find("a")
                if link_tag and link_tag.get("href"):
                    urls.append({'url': link_tag.get("href"), 'cat': category, 'sub': sub_category})
                    pbar.update(1)
                    urls_found_on_this_page += 1
                    if len(urls) >= self.target_articles: break
            
            if urls_found_on_this_page > 0:
                time_of_last_progress = time.time()
            
            page_num += 1
        
        pbar.close()
        return urls

    def run(self, valid_subcategories):

        # Nhận danh sách sub-category hợp lệ, duyệt để lấy URL.
        # Trả về: list các dictionary thông tin URL (định dạng JSON).

        all_urls_info = []
        with self.createSession() as session:
            print(f"\n--- Giai đoạn 2: Bắt đầu thu thập URL từ {len(valid_subcategories)} sub-category hợp lệ ---")
            for cat_info in valid_subcategories:
                all_urls_info.extend(self.collectSubcategoryUrls(session, cat_info['cat'], cat_info['sub']))
        print(f"--- Hoàn thành Giai đoạn 2. Thu thập được {len(all_urls_info)} URL. ---")
        return all_urls_info


# --- GIAI ĐOẠN 3: CRAWL NỘI DUNG (ĐÃ CẬP NHẬT) ---
class ArticleCrawler(BaseCrawler):
    def __init__(self, config):
        super().__init__(config)
        self.max_workers = config.get("MAX_CONCURRENT_WORKERS", 6)
        self.article_timeout = config.get("ARTICLE_TIMEOUT", 10)
        self.max_failures = config.get("MAX_CONSECUTIVE_FAILURES", 3)

    def scrapeArticleDetails(self, session, url_info):

        # Lấy chi tiết nội dung từ một dictionary thông tin URL.

        url = url_info['url']
        category = url_info['cat']
        sub_category = url_info['sub']
        
        content = self.fetchPageContent(session, url)
        if not content: return None
        soup = BeautifulSoup(content, "lxml")
        
        title = soup.find("h1", class_="title-detail").get_text(strip=True) if soup.find("h1", class_="title-detail") else ""
        description = soup.find("p", class_="description").get_text(strip=True) if soup.find("p", class_="description") else ""
        date_obj = self.convertVietnameseDate(soup.find("span", class_="date").text.strip()) if soup.find("span", class_="date") else None
        
        if not all([title, description, date_obj]) or date_obj.year < self.min_year:
            return None
        
        article_body = " ".join(p.get_text(" ", strip=True) for p in soup.find_all("p", class_="Normal"))
        word_count = len(article_body.split())

        if self.min_words < word_count < self.max_words:
            print(f"{sub_category}: Success")
            return {
                "category": category, "sub_category": sub_category, "url": url,
                "title": title, "description": description, "content": article_body,
                "date": date_obj.strftime("%Y-%m-%d %H:%M:%S"), "words": word_count,
            }
        return None
    
    def run(self, urls_to_crawl, existing_article_urls=set()):

        # Nhận danh sách URL, crawl đa luồng với cơ chế timeout và tự động ngắt.
        # Trả về: list các dictionary bài viết (định dạng JSON).

        final_urls_to_scrape = [info for info in urls_to_crawl if info['url'] not in existing_article_urls]
        
        print(f"\n--- Giai đoạn 3: Bắt đầu crawl nội dung ---")
        print(f"Tổng cộng {len(urls_to_crawl)} URL, trong đó có {len(final_urls_to_scrape)} URL mới cần crawl.")
        
        if not final_urls_to_scrape:
            print("--- Hoàn thành Giai đoạn 3. Không có bài báo mới nào để crawl. ---")
            return []

        new_articles = []
        consecutive_failures = 0
        
        with self.createSession() as session:
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                future_to_info = {
                    executor.submit(self.scrapeArticleDetails, session, info): info 
                    for info in final_urls_to_scrape
                }
                
                progress_bar = tqdm.tqdm(as_completed(future_to_info), total=len(final_urls_to_scrape), desc="Crawling Articles")
                
                for future in progress_bar:
                    try:
                        result = future.result(timeout=self.article_timeout)
                        
                        if result:
                            new_articles.append(result)
                            consecutive_failures = 0

                    except TimeoutError:
                        consecutive_failures += 1

                    # Kiểm tra điều kiện dừng sau mỗi lần xử lý
                    if consecutive_failures >= self.max_failures:
                        tqdm.tqdm.write(f"\n[CẢNH BÁO] Đã có {self.max_failures} lỗi liên tiếp. Dừng crawl...")
                        for f in future_to_info:
                            f.cancel()
                        break

        print(f"--- Hoàn thành Giai đoạn 3. Crawl được {len(new_articles)} bài báo mới. ---")
        return new_articles