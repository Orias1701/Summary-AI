# Tên file: Libraries/Crawler.py

import pandas as pd
import requests
import json
import os
import re
import tqdm
import time
import random
import itertools
from bs4 import BeautifulSoup
from datetime import datetime
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError

# --- LỚP CƠ SỞ (BASE CLASS) ---
class BaseCrawler:
    """
    Lớp cơ sở chứa các hàm và thuộc tính chung. Sử dụng 'requests'.
    """
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


# --- GIAI ĐOẠN 2: THU THẬP URL ---
class UrlCollector(BaseCrawler):
    def __init__(self, config):
        super().__init__(config)
        self.target_articles = config.get("TARGET_ARTICLES_PER_SUBTYPE", 30)
        self.page_timeout = config.get("URL_PAGE_TIMEOUT", 10)
        self.max_page_failures = config.get("URL_MAX_PAGE_FAILURES", 3)
        self.max_subcategory_failures = config.get("URL_MAX_SUBCATEGORY_FAILURES", 3)

    def _collectSubcategoryUrls(self, session, category, sub_category):
        urls = []
        page_num = 1
        consecutive_page_failures = 0
        pbar = tqdm.tqdm(total=self.target_articles, desc=f"URL: {category}/{sub_category}")
        
        while len(urls) < self.target_articles:
            if consecutive_page_failures >= self.max_page_failures:
                tqdm.tqdm.write(f"[!] Bỏ qua sub-category: '{category}/{sub_category}' do {self.max_page_failures} lần timeout liên tiếp.")
                pbar.close()
                return None # Trả về None để báo hiệu thất bại
            
            list_url = f"{self.base_url}/{category}/{sub_category}-p{page_num}"
            content = self.fetchPageContent(session, list_url, timeout=self.page_timeout)
            
            if not content:
                consecutive_page_failures += 1
                page_num += 1
                continue

            soup = BeautifulSoup(content, "lxml")
            title_tags = soup.find_all(class_="title-news")
            if not title_tags:
                consecutive_page_failures += 1
                page_num += 1
                continue

            found_new = False
            for title in title_tags:
                if link := title.find("a", href=True):
                    urls.append({'url': link['href'], 'cat': category, 'sub': sub_category})
                    pbar.update(1)
                    found_new = True
                    if len(urls) >= self.target_articles: break
            
            if found_new: consecutive_page_failures = 0
            else: consecutive_page_failures += 1
            page_num += 1
        
        pbar.close()
        return urls

    def run(self, valid_subcategories):
        all_urls_info = []
        grouped_by_cat = {k: list(v) for k, v in itertools.groupby(sorted(valid_subcategories, key=lambda x: x['cat']), key=lambda x: x['cat'])}
        
        try:
            with self.createSession() as session:
                for category, subcategories_list in grouped_by_cat.items():
                    consecutive_subcategory_failures = 0
                    print(f"\n--- Đang xử lý category: {category} ---")
                    
                    for sub_info in subcategories_list:
                        if consecutive_subcategory_failures >= self.max_subcategory_failures:
                            tqdm.tqdm.write(f"[!!] Bỏ qua category: '{category}' do {self.max_subcategory_failures} sub-category lỗi liên tiếp.")
                            break # Thoát vòng lặp sub-category, chuyển sang category tiếp theo
                        
                        result = self._collectSubcategoryUrls(session, sub_info['cat'], sub_info['sub'])
                        if result is not None:
                            all_urls_info.extend(result)
                            consecutive_subcategory_failures = 0
                        else:
                            consecutive_subcategory_failures += 1
        except KeyboardInterrupt:
            print("\n[DỪNG] Đã nhận lệnh dừng từ người dùng.")
        finally:
            print(f"--- Giai đoạn 2 Kết thúc. Thu thập được tổng cộng {len(all_urls_info)} URL. ---")
            return all_urls_info

# --- GIAI ĐOẠN 3: CRAWL NỘI DUNG ---
class ArticleCrawler(BaseCrawler):
    def __init__(self, config):
        super().__init__(config)
        self.max_workers = config.get("MAX_CONCURRENT_WORKERS", 6)
        self.article_timeout = config.get("ARTICLE_TIMEOUT", 5)

    def scrapeArticleDetails(self, session, url_info):
        url = url_info['url']
        category = url_info['cat']
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
            return {"category": category, "sub_category": sub_category, "url": url, "title": title, "description": description, "content": article_body, "date": date_obj.strftime("%Y-%m-%d %H:%M:%S"), "words": word_count}
        return None
    
    def run(self, urls_to_crawl, existing_article_urls=set()):
        final_urls_to_scrape = [info for info in urls_to_crawl if info['url'] not in existing_article_urls]
        
        print(f"\n--- Giai đoạn 3: Bắt đầu crawl nội dung từ {len(final_urls_to_scrape)} URL mới ---")
        if not final_urls_to_scrape: return [], []

        new_articles = []
        crawled_urls = []
        try:
            with self.createSession() as session:
                with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                    future_to_info = {executor.submit(self.scrapeArticleDetails, session, info): info for info in final_urls_to_scrape}
                    progress_bar = tqdm.tqdm(as_completed(future_to_info), total=len(final_urls_to_scrape), desc="Crawling Articles")
                    
                    for future in progress_bar:
                        try:
                            result = future.result()
                            if result:
                                new_articles.append(result)
                                crawled_urls.append(result['url'])
                        except Exception: continue
        except KeyboardInterrupt:
            print("\n[DỪNG] Đã nhận lệnh dừng từ người dùng.")
        finally:
            print(f"--- Giai đoạn 3 Kết thúc. Crawl thành công {len(new_articles)} bài báo. ---")
            return new_articles, crawled_urls