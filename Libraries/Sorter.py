# Tên file: Libraries/sorter_module.py

import json
import re

class ArticleSorter:
    """
    Một công cụ để xóa bản ghi trùng lặp và sắp xếp danh sách các bài báo
    dựa trên thứ tự tùy chỉnh về category, subcategory và chỉ số URL.
    """
    def __init__(self, categories_file_path):
        """
        Khởi tạo Sorter bằng cách đọc file JSON chứa thứ tự các category.
        """
        self.category_order = self._load_category_order(categories_file_path)

    def _load_category_order(self, file_path):
        """
        Đọc file JSON và tạo ra một bản đồ thứ tự cho category và subcategory.
        """
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                categories_dict = json.load(f)
            
            category_map = {category: index for index, category in enumerate(categories_dict.keys())}
            subcategory_map = {}
            for category, sub_list in categories_dict.items():
                subcategory_map[category] = {sub: index for index, sub in enumerate(sub_list)}
            
            return {'categories': category_map, 'subcategories': subcategory_map}
        except (FileNotFoundError, json.JSONDecodeError):
            print(f"Lỗi: Không thể đọc file thứ tự category tại '{file_path}'")
            return None

    def _get_sort_key(self, article):
        """
        Tạo ra một 'khóa' để sắp xếp cho mỗi bài báo.
        """
        if not self.category_order:
            return (0, 0, 0)

        cat_order = self.category_order['categories'].get(article.get('category'), 999)
        sub_cat_order = self.category_order['subcategories'].get(article.get('category'), {}).get(article.get('sub_category'), 999)
        
        url_index = 0
        if url := article.get('url'):
            match = re.search(r'-(\d+)\.html', url)
            if match:
                url_index = int(match.group(1))
        
        return (cat_order, sub_cat_order, url_index)

    def sort_and_deduplicate(self, articles_list):
        """
        Xóa các bài báo trùng lặp (dựa trên URL) và sắp xếp danh sách.
        """
        if not self.category_order or not isinstance(articles_list, list):
            print("Không thể xử lý do thiếu dữ liệu hoặc sai định dạng.")
            return articles_list

        # --- PHẦN MỚI: XÓA TRÙNG LẶP ---
        print(f"Dữ liệu gốc có {len(articles_list)} bài báo.")
        seen_urls = set()
        unique_articles = []
        for article in articles_list:
            url = article.get('url')
            if url and url not in seen_urls:
                unique_articles.append(article)
                seen_urls.add(url)
        
        num_duplicates = len(articles_list) - len(unique_articles)
        if num_duplicates > 0:
            print(f"Đã tìm thấy và loại bỏ {num_duplicates} bản ghi trùng lặp.")
        
        # --- PHẦN SẮP XẾP ---
        print(f"Bắt đầu sắp xếp {len(unique_articles)} bài báo...")
        
        sorted_list = sorted(unique_articles, key=self._get_sort_key)
        
        print("Sắp xếp hoàn tất.")
        return sorted_list