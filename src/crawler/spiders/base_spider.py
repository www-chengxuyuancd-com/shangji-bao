"""
基础爬虫模板。
具体的网站爬虫需要继承此类并实现 parse 方法。
"""
import scrapy

from src.crawler.items import SearchResultItem


class BaseSearchSpider(scrapy.Spider):
    """
    抽象基础爬虫，子类需实现：
    - build_search_url(keyword, region): 根据关键词和地区构建搜索 URL
    - parse(response): 解析搜索结果列表页
    - parse_detail(response): 解析详情页（可选）
    """
    name = "base_search"
    custom_settings = {}

    def __init__(self, keyword=None, region=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.search_keyword = keyword
        self.search_region = region

    def start_requests(self):
        if not self.search_keyword:
            self.logger.error("No keyword provided")
            return

        url = self.build_search_url(self.search_keyword, self.search_region)
        yield scrapy.Request(url, callback=self.parse)

    def build_search_url(self, keyword: str, region: str | None) -> str:
        raise NotImplementedError

    def parse(self, response, **kwargs):
        raise NotImplementedError

    def build_item(self, title, url, html="", source_type=None, region_name=None, publish_date=None):
        item = SearchResultItem()
        item["title"] = title
        item["url"] = url
        item["html"] = html
        item["source_type"] = source_type
        item["region_name"] = region_name
        item["publish_date"] = publish_date
        return item
