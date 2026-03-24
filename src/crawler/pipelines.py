import hashlib
import logging

from pymongo import MongoClient
from prisma import Prisma

logger = logging.getLogger(__name__)


class DedupPipeline:
    """基于 URL hash 去重，跳过已采集的页面。"""

    def open_spider(self, spider):
        self.prisma = Prisma()
        self.prisma.connect()

    def close_spider(self, spider):
        self.prisma.disconnect()

    def process_item(self, item, spider):
        url_hash = hashlib.sha256(item["url"].encode("utf-8")).hexdigest()
        existing = self.prisma.visitedurl.find_unique(where={"urlHash": url_hash})
        if existing:
            from scrapy.exceptions import DropItem
            raise DropItem(f"Duplicate URL: {item['url']}")
        item["_url_hash"] = url_hash
        return item


class MongoStoragePipeline:
    """将原始 HTML 和 hash 存入 MongoDB。"""

    def open_spider(self, spider):
        self.client = MongoClient(spider.settings.get("MONGO_URI"))
        self.db = self.client.get_default_database()
        self.collection = self.db["raw_pages"]

    def close_spider(self, spider):
        self.client.close()

    def process_item(self, item, spider):
        html = item.get("html", "")
        content_hash = hashlib.sha256(html.encode("utf-8")).hexdigest()

        result = self.collection.insert_one({
            "url": item["url"],
            "html": html,
            "content_hash": content_hash,
            "meta": {
                "title": item.get("title", ""),
                "source_type": item.get("source_type", ""),
            },
        })
        item["_mongo_doc_id"] = str(result.inserted_id)
        return item


class PostgresStoragePipeline:
    """将结构化数据存入 PostgreSQL。"""

    def open_spider(self, spider):
        self.prisma = Prisma()
        self.prisma.connect()

    def close_spider(self, spider):
        self.prisma.disconnect()

    def process_item(self, item, spider):
        url_hash = item.get("_url_hash", hashlib.sha256(item["url"].encode("utf-8")).hexdigest())

        self.prisma.visitedurl.create(data={
            "url": item["url"],
            "urlHash": url_hash,
            "status": 200,
        })

        region = None
        region_name = item.get("region_name")
        if region_name:
            region = self.prisma.searchregion.find_first(where={"name": region_name})

        self.prisma.searchresult.create(data={
            "title": item.get("title", ""),
            "url": item["url"],
            "urlHash": url_hash,
            "sourceType": item.get("source_type"),
            "regionId": region.id if region else None,
            "publishDate": item.get("publish_date"),
            "mongoDocId": item.get("_mongo_doc_id"),
        })

        return item
