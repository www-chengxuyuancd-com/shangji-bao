import os
from dotenv import load_dotenv

load_dotenv()

BOT_NAME = "shangji_bao"
SPIDER_MODULES = ["src.crawler.spiders"]
NEWSPIDER_MODULE = "src.crawler.spiders"

ROBOTSTXT_OBEY = True
CONCURRENT_REQUESTS = 8
DOWNLOAD_DELAY = 1
RANDOMIZE_DOWNLOAD_DELAY = True

DEFAULT_REQUEST_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
}

ITEM_PIPELINES = {
    "src.crawler.pipelines.DedupPipeline": 100,
    "src.crawler.pipelines.MongoStoragePipeline": 200,
    "src.crawler.pipelines.PostgresStoragePipeline": 300,
}

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://shangji:shangji123@localhost:5432/shangjibao")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://shangji:shangji123@localhost:27017/shangjibao?authSource=admin")

LOG_LEVEL = "INFO"
REQUEST_FINGERPRINTER_IMPLEMENTATION = "2.7"
TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"
FEED_EXPORT_ENCODING = "utf-8"
