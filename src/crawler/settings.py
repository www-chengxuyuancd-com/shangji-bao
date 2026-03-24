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

DOWNLOADER_MIDDLEWARES = {
    "scrapy.downloadermiddlewares.useragent.UserAgentMiddleware": None,
    "src.crawler.middlewares.RandomUserAgentMiddleware": 400,
    "src.crawler.middlewares.ProxyMiddleware": 410,
    "src.crawler.middlewares.LoginMiddleware": 420,
    "src.crawler.middlewares.CaptchaDetectionMiddleware": 430,
}

ITEM_PIPELINES = {
    "src.crawler.pipelines.DedupPipeline": 100,
    "src.crawler.pipelines.MongoStoragePipeline": 200,
    "src.crawler.pipelines.PostgresStoragePipeline": 300,
}

# ===== 数据库 =====
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://shangji:shangji123@localhost:5432/shangjibao")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://shangji:shangji123@localhost:27017/shangjibao?authSource=admin")

# ===== 代理池 =====
# 静态代理列表（逗号分隔）
PROXY_LIST = os.getenv("PROXY_LIST", "")
# 代理 API 地址（返回纯文本列表或 JSON）
PROXY_API_URL = os.getenv("PROXY_API_URL", "")
# 代理 API 拉取间隔（秒）
PROXY_API_INTERVAL = int(os.getenv("PROXY_API_INTERVAL", "60"))

# ===== 验证码 =====
# 第三方打码平台 API 地址
CAPTCHA_API_URL = os.getenv("CAPTCHA_API_URL", "")
# 第三方打码平台 API Key
CAPTCHA_API_KEY = os.getenv("CAPTCHA_API_KEY", "")
# 打码超时（秒）
CAPTCHA_API_TIMEOUT = int(os.getenv("CAPTCHA_API_TIMEOUT", "30"))

LOG_LEVEL = "INFO"
REQUEST_FINGERPRINTER_IMPLEMENTATION = "2.7"
TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"
FEED_EXPORT_ENCODING = "utf-8"
