import logging
import random
import time
from urllib.parse import urlparse

from scrapy import signals
from scrapy.exceptions import IgnoreRequest
from scrapy.http import HtmlResponse

from src.crawler.anti_block.proxy_pool import get_proxy_pool
from src.crawler.anti_block.captcha import get_captcha_solver
from src.crawler.anti_block.login import get_login_manager

logger = logging.getLogger(__name__)

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:133.0) Gecko/20100101 Firefox/133.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.2 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0",
]

BLOCKED_STATUS_CODES = {403, 429, 503, 407}

CAPTCHA_INDICATORS = [
    "captcha", "验证码", "verify", "slider", "滑块",
    "human verification", "access denied", "请完成验证",
]


class RandomUserAgentMiddleware:
    """随机 User-Agent 中间件。"""

    def process_request(self, request, spider):
        request.headers["User-Agent"] = random.choice(USER_AGENTS)


class ProxyMiddleware:
    """
    代理中间件。
    - 请求时自动从代理池获取代理
    - 根据响应状态自动反馈代理质量
    - 被封禁时自动换代理重试
    """

    MAX_RETRY = 3

    @classmethod
    def from_crawler(cls, crawler):
        m = cls()
        pool = get_proxy_pool()

        proxy_list = crawler.settings.get("PROXY_LIST", "")
        if proxy_list:
            pool.add_many([p.strip() for p in proxy_list.split(",") if p.strip()])

        proxy_api = crawler.settings.get("PROXY_API_URL", "")
        if proxy_api:
            interval = crawler.settings.getint("PROXY_API_INTERVAL", 60)
            pool.set_api(proxy_api, interval)

        return m

    def process_request(self, request, spider):
        if request.meta.get("no_proxy"):
            return

        pool = get_proxy_pool()
        if pool.total_count == 0:
            return

        proxy = pool.get()
        if proxy:
            request.meta["proxy"] = proxy
            request.meta["_proxy_url"] = proxy
            logger.debug("Using proxy: %s for %s", proxy, request.url)

    def process_response(self, request, response, spider):
        proxy_url = request.meta.get("_proxy_url")
        if not proxy_url:
            return response

        pool = get_proxy_pool()
        if response.status in BLOCKED_STATUS_CODES:
            pool.feedback(proxy_url, success=False)
            retries = request.meta.get("_proxy_retry", 0)
            if retries < self.MAX_RETRY:
                logger.info("Proxy blocked (HTTP %d), retrying: %s", response.status, request.url)
                new_request = request.copy()
                new_request.meta["_proxy_retry"] = retries + 1
                new_request.dont_filter = True
                return new_request
            logger.warning("Max proxy retries reached for: %s", request.url)
        else:
            pool.feedback(proxy_url, success=True)

        return response

    def process_exception(self, request, exception, spider):
        proxy_url = request.meta.get("_proxy_url")
        if proxy_url:
            get_proxy_pool().feedback(proxy_url, success=False)


class CaptchaDetectionMiddleware:
    """
    验证码检测中间件。
    检测响应中是否包含验证码页面，尝试自动处理。
    """

    @classmethod
    def from_crawler(cls, crawler):
        m = cls()
        api_url = crawler.settings.get("CAPTCHA_API_URL", "")
        api_key = crawler.settings.get("CAPTCHA_API_KEY", "")
        if api_url and api_key:
            from src.crawler.anti_block.captcha import setup_third_party_solver
            timeout = crawler.settings.getint("CAPTCHA_API_TIMEOUT", 30)
            setup_third_party_solver(api_url, api_key, timeout)
        return m

    def process_response(self, request, response, spider):
        if not self._is_captcha_page(response):
            return response

        logger.info("Captcha detected on: %s", request.url)
        solver = get_captcha_solver()
        result = solver.solve(
            captcha_type="image",
            page_url=request.url,
            meta={"status": response.status},
        )

        if result.success:
            logger.info("Captcha solved, retrying: %s", request.url)
            new_request = request.copy()
            new_request.dont_filter = True
            new_request.meta["captcha_answer"] = result.answer
            return new_request

        logger.warning("Captcha not solved for: %s - %s", request.url, result.error)
        return response

    def _is_captcha_page(self, response) -> bool:
        if response.status in (200, 302):
            text = response.text.lower() if hasattr(response, "text") else ""
            return any(indicator in text for indicator in CAPTCHA_INDICATORS)
        return False


class LoginMiddleware:
    """
    登录中间件。
    对需要登录的域名自动附加 cookies 和 headers。
    检测到登录失效时自动重新登录。
    """

    LOGIN_REQUIRED_INDICATORS = [
        "请登录", "login required", "sign in", "登录后查看",
        "请先登录", "会员登录", "用户登录",
    ]

    def process_request(self, request, spider):
        domain = urlparse(request.url).netloc
        manager = get_login_manager()
        cookies = manager.get_cookies(domain)
        if cookies:
            request.cookies.update(cookies)
            headers = manager.get_headers(domain)
            for k, v in headers.items():
                request.headers[k] = v

    def process_response(self, request, response, spider):
        if not self._needs_login(response):
            return response

        domain = urlparse(request.url).netloc
        manager = get_login_manager()
        logger.info("Login required detected, re-login: %s", domain)
        manager.invalidate(domain)
        cookies = manager.get_cookies(domain)

        if cookies:
            new_request = request.copy()
            new_request.cookies.update(cookies)
            new_request.dont_filter = True
            return new_request

        logger.warning("Re-login failed for: %s", domain)
        return response

    def _needs_login(self, response) -> bool:
        if response.status in (401, 302, 303):
            location = response.headers.get("Location", b"").decode("utf-8", errors="ignore")
            if "login" in location.lower():
                return True

        if response.status == 200 and hasattr(response, "text"):
            text = response.text.lower()
            return any(indicator in text for indicator in self.LOGIN_REQUIRED_INDICATORS)

        return False


class ShangjiBaoSpiderMiddleware:

    @classmethod
    def from_crawler(cls, crawler):
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_spider_input(self, response, spider):
        return None

    def process_spider_output(self, response, result, spider):
        for i in result:
            yield i

    def process_spider_exception(self, response, exception, spider):
        pass

    def spider_opened(self, spider):
        spider.logger.info("Spider opened: %s", spider.name)
