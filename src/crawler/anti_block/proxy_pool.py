"""
IP 代理池模块。

支持两种模式:
1. 静态代理列表: 通过配置文件或环境变量提供代理列表
2. 代理 API: 通过第三方代理服务 API 动态获取代理

代理格式: scheme://[user:pass@]host:port
示例:
  - http://127.0.0.1:8080
  - http://user:pass@proxy.example.com:9090
  - socks5://127.0.0.1:1080
"""
import logging
import random
import threading
import time
from dataclasses import dataclass, field

import requests

logger = logging.getLogger(__name__)


@dataclass
class ProxyInfo:
    url: str
    fail_count: int = 0
    success_count: int = 0
    last_used: float = 0
    disabled: bool = False
    response_time: float = 0

    @property
    def score(self) -> float:
        total = self.success_count + self.fail_count
        if total == 0:
            return 50.0
        return (self.success_count / total) * 100

    def record_success(self, response_time: float = 0):
        self.success_count += 1
        self.fail_count = max(0, self.fail_count - 1)
        self.response_time = response_time
        self.disabled = False

    def record_failure(self):
        self.fail_count += 1
        if self.fail_count >= 5:
            self.disabled = True


class ProxyPool:
    """
    代理池管理器。

    用法:
        pool = ProxyPool()
        pool.add("http://1.2.3.4:8080")
        pool.add("http://5.6.7.8:9090")
        proxy = pool.get()          # 获取一个可用代理
        pool.feedback(proxy, True)   # 反馈使用结果
    """

    MAX_FAIL_COUNT = 5
    VALIDATION_URL = "https://httpbin.org/ip"
    VALIDATION_TIMEOUT = 10

    def __init__(self):
        self._proxies: dict[str, ProxyInfo] = {}
        self._lock = threading.Lock()
        self._api_url: str | None = None
        self._api_interval: int = 60
        self._last_api_fetch: float = 0

    def add(self, proxy_url: str):
        with self._lock:
            if proxy_url not in self._proxies:
                self._proxies[proxy_url] = ProxyInfo(url=proxy_url)
                logger.info("Proxy added: %s", proxy_url)

    def add_many(self, proxy_urls: list[str]):
        for url in proxy_urls:
            self.add(url.strip())

    def remove(self, proxy_url: str):
        with self._lock:
            self._proxies.pop(proxy_url, None)

    def set_api(self, api_url: str, fetch_interval: int = 60):
        """设置代理 API 地址，池会定时从该 API 拉取新代理。"""
        self._api_url = api_url
        self._api_interval = fetch_interval

    def _fetch_from_api(self):
        if not self._api_url:
            return
        now = time.time()
        if now - self._last_api_fetch < self._api_interval:
            return
        self._last_api_fetch = now
        try:
            resp = requests.get(self._api_url, timeout=10)
            resp.raise_for_status()
            data = resp.json() if resp.headers.get("content-type", "").startswith("application/json") else None

            if data and isinstance(data, list):
                proxies = data
            elif data and isinstance(data, dict) and "proxies" in data:
                proxies = data["proxies"]
            else:
                proxies = [line.strip() for line in resp.text.strip().splitlines() if line.strip()]

            for p in proxies:
                proxy_str = p if "://" in p else f"http://{p}"
                self.add(proxy_str)

            logger.info("Fetched %d proxies from API", len(proxies))
        except Exception as e:
            logger.warning("Failed to fetch proxies from API: %s", e)

    def get(self) -> str | None:
        """获取一个可用代理 URL，按评分加权随机选取。"""
        self._fetch_from_api()

        with self._lock:
            available = [p for p in self._proxies.values() if not p.disabled]
            if not available:
                logger.warning("No available proxies in pool")
                return None

            weights = [max(p.score, 1.0) for p in available]
            chosen = random.choices(available, weights=weights, k=1)[0]
            chosen.last_used = time.time()
            return chosen.url

    def feedback(self, proxy_url: str, success: bool, response_time: float = 0):
        """反馈代理使用结果，用于调整评分。"""
        with self._lock:
            info = self._proxies.get(proxy_url)
            if not info:
                return
            if success:
                info.record_success(response_time)
            else:
                info.record_failure()
                if info.disabled:
                    logger.info("Proxy disabled due to failures: %s", proxy_url)

    def validate(self, proxy_url: str) -> bool:
        """验证单个代理是否可用。"""
        try:
            resp = requests.get(
                self.VALIDATION_URL,
                proxies={"http": proxy_url, "https": proxy_url},
                timeout=self.VALIDATION_TIMEOUT,
            )
            return resp.status_code == 200
        except Exception:
            return False

    def validate_all(self):
        """验证池中所有代理，禁用不可用的。"""
        with self._lock:
            urls = list(self._proxies.keys())

        for url in urls:
            ok = self.validate(url)
            self.feedback(url, ok)
            if not ok:
                logger.info("Proxy validation failed: %s", url)

    @property
    def available_count(self) -> int:
        with self._lock:
            return sum(1 for p in self._proxies.values() if not p.disabled)

    @property
    def total_count(self) -> int:
        return len(self._proxies)

    def stats(self) -> list[dict]:
        with self._lock:
            return [
                {
                    "url": p.url,
                    "score": round(p.score, 1),
                    "success": p.success_count,
                    "fail": p.fail_count,
                    "disabled": p.disabled,
                }
                for p in self._proxies.values()
            ]


# 全局单例
_pool: ProxyPool | None = None


def get_proxy_pool() -> ProxyPool:
    global _pool
    if _pool is None:
        _pool = ProxyPool()
    return _pool
