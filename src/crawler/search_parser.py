"""
搜索引擎结果页面解析器。

从各搜索引擎的搜索结果 HTML 中提取结果链接。
"""
import re
from urllib.parse import urlparse, parse_qs, unquote


def extract_search_results(html: str, engine_domain: str) -> list[dict]:
    """
    从搜索引擎结果页中提取有效的结果链接。

    Args:
        html: 搜索结果页的 HTML
        engine_domain: 搜索引擎域名（如 www.baidu.com）

    Returns:
        [{"url": "...", "title": "..."}]
    """
    if "baidu.com" in engine_domain:
        return _parse_baidu(html)
    elif "bing.com" in engine_domain:
        return _parse_bing(html)
    elif "sogou.com" in engine_domain:
        return _parse_sogou(html)
    elif "so.com" in engine_domain:
        return _parse_360(html)
    else:
        return _parse_generic(html, engine_domain)


def _parse_baidu(html: str) -> list[dict]:
    """解析百度搜索结果。百度的 href 是跳转链接，需要从 data-url 或 mu 属性获取真实 URL。"""
    results = []

    for pattern in [
        re.compile(r'mu="(https?://[^"]+)"', re.IGNORECASE),
        re.compile(r'data-url="(https?://[^"]+)"', re.IGNORECASE),
    ]:
        for match in pattern.finditer(html):
            url = match.group(1)
            if _is_valid_result_url(url, "baidu.com"):
                results.append({"url": url, "title": ""})

    href_pattern = re.compile(
        r'<h3[^>]*>\s*<a[^>]+href="(https?://[^"]+)"[^>]*>(.*?)</a>',
        re.IGNORECASE | re.DOTALL,
    )
    for match in href_pattern.finditer(html):
        url = match.group(1)
        title = re.sub(r"<[^>]+>", "", match.group(2)).strip()
        if "baidu.com/link" in url:
            continue
        if _is_valid_result_url(url, "baidu.com"):
            results.append({"url": url, "title": title})

    return _dedupe_results(results)


def _parse_bing(html: str) -> list[dict]:
    """解析必应搜索结果。"""
    results = []
    pattern = re.compile(
        r'<li[^>]*class="[^"]*b_algo[^"]*"[^>]*>.*?<a[^>]+href="(https?://[^"]+)"[^>]*>(.*?)</a>',
        re.IGNORECASE | re.DOTALL,
    )
    for match in pattern.finditer(html):
        url = match.group(1)
        title = re.sub(r"<[^>]+>", "", match.group(2)).strip()
        if _is_valid_result_url(url, "bing.com"):
            results.append({"url": url, "title": title})

    if not results:
        results = _parse_generic(html, "bing.com")

    return _dedupe_results(results)


def _parse_sogou(html: str) -> list[dict]:
    """解析搜狗搜索结果。"""
    results = []
    pattern = re.compile(
        r'<h3[^>]*>.*?<a[^>]+href="(https?://[^"]+)"[^>]*>(.*?)</a>',
        re.IGNORECASE | re.DOTALL,
    )
    for match in pattern.finditer(html):
        url = match.group(1)
        title = re.sub(r"<[^>]+>", "", match.group(2)).strip()
        if _is_valid_result_url(url, "sogou.com"):
            results.append({"url": url, "title": title})

    if not results:
        results = _parse_generic(html, "sogou.com")

    return _dedupe_results(results)


def _parse_360(html: str) -> list[dict]:
    """解析 360 搜索结果。"""
    results = []
    pattern = re.compile(
        r'<h3[^>]*>.*?<a[^>]+href="(https?://[^"]+)"[^>]*>(.*?)</a>',
        re.IGNORECASE | re.DOTALL,
    )
    for match in pattern.finditer(html):
        url = match.group(1)
        title = re.sub(r"<[^>]+>", "", match.group(2)).strip()
        if _is_valid_result_url(url, "so.com"):
            results.append({"url": url, "title": title})

    if not results:
        results = _parse_generic(html, "so.com")

    return _dedupe_results(results)


def _parse_generic(html: str, engine_domain: str) -> list[dict]:
    """通用解析：提取所有 <a href> 中非搜索引擎自身域名的 http(s) 链接。"""
    results = []
    pattern = re.compile(r'<a[^>]+href="(https?://[^"]+)"', re.IGNORECASE)
    for match in pattern.finditer(html):
        url = match.group(1)
        if _is_valid_result_url(url, engine_domain):
            results.append({"url": url, "title": ""})
    return _dedupe_results(results)


SKIP_DOMAINS = {
    "baidu.com", "www.baidu.com", "baidustatic.com", "bdstatic.com", "bdimg.com",
    "bing.com", "www.bing.com", "cn.bing.com", "microsoft.com",
    "sogou.com", "www.sogou.com",
    "so.com", "www.so.com", "360.cn",
    "google.com", "googleapis.com",
}


def _is_valid_result_url(url: str, engine_domain: str) -> bool:
    """判断 URL 是否为有效的搜索结果（排除搜索引擎自身、广告等）。"""
    try:
        parsed = urlparse(url)
        domain = parsed.netloc.lower()

        if not domain:
            return False

        for skip in SKIP_DOMAINS:
            if domain == skip or domain.endswith("." + skip):
                return False

        if engine_domain and (domain == engine_domain or domain.endswith("." + engine_domain)):
            return False

        if parsed.scheme not in ("http", "https"):
            return False

        return True
    except Exception:
        return False


def _dedupe_results(results: list[dict]) -> list[dict]:
    seen = set()
    deduped = []
    for r in results:
        if r["url"] not in seen:
            seen.add(r["url"])
            deduped.append(r)
    return deduped
