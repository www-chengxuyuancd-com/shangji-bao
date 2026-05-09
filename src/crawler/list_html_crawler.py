"""
通用「列表 HTML」爬虫。

适配场景：
- 列表页是服务端渲染的 HTML（不是 JS 动态加载）
- 通过 URL 模板翻页（例如 /zbxx/17_{page}.html、/news/list.php?catid=4&page={page}）
- 通过 CSS/XPath 选择器提取每条记录的标题/详情链接/日期/地区/来源

去重: 复用 visited_urls 表（urlHash = md5(detail_url)）。
节流: 每条详情/列表请求后 sleep(1 / source.rateLimit)。

config 字段：
- list_url_template: 必填，列表页 URL 模板，可用占位符 {rubric}/{page}/{name}
- rubrics: 类目列表 [{"id":..,"name":..}]，至少一项；可省略变成单类目
- first_page: 起始页码（默认 1）
- max_pages: 每个类目最多翻多少页（默认 30）
- single_page_only: True 时只抓首页（用于 ceb / sczbcg 这类有反爬翻页的站）
- stop_on_all_visited: 列表整页都已抓过就提前结束（增量抓）
- fetch_detail: 是否拉详情页 HTML（默认 True；SPA 站可设 False，只存元数据）
- encoding: 强制网页解码字符集（None 时自动）
- extra_domains: 允许的额外域名（详情页可能跨子域）
- extra_headers: 额外请求头
- detail_url_template: 详情 URL 模板，{value} 来自 selectors.url_extract 的捕获组。
  例如 ceb 站列表 href 是 'javascript:urlOpen(uuid)'，配置：
    "selectors": {"url": ".//a/@href", "url_extract": "urlOpen\\('([^']+)'\\)"},
    "detail_url_template": "https://ctbpsp.com/#/bulletinDetail?uuid={value}"
- selectors:
    - items: 必填。每条记录的容器选择器（CSS 或以 / 开头的 XPath）
    - title / url / date / region: 取值表达式
    - url_extract: 可选正则，对 url 字段做二次提取
"""
from __future__ import annotations

import hashlib
import logging
import re
import time
from datetime import datetime, timezone
from typing import Any, Callable
from urllib.parse import urljoin, urlparse

import requests
from parsel import Selector

logger = logging.getLogger(__name__)

_DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
}


def _md5(s: str) -> str:
    return hashlib.md5(s.encode("utf-8")).hexdigest()


def _extract_domain(url: str) -> str:
    try:
        return urlparse(url).netloc
    except Exception:
        return ""


def _decode(resp, forced_encoding: str | None = None) -> str:
    raw = resp.content
    if forced_encoding:
        try:
            return raw.decode(forced_encoding)
        except (UnicodeDecodeError, LookupError):
            pass
    if resp.encoding and resp.encoding.lower().replace("-", "") not in ("iso88591", "latin1"):
        try:
            return raw.decode(resp.encoding)
        except (UnicodeDecodeError, LookupError):
            pass
    for enc in ("utf-8", "gbk", "gb18030", "big5"):
        try:
            return raw.decode(enc)
        except (UnicodeDecodeError, LookupError):
            continue
    return raw.decode("utf-8", errors="replace")


_DATE_FORMATS = (
    "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d",
    "%Y/%m/%d %H:%M:%S", "%Y/%m/%d", "%Y.%m.%d",
    "%m-%d", "%m/%d",
)


def _parse_date(s: str | None) -> datetime | None:
    if not s:
        return None
    s = s.strip().strip("[]()【】 ")
    for fmt in _DATE_FORMATS:
        try:
            dt = datetime.strptime(s, fmt)
            if fmt in ("%m-%d", "%m/%d"):
                dt = dt.replace(year=datetime.now().year)
            return dt
        except ValueError:
            continue
    return None


def _save_raw_page(collection, url: str, html: str, source_type: str,
                   title: str = "", search_query: str = "", source_name: str = ""):
    """按 url upsert 到 raw_pages（统一封装在 src.db.mongo.upsert_raw_page）。"""
    from src.db.mongo import upsert_raw_page
    upsert_raw_page(
        collection, url=url, html=html,
        source_type=source_type, title=title,
        search_query=search_query, source_name=source_name,
    )


def _select_one(sel: Selector, expr: str) -> str:
    """支持 CSS（含 ::text/::attr()）和 XPath（以 / 或 ./ 开头）。"""
    if not expr:
        return ""
    try:
        if expr.startswith(("/", "./", "(/", "(.")):
            v = sel.xpath(expr).get()
        else:
            v = sel.css(expr).get()
        return (v or "").strip()
    except Exception:
        return ""


def crawl_list_html(prisma, source, cfg: dict, raw_pages, tracker, job_id: int,
                    check_status: Callable[[Any, int], str]):
    """
    通用列表 HTML 爬虫主流程。

    分类（rubric）× 翻页（page）→ 列表 → 每条详情。
    """
    base = (source.baseUrl or "").rstrip("/")
    base_domain = _extract_domain(base)
    rate = source.rateLimit if source.rateLimit and source.rateLimit > 0 else 0.2
    delay = 1.0 / rate

    selectors = cfg.get("selectors") or {}
    items_sel = selectors.get("items")
    title_sel = selectors.get("title") or "a::attr(title)"
    url_sel = selectors.get("url") or "a::attr(href)"
    date_sel = selectors.get("date") or ""
    region_sel = selectors.get("region") or ""
    url_extract = selectors.get("url_extract")
    url_extract_re = re.compile(url_extract) if url_extract else None
    detail_url_template = cfg.get("detail_url_template")

    list_url_template = cfg.get("list_url_template") or ""
    rubrics = cfg.get("rubrics") or [{"id": "", "name": ""}]
    first_page = int(cfg.get("first_page", 1))
    max_pages = int(cfg.get("max_pages", 30))
    single_page_only = bool(cfg.get("single_page_only", False))
    stop_on_all_visited = bool(cfg.get("stop_on_all_visited", True))
    encoding = cfg.get("encoding")
    fetch_detail = bool(cfg.get("fetch_detail", True))
    extra_domains = cfg.get("extra_domains") or []
    extra_headers = cfg.get("extra_headers") or {}

    if not items_sel or not list_url_template:
        logger.warning("[%s] list_html 缺少 items/list_url_template，跳过", source.name)
        return

    session = requests.Session()
    session.headers.update(_DEFAULT_HEADERS)
    if extra_headers:
        session.headers.update(extra_headers)

    allowed_domains = {base_domain, *[d.strip() for d in extra_domains if d]}

    for rubric in rubrics:
        rubric_id = (rubric.get("id") if isinstance(rubric, dict) else "") or ""
        rubric_name = (rubric.get("name") if isinstance(rubric, dict) else "") or ""
        tracker.update(query=f"[{source.name}] {rubric_name or rubric_id or '默认'}")
        consecutive_empty = 0

        last_page = first_page + 1 if single_page_only else first_page + max_pages

        for page in range(first_page, last_page):
            if check_status(prisma, job_id) in ("cancelled", "failed"):
                session.close()
                return

            try:
                list_url = list_url_template.format(
                    rubric=rubric_id, page=page, name=rubric_name,
                )
            except KeyError as e:
                logger.warning("[%s] list_url_template 缺少键 %s", source.name, e)
                break

            list_url_abs = list_url if list_url.startswith("http") else urljoin(base + "/", list_url)

            try:
                resp = session.get(list_url_abs, timeout=20)
            except Exception as e:
                logger.warning("[%s] 列表页请求异常 %s: %s", source.name, list_url_abs, e)
                tracker.update(errors=1, pages=1)
                time.sleep(delay)
                break

            tracker.update(pages=1)
            if resp.status_code != 200:
                logger.warning("[%s] 列表页 HTTP %d: %s",
                               source.name, resp.status_code, list_url_abs)
                tracker.update(errors=1)
                break

            html = _decode(resp, encoding)
            sel = Selector(text=html)
            try:
                items = sel.css(items_sel) if not items_sel.startswith(("/", "./")) else sel.xpath(items_sel)
            except Exception as e:
                logger.warning("[%s] items 选择器解析失败: %s", source.name, e)
                break

            if not items:
                break

            new_count = 0
            for item in items:
                if check_status(prisma, job_id) in ("cancelled", "failed"):
                    session.close()
                    return

                href = _select_one(item, url_sel)
                if not href:
                    continue

                if url_extract_re:
                    m = url_extract_re.search(href)
                    if not m:
                        continue
                    captured = m.group(1) if m.groups() else m.group(0)
                    if detail_url_template:
                        detail_url = detail_url_template.format(value=captured)
                    else:
                        detail_url = captured
                else:
                    detail_url = href

                if not detail_url.startswith("http"):
                    detail_url = urljoin(list_url_abs, detail_url)
                parsed = urlparse(detail_url)
                if parsed.scheme not in ("http", "https"):
                    continue
                if parsed.netloc not in allowed_domains:
                    continue

                title = _select_one(item, title_sel)
                if not title:
                    title = (item.css("a::text").get() or "").strip()

                date_text = _select_one(item, date_sel) if date_sel else ""
                region_text = _select_one(item, region_sel) if region_sel else ""

                url_hash = _md5(detail_url)
                if prisma.visitedurl.find_unique(where={"urlHash": url_hash}):
                    continue

                ok = _store_one(
                    session=session,
                    prisma=prisma,
                    source=source,
                    detail_url=detail_url,
                    url_hash=url_hash,
                    title=title[:500] if title else detail_url[:500],
                    publish_date=_parse_date(date_text),
                    region_text=region_text,
                    rubric_name=rubric_name,
                    base_domain=parsed.netloc,
                    raw_pages=raw_pages,
                    tracker=tracker,
                    fetch_detail=fetch_detail,
                )
                if ok:
                    new_count += 1
                if fetch_detail:
                    time.sleep(delay)

            if single_page_only:
                break

            if stop_on_all_visited and new_count == 0:
                consecutive_empty += 1
                if consecutive_empty >= 2:
                    logger.info("[%s] %s 连续 2 页全已抓过，提前结束",
                                source.name, rubric_name)
                    break
            else:
                consecutive_empty = 0

            time.sleep(delay)

    session.close()


def _store_one(*, session, prisma, source, detail_url, url_hash, title,
               publish_date, region_text, rubric_name, base_domain,
               raw_pages, tracker, fetch_detail) -> bool:
    """抓详情 HTML（可选）+ 写 Mongo + 写 visited_urls + 写 search_results。"""
    html = ""
    status_code = 0

    if fetch_detail:
        try:
            resp = session.get(detail_url, timeout=20)
            status_code = resp.status_code
            if resp.status_code == 200:
                html = _decode(resp)
        except Exception as e:
            logger.debug("详情页抓取失败 %s: %s", detail_url, e)
            tracker.update(errors=1)

    try:
        if html and raw_pages is not None:
            _save_raw_page(
                raw_pages, detail_url, html,
                source_type="list_html",
                title=title,
                search_query=rubric_name,
                source_name=source.name,
            )
    except Exception as e:
        logger.warning("写入 raw_pages 失败 %s: %s", detail_url, e)

    try:
        prisma.visitedurl.upsert(
            where={"urlHash": url_hash},
            data={
                "create": {
                    "url": detail_url,
                    "urlHash": url_hash,
                    "searchQuery": rubric_name,
                    "status": status_code,
                },
                "update": {
                    "status": status_code,
                    "searchQuery": rubric_name,
                },
            },
        )
    except Exception as e:
        logger.debug("visitedurl upsert 失败 %s: %s", detail_url, e)

    try:
        prisma.searchresult.create(data={
            "title": title or detail_url[:500],
            "url": detail_url,
            "urlHash": url_hash,
            "domain": base_domain,
            "sourceType": "list_html",
            "sourceName": source.name,
            "searchQuery": rubric_name or region_text or None,
            "publishDate": publish_date,
        })
        tracker.update(results=1)
        return True
    except Exception as e:
        logger.debug("searchresult.create 失败 %s: %s", detail_url, e)
        return False
