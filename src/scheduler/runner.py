"""
爬虫任务执行器。

支持实时进度上报、暂停/继续、容器重启后自动恢复。
"""
import hashlib
import json
import logging
import multiprocessing
import os
import re
import time
import threading
from datetime import datetime, timezone
from urllib.parse import urlparse

from prisma import Prisma
from pymongo import MongoClient

logger = logging.getLogger(__name__)

_CHARSET_RE = re.compile(
    r'''<meta[^>]+charset\s*=\s*["']?\s*([a-zA-Z0-9_-]+)''',
    re.IGNORECASE,
)

# 标题相关的多套提取规则，按优先级排序
_TITLE_TAG_RE = re.compile(r"<title[^>]*>(.*?)</title>", re.IGNORECASE | re.DOTALL)
_META_ARTICLE_TITLE_RE = re.compile(
    r'<meta\s+name\s*=\s*["\']ArticleTitle["\']\s+content\s*=\s*["\']([^"\']+)["\']',
    re.IGNORECASE,
)
_META_OG_TITLE_RE = re.compile(
    r'<meta\s+property\s*=\s*["\']og:title["\']\s+content\s*=\s*["\']([^"\']+)["\']',
    re.IGNORECASE,
)
_H1_RE = re.compile(r"<h1[^>]*>(.*?)</h1>", re.IGNORECASE | re.DOTALL)
_HTML_TAG_RE = re.compile(r"<[^>]+>")

# 反爬/空内容/错误页常见特征（命中则视为抓取失败，不写入空标题）
_ANTIBOT_PATTERNS = (
    "请完成安全验证", "请完成验证", "访问太频繁", "访问过于频繁",
    "请输入验证码", "verification required", "are you a human",
    "您的访问出错了", "您访问的页面不存在", "页面找不到",
    "稍后重试", "robot check", "请稍后再试",
)

# requests 默认对没有 charset 的响应回退到 ISO-8859-1，我们要把它当成"未知编码"
_LATIN_FALLBACKS = ("iso88591", "latin1", "latin-1")

# 抓取通用页面用的 headers：带 Referer/Accept-Encoding，对政府站更友好
_FETCH_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Upgrade-Insecure-Requests": "1",
}


def _build_request_headers(url: str) -> dict:
    """根据目标 URL 构造请求头，自动加上同站 Referer，提高被某些政府站接受的概率。"""
    headers = dict(_FETCH_HEADERS)
    try:
        parsed = urlparse(url)
        if parsed.scheme and parsed.netloc:
            headers["Referer"] = f"{parsed.scheme}://{parsed.netloc}/"
    except Exception:
        pass
    return headers


def _decode_response(resp) -> str:
    """智能解码 requests 响应。

    顺序：
      1. response.encoding（若不是 ISO-8859-1 这种 RFC fallback）
      2. <meta charset> 声明（覆盖 HTML4/HTML5 两种写法）
      3. apparent_encoding（chardet 检测）
      4. 常见中文编码暴力枚举
      5. utf-8 with errors=replace（兜底）
    """
    raw = resp.content
    if not raw:
        return ""

    if resp.encoding and resp.encoding.lower().replace("-", "") not in _LATIN_FALLBACKS:
        try:
            return raw.decode(resp.encoding)
        except (UnicodeDecodeError, LookupError):
            pass

    head = raw[:4096]
    try:
        head_str = head.decode("ascii", errors="ignore")
    except Exception:
        head_str = ""
    m = _CHARSET_RE.search(head_str)
    if m:
        charset = m.group(1).strip().lower().replace("-", "")
        if charset not in _LATIN_FALLBACKS:
            try:
                return raw.decode(m.group(1).strip())
            except (UnicodeDecodeError, LookupError):
                pass

    try:
        apparent = resp.apparent_encoding
        if apparent and apparent.lower().replace("-", "") not in _LATIN_FALLBACKS:
            return raw.decode(apparent)
    except (UnicodeDecodeError, LookupError, AttributeError):
        pass

    for enc in ("utf-8", "gbk", "gb18030", "gb2312", "big5"):
        try:
            return raw.decode(enc)
        except (UnicodeDecodeError, LookupError):
            continue

    return raw.decode("utf-8", errors="replace")


def _clean_title_text(s: str) -> str:
    """清洗标题：去 HTML 标签、合并空白、去常见后缀。"""
    if not s:
        return ""
    s = _HTML_TAG_RE.sub(" ", s)
    s = s.replace("\xa0", " ").replace("\u3000", " ")
    s = re.sub(r"\s+", " ", s).strip()
    # 去常见站点名后缀（"xxx - 中国政府采购网" -> "xxx"），但保留主体
    s = re.sub(
        r"\s*[-_|｜·»]\s*(?:中国政府采购网|政府采购网|政采网|招标公告网|中国招标网|百度搜索|百度).*$",
        "",
        s,
    )
    return s.strip()[:500]


def _extract_title_from_html(html: str, fallback: str = "") -> str:
    """从 HTML 中按多套规则提取标题，命中第一条即返回。"""
    if not html:
        return fallback

    m = _META_ARTICLE_TITLE_RE.search(html)
    if m:
        t = _clean_title_text(m.group(1))
        if t and len(t) >= 4:
            return t

    m = _META_OG_TITLE_RE.search(html)
    if m:
        t = _clean_title_text(m.group(1))
        if t and len(t) >= 4:
            return t

    m = _TITLE_TAG_RE.search(html)
    if m:
        t = _clean_title_text(m.group(1))
        if t and len(t) >= 4 and not _is_generic_title(t):
            return t

    m = _H1_RE.search(html)
    if m:
        t = _clean_title_text(m.group(1))
        if t and len(t) >= 4:
            return t

    return fallback


# 通用站点 title（命中则倾向用 ArticleTitle/H1 等更精确的字段）
_GENERIC_TITLE_PATTERNS = (
    "中国政府采购网", "政府采购信息网", "招标采购导航",
    "百度搜索", "bing", "google", "搜狗搜索", "360搜索",
    "首页", "网站首页", "index", "untitled",
)


def _is_generic_title(t: str) -> bool:
    if not t:
        return True
    low = t.lower().strip()
    if len(low) < 4:
        return True
    for p in _GENERIC_TITLE_PATTERNS:
        if low == p.lower() or low.endswith(p.lower()):
            return True
    return False


def _looks_like_antibot_page(html: str) -> bool:
    """识别反爬验证页 / 错误页。"""
    if not html:
        return True
    if len(html) < 1024:
        # 政府公告页正常都 >5KB，<1KB 多半是错误/拦截页
        return True
    snippet = html[:8192]
    for kw in _ANTIBOT_PATTERNS:
        if kw in snippet:
            return True
    return False


def _extract_domain(url: str) -> str:
    try:
        return urlparse(url).netloc
    except Exception:
        return ""


def _parse_source_config(cfg_str: str | None) -> dict:
    if not cfg_str:
        return {}
    try:
        v = json.loads(cfg_str)
        return v if isinstance(v, dict) else {}
    except Exception:
        return {}


def _get_mongo_collection():
    uri = os.getenv("MONGO_URI", "mongodb://mongodb:mongodb@localhost:27017/shangjibao?authSource=admin")
    client = MongoClient(uri)
    db = client.get_default_database()
    return client, db["raw_pages"]


def _save_raw_page(collection, url, html, source_type, title="", search_query="", source_name=""):
    """按 url upsert 到 raw_pages（统一封装在 src.db.mongo.upsert_raw_page）。"""
    from src.db.mongo import upsert_raw_page
    upsert_raw_page(
        collection, url=url, html=html,
        source_type=source_type, title=title,
        search_query=search_query, source_name=source_name,
    )


def fix_orphaned_jobs():
    """容器启动时调用，将孤儿 running/paused 任务标记为 interrupted。"""
    try:
        prisma = Prisma()
        prisma.connect()
        orphans = prisma.crawljob.find_many(
            where={"status": {"in": ["running", "paused"]}}
        )
        for j in orphans:
            prisma.crawljob.update(
                where={"id": j.id},
                data={
                    "status": "interrupted",
                    "currentQuery": f"进程中断（容器重启），可点击继续",
                },
            )
            logger.info("Marked orphaned job %d as interrupted", j.id)
        prisma.disconnect()
    except Exception as e:
        logger.warning("fix_orphaned_jobs failed: %s", e)


class ProgressTracker:
    """线程安全的进度追踪器，定期批量写入数据库。"""
    def __init__(self, prisma, job_id, flush_interval=2.0,
                 init_pages=0, init_queries=0, init_results=0, init_errors=0):
        self._prisma = prisma
        self._job_id = job_id
        self._lock = threading.Lock()
        self._done_pages = init_pages
        self._done_queries = init_queries
        self._result_count = init_results
        self._error_count = init_errors
        self._current_query = ""
        self._dirty = False
        self._flush_interval = flush_interval
        self._stop = False
        self._thread = threading.Thread(target=self._auto_flush, daemon=True)
        self._thread.start()

    def _auto_flush(self):
        while not self._stop:
            time.sleep(self._flush_interval)
            self.flush()

    def update(self, pages=0, queries=0, results=0, errors=0, query=None):
        with self._lock:
            self._done_pages += pages
            self._done_queries += queries
            self._result_count += results
            self._error_count += errors
            if query is not None:
                self._current_query = query
            self._dirty = True

    def flush(self):
        with self._lock:
            if not self._dirty:
                return
            data = {
                "donePages": self._done_pages,
                "doneQueries": self._done_queries,
                "resultCount": self._result_count,
                "errorCount": self._error_count,
                "currentQuery": self._current_query[:500] if self._current_query else None,
            }
            self._dirty = False
        try:
            self._prisma.crawljob.update(where={"id": self._job_id}, data=data)
        except Exception as e:
            logger.warning("Progress flush failed: %s", e)

    def stop(self):
        self._stop = True
        self.flush()

    @property
    def stats(self):
        with self._lock:
            return {
                "done_pages": self._done_pages,
                "done_queries": self._done_queries,
                "result_count": self._result_count,
                "error_count": self._error_count,
            }


def _check_job_status(prisma, job_id) -> str:
    """检查任务状态，支持暂停等待。返回当前状态。"""
    while True:
        job = prisma.crawljob.find_unique(where={"id": job_id})
        if not job:
            return "cancelled"
        if job.status == "paused":
            time.sleep(2)
            continue
        return job.status


def _run_crawl_job(job_id: int, skip_queries: int = 0):
    """在子进程中执行的爬虫任务主函数。skip_queries > 0 时跳过前 N 个查询组合（用于继续）。"""
    prisma = Prisma()
    prisma.connect()
    mongo_client, raw_pages = _get_mongo_collection()

    try:
        job = prisma.crawljob.find_unique(where={"id": job_id})
        init_pages = job.donePages if skip_queries > 0 else 0
        init_queries = skip_queries
        init_results = job.resultCount if skip_queries > 0 else 0
        init_errors = job.errorCount if skip_queries > 0 else 0

        prisma.crawljob.update(
            where={"id": job_id},
            data={"status": "running", "startedAt": job.startedAt or datetime.now(timezone.utc)},
        )

        sources = prisma.crawlsource.find_many(where={"enabled": True})
        keywords = prisma.searchkeyword.find_many(where={"enabled": True})
        regions = prisma.searchregion.find_many(where={"enabled": True})
        suffixes = prisma.searchsuffix.find_many(where={"enabled": True})

        all_regions = prisma.searchregion.find_many()
        region_by_id = {r.id: r for r in all_regions}
        SUB_LEVELS = {"street", "town", "village", "community"}

        def _region_search_name(region):
            """村/镇/社区/街道级别自动向上拼接到区/县"""
            if region.level not in SUB_LEVELS:
                return region.name
            parts = [region.name]
            cur = region
            while cur.parentId:
                parent = region_by_id.get(cur.parentId)
                if not parent:
                    break
                parts.insert(0, parent.name)
                if parent.level == "district":
                    break
                cur = parent
            return " ".join(parts)

        # 调度策略：
        #   1. 把 source 分成两批：网站类（定向爬虫，密度高）和搜索引擎类（辅助发现，量大且重复多）
        #   2. 先全部跑网站类，再跑搜索引擎类
        #      这样即使任务被中断/客户重启，至少网站类结果是齐的
        #   3. 给每个 search_engine source 的 query 数加上限（默认 200），
        #      避免一个 source 把 keyword × region × suffix 展开成上千个 query 独占资源
        SE_MAX_QUERIES_PER_SOURCE = int(os.getenv("SE_MAX_QUERIES_PER_SOURCE", "200"))

        site_sources = [s for s in sources if s.sourceCategory != "search_engine"]
        se_sources = [s for s in sources if s.sourceCategory == "search_engine" and s.searchUrlTemplate]

        query_combos = []

        # ---- 网站类：每个 source 一个 combo（内部走 BFS / API / list_html 自有翻页） ----
        for source in site_sources:
            query_combos.append({
                "source": source,
                "query": f"[BFS] {source.name}",
                "region": None,
                "max_pages": 0,
            })

        # ---- 搜索引擎类：keyword × region × suffix 笛卡尔积，但每个 source 限量 ----
        for source in se_sources:
            region_list = regions if regions else [None]
            suffix_list = suffixes if suffixes else [None]
            se_combos_for_this_source: list[dict] = []
            for kw in keywords:
                for region in region_list:
                    for suffix in suffix_list:
                        parts = [kw.keyword]
                        if region:
                            parts.append(_region_search_name(region))
                        if suffix:
                            parts.append(suffix.suffix)
                        query_str = " ".join(parts)
                        se_combos_for_this_source.append({
                            "source": source,
                            "query": query_str,
                            "region": region,
                            "max_pages": source.maxPages or 10,
                        })
            if SE_MAX_QUERIES_PER_SOURCE > 0 and len(se_combos_for_this_source) > SE_MAX_QUERIES_PER_SOURCE:
                logger.info(
                    "[%s] 展开 %d 个 query，超过单 source 上限 %d，截断",
                    source.name, len(se_combos_for_this_source), SE_MAX_QUERIES_PER_SOURCE,
                )
                se_combos_for_this_source = se_combos_for_this_source[:SE_MAX_QUERIES_PER_SOURCE]
            query_combos.extend(se_combos_for_this_source)

        logger.info(
            "[crawl-job %d] 共 %d 个 query_combo: 网站类 %d 个（先跑），搜索引擎类 %d 个（后跑，单 source 上限 %d）",
            job_id, len(query_combos), len(site_sources),
            len(query_combos) - len(site_sources), SE_MAX_QUERIES_PER_SOURCE,
        )

        total_pages = sum(
            c["max_pages"] if c["max_pages"] > 0 else 50
            for c in query_combos
        )

        prisma.crawljob.update(
            where={"id": job_id},
            data={
                "totalSources": len(sources),
                "totalQueries": len(query_combos),
                "totalPages": total_pages,
                "currentQuery": f"{'继续执行，跳过前 ' + str(skip_queries) + ' 个查询...' if skip_queries > 0 else '已生成 ' + str(len(query_combos)) + ' 个查询组合，开始抓取...'}",
            },
        )

        tracker = ProgressTracker(
            prisma, job_id, flush_interval=1.5,
            init_pages=init_pages, init_queries=init_queries,
            init_results=init_results, init_errors=init_errors,
        )
        error_logs = []
        done_sources = set()

        for idx, combo in enumerate(query_combos):
            if idx < skip_queries:
                done_sources.add(combo["source"].id)
                continue

            status = _check_job_status(prisma, job_id)
            if status in ("cancelled", "failed"):
                break

            source = combo["source"]
            query = combo["query"]
            tracker.update(query=f"[{source.name}] {query}")

            try:
                if source.sourceCategory == "search_engine":
                    _crawl_one_query(
                        prisma, source, query, combo["region"],
                        combo["max_pages"], raw_pages, tracker, job_id,
                    )
                else:
                    cfg = _parse_source_config(source.config)
                    cfg_type = cfg.get("type")
                    if cfg_type == "gov_api_sc_ggzy":
                        from src.crawler.gov_api_crawler import crawl_sc_ggzy
                        crawl_sc_ggzy(
                            prisma, source, cfg, raw_pages, tracker,
                            job_id, _check_job_status,
                        )
                    elif cfg_type == "ccgp_sichuan":
                        from src.crawler.ccgp_sichuan_crawler import crawl_ccgp_sichuan
                        crawl_ccgp_sichuan(
                            prisma, source, cfg, raw_pages, tracker,
                            job_id, _check_job_status,
                        )
                    elif cfg_type == "list_html":
                        from src.crawler.list_html_crawler import crawl_list_html
                        crawl_list_html(
                            prisma, source, cfg, raw_pages, tracker,
                            job_id, _check_job_status,
                        )
                    else:
                        _crawl_website(
                            prisma, source, raw_pages, tracker,
                            extra_domains=cfg.get("extra_domains"),
                        )
            except Exception as e:
                msg = f"[{source.name}] {query}: {e}"
                logger.error(msg)
                error_logs.append(msg)
                tracker.update(errors=1)

            tracker.update(queries=1)
            done_sources.add(source.id)

            prisma.crawljob.update(
                where={"id": job_id},
                data={"doneSources": len(done_sources)},
            )

        tracker.stop()
        stats = tracker.stats

        final_status = _check_job_status(prisma, job_id)
        if final_status in ("cancelled",):
            return

        prisma.crawljob.update(
            where={"id": job_id},
            data={
                "status": "completed",
                "finishedAt": datetime.now(timezone.utc),
                "totalPages": stats["done_pages"] if stats["done_pages"] > 0 else total_pages,
                "donePages": stats["done_pages"],
                "doneQueries": stats["done_queries"],
                "resultCount": stats["result_count"],
                "errorCount": stats["error_count"],
                "currentQuery": None,
                "errorLog": "\n".join(error_logs) if error_logs else None,
            },
        )

        if job.triggerType == "scheduled":
            _auto_pipeline_after_crawl(prisma, job.scheduleId)

    except Exception as e:
        logger.error("Crawl job %d failed: %s", job_id, e)
        prisma.crawljob.update(
            where={"id": job_id},
            data={
                "status": "failed",
                "finishedAt": datetime.now(timezone.utc),
                "errorLog": str(e),
            },
        )
    finally:
        mongo_client.close()
        prisma.disconnect()


def _crawl_one_query(prisma, source, query_str, region, max_pages, raw_pages, tracker, job_id):
    import requests as req
    from src.crawler.search_parser import extract_search_results

    engine_domain = _extract_domain(source.baseUrl)

    for page in range(1, max_pages + 1):
        status = _check_job_status(prisma, job_id)
        if status in ("cancelled", "failed"):
            return

        try:
            search_url = source.searchUrlTemplate.format(
                keyword=query_str, region="", page=page,
            )
            search_url_hash = hashlib.md5(search_url.encode("utf-8")).hexdigest()

            resp = req.get(
                search_url, timeout=15,
                headers=_build_request_headers(search_url),
                allow_redirects=True,
            )

            prisma.visitedurl.upsert(
                where={"urlHash": search_url_hash},
                data={
                    "create": {"url": search_url, "urlHash": search_url_hash, "searchQuery": query_str, "status": resp.status_code},
                    "update": {"status": resp.status_code, "searchQuery": query_str},
                },
            )

            if resp.status_code == 200:
                resp_html = _decode_response(resp)
                if raw_pages is not None:
                    _save_raw_page(raw_pages, search_url, resp_html, "search_engine", search_query=query_str, source_name=source.name)

                search_results = extract_search_results(resp_html, engine_domain)

                for sr in search_results:
                    _fetch_and_store_result(
                        prisma, sr, source, query_str, region, raw_pages, tracker,
                    )

            delay = 1.0 / source.rateLimit if source.rateLimit > 0 else 1.0
            time.sleep(delay)

        except Exception as e:
            tracker.update(errors=1)
            logger.warning("Search page error [%s]: %s", source.name, e)

        tracker.update(pages=1)


def _fetch_and_store_result(prisma, sr, source, query_str, region, raw_pages, tracker):
    """抓单个搜索结果的详情页并入库。

    标题选择优先级：
      ArticleTitle meta > og:title > <title> > <h1> > 搜索引擎给的 title > URL
    若识别为反爬/错误页，记 warning 并不创建 SearchResult。
    抓取失败带一次轻量重试（去掉部分可疑请求头）。
    """
    import requests as req

    result_url = sr["url"]
    serp_title = (sr.get("title") or "").strip()
    result_url_hash = hashlib.md5(result_url.encode("utf-8")).hexdigest()
    result_domain = _extract_domain(result_url)

    existing = prisma.searchresult.find_unique(where={"urlHash": result_url_hash})
    if existing:
        return

    page_html = None
    status_code = None
    fetch_err: str | None = None
    for attempt in range(2):
        try:
            page_resp = req.get(
                result_url,
                timeout=15,
                headers=_build_request_headers(result_url),
                allow_redirects=True,
            )
            status_code = page_resp.status_code
            if page_resp.status_code == 200:
                page_html = _decode_response(page_resp)
                break
            else:
                fetch_err = f"HTTP {page_resp.status_code}"
        except Exception as ex:
            fetch_err = str(ex)
        time.sleep(0.5)

    if page_html is None:
        logger.warning(
            "fetch detail failed [%s] url=%s err=%s",
            source.name, result_url[:120], fetch_err,
        )
    elif _looks_like_antibot_page(page_html):
        logger.warning(
            "anti-bot / error page detected [%s] url=%s size=%d",
            source.name, result_url[:120], len(page_html),
        )
        # 标记一下 visited，避免下次还重复抓
        try:
            prisma.visitedurl.upsert(
                where={"urlHash": result_url_hash},
                data={
                    "create": {
                        "url": result_url, "urlHash": result_url_hash,
                        "searchQuery": query_str,
                        "status": status_code or 599,
                    },
                    "update": {
                        "status": status_code or 599, "searchQuery": query_str,
                    },
                },
            )
        except Exception:
            pass
        return

    # 选标题：优先页面正文里的 ArticleTitle/og:title/<title>/h1，再退回 serp_title
    page_title = ""
    if page_html:
        page_title = _extract_title_from_html(page_html, fallback="")

    if not page_title or _is_generic_title(page_title):
        page_title = serp_title or page_title

    if not page_title:
        # 实在没标题，用 URL 兜底但记录 warning，避免静默存空标题
        logger.warning(
            "no title extractable [%s] url=%s (serp_title='%s')",
            source.name, result_url[:120], serp_title[:80],
        )
        page_title = result_url

    page_title = page_title[:500]

    if page_html and raw_pages is not None:
        try:
            _save_raw_page(
                raw_pages, result_url, page_html, "search_result",
                page_title, search_query=query_str, source_name=source.name,
            )
        except Exception as ex:
            logger.debug("save raw_page failed for %s: %s", result_url[:80], ex)

    if status_code:
        try:
            prisma.visitedurl.upsert(
                where={"urlHash": result_url_hash},
                data={
                    "create": {
                        "url": result_url, "urlHash": result_url_hash,
                        "searchQuery": query_str, "status": status_code,
                    },
                    "update": {
                        "status": status_code, "searchQuery": query_str,
                    },
                },
            )
        except Exception:
            pass

    sub_delay = 1.0 / source.rateLimit if source.rateLimit > 0 else 1.0
    time.sleep(sub_delay)

    try:
        prisma.searchresult.create(data={
            "title": page_title,
            "url": result_url,
            "urlHash": result_url_hash,
            "domain": result_domain,
            "sourceName": source.name,
            "searchQuery": query_str,
            "regionId": region.id if region else None,
        })
        tracker.update(results=1)
    except Exception:
        pass


def _crawl_website(prisma, source, raw_pages, tracker, extra_domains=None):
    from collections import deque

    max_depth = source.maxDepth or 5
    base_domain = _extract_domain(source.baseUrl)
    extra_domains = list(extra_domains or [])

    visited = set()
    queue = deque()
    queue.append((source.baseUrl, 0))

    while queue:
        url, depth = queue.popleft()
        if depth > max_depth:
            continue

        url_hash = hashlib.md5(url.encode("utf-8")).hexdigest()
        if url_hash in visited:
            continue
        visited.add(url_hash)

        existing = prisma.visitedurl.find_unique(where={"urlHash": url_hash})
        if existing:
            tracker.update(pages=1)
            continue

        try:
            import requests as req
            resp = req.get(
                url, timeout=15,
                headers=_build_request_headers(url),
                allow_redirects=True,
            )

            prisma.visitedurl.create(data={
                "url": url, "urlHash": url_hash, "status": resp.status_code,
            })

            if resp.status_code == 200:
                html = _decode_response(resp)
                domain = _extract_domain(url)

                if _looks_like_antibot_page(html):
                    logger.warning(
                        "anti-bot / error page detected [%s] url=%s size=%d",
                        source.name, url[:120], len(html or ""),
                    )
                    delay = 1.0 / source.rateLimit if source.rateLimit > 0 else 1.0
                    time.sleep(delay)
                    tracker.update(pages=1)
                    continue

                title = _extract_title_from_html(html, fallback="")

                if raw_pages is not None:
                    _save_raw_page(raw_pages, url, html, "website", title, source_name=source.name)

                result_exists = prisma.searchresult.find_unique(where={"urlHash": url_hash})
                if not result_exists:
                    prisma.searchresult.create(data={
                        "title": (title or url)[:500],
                        "url": url,
                        "urlHash": url_hash,
                        "domain": domain,
                        "sourceName": source.name,
                    })
                    tracker.update(results=1)

                if depth < max_depth:
                    from src.crawler.link_extractor import extract_same_domain_links
                    links = extract_same_domain_links(
                        html, url, base_domain, extra_domains=extra_domains,
                    )
                    for link in links:
                        link_hash = hashlib.md5(link.encode("utf-8")).hexdigest()
                        if link_hash not in visited:
                            queue.append((link, depth + 1))

            delay = 1.0 / source.rateLimit if source.rateLimit > 0 else 1.0
            time.sleep(delay)

        except Exception as e:
            tracker.update(errors=1)
            logger.warning("Website crawl error [%s]: %s", url, e)

        tracker.update(pages=1)


def start_crawl_job(trigger_type: str = "manual", schedule_id: int | None = None) -> int:
    prisma = Prisma()
    prisma.connect()
    data = {"status": "pending", "triggerType": trigger_type}
    if schedule_id is not None:
        data["scheduleId"] = schedule_id
    job = prisma.crawljob.create(data=data)
    prisma.disconnect()

    process = multiprocessing.Process(target=_run_crawl_job, args=(job.id,), daemon=True)
    process.start()

    return int(job.id)


def resume_crawl_job(job_id: int) -> int:
    """从中断/暂停处继续执行抓取任务。"""
    prisma = Prisma()
    prisma.connect()
    job = prisma.crawljob.find_unique(where={"id": job_id})
    if not job or job.status not in ("interrupted", "paused", "failed"):
        prisma.disconnect()
        raise ValueError(f"任务 {job_id} 不可继续（状态: {job.status if job else '不存在'}）")

    skip = job.doneQueries or 0
    prisma.disconnect()

    process = multiprocessing.Process(target=_run_crawl_job, args=(job_id, skip), daemon=True)
    process.start()

    return job_id


def _auto_pipeline_after_crawl(prisma, schedule_id=None):
    """爬虫完成后的自动化流水线：解析 → 发送通知。根据调度配置决定。"""
    schedule = None
    if schedule_id:
        schedule = prisma.crawlschedule.find_unique(where={"id": schedule_id})
    if not schedule:
        schedule = prisma.crawlschedule.find_first(where={"enabled": True})
    if not schedule:
        logger.info("No schedule config found, skipping auto-pipeline")
        return

    auto_parse = getattr(schedule, "autoParse", True)
    auto_notify = getattr(schedule, "autoNotify", True)

    if not auto_parse:
        logger.info("Auto-parse disabled for schedule [%s], skipping pipeline", schedule.name)
        return

    logger.info("Auto-pipeline: starting parse after crawl (schedule=%s)", schedule.name)
    try:
        parse_job = prisma.crawljob.create(data={
            "status": "pending",
            "triggerType": "auto_parse",
        })
        _run_parse_in_process(parse_job.id, auto_notify)
    except Exception as e:
        logger.error("Auto-pipeline parse failed: %s", e)


def _run_parse_in_process(parse_job_id: int, auto_notify: bool):
    """在新进程中执行解析，完成后根据配置自动发送通知。"""
    # 注意：爬虫任务自身运行在 multiprocessing 子进程内。
    # 如果当前进程是 daemon，则 Python 不允许再创建子进程，
    # 否则会抛出 "daemonic processes are not allowed to have children"。
    # 这会导致 auto_parse 任务一直停留在 pending。
    if multiprocessing.current_process().daemon:
        logger.info("Current process is daemon, run auto-parse inline (job_id=%s)", parse_job_id)
        _auto_parse_and_notify(parse_job_id, auto_notify)
        return

    process = multiprocessing.Process(
        target=_auto_parse_and_notify,
        args=(parse_job_id, auto_notify),
        daemon=True,
    )
    process.start()


def _auto_parse_and_notify(parse_job_id: int, auto_notify: bool):
    """解析 + 通知的完整流水线（在子进程中执行）。"""
    from src.parser.engine import _run_parse_job

    _run_parse_job(parse_job_id)

    if not auto_notify:
        logger.info("Auto-notify disabled, skipping send")
        return

    logger.info("Auto-pipeline: sending notifications after parse")
    try:
        from src.notify.engine import send_notifications
        result = send_notifications()
        logger.info(
            "Auto-notify done: sent=%d, prepared=%d, skipped=%d, failed=%d",
            result.get("sent", 0), result.get("prepared", 0),
            result.get("skipped", 0), result.get("failed", 0),
        )
    except Exception as e:
        logger.error("Auto-pipeline notify failed: %s", e)
