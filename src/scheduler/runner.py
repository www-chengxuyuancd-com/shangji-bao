"""
爬虫任务执行器。

支持实时进度上报、暂停/继续、容器重启后自动恢复。
"""
import hashlib
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


def _decode_response(resp) -> str:
    raw = resp.content
    if resp.encoding and resp.encoding.lower().replace("-", "") not in ("iso88591", "latin1"):
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
        charset = m.group(1).strip()
        try:
            return raw.decode(charset)
        except (UnicodeDecodeError, LookupError):
            pass

    for enc in ("utf-8", "gbk", "gb2312", "gb18030", "big5"):
        try:
            return raw.decode(enc)
        except (UnicodeDecodeError, LookupError):
            continue

    return raw.decode("utf-8", errors="replace")


def _extract_domain(url: str) -> str:
    try:
        return urlparse(url).netloc
    except Exception:
        return ""


def _get_mongo_collection():
    uri = os.getenv("MONGO_URI", "mongodb://mongodb:mongodb@localhost:27017/shangjibao?authSource=admin")
    client = MongoClient(uri)
    db = client.get_default_database()
    return client, db["raw_pages"]


def _save_raw_page(collection, url, html, source_type, title="", search_query="", source_name=""):
    content_hash = hashlib.sha256(html.encode("utf-8")).hexdigest()
    collection.insert_one({
        "url": url,
        "html": html,
        "content_hash": content_hash,
        "crawled_at": datetime.now(timezone.utc),
        "search_query": search_query,
        "source_name": source_name,
        "meta": {"title": title, "source_type": source_type},
    })


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

        query_combos = []
        for source in sources:
            if source.sourceCategory == "search_engine" and source.searchUrlTemplate:
                region_list = regions if regions else [None]
                suffix_list = suffixes if suffixes else [None]
                for kw in keywords:
                    for region in region_list:
                        for suffix in suffix_list:
                            parts = [kw.keyword]
                            if region:
                                parts.append(_region_search_name(region))
                            if suffix:
                                parts.append(suffix.suffix)
                            query_str = " ".join(parts)
                            query_combos.append({
                                "source": source,
                                "query": query_str,
                                "region": region,
                                "max_pages": source.maxPages or 10,
                            })
            else:
                query_combos.append({
                    "source": source,
                    "query": f"[BFS] {source.name}",
                    "region": None,
                    "max_pages": 0,
                })

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
                    _crawl_website(prisma, source, raw_pages, tracker)
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

            resp = req.get(search_url, timeout=15, headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            })

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
    import requests as req

    result_url = sr["url"]
    result_title = sr.get("title", "") or result_url
    result_url_hash = hashlib.md5(result_url.encode("utf-8")).hexdigest()
    result_domain = _extract_domain(result_url)

    existing = prisma.searchresult.find_unique(where={"urlHash": result_url_hash})
    if existing:
        return

    page_title = result_title
    try:
        page_resp = req.get(result_url, timeout=15, headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        })
        if page_resp.status_code == 200:
            page_html = _decode_response(page_resp)
            if "<title>" in page_html.lower():
                ts = page_html.lower().find("<title>") + 7
                te = page_html.lower().find("</title>", ts)
                if te > ts:
                    page_title = page_html[ts:te].strip()[:500] or page_title

            if raw_pages is not None:
                _save_raw_page(raw_pages, result_url, page_html, "search_result", page_title, search_query=query_str, source_name=source.name)

            prisma.visitedurl.upsert(
                where={"urlHash": result_url_hash},
                data={
                    "create": {"url": result_url, "urlHash": result_url_hash, "searchQuery": query_str, "status": page_resp.status_code},
                    "update": {"status": page_resp.status_code, "searchQuery": query_str},
                },
            )

        sub_delay = 1.0 / source.rateLimit if source.rateLimit > 0 else 1.0
        time.sleep(sub_delay)
    except Exception as ex:
        logger.debug("Failed to fetch result page %s: %s", result_url, ex)

    try:
        prisma.searchresult.create(data={
            "title": page_title[:500],
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


def _crawl_website(prisma, source, raw_pages, tracker):
    from collections import deque

    max_depth = source.maxDepth or 5
    base_domain = _extract_domain(source.baseUrl)

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
            resp = req.get(url, timeout=15, headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            })

            prisma.visitedurl.create(data={
                "url": url, "urlHash": url_hash, "status": resp.status_code,
            })

            if resp.status_code == 200:
                html = _decode_response(resp)
                domain = _extract_domain(url)

                title = ""
                if "<title>" in html.lower():
                    start = html.lower().find("<title>") + 7
                    end = html.lower().find("</title>", start)
                    if end > start:
                        title = html[start:end].strip()[:500]

                if raw_pages is not None:
                    _save_raw_page(raw_pages, url, html, "website", title, source_name=source.name)

                result_exists = prisma.searchresult.find_unique(where={"urlHash": url_hash})
                if not result_exists:
                    prisma.searchresult.create(data={
                        "title": title or url,
                        "url": url,
                        "urlHash": url_hash,
                        "domain": domain,
                        "sourceName": source.name,
                    })
                    tracker.update(results=1)

                if depth < max_depth:
                    from src.crawler.link_extractor import extract_same_domain_links
                    links = extract_same_domain_links(html, url, base_domain)
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
