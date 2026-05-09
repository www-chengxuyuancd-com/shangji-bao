"""
四川省公共资源交易信息网（ggzyjy.sc.gov.cn）专用爬虫。

该站列表页是 JS 动态渲染，靠后端 ES 接口 getFullTextDataNew 拉数据。
直接调接口比 BFS HTML 高效得多，且能拿到完整元数据（标题/发布时间/来源/地区）。

去重: 复用 visited_urls 表（urlHash = md5(detail_url)），任何一条详情 URL
只要在表里出现过，就跳过，不再二次抓取。
"""
from __future__ import annotations

import hashlib
import logging
import re
import time
from datetime import datetime
from typing import Any, Callable
from urllib.parse import urlparse

import requests

logger = logging.getLogger(__name__)

# 静态 HTML 详情页可能不带 charset 响应头，requests 默认会把 encoding
# 填成 ISO-8859-1，导致中文乱码、解析时被 is_valid_content 判为"无中文"。
# 这里复用一个智能解码：忽略 ISO-8859-1，优先看 <meta charset>，再退化为常见中文编码。
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
    try:
        head_str = raw[:4096].decode("ascii", errors="ignore")
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

_DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    "Content-Type": "application/json;charset=UTF-8",
    "Origin": "https://ggzyjy.sc.gov.cn",
    "Referer": "https://ggzyjy.sc.gov.cn/jyxx/transactionInfo.html",
}

_DETAIL_HEADERS = {
    "User-Agent": _DEFAULT_HEADERS["User-Agent"],
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": _DEFAULT_HEADERS["Accept-Language"],
}


def _build_payload(category_num: str, pn: int, rn: int,
                   start_time: str = "", end_time: str = "") -> dict[str, Any]:
    """构造 getFullTextDataNew 的请求体（参考站点 transactionInfo.js 抓包）。"""
    return {
        "token": "",
        "pn": pn,
        "rn": rn,
        "sdt": "",
        "edt": "",
        "wd": "",
        "inc_wd": "",
        "exc_wd": "",
        "fields": "title;linkurl",
        "cnum": "001",
        "sort": '{"webdate":"0"}',
        "ssort": "title",
        "cl": 200,
        "terminal": "",
        "condition": [
            {
                "fieldName": "categorynum",
                "equal": category_num,
                "notEqual": None,
                "equalList": None,
                "notEqualList": None,
                "isLike": True,
                "likeType": 2,
            }
        ],
        "time": [{"fieldName": "webdate", "startTime": start_time, "endTime": end_time}],
        "highlights": "title",
        "statistics": None,
        "unionCondition": None,
        "accuracy": "",
        "noParticiple": "0",
        "searchRange": None,
        "isBusiness": 1,
    }


def _month_windows(start: str, end: str) -> list[tuple[str, str]]:
    """
    生成 [start, end] 之间的逐月时间窗口，闭区间字符串形式 'YYYY-MM-DD'。

    例: ('2024-01', '2024-03') -> [('2024-01-01','2024-01-31'),
                                   ('2024-02-01','2024-02-29'),
                                   ('2024-03-01','2024-03-31')]
    输入支持 'YYYY-MM' 或 'YYYY-MM-DD'。最近月份在前，按 webdate 降序回填体验更直观。
    """
    def _norm(s: str) -> tuple[int, int]:
        s = s.strip()
        parts = s.split("-")
        return int(parts[0]), int(parts[1])

    sy, sm = _norm(start)
    ey, em = _norm(end)
    out: list[tuple[str, str]] = []
    y, m = ey, em
    while (y, m) >= (sy, sm):
        from calendar import monthrange
        last = monthrange(y, m)[1]
        out.append((f"{y:04d}-{m:02d}-01", f"{y:04d}-{m:02d}-{last:02d}"))
        m -= 1
        if m == 0:
            y -= 1
            m = 12
    return out


def _md5(s: str) -> str:
    return hashlib.md5(s.encode("utf-8")).hexdigest()


def _extract_domain(url: str) -> str:
    try:
        return urlparse(url).netloc
    except Exception:
        return ""


def _parse_webdate(s: str | None) -> datetime | None:
    if not s:
        return None
    s = s.strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%Y/%m/%d %H:%M:%S", "%Y/%m/%d"):
        try:
            return datetime.strptime(s, fmt)
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


def crawl_sc_ggzy(prisma, source, cfg: dict, raw_pages, tracker, job_id: int,
                  check_status: Callable[[Any, int], str]):
    """
    抓取四川省公共资源交易信息网。

    Args:
        prisma: 已连接的 Prisma 客户端
        source: CrawlSource 记录
        cfg: 解析后的 source.config（dict）
        raw_pages: pymongo collection（raw_pages），可能为 None
        tracker: ProgressTracker
        job_id: 当前 CrawlJob id
        check_status: 函数 (prisma, job_id) -> status，用于响应暂停/取消
    """
    base = (source.baseUrl or "https://ggzyjy.sc.gov.cn").rstrip("/")
    endpoint_path = cfg.get("endpoint", "/inteligentsearch/rest/esinteligentsearch/getFullTextDataNew")
    endpoint = base + endpoint_path
    page_size = int(cfg.get("page_size", 20))
    max_pages_per_cat = int(cfg.get("max_pages_per_category", 100))
    stop_on_all_visited = bool(cfg.get("stop_on_all_visited", True))
    categories = cfg.get("categories") or []
    rate = source.rateLimit if source.rateLimit and source.rateLimit > 0 else 0.2
    delay = 1.0 / rate

    # 历史回填配置（可选）：
    #   time_partition: "month"   开启按月分片，否则按原行为（不带时间窗口）
    #   start_date:     "2018-01" 历史回填起点（含），仅当 time_partition 时生效
    #   end_date:       "2026-05" 历史回填终点（含），缺省=今天月份
    time_partition = (cfg.get("time_partition") or "").lower()
    start_date = cfg.get("start_date") or ""
    end_date = cfg.get("end_date") or datetime.now().strftime("%Y-%m")

    if time_partition == "month" and start_date:
        windows = _month_windows(start_date, end_date)
        logger.info("[%s] 启用按月分片回填：%s ~ %s 共 %d 个月分片",
                    source.name, start_date, end_date, len(windows))
    else:
        windows = [("", "")]  # 单段、不带时间过滤，保持旧行为
        logger.info("[%s] 单段抓取（不分片，按 webdate 倒序）", source.name)

    logger.info(
        "[%s] 配置: categories=%d, page_size=%d, max_pages_per_category=%d, "
        "rate=%.2f req/s (delay=%.2fs), stop_on_all_visited=%s, endpoint=%s",
        source.name, len(categories), page_size, max_pages_per_cat,
        rate, delay, stop_on_all_visited, endpoint,
    )

    base_domain = _extract_domain(base)

    if not categories:
        logger.warning("[%s] 未配置 categories，跳过", source.name)
        return

    session = requests.Session()
    session.headers.update(_DEFAULT_HEADERS)

    # 全局累计统计 + 节流式心跳日志
    stats = {
        "api_pages": 0, "api_errors": 0,
        "details_ok": 0, "details_fail": 0,
        "skipped_visited": 0, "new_records": 0,
        "force_done": 0,
    }
    job_t0 = time.time()
    last_heartbeat = time.time()

    def _heartbeat(prefix: str):
        nonlocal last_heartbeat
        now = time.time()
        if now - last_heartbeat < 30:  # 至少 30 秒一次心跳
            return
        elapsed = now - job_t0
        rate_d = stats["details_ok"] / elapsed if elapsed > 0 else 0
        logger.info(
            "[%s] %s 心跳 累计: api页=%d (err=%d) | 详情 ok=%d fail=%d | "
            "新增=%d 跳过已抓=%d | 已运行=%.0fs 速度=%.2f详情/s",
            source.name, prefix,
            stats["api_pages"], stats["api_errors"],
            stats["details_ok"], stats["details_fail"],
            stats["new_records"], stats["skipped_visited"],
            elapsed, rate_d,
        )
        last_heartbeat = now

    # ---- 0) force_urls：直接拉详情，不依赖列表分页 ----
    force_urls: list[str] = list(cfg.get("force_urls") or [])
    if force_urls:
        logger.info("[%s] force_urls 强制抓取列表: %d 条", source.name, len(force_urls))
        for fu in force_urls:
            if check_status(prisma, job_id) in ("cancelled", "failed"):
                session.close()
                return
            full_url = fu.strip()
            if not full_url:
                continue
            url_hash = _md5(full_url)
            logger.info("[%s] force_url -> %s", source.name, full_url)
            ok = _fetch_detail_and_store(
                session=session, prisma=prisma, source=source,
                full_url=full_url, url_hash=url_hash,
                title="", publish_date=None, cat_name="force_url",
                zhuanzai=None, base_domain=base_domain,
                raw_pages=raw_pages, tracker=tracker, source_label="force_url",
                upsert_search_result=True,
            )
            if ok:
                stats["force_done"] += 1
                stats["details_ok"] += 1
                stats["new_records"] += 1
            else:
                stats["details_fail"] += 1
            time.sleep(delay)

    for cat in categories:
        cat_num = cat.get("num") if isinstance(cat, dict) else None
        cat_name = (cat.get("name") if isinstance(cat, dict) else "") or ""
        if not cat_num:
            continue

        logger.info("[%s] >>>>> 开始分类 %s (%s)", source.name, cat_name, cat_num)

        for win_idx, (win_start, win_end) in enumerate(windows, 1):
            label = f"{win_start[:7]}" if win_start else "全部时间"
            tracker.update(query=f"[{source.name}] {cat_name}({cat_num}) {label}")
            consecutive_empty_pages = 0
            seg_t0 = time.time()
            seg_new = 0
            seg_skip = 0

            logger.info("[%s] %s %s 进入分片 (%d/%d)",
                        source.name, cat_name, label, win_idx, len(windows))

            for page_idx in range(max_pages_per_cat):
                if check_status(prisma, job_id) in ("cancelled", "failed"):
                    logger.info("[%s] 收到取消信号，提前退出", source.name)
                    session.close()
                    return

                pn = page_idx * page_size
                payload = _build_payload(cat_num, pn, page_size, win_start, win_end)

                t_api = time.time()
                try:
                    resp = session.post(endpoint, json=payload, timeout=20)
                except Exception as e:
                    logger.warning("[%s] %s %s 第%d页接口请求异常: %s",
                                   source.name, cat_name, label, page_idx + 1, e)
                    tracker.update(errors=1, pages=1)
                    stats["api_errors"] += 1
                    stats["api_pages"] += 1
                    time.sleep(delay)
                    break

                api_dt = time.time() - t_api
                tracker.update(pages=1)
                stats["api_pages"] += 1

                if resp.status_code != 200:
                    logger.warning("[%s] %s %s 第%d页 HTTP %d (耗时%.2fs)",
                                   source.name, cat_name, label, page_idx + 1,
                                   resp.status_code, api_dt)
                    tracker.update(errors=1)
                    stats["api_errors"] += 1
                    break

                try:
                    data = resp.json()
                except Exception as e:
                    logger.warning("[%s] %s %s 第%d页响应非 JSON: %s",
                                   source.name, cat_name, label, page_idx + 1, e)
                    tracker.update(errors=1)
                    stats["api_errors"] += 1
                    break

                result = data.get("result") or {}
                records = result.get("records") or []
                total_count = result.get("totalcount")

                if api_dt > 3.0:
                    logger.warning("[%s] %s %s 第%d页 接口慢 (%.2fs)",
                                   source.name, cat_name, label, page_idx + 1, api_dt)

                if not records:
                    logger.info("[%s] %s %s 第%d页 无数据，结束本分片 (totalcount=%s)",
                                source.name, cat_name, label, page_idx + 1, total_count)
                    break

                new_count = 0
                skip_count = 0
                fail_count = 0
                page_t0 = time.time()
                for rec in records:
                    if check_status(prisma, job_id) in ("cancelled", "failed"):
                        logger.info("[%s] 收到取消信号，提前退出", source.name)
                        session.close()
                        return

                    link = (rec.get("linkurl") or "").strip()
                    if not link:
                        continue
                    full_url = link if link.startswith("http") else base + link
                    url_hash = _md5(full_url)

                    if prisma.visitedurl.find_unique(where={"urlHash": url_hash}):
                        skip_count += 1
                        continue

                    title = (rec.get("title") or rec.get("titlenew") or "").strip()[:500]
                    webdate = _parse_webdate(rec.get("webdate") or rec.get("infodate"))
                    zhuanzai = (rec.get("zhuanzai") or "").strip() or None

                    ok = _fetch_detail_and_store(
                        session=session,
                        prisma=prisma,
                        source=source,
                        full_url=full_url,
                        url_hash=url_hash,
                        title=title,
                        publish_date=webdate,
                        cat_name=cat_name,
                        zhuanzai=zhuanzai,
                        base_domain=base_domain,
                        raw_pages=raw_pages,
                        tracker=tracker,
                        source_label=f"{cat_name} {label}",
                    )
                    if ok:
                        new_count += 1
                        stats["details_ok"] += 1
                    else:
                        fail_count += 1
                        stats["details_fail"] += 1
                    time.sleep(delay)

                stats["new_records"] += new_count
                stats["skipped_visited"] += skip_count
                seg_new += new_count
                seg_skip += skip_count
                page_dt = time.time() - page_t0
                logger.info(
                    "[%s] %s %s 第%d页 完成: api=%.2fs 详情=%.1fs 总数=%s "
                    "返回=%d 新增=%d 跳过已抓=%d 失败=%d",
                    source.name, cat_name, label, page_idx + 1,
                    api_dt, page_dt, total_count,
                    len(records), new_count, skip_count, fail_count,
                )
                _heartbeat(f"{cat_name} {label}")

                if stop_on_all_visited and new_count == 0:
                    consecutive_empty_pages += 1
                    if consecutive_empty_pages >= 2:
                        logger.info("[%s] %s %s 连续 2 页全是已抓过的，结束本分片",
                                    source.name, cat_name, label)
                        break
                else:
                    consecutive_empty_pages = 0

                time.sleep(delay)

            seg_elapsed = time.time() - seg_t0
            logger.info(
                "[%s] %s %s 分片完成 (%d/%d) 用时%.0fs 新增=%d 跳过=%d",
                source.name, cat_name, label, win_idx, len(windows),
                seg_elapsed, seg_new, seg_skip,
            )

    job_elapsed = time.time() - job_t0
    logger.info(
        "[%s] <<<<< 全部完成: api页=%d (err=%d) | 详情 ok=%d fail=%d | "
        "新增=%d 跳过已抓=%d | force=%d | 总用时=%.0fs",
        source.name,
        stats["api_pages"], stats["api_errors"],
        stats["details_ok"], stats["details_fail"],
        stats["new_records"], stats["skipped_visited"],
        stats["force_done"],
        job_elapsed,
    )
    session.close()


def _fetch_detail_and_store(*, session, prisma, source, full_url, url_hash,
                            title, publish_date, cat_name, zhuanzai,
                            base_domain, raw_pages, tracker,
                            source_label: str = "",
                            upsert_search_result: bool = False) -> bool:
    """拉详情 HTML，写 Mongo + Postgres。返回是否成功新增。"""
    html = ""
    status_code = 0
    err_msg = None
    t0 = time.time()
    try:
        resp = session.get(full_url, timeout=20, headers=_DETAIL_HEADERS)
        status_code = resp.status_code
        if resp.status_code == 200:
            html = _decode_response(resp)
    except Exception as e:
        err_msg = f"{type(e).__name__}: {e}"
        tracker.update(errors=1)

    dt = time.time() - t0
    if err_msg:
        logger.warning("[%s] %s 详情抓取异常 (%.1fs): %s | url=%s",
                       source.name, source_label, dt, err_msg, full_url)
    elif status_code != 200:
        logger.warning("[%s] %s 详情 HTTP %d (%.1fs) | url=%s",
                       source.name, source_label, status_code, dt, full_url)
    elif dt > 3.0:
        logger.warning("[%s] %s 详情慢请求 (%.1fs) | url=%s",
                       source.name, source_label, dt, full_url)

    try:
        if html and raw_pages is not None:
            _save_raw_page(
                raw_pages, full_url, html,
                source_type="gov_api",
                title=title,
                search_query=cat_name,
                source_name=source.name,
            )
    except Exception as e:
        logger.warning("[%s] 写入 raw_pages 失败 %s: %s", source.name, full_url, e)

    try:
        prisma.visitedurl.upsert(
            where={"urlHash": url_hash},
            data={
                "create": {
                    "url": full_url,
                    "urlHash": url_hash,
                    "searchQuery": cat_name,
                    "status": status_code,
                },
                "update": {
                    "status": status_code,
                    "searchQuery": cat_name,
                },
            },
        )
    except Exception as e:
        logger.warning("[%s] visitedurl upsert 失败 %s: %s", source.name, full_url, e)

    sr_data = {
        "title": title or full_url[:500],
        "url": full_url,
        "urlHash": url_hash,
        "domain": base_domain,
        "sourceType": "gov_api",
        "sourceName": zhuanzai or source.name,
        "searchQuery": cat_name,
        "publishDate": publish_date,
    }
    try:
        if upsert_search_result:
            prisma.searchresult.upsert(
                where={"urlHash": url_hash},
                data={
                    "create": sr_data,
                    "update": {
                        "title": sr_data["title"],
                        "publishDate": publish_date,
                        "searchQuery": cat_name,
                    },
                },
            )
        else:
            prisma.searchresult.create(data=sr_data)
        tracker.update(results=1)
        return True
    except Exception as e:
        logger.warning("[%s] searchresult write 失败 %s: %s", source.name, full_url, e)
        return False
