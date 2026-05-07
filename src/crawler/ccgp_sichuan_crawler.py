"""
四川政府采购网（ccgp-sichuan.gov.cn）专用爬虫。

该站是 Vue SPA，列表/详情都走后端 JSON 接口（无 token）：
- 列表: GET /gpcms/rest/web/v2/info/selectInfoForIndex?currPage=&pageSize=&channelId=&siteId=
  返回 {data:{rows:[...], total: N}}
- 详情: GET /gpcms/rest/web/v2/info/getInfoById?id=<uuid>
  返回 {data:{id,title,description,content,...}}
- 站点首页配置: siteId=94c965cc-c55d-4f92-8469-d5875c68bd04（"四川政府采购网"）
- 主要 channel:
    公告信息   c5bff13f-21ca-4dac-b158-cb40accd3035 （core: 招标/中标/合同/废标 公告）
    公示信息   6d48e0f7-8dff-412f-9f89-83f01a2d296f
    工作通知   af39a1a6-1253-49fb-939e-cb929b7f5cd7
    工作动态   75dd6dc8-82e8-4a71-b0d4-899c7de1eb7f

config 字段：
- site_id: 站点 ID（默认上面的四川主站）
- channels: [{id, name}] 必填
- page_size: 默认 50
- max_pages_per_channel: 默认 200
- stop_on_all_visited: 默认 true（增量抓取）
- force_urls: 必抓详情 URL 列表（即使分页未覆盖到，也会被强制拉取）
- detail_url_template: 详情页可读 URL 模板（默认 https://www.ccgp-sichuan.gov.cn/maincms-web/article?type=notice&id={id}）

去重: visited_urls.urlHash = md5(detail_url)
节流: source.rateLimit
"""
from __future__ import annotations

import hashlib
import logging
import time
from datetime import datetime, timezone
from typing import Any, Callable
from urllib.parse import urlparse, parse_qs

import requests

logger = logging.getLogger(__name__)

_DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    "Referer": "https://www.ccgp-sichuan.gov.cn/",
    "Origin": "https://www.ccgp-sichuan.gov.cn",
}

_BASE = "https://www.ccgp-sichuan.gov.cn"
_LIST_PATH = "/gpcms/rest/web/v2/info/selectInfoForIndex"
_DETAIL_PATH = "/gpcms/rest/web/v2/info/getInfoById"
_DEFAULT_DETAIL_URL_TPL = (
    "https://www.ccgp-sichuan.gov.cn/maincms-web/article?type=notice&id={id}"
)


def _md5(s: str) -> str:
    return hashlib.md5(s.encode("utf-8")).hexdigest()


def _parse_dt(s: str | None) -> datetime | None:
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


def _extract_id_from_url(url: str) -> str | None:
    """从 https://www.ccgp-sichuan.gov.cn/maincms-web/article?type=notice&id=xxx 取出 id。"""
    if not url:
        return None
    try:
        q = parse_qs(urlparse(url).query)
    except Exception:
        return None
    vals = q.get("id") or []
    return vals[0] if vals else None


def _build_detail_html(detail: dict) -> str:
    """把 getInfoById 返回的 dict 拼成完整 HTML，方便后续解析器走通用流程。"""
    title = detail.get("title") or ""
    body_html = detail.get("content") or ""
    summary = detail.get("description") or ""
    if not body_html and summary:
        body_html = f"<div class='summary'>{summary}</div>"
    fields = []
    for k_zh, k in (
        ("发布时间", "noticeTime"),
        ("发布机构", "author"),
        ("地区", "regionName"),
        ("公告类型", "noticeTypeName"),
        ("项目编号", "openTenderCode"),
        ("预算金额", "budget"),
        ("成交金额", "successfulMoney"),
        ("采购人", "purchaser"),
        ("代理机构", "agency"),
        ("联系人", "contactPerson"),
        ("联系电话", "contactNumber"),
        ("中标供应商", "bidCompany"),
    ):
        v = detail.get(k)
        if v not in (None, ""):
            fields.append(f"<p><b>{k_zh}：</b>{v}</p>")
    return (
        f"<html><head><meta charset='utf-8'><title>{title}</title></head>"
        f"<body><h1>{title}</h1>"
        f"{''.join(fields)}"
        f"<div class='content'>{body_html}</div></body></html>"
    )


def _store_one(*, prisma, source, raw_pages, tracker, full_url: str, url_hash: str,
               title: str, publish_date, region_name: str | None,
               notice_type_name: str | None, html: str, status_code: int,
               channel_label: str, base_domain: str,
               upsert_search_result: bool = False) -> bool:
    """统一落库：raw_pages + visited_urls + search_results。返回是否新增/更新成功。"""
    try:
        if html and raw_pages is not None:
            _save_raw_page(
                raw_pages, full_url, html,
                source_type="gov_api",
                title=title,
                search_query=channel_label,
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
                    "searchQuery": channel_label,
                    "status": status_code,
                },
                "update": {
                    "status": status_code,
                    "searchQuery": channel_label,
                },
            },
        )
    except Exception as e:
        logger.warning("[%s] visitedurl upsert 失败 %s: %s", source.name, full_url, e)

    sr_data = {
        "title": (title or full_url)[:500],
        "url": full_url,
        "urlHash": url_hash,
        "domain": base_domain,
        "sourceType": "gov_api",
        "sourceName": source.name,
        "searchQuery": channel_label,
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
                        "searchQuery": channel_label,
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


def _fetch_detail_by_id(session, info_id: str) -> tuple[dict | None, int, float]:
    """调 getInfoById。返回 (data_dict, status_code, elapsed_seconds)。"""
    t0 = time.time()
    try:
        r = session.get(_BASE + _DETAIL_PATH, params={"id": info_id}, timeout=20)
    except Exception as e:
        logger.warning("getInfoById 请求异常 id=%s: %s", info_id, e)
        return None, 0, time.time() - t0
    dt = time.time() - t0
    if r.status_code != 200:
        return None, r.status_code, dt
    try:
        body = r.json()
    except Exception:
        return None, r.status_code, dt
    if str(body.get("code")) != "200":
        return None, r.status_code, dt
    return (body.get("data") or None), r.status_code, dt


def _fetch_one_force_url(session, prisma, source, raw_pages, tracker,
                        full_url: str, base_domain: str, detail_url_tpl: str) -> bool:
    """处理 force_urls 单条：解析 id → 拉详情 → 落库。即使 visited 过也覆盖，确保数据存在。"""
    info_id = _extract_id_from_url(full_url)
    if not info_id:
        logger.warning("[%s] force_url 无法解析出 id: %s", source.name, full_url)
        return False

    canonical_url = detail_url_tpl.format(id=info_id)
    url_hash = _md5(canonical_url)

    detail, status_code, dt = _fetch_detail_by_id(session, info_id)
    if dt > 3.0:
        logger.warning("[%s] force_url 详情慢 (%.1fs) id=%s", source.name, dt, info_id)
    if not detail:
        logger.warning("[%s] force_url 详情拉取失败 id=%s status=%d", source.name, info_id, status_code)
        return False

    title = detail.get("title") or info_id
    pub = _parse_dt(detail.get("noticeTime") or detail.get("publishTime"))
    region_name = detail.get("regionName")
    notice_type_name = detail.get("noticeTypeName")

    html = _build_detail_html(detail)

    # force_urls 即便 visited 过也强制重新写一份 raw_page，让解析器有数据可解
    return _store_one(
        prisma=prisma, source=source, raw_pages=raw_pages, tracker=tracker,
        full_url=canonical_url, url_hash=url_hash,
        title=title, publish_date=pub, region_name=region_name,
        notice_type_name=notice_type_name, html=html, status_code=status_code,
        channel_label="force_url", base_domain=base_domain,
        upsert_search_result=True,
    )


def crawl_ccgp_sichuan(prisma, source, cfg: dict, raw_pages, tracker, job_id: int,
                       check_status: Callable[[Any, int], str]):
    """主入口。"""
    site_id = cfg.get("site_id") or "94c965cc-c55d-4f92-8469-d5875c68bd04"
    channels = cfg.get("channels") or [
        {"id": "c5bff13f-21ca-4dac-b158-cb40accd3035", "name": "公告信息"},
    ]
    page_size = int(cfg.get("page_size", 50))
    max_pages = int(cfg.get("max_pages_per_channel", 200))
    stop_on_all_visited = bool(cfg.get("stop_on_all_visited", True))
    detail_url_tpl = cfg.get("detail_url_template", _DEFAULT_DETAIL_URL_TPL)
    force_urls: list[str] = list(cfg.get("force_urls") or [])

    rate = source.rateLimit if source.rateLimit and source.rateLimit > 0 else 0.2
    delay = 1.0 / rate
    base_domain = urlparse(_BASE).netloc

    logger.info(
        "[%s] 启动 ccgp-sichuan 爬虫: channels=%d page_size=%d max_pages=%d "
        "stop_on_all_visited=%s force_urls=%d rate=%.2f/s",
        source.name, len(channels), page_size, max_pages,
        stop_on_all_visited, len(force_urls), rate,
    )

    session = requests.Session()
    session.headers.update(_DEFAULT_HEADERS)

    stats = {"api_pages": 0, "details_ok": 0, "details_fail": 0,
             "skipped_visited": 0, "new_records": 0, "force_done": 0}
    job_t0 = time.time()
    last_heartbeat = time.time()

    def _heartbeat(prefix: str):
        nonlocal last_heartbeat
        now = time.time()
        if now - last_heartbeat < 30:
            return
        elapsed = now - job_t0
        rate_d = stats["details_ok"] / elapsed if elapsed > 0 else 0
        logger.info(
            "[%s] %s 心跳 累计: api页=%d 详情ok=%d fail=%d 新增=%d 跳过已抓=%d "
            "force=%d 已运行=%.0fs 速度=%.2f详情/s",
            source.name, prefix, stats["api_pages"], stats["details_ok"],
            stats["details_fail"], stats["new_records"], stats["skipped_visited"],
            stats["force_done"], elapsed, rate_d,
        )
        last_heartbeat = now

    # ---- 1) 先处理 force_urls，确保 user 指定的链接一定被抓到 ----
    for fu in force_urls:
        if check_status(prisma, job_id) in ("cancelled", "failed"):
            session.close()
            return
        logger.info("[%s] 处理 force_url: %s", source.name, fu)
        ok = _fetch_one_force_url(
            session, prisma, source, raw_pages, tracker,
            fu, base_domain, detail_url_tpl,
        )
        if ok:
            stats["force_done"] += 1
            stats["details_ok"] += 1
            stats["new_records"] += 1
        else:
            stats["details_fail"] += 1
        time.sleep(delay)

    # ---- 2) 按 channel 翻页 ----
    for ch in channels:
        ch_id = ch.get("id") if isinstance(ch, dict) else None
        ch_name = (ch.get("name") if isinstance(ch, dict) else "") or ""
        if not ch_id:
            continue

        logger.info("[%s] >>>>> 开始 channel %s (%s)", source.name, ch_name, ch_id)
        consecutive_empty_pages = 0
        total_count = None

        for page_idx in range(1, max_pages + 1):
            if check_status(prisma, job_id) in ("cancelled", "failed"):
                session.close()
                return

            tracker.update(query=f"[{source.name}] {ch_name} 第{page_idx}页")
            t_api = time.time()
            try:
                resp = session.get(_BASE + _LIST_PATH, params={
                    "currPage": page_idx,
                    "pageSize": page_size,
                    "channelId": ch_id,
                    "siteId": site_id,
                }, timeout=20)
            except Exception as e:
                logger.warning("[%s] %s 第%d页接口异常: %s", source.name, ch_name, page_idx, e)
                tracker.update(errors=1, pages=1)
                stats["api_pages"] += 1
                time.sleep(delay)
                break
            api_dt = time.time() - t_api
            tracker.update(pages=1)
            stats["api_pages"] += 1
            if resp.status_code != 200:
                logger.warning("[%s] %s 第%d页 HTTP %d", source.name, ch_name, page_idx, resp.status_code)
                tracker.update(errors=1)
                break
            try:
                body = resp.json()
            except Exception as e:
                logger.warning("[%s] %s 第%d页响应非 JSON: %s", source.name, ch_name, page_idx, e)
                tracker.update(errors=1)
                break
            data = body.get("data") or {}
            rows = data.get("rows") or []
            if total_count is None:
                total_count = data.get("total")
            if not rows:
                logger.info("[%s] %s 第%d页 无数据，结束 (total=%s)",
                            source.name, ch_name, page_idx, total_count)
                break

            new_count = 0
            skip_count = 0
            fail_count = 0
            page_t0 = time.time()
            for rec in rows:
                if check_status(prisma, job_id) in ("cancelled", "failed"):
                    session.close()
                    return

                info_id = rec.get("id")
                if not info_id:
                    continue
                full_url = detail_url_tpl.format(id=info_id)
                url_hash = _md5(full_url)

                if prisma.visitedurl.find_unique(where={"urlHash": url_hash}):
                    skip_count += 1
                    continue

                title = (rec.get("title") or "").strip()[:500]
                pub = _parse_dt(rec.get("noticeTime") or rec.get("publishTime"))
                region_name = rec.get("regionName")
                notice_type_name = rec.get("noticeTypeName")

                # 拉完整详情（list 接口已带 description，但 content 可能缺，仍调一次详情）
                detail, status_code, dt = _fetch_detail_by_id(session, info_id)
                if dt > 3.0:
                    logger.warning("[%s] %s 详情慢 (%.1fs) id=%s", source.name, ch_name, dt, info_id)
                if not detail:
                    detail = rec  # 退而求其次用 list 字段
                    if status_code == 0:
                        status_code = -1
                html = _build_detail_html(detail)

                ok = _store_one(
                    prisma=prisma, source=source, raw_pages=raw_pages, tracker=tracker,
                    full_url=full_url, url_hash=url_hash, title=title or info_id,
                    publish_date=pub, region_name=region_name,
                    notice_type_name=notice_type_name, html=html,
                    status_code=status_code if status_code > 0 else 200 if detail else 0,
                    channel_label=ch_name, base_domain=base_domain,
                )
                if ok:
                    new_count += 1
                    stats["details_ok"] += 1
                    stats["new_records"] += 1
                else:
                    fail_count += 1
                    stats["details_fail"] += 1
                time.sleep(delay)

            stats["skipped_visited"] += skip_count
            page_dt = time.time() - page_t0
            logger.info(
                "[%s] %s 第%d页完成: api=%.2fs 详情=%.1fs total=%s 返回=%d "
                "新增=%d 跳过已抓=%d 失败=%d",
                source.name, ch_name, page_idx,
                api_dt, page_dt, total_count, len(rows),
                new_count, skip_count, fail_count,
            )
            _heartbeat(ch_name)

            if stop_on_all_visited and new_count == 0:
                consecutive_empty_pages += 1
                if consecutive_empty_pages >= 2:
                    logger.info("[%s] %s 连续 2 页无新增，结束 channel", source.name, ch_name)
                    break
            else:
                consecutive_empty_pages = 0

            time.sleep(delay)

    job_elapsed = time.time() - job_t0
    logger.info(
        "[%s] <<<<< 全部完成: api页=%d 详情ok=%d fail=%d 新增=%d 跳过=%d force=%d 总用时=%.0fs",
        source.name, stats["api_pages"], stats["details_ok"], stats["details_fail"],
        stats["new_records"], stats["skipped_visited"], stats["force_done"], job_elapsed,
    )
    session.close()
