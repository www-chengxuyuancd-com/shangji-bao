"""
通知引擎。

使用全局 NotifyConfig 过滤配置，扫描所有已解析数据，
为每条数据标注是否符合发送条件及原因，再通过渠道发送。
"""
import json
import logging
from datetime import datetime, timedelta, timezone

from prisma import Prisma

from src.notify.channels import get_channel

logger = logging.getLogger(__name__)


def _get_or_create_config(prisma):
    """获取全局通知配置（单行），不存在则创建默认配置。"""
    cfg = prisma.notifyconfig.find_first()
    if not cfg:
        cfg = prisma.notifyconfig.create(data={})
    return cfg


_REGION_SUFFIXES = ("市", "区", "县", "镇", "乡", "街道", "社区", "村")


def _strip_region_suffix(name: str) -> str:
    """去除地区名称的行政后缀，如 龙华区→龙华, 观澜镇→观澜。"""
    for suffix in _REGION_SUFFIXES:
        if name.endswith(suffix) and len(name) > len(suffix) + 1:
            return name[: -len(suffix)]
    return name


def _build_region_name_set(regions, exclude_levels=None) -> set[str]:
    """构建地区名称集合。仅对市级别去后缀做模糊匹配，其余级别完全匹配。"""
    names = set()
    for r in regions:
        if exclude_levels and r.level in exclude_levels:
            continue
        if not r.name or len(r.name) < 2:
            continue
        names.add(r.name)
        if r.level == "city":
            stripped = _strip_region_suffix(r.name)
            if len(stripped) >= 2:
                names.add(stripped)
    return names


def _get_region_names_for_filter(prisma):
    """获取市级及以下的地区名称（排除省级别，范围太广）。市级去后缀模糊匹配，其余完全匹配。"""
    all_regions = prisma.searchregion.find_many()
    return _build_region_name_set(all_regions, exclude_levels={"province"})


def _get_all_region_names(prisma):
    """获取所有地区名称（用于内容匹配显示）。排除省级别，市级去后缀，其余完全匹配。"""
    all_regions = prisma.searchregion.find_many()
    return _build_region_name_set(all_regions, exclude_levels={"province"})


def _match_region(location, region_names):
    """检查 location 是否匹配后台配置的任意地区（区/县及以下）。"""
    if not location:
        return True
    return any(name in location for name in region_names)


def _find_matched_regions(title: str, content: str, all_region_names: set) -> str:
    """在标题和正文中查找匹配到的配置地区，返回逗号分隔的地区名。"""
    text = f"{title or ''} {content or ''}"
    matched = [name for name in all_region_names if name in text]
    matched.sort(key=lambda n: text.index(n))
    seen = []
    for m in matched:
        if m not in seen:
            seen.append(m)
    return ",".join(seen[:5]) if seen else ""


def _format_message(item) -> tuple[str, str]:
    title = item.title or "无标题"
    parts = []

    if item.noticeType:
        parts.append(f"类型: {item.noticeType}")
    if item.bidder:
        parts.append(f"招标方: {item.bidder}")
    if item.location:
        parts.append(f"地点: {item.location}")
    if item.amount:
        parts.append(f"金额: {item.amount}")
    if item.publishDate:
        parts.append(f"发布: {item.publishDate.strftime('%Y-%m-%d')}")
    if item.bidEndTime:
        parts.append(f"截止: {item.bidEndTime.strftime('%Y-%m-%d %H:%M')}")
    if item.contact:
        parts.append(f"联系: {item.contact}")
    if item.summary:
        parts.append(f"摘要: {item.summary[:150]}")
    parts.append(f"链接: {item.url}")

    return title, "\n".join(parts)


def check_item_filter(item, cfg, exclude_types, region_names, blacklist_words=None) -> str | None:
    """检查一条 ParsedResult 是否应被跳过。返回跳过原因或 None（符合条件）。"""
    now = datetime.now(timezone.utc)

    if cfg.onlyRelevant and item.isRelevant is not True:
        return "not_relevant"

    if exclude_types and item.noticeType and item.noticeType in exclude_types:
        return "type"

    if blacklist_words and item.title:
        for bw in blacklist_words:
            if bw in item.title:
                return "blacklist"

    if cfg.filterRegion and not _match_region(item.location, region_names):
        return "region"

    cutoff = now - timedelta(days=cfg.filterDays)
    date_ok = False
    if item.publishDate and item.publishDate >= cutoff:
        date_ok = True
    if cfg.filterFuture and item.bidEndTime and item.bidEndTime >= now:
        date_ok = True
    if not item.publishDate and not item.bidEndTime:
        date_ok = True
    if not date_ok:
        return "date"

    return None


def prepare_notifications(prisma: Prisma | None = None) -> dict:
    """
    预生成通知消息：扫描所有 ParsedResult，
    根据全局 NotifyConfig 为每个渠道生成 pending 或 skipped 的 NotifyMessage。
    """
    own_prisma = prisma is None
    if own_prisma:
        prisma = Prisma()
        prisma.connect()

    try:
        cfg = _get_or_create_config(prisma)
        channels = prisma.notifychannel.find_many(where={"enabled": True})
        if not channels:
            return {"prepared": 0, "skipped": 0, "existing": 0, "errors": ["没有启用的通知渠道"]}

        exclude_types = set()
        if cfg.excludeTypes:
            exclude_types = {t.strip() for t in cfg.excludeTypes.split(",") if t.strip()}

        blacklist_words = set()
        if cfg.titleBlacklist:
            blacklist_words = {w.strip() for w in cfg.titleBlacklist.split(",") if w.strip()}

        region_names = _get_region_names_for_filter(prisma)
        all_region_names = _get_all_region_names(prisma)

        all_items = prisma.parsedresult.find_many(
            order={"publishDate": "desc"},
            take=2000,
        )

        stats = {"prepared": 0, "skipped": 0, "existing": 0, "errors": []}

        for ch in channels:
            for item in all_items:
                existing = prisma.notifymessage.find_first(
                    where={"channelId": ch.id, "urlHash": item.urlHash}
                )
                if existing:
                    stats["existing"] += 1
                    continue

                title, content = _format_message(item)
                skip_reason = check_item_filter(item, cfg, exclude_types, region_names, blacklist_words)
                matched_region = _find_matched_regions(
                    item.title, item.summary or item.location or "", all_region_names
                )

                prisma.notifymessage.create(data={
                    "channelId": ch.id,
                    "parsedId": item.id,
                    "urlHash": item.urlHash,
                    "status": "skipped" if skip_reason else "pending",
                    "title": item.title,
                    "url": item.url,
                    "noticeType": item.noticeType,
                    "publishDate": item.publishDate,
                    "content": f"{title}\n\n{content}",
                    "skipReason": skip_reason,
                    "matchedRegion": matched_region or None,
                })

                if skip_reason:
                    stats["skipped"] += 1
                else:
                    stats["prepared"] += 1

        return stats
    finally:
        if own_prisma:
            prisma.disconnect()


def send_notifications(prisma: Prisma | None = None) -> dict:
    """先 prepare，再发送所有 pending 状态的消息。"""
    own_prisma = prisma is None
    if own_prisma:
        prisma = Prisma()
        prisma.connect()

    try:
        prep = prepare_notifications(prisma)

        channels = prisma.notifychannel.find_many(where={"enabled": True})
        ch_map = {ch.id: ch for ch in channels}

        pending_msgs = prisma.notifymessage.find_many(
            where={"status": "pending"},
            order={"createdAt": "asc"},
        )

        stats = {
            "sent": 0,
            "skipped": prep["skipped"],
            "failed": 0,
            "prepared": prep["prepared"],
            "existing": prep["existing"],
            "errors": list(prep["errors"]),
        }

        for msg in pending_msgs:
            ch = ch_map.get(msg.channelId)
            if not ch:
                continue

            try:
                config = json.loads(ch.config or "{}")
            except json.JSONDecodeError:
                stats["errors"].append(f"[{ch.name}] config JSON 解析失败")
                continue

            channel_impl = get_channel(ch.channelType)

            lines = (msg.content or "").split("\n\n", 1)
            title = lines[0] if lines else "无标题"
            content = lines[1] if len(lines) > 1 else ""

            try:
                channel_impl.send(config, title, content)
                prisma.notifymessage.update(
                    where={"id": msg.id},
                    data={
                        "status": "sent",
                        "sentAt": datetime.now(timezone.utc),
                        "errorMsg": None,
                    },
                )
                stats["sent"] += 1
            except Exception as e:
                error_msg = str(e)[:500]
                logger.warning("Notify failed [%s -> %s]: %s", ch.name, msg.url or "", e)
                prisma.notifymessage.update(
                    where={"id": msg.id},
                    data={"status": "failed", "errorMsg": error_msg},
                )
                stats["failed"] += 1
                stats["errors"].append(f"[{ch.name}] {(msg.url or '')[:50]}: {error_msg}")

        return stats
    finally:
        if own_prisma:
            prisma.disconnect()


def reevaluate_messages(prisma: Prisma | None = None) -> dict:
    """
    根据最新 NotifyConfig 重新评估所有非手动、非已发送消息的状态。
    - pending 消息如果不再符合条件 → skipped
    - skipped（非手动）消息如果现在符合条件 → pending
    同时更新 matchedRegion。
    """
    own_prisma = prisma is None
    if own_prisma:
        prisma = Prisma()
        prisma.connect()

    try:
        cfg = _get_or_create_config(prisma)
        exclude_types = set()
        if cfg.excludeTypes:
            exclude_types = {t.strip() for t in cfg.excludeTypes.split(",") if t.strip()}

        blacklist_words = set()
        if cfg.titleBlacklist:
            blacklist_words = {w.strip() for w in cfg.titleBlacklist.split(",") if w.strip()}

        region_names = _get_region_names_for_filter(prisma)
        all_region_names = _get_all_region_names(prisma)

        msgs = prisma.notifymessage.find_many(
            where={
                "status": {"in": ["pending", "skipped"]},
            },
        )

        parsed_cache: dict = {}
        to_skip = 0
        to_restore = 0

        for msg in msgs:
            if msg.status == "skipped" and msg.skipReason == "manual":
                continue

            pid = msg.parsedId
            if pid not in parsed_cache:
                parsed_cache[pid] = prisma.parsedresult.find_unique(where={"id": pid})
            item = parsed_cache[pid]
            if not item:
                continue

            skip_reason = check_item_filter(item, cfg, exclude_types, region_names, blacklist_words)
            matched_region = _find_matched_regions(
                item.title, item.summary or item.location or "", all_region_names
            )

            if msg.status == "pending" and skip_reason:
                prisma.notifymessage.update(
                    where={"id": msg.id},
                    data={
                        "status": "skipped",
                        "skipReason": skip_reason,
                        "matchedRegion": matched_region or None,
                    },
                )
                to_skip += 1
            elif msg.status == "skipped" and not skip_reason:
                prisma.notifymessage.update(
                    where={"id": msg.id},
                    data={
                        "status": "pending",
                        "skipReason": None,
                        "matchedRegion": matched_region or None,
                    },
                )
                to_restore += 1
            else:
                if (matched_region or "") != (msg.matchedRegion or ""):
                    prisma.notifymessage.update(
                        where={"id": msg.id},
                        data={"matchedRegion": matched_region or None},
                    )

        return {"to_skip": to_skip, "to_restore": to_restore, "evaluated": len(msgs)}
    finally:
        if own_prisma:
            prisma.disconnect()
