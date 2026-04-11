"""
通知引擎。

扫描已解析的数据，按过滤条件筛选，通过配置的渠道发送通知。
去重：同一渠道 + 同一 urlHash 不会重复发送。
"""
import json
import logging
from datetime import datetime, timedelta, timezone

from prisma import Prisma

from src.notify.channels import get_channel

logger = logging.getLogger(__name__)


def _get_region_names_for_filter(prisma):
    """获取区/县及以下级别的地区名称（排除省、市级别，太宽泛）。"""
    all_regions = prisma.searchregion.find_many()
    return {r.name for r in all_regions if r.level not in ("province", "city")}


def _match_region(location, region_names):
    """检查 location 是否匹配后台配置的任意地区（区/县及以下）。"""
    if not location:
        return True
    return any(name in location for name in region_names)


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


def send_notifications(prisma: Prisma | None = None) -> dict:
    own_prisma = prisma is None
    if own_prisma:
        prisma = Prisma()
        prisma.connect()

    try:
        channels = prisma.notifychannel.find_many(where={"enabled": True})
        if not channels:
            return {"sent": 0, "skipped": 0, "failed": 0, "errors": ["没有启用的通知渠道"]}

        region_names = _get_region_names_for_filter(prisma)

        stats = {"sent": 0, "skipped": 0, "failed": 0, "errors": []}

        for ch in channels:
            try:
                config = json.loads(ch.config or "{}")
            except json.JSONDecodeError:
                stats["errors"].append(f"[{ch.name}] config JSON 解析失败")
                continue

            channel_impl = get_channel(ch.channelType)

            exclude_types = set()
            if ch.excludeTypes:
                exclude_types = {t.strip() for t in ch.excludeTypes.split(",") if t.strip()}

            now = datetime.now(timezone.utc)
            months_ago = now - timedelta(days=ch.filterMonths * 30)

            where_conditions = [{"isRelevant": True}]

            date_or = [{"publishDate": {"gte": months_ago}}]
            if ch.filterFuture:
                date_or.append({"bidEndTime": {"gte": now}})
            where_conditions.append({"OR": date_or})

            if exclude_types:
                for et in exclude_types:
                    where_conditions.append({"NOT": {"noticeType": et}})

            items = prisma.parsedresult.find_many(
                where={"AND": where_conditions},
                order={"publishDate": "desc"},
                take=200,
            )

            for item in items:
                if ch.filterRegion and not _match_region(item.location, region_names):
                    stats["skipped"] += 1
                    continue

                existing = prisma.notifymessage.find_first(
                    where={"channelId": ch.id, "urlHash": item.urlHash}
                )
                if existing and existing.status == "sent":
                    stats["skipped"] += 1
                    continue

                title, content = _format_message(item)

                try:
                    channel_impl.send(config, title, content)

                    if existing:
                        prisma.notifymessage.update(
                            where={"id": existing.id},
                            data={
                                "status": "sent",
                                "content": f"{title}\n\n{content}",
                                "sentAt": datetime.now(timezone.utc),
                                "errorMsg": None,
                            },
                        )
                    else:
                        prisma.notifymessage.create(data={
                            "channelId": ch.id,
                            "parsedId": item.id,
                            "urlHash": item.urlHash,
                            "status": "sent",
                            "content": f"{title}\n\n{content}",
                            "sentAt": datetime.now(timezone.utc),
                        })
                    stats["sent"] += 1

                except Exception as e:
                    error_msg = str(e)[:500]
                    logger.warning("Notify failed [%s -> %s]: %s", ch.name, item.url[:60], e)
                    if existing:
                        prisma.notifymessage.update(
                            where={"id": existing.id},
                            data={"errorMsg": error_msg},
                        )
                    else:
                        prisma.notifymessage.create(data={
                            "channelId": ch.id,
                            "parsedId": item.id,
                            "urlHash": item.urlHash,
                            "status": "failed",
                            "content": f"{title}\n\n{content}",
                            "errorMsg": error_msg,
                        })
                    stats["failed"] += 1
                    stats["errors"].append(f"[{ch.name}] {item.url[:50]}: {error_msg}")

        return stats

    finally:
        if own_prisma:
            prisma.disconnect()
