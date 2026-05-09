import hashlib
from datetime import datetime, timezone

from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database

from src.config import get_config

_client: MongoClient | None = None
_db: Database | None = None


def get_mongo_db() -> Database:
    global _client, _db
    if _db is None:
        cfg = get_config()
        _client = MongoClient(cfg.MONGO_URI)
        _db = _client.get_default_database()
    return _db


def get_raw_pages_collection() -> Collection:
    return get_mongo_db()["raw_pages"]


_raw_pages_indexes_ensured = False


def ensure_raw_pages_indexes() -> None:
    """
    保证 raw_pages 集合上有必要的索引。

    背景：raw_pages 是个 5GB+ 的大集合，没有 url 索引时
    `find({"url": ...})` / `find({"url": {"$in": [...]}})` 会扫整个集合，
    导致 /admin/parsed 这种翻页页面每次几秒到几十秒。
    """
    global _raw_pages_indexes_ensured
    if _raw_pages_indexes_ensured:
        return
    try:
        coll = get_raw_pages_collection()
        # 不加 unique，避免历史重复数据触发报错；去重交给 dedup 脚本和写入逻辑。
        coll.create_index("url", name="url_idx", background=True)
        coll.create_index("content_hash", name="content_hash_idx", background=True)
        coll.create_index([("url", 1), ("crawled_at", -1)],
                          name="url_crawled_at_idx", background=True)
        _raw_pages_indexes_ensured = True
    except Exception:
        # 索引建失败不应阻塞应用启动；下次再试
        pass


def upsert_raw_page(
    collection: Collection,
    url: str,
    html: str,
    *,
    source_type: str = "",
    title: str = "",
    search_query: str = "",
    source_name: str = "",
    extra_meta: dict | None = None,
) -> str:
    """
    按 url upsert 一条 raw_pages 文档，返回 _id 字符串。

    语义：
      - 同一 url 已存在：覆盖 html / content_hash / crawled_at / meta / search_query / source_name
        （永远保留最新一次抓取，避免 mongo 里同一 url 留多份重复）
      - 不存在：insert
      - 老文档里如果有 _id 之外的额外字段（如某些爬虫自己塞的字段），不会被删除（用 $set）

    历史背景：以前各处写入是 insert_one，没去重逻辑，导致 raw_pages 出现 url 重复
    平均 8x 膨胀。改成 upsert 之后新抓取永远不再重复。
    """
    content_hash = hashlib.sha256(html.encode("utf-8")).hexdigest()
    meta = {"title": title, "source_type": source_type}
    if extra_meta:
        meta.update(extra_meta)
    set_fields = {
        "url": url,
        "html": html,
        "content_hash": content_hash,
        "crawled_at": datetime.now(timezone.utc),
        "search_query": search_query,
        "source_name": source_name,
        "meta": meta,
    }
    result = collection.update_one(
        {"url": url},
        {"$set": set_fields},
        upsert=True,
    )
    if result.upserted_id is not None:
        return str(result.upserted_id)
    # 已存在的情况下 update_one 不返回 _id，回查一次
    existing = collection.find_one({"url": url}, {"_id": 1})
    return str(existing["_id"]) if existing else ""


def store_raw_page(url: str, html: str, meta: dict | None = None) -> str:
    """
    （Scrapy 等老路径调用的薄封装）按 url upsert，并返回文档 _id。
    """
    collection = get_raw_pages_collection()
    extra_meta = dict(meta or {})
    title = extra_meta.pop("title", "")
    source_type = extra_meta.pop("source_type", "")
    return upsert_raw_page(
        collection, url=url, html=html,
        source_type=source_type, title=title,
        extra_meta=extra_meta if extra_meta else None,
    )


def close_mongo():
    global _client, _db
    if _client is not None:
        _client.close()
        _client = None
        _db = None
