import hashlib

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


def store_raw_page(url: str, html: str, meta: dict | None = None) -> str:
    """存储原始网页到 MongoDB，返回文档 ID。"""
    collection = get_raw_pages_collection()
    content_hash = hashlib.sha256(html.encode("utf-8")).hexdigest()

    existing = collection.find_one({"content_hash": content_hash})
    if existing:
        return str(existing["_id"])

    doc = {
        "url": url,
        "html": html,
        "content_hash": content_hash,
        "meta": meta or {},
    }
    result = collection.insert_one(doc)
    return str(result.inserted_id)


def close_mongo():
    global _client, _db
    if _client is not None:
        _client.close()
        _client = None
        _db = None
