#!/usr/bin/env python
"""
按 url 去重 mongo raw_pages 集合，每个 url 只保留最新的一份。

背景：早期的 store_raw_page / 各 crawler 直接 insert_one，没有 upsert，
导致同一 url 在 raw_pages 里被存了多份，造成：
- mongo 体积膨胀（5GB+）
- 翻页时 find 慢
- /admin/parsed 顶部"待解析"统计混乱

用法：
    # Docker 环境（项目使用 uv）：
    docker compose exec -T web uv run python scripts/dedup_raw_pages.py --dry-run
    docker compose exec -T web uv run python scripts/dedup_raw_pages.py

    # 本地（如果配置了 MONGO_URI 和 .env）
    uv run python scripts/dedup_raw_pages.py --dry-run

参数：
    --dry-run      只统计将要删多少，不实际删除
    --keep-strategy {newest,oldest}  保留哪一份（默认 newest）
    --batch-size N 每批处理多少 url（默认 5000）
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from collections import defaultdict


def main() -> int:
    parser = argparse.ArgumentParser(description="按 url 去重 raw_pages")
    parser.add_argument("--dry-run", action="store_true", help="只统计，不实际删除")
    parser.add_argument("--keep-strategy", choices=["newest", "oldest"], default="newest")
    parser.add_argument("--batch-size", type=int, default=5000)
    args = parser.parse_args()

    try:
        from pymongo import MongoClient
    except ImportError:
        print("ERROR: pymongo 未安装。Docker 内请用 `uv run python ...`", file=sys.stderr)
        return 1

    uri = os.getenv("MONGO_URI", "mongodb://mongodb:mongodb@localhost:27017/shangjibao?authSource=admin")
    mc = MongoClient(uri, serverSelectionTimeoutMS=5000)
    db = mc.get_default_database()
    rp = db["raw_pages"]

    t0 = time.time()
    total_before = rp.estimated_document_count()
    print(f"[dedup] raw_pages 总数: {total_before}")

    # 用聚合按 url 分组，找出有重复的 url。这一步会让 mongo 扫整个集合，
    # 大集合上比较慢；为了避免占用过多内存，分批拉取再删除。
    print("[dedup] 扫描重复 url（这一步可能要几分钟）...")
    pipeline = [
        {"$group": {"_id": "$url", "count": {"$sum": 1}}},
        {"$match": {"count": {"$gt": 1}}},
    ]
    dup_urls = []
    for d in rp.aggregate(pipeline, allowDiskUse=True):
        dup_urls.append((d["_id"], d["count"]))
    dup_url_count = len(dup_urls)
    extra_total = sum(c - 1 for _, c in dup_urls)
    print(f"[dedup] 重复 url 数: {dup_url_count}, 多余文档数: {extra_total}  (耗时 {time.time()-t0:.1f}s)")

    if not dup_urls:
        print("[dedup] 没有重复，无需处理。")
        return 0

    if args.dry_run:
        print("[dedup] --dry-run 模式：只展示统计，不删除。")
        for u, c in dup_urls[:10]:
            print(f"  n={c}  {u[:90]}")
        if dup_url_count > 10:
            print(f"  ... 还有 {dup_url_count - 10} 条")
        return 0

    sort_dir = -1 if args.keep_strategy == "newest" else 1

    deleted = 0
    processed_url = 0
    t1 = time.time()
    for i, (url, _cnt) in enumerate(dup_urls):
        # 拉这个 url 下的所有 doc 的 _id，按 crawled_at 排序
        # 为了兼容没有 crawled_at 的老数据，fallback 到 _id
        docs = list(rp.find(
            {"url": url},
            {"_id": 1, "crawled_at": 1},
        ))
        # crawled_at 缺失的视为最旧（datetime.min），用 _id 当备用排序键
        def _sort_key(d):
            return (d.get("crawled_at") or 0, str(d["_id"]))
        docs.sort(key=_sort_key, reverse=(sort_dir == -1))
        keep = docs[0]
        to_delete = [d["_id"] for d in docs[1:]]
        if to_delete:
            r = rp.delete_many({"_id": {"$in": to_delete}})
            deleted += r.deleted_count
        processed_url += 1
        if processed_url % 500 == 0:
            elapsed = time.time() - t1
            rate = processed_url / max(0.1, elapsed)
            remaining = (dup_url_count - processed_url) / max(0.1, rate)
            print(f"[dedup] 进度 {processed_url}/{dup_url_count}  已删 {deleted}  "
                  f"速率 {rate:.0f} url/s  预计剩余 {remaining:.0f}s")

    total_after = rp.estimated_document_count()
    print(f"\n[dedup] 完成：删除 {deleted} 条文档")
    print(f"        raw_pages 从 {total_before} -> {total_after} (-{total_before - total_after})")
    print(f"        总耗时 {time.time()-t0:.1f}s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
