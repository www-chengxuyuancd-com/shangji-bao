#!/usr/bin/env python
"""
清理 mongo raw_pages 里 source_type=search_engine 的文档（即 Bing/百度 等
搜索引擎结果页本身）。

为什么要清：
    _crawl_one_query 在抓搜索引擎翻页时会把 SERP 整页 HTML 也 upsert 到
    raw_pages（source_type='search_engine'），用于排查；但每次解析都会扫
    它们，让 _run_parse_job 的 totalPages 比业务量大好几倍（客户机上
    raw_pages 估算 44 万、其中 18 万是 SERP）。
    
    新代码已在 _run_parse_job 主循环跳过 source_type=search_engine，
    存量这些文档不会再生成新孤儿；但 mongo 仍然要扫到它们，所以也建议
    一次性删掉，让 totalPages 缩到正常业务量级。

用法：
    docker compose exec -T web uv run python scripts/cleanup_serp_raw_pages.py --dry-run
    docker compose exec -T web uv run python scripts/cleanup_serp_raw_pages.py

参数：
    --dry-run   只统计不删
    --batch N   每批 delete 多少（默认 10000）
"""
from __future__ import annotations

import argparse
import os
import time

from pymongo import MongoClient


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--batch", type=int, default=10000)
    args = ap.parse_args()

    uri = os.getenv(
        "MONGO_URI",
        "mongodb://mongodb:mongodb@mongodb:27017/shangjibao?authSource=admin",
    )
    mc = MongoClient(uri)
    db = mc.get_default_database()
    coll = db["raw_pages"]

    total = coll.estimated_document_count()
    print(f"raw_pages 估算总数: {total}")

    serp_filter = {"meta.source_type": "search_engine"}
    serp_cnt = coll.count_documents(serp_filter)
    print(f"source_type=search_engine 文档数: {serp_cnt}")
    print(f"清理后 raw_pages 估算: {total - serp_cnt}")

    if serp_cnt == 0 or args.dry_run:
        mc.close()
        return

    print("3 秒后开始删除，Ctrl+C 取消 …")
    time.sleep(3)

    deleted_total = 0
    t0 = time.time()
    while True:
        ids = list(
            coll.find(serp_filter, {"_id": 1}).limit(args.batch)
        )
        if not ids:
            break
        res = coll.delete_many({"_id": {"$in": [d["_id"] for d in ids]}})
        deleted_total += res.deleted_count
        print(f"  已删 {deleted_total}/{serp_cnt}  (本批 {res.deleted_count}, 累计 {time.time() - t0:.1f}s)")
        if res.deleted_count == 0:
            break

    print()
    print(f"完成：删除 {deleted_total} 条，耗时 {time.time() - t0:.1f}s")
    mc.close()


if __name__ == "__main__":
    main()
