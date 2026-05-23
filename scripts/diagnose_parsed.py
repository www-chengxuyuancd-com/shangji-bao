#!/usr/bin/env python
"""
诊断 /admin/parsed 页面统计异常的脚本。

背景：
    /admin/parsed 顶部出现过 "已解析 32197 / 解析报错 200354 / 待抓取 0" 的怪现象，
    其中 ParsedResult (232551) > SearchResult (72077)，导致老的减法公式
    `unfetched = max(0, search_total - parsed_total)` 永远为 0。

本脚本会：
    1. 输出 SearchResult / ParsedResult / raw_pages 三表的总数
    2. 用 LEFT JOIN 计算 "真正待解析" 与 "孤儿 ParsedResult" 数量
    3. 按 parseErrors 文本前缀分组，看报错都是哪些类型（最容易膨胀的来源）
    4. 看 ParsedResult 是否真的按 url_hash unique（理论上 schema 是 unique 的，
       但如果之前手动迁移过，重复键会导致 prisma 行为异常）
    5. 列出 10 条孤儿 ParsedResult 的 url 样本，便于人工抽查

用法（客户机）：
    docker compose exec -T web uv run python scripts/diagnose_parsed.py

只读，不会修改任何数据。
"""
from __future__ import annotations

import os
from collections import Counter

from prisma import Prisma
from pymongo import MongoClient


def main() -> None:
    prisma = Prisma()
    prisma.connect()

    print("=" * 60)
    print("ParsedResult 异常诊断")
    print("=" * 60)

    sr_total = prisma.searchresult.count()
    pr_ok = prisma.parsedresult.count(where={"parseErrors": None})
    pr_err = prisma.parsedresult.count(where={"parseErrors": {"not": None}})
    pr_total = pr_ok + pr_err
    print(f"SearchResult 总数         : {sr_total}")
    print(f"ParsedResult 总数         : {pr_total} (ok={pr_ok}, error={pr_err})")
    print(f"ParsedResult - SearchResult 差: {pr_total - sr_total}")
    print()

    try:
        uri = os.getenv(
            "MONGO_URI",
            "mongodb://mongodb:mongodb@mongodb:27017/shangjibao?authSource=admin",
        )
        mc = MongoClient(uri, serverSelectionTimeoutMS=3000)
        mdb = mc.get_default_database()
        raw_total = mdb["raw_pages"].estimated_document_count()
        print(f"raw_pages estimated_count: {raw_total}")
        try:
            distinct_urls = len(mdb["raw_pages"].distinct("url"))
            print(f"raw_pages distinct urls  : {distinct_urls}")
            if distinct_urls and raw_total / max(1, distinct_urls) > 1.2:
                print("  ⚠ raw_pages 存在大量重复 url，建议跑 scripts/dedup_raw_pages.py")
        except Exception as e:
            print(f"  (distinct 查询失败：{e})")
        mc.close()
    except Exception as e:
        print(f"无法连接 mongo: {e}")
    print()

    print("-" * 60)
    print("待抓取 / 孤儿 解析 (LEFT JOIN)")
    print("-" * 60)
    rows = prisma.query_raw(
        """
        SELECT COUNT(*) AS cnt
        FROM search_results sr
        LEFT JOIN parsed_results pr ON sr.url_hash = pr.url_hash
        WHERE pr.id IS NULL
        """
    )
    print(f"SearchResult 无 ParsedResult (真·待解析): {int(rows[0]['cnt']) if rows else 0}")

    rows = prisma.query_raw(
        """
        SELECT COUNT(*) AS cnt
        FROM parsed_results pr
        LEFT JOIN search_results sr ON pr.url_hash = sr.url_hash
        WHERE sr.id IS NULL
        """
    )
    orphan_total = int(rows[0]["cnt"]) if rows else 0
    print(f"ParsedResult 无 SearchResult (孤儿)     : {orphan_total}")
    print()

    print("-" * 60)
    print("parseErrors 文本分组（看报错都来自哪里）")
    print("-" * 60)
    rows = prisma.query_raw(
        """
        SELECT
            COALESCE(SUBSTRING(parse_errors FROM 1 FOR 60), '<null>') AS err_prefix,
            COUNT(*) AS cnt
        FROM parsed_results
        WHERE parse_errors IS NOT NULL
        GROUP BY err_prefix
        ORDER BY cnt DESC
        LIMIT 20
        """
    )
    if rows:
        for r in rows:
            print(f"  {int(r['cnt']):>8}  {r['err_prefix']}")
    else:
        print("  (无 parseErrors)")
    print()

    print("-" * 60)
    print("孤儿 ParsedResult 样例（最多 10 条）")
    print("-" * 60)
    rows = prisma.query_raw(
        """
        SELECT pr.id, pr.url, pr.created_at, pr.source_name, pr.parse_errors IS NOT NULL AS has_err
        FROM parsed_results pr
        LEFT JOIN search_results sr ON pr.url_hash = sr.url_hash
        WHERE sr.id IS NULL
        ORDER BY pr.created_at DESC
        LIMIT 10
        """
    )
    if rows:
        for r in rows:
            url = (r.get("url") or "")[:90]
            print(f"  id={r['id']} src={r.get('source_name')} err={r.get('has_err')} {url}")
    else:
        print("  (无孤儿)")
    print()

    print("-" * 60)
    print("noticeType 分布（看是不是被 list_page/search_serp 撑爆）")
    print("-" * 60)
    rows = prisma.query_raw(
        """
        SELECT COALESCE(notice_type, '<null>') AS nt, COUNT(*) AS cnt
        FROM parsed_results
        GROUP BY nt
        ORDER BY cnt DESC
        LIMIT 20
        """
    )
    for r in rows:
        print(f"  {int(r['cnt']):>8}  {r['nt']}")
    print()

    if orphan_total > 0:
        print("=" * 60)
        print("⚠ 检测到孤儿 ParsedResult。建议：")
        print("  1. 先看上面 noticeType / parseErrors 分布，确认这些孤儿是不是历史脏数据")
        print("  2. 如果都是 list_page / search_serp / 反爬错误且无业务价值，可以直接删除：")
        print("       DELETE FROM parsed_results pr")
        print("       WHERE NOT EXISTS (")
        print("         SELECT 1 FROM search_results sr WHERE sr.url_hash = pr.url_hash")
        print("       );")
        print("     （建议先 BEGIN; 看影响行数再 COMMIT）")
        print("  3. 如果想保留，但希望 /admin/parsed 顶部数字正常，最新代码已用 LEFT JOIN")
        print("     直接算待解析数，不再受减法负数影响。")
        print("=" * 60)

    prisma.disconnect()


if __name__ == "__main__":
    main()
