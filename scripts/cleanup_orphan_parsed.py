#!/usr/bin/env python
"""
清理 parsed_results 表里的"孤儿"行（无对应 SearchResult 的）。

为什么会出现：
    早期 search_engine crawler 把 Bing/百度 等搜索引擎翻页 URL 也存进了
    mongo raw_pages，_run_parse_job 扫到它们时按 search_serp 落入 parsed_results，
    但 SearchResult 表只存"搜索到的目标 URL"，不会有这些搜索引擎自身的 URL，
    于是 parsed_results 里就累积了 18 万条孤儿（典型 url 形如
    https://cn.bing.com/search?q=...&first=NN）。

影响：
    /admin/parsed 顶部"解析报错"被 search_serp 撑到 20 万；
    每次 _run_parse_job 都要把 23 万 parsed_results 拉进内存做 hash 索引，
    导致 auto_parse 启动时 jobs 页面长时间停留在 0/0。

用法（客户机）：
    # 先 dry-run，只报数：
    docker compose exec -T web uv run python scripts/cleanup_orphan_parsed.py --dry-run

    # 只清掉 search_serp / list_page / 内容无效 这三类无业务价值的孤儿（推荐）：
    docker compose exec -T web uv run python scripts/cleanup_orphan_parsed.py --safe

    # 清掉所有孤儿（包括少量"招标/中标/合同"等已经没有 SearchResult 的真业务行）：
    docker compose exec -T web uv run python scripts/cleanup_orphan_parsed.py --all

参数：
    --dry-run   只统计，不删
    --safe      只删 noticeType 在 {search_serp, list_page} 或 parseErrors 含
                "search_serp"/"listing_page"/"content_invalid" 的孤儿（默认即此模式）
    --all       删全部孤儿（包含可能有业务意义的，慎用）
    --batch N   每次 DELETE 的最大行数（默认 5000，避免长事务锁表）
"""
from __future__ import annotations

import argparse
import time

from prisma import Prisma


def _print_breakdown(prisma: Prisma) -> None:
    rows = prisma.query_raw(
        """
        SELECT
          COALESCE(pr.notice_type, '<null>') AS nt,
          COUNT(*) AS cnt
        FROM parsed_results pr
        LEFT JOIN search_results sr ON pr.url_hash = sr.url_hash
        WHERE sr.id IS NULL
        GROUP BY nt
        ORDER BY cnt DESC
        """
    )
    print("-" * 60)
    print("孤儿 ParsedResult 按 noticeType 分布：")
    print("-" * 60)
    total = 0
    for r in rows:
        c = int(r["cnt"])
        total += c
        print(f"  {c:>8}  {r['nt']}")
    print(f"  -------- 合计 {total}")
    print()


def _count_orphans(prisma: Prisma, mode: str) -> int:
    if mode == "all":
        rows = prisma.query_raw(
            """
            SELECT COUNT(*) AS cnt
            FROM parsed_results pr
            LEFT JOIN search_results sr ON pr.url_hash = sr.url_hash
            WHERE sr.id IS NULL
            """
        )
    else:
        rows = prisma.query_raw(
            """
            SELECT COUNT(*) AS cnt
            FROM parsed_results pr
            LEFT JOIN search_results sr ON pr.url_hash = sr.url_hash
            WHERE sr.id IS NULL
              AND (
                pr.notice_type IN ('search_serp', 'list_page')
                OR pr.parse_errors LIKE 'search_serp:%%'
                OR pr.parse_errors LIKE 'listing_page:%%'
                OR pr.parse_errors LIKE 'content_invalid:%%'
              )
            """
        )
    return int(rows[0]["cnt"]) if rows else 0


def _delete_batch(prisma: Prisma, mode: str, batch: int) -> int:
    """删一批，返回实际删除行数。"""
    if mode == "all":
        sql = f"""
            DELETE FROM parsed_results
            WHERE id IN (
              SELECT pr.id
              FROM parsed_results pr
              LEFT JOIN search_results sr ON pr.url_hash = sr.url_hash
              WHERE sr.id IS NULL
              LIMIT {batch}
            )
        """
    else:
        sql = f"""
            DELETE FROM parsed_results
            WHERE id IN (
              SELECT pr.id
              FROM parsed_results pr
              LEFT JOIN search_results sr ON pr.url_hash = sr.url_hash
              WHERE sr.id IS NULL
                AND (
                  pr.notice_type IN ('search_serp', 'list_page')
                  OR pr.parse_errors LIKE 'search_serp:%%'
                  OR pr.parse_errors LIKE 'listing_page:%%'
                  OR pr.parse_errors LIKE 'content_invalid:%%'
                )
              LIMIT {batch}
            )
        """
    return prisma.execute_raw(sql)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="只统计不删除")
    g = ap.add_mutually_exclusive_group()
    g.add_argument("--safe", action="store_true",
                   help="只清 search_serp / list_page / content_invalid 类（默认）")
    g.add_argument("--all", action="store_true",
                   help="清全部孤儿（含可能有业务意义的少量行，慎用）")
    ap.add_argument("--batch", type=int, default=5000)
    args = ap.parse_args()

    mode = "all" if args.all else "safe"

    prisma = Prisma()
    prisma.connect()

    _print_breakdown(prisma)

    target = _count_orphans(prisma, mode)
    print(f"将要删除（mode={mode}）: {target} 行")
    if args.dry_run or target == 0:
        prisma.disconnect()
        return

    print("3 秒后开始删除，Ctrl+C 取消 …")
    time.sleep(3)

    deleted_total = 0
    t0 = time.time()
    while True:
        n = _delete_batch(prisma, mode, args.batch)
        deleted_total += n
        elapsed = time.time() - t0
        print(f"  已删除 {deleted_total}/{target}（本批 {n}，累计 {elapsed:.1f}s）")
        if n == 0:
            break

    print()
    print(f"完成：共删除 {deleted_total} 行，耗时 {time.time() - t0:.1f}s")
    prisma.disconnect()


if __name__ == "__main__":
    main()
