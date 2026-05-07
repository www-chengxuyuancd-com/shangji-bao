"""
修复 mongo raw_pages 里乱码的 HTML（mojibake）。

背景：早期 src/crawler/gov_api_crawler.py 的 _fetch_detail_and_store 直接用
    raw.decode(resp.encoding or "utf-8")
解码 HTTP 响应。当响应头不带 charset 时，requests 会把 resp.encoding 默认填成
"ISO-8859-1"，于是 UTF-8 字节被按 Latin-1 解码——所有中文都变成 "åå·ç" 这种乱码。
解码本身不报错（Latin-1 是单字节编码，所有字节都合法），所以错误的字符串被原样存进
了 mongo。is_valid_content 判定其"无中文"，导致解析全部失败。

修复办法：把乱码 str 重新编回 latin-1，再用 utf-8 解码即可恢复原始内容。
    fixed = bad.encode("latin-1").decode("utf-8")

用法（注意：项目依赖在 uv 虚拟环境里，必须用 `uv run python` 而不是 `python`）：
    # docker（推荐）：
    docker compose exec -T web uv run python scripts/fix_mojibake_raw_pages.py --dry-run
    docker compose exec -T web uv run python scripts/fix_mojibake_raw_pages.py
    # 本机：
    uv run python scripts/fix_mojibake_raw_pages.py [--dry-run] [--limit N]
"""
from __future__ import annotations

import argparse
import os
import re
import sys

try:
    from pymongo import MongoClient
except ImportError:
    sys.stderr.write(
        "[fix-mojibake] 找不到 pymongo。本项目依赖装在 uv 虚拟环境里，\n"
        "请使用：\n"
        "  docker compose exec -T web uv run python scripts/fix_mojibake_raw_pages.py [--dry-run]\n"
        "或本机：uv run python scripts/fix_mojibake_raw_pages.py [--dry-run]\n"
    )
    sys.exit(2)

# 检测"UTF-8 字节被按 Latin-1 解码"的乱码特征：
# UTF-8 中文是 3 字节序列 [c2-ef][80-bf][80-bf]，按 Latin-1 解码后看起来就是
# Â-ï 后跟两个 控制符/¡-¿ 区间字符，反复出现。
_MOJIBAKE_RE = re.compile(r"(?:[\u00c2-\u00ef][\u0080-\u00bf]{2}){3,}")
# 正常中文出现频率（用于判定真的就是中文文档而不是英文/十六进制等）
_CHINESE_RE = re.compile(r"[\u4e00-\u9fff]")


def looks_mojibake(html: str) -> bool:
    if not html:
        return False
    sample = html[:8000]
    chinese = len(_CHINESE_RE.findall(sample))
    if chinese >= 30:
        # 已经有不少中文，认为正常
        return False
    bad_hits = len(_MOJIBAKE_RE.findall(sample))
    return bad_hits >= 3


def try_fix(html: str) -> str | None:
    try:
        return html.encode("latin-1").decode("utf-8")
    except (UnicodeEncodeError, UnicodeDecodeError):
        return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="只统计不写回")
    ap.add_argument("--limit", type=int, default=0, help="最多处理 N 条（0=不限）")
    ap.add_argument(
        "--source-type",
        default="",
        help="只处理某个 meta.source_type；留空则处理所有",
    )
    args = ap.parse_args()

    uri = os.getenv(
        "MONGO_URI",
        "mongodb://mongodb:mongodb@localhost:27017/shangjibao?authSource=admin",
    )
    print(f"[fix-mojibake] connect: {uri}")
    mc = MongoClient(uri, serverSelectionTimeoutMS=5000)
    mdb = mc.get_default_database()
    raw_pages = mdb["raw_pages"]

    flt: dict = {}
    if args.source_type:
        flt["meta.source_type"] = args.source_type

    total = raw_pages.count_documents(flt)
    print(f"[fix-mojibake] 候选总数: {total} (filter={flt or '{}'})")

    scanned = 0
    detected = 0
    fixed = 0
    skipped_unfixable = 0

    cursor = raw_pages.find(flt, {"_id": 1, "html": 1, "url": 1}, no_cursor_timeout=True)
    try:
        for doc in cursor:
            scanned += 1
            html = doc.get("html") or ""
            if not looks_mojibake(html):
                if scanned % 500 == 0:
                    print(f"  ... 已扫描 {scanned}/{total} (异常 {detected}, 修复 {fixed})")
                continue
            detected += 1
            new_html = try_fix(html)
            if not new_html or not _CHINESE_RE.search(new_html[:4000]):
                skipped_unfixable += 1
                if skipped_unfixable <= 5:
                    print(f"  [skip] {doc.get('url','')[:100]} 无法修复")
                continue
            if args.dry_run:
                fixed += 1
                if fixed <= 5:
                    sample = new_html[:200].replace("\n", " ")
                    print(f"  [dry] {doc.get('url','')[:80]} -> {sample}")
            else:
                raw_pages.update_one(
                    {"_id": doc["_id"]},
                    {"$set": {"html": new_html}},
                )
                fixed += 1
                if fixed <= 5:
                    print(f"  [fix] {doc.get('url','')[:100]}")
            if args.limit and scanned >= args.limit:
                break
    finally:
        cursor.close()

    print(
        f"[fix-mojibake] 完成: 扫描 {scanned}, 检测出乱码 {detected}, "
        f"成功修复 {fixed}, 跳过无法修复 {skipped_unfixable}"
    )
    if not args.dry_run and fixed > 0:
        print("[fix-mojibake] 接下来到 /admin/parsed 点 启动解析 即可重新解析这些页面")


if __name__ == "__main__":
    sys.exit(main() or 0)
