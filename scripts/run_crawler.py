"""
爬虫启动脚本。

用法:
    uv run python scripts/run_crawler.py --keyword "人工智能" --region "四川"
    uv run python scripts/run_crawler.py --all  # 运行所有已配置的关键词和地区
"""
import argparse

from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

from src.db.prisma_client import get_prisma, close_prisma


def run_single(keyword: str, region: str | None = None):
    settings = get_project_settings()
    process = CrawlerProcess(settings)

    # 在此处替换为实际的 spider 类名
    # process.crawl("your_spider_name", keyword=keyword, region=region)
    # process.start()

    print(f"[提示] 爬虫模块已就绪，请实现具体的 Spider 后再运行。")
    print(f"  关键词: {keyword}")
    print(f"  地区: {region or '全部'}")


def run_all():
    prisma = get_prisma()
    keywords = prisma.searchkeyword.find_many(where={"enabled": True})
    regions = prisma.searchregion.find_many(where={"enabled": True})
    close_prisma()

    if not keywords:
        print("未配置任何关键词，请在管理后台添加。")
        return

    for kw in keywords:
        for region in regions:
            print(f"任务: keyword={kw.keyword}, region={region.name}")
            run_single(kw.keyword, region.name)

        if not regions:
            run_single(kw.keyword)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="商机宝爬虫启动脚本")
    parser.add_argument("--keyword", "-k", help="搜索关键词")
    parser.add_argument("--region", "-r", help="搜索地区")
    parser.add_argument("--all", action="store_true", help="运行所有已配置的关键词和地区组合")
    args = parser.parse_args()

    if args.all:
        run_all()
    elif args.keyword:
        run_single(args.keyword, args.region)
    else:
        parser.print_help()
