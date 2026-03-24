"""
初始化默认的爬取入口（搜索引擎 + 示例网站）。

用法:
    uv run python scripts/seed_sources.py
"""
from src.db.prisma_client import get_prisma, close_prisma

DEFAULT_SOURCES = [
    {
        "name": "百度搜索",
        "sourceCategory": "search_engine",
        "baseUrl": "https://www.baidu.com",
        "searchUrlTemplate": "https://www.baidu.com/s?wd={keyword}+{region}+招标公告&pn={page}0",
        "rateLimit": 2.0,
        "notes": "百度反爬较严格，建议低速率；pn 参数为页码*10",
    },
    {
        "name": "必应搜索",
        "sourceCategory": "search_engine",
        "baseUrl": "https://cn.bing.com",
        "searchUrlTemplate": "https://cn.bing.com/search?q={keyword}+{region}+招标公告&first={page}1",
        "rateLimit": 5.0,
        "notes": "必应对爬虫相对友好；first 参数为 (页码-1)*10+1",
    },
    {
        "name": "搜狗搜索",
        "sourceCategory": "search_engine",
        "baseUrl": "https://www.sogou.com",
        "searchUrlTemplate": "https://www.sogou.com/web?query={keyword}+{region}+招标公告&page={page}",
        "rateLimit": 3.0,
        "notes": "腾讯旗下搜索引擎；page 参数为页码",
    },
    {
        "name": "360搜索",
        "sourceCategory": "search_engine",
        "baseUrl": "https://www.so.com",
        "searchUrlTemplate": "https://www.so.com/s?q={keyword}+{region}+招标公告&pn={page}",
        "rateLimit": 3.0,
        "notes": "360搜索；pn 参数为页码",
    },
    {
        "name": "腾讯搜索(搜狗)",
        "sourceCategory": "search_engine",
        "baseUrl": "https://www.sogou.com",
        "searchUrlTemplate": "https://www.sogou.com/web?query={keyword}+{region}+招标公告&page={page}",
        "rateLimit": 3.0,
        "enabled": False,
        "notes": "腾讯已将搜搜合并到搜狗，与搜狗搜索共用，默认禁用避免重复",
    },
    {
        "name": "四川省电子招标投标交易平台",
        "sourceCategory": "website",
        "baseUrl": "https://www.scebid.com/westarWeb/portal",
        "rateLimit": 5.0,
        "notes": "四川省电子招标投标信息公开平台",
    },
    {
        "name": "四川政府采购网招标代理",
        "sourceCategory": "website",
        "baseUrl": "http://www.sczbdl.cn/",
        "rateLimit": 5.0,
        "notes": "四川省政府采购招标代理网站",
    },
]


def seed():
    prisma = get_prisma()
    created = 0
    skipped = 0

    for source in DEFAULT_SOURCES:
        existing = prisma.crawlsource.find_first(
            where={"name": source["name"]}
        )
        if existing:
            skipped += 1
            print(f"  跳过（已存在）: {source['name']}")
            continue

        enabled = source.pop("enabled", True)
        prisma.crawlsource.create(data={**source, "enabled": enabled})
        created += 1
        status = "启用" if enabled else "禁用"
        print(f"  创建: {source['name']} [{status}]")

    print(f"\n完成: 新建 {created} 个, 跳过 {skipped} 个")
    close_prisma()


if __name__ == "__main__":
    print("正在初始化默认爬取入口...\n")
    seed()
