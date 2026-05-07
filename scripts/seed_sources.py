"""
初始化默认的爬取入口（搜索引擎 + 政府门户 / 招标平台）。

用法:
    uv run python scripts/seed_sources.py
"""
import json

from src.db.prisma_client import get_prisma, close_prisma


_SC_GGZY_CONFIG = json.dumps({
    "type": "gov_api_sc_ggzy",
    "endpoint": "/inteligentsearch/rest/esinteligentsearch/getFullTextDataNew",
    "categories": [
        {"num": "002001", "name": "工程建设"},
        {"num": "002002", "name": "政府采购"},
        {"num": "002003", "name": "国有资产"},
        {"num": "002004", "name": "土地使用权"},
        {"num": "002005", "name": "矿业权"},
        {"num": "002007", "name": "其他类别"},
        {"num": "002008", "name": "代理机构比选"},
        {"num": "002010", "name": "国企采购"},
        {"num": "002011", "name": "农村(基层)产权"},
    ],
    "page_size": 20,
    "max_pages_per_category": 100,
    "stop_on_all_visited": True,
    # 强制必抓的具体详情 URL（即使分页未覆盖到也会被拉到）
    "force_urls": [
        "https://ggzyjy.sc.gov.cn/jyxx/002001/002001009/20260116/aa960af2-8f62-455a-a1d9-ef288ccf45c2.html",
    ],
}, ensure_ascii=False)


_CCGP_SICHUAN_CONFIG = json.dumps({
    "type": "ccgp_sichuan",
    "site_id": "94c965cc-c55d-4f92-8469-d5875c68bd04",
    "channels": [
        # 招标/中标/合同/废标/采购意向 等的核心入口
        {"id": "c5bff13f-21ca-4dac-b158-cb40accd3035", "name": "公告信息"},
        {"id": "6d48e0f7-8dff-412f-9f89-83f01a2d296f", "name": "公示信息"},
    ],
    "page_size": 50,
    "max_pages_per_channel": 200,
    "stop_on_all_visited": True,
    "detail_url_template": "https://www.ccgp-sichuan.gov.cn/maincms-web/article?type=notice&id={id}",
    # 强制必抓
    "force_urls": [
        "https://www.ccgp-sichuan.gov.cn/maincms-web/article?type=notice&id=5d2bd28e-47bc-41af-9cc9-464c29fff300",
    ],
}, ensure_ascii=False)


_SCBID_CONFIG = json.dumps({
    "type": "list_html",
    "rubrics": [
        {"id": "17", "name": "货物类"},
        {"id": "18", "name": "工程类"},
        {"id": "19", "name": "服务类"},
    ],
    "list_url_template": "https://www.scbid.com/zbxx/{rubric}_{page}.html",
    "first_page": 1,
    "max_pages": 30,
    "stop_on_all_visited": True,
    "fetch_detail": True,
    "selectors": {
        "items": '//a[contains(@href,"/bx/detail/")]/ancestor::div[contains(@class,"row")][1]',
        "url": './/a[contains(@href,"/bx/detail/")]/@href',
        "title": './/a[contains(@href,"/bx/detail/")]/text()',
        "region": './/a[contains(@class,"region")]/text()',
        "date": './/div[contains(@class,"stime")]/text()',
    },
}, ensure_ascii=False)


_CNBIDING_CONFIG = json.dumps({
    "type": "list_html",
    "rubrics": [
        {"id": "4", "name": "招标公告"},
        {"id": "5", "name": "结果公告"},
        {"id": "6", "name": "变更公告"},
    ],
    "list_url_template": "https://www.cnbiding.com.cn/news/list.php?catid={rubric}&page={page}",
    "first_page": 1,
    "max_pages": 30,
    "stop_on_all_visited": True,
    "fetch_detail": True,
    "extra_domains": ["www.cabiding.com.cn"],
    "selectors": {
        "items": '//a[contains(@href, "cabiding.com.cn/news/") and contains(@href, ".html")]/ancestor::li[1]',
        "url": './/a/@href',
        "title": './/a/@title',
        "region": './/span[contains(@class,"layui-layout-center")]/text()',
        "date": './/span[contains(@class,"layui-layout-right")]/text()',
    },
}, ensure_ascii=False)


_CEB_CONFIG = json.dumps({
    "type": "list_html",
    "rubrics": [
        {"id": "88", "name": "招标公告"},
        {"id": "89", "name": "更正公告"},
        {"id": "90", "name": "中标结果"},
        {"id": "91", "name": "中标候选人"},
        {"id": "92", "name": "资格预审"},
    ],
    "list_url_template": "https://bulletin.cebpubservice.com/xxfbcmses/search/bulletin.html?dates=10&categoryId={rubric}&page={page}&showStatus=1",
    "first_page": 1,
    "max_pages": 1,
    "single_page_only": True,
    "fetch_detail": False,
    "extra_domains": ["ctbpsp.com"],
    "selectors": {
        "items": '//tr[.//a[contains(@href, "urlOpen")]]',
        "url": './/a[contains(@href, "urlOpen")]/@href',
        "url_extract": "urlOpen\\('([^']+)'\\)",
        "title": './/a[contains(@href, "urlOpen")]/@title',
    },
    "detail_url_template": "https://ctbpsp.com/#/bulletinDetail?uuid={value}&inpvalue=&dataSource=0&tenderAgency=",
    "notes": "翻页需要 VAPTCHA 人机验证，目前只抓首页 5 类 × ~15 条。详情页是 SPA，不抓详情 HTML，仅记录元数据",
}, ensure_ascii=False)


_SCZBCG_CONFIG = json.dumps({
    "type": "list_html",
    "rubrics": [{"id": "", "name": "首页"}],
    "list_url_template": "http://www.sczbcg.com/index.html",
    "first_page": 1,
    "max_pages": 1,
    "single_page_only": True,
    "fetch_detail": True,
    "selectors": {
        "items": '//a[contains(@href, "/news/details")]/ancestor::li[1]',
        "url": './/a[contains(@href, "/news/details")]/@href',
        "title": './/a[contains(@href, "/news/details")]/@title',
    },
    "notes": "首页有约 40 条公告（其中部分行业资讯/法规会被相关性模型过滤掉）。详情 nid 加密、无翻页机制",
}, ensure_ascii=False)


_QY_ZHAOBIAO_CONFIG = json.dumps({
    "type": "list_html",
    "rubrics": [{"id": "1", "name": "招标公告"}],
    "list_url_template": "https://qy.zhaobiao.cn/enterprise_v_c1316c35e52e427a70fc49a0245516df_gonggao_{page}.html",
    "first_page": 1,
    "max_pages": 5,
    "stop_on_all_visited": True,
    "fetch_detail": True,
    "extra_domains": ["zb.zhaobiao.cn"],
    "selectors": {
        "items": '//a[contains(@href, "zhaobiao.cn/bidding_v_")]/ancestor::tr[1]',
        "url": './/a[contains(@href, "zhaobiao.cn/bidding_v_")]/@href',
        "title": './/a[contains(@href, "zhaobiao.cn/bidding_v_")]/@title',
        "region": "./td[2]/text()",
        "date": "./td[3]/text()",
    },
    "notes": "示例：四川蜀财招标代理（一个企业页）。如需多企业，可修改 baseUrl/list_url_template",
}, ensure_ascii=False)


_ZZJYZX_CONFIG = json.dumps({
    "type": "bfs",
    "extra_domains": [],
}, ensure_ascii=False)


DEFAULT_SOURCES = [
    {
        "name": "百度搜索",
        "sourceCategory": "search_engine",
        "baseUrl": "https://www.baidu.com",
        "searchUrlTemplate": "https://www.baidu.com/s?wd={keyword}&pn={page}0",
        "rateLimit": 2.0,
        "notes": "百度反爬较严格，建议低速率；pn 参数为页码*10",
    },
    {
        "name": "必应搜索",
        "sourceCategory": "search_engine",
        "baseUrl": "https://cn.bing.com",
        "searchUrlTemplate": "https://cn.bing.com/search?q={keyword}&first={page}1",
        "rateLimit": 5.0,
        "notes": "必应对爬虫相对友好；first 参数为 (页码-1)*10+1",
    },
    {
        "name": "搜狗搜索",
        "sourceCategory": "search_engine",
        "baseUrl": "https://www.sogou.com",
        "searchUrlTemplate": "https://www.sogou.com/web?query={keyword}&page={page}",
        "rateLimit": 3.0,
        "notes": "腾讯旗下搜索引擎；page 参数为页码",
    },
    {
        "name": "360搜索",
        "sourceCategory": "search_engine",
        "baseUrl": "https://www.so.com",
        "searchUrlTemplate": "https://www.so.com/s?q={keyword}&pn={page}",
        "rateLimit": 3.0,
        "notes": "360搜索；pn 参数为页码",
    },
    {
        "name": "腾讯搜索(搜狗)",
        "sourceCategory": "search_engine",
        "baseUrl": "https://www.sogou.com",
        "searchUrlTemplate": "https://www.sogou.com/web?query={keyword}&page={page}",
        "rateLimit": 3.0,
        "enabled": False,
        "notes": "腾讯已将搜搜合并到搜狗，与搜狗搜索共用，默认禁用避免重复",
    },
    {
        "name": "四川省电子招标投标交易平台",
        "sourceCategory": "website",
        "baseUrl": "https://www.scebid.com/westarWeb/portal",
        "rateLimit": 0.2,
        "maxDepth": 3,
        "notes": "四川省电子招标投标信息公开平台（BFS 抓取，5 秒/次）",
    },
    {
        "name": "四川政府采购网招标代理",
        "sourceCategory": "website",
        "baseUrl": "http://www.sczbdl.cn/",
        "rateLimit": 0.2,
        "maxDepth": 3,
        "notes": "四川省政府采购招标代理网站（BFS 抓取，5 秒/次）",
    },
    {
        "name": "四川政府采购网（ccgp-sichuan）",
        "sourceCategory": "website",
        "baseUrl": "https://www.ccgp-sichuan.gov.cn",
        "rateLimit": 0.2,
        "maxDepth": 0,
        "notes": "四川政府采购官方门户。Vue SPA，走 gpcms JSON 接口（公告信息+公示信息），5 秒/次",
        "config": _CCGP_SICHUAN_CONFIG,
    },
    {
        "name": "四川省公共资源交易信息网",
        "sourceCategory": "website",
        "baseUrl": "https://ggzyjy.sc.gov.cn",
        "rateLimit": 0.2,
        "maxDepth": 0,
        "notes": "四川省公共资源交易门户，走 JSON 接口全量抓取（9 个业务类别），5 秒/次",
        "config": _SC_GGZY_CONFIG,
    },
    {
        "name": "资中县公共资源交易中心",
        "sourceCategory": "website",
        "baseUrl": "http://www.zzjyzx.org.cn/TPFront/",
        "rateLimit": 0.2,
        "maxDepth": 3,
        "notes": "资中县公共资源交易（内江市分中心），列表服务端渲染，BFS 抓取，5 秒/次",
    },
    {
        "name": "四川招投标网",
        "sourceCategory": "website",
        "baseUrl": "https://www.scbid.com",
        "rateLimit": 0.2,
        "maxDepth": 0,
        "notes": "四川招投标公共服务平台。按货物/工程/服务三类翻页抓取，5 秒/次",
        "config": _SCBID_CONFIG,
    },
    {
        "name": "中国招标与采购网",
        "sourceCategory": "website",
        "baseUrl": "https://www.cnbiding.com.cn",
        "rateLimit": 0.2,
        "maxDepth": 0,
        "notes": "全国汇总，详情跨域到 cabiding.com.cn。按招标/结果/变更三类抓取，5 秒/次",
        "config": _CNBIDING_CONFIG,
    },
    {
        "name": "中国招标投标公共服务平台",
        "sourceCategory": "website",
        "baseUrl": "https://bulletin.cebpubservice.com",
        "rateLimit": 0.2,
        "maxDepth": 0,
        "notes": "全国权威平台。翻页需要 VAPTCHA，仅抓 5 类首页（约 75 条/次）。详情是 SPA，不抓详情 HTML",
        "config": _CEB_CONFIG,
    },
    {
        "name": "中国招标采购信息网(sczbcg)",
        "sourceCategory": "website",
        "baseUrl": "http://www.sczbcg.com",
        "rateLimit": 0.2,
        "maxDepth": 0,
        "notes": "首页约 40 条公告，详情 nid 加密、无翻页机制。每次只抓首页",
        "config": _SCZBCG_CONFIG,
    },
    {
        "name": "招标网-四川蜀财招标代理",
        "sourceCategory": "website",
        "baseUrl": "https://qy.zhaobiao.cn",
        "rateLimit": 0.2,
        "maxDepth": 0,
        "enabled": False,
        "notes": "招标网企业公告页（示例：四川蜀财），详情跨域到 zb.zhaobiao.cn。默认禁用，需要时手动启用",
        "config": _QY_ZHAOBIAO_CONFIG,
    },
    {
        "name": "内江市公共资源交易中心",
        "sourceCategory": "website",
        "baseUrl": "http://www.njsggzy.cn:180/",
        "rateLimit": 0.2,
        "maxDepth": 0,
        "enabled": False,
        "notes": "Epoint 同款架构，但接口需要 YZM 验证码。已暂缓，待研究后再启用",
    },
    {
        "name": "中移动b2b采购",
        "sourceCategory": "website",
        "baseUrl": "https://b2b.10086.cn/",
        "rateLimit": 0.2,
        "maxDepth": 0,
        "enabled": False,
        "notes": "SPA + 强反爬，curl 直接 SSL 握手失败。占位禁用，待研究",
    },
]


_USER_CONTROLLED_FIELDS = {"enabled", "rateLimit", "maxDepth", "maxPages"}
_CODE_CONTROLLED_FIELDS = {
    "sourceCategory", "baseUrl", "searchUrlTemplate", "config", "notes",
}


def seed():
    """
    幂等同步：
    - 不存在 → 用默认值创建（包含 enabled / rateLimit）
    - 已存在 → 智能合并：只覆盖代码相关字段（config/notes/baseUrl/...），
              保留用户在管理后台手动调过的 enabled/rateLimit/maxDepth/maxPages
    """
    prisma = get_prisma()
    created = 0
    updated = 0
    unchanged = 0

    for source in DEFAULT_SOURCES:
        name = source["name"]
        defaults = dict(source)
        existing = prisma.crawlsource.find_first(where={"name": name})

        if not existing:
            enabled = defaults.pop("enabled", True)
            prisma.crawlsource.create(data={**defaults, "enabled": enabled})
            created += 1
            print(f"  创建: {name} [{'启用' if enabled else '禁用'}]")
            continue

        update_data = {}
        for k in _CODE_CONTROLLED_FIELDS:
            if k in defaults:
                old_val = getattr(existing, k, None)
                new_val = defaults[k]
                if old_val != new_val:
                    update_data[k] = new_val

        if update_data:
            prisma.crawlsource.update(where={"id": existing.id}, data=update_data)
            updated += 1
            keys = ",".join(sorted(update_data.keys()))
            print(f"  更新: {name} (字段: {keys})")
        else:
            unchanged += 1
            print(f"  保持: {name} (无代码字段变更)")

    print(f"\n完成: 新建 {created} 个, 更新 {updated} 个, 保持 {unchanged} 个")
    print("提示: enabled / rateLimit / maxDepth / maxPages 等用户配置字段不会被 seed 覆盖")
    close_prisma()


if __name__ == "__main__":
    print("正在初始化默认爬取入口...\n")
    seed()
