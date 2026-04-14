"""
初始化行政区划数据。

1. 导入全国省市区县数据（从 pca-code.json），默认 enabled=false
2. 导入四川省 > 内江市详细的镇/街道/村/社区数据，默认 enabled=true

幂等脚本，重复运行不会产生重复数据。
在 Docker 启动时自动执行。

用法:
    uv run python scripts/seed_regions.py
"""
import json
import os

from src.db.prisma_client import get_prisma, close_prisma

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PCA_JSON_PATH = os.path.join(SCRIPT_DIR, "pca-code.json")

NEIJIANG_DATA = {
    "市中区": {
        "streets": ["城东街道", "城西街道", "玉溪街道", "牌楼街道", "乐贤街道"],
        "towns": {
            "白马镇": {}, "史家镇": {}, "凌家镇": {}, "朝阳镇": {},
            "永安镇": {}, "全安镇": {}, "龙门镇": {},
        },
    },
    "东兴区": {
        "streets": ["东兴街道", "西林街道", "新江街道"],
        "towns": {
            "田家镇": {}, "郭北镇": {}, "高粱镇": {}, "白合镇": {},
            "顺河镇": {}, "双才镇": {}, "杨家镇": {}, "椑木镇": {},
            "石子镇": {}, "平坦镇": {}, "永兴镇": {}, "双桥镇": {},
            "富溪镇": {}, "永福镇": {},
        },
    },
    "隆昌市": {
        "streets": ["古湖街道", "金鹅街道"],
        "towns": {
            "响石镇": {}, "圣灯镇": {}, "黄家镇": {}, "双凤镇": {},
            "龙市镇": {}, "界市镇": {}, "石碾镇": {}, "石燕桥镇": {},
            "胡家镇": {}, "云顶镇": {}, "普润镇": {},
        },
    },
    "资中县": {
        "streets": [],
        "towns": {
            "重龙镇": {}, "归德镇": {}, "鱼溪镇": {}, "铁佛镇": {},
            "球溪镇": {}, "龙结镇": {}, "罗泉镇": {}, "发轮镇": {},
            "银山镇": {}, "太平镇": {}, "水南镇": {}, "新桥镇": {},
            "明心寺镇": {}, "双河镇": {}, "公民镇": {}, "龙江镇": {},
            "双龙镇": {}, "高楼镇": {}, "陈家镇": {}, "孟塘镇": {},
            "马鞍镇": {}, "狮子镇": {},
        },
    },
    "威远县": {
        "streets": [],
        "towns": {
            "严陵镇": {}, "新店镇": {}, "向义镇": {}, "界牌镇": {},
            "龙会镇": {}, "高石镇": {}, "东联镇": {}, "镇西镇": {},
            "山王镇": {}, "观英滩镇": {}, "新场镇": {},
            "连界镇": {
                "villages": [
                    "连界村", "新农村", "中峰村", "盘古村", "先锋村", "杉树村",
                    "荣胜村", "中岭村", "五堡墩村", "民新村", "国防村", "广阳村",
                    "勇敢村", "永富村", "镇江村", "凉山村",
                ],
                "communities": ["建设街社区", "船石社区", "联合社区"],
            },
            "越溪镇": {
                "villages": [
                    "龙洞村", "平安村", "发展村", "金堂村", "俩母山村", "水源村",
                    "吉祥村", "楠木村", "海潮村", "涌溪村", "双石村", "青龙村",
                    "青宁村", "天宫村",
                ],
                "communities": ["场镇社区", "碗厂社区"],
            },
            "小河镇": {
                "villages": [
                    "民治村", "开元村", "牌坊村", "响水村", "平乐村",
                    "新同村", "大岩村", "新古村", "铁厂村", "复立村",
                ],
                "communities": ["回龙社区", "立石桥社区"],
            },
        },
    },
}


def _upsert(prisma, name, level, parent_id=None, code=None, enabled=True):
    """创建或更新一条地区记录，返回该记录。"""
    where_filter = {"name": name, "parentId": parent_id}
    if parent_id is None:
        where_filter = {"name": name, "parentId": None}
    existing = prisma.searchregion.find_first(where=where_filter)
    if existing:
        return existing
    data = {"name": name, "level": level, "enabled": enabled}
    if parent_id is not None:
        data["parentId"] = parent_id
    if code:
        data["code"] = code
    return prisma.searchregion.create(data=data)


def _seed_national(prisma):
    """导入全国省市区县数据（默认 enabled=false）。"""
    if not os.path.exists(PCA_JSON_PATH):
        print(f"  [跳过] 全国数据文件不存在: {PCA_JSON_PATH}")
        return

    with open(PCA_JSON_PATH, encoding="utf-8") as f:
        data = json.load(f)

    p_count, c_count, d_count = 0, 0, 0

    for prov in data:
        province = _upsert(prisma, prov["name"], "province", code=prov.get("code"), enabled=False)
        p_count += 1

        for city_data in prov.get("children", []):
            city_name = city_data["name"]
            if city_name == "市辖区":
                for dist_data in city_data.get("children", []):
                    _upsert(prisma, dist_data["name"], "district", province.id, code=dist_data.get("code"), enabled=False)
                    d_count += 1
                continue

            city = _upsert(prisma, city_name, "city", province.id, code=city_data.get("code"), enabled=False)
            c_count += 1

            for dist_data in city_data.get("children", []):
                _upsert(prisma, dist_data["name"], "district", city.id, code=dist_data.get("code"), enabled=False)
                d_count += 1

    print(f"  全国数据: 省 {p_count}, 市 {c_count}, 区县 {d_count}")


def _seed_neijiang(prisma):
    """导入四川省内江市详细数据（默认 enabled=true）。"""
    province = prisma.searchregion.find_first(where={"name": "四川省", "parentId": None})
    if not province:
        province = _upsert(prisma, "四川省", "province", enabled=True)
    else:
        prisma.searchregion.update(where={"id": province.id}, data={"enabled": True})
    print(f"  省: {province.name} (id={province.id})")

    city = prisma.searchregion.find_first(where={"name": "内江市", "parentId": province.id})
    if not city:
        city = _upsert(prisma, "内江市", "city", province.id, enabled=True)
    else:
        prisma.searchregion.update(where={"id": city.id}, data={"enabled": True})
    print(f"    市: {city.name} (id={city.id})")

    for district_name, children in NEIJIANG_DATA.items():
        district = prisma.searchregion.find_first(where={"name": district_name, "parentId": city.id})
        if not district:
            district = _upsert(prisma, district_name, "district", city.id, enabled=True)
        else:
            prisma.searchregion.update(where={"id": district.id}, data={"enabled": True})
        print(f"      区/县: {district.name} (id={district.id})")

        for street_name in children.get("streets", []):
            s = _upsert(prisma, street_name, "street", district.id, enabled=True)
            print(f"        街道: {s.name}")

        for town_name, town_data in children.get("towns", {}).items():
            t = _upsert(prisma, town_name, "town", district.id, enabled=True)
            print(f"        镇: {t.name} (id={t.id})")

            for village_name in town_data.get("villages", []):
                v = _upsert(prisma, village_name, "village", t.id, enabled=True)
                print(f"          村: {v.name}")

            for comm_name in town_data.get("communities", []):
                c = _upsert(prisma, comm_name, "community", t.id, enabled=True)
                print(f"          社区: {c.name}")


def main():
    prisma = get_prisma()

    print("导入全国省市区县数据...")
    _seed_national(prisma)

    print("\n导入内江市详细数据（启用）...")
    _seed_neijiang(prisma)

    close_prisma()
    print("\n行政区划数据导入完成！")


if __name__ == "__main__":
    main()
