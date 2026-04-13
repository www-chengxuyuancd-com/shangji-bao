from datetime import datetime, timedelta, timezone

from flask import Blueprint, render_template, request

from src.db.prisma_client import get_prisma

frontend_bp = Blueprint("frontend", __name__)

LEVEL_LABELS = {
    "province": "省", "city": "市", "district": "区/县",
    "street": "街道", "town": "镇", "village": "村", "community": "社区",
}


def _build_region_tree(prisma):
    """构建地区层级树，只返回 district 及以下的启用节点（用于前端地区筛选UI）。"""
    all_regions = prisma.searchregion.find_many(
        where={"enabled": True},
        order={"id": "asc"},
    )

    by_parent = {}
    for r in all_regions:
        pid = r.parentId or 0
        by_parent.setdefault(pid, []).append(r)

    def build(parent_id=0):
        nodes = []
        for r in by_parent.get(parent_id, []):
            children = build(r.id)
            nodes.append({
                "id": int(r.id),
                "name": r.name,
                "level": r.level,
                "level_label": LEVEL_LABELS.get(r.level, r.level),
                "children": children,
            })
        return nodes

    full_tree = build(0)

    districts = []
    def collect_districts(nodes):
        for n in nodes:
            if n["level"] == "district":
                districts.append(n)
            else:
                collect_districts(n.get("children", []))
    collect_districts(full_tree)
    return districts


_REGION_SUFFIXES = ("市", "区", "县", "镇", "乡", "街道", "社区", "村")


def _strip_region_suffix(name: str) -> str:
    for suffix in _REGION_SUFFIXES:
        if name.endswith(suffix) and len(name) > len(suffix) + 1:
            return name[: -len(suffix)]
    return name


def _get_all_region_names(prisma):
    """获取区/县及以下级别的地区名称（排除省级别，太宽泛），包含去后缀版本用于模糊匹配。"""
    all_regions = prisma.searchregion.find_many()
    names = set()
    for r in all_regions:
        if r.level == "province":
            continue
        if not r.name or len(r.name) < 2:
            continue
        names.add(r.name)
        stripped = _strip_region_suffix(r.name)
        if len(stripped) >= 2:
            names.add(stripped)
    return names


def _filter_by_region(results, region_names):
    """
    过滤结果：
    - 如果 location 为空/None -> 保留（文章没提取到地区）
    - 如果 location 有值 -> 检查是否包含后台任意地区名，匹配则保留
    """
    filtered = []
    for r in results:
        if not r.location:
            filtered.append(r)
            continue
        loc = r.location
        if any(name in loc for name in region_names):
            filtered.append(r)
    return filtered


@frontend_bp.route("/")
def index():
    prisma = get_prisma()
    region_tree = _build_region_tree(prisma)
    region_names = _get_all_region_names(prisma)

    where = {"isRelevant": {"not": False}}

    results_raw = prisma.parsedresult.find_many(
        where=where,
        order={"publishDate": "desc"},
        take=100,
    )

    results = _filter_by_region(results_raw, region_names)[:20]
    total = len(results)

    return render_template(
        "frontend/search.html",
        results=results,
        region_tree=region_tree,
        keyword="",
        region_name="",
        source_type="",
        date_range="",
        date_from="",
        date_to="",
        page=1,
        total=total,
        total_pages=1,
        is_home=True,
    )


@frontend_bp.route("/search")
def search():
    prisma = get_prisma()

    keyword = request.args.get("q", "").strip()
    region_name = request.args.get("region", "").strip()
    source_type = request.args.get("source_type", "").strip()
    date_range = request.args.get("date_range", "").strip()
    date_from = request.args.get("date_from", "").strip()
    date_to = request.args.get("date_to", "").strip()
    page = request.args.get("page", 1, type=int)
    per_page = 20

    conditions = [{"isRelevant": {"not": False}}]

    if keyword:
        conditions.append({
            "OR": [
                {"title": {"contains": keyword}},
                {"summary": {"contains": keyword}},
                {"searchQuery": {"contains": keyword}},
            ]
        })
    if region_name:
        conditions.append({
            "OR": [
                {"location": {"contains": region_name}},
                {"searchQuery": {"contains": region_name}},
            ]
        })
    if source_type:
        conditions.append({"noticeType": source_type})

    pub_filter = {}
    now = datetime.now(timezone.utc)
    if date_range == "today":
        pub_filter["gte"] = now.replace(hour=0, minute=0, second=0) - timedelta(hours=8)
    elif date_range == "3d":
        pub_filter["gte"] = now - timedelta(days=3)
    elif date_range == "7d":
        pub_filter["gte"] = now - timedelta(days=7)
    elif date_range == "30d":
        pub_filter["gte"] = now - timedelta(days=30)
    elif date_range == "custom":
        if date_from:
            try:
                pub_filter["gte"] = datetime.strptime(date_from, "%Y-%m-%d")
            except ValueError:
                pass
        if date_to:
            try:
                pub_filter["lte"] = datetime.strptime(date_to, "%Y-%m-%d").replace(hour=23, minute=59, second=59)
            except ValueError:
                pass
    if pub_filter:
        conditions.append({"publishDate": pub_filter})

    where = {"AND": conditions}

    region_names = _get_all_region_names(prisma)

    if not region_name:
        over_fetch = per_page * 5
        total_db = prisma.parsedresult.count(where=where)
        offset = 0
        all_filtered = []

        while len(all_filtered) < (page * per_page) and offset < total_db:
            batch = prisma.parsedresult.find_many(
                where=where,
                order={"publishDate": "desc"},
                skip=offset,
                take=over_fetch,
            )
            if not batch:
                break
            filtered = _filter_by_region(batch, region_names)
            all_filtered.extend(filtered)
            offset += over_fetch

        total = len(all_filtered)
        start = (page - 1) * per_page
        results = all_filtered[start:start + per_page]
    else:
        total = prisma.parsedresult.count(where=where)
        results = prisma.parsedresult.find_many(
            where=where,
            order={"publishDate": "desc"},
            skip=(page - 1) * per_page,
            take=per_page,
        )

    total_pages = (total + per_page - 1) // per_page if total > 0 else 1

    region_tree = _build_region_tree(prisma)

    return render_template(
        "frontend/search.html",
        results=results,
        region_tree=region_tree,
        keyword=keyword,
        region_name=region_name,
        source_type=source_type,
        date_range=date_range,
        date_from=date_from,
        date_to=date_to,
        page=page,
        total=total,
        total_pages=total_pages,
    )
