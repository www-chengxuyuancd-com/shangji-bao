from flask import Blueprint, render_template, request

from src.db.prisma_client import get_prisma

frontend_bp = Blueprint("frontend", __name__)


@frontend_bp.route("/")
def index():
    prisma = get_prisma()
    regions = prisma.searchregion.find_many(where={"enabled": True}, order={"id": "asc"})
    return render_template("frontend/search.html", regions=regions)


@frontend_bp.route("/search")
def search():
    prisma = get_prisma()

    keyword = request.args.get("q", "").strip()
    region_id = request.args.get("region_id", type=int)
    source_type = request.args.get("source_type", "").strip()
    page = request.args.get("page", 1, type=int)
    per_page = 20

    where = {}
    if keyword:
        where["title"] = {"contains": keyword}
    if region_id:
        where["regionId"] = region_id
    if source_type:
        where["sourceType"] = source_type

    total = prisma.searchresult.count(where=where)
    results = prisma.searchresult.find_many(
        where=where,
        include={"region": True},
        order={"createdAt": "desc"},
        skip=(page - 1) * per_page,
        take=per_page,
    )

    regions = prisma.searchregion.find_many(where={"enabled": True}, order={"id": "asc"})
    total_pages = (total + per_page - 1) // per_page

    return render_template(
        "frontend/search.html",
        results=results,
        regions=regions,
        keyword=keyword,
        region_id=region_id,
        source_type=source_type,
        page=page,
        total=total,
        total_pages=total_pages,
    )
