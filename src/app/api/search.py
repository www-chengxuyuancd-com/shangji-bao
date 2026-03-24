from flask import Blueprint, jsonify, request

from src.db.prisma_client import get_prisma

api_bp = Blueprint("api", __name__)


@api_bp.route("/search")
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

    return jsonify({
        "total": total,
        "page": page,
        "per_page": per_page,
        "results": [
            {
                "id": int(r.id),
                "title": r.title,
                "url": r.url,
                "source_type": r.sourceType,
                "region": r.region.name if r.region else None,
                "publish_date": r.publishDate.isoformat() if r.publishDate else None,
                "created_at": r.createdAt.isoformat(),
            }
            for r in results
        ],
    })


@api_bp.route("/regions")
def list_regions():
    prisma = get_prisma()
    regions = prisma.searchregion.find_many(where={"enabled": True}, order={"id": "asc"})
    return jsonify([{"id": int(r.id), "name": r.name, "code": r.code} for r in regions])


@api_bp.route("/keywords")
def list_keywords():
    prisma = get_prisma()
    keywords = prisma.searchkeyword.find_many(where={"enabled": True}, order={"id": "asc"})
    return jsonify([{"id": int(k.id), "keyword": k.keyword} for k in keywords])
