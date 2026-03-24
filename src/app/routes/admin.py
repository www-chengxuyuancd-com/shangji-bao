from functools import wraps

from flask import Blueprint, render_template, request, redirect, url_for, session, flash

from src.config import get_config
from src.db.prisma_client import get_prisma

admin_bp = Blueprint("admin", __name__)


def login_required(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        if not session.get("admin_logged_in"):
            return redirect(url_for("admin.login"))
        return f(*args, **kwargs)
    return wrapper


@admin_bp.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        cfg = get_config()
        username = request.form.get("username", "")
        password = request.form.get("password", "")
        if username == cfg.ADMIN_USERNAME and password == cfg.ADMIN_PASSWORD:
            session["admin_logged_in"] = True
            return redirect(url_for("admin.dashboard"))
        flash("用户名或密码错误", "error")
    return render_template("admin/login.html")


@admin_bp.route("/logout")
def logout():
    session.pop("admin_logged_in", None)
    return redirect(url_for("admin.login"))


@admin_bp.route("/")
@login_required
def dashboard():
    prisma = get_prisma()
    source_count = prisma.crawlsource.count()
    keyword_count = prisma.searchkeyword.count()
    region_count = prisma.searchregion.count()
    result_count = prisma.searchresult.count()
    return render_template(
        "admin/dashboard.html",
        source_count=source_count,
        keyword_count=keyword_count,
        region_count=region_count,
        result_count=result_count,
    )


@admin_bp.route("/keywords")
@login_required
def keywords():
    prisma = get_prisma()
    items = prisma.searchkeyword.find_many(order={"id": "desc"})
    return render_template("admin/keywords.html", keywords=items)


@admin_bp.route("/keywords/add", methods=["POST"])
@login_required
def keyword_add():
    keyword = request.form.get("keyword", "").strip()
    if keyword:
        prisma = get_prisma()
        try:
            prisma.searchkeyword.create(data={"keyword": keyword})
            flash("关键词添加成功", "success")
        except Exception:
            flash("关键词已存在", "error")
    return redirect(url_for("admin.keywords"))


@admin_bp.route("/keywords/<int:kid>/toggle", methods=["POST"])
@login_required
def keyword_toggle(kid):
    prisma = get_prisma()
    item = prisma.searchkeyword.find_unique(where={"id": kid})
    if item:
        prisma.searchkeyword.update(where={"id": kid}, data={"enabled": not item.enabled})
    return redirect(url_for("admin.keywords"))


@admin_bp.route("/keywords/<int:kid>/delete", methods=["POST"])
@login_required
def keyword_delete(kid):
    prisma = get_prisma()
    prisma.searchkeyword.delete(where={"id": kid})
    flash("关键词已删除", "success")
    return redirect(url_for("admin.keywords"))


@admin_bp.route("/regions")
@login_required
def regions():
    prisma = get_prisma()
    items = prisma.searchregion.find_many(order={"id": "desc"})
    return render_template("admin/regions.html", regions=items)


@admin_bp.route("/regions/add", methods=["POST"])
@login_required
def region_add():
    name = request.form.get("name", "").strip()
    code = request.form.get("code", "").strip() or None
    if name:
        prisma = get_prisma()
        try:
            prisma.searchregion.create(data={"name": name, "code": code})
            flash("地区添加成功", "success")
        except Exception:
            flash("地区已存在", "error")
    return redirect(url_for("admin.regions"))


@admin_bp.route("/regions/<int:rid>/toggle", methods=["POST"])
@login_required
def region_toggle(rid):
    prisma = get_prisma()
    item = prisma.searchregion.find_unique(where={"id": rid})
    if item:
        prisma.searchregion.update(where={"id": rid}, data={"enabled": not item.enabled})
    return redirect(url_for("admin.regions"))


@admin_bp.route("/regions/<int:rid>/delete", methods=["POST"])
@login_required
def region_delete(rid):
    prisma = get_prisma()
    prisma.searchregion.delete(where={"id": rid})
    flash("地区已删除", "success")
    return redirect(url_for("admin.regions"))


# ==================== 爬取入口管理 ====================

@admin_bp.route("/sources")
@login_required
def sources():
    prisma = get_prisma()
    category = request.args.get("category", "")
    where = {"sourceCategory": category} if category else {}
    items = prisma.crawlsource.find_many(where=where, order={"id": "desc"})
    return render_template("admin/sources.html", sources=items, current_category=category)


@admin_bp.route("/sources/add", methods=["POST"])
@login_required
def source_add():
    name = request.form.get("name", "").strip()
    source_category = request.form.get("source_category", "website").strip()
    base_url = request.form.get("base_url", "").strip()
    search_url_template = request.form.get("search_url_template", "").strip() or None
    rate_limit = float(request.form.get("rate_limit", "10") or "10")
    notes = request.form.get("notes", "").strip() or None

    if name and base_url:
        prisma = get_prisma()
        prisma.crawlsource.create(data={
            "name": name,
            "sourceCategory": source_category,
            "baseUrl": base_url,
            "searchUrlTemplate": search_url_template,
            "rateLimit": rate_limit,
            "notes": notes,
        })
        flash("爬取入口添加成功", "success")
    else:
        flash("名称和基础 URL 不能为空", "error")
    return redirect(url_for("admin.sources"))


@admin_bp.route("/sources/<int:sid>/edit", methods=["POST"])
@login_required
def source_edit(sid):
    prisma = get_prisma()
    data = {}
    for field in ("name", "base_url", "search_url_template", "notes"):
        val = request.form.get(field)
        if val is not None:
            prisma_field = {
                "name": "name",
                "base_url": "baseUrl",
                "search_url_template": "searchUrlTemplate",
                "notes": "notes",
            }[field]
            data[prisma_field] = val.strip() or None if field != "name" else val.strip()
    rate_limit = request.form.get("rate_limit")
    if rate_limit:
        data["rateLimit"] = float(rate_limit)
    source_category = request.form.get("source_category")
    if source_category:
        data["sourceCategory"] = source_category.strip()

    if data:
        prisma.crawlsource.update(where={"id": sid}, data=data)
        flash("爬取入口已更新", "success")
    return redirect(url_for("admin.sources"))


@admin_bp.route("/sources/<int:sid>/toggle", methods=["POST"])
@login_required
def source_toggle(sid):
    prisma = get_prisma()
    item = prisma.crawlsource.find_unique(where={"id": sid})
    if item:
        prisma.crawlsource.update(where={"id": sid}, data={"enabled": not item.enabled})
    return redirect(url_for("admin.sources"))


@admin_bp.route("/sources/<int:sid>/delete", methods=["POST"])
@login_required
def source_delete(sid):
    prisma = get_prisma()
    prisma.crawlsource.delete(where={"id": sid})
    flash("爬取入口已删除", "success")
    return redirect(url_for("admin.sources"))


@admin_bp.route("/sources/batch-rate", methods=["POST"])
@login_required
def source_batch_rate():
    """批量修改选中入口的请求速率。"""
    prisma = get_prisma()
    ids_raw = request.form.getlist("source_ids")
    rate_limit = request.form.get("batch_rate_limit", "").strip()

    if not ids_raw or not rate_limit:
        flash("请选择入口并填写速率", "error")
        return redirect(url_for("admin.sources"))

    try:
        rate = float(rate_limit)
    except ValueError:
        flash("速率值无效", "error")
        return redirect(url_for("admin.sources"))

    count = 0
    for sid in ids_raw:
        try:
            prisma.crawlsource.update(where={"id": int(sid)}, data={"rateLimit": rate})
            count += 1
        except Exception:
            pass

    flash(f"已批量更新 {count} 个入口的速率为 {rate} 次/秒", "success")
    return redirect(url_for("admin.sources"))
