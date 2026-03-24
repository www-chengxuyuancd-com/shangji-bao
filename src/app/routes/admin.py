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
    keyword_count = prisma.searchkeyword.count()
    region_count = prisma.searchregion.count()
    result_count = prisma.searchresult.count()
    return render_template(
        "admin/dashboard.html",
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
