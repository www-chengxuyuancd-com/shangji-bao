import time
from functools import wraps

from flask import Blueprint, render_template, request, redirect, url_for, session, flash, jsonify

from src.config import get_config
from src.db.prisma_client import get_prisma

admin_bp = Blueprint("admin", __name__)


# raw_pages 全集合 count 在客户机上很慢（5GB+ 集合，无索引），缓存 30s。
_MONGO_COUNT_CACHE: dict = {"value": 0, "at": 0.0}
_MONGO_COUNT_TTL = 30.0


def _get_mongo_raw_pages_count_cached(raw_pages_coll) -> int:
    """raw_pages 总文档数，30 秒进程内缓存。任何线程调用都安全（GIL 保护 dict 写入）。"""
    now = time.time()
    if now - _MONGO_COUNT_CACHE["at"] < _MONGO_COUNT_TTL and _MONGO_COUNT_CACHE["value"] > 0:
        return _MONGO_COUNT_CACHE["value"]
    try:
        # estimated_document_count() 直接读元数据，比 count_documents({}) 快几个数量级
        n = raw_pages_coll.estimated_document_count()
    except Exception:
        try:
            n = raw_pages_coll.count_documents({})
        except Exception:
            n = 0
    _MONGO_COUNT_CACHE["value"] = n
    _MONGO_COUNT_CACHE["at"] = now
    return n


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
    source_enabled = prisma.crawlsource.count(where={"enabled": True})
    source_total = prisma.crawlsource.count()
    keyword_enabled = prisma.searchkeyword.count(where={"enabled": True})
    keyword_total = prisma.searchkeyword.count()
    region_enabled = prisma.searchregion.count(where={"enabled": True})
    region_total = prisma.searchregion.count()
    suffix_enabled = prisma.searchsuffix.count(where={"enabled": True})
    suffix_total = prisma.searchsuffix.count()
    result_count = prisma.searchresult.count()
    return render_template(
        "admin/dashboard.html",
        source_enabled=source_enabled,
        source_total=source_total,
        keyword_enabled=keyword_enabled,
        keyword_total=keyword_total,
        region_enabled=region_enabled,
        region_total=region_total,
        suffix_enabled=suffix_enabled,
        suffix_total=suffix_total,
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
    if request.headers.get("X-Requested-With") == "XMLHttpRequest":
        new_state = not item.enabled if item else False
        return jsonify({"ok": True, "enabled": new_state})
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
    all_regions = prisma.searchregion.find_many(order={"id": "asc"})

    LEVEL_LABELS = {"province": "省", "city": "市", "district": "区/县", "street": "街道", "town": "镇", "village": "村", "community": "社区"}

    by_parent = {}
    child_count = {}
    for r in all_regions:
        pid = r.parentId or 0
        by_parent.setdefault(pid, []).append(r)
        child_count[r.id] = child_count.get(r.id, 0)
        if r.parentId:
            child_count[r.parentId] = child_count.get(r.parentId, 0) + 1

    roots = []
    for r in by_parent.get(0, []):
        roots.append({
            "id": int(r.id),
            "name": r.name,
            "code": r.code,
            "level": r.level,
            "level_label": LEVEL_LABELS.get(r.level, r.level),
            "enabled": r.enabled,
            "child_count": child_count.get(r.id, 0),
        })

    total = len(all_regions)
    enabled = sum(1 for r in all_regions if r.enabled)

    return render_template("admin/regions.html", tree=roots, total=total, enabled=enabled, level_labels=LEVEL_LABELS)


@admin_bp.route("/regions/children/<int:parent_id>", methods=["GET"])
@login_required
def region_children(parent_id):
    """AJAX 接口：获取某节点的直接子节点。"""
    prisma = get_prisma()
    LEVEL_LABELS = {"province": "省", "city": "市", "district": "区/县", "street": "街道", "town": "镇", "village": "村", "community": "社区"}

    children = prisma.searchregion.find_many(
        where={"parentId": parent_id},
        order={"id": "asc"},
    )

    all_regions = prisma.searchregion.find_many(order={"id": "asc"})
    child_count = {}
    for r in all_regions:
        if r.parentId:
            child_count[r.parentId] = child_count.get(r.parentId, 0) + 1

    result = []
    for r in children:
        result.append({
            "id": int(r.id),
            "name": r.name,
            "code": r.code,
            "level": r.level,
            "level_label": LEVEL_LABELS.get(r.level, r.level),
            "enabled": r.enabled,
            "child_count": child_count.get(r.id, 0),
        })
    return jsonify(result)


@admin_bp.route("/regions/search", methods=["GET"])
@login_required
def region_search():
    """AJAX 接口：按名称搜索区域，返回匹配结果及其完整路径。"""
    q = request.args.get("q", "").strip()
    if not q or len(q) < 2:
        return jsonify([])

    prisma = get_prisma()
    LEVEL_LABELS = {"province": "省", "city": "市", "district": "区/县", "street": "街道", "town": "镇", "village": "村", "community": "社区"}

    matches = prisma.searchregion.find_many(
        where={"name": {"contains": q}},
        take=50,
        order={"id": "asc"},
    )

    all_regions = prisma.searchregion.find_many()
    by_id = {r.id: r for r in all_regions}

    results = []
    for r in matches:
        path_parts = []
        cur = r
        while cur.parentId and cur.parentId in by_id:
            cur = by_id[cur.parentId]
            path_parts.insert(0, cur.name)
        path_parts.append(r.name)

        results.append({
            "id": int(r.id),
            "name": r.name,
            "level": r.level,
            "level_label": LEVEL_LABELS.get(r.level, r.level),
            "enabled": r.enabled,
            "path": " > ".join(path_parts),
        })
    return jsonify(results)


@admin_bp.route("/regions/add", methods=["POST"])
@login_required
def region_add():
    name = request.form.get("name", "").strip()
    code = request.form.get("code", "").strip() or None
    level = request.form.get("level", "town").strip()
    parent_id = request.form.get("parent_id", "").strip()
    parent_id = int(parent_id) if parent_id else None
    if name:
        prisma = get_prisma()
        try:
            prisma.searchregion.create(data={
                "name": name, "code": code, "level": level, "parentId": parent_id,
            })
            flash("地区添加成功", "success")
        except Exception:
            flash("地区已存在或添加失败", "error")
    return redirect(url_for("admin.regions"))


@admin_bp.route("/regions/<int:rid>/toggle", methods=["POST"])
@login_required
def region_toggle(rid):
    prisma = get_prisma()
    item = prisma.searchregion.find_unique(where={"id": rid})
    if item:
        prisma.searchregion.update(where={"id": rid}, data={"enabled": not item.enabled})
    if request.headers.get("X-Requested-With") == "XMLHttpRequest":
        new_state = not item.enabled if item else False
        return jsonify({"ok": True, "enabled": new_state})
    return redirect(url_for("admin.regions"))


@admin_bp.route("/regions/batch-toggle", methods=["POST"])
@login_required
def region_batch_toggle():
    data = request.get_json(silent=True) or {}
    ids = data.get("ids", [])
    enabled = data.get("enabled", True)
    if not ids:
        return jsonify({"ok": False, "error": "未选择任何地区"}), 400
    prisma = get_prisma()
    count = 0
    for rid in ids:
        try:
            prisma.searchregion.update(where={"id": int(rid)}, data={"enabled": enabled})
            count += 1
        except Exception:
            pass
    return jsonify({"ok": True, "count": count})


@admin_bp.route("/regions/<int:rid>/delete", methods=["POST"])
@login_required
def region_delete(rid):
    prisma = get_prisma()
    children = prisma.searchregion.find_many(where={"parentId": rid})
    for child in children:
        grandchildren = prisma.searchregion.find_many(where={"parentId": child.id})
        for gc in grandchildren:
            prisma.searchregion.delete(where={"id": gc.id})
        prisma.searchregion.delete(where={"id": child.id})
    prisma.searchregion.delete(where={"id": rid})
    flash("地区及其子级已删除", "success")
    return redirect(url_for("admin.regions"))


# ==================== 搜索后缀管理 ====================

@admin_bp.route("/suffixes")
@login_required
def suffixes():
    prisma = get_prisma()
    items = prisma.searchsuffix.find_many(order={"id": "desc"})
    return render_template("admin/suffixes.html", suffixes=items)


@admin_bp.route("/suffixes/add", methods=["POST"])
@login_required
def suffix_add():
    suffix = request.form.get("suffix", "").strip()
    if suffix:
        prisma = get_prisma()
        try:
            prisma.searchsuffix.create(data={"suffix": suffix})
            flash("后缀添加成功", "success")
        except Exception:
            flash("后缀已存在", "error")
    return redirect(url_for("admin.suffixes"))


@admin_bp.route("/suffixes/<int:sid>/toggle", methods=["POST"])
@login_required
def suffix_toggle(sid):
    prisma = get_prisma()
    item = prisma.searchsuffix.find_unique(where={"id": sid})
    if item:
        prisma.searchsuffix.update(where={"id": sid}, data={"enabled": not item.enabled})
    if request.headers.get("X-Requested-With") == "XMLHttpRequest":
        new_state = not item.enabled if item else False
        return jsonify({"ok": True, "enabled": new_state})
    return redirect(url_for("admin.suffixes"))


@admin_bp.route("/suffixes/<int:sid>/delete", methods=["POST"])
@login_required
def suffix_delete(sid):
    prisma = get_prisma()
    prisma.searchsuffix.delete(where={"id": sid})
    flash("后缀已删除", "success")
    return redirect(url_for("admin.suffixes"))


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
    max_pages = int(request.form.get("max_pages", "10") or "10")
    max_depth = int(request.form.get("max_depth", "5") or "5")
    notes = request.form.get("notes", "").strip() or None

    if name and base_url:
        prisma = get_prisma()
        prisma.crawlsource.create(data={
            "name": name,
            "sourceCategory": source_category,
            "baseUrl": base_url,
            "searchUrlTemplate": search_url_template,
            "rateLimit": rate_limit,
            "maxPages": max_pages,
            "maxDepth": max_depth,
            "notes": notes,
        })
        flash("爬取入口添加成功", "success")
    else:
        flash("名称和基础 URL 不能为空", "error")
    return redirect(url_for("admin.sources"))


@admin_bp.route("/sources/<int:sid>/edit", methods=["POST"])
@login_required
def source_edit(sid):
    import json as _json

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
        try:
            data["rateLimit"] = float(rate_limit)
        except ValueError:
            flash(f"速率必须是数字: {rate_limit}", "error")
            return redirect(url_for("admin.sources"))

    max_pages = request.form.get("max_pages")
    if max_pages is not None and max_pages != "":
        try:
            data["maxPages"] = int(max_pages)
        except ValueError:
            flash(f"最大抓取页数必须是整数: {max_pages}", "error")
            return redirect(url_for("admin.sources"))

    max_depth = request.form.get("max_depth")
    if max_depth is not None and max_depth != "":
        try:
            data["maxDepth"] = int(max_depth)
        except ValueError:
            flash(f"最大遍历深度必须是整数: {max_depth}", "error")
            return redirect(url_for("admin.sources"))

    source_category = request.form.get("source_category")
    if source_category:
        data["sourceCategory"] = source_category.strip()

    config_raw = request.form.get("config")
    if config_raw is not None:
        config_raw = config_raw.strip()
        if config_raw == "":
            data["config"] = None
        else:
            try:
                _json.loads(config_raw)
            except _json.JSONDecodeError as e:
                flash(f"config 不是合法 JSON：{e}", "error")
                return redirect(url_for("admin.sources"))
            data["config"] = config_raw

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
    if request.headers.get("X-Requested-With") == "XMLHttpRequest":
        new_state = not item.enabled if item else False
        return jsonify({"ok": True, "enabled": new_state})
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


# ==================== 抓取任务管理 ====================

@admin_bp.route("/jobs")
@login_required
def jobs():
    prisma = get_prisma()
    items = prisma.crawljob.find_many(order={"id": "desc"}, take=50)
    return render_template("admin/jobs.html", jobs=items)


@admin_bp.route("/jobs/start", methods=["POST"])
@login_required
def job_start():
    from src.scheduler.runner import start_crawl_job
    job_id = start_crawl_job(trigger_type="manual")
    flash(f"抓取任务已启动，任务 ID: {job_id}", "success")
    return redirect(url_for("admin.jobs"))


@admin_bp.route("/api/jobs/<int:jid>/status")
@login_required
def job_status(jid):
    prisma = get_prisma()
    job = prisma.crawljob.find_unique(where={"id": jid})
    if not job:
        return jsonify({"error": "not found"}), 404
    return jsonify({
        "id": int(job.id),
        "status": job.status,
        "trigger_type": job.triggerType,
        "total_sources": job.totalSources,
        "done_sources": job.doneSources,
        "total_pages": job.totalPages,
        "done_pages": job.donePages,
        "total_queries": job.totalQueries,
        "done_queries": job.doneQueries,
        "result_count": job.resultCount,
        "error_count": job.errorCount,
        "current_query": job.currentQuery,
        "started_at": job.startedAt.isoformat() if job.startedAt else None,
        "finished_at": job.finishedAt.isoformat() if job.finishedAt else None,
    })


@admin_bp.route("/jobs/<int:jid>/cancel", methods=["POST"])
@login_required
def job_cancel(jid):
    prisma = get_prisma()
    job = prisma.crawljob.find_unique(where={"id": jid})
    if job and job.status in ("pending", "running", "paused"):
        prisma.crawljob.update(where={"id": jid}, data={"status": "cancelled"})
        flash("任务已取消", "success")
    return redirect(url_for("admin.jobs"))


@admin_bp.route("/jobs/<int:jid>/pause", methods=["POST"])
@login_required
def job_pause(jid):
    prisma = get_prisma()
    job = prisma.crawljob.find_unique(where={"id": jid})
    if job and job.status == "running":
        prisma.crawljob.update(where={"id": jid}, data={"status": "paused", "currentQuery": "已暂停，等待继续..."})
        flash("任务已暂停", "success")
    return redirect(url_for("admin.jobs"))


@admin_bp.route("/jobs/<int:jid>/resume", methods=["POST"])
@login_required
def job_resume(jid):
    from src.scheduler.runner import resume_crawl_job
    try:
        resume_crawl_job(jid)
        flash(f"任务 {jid} 已继续执行", "success")
    except Exception as e:
        flash(f"继续失败: {e}", "error")
    return redirect(url_for("admin.jobs"))


# ==================== 抓取结果浏览 ====================

@admin_bp.route("/results")
@login_required
def results():
    prisma = get_prisma()
    page = request.args.get("page", 1, type=int)
    per_page = 20
    domain_filter = request.args.get("domain", "").strip()
    keyword_filter = request.args.get("q", "").strip()
    query_filter = request.args.get("sq", "").strip()
    url_filter = request.args.get("url", "").strip()

    where = {}
    if domain_filter:
        where["domain"] = {"contains": domain_filter}
    if keyword_filter:
        where["title"] = {"contains": keyword_filter}
    if query_filter:
        where["searchQuery"] = {"contains": query_filter}
    if url_filter:
        where["url"] = {"contains": url_filter}

    total = prisma.searchresult.count(where=where)
    items = prisma.searchresult.find_many(
        where=where,
        include={"region": True},
        order={"createdAt": "desc"},
        skip=(page - 1) * per_page,
        take=per_page,
    )
    total_pages = (total + per_page - 1) // per_page

    engines = prisma.crawlsource.find_many(
        where={"sourceCategory": "search_engine", "searchUrlTemplate": {"not": None}},
    )
    source_search_map = {}
    for eng in engines:
        if eng.searchUrlTemplate:
            source_search_map[eng.name] = eng.searchUrlTemplate

    return render_template(
        "admin/results.html",
        results=items,
        page=page,
        per_page=per_page,
        total=total,
        total_pages=total_pages,
        domain_filter=domain_filter,
        keyword_filter=keyword_filter,
        query_filter=query_filter,
        url_filter=url_filter,
        source_search_map=source_search_map,
    )


# ==================== 调度配置 ====================

@admin_bp.route("/schedules")
@login_required
def schedules():
    prisma = get_prisma()
    items = prisma.crawlschedule.find_many(order={"id": "desc"})
    from src.scheduler.scheduler import get_next_run_times
    next_runs = get_next_run_times()
    return render_template("admin/schedules.html", schedules=items, next_runs=next_runs)


@admin_bp.route("/schedules/add", methods=["POST"])
@login_required
def schedule_add():
    name = request.form.get("name", "").strip()
    schedule_type = request.form.get("schedule_type", "daily")
    times_per_day = int(request.form.get("times_per_day", "1") or "1")
    start_hour = int(request.form.get("start_hour", "2") or "2")
    start_minute = int(request.form.get("start_minute", "0") or "0")
    weekdays = request.form.get("weekdays", "").strip() or None
    auto_parse = request.form.get("auto_parse") == "on"
    auto_notify = request.form.get("auto_notify") == "on"

    if name:
        prisma = get_prisma()
        prisma.crawlschedule.create(data={
            "name": name,
            "scheduleType": schedule_type,
            "timesPerDay": times_per_day,
            "startHour": start_hour,
            "startMinute": start_minute,
            "weekdays": weekdays,
            "autoParse": auto_parse,
            "autoNotify": auto_notify,
        })
        flash("调度配置添加成功", "success")
        _reload_schedules()
    return redirect(url_for("admin.schedules"))


@admin_bp.route("/schedules/<int:sid>/toggle", methods=["POST"])
@login_required
def schedule_toggle(sid):
    prisma = get_prisma()
    item = prisma.crawlschedule.find_unique(where={"id": sid})
    if item:
        prisma.crawlschedule.update(where={"id": sid}, data={"enabled": not item.enabled})
        _reload_schedules()
    return redirect(url_for("admin.schedules"))


@admin_bp.route("/schedules/<int:sid>/delete", methods=["POST"])
@login_required
def schedule_delete(sid):
    prisma = get_prisma()
    prisma.crawlschedule.delete(where={"id": sid})
    flash("调度配置已删除", "success")
    _reload_schedules()
    return redirect(url_for("admin.schedules"))


# ==================== 内容解析 ====================

@admin_bp.route("/parsed")
@login_required
def parsed_list():
    """
    内容解析列表。

    主表为 PG SearchResult（即"所有抓取过的 URL"），left-join Mongo raw_pages
    （拿 HTML/crawled_at）和 PG parsedresult（拿解析字段）。

    每行解析状态：
      - parsed    : 有解析记录、无 parseErrors
      - error     : 有解析记录、parseErrors 非空
      - unparsed  : raw_pages 里有 HTML，但 parsedresult 里没记录
      - unfetched : raw_pages 里都没有，HTML 还没下载（如 force_url 当时失败 / 搜索引擎只发现了 URL）
    """
    import hashlib
    import os
    from pymongo import MongoClient

    prisma = get_prisma()
    page = request.args.get("page", 1, type=int)
    per_page = min(request.args.get("per_page", 20, type=int), 500)
    q = request.args.get("q", "").strip()
    bidder_q = request.args.get("bidder", "").strip()
    location_q = request.args.get("location", "").strip()
    url_q = request.args.get("url", "").strip()
    relevant_q = request.args.get("relevant", "").strip()
    notice_type_q = request.args.get("notice_type", "").strip()
    parse_status_q = request.args.get("parse_status", "").strip()  # "" / parsed / unparsed / error / unfetched
    sort_by = request.args.get("sort", "createdAt")
    sort_dir = request.args.get("dir", "desc")

    # ---- 全局统计 ----
    # 全部走 PG count（毫秒级），不再扫 mongo（mongo 在客户机上有大量 url 重复，
    # count_documents 既慢又会算出"待解析 20 万"这种被重复 doc 干扰的伪数字）。
    #   抓取记录 = SearchResult 总数（按 urlHash 唯一）
    #   已解析  = ParsedResult.parseErrors=NULL 的条数
    #   解析报错 = ParsedResult.parseErrors IS NOT NULL 的条数
    #   待抓取  = SearchResult 里 ParsedResult 还没记录的（约等于"还没开始解析"）
    search_total = prisma.searchresult.count()
    parsed_ok_total = prisma.parsedresult.count(where={"parseErrors": None})
    parsed_err_total = prisma.parsedresult.count(where={"parseErrors": {"not": None}})
    parsed_total = parsed_ok_total + parsed_err_total

    raw_pages = None
    mc = None
    try:
        uri = os.getenv("MONGO_URI", "mongodb://mongodb:mongodb@localhost:27017/shangjibao?authSource=admin")
        mc = MongoClient(uri, serverSelectionTimeoutMS=2000)
        mdb = mc.get_default_database()
        raw_pages = mdb["raw_pages"]
        mongo_total = _get_mongo_raw_pages_count_cached(raw_pages)
    except Exception:
        raw_pages = None
        mongo_total = 0

    # 待抓取 = SearchResult 中 url_hash 不在 ParsedResult 的条数。
    # 注意：之前用 search_total - parsed_total 做减法，
    # 当 ParsedResult > SearchResult（出现孤儿 ParsedResult，比如旧版 gov_api 没回写
    # SearchResult、或解析时按 mongo url 落库但 SearchResult 没对应记录），
    # 减出来是负数被 max(0,) 截成 0，UI 永远显示 0，掩盖真实待解析量。
    # 改用 SQL LEFT JOIN 准确数。
    try:
        rows = prisma.query_raw(
            """
            SELECT COUNT(*) AS cnt
            FROM search_results sr
            LEFT JOIN parsed_results pr ON sr.url_hash = pr.url_hash
            WHERE pr.id IS NULL
            """
        )
        unfetched_total = int(rows[0]["cnt"]) if rows else 0
    except Exception as _e:
        logger.warning("parsed_list 待抓取统计 SQL 失败，回退到减法：%s", _e)
        unfetched_total = max(0, search_total - parsed_total)

    # 孤儿 ParsedResult：在 ParsedResult 中但没有对应 SearchResult。
    # 这通常是历史脏数据（早期 gov_api 直入 ParsedResult / 重复解析任务等），
    # 用于在 UI 上提示用户。
    try:
        rows = prisma.query_raw(
            """
            SELECT COUNT(*) AS cnt
            FROM parsed_results pr
            LEFT JOIN search_results sr ON pr.url_hash = sr.url_hash
            WHERE sr.id IS NULL
            """
        )
        orphan_parsed_total = int(rows[0]["cnt"]) if rows else 0
    except Exception:
        orphan_parsed_total = 0

    # 兼容旧模板字段名：unparsed_total 用解析报错数代替
    unparsed_total = parsed_err_total

    # ---- 列表查询：以 SearchResult 为主表 ----
    items = []
    total = 0

    # 仅"已解析"相关字段过滤（bidder/location/relevant/notice_type/parse_status=parsed|error）
    # 时走 ParsedResult 主表分页，因为这些字段都只在 ParsedResult 上才有意义。
    wants_parsed_join = bool(
        bidder_q or location_q or relevant_q or notice_type_q
        or parse_status_q in ("parsed", "error")
    )

    if wants_parsed_join:
        where = {}
        if q:
            where["title"] = {"contains": q}
        if url_q:
            where["url"] = {"contains": url_q}
        if bidder_q:
            where["bidder"] = {"contains": bidder_q}
        if location_q:
            where["location"] = {"contains": location_q}
        if relevant_q == "yes":
            where["isRelevant"] = True
        elif relevant_q == "no":
            where["isRelevant"] = False
        if notice_type_q:
            where["noticeType"] = notice_type_q
        if parse_status_q == "error":
            where["parseErrors"] = {"not": None}
        elif parse_status_q == "parsed":
            where["parseErrors"] = None

        allowed_sorts = ["createdAt", "publishDate", "amountValue", "relevanceScore",
                         "title", "bidder", "location", "noticeType"]
        sort_field = sort_by if sort_by in allowed_sorts else "createdAt"
        order_dir = "asc" if sort_dir == "asc" else "desc"

        total = prisma.parsedresult.count(where=where)
        parsed_rows = prisma.parsedresult.find_many(
            where=where, order={sort_field: order_dir},
            skip=(page - 1) * per_page, take=per_page,
        )

        mongo_by_hash: dict = {}
        if raw_pages is not None and parsed_rows:
            for d in raw_pages.find(
                {"url": {"$in": [pr.url for pr in parsed_rows]}},
                {"_id": 1, "url": 1, "crawled_at": 1, "meta": 1, "source_name": 1},
            ):
                h = hashlib.md5(d["url"].encode("utf-8")).hexdigest()
                if h not in mongo_by_hash:
                    mongo_by_hash[h] = d

        sr_by_hash: dict = {}
        if parsed_rows:
            sr_list = prisma.searchresult.find_many(
                where={"urlHash": {"in": [pr.urlHash for pr in parsed_rows if pr.urlHash]}},
            )
            sr_by_hash = {sr.urlHash: sr for sr in sr_list}

        for pr in parsed_rows:
            doc = mongo_by_hash.get(pr.urlHash)
            sr = sr_by_hash.get(pr.urlHash)
            items.append(_build_parsed_row(pr, doc, sr))
    else:
        # 主路径：SearchResult 翻页，左联 raw_pages 与 ParsedResult
        sr_where: dict = {}
        if q:
            sr_where["title"] = {"contains": q}
        if url_q:
            sr_where["url"] = {"contains": url_q}

        allowed_sr_sorts = ["createdAt", "publishDate", "title"]
        sort_field = sort_by if sort_by in allowed_sr_sorts else "createdAt"
        order_dir = "asc" if sort_dir == "asc" else "desc"

        # parse_status=unparsed/unfetched 时，over-fetch 后客户端过滤
        over_factor = 5 if parse_status_q in ("unparsed", "unfetched") else 1
        skip = (page - 1) * per_page
        fetch_limit = per_page * over_factor

        sr_count = prisma.searchresult.count(where=sr_where)
        sr_rows = prisma.searchresult.find_many(
            where=sr_where, order={sort_field: order_dir},
            skip=skip, take=fetch_limit,
        )

        mongo_by_hash: dict = {}
        if raw_pages is not None and sr_rows:
            for d in raw_pages.find(
                {"url": {"$in": [sr.url for sr in sr_rows]}},
                {"_id": 1, "url": 1, "crawled_at": 1, "meta": 1, "source_name": 1},
            ):
                h = hashlib.md5(d["url"].encode("utf-8")).hexdigest()
                cur = mongo_by_hash.get(h)
                if cur is None or (
                    d.get("crawled_at") and (cur.get("crawled_at") is None or d["crawled_at"] > cur["crawled_at"])
                ):
                    mongo_by_hash[h] = d

        parsed_by_hash: dict = {}
        hash_set = [sr.urlHash for sr in sr_rows if sr.urlHash]
        if hash_set:
            pr_list = prisma.parsedresult.find_many(where={"urlHash": {"in": hash_set}})
            parsed_by_hash = {pr.urlHash: pr for pr in pr_list}

        collected = 0
        for sr in sr_rows:
            doc = mongo_by_hash.get(sr.urlHash)
            pr = parsed_by_hash.get(sr.urlHash)

            if parse_status_q == "unfetched" and doc is not None:
                continue
            if parse_status_q == "unparsed" and (doc is None or pr is not None):
                continue

            items.append(_build_parsed_row(pr, doc, sr))
            collected += 1
            if collected >= per_page:
                break

        if parse_status_q == "unparsed":
            total = unparsed_total
        elif parse_status_q == "unfetched":
            total = unfetched_total
        else:
            total = sr_count

    if mc is not None:
        try:
            mc.close()
        except Exception:
            pass

    total_pages = max(1, (total + per_page - 1) // per_page) if total else 1

    # ---- 模型可用状态 ----
    model_path = os.path.join(
        os.getenv("MODEL_DIR", os.path.join(os.path.dirname(__file__), "..", "..", "classifier", "data", "models")),
        "relevance_model.bin",
    )
    fasttext_exists = os.path.exists(model_path)

    bert_model_exists = False
    try:
        from src.classifier.bert_trainer import BERT_MODEL_DIR
        bert_model_exists = os.path.exists(os.path.join(BERT_MODEL_DIR, "config.json"))
    except Exception:
        pass

    model_exists = bert_model_exists or fasttext_exists

    return render_template(
        "admin/parsed.html",
        items=items,
        page=page,
        per_page=per_page,
        total=total,
        total_pages=total_pages,
        q=q,
        bidder_q=bidder_q,
        location_q=location_q,
        url_q=url_q,
        relevant_q=relevant_q,
        notice_type_q=notice_type_q,
        parse_status_q=parse_status_q,
        sort_by=sort_by,
        sort_dir=sort_dir,
        search_total=search_total,
        parsed_total=parsed_total,
        mongo_total=mongo_total,
        unparsed=unparsed_total,
        unfetched=unfetched_total,
        orphan_parsed=orphan_parsed_total,
        model_exists=model_exists,
        bert_model_exists=bert_model_exists,
    )


def _build_parsed_row(pr, mongo_doc, search_result=None) -> dict:
    """
    把 (ParsedResult, raw_pages doc, SearchResult) 整合成模板需要的统一行。
    任一可为 None。
    """
    sr = search_result
    if pr is None and mongo_doc is None and sr is None:
        return {}

    meta = (mongo_doc or {}).get("meta") or {}
    raw_title = meta.get("title") or ""
    raw_url = (
        (mongo_doc or {}).get("url")
        or (pr.url if pr else None)
        or (sr.url if sr else "")
    )
    raw_source = (
        (mongo_doc or {}).get("source_name")
        or (pr.sourceName if pr else None)
        or (sr.sourceName if sr else "")
        or ""
    )
    raw_search = (
        (mongo_doc or {}).get("search_query")
        or (pr.searchQuery if pr else None)
        or (sr.searchQuery if sr else "")
        or ""
    )
    raw_crawled_at = (mongo_doc or {}).get("crawled_at")
    raw_source_type = meta.get("source_type") or (sr.sourceType if sr else "") or ""

    # 解析状态四档
    if pr is not None:
        parse_status = "error" if pr.parseErrors else "parsed"
        parse_errors = pr.parseErrors
    elif mongo_doc is not None:
        parse_status = "unparsed"
        parse_errors = None
    else:
        parse_status = "unfetched"
        parse_errors = None

    # 标题优先级：parsed.title > sr.title > meta.title > url
    title = None
    if pr and pr.title:
        title = pr.title
    elif sr and sr.title and not sr.title.startswith("http"):
        # SearchResult.title 偶尔是 url 兜底（gov_api 详情页失败时），过滤掉
        title = sr.title
    elif raw_title:
        title = raw_title
    title = title or "(未抓到标题)"

    return {
        "id": pr.id if pr else None,
        "sr_id": sr.id if sr else None,
        "mongo_id": str(mongo_doc["_id"]) if mongo_doc else (pr.mongoDocId if pr else None),
        "url": raw_url,
        "title": title,
        "summary": pr.summary if pr else None,
        "noticeType": pr.noticeType if pr else None,
        "publishDate": (pr.publishDate if pr else None) or (sr.publishDate if sr else None),
        "bidder": pr.bidder if pr else None,
        "location": pr.location if pr else None,
        "amount": pr.amount if pr else None,
        "amountValue": pr.amountValue if pr else None,
        "bidEndTime": pr.bidEndTime if pr else None,
        "contact": pr.contact if pr else None,
        "isRelevant": pr.isRelevant if pr else None,
        "relevanceScore": pr.relevanceScore if pr else None,
        "matchedKeywords": pr.matchedKeywords if pr else None,
        "searchQuery": raw_search,
        "sourceName": raw_source,
        "sourceType": raw_source_type,
        "createdAt": (
            (pr.createdAt if pr else None)
            or raw_crawled_at
            or (sr.createdAt if sr else None)
        ),
        "crawledAt": raw_crawled_at,
        "fetchedAt": (sr.createdAt if sr else None),
        "parseStatus": parse_status,
        "parseErrors": parse_errors,
        "parserVersion": pr.parserVersion if pr else None,
    }


@admin_bp.route("/parsed/start", methods=["POST"])
@login_required
def parse_start():
    """
    启动解析任务，mode 参数控制处理范围：
      - unparsed_only:  仅解析未解析的
      - errors_only:    仅重解析报错的
      - all:            重新解析全部
      - unparsed_and_errors（默认）：未解析 + 报错
    """
    from src.parser.engine import start_parse_job
    mode = request.form.get("mode", "unparsed_and_errors")
    job_id = start_parse_job(mode=mode)
    mode_label = {
        "unparsed_only": "解析未解析",
        "errors_only": "重解析报错",
        "all": "重新解析全部",
        "unparsed_and_errors": "解析未解析+报错",
    }.get(mode, mode)
    flash(f"已启动「{mode_label}」任务，任务 ID: {job_id}", "success")
    return redirect(url_for("admin.parsed_list"))


@admin_bp.route("/parsed/rejudge", methods=["POST"])
@login_required
def parse_rejudge():
    from src.parser.engine import start_relevance_rejudge
    job_id = start_relevance_rejudge()
    flash(f"相关性重新判定任务已启动（使用最新模型），任务 ID: {job_id}", "success")
    return redirect(url_for("admin.parsed_list"))


@admin_bp.route("/parsed/parse-one", methods=["POST"])
@login_required
def parse_one_url():
    """
    对一个单独 URL 触发同步抓取+解析：
      - 若 raw_pages 已有 HTML → 直接解析
      - 否则 → 立即 HTTP 抓一次详情页，写入 raw_pages 再解析（可救回 force_url 当时失败的孤儿）
    """
    import hashlib
    import os
    import time
    from datetime import datetime, timezone
    from pymongo import MongoClient

    target_url = (request.form.get("url") or "").strip()
    if not target_url:
        flash("URL 不能为空", "error")
        return redirect(url_for("admin.parsed_list"))

    prisma = get_prisma()
    url_hash = hashlib.md5(target_url.encode("utf-8")).hexdigest()

    mc = None
    try:
        uri = os.getenv(
            "MONGO_URI",
            "mongodb://mongodb:mongodb@localhost:27017/shangjibao?authSource=admin",
        )
        mc = MongoClient(uri, serverSelectionTimeoutMS=2000)
        mdb = mc.get_default_database()
        raw_pages = mdb["raw_pages"]
        doc = raw_pages.find_one({"url": target_url}, sort=[("crawled_at", -1)])
    except Exception as e:
        flash(f"无法连接 MongoDB: {e}", "error")
        if mc is not None:
            mc.close()
        return redirect(url_for("admin.parsed_list"))

    # 没 doc 或 HTML 太短 → 立刻抓一次（这正是 unfetched 的救援路径）
    needs_refetch = (not doc) or (not (doc.get("html") or "")) or len(doc.get("html") or "") < 50
    if needs_refetch:
        try:
            import requests
            from src.scheduler.runner import _decode_response  # 智能编码处理
            from src.crawler.gov_api_crawler import _save_raw_page  # 与 gov_api 一致

            sr_existing = prisma.searchresult.find_unique(where={"urlHash": url_hash})
            referer = "https://" + (target_url.split("/", 3)[2]) + "/" if "//" in target_url else ""

            t0 = time.time()
            resp = requests.get(
                target_url,
                timeout=20,
                headers={
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                                  "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
                    "Referer": referer,
                },
            )
            dt = time.time() - t0
            if resp.status_code != 200:
                if mc is not None:
                    mc.close()
                flash(f"抓取失败 HTTP {resp.status_code}（耗时 {dt:.1f}s）：{target_url[:80]}", "error")
                return redirect(url_for("admin.parsed_list"))
            html_fetched = _decode_response(resp)
            if not html_fetched or len(html_fetched) < 50:
                if mc is not None:
                    mc.close()
                flash(f"抓回的 HTML 过短/为空，无法解析：{target_url[:80]}", "error")
                return redirect(url_for("admin.parsed_list"))
            _save_raw_page(
                raw_pages, target_url, html_fetched,
                source_type=(sr_existing.sourceType if sr_existing else "manual_refetch"),
                title=(sr_existing.title if sr_existing else ""),
                search_query=(sr_existing.searchQuery if sr_existing else "manual"),
                source_name=(sr_existing.sourceName if sr_existing else "manual"),
            )
            doc = raw_pages.find_one({"url": target_url}, sort=[("crawled_at", -1)])
            flash(f"已重新抓取 ({dt:.1f}s, {len(html_fetched)} 字节)，开始解析…", "success")
        except Exception as e:
            if mc is not None:
                mc.close()
            flash(f"抓取异常：{type(e).__name__}: {e}", "error")
            return redirect(url_for("admin.parsed_list"))

    html = doc.get("html") or ""
    if not html or len(html) < 50:
        if mc is not None:
            mc.close()
        flash("原始页面 HTML 为空或过短，无法解析", "error")
        return redirect(url_for("admin.parsed_list"))

    meta = doc.get("meta") or {}
    user_keywords = [
        kw.keyword for kw in prisma.searchkeyword.find_many(where={"enabled": True})
    ]
    context = {
        "url": target_url,
        "title": meta.get("title", ""),
        "search_query": doc.get("search_query", "") or "",
        "source_name": doc.get("source_name", "") or "",
        "user_keywords": user_keywords,
    }

    from src.parser.engine import parse_one
    from src.parser.base import PARSER_VERSION

    try:
        result = parse_one(html, target_url, context)
    except Exception as e:
        if mc is not None:
            mc.close()
        flash(f"解析异常：{e}", "error")
        return redirect(url_for("admin.parsed_list"))

    existing = prisma.parsedresult.find_unique(where={"urlHash": url_hash})

    base_data = {
        "url": target_url,
        "urlHash": url_hash,
        "mongoDocId": str(doc["_id"]),
        "searchQuery": context.get("search_query") or None,
        "sourceName": context.get("source_name") or None,
        "parserVersion": PARSER_VERSION,
        "createdBy": "manual",
        "updatedBy": "manual",
    }

    if result.get("_invalid"):
        data = {
            **base_data,
            "title": context.get("title") or meta.get("title") or None,
            "isRelevant": False,
            "parseErrors": "\n".join(result.get("_errors", [])) or "content_invalid",
        }
    elif result.get("_listing"):
        data = {
            **base_data,
            "title": context.get("title") or meta.get("title") or None,
            "noticeType": "list_page",
            "isRelevant": False,
            "relevanceScore": 0.0,
            "parseErrors": "\n".join(result.get("_errors", [])) or None,
        }
    else:
        amount_data = result.get("amount") or {}
        amount_display = amount_data.get("display") if isinstance(amount_data, dict) else None
        amount_value = amount_data.get("value") if isinstance(amount_data, dict) else None
        rel = result.get("relevance") or {}
        data = {
            **base_data,
            "title": result.get("title") or context.get("title") or None,
            "summary": result.get("summary"),
            "publishDate": result.get("publish_date"),
            "bidder": result.get("bidder"),
            "location": result.get("location"),
            "bidStartTime": result.get("bid_start_time"),
            "bidEndTime": result.get("bid_end_time"),
            "amount": amount_display,
            "amountValue": amount_value,
            "contact": result.get("contact"),
            "noticeType": result.get("notice_type"),
            "isRelevant": rel.get("is_relevant"),
            "relevanceScore": rel.get("score"),
            "matchedKeywords": rel.get("matched"),
            "parseErrors": "\n".join(result.get("_errors", [])) or None,
        }

    try:
        if existing:
            update_data = {k: v for k, v in data.items() if k not in ("urlHash", "createdBy")}
            update_data["updatedAt"] = datetime.now(timezone.utc)
            prisma.parsedresult.update(where={"id": existing.id}, data=update_data)
            flash(f"已重新解析：{(data.get('title') or target_url)[:60]}", "success")
        else:
            prisma.parsedresult.create(data=data)
            flash(f"已解析：{(data.get('title') or target_url)[:60]}", "success")
    except Exception as e:
        flash(f"写入解析结果失败：{e}", "error")

    if mc is not None:
        mc.close()
    return redirect(url_for("admin.parsed_list"))


@admin_bp.route("/parsed/<int:pid>")
@login_required
def parsed_detail(pid):
    prisma = get_prisma()
    item = prisma.parsedresult.find_unique(where={"id": pid})
    if not item:
        flash("记录不存在", "error")
        return redirect(url_for("admin.parsed_list"))
    return render_template("admin/parsed_detail.html", item=item)


@admin_bp.route("/results/<int:rid>/delete", methods=["POST"])
@login_required
def result_delete(rid):
    prisma = get_prisma()
    prisma.searchresult.delete(where={"id": rid})
    if request.headers.get("X-Requested-With") == "XMLHttpRequest":
        return jsonify({"ok": True})
    flash("已删除", "success")
    return redirect(url_for("admin.results"))


@admin_bp.route("/results/batch-delete", methods=["POST"])
@login_required
def result_batch_delete():
    data = request.get_json(silent=True) or {}
    ids = data.get("ids", [])
    if not ids:
        return jsonify({"ok": False, "error": "未选择任何记录"}), 400
    prisma = get_prisma()
    deleted = 0
    for rid in ids:
        try:
            prisma.searchresult.delete(where={"id": int(rid)})
            deleted += 1
        except Exception:
            pass
    return jsonify({"ok": True, "deleted": deleted})


@admin_bp.route("/parsed/<int:pid>/delete", methods=["POST"])
@login_required
def parsed_delete(pid):
    prisma = get_prisma()
    item = prisma.parsedresult.find_unique(where={"id": pid})
    if item:
        prisma.notifymessage.delete_many(where={"urlHash": item.urlHash})
        prisma.labeledsample.delete_many(where={"urlHash": item.urlHash})
        prisma.parsedresult.delete(where={"id": pid})
    if request.headers.get("X-Requested-With") == "XMLHttpRequest":
        return jsonify({"ok": True})
    flash("已删除", "success")
    return redirect(url_for("admin.parsed_list"))


@admin_bp.route("/parsed/batch-delete", methods=["POST"])
@login_required
def parsed_batch_delete():
    """
    批量删除。ids 支持两种格式：
      - 纯整数（旧格式）：当 ParsedResult.id 处理
      - "pr:<int>"      ：删除 ParsedResult（连带 NotifyMessage / LabeledSample）
      - "sr:<int>"      ：删除 SearchResult（用于 unfetched 状态的行）
    """
    data = request.get_json(silent=True) or {}
    ids = data.get("ids", [])
    if not ids:
        return jsonify({"ok": False, "error": "未选择任何记录"}), 400
    prisma = get_prisma()
    deleted_pr = 0
    deleted_sr = 0
    for raw_id in ids:
        try:
            kind, sid = ("pr", int(raw_id)) if isinstance(raw_id, int) else None, None
            if isinstance(raw_id, int):
                kind, sid = "pr", raw_id
            elif isinstance(raw_id, str):
                if ":" in raw_id:
                    k, v = raw_id.split(":", 1)
                    if k in ("pr", "sr"):
                        kind, sid = k, int(v)
                    else:
                        kind, sid = "pr", int(v)
                else:
                    kind, sid = "pr", int(raw_id)
            else:
                continue

            if kind == "pr":
                item = prisma.parsedresult.find_unique(where={"id": sid})
                if item:
                    prisma.notifymessage.delete_many(where={"urlHash": item.urlHash})
                    prisma.labeledsample.delete_many(where={"urlHash": item.urlHash})
                    prisma.parsedresult.delete(where={"id": sid})
                    deleted_pr += 1
            elif kind == "sr":
                # 删 SearchResult 不删 ParsedResult（如果有的话），按需
                prisma.searchresult.delete(where={"id": sid})
                deleted_sr += 1
        except Exception:
            pass
    return jsonify({
        "ok": True,
        "deleted": deleted_pr + deleted_sr,
        "deleted_parsed": deleted_pr,
        "deleted_searchresult": deleted_sr,
    })


# ==================== 消息通知 ====================


def _get_notify_config(prisma):
    cfg = prisma.notifyconfig.find_first()
    if not cfg:
        cfg = prisma.notifyconfig.create(data={})
    return cfg


@admin_bp.route("/notify")
@login_required
def notify_list():
    from datetime import datetime, timedelta, timezone

    prisma = get_prisma()
    cfg = _get_notify_config(prisma)
    channels = prisma.notifychannel.find_many(order={"id": "desc"})
    page = request.args.get("page", 1, type=int)
    per_page = min(request.args.get("per_page", 30, type=int), 500)
    status_filter = request.args.get("status", "").strip()
    days_filter = request.args.get("days", "", type=str).strip()
    q = request.args.get("q", "").strip()
    url_q = request.args.get("url", "").strip()
    notice_type_filter = request.args.get("notice_type", "").strip()

    msg_where: dict = {}
    if status_filter:
        msg_where["status"] = status_filter
    if days_filter:
        try:
            d = int(days_filter)
            since = datetime.now(timezone.utc) - timedelta(days=d)
            msg_where["createdAt"] = {"gte": since}
        except ValueError:
            pass
    if q:
        msg_where["title"] = {"contains": q}
    if url_q:
        msg_where["url"] = {"contains": url_q}
    if notice_type_filter:
        msg_where["noticeType"] = notice_type_filter

    total = prisma.notifymessage.count(where=msg_where)
    messages = prisma.notifymessage.find_many(
        where=msg_where,
        include={"channel": True},
        order={"createdAt": "desc"},
        skip=(page - 1) * per_page,
        take=per_page,
    )
    total_pages = (total + per_page - 1) // per_page

    sent_count = prisma.notifymessage.count(where={"status": "sent"})
    failed_count = prisma.notifymessage.count(where={"status": "failed"})
    pending_count = prisma.notifymessage.count(where={"status": "pending"})
    skipped_count = prisma.notifymessage.count(where={"status": "skipped"})

    return render_template(
        "admin/notify.html",
        cfg=cfg,
        channels=channels,
        messages=messages,
        page=page,
        per_page=per_page,
        total=total,
        total_pages=total_pages,
        status_filter=status_filter,
        days_filter=days_filter,
        q=q,
        url_q=url_q,
        notice_type_filter=notice_type_filter,
        sent_count=sent_count,
        failed_count=failed_count,
        pending_count=pending_count,
        skipped_count=skipped_count,
    )


@admin_bp.route("/notify/config", methods=["POST"])
@login_required
def notify_config_save():
    prisma = get_prisma()
    cfg = _get_notify_config(prisma)
    filter_months = int(request.form.get("filter_months", "3") or "0")
    filter_extra_days = int(request.form.get("filter_extra_days", "0") or "0")
    filter_days = filter_months * 30 + filter_extra_days
    if filter_days < 1:
        filter_days = 90
    filter_future = request.form.get("filter_future") == "on"
    filter_region = request.form.get("filter_region") == "on"
    only_relevant = request.form.get("only_relevant") == "on"
    exclude_types = ",".join(request.form.getlist("exclude_types"))
    title_blacklist = request.form.get("title_blacklist", "").strip()
    exclude_regions = request.form.get("exclude_regions", "").strip()

    prisma.notifyconfig.update(
        where={"id": cfg.id},
        data={
            "filterDays": filter_days,
            "filterFuture": filter_future,
            "filterRegion": filter_region,
            "onlyRelevant": only_relevant,
            "excludeTypes": exclude_types or None,
            "titleBlacklist": title_blacklist or None,
            "excludeRegions": exclude_regions or None,
        },
    )

    from src.notify.engine import reevaluate_messages
    result = reevaluate_messages(prisma)
    msg = "通知过滤配置已保存"
    if result["to_skip"] or result["to_restore"]:
        msg += f"，已重新评估 {result['evaluated']} 条消息"
        if result["to_skip"]:
            msg += f"（{result['to_skip']} 条新跳过"
        if result["to_restore"]:
            msg += f"{'，' if result['to_skip'] else '（'}{result['to_restore']} 条恢复待发送"
        msg += "）"
    flash(msg, "success")
    return redirect(url_for("admin.notify_list"))


@admin_bp.route("/notify/channel/add", methods=["POST"])
@login_required
def notify_channel_add():
    name = request.form.get("name", "").strip()
    channel_type = request.form.get("channel_type", "qq").strip()
    config_str = request.form.get("config", "{}").strip()

    if not name:
        flash("渠道名称不能为空", "error")
        return redirect(url_for("admin.notify_list"))

    try:
        import json
        json.loads(config_str)
    except Exception:
        flash("配置 JSON 格式错误", "error")
        return redirect(url_for("admin.notify_list"))

    prisma = get_prisma()
    prisma.notifychannel.create(data={
        "name": name,
        "channelType": channel_type,
        "config": config_str,
    })
    flash("通知渠道添加成功", "success")
    return redirect(url_for("admin.notify_list"))


@admin_bp.route("/notify/channel/<int:cid>/edit", methods=["POST"])
@login_required
def notify_channel_edit(cid):
    prisma = get_prisma()
    item = prisma.notifychannel.find_unique(where={"id": cid})
    if not item:
        flash("渠道不存在", "error")
        return redirect(url_for("admin.notify_list"))

    name = request.form.get("name", "").strip()
    channel_type = request.form.get("channel_type", "qq").strip()
    config_str = request.form.get("config", "{}").strip()

    if not name:
        flash("渠道名称不能为空", "error")
        return redirect(url_for("admin.notify_list"))

    try:
        import json
        json.loads(config_str)
    except Exception:
        flash("配置 JSON 格式错误", "error")
        return redirect(url_for("admin.notify_list"))

    prisma.notifychannel.update(
        where={"id": cid},
        data={
            "name": name,
            "channelType": channel_type,
            "config": config_str,
        },
    )
    flash("渠道配置已更新", "success")
    return redirect(url_for("admin.notify_list"))


@admin_bp.route("/notify/channel/<int:cid>/toggle", methods=["POST"])
@login_required
def notify_channel_toggle(cid):
    prisma = get_prisma()
    item = prisma.notifychannel.find_unique(where={"id": cid})
    if item:
        prisma.notifychannel.update(where={"id": cid}, data={"enabled": not item.enabled})
    if request.headers.get("X-Requested-With") == "XMLHttpRequest":
        new_state = not item.enabled if item else False
        return jsonify({"ok": True, "enabled": new_state})
    return redirect(url_for("admin.notify_list"))


@admin_bp.route("/notify/channel/<int:cid>/delete", methods=["POST"])
@login_required
def notify_channel_delete(cid):
    prisma = get_prisma()
    prisma.notifymessage.delete_many(where={"channelId": cid})
    prisma.notifychannel.delete(where={"id": cid})
    flash("渠道已删除", "success")
    return redirect(url_for("admin.notify_list"))


@admin_bp.route("/notify/prepare", methods=["POST"])
@login_required
def notify_prepare():
    """预生成待发送消息（不实际发送），可先预览再发送。"""
    from src.notify.engine import prepare_notifications
    stats = prepare_notifications()
    msg = f"预生成完成: 待发送 {stats['prepared']}，跳过 {stats['skipped']}，已存在 {stats['existing']}"
    if stats["errors"]:
        msg += f"\n错误: {'; '.join(stats['errors'][:3])}"
    flash(msg, "success")
    return redirect(url_for("admin.notify_list", status="pending"))


@admin_bp.route("/notify/send", methods=["POST"])
@login_required
def notify_send():
    from src.notify.engine import send_notifications
    stats = send_notifications()
    msg = f"发送完成: 成功 {stats['sent']}，跳过 {stats['skipped']}，失败 {stats['failed']}"
    if stats["errors"]:
        msg += f"\n错误: {'; '.join(stats['errors'][:3])}"
    flash(msg, "success" if stats["failed"] == 0 else "error")
    return redirect(url_for("admin.notify_list"))


@admin_bp.route("/notify/msg/<int:mid>/skip", methods=["POST"])
@login_required
def notify_msg_skip(mid):
    """手动标记某条消息为不发送。"""
    prisma = get_prisma()
    msg = prisma.notifymessage.find_first(where={"id": mid})
    if msg and msg.status in ("pending", "failed"):
        prisma.notifymessage.update(
            where={"id": mid},
            data={"status": "skipped", "skipReason": "manual"},
        )
        flash(f"消息 #{mid} 已标记为不发送", "success")
    else:
        flash("消息不存在或状态不允许此操作", "error")
    return redirect(request.referrer or url_for("admin.notify_list"))


@admin_bp.route("/notify/msg/<int:mid>/unskip", methods=["POST"])
@login_required
def notify_msg_unskip(mid):
    """把跳过的消息恢复为待发送。"""
    prisma = get_prisma()
    msg = prisma.notifymessage.find_first(where={"id": mid})
    if msg and msg.status == "skipped":
        prisma.notifymessage.update(
            where={"id": mid},
            data={"status": "pending", "skipReason": None},
        )
        flash(f"消息 #{mid} 已恢复为待发送", "success")
    else:
        flash("消息不存在或状态不允许此操作", "error")
    return redirect(request.referrer or url_for("admin.notify_list"))


@admin_bp.route("/notify/msg/batch-skip", methods=["POST"])
@login_required
def notify_msg_batch_skip():
    """批量标记消息为不发送。"""
    prisma = get_prisma()
    ids = request.form.getlist("msg_ids", type=int)
    if ids:
        count = prisma.notifymessage.update_many(
            where={"id": {"in": ids}, "status": {"in": ["pending", "failed"]}},
            data={"status": "skipped", "skipReason": "manual"},
        )
        flash(f"已标记 {count} 条消息为不发送", "success")
    return redirect(request.referrer or url_for("admin.notify_list"))


@admin_bp.route("/notify/msg/<int:mid>/retry", methods=["POST"])
@login_required
def notify_msg_retry(mid):
    """把失败的消息恢复为待发送。"""
    prisma = get_prisma()
    msg = prisma.notifymessage.find_first(where={"id": mid})
    if msg and msg.status == "failed":
        prisma.notifymessage.update(
            where={"id": mid},
            data={"status": "pending", "errorMsg": None},
        )
        flash(f"消息 #{mid} 已恢复为待发送", "success")
    else:
        flash("消息不存在或状态不允许此操作", "error")
    return redirect(request.referrer or url_for("admin.notify_list"))


# ==================== 数据标注 ====================

@admin_bp.route("/labeling")
@login_required
def labeling_list():
    prisma = get_prisma()
    page = request.args.get("page", 1, type=int)
    per_page = request.args.get("per_page", 20, type=int)
    per_page = min(max(per_page, 20), 500)
    label_filter = request.args.get("label", "").strip()
    sq = request.args.get("sq", "").strip()
    title_q = request.args.get("title", "").strip()
    url_q = request.args.get("url", "").strip()
    id_q = request.args.get("id", "").strip()
    labeled_by = request.args.get("labeled_by", "").strip()

    where = {}
    if label_filter == "unlabeled":
        where["label"] = None
    elif label_filter == "relevant":
        where["label"] = 1
    elif label_filter == "irrelevant":
        where["label"] = 0
    if sq:
        where["searchQuery"] = {"contains": sq}
    if title_q:
        where["title"] = {"contains": title_q}
    if url_q:
        where["url"] = {"contains": url_q}
    if id_q:
        try:
            where["id"] = int(id_q)
        except ValueError:
            pass
    if labeled_by == "ai":
        where["labeledBy"] = "ai"
    elif labeled_by == "human":
        where["labeledBy"] = "admin"
    elif labeled_by == "none":
        where["labeledBy"] = None

    total = prisma.labeledsample.count(where=where)
    items = prisma.labeledsample.find_many(
        where=where,
        order=[{"label": "desc"}, {"createdAt": "desc"}],
        skip=(page - 1) * per_page,
        take=per_page,
    )
    total_pages = (total + per_page - 1) // per_page

    total_all = prisma.labeledsample.count()
    labeled_count = prisma.labeledsample.count(where={"label": {"not": None}})
    unlabeled_count = total_all - labeled_count
    relevant_count = prisma.labeledsample.count(where={"label": 1})
    irrelevant_count = prisma.labeledsample.count(where={"label": 0})
    ai_count = prisma.labeledsample.count(where={"labeledBy": "ai"})

    import os
    model_exists = False
    try:
        from src.classifier.trainer import MODEL_PATH
        model_exists = os.path.exists(MODEL_PATH)
    except Exception:
        pass

    bert_model_exists = False
    bert_meta = None
    try:
        from src.classifier.bert_trainer import BERT_MODEL_DIR
        import json
        meta_path = os.path.join(BERT_MODEL_DIR, "meta.json")
        if os.path.exists(meta_path):
            bert_model_exists = True
            with open(meta_path) as f:
                bert_meta = json.load(f)
    except Exception:
        pass

    notice_model_exists = False
    notice_meta = None
    try:
        from src.classifier.notice_trainer import NOTICE_MODEL_DIR
        import json
        meta_path = os.path.join(NOTICE_MODEL_DIR, "meta.json")
        if os.path.exists(meta_path):
            notice_model_exists = True
            with open(meta_path) as f:
                notice_meta = json.load(f)
    except Exception:
        pass

    notice_type_count = prisma.parsedresult.count(where={"noticeType": {"not": None}})

    return render_template(
        "admin/labeling.html",
        items=items,
        page=page,
        total=total,
        total_pages=total_pages,
        label_filter=label_filter,
        sq=sq,
        title_q=title_q,
        url_q=url_q,
        id_q=id_q,
        labeled_by=labeled_by,
        total_all=total_all,
        labeled_count=labeled_count,
        unlabeled_count=unlabeled_count,
        relevant_count=relevant_count,
        irrelevant_count=irrelevant_count,
        ai_count=ai_count,
        per_page=per_page,
        model_exists=model_exists,
        bert_model_exists=bert_model_exists,
        bert_meta=bert_meta,
        notice_model_exists=notice_model_exists,
        notice_meta=notice_meta,
        notice_type_count=notice_type_count,
    )


@admin_bp.route("/labeling/import", methods=["POST"])
@login_required
def labeling_import():
    """
    从解析结果导入标注样本。

    支持的 form 参数：
      - limit  最多导入多少条（默认 5000，避免一次撑爆）
      - domain 按域名后缀过滤；如填 "sc.gov.cn" 则匹配 *.sc.gov.cn
      - include_low_quality:
                "1" 表示也导入列表页/搜索引擎页/解析报错；默认不导入

    导入策略：
      1. 跳过已经存在 LabeledSample 的（按 urlHash 去重）
      2. 默认跳过没有意义的内容（noticeType=list_page/search_serp、parseErrors 非空）
      3. 按 createdAt desc 取最新的，先导新数据
    """
    import os
    from pymongo import MongoClient

    prisma = get_prisma()

    # ---- 表单参数 ----
    try:
        limit = int(request.form.get("limit", "5000") or "5000")
    except ValueError:
        limit = 5000
    limit = max(1, min(limit, 100000))  # 0 < limit ≤ 10w，防呆

    domain_filter = (request.form.get("domain", "") or "").strip().lower().lstrip(".")
    include_low_quality = request.form.get("include_low_quality") == "1"

    # ---- 用 SearchResult 找出符合 domain 的 urlHash 集合（如果填了 domain） ----
    domain_hash_set: set[str] | None = None
    if domain_filter:
        # 按后缀匹配：domain 字段以 .domain_filter 结尾或等于 domain_filter
        # 用 contains 做粗筛，再 Python 端做严格后缀匹配（避免 sc.gov.cn 匹中 abcsc.gov.cn）
        candidates = prisma.searchresult.find_many(
            where={"domain": {"contains": domain_filter}},
        )
        domain_hash_set = set()
        for sr in candidates:
            d = (sr.domain or "").lower()
            if d == domain_filter or d.endswith("." + domain_filter):
                if sr.urlHash:
                    domain_hash_set.add(sr.urlHash)
        if not domain_hash_set:
            flash(f"没有找到 domain ≈ {domain_filter} 的 SearchResult", "warning")
            return redirect(url_for("admin.labeling_list"))

    # ---- 候选 ParsedResult 的 where 条件 ----
    pr_where: dict = {}
    if not include_low_quality:
        pr_where["parseErrors"] = None
        pr_where["noticeType"] = {"notIn": ["list_page", "search_serp"]}

    # 已经导入过的 urlHash（一次性拉到内存，避免逐条 find_unique）
    existing_hashes: set[str] = set()
    BATCH = 5000
    offset = 0
    while True:
        chunk = prisma.labeledsample.find_many(skip=offset, take=BATCH)
        if not chunk:
            break
        for ls in chunk:
            if ls.urlHash:
                existing_hashes.add(ls.urlHash)
        if len(chunk) < BATCH:
            break
        offset += BATCH

    # 拉候选 ParsedResult；按最新优先
    parsed_items = prisma.parsedresult.find_many(
        where=pr_where,
        order={"createdAt": "desc"},
        # 多取一些以应对去重后 < limit
        take=min(limit * 3, 100000),
    )

    uri = os.getenv("MONGO_URI", "mongodb://mongodb:mongodb@localhost:27017/shangjibao?authSource=admin")
    mc = MongoClient(uri, serverSelectionTimeoutMS=3000)
    mdb = mc.get_default_database()
    raw_pages = mdb["raw_pages"]

    imported = 0
    skipped_existing = 0
    skipped_low_quality = 0
    skipped_domain = 0

    clean = lambda s: s.replace("\x00", "") if s else s

    for p in parsed_items:
        if imported >= limit:
            break
        if p.urlHash in existing_hashes:
            skipped_existing += 1
            continue
        if domain_hash_set is not None and p.urlHash not in domain_hash_set:
            skipped_domain += 1
            continue

        content = None
        if p.mongoDocId:
            try:
                from bson import ObjectId
                doc = raw_pages.find_one({"_id": ObjectId(p.mongoDocId)})
                if doc and doc.get("html"):
                    from src.parser.extractors import html_to_text
                    content = html_to_text(doc["html"])[:3000]
            except Exception:
                content = None
        if not content:
            content = p.summary or p.title or ""

        try:
            prisma.labeledsample.create(data={
                "parsedId": p.id,
                "url": p.url,
                "urlHash": p.urlHash,
                "title": clean(p.title),
                "content": clean(content),
                "searchQuery": clean(p.searchQuery),
                "sourceName": clean(p.sourceName),
            })
            imported += 1
            existing_hashes.add(p.urlHash)
        except Exception:
            skipped_existing += 1

    mc.close()

    parts = [f"新增 {imported} 条"]
    if skipped_existing:
        parts.append(f"跳过 {skipped_existing} 条已存在")
    if skipped_domain:
        parts.append(f"跳过 {skipped_domain} 条非目标域名")
    if domain_filter:
        parts.append(f"domain≈{domain_filter}")
    if include_low_quality:
        parts.append("含低质量")
    flash("导入完成: " + ", ".join(parts), "success")
    return redirect(url_for("admin.labeling_list"))


@admin_bp.route("/labeling/domains")
@login_required
def labeling_domains():
    """
    返回当前 SearchResult 中出现过的根域名（去掉 www. 前缀的 host），
    给标注页面的"按域名导入"下拉用。
    JSON 响应： {"domains": [{"domain": "sc.gov.cn", "count": 1234}, ...]}
    """
    prisma = get_prisma()
    rows = prisma.query_raw(
        """
        SELECT
            COALESCE(domain, '') AS domain,
            COUNT(*) AS count
        FROM search_results
        WHERE domain IS NOT NULL AND domain <> ''
        GROUP BY domain
        ORDER BY COUNT(*) DESC
        LIMIT 200
        """
    )
    return jsonify({"domains": [{"domain": r["domain"], "count": int(r["count"])} for r in rows]})


@admin_bp.route("/labeling/<int:sid>/label", methods=["POST"])
@login_required
def labeling_set_label(sid):
    from datetime import datetime, timezone
    data = request.get_json(silent=True) or {}
    label_val = data.get("label")
    if label_val not in (0, 1):
        return jsonify({"ok": False, "error": "无效标注值"}), 400

    prisma = get_prisma()
    prisma.labeledsample.update(
        where={"id": sid},
        data={
            "label": label_val,
            "labeledBy": "admin",
            "labeledAt": datetime.now(timezone.utc),
        },
    )
    return jsonify({"ok": True, "label": label_val})


@admin_bp.route("/labeling/<int:sid>/delete", methods=["POST"])
@login_required
def labeling_delete(sid):
    prisma = get_prisma()
    try:
        prisma.labeledsample.delete(where={"id": sid})
    except Exception:
        pass
    if request.headers.get("X-Requested-With") == "XMLHttpRequest":
        return jsonify({"ok": True})
    flash("已删除", "success")
    return redirect(url_for("admin.labeling_list"))


@admin_bp.route("/labeling/batch-delete", methods=["POST"])
@login_required
def labeling_batch_delete():
    data = request.get_json(silent=True) or {}
    ids = data.get("ids", [])
    if not ids:
        return jsonify({"ok": False, "error": "未选择任何记录"}), 400
    prisma = get_prisma()
    deleted = 0
    for sid in ids:
        try:
            prisma.labeledsample.delete(where={"id": int(sid)})
            deleted += 1
        except Exception:
            pass
    return jsonify({"ok": True, "deleted": deleted})


@admin_bp.route("/labeling/batch-label", methods=["POST"])
@login_required
def labeling_batch_label():
    from datetime import datetime, timezone
    data = request.get_json(silent=True) or {}
    ids = data.get("ids", [])
    label_val = data.get("label")
    if label_val not in (0, 1):
        return jsonify({"ok": False, "error": "无效标注值"}), 400
    if not ids:
        return jsonify({"ok": False, "error": "未选择任何记录"}), 400
    prisma = get_prisma()
    updated = 0
    for sid in ids:
        try:
            prisma.labeledsample.update(
                where={"id": int(sid)},
                data={
                    "label": label_val,
                    "labeledBy": "admin",
                    "labeledAt": datetime.now(timezone.utc),
                },
            )
            updated += 1
        except Exception:
            pass
    return jsonify({"ok": True, "updated": updated, "label": label_val})


@admin_bp.route("/labeling/train", methods=["POST"])
@login_required
def labeling_train():
    from src.classifier.trainer import train_model
    prisma = get_prisma()
    result = train_model(prisma)
    if result.get("success"):
        flash(
            f"FastText 模型训练完成！样本 {result['samples']} 条（相关 {result['relevant']} / 不相关 {result['irrelevant']}），"
            f"精确率 {result['precision']}，召回率 {result['recall']}",
            "success",
        )
    else:
        flash(f"训练失败: {result.get('error', '未知错误')}", "error")
    return redirect(url_for("admin.labeling_list"))


@admin_bp.route("/labeling/train-bert", methods=["POST"])
@login_required
def labeling_train_bert():
    import os
    from src.classifier.bert_trainer import start_training_async, get_training_status
    status = get_training_status()
    if status["running"]:
        return jsonify({"ok": False, "error": "BERT 训练正在进行中，请等待完成"})
    db_url = os.environ.get("DATABASE_URL", "")
    started = start_training_async(db_url)
    if not started:
        return jsonify({"ok": False, "error": "无法启动训练"})
    return jsonify({"ok": True, "message": "BERT 训练已启动"})


@admin_bp.route("/labeling/train-bert-status", methods=["GET"])
@login_required
def labeling_train_bert_status():
    from src.classifier.bert_trainer import get_training_status
    status = get_training_status()
    return jsonify(status)


@admin_bp.route("/labeling/train-notice", methods=["POST"])
@login_required
def labeling_train_notice():
    import os
    from src.classifier.notice_trainer import start_training_async, get_training_status
    status = get_training_status()
    if status["running"]:
        return jsonify({"ok": False, "error": "公告类型训练正在进行中，请等待完成"})
    db_url = os.environ.get("DATABASE_URL", "")
    started = start_training_async(db_url)
    if not started:
        return jsonify({"ok": False, "error": "无法启动训练"})
    return jsonify({"ok": True, "message": "公告类型训练已启动"})


@admin_bp.route("/labeling/train-notice-status", methods=["GET"])
@login_required
def labeling_train_notice_status():
    from src.classifier.notice_trainer import get_training_status
    status = get_training_status()
    return jsonify(status)


@admin_bp.route("/labeling/ai-predict-one", methods=["POST"])
@login_required
def labeling_ai_predict_one():
    """AI 预标注单条记录"""
    from datetime import datetime, timezone
    from src.llm.client import predict_label

    data = request.get_json(silent=True) or {}
    sid = data.get("id")
    if not sid:
        return jsonify({"ok": False, "error": "缺少 id"}), 400

    prisma = get_prisma()
    sample = prisma.labeledsample.find_unique(where={"id": int(sid)})
    if not sample:
        return jsonify({"ok": False, "error": "记录不存在"}), 404

    result = predict_label(
        title=sample.title or "",
        content=sample.content or "",
        search_query=sample.searchQuery or "",
    )
    if not result or result["label"] not in (0, 1):
        return jsonify({"ok": False, "error": "AI 判断失败，请检查大模型配置"}), 500

    prisma.labeledsample.update(
        where={"id": int(sid)},
        data={
            "label": result["label"],
            "labeledBy": "ai",
            "labeledAt": datetime.now(timezone.utc),
        },
    )
    return jsonify({"ok": True, "id": int(sid), "label": result["label"], "reason": result.get("reason", "")})


@admin_bp.route("/labeling/ambiguous", methods=["GET"])
@login_required
def labeling_ambiguous():
    """按 BERT 预测分数取最摇摆（接近 0.5）的未标注样本。

    Query params:
        scan_size   候选池大小，从未标注样本中拉取的最大条数，默认 500
        take        返回前 N 条最摇摆样本，默认 50
        low         分数下界（含），默认 0.3
        high        分数上界（含），默认 0.7
        search_query/title/url   可选过滤
    """
    from src.classifier.bert_predictor import BertRelevancePredictor

    prisma = get_prisma()

    try:
        scan_size = int(request.args.get("scan_size", "500"))
    except ValueError:
        scan_size = 500
    try:
        take = int(request.args.get("take", "50"))
    except ValueError:
        take = 50
    try:
        low = float(request.args.get("low", "0.3"))
    except ValueError:
        low = 0.3
    try:
        high = float(request.args.get("high", "0.7"))
    except ValueError:
        high = 0.7
    scan_size = max(20, min(scan_size, 5000))
    take = max(5, min(take, 500))

    predictor = BertRelevancePredictor.get_instance()
    if not predictor.available:
        return jsonify({
            "ok": False,
            "error": "BERT 模型未训练或加载失败，请先训练 BERT",
        }), 400

    where: dict = {"label": None}
    sq = request.args.get("sq", "").strip()
    title_q = request.args.get("title", "").strip()
    url_q = request.args.get("url", "").strip()
    if sq:
        where["searchQuery"] = {"contains": sq}
    if title_q:
        where["title"] = {"contains": title_q}
    if url_q:
        where["url"] = {"contains": url_q}

    candidates = prisma.labeledsample.find_many(
        where=where,
        order={"createdAt": "desc"},
        take=scan_size,
    )
    if not candidates:
        return jsonify({
            "ok": True, "items": [], "scanned": 0, "in_range": 0,
            "scan_size": scan_size, "take": take, "low": low, "high": high,
        })

    texts = [
        ((s.title or "") + " [SEP] " + (s.content or "")[:500]).strip(" [SEP] ")
        for s in candidates
    ]
    titles = [s.title or "" for s in candidates]
    preds = predictor.predict_batch(texts, titles, batch_size=64)

    scored: list[tuple[float, object]] = []
    for s, p in zip(candidates, preds):
        if p is None:
            continue
        score = float(p["score"])
        if low <= score <= high:
            scored.append((score, s))

    # 按"摇摆程度"排序：|score - 0.5| 升序，越接近 0.5 越靠前
    scored.sort(key=lambda x: abs(x[0] - 0.5))
    top = scored[:take]

    items_out = []
    for score, s in top:
        items_out.append({
            "id": int(s.id),
            "title": s.title or "",
            "url": s.url,
            "search_query": s.searchQuery or "",
            "source_name": s.sourceName or "",
            "content_preview": (s.content or "")[:500],
            "bert_score": round(score, 4),
            "labeled_by": s.labeledBy or "",
        })

    return jsonify({
        "ok": True,
        "items": items_out,
        "scanned": len(candidates),
        "in_range": len(scored),
        "scan_size": scan_size,
        "take": take,
        "low": low,
        "high": high,
    })


# ==================== 大模型配置 ====================

@admin_bp.route("/llm-config")
@login_required
def llm_config():
    prisma = get_prisma()
    configs = prisma.llmconfig.find_many(order={"createdAt": "desc"})
    prompts = prisma.labelingprompt.find_many(order={"createdAt": "desc"})

    from src.llm.client import DEFAULT_LABELING_PROMPT
    return render_template(
        "admin/llm_config.html",
        configs=configs,
        prompts=prompts,
        default_prompt=DEFAULT_LABELING_PROMPT,
    )


@admin_bp.route("/llm-config/save", methods=["POST"])
@login_required
def llm_config_save():
    prisma = get_prisma()
    config_id = request.form.get("config_id", "").strip()
    provider = request.form.get("provider", "deepseek").strip()
    api_key = request.form.get("api_key", "").strip()
    base_url = request.form.get("base_url", "").strip()
    model = request.form.get("model", "deepseek-chat").strip()
    temperature = float(request.form.get("temperature", "0.1"))
    max_tokens = int(request.form.get("max_tokens", "200"))
    enabled = request.form.get("enabled") == "on"

    if not api_key or not base_url:
        flash("API Key 和 Base URL 不能为空", "error")
        return redirect(url_for("admin.llm_config"))

    data = {
        "provider": provider,
        "apiKey": api_key,
        "baseUrl": base_url,
        "model": model,
        "temperature": temperature,
        "maxTokens": max_tokens,
        "enabled": enabled,
    }

    if config_id:
        prisma.llmconfig.update(where={"id": int(config_id)}, data=data)
        flash("配置已更新", "success")
    else:
        prisma.llmconfig.create(data=data)
        flash("配置已创建", "success")

    return redirect(url_for("admin.llm_config"))


@admin_bp.route("/llm-config/<int:cid>/delete", methods=["POST"])
@login_required
def llm_config_delete(cid):
    prisma = get_prisma()
    try:
        prisma.llmconfig.delete(where={"id": cid})
        flash("配置已删除", "success")
    except Exception:
        flash("删除失败", "error")
    return redirect(url_for("admin.llm_config"))


@admin_bp.route("/llm-config/prompt/save", methods=["POST"])
@login_required
def llm_prompt_save():
    prisma = get_prisma()
    prompt_id = request.form.get("prompt_id", "").strip()
    name = request.form.get("name", "").strip()
    prompt = request.form.get("prompt", "").strip()
    is_default = request.form.get("is_default") == "on"

    if not name or not prompt:
        flash("名称和提示词不能为空", "error")
        return redirect(url_for("admin.llm_config"))

    if is_default:
        prisma.labelingprompt.update_many(
            where={"isDefault": True},
            data={"isDefault": False},
        )

    data = {"name": name, "prompt": prompt, "isDefault": is_default}

    if prompt_id:
        prisma.labelingprompt.update(where={"id": int(prompt_id)}, data=data)
        flash("提示词已更新", "success")
    else:
        prisma.labelingprompt.create(data=data)
        flash("提示词已创建", "success")

    return redirect(url_for("admin.llm_config"))


@admin_bp.route("/llm-config/prompt/<int:pid>/delete", methods=["POST"])
@login_required
def llm_prompt_delete(pid):
    prisma = get_prisma()
    try:
        prisma.labelingprompt.delete(where={"id": pid})
        flash("提示词已删除", "success")
    except Exception:
        flash("删除失败", "error")
    return redirect(url_for("admin.llm_config"))


@admin_bp.route("/llm-config/test", methods=["POST"])
@login_required
def llm_config_test():
    """测试 LLM 连接是否正常"""
    from src.llm.client import predict_label
    result = predict_label(
        title="弱电智能化系统采购项目竞争性磋商公告",
        content="XX市人民医院弱电智能化系统采购项目，采用竞争性磋商方式采购，预算金额200万元。采购内容包括综合布线、安防监控、楼宇自控等弱电系统。",
        search_query="弱电 招标",
    )
    if result:
        return jsonify({"ok": True, "result": result})
    return jsonify({"ok": False, "error": "调用失败，请检查配置"}), 400


def _reload_schedules():
    try:
        from src.scheduler.scheduler import get_scheduler, sync_schedules
        scheduler = get_scheduler()
        if not scheduler.running:
            scheduler.start()
        sync_schedules()
    except Exception as e:
        import logging
        logging.getLogger(__name__).error("_reload_schedules failed: %s", e, exc_info=True)
