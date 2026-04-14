from functools import wraps

from flask import Blueprint, render_template, request, redirect, url_for, session, flash, jsonify

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

    where = {}
    if domain_filter:
        where["domain"] = {"contains": domain_filter}
    if keyword_filter:
        where["title"] = {"contains": keyword_filter}
    if query_filter:
        where["searchQuery"] = {"contains": query_filter}

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
        source_search_map=source_search_map,
    )


# ==================== 调度配置 ====================

@admin_bp.route("/schedules")
@login_required
def schedules():
    prisma = get_prisma()
    items = prisma.crawlschedule.find_many(order={"id": "desc"})
    return render_template("admin/schedules.html", schedules=items)


@admin_bp.route("/schedules/add", methods=["POST"])
@login_required
def schedule_add():
    name = request.form.get("name", "").strip()
    schedule_type = request.form.get("schedule_type", "daily")
    times_per_day = int(request.form.get("times_per_day", "1") or "1")
    start_hour = int(request.form.get("start_hour", "2") or "2")
    start_minute = int(request.form.get("start_minute", "0") or "0")
    weekdays = request.form.get("weekdays", "").strip() or None

    if name:
        prisma = get_prisma()
        prisma.crawlschedule.create(data={
            "name": name,
            "scheduleType": schedule_type,
            "timesPerDay": times_per_day,
            "startHour": start_hour,
            "startMinute": start_minute,
            "weekdays": weekdays,
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
    prisma = get_prisma()
    page = request.args.get("page", 1, type=int)
    per_page = min(request.args.get("per_page", 20, type=int), 500)
    q = request.args.get("q", "").strip()
    bidder_q = request.args.get("bidder", "").strip()
    location_q = request.args.get("location", "").strip()
    relevant_q = request.args.get("relevant", "").strip()
    notice_type_q = request.args.get("notice_type", "").strip()
    sort_by = request.args.get("sort", "createdAt")
    sort_dir = request.args.get("dir", "desc")

    where = {}
    if q:
        where["title"] = {"contains": q}
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

    allowed_sorts = ["createdAt", "publishDate", "amountValue", "relevanceScore", "title", "bidder", "location", "noticeType"]
    if sort_by not in allowed_sorts:
        sort_by = "createdAt"
    order_dir = "asc" if sort_dir == "asc" else "desc"

    total = prisma.parsedresult.count(where=where)
    items = prisma.parsedresult.find_many(
        where=where,
        order={sort_by: order_dir},
        skip=(page - 1) * per_page,
        take=per_page,
    )
    total_pages = (total + per_page - 1) // per_page

    parsed_total = prisma.parsedresult.count()
    mongo_total = 0
    try:
        import os
        from pymongo import MongoClient
        uri = os.getenv("MONGO_URI", "mongodb://mongodb:mongodb@localhost:27017/shangjibao?authSource=admin")
        mc = MongoClient(uri, serverSelectionTimeoutMS=2000)
        mdb = mc.get_default_database()
        mongo_total = mdb["raw_pages"].count_documents(
            {"meta.source_type": {"$in": ["search_result", "website"]}}
        )
        mc.close()
    except Exception:
        pass
    unparsed = max(0, mongo_total - parsed_total)

    import os as _os
    model_path = _os.path.join(
        _os.getenv("MODEL_DIR", _os.path.join(_os.path.dirname(__file__), "..", "..", "classifier", "data", "models")),
        "relevance_model.bin",
    )
    fasttext_exists = _os.path.exists(model_path)

    bert_model_exists = False
    try:
        from src.classifier.bert_trainer import BERT_MODEL_DIR
        bert_model_exists = _os.path.exists(_os.path.join(BERT_MODEL_DIR, "config.json"))
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
        relevant_q=relevant_q,
        notice_type_q=notice_type_q,
        sort_by=sort_by,
        sort_dir=sort_dir,
        parsed_total=parsed_total,
        mongo_total=mongo_total,
        unparsed=unparsed,
        model_exists=model_exists,
        bert_model_exists=bert_model_exists,
    )


@admin_bp.route("/parsed/start", methods=["POST"])
@login_required
def parse_start():
    from src.parser.engine import start_parse_job
    job_id = start_parse_job()
    flash(f"解析任务已启动，任务 ID: {job_id}", "success")
    return redirect(url_for("admin.parsed_list"))


@admin_bp.route("/parsed/rejudge", methods=["POST"])
@login_required
def parse_rejudge():
    from src.parser.engine import start_relevance_rejudge
    job_id = start_relevance_rejudge()
    flash(f"相关性重新判定任务已启动（使用最新模型），任务 ID: {job_id}", "success")
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
    data = request.get_json(silent=True) or {}
    ids = data.get("ids", [])
    if not ids:
        return jsonify({"ok": False, "error": "未选择任何记录"}), 400
    prisma = get_prisma()
    deleted = 0
    for pid in ids:
        try:
            item = prisma.parsedresult.find_unique(where={"id": int(pid)})
            if item:
                prisma.notifymessage.delete_many(where={"urlHash": item.urlHash})
                prisma.labeledsample.delete_many(where={"urlHash": item.urlHash})
                prisma.parsedresult.delete(where={"id": int(pid)})
                deleted += 1
        except Exception:
            pass
    return jsonify({"ok": True, "deleted": deleted})


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

    prisma.notifyconfig.update(
        where={"id": cfg.id},
        data={
            "filterDays": filter_days,
            "filterFuture": filter_future,
            "filterRegion": filter_region,
            "onlyRelevant": only_relevant,
            "excludeTypes": exclude_types or None,
            "titleBlacklist": title_blacklist or None,
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
    import hashlib
    import os
    from pymongo import MongoClient

    prisma = get_prisma()
    parsed_items = prisma.parsedresult.find_many(take=2000)

    uri = os.getenv("MONGO_URI", "mongodb://mongodb:mongodb@localhost:27017/shangjibao?authSource=admin")
    mc = MongoClient(uri, serverSelectionTimeoutMS=3000)
    mdb = mc.get_default_database()
    raw_pages = mdb["raw_pages"]

    imported = 0
    skipped = 0
    for p in parsed_items:
        existing = prisma.labeledsample.find_unique(where={"urlHash": p.urlHash})
        if existing:
            skipped += 1
            continue

        content = None
        if p.mongoDocId:
            from bson import ObjectId
            doc = raw_pages.find_one({"_id": ObjectId(p.mongoDocId)})
            if doc and doc.get("html"):
                from src.parser.extractors import html_to_text
                content = html_to_text(doc["html"])[:3000]
        if not content:
            content = p.summary or p.title or ""

        clean = lambda s: s.replace("\x00", "") if s else s

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
        except Exception:
            skipped += 1

    mc.close()
    flash(f"导入完成: 新增 {imported} 条, 跳过 {skipped} 条已存在的", "success")
    return redirect(url_for("admin.labeling_list"))


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
        from src.scheduler.scheduler import sync_schedules
        sync_schedules()
    except Exception:
        pass
