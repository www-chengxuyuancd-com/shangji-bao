"""
解析引擎。

从 MongoDB 读取原始 HTML，用可配置的提取器提取各字段，
结果存入 PostgreSQL parsed_results 表。
"""
import hashlib
import logging
import os
import multiprocessing
from datetime import datetime, timezone

from prisma import Prisma
from pymongo import MongoClient

from src.parser.base import PARSER_VERSION
from src.parser.extractors import (
    html_to_text,
    is_valid_content,
    detect_listing_page,
    is_search_engine_url,
    PublishDateExtractor,
    BidderExtractor,
    LocationExtractor,
    BidStartTimeExtractor,
    BidEndTimeExtractor,
    AmountExtractor,
    ContactExtractor,
    TitleExtractor,
    SummaryExtractor,
    NoticeTypeExtractor,
    RelevanceExtractor,
)

logger = logging.getLogger(__name__)

EXTRACTORS = {
    "publish_date": PublishDateExtractor(),
    "bidder": BidderExtractor(),
    "location": LocationExtractor(),
    "bid_start_time": BidStartTimeExtractor(),
    "bid_end_time": BidEndTimeExtractor(),
    "amount": AmountExtractor(),
    "contact": ContactExtractor(),
    "title": TitleExtractor(),
    "summary": SummaryExtractor(),
    "notice_type": NoticeTypeExtractor(),
    "relevance": RelevanceExtractor(),
}


def parse_one(html: str, url: str, context: dict) -> dict:
    """对一份 HTML 执行全字段提取，返回结构化 dict。"""
    text = html_to_text(html)
    result = {}
    errors = []

    # 最先短路：搜索引擎结果页（百度/Bing/Google 等）只是发现链接的中间页，
    # 不该被当业务详情解析，更不该参与相关性评分。
    if is_search_engine_url(url):
        result["_listing"] = True
        result["_listing_info"] = {"rule": "search_serp"}
        result["_search_serp"] = True
        errors.append(
            "search_serp: 搜索引擎结果页（如百度/Bing/Google），无业务正文内容，已自动判定为不相关"
        )
        result["_errors"] = errors
        return result

    if not is_valid_content(text):
        result["_invalid"] = True
        errors.append("content_invalid: 页面内容无效（JS/CSS/反爬页面），无有效中文内容")
        result["_errors"] = errors
        return result

    listing_info = detect_listing_page(html, text)
    if listing_info is not None:
        result["_listing"] = True
        result["_listing_info"] = listing_info
        errors.append(
            "listing_page: 命中列表聚合页特征（"
            f"链接条目={listing_info['text_links']}, 含日期条目={listing_info['dated_links']}, "
            f"占比={listing_info['ratio']}, 命中规则={listing_info.get('rule', '-')}）"
            "，已自动判定为不相关"
        )
        result["_errors"] = errors
        return result

    for field_name, extractor in EXTRACTORS.items():
        try:
            val = extractor.extract(text, html, context)
            result[field_name] = val
        except Exception as e:
            errors.append(f"{field_name}: {e}")
            result[field_name] = None

    result["_errors"] = errors
    return result


def _maybe_log_parse_progress(done, total, parsed_count, error_count, last_log_at, lg):
    """打一行进度日志（包含 ETA / 速率），不让 UI 看着像卡死。"""
    import time as _t
    now = _t.time()
    pct = (done * 100.0 / total) if total else 0.0
    lg.info(
        "[parse-job] 进度 %d/%d (%.1f%%) 已写入=%d 错误=%d",
        done, total, pct, parsed_count, error_count,
    )


def _run_parse_job(job_id: int, mode: str = "unparsed_and_errors"):
    """
    在子进程中执行的解析任务。

    mode 控制处理范围：
      - "unparsed_only"  ：只解析还没有 ParsedResult 的 url（包括 ok 和 error 的都跳过）
      - "errors_only"    ：只重新处理 parseErrors 非空的 url
      - "all"            ：重新解析所有 raw_pages（已成功的也会被覆盖）
      - "unparsed_and_errors"（默认，兼容旧行为）：跳过已成功的，处理未解析+报错的
    """
    import time as _time

    prisma = Prisma()
    prisma.connect()

    uri = os.getenv("MONGO_URI", "mongodb://mongodb:mongodb@localhost:27017/shangjibao?authSource=admin")
    mongo_client = MongoClient(uri)
    db = mongo_client.get_default_database()
    raw_pages = db["raw_pages"]

    try:
        prisma.crawljob.update(
            where={"id": job_id},
            data={"status": "running", "startedAt": datetime.now(timezone.utc)},
        )

        user_keywords = [kw.keyword for kw in prisma.searchkeyword.find_many(where={"enabled": True})]

        # 把 ParsedResult 的 urlHash 索引拉到内存里：
        #   - existing_ok_hashes: 已成功解析的（parseErrors=null）
        #   - existing_err_map:   解析报错的 urlHash -> id（重试时 update 用）
        # 一次 SQL ≈ 200ms；以前每条都做 find_unique = N×1ms RPC，跑 2 万条要 20s+。
        # 更重要的是：跳过的页面甚至不需要从 mongo 读取 HTML（5GB+ 的 raw_pages
        # 只读取真正要解析的那几千条 HTML），内存与 IO 大幅下降。
        t0 = _time.time()
        existing_ok_hashes: set[str] = set()
        existing_err_map: dict[str, int] = {}
        BATCH = 5000
        offset = 0
        while True:
            chunk = prisma.parsedresult.find_many(skip=offset, take=BATCH)
            if not chunk:
                break
            for pr in chunk:
                if pr.parseErrors:
                    existing_err_map[pr.urlHash] = pr.id
                else:
                    existing_ok_hashes.add(pr.urlHash)
            if len(chunk) < BATCH:
                break
            offset += BATCH
        logger.info(
            "[parse-job] mode=%s 预加载 ParsedResult 索引: 已成功 %d，失败可重试 %d (耗时 %.1fs)",
            mode, len(existing_ok_hashes), len(existing_err_map), _time.time() - t0,
        )

        # 用 mongo 游标流式扫描 raw_pages，绝不一次性 list 出来。
        # raw_pages 总体积 5GB+ 一次性加载会让客户机直接 swap。
        total = raw_pages.count_documents({})
        prisma.crawljob.update(
            where={"id": job_id},
            data={"totalPages": total},
        )
        logger.info("[parse-job] 启动：raw_pages 总数 %d，mode=%s", total, mode)

        done = 0
        parsed_count = 0
        error_count = 0
        skipped_count = 0
        error_logs = []
        last_log_at = _time.time()
        last_progress_db_update_at = _time.time()

        def _flush_progress():
            """定期把进度写回 PG，让 UI 看得到节奏。"""
            try:
                prisma.crawljob.update(
                    where={"id": job_id},
                    data={
                        "donePages": done,
                        "resultCount": parsed_count,
                        "errorCount": error_count,
                    },
                )
            except Exception as _pe:
                logger.warning("[parse-job] 写进度失败: %s", _pe)

        cursor = raw_pages.find(
            {},
            {"_id": 1, "url": 1, "html": 1, "search_query": 1, "source_name": 1, "meta": 1},
            batch_size=50,
            no_cursor_timeout=True,
        )
        try:
            for doc in cursor:
                url = doc.get("url", "")
                url_hash = hashlib.md5(url.encode("utf-8")).hexdigest()

                # 跳过搜索引擎自身的结果页（Bing/百度 等的 /search?q=… 页面）。
                # 这些是 _crawl_one_query 顺手存进 raw_pages 的，不属于业务详情页。
                # 之前没跳：每次解析都会落一行 parseErrors='search_serp:…' 到 ParsedResult，
                # 客户机上累积了 18 万条孤儿（SearchResult 里没有它们的 hash）。
                src_type = (doc.get("meta") or {}).get("source_type") or ""
                if src_type == "search_engine":
                    done += 1
                    skipped_count += 1
                    if (
                        done % 500 == 0
                        or _time.time() - last_progress_db_update_at >= 2.0
                    ):
                        _flush_progress()
                        last_progress_db_update_at = _time.time()
                    continue

                in_ok = url_hash in existing_ok_hashes
                in_err = url_hash in existing_err_map

                # 根据 mode 决定本条跳过与否
                skip = False
                if mode == "unparsed_only":
                    # 已经有任何 ParsedResult（不论成功失败）都跳过
                    if in_ok or in_err:
                        skip = True
                elif mode == "errors_only":
                    # 只处理报错的
                    if not in_err:
                        skip = True
                elif mode == "all":
                    pass  # 全部处理
                else:  # unparsed_and_errors（默认）
                    if in_ok:
                        skip = True

                if skip:
                    done += 1
                    skipped_count += 1
                    # ★ 关键修复：跳过分支里也要定期把 done 回写到 PG，
                    # 否则前面 N 万条 ok 记录扫过去时 UI 永远停留在 0/0
                    if (
                        done % 500 == 0
                        or _time.time() - last_progress_db_update_at >= 2.0
                    ):
                        _flush_progress()
                        last_progress_db_update_at = _time.time()
                    if _time.time() - last_log_at >= 5.0:
                        _maybe_log_parse_progress(
                            done, total, parsed_count, error_count, last_log_at, logger,
                        )
                        last_log_at = _time.time()
                    continue

                # 走到这里说明这条要"做工"：决定是 update（已存在）还是 create
                # mode=all 时已成功的也会落到这里，需要按已存在 update 而不是 create
                if in_err:
                    existing_id = existing_err_map.get(url_hash)
                else:
                    existing_id = None  # ok 路径在 all 模式下，需要回查 id 来 update

                class _Stub:
                    def __init__(self, pid): self.id = pid
                existing = _Stub(existing_id) if existing_id else None

                # mode=all 且 in_ok：需要查一次拿到旧记录的 id，update 覆盖
                if existing is None and in_ok:
                    pr_old = prisma.parsedresult.find_unique(where={"urlHash": url_hash})
                    if pr_old:
                        existing = _Stub(pr_old.id)

                html = doc.get("html", "")
                if not html or len(html) < 50:
                    done += 1
                    continue

                meta = doc.get("meta", {})
                context = {
                    "url": url,
                    "title": meta.get("title", ""),
                    "search_query": doc.get("search_query", ""),
                    "source_name": doc.get("source_name", ""),
                    "user_keywords": user_keywords,
                }

                try:
                    result = parse_one(html, url, context)

                    def _write(data: dict):
                        """create 或 update。"""
                        if existing is not None:
                            update_data = {k: v for k, v in data.items()
                                           if k not in ("urlHash", "createdBy")}
                            prisma.parsedresult.update(
                                where={"id": existing.id},
                                data=update_data,
                            )
                        else:
                            prisma.parsedresult.create(data=data)

                    if result.get("_invalid"):
                        _write({
                            "url": url,
                            "urlHash": url_hash,
                            "mongoDocId": str(doc["_id"]),
                            "title": context.get("title") or meta.get("title"),
                            "searchQuery": context.get("search_query"),
                            "sourceName": context.get("source_name"),
                            "isRelevant": False,
                            "parserVersion": PARSER_VERSION,
                            "parseErrors": "\n".join(result.get("_errors", [])),
                            "createdBy": "system",
                        })
                        error_count += 1
                        error_logs.append(f"[{url[:60]}] 内容无效(JS/反爬)")
                        done += 1
                        continue

                    if result.get("_listing"):
                        _write({
                            "url": url,
                            "urlHash": url_hash,
                            "mongoDocId": str(doc["_id"]),
                            "title": context.get("title") or meta.get("title"),
                            "searchQuery": context.get("search_query"),
                            "sourceName": context.get("source_name"),
                            "noticeType": "search_serp" if result.get("_search_serp") else "list_page",
                            "isRelevant": False,
                            "relevanceScore": 0.0,
                            "parserVersion": PARSER_VERSION,
                            "parseErrors": "\n".join(result.get("_errors", [])),
                            "createdBy": "system",
                        })
                        parsed_count += 1
                        done += 1
                        continue

                    amount_data = result.get("amount")
                    amount_display = None
                    amount_value = None
                    if isinstance(amount_data, dict):
                        amount_display = amount_data.get("display")
                        amount_value = amount_data.get("value")

                    relevance_data = result.get("relevance", {}) or {}

                    _write({
                        "url": url,
                        "urlHash": url_hash,
                        "mongoDocId": str(doc["_id"]),
                        "title": result.get("title"),
                        "summary": result.get("summary"),
                        "publishDate": result.get("publish_date"),
                        "bidder": result.get("bidder"),
                        "location": result.get("location"),
                        "bidStartTime": result.get("bid_start_time"),
                        "bidEndTime": result.get("bid_end_time"),
                        "amount": amount_display,
                        "amountValue": amount_value,
                        "contact": result.get("contact"),
                        "searchQuery": context.get("search_query"),
                        "sourceName": context.get("source_name"),
                        "noticeType": result.get("notice_type"),
                        "isRelevant": relevance_data.get("is_relevant"),
                        "relevanceScore": relevance_data.get("score"),
                        "matchedKeywords": relevance_data.get("matched"),
                        "parserVersion": PARSER_VERSION,
                        "parseErrors": "\n".join(result.get("_errors", [])) or None,
                        "createdBy": "system",
                    })
                    parsed_count += 1

                except Exception as e:
                    error_count += 1
                    msg = f"[{url[:80]}] {e}"
                    error_logs.append(msg)
                    logger.warning("Parse error: %s", msg)

                done += 1
                # 进度回写：每 20 条 OR 距上次写超过 2s 就 flush 一次
                if done % 20 == 0 or _time.time() - last_progress_db_update_at >= 2.0:
                    _flush_progress()
                    last_progress_db_update_at = _time.time()
                if _time.time() - last_log_at >= 5.0:
                    _maybe_log_parse_progress(
                        done, total, parsed_count, error_count, last_log_at, logger,
                    )
                    last_log_at = _time.time()
        finally:
            cursor.close()
            # 收尾再写一次最终进度
            _flush_progress()
            logger.info(
                "[parse-job] 完成：扫描 %d/%d, 跳过 %d, 写入 %d, 报错 %d",
                done, total, skipped_count, parsed_count, error_count,
            )

        prisma.crawljob.update(
            where={"id": job_id},
            data={
                "status": "completed",
                "finishedAt": datetime.now(timezone.utc),
                "totalPages": total,
                "donePages": done,
                "resultCount": parsed_count,
                "errorCount": error_count,
                "errorLog": "\n".join(error_logs[-50:]) if error_logs else None,
            },
        )

        try:
            from src.notify.engine import prepare_notifications
            prep = prepare_notifications(prisma)
            logger.info("Auto-prepare after parse: prepared=%d skipped=%d", prep["prepared"], prep["skipped"])
        except Exception as pe:
            logger.warning("Auto-prepare notifications failed: %s", pe)

    except Exception as e:
        logger.error("Parse job %d failed: %s", job_id, e)
        prisma.crawljob.update(
            where={"id": job_id},
            data={
                "status": "failed",
                "finishedAt": datetime.now(timezone.utc),
                "errorLog": str(e),
            },
        )
    finally:
        mongo_client.close()
        prisma.disconnect()


def start_parse_job(mode: str = "unparsed_and_errors") -> int:
    """
    创建并启动一个解析任务，返回 job_id。

    mode:
      - unparsed_only       仅解析未解析的（跳过已有 ParsedResult 的所有 url）
      - errors_only         仅重解析报错的（parseErrors 非空）
      - all                 重新解析全部 raw_pages
      - unparsed_and_errors 默认：未解析 + 报错（已成功的跳过）
    """
    valid_modes = {"unparsed_only", "errors_only", "all", "unparsed_and_errors"}
    if mode not in valid_modes:
        mode = "unparsed_and_errors"

    prisma = Prisma()
    prisma.connect()
    job = prisma.crawljob.create(data={
        "status": "pending",
        "triggerType": f"parse:{mode}",
    })
    prisma.disconnect()

    process = multiprocessing.Process(target=_run_parse_job, args=(job.id, mode), daemon=True)
    process.start()

    return int(job.id)


def _run_relevance_rejudge(job_id: int):
    """在子进程中用最新分类模型批量重新判定所有已解析结果的相关性。

    优化点：
      1. BERT 走批量推理（predict_batch），相比逐条推理可加速 5~50x。
      2. 列表/搜索结果页的"零成本"判定先做、单独立即写库。
      3. 模型推理与 DB 写入按 BATCH_SIZE 分批；DB 写按 5 条刷一次进度，
         让前端进度条手感更顺。
      4. 批大小通过环境变量 REJUDGE_BATCH_SIZE 调（默认 64，128G 机器可调到 128~256）。
    """
    prisma = Prisma()
    prisma.connect()

    uri = os.getenv("MONGO_URI", "mongodb://mongodb:mongodb@localhost:27017/shangjibao?authSource=admin")
    mongo_client = MongoClient(uri)
    db = mongo_client.get_default_database()
    raw_pages = db["raw_pages"]

    BATCH_SIZE = int(os.getenv("REJUDGE_BATCH_SIZE", "64"))
    PROGRESS_FLUSH_EVERY = int(os.getenv("REJUDGE_PROGRESS_EVERY", "5"))

    try:
        prisma.crawljob.update(
            where={"id": job_id},
            data={"status": "running", "startedAt": datetime.now(timezone.utc)},
        )

        bert_predictor = None
        try:
            from src.classifier.bert_predictor import BertRelevancePredictor
            BertRelevancePredictor.reload()
            bert_predictor = BertRelevancePredictor.get_instance()
        except Exception:
            pass
        bert_available = bool(bert_predictor and bert_predictor.available)

        fasttext_available = False
        try:
            from src.classifier.predictor import RelevancePredictor
            RelevancePredictor.reload()
            fasttext_available = RelevancePredictor.get_instance().available
        except Exception:
            pass

        if not bert_available and not fasttext_available:
            prisma.crawljob.update(
                where={"id": job_id},
                data={
                    "status": "failed",
                    "finishedAt": datetime.now(timezone.utc),
                    "errorLog": "没有可用的分类模型（BERT / FastText），请先在数据标注页面训练模型",
                },
            )
            return

        model_name = "BERT" if bert_available else "FastText"
        logger.info(
            "Relevance rejudge using %s model, batch_size=%d",
            model_name, BATCH_SIZE,
        )

        user_keywords = [kw.keyword for kw in prisma.searchkeyword.find_many(where={"enabled": True})]
        threshold = 0.5

        all_results = prisma.parsedresult.find_many()
        total = len(all_results)
        prisma.crawljob.update(where={"id": job_id}, data={"totalPages": total})

        done = 0
        updated = 0
        error_count = 0
        error_logs: list[str] = []
        last_flush_done = 0

        notice_type_extractor = NoticeTypeExtractor()

        def _flush_progress(force: bool = False):
            nonlocal last_flush_done
            if not force and (done - last_flush_done) < PROGRESS_FLUSH_EVERY:
                return
            try:
                prisma.crawljob.update(
                    where={"id": job_id},
                    data={
                        "donePages": done,
                        "resultCount": updated,
                        "errorCount": error_count,
                    },
                )
                last_flush_done = done
            except Exception as e:
                logger.warning("rejudge progress flush failed: %s", e)

        # ---- 预处理：把"立即可判定/可跳过"的条目先处理掉，剩下的攒批做模型推理 ----
        pending: list[dict] = []  # 待批量打分的条目：{pr, text, context}

        from bson import ObjectId  # 移到循环外避免反复 import

        def _flush_pending():
            """对 pending 队列批量打分并写库。"""
            nonlocal done, updated, error_count
            if not pending:
                return

            texts = [item["text"] for item in pending]
            titles = [item["context"].get("title", "") for item in pending]

            bert_scores: list[float | None] = [None] * len(pending)
            if bert_available:
                try:
                    preds = bert_predictor.predict_batch(
                        texts, titles, batch_size=BATCH_SIZE,
                    )
                    bert_scores = [
                        (p["score"] if p is not None else None) for p in preds
                    ]
                except Exception as e:
                    logger.warning("rejudge: batch BERT inference failed: %s", e)

            # FastText 单例 predictor（仅 BERT 不可用时使用）
            ft_predictor = None
            if not bert_available and fasttext_available:
                try:
                    from src.classifier.predictor import RelevancePredictor
                    ft_predictor = RelevancePredictor.get_instance()
                except Exception:
                    ft_predictor = None

            for idx, item in enumerate(pending):
                pr = item["pr"]
                text = item["text"]
                context = item["context"]
                try:
                    # 关键词命中率（与 RelevanceExtractor 行为一致）
                    text_lower = text.lower()
                    matched = [
                        kw for kw in user_keywords if kw and kw.lower() in text_lower
                    ]
                    kw_score = (
                        len(matched) / len(user_keywords) if user_keywords else 0
                    )

                    bert_score = bert_scores[idx]
                    if bert_score is not None:
                        is_relevant = bert_score >= threshold
                        final_score = bert_score
                    elif ft_predictor is not None:
                        ml_label = None
                        ml_confidence = None
                        try:
                            pred = ft_predictor.predict(
                                text, title=context.get("title", "")
                            )
                            if pred:
                                ml_label = pred.get("label")
                                ml_confidence = pred.get("confidence")
                        except Exception:
                            pass

                        if (
                            ml_label is not None
                            and ml_confidence is not None
                            and ml_confidence > 0.7
                        ):
                            is_relevant = ml_label == "relevant"
                            final_score = (
                                ml_confidence if is_relevant else 1 - ml_confidence
                            )
                        elif user_keywords:
                            is_relevant = kw_score > 0
                            final_score = kw_score
                        else:
                            is_relevant = None
                            final_score = None
                    elif user_keywords:
                        is_relevant = kw_score > 0
                        final_score = kw_score
                    else:
                        is_relevant = None
                        final_score = None

                    update_data = {
                        "isRelevant": is_relevant,
                        "relevanceScore": (
                            round(final_score, 4) if final_score is not None else None
                        ),
                        "matchedKeywords": ",".join(matched),
                    }

                    if not pr.noticeType:
                        nt = notice_type_extractor.extract(text, "", context)
                        if nt:
                            update_data["noticeType"] = nt

                    prisma.parsedresult.update(
                        where={"id": pr.id}, data=update_data,
                    )
                    updated += 1
                except Exception as e:
                    error_count += 1
                    msg = f"[{(pr.url or '')[:80]}] {e}"
                    error_logs.append(msg)
                    logger.warning("Relevance rejudge error: %s", msg)

                done += 1
                _flush_progress()

            pending.clear()

        for pr in all_results:
            try:
                if pr.noticeType in ("list_page", "search_serp"):
                    done += 1
                    _flush_progress()
                    continue

                # 优先做"搜索引擎结果页"判定（成本最低，零 IO）
                if is_search_engine_url(pr.url or ""):
                    prisma.parsedresult.update(
                        where={"id": pr.id},
                        data={
                            "isRelevant": False,
                            "relevanceScore": 0.0,
                            "noticeType": "search_serp",
                            "parseErrors": "search_serp: 搜索引擎结果页（如百度/Bing/Google），无业务正文",
                        },
                    )
                    updated += 1
                    done += 1
                    _flush_progress()
                    continue

                text = None
                html_for_listing = None
                if pr.mongoDocId:
                    mongo_doc = raw_pages.find_one({"_id": ObjectId(pr.mongoDocId)})
                    if mongo_doc and mongo_doc.get("html"):
                        html_for_listing = mongo_doc["html"]
                        text = html_to_text(html_for_listing)

                if not text:
                    text = (pr.summary or "") + " " + (pr.title or "")

                if not text.strip():
                    done += 1
                    _flush_progress()
                    continue

                if html_for_listing:
                    listing_info = detect_listing_page(html_for_listing, text)
                    if listing_info is not None:
                        prisma.parsedresult.update(
                            where={"id": pr.id},
                            data={
                                "isRelevant": False,
                                "relevanceScore": 0.0,
                                "noticeType": "list_page",
                                "parseErrors": (
                                    "listing_page: 命中列表聚合页特征（"
                                    f"链接条目={listing_info['text_links']}, "
                                    f"含日期条目={listing_info['dated_links']}, "
                                    f"占比={listing_info['ratio']}, "
                                    f"命中规则={listing_info.get('rule', '-')}）"
                                ),
                            },
                        )
                        updated += 1
                        done += 1
                        _flush_progress()
                        continue

                context = {
                    "url": pr.url,
                    "title": pr.title or "",
                    "search_query": pr.searchQuery or "",
                    "source_name": pr.sourceName or "",
                    "user_keywords": user_keywords,
                    "relevance_threshold": threshold,
                }

                pending.append({"pr": pr, "text": text, "context": context})

                if len(pending) >= BATCH_SIZE:
                    _flush_pending()

            except Exception as e:
                error_count += 1
                msg = f"[{(pr.url or '')[:80]}] {e}"
                error_logs.append(msg)
                logger.warning("Relevance rejudge error: %s", msg)
                done += 1
                _flush_progress()

        _flush_pending()
        _flush_progress(force=True)

        prisma.crawljob.update(
            where={"id": job_id},
            data={
                "status": "completed",
                "finishedAt": datetime.now(timezone.utc),
                "totalPages": total,
                "donePages": done,
                "resultCount": updated,
                "errorCount": error_count,
                "errorLog": "\n".join(error_logs[-50:]) if error_logs else None,
            },
        )

        try:
            from src.notify.engine import prepare_notifications
            prep = prepare_notifications(prisma)
            logger.info("Auto-prepare after rejudge: prepared=%d skipped=%d", prep["prepared"], prep["skipped"])
        except Exception as pe:
            logger.warning("Auto-prepare notifications after rejudge failed: %s", pe)

    except Exception as e:
        logger.error("Relevance rejudge job %d failed: %s", job_id, e)
        prisma.crawljob.update(
            where={"id": job_id},
            data={"status": "failed", "finishedAt": datetime.now(timezone.utc), "errorLog": str(e)},
        )
    finally:
        mongo_client.close()
        prisma.disconnect()


def start_relevance_rejudge() -> int:
    """创建并启动相关性重新判定任务，返回 job_id。"""
    prisma = Prisma()
    prisma.connect()
    job = prisma.crawljob.create(data={
        "status": "pending",
        "triggerType": "relevance_rejudge",
    })
    prisma.disconnect()

    process = multiprocessing.Process(target=_run_relevance_rejudge, args=(job.id,), daemon=True)
    process.start()

    return int(job.id)
