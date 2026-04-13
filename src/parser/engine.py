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

    if not is_valid_content(text):
        result["_invalid"] = True
        errors.append("content_invalid: 页面内容无效（JS/CSS/反爬页面），无有效中文内容")
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


def _run_parse_job(job_id: int):
    """在子进程中执行的解析任务。"""
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

        docs = list(raw_pages.find(
            {"meta.source_type": {"$in": ["search_result", "website"]}},
            {"_id": 1, "url": 1, "html": 1, "search_query": 1, "source_name": 1, "meta": 1},
        ))

        total = len(docs)
        prisma.crawljob.update(
            where={"id": job_id},
            data={"totalPages": total},
        )

        done = 0
        parsed_count = 0
        error_count = 0
        error_logs = []

        for doc in docs:
            url = doc.get("url", "")
            url_hash = hashlib.md5(url.encode("utf-8")).hexdigest()

            existing = prisma.parsedresult.find_unique(where={"urlHash": url_hash})
            if existing:
                done += 1
                continue

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

                if result.get("_invalid"):
                    prisma.parsedresult.create(data={
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

                amount_data = result.get("amount")
                amount_display = None
                amount_value = None
                if isinstance(amount_data, dict):
                    amount_display = amount_data.get("display")
                    amount_value = amount_data.get("value")

                relevance_data = result.get("relevance", {}) or {}

                prisma.parsedresult.create(data={
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
            if done % 20 == 0:
                prisma.crawljob.update(
                    where={"id": job_id},
                    data={
                        "donePages": done,
                        "resultCount": parsed_count,
                        "errorCount": error_count,
                    },
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


def start_parse_job() -> int:
    """创建并启动一个解析任务，返回 job_id。"""
    prisma = Prisma()
    prisma.connect()
    job = prisma.crawljob.create(data={
        "status": "pending",
        "triggerType": "parse",
    })
    prisma.disconnect()

    process = multiprocessing.Process(target=_run_parse_job, args=(job.id,), daemon=True)
    process.start()

    return int(job.id)


def _run_relevance_rejudge(job_id: int):
    """在子进程中用最新 FastText 模型重新判定所有已解析结果的相关性。"""
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

        from src.classifier.predictor import RelevancePredictor
        RelevancePredictor.reload()
        predictor = RelevancePredictor.get_instance()
        if not predictor.available:
            prisma.crawljob.update(
                where={"id": job_id},
                data={
                    "status": "failed",
                    "finishedAt": datetime.now(timezone.utc),
                    "errorLog": "FastText 模型不存在，请先在数据标注页面训练模型",
                },
            )
            return

        user_keywords = [kw.keyword for kw in prisma.searchkeyword.find_many(where={"enabled": True})]

        all_results = prisma.parsedresult.find_many()
        total = len(all_results)
        prisma.crawljob.update(where={"id": job_id}, data={"totalPages": total})

        done = 0
        updated = 0
        error_count = 0
        error_logs = []

        relevance_extractor = RelevanceExtractor()
        notice_type_extractor = NoticeTypeExtractor()

        for pr in all_results:
            try:
                text = None
                if pr.mongoDocId:
                    from bson import ObjectId
                    mongo_doc = raw_pages.find_one({"_id": ObjectId(pr.mongoDocId)})
                    if mongo_doc and mongo_doc.get("html"):
                        text = html_to_text(mongo_doc["html"])

                if not text:
                    text = (pr.summary or "") + " " + (pr.title or "")

                if not text.strip():
                    done += 1
                    continue

                context = {
                    "url": pr.url,
                    "title": pr.title or "",
                    "search_query": pr.searchQuery or "",
                    "source_name": pr.sourceName or "",
                    "user_keywords": user_keywords,
                }

                rel = relevance_extractor.extract(text, "", context)

                update_data = {
                    "isRelevant": rel.get("is_relevant"),
                    "relevanceScore": rel.get("score"),
                    "matchedKeywords": rel.get("matched"),
                }

                if not pr.noticeType:
                    nt = notice_type_extractor.extract(text, "", context)
                    if nt:
                        update_data["noticeType"] = nt

                prisma.parsedresult.update(
                    where={"id": pr.id},
                    data=update_data,
                )
                updated += 1

            except Exception as e:
                error_count += 1
                msg = f"[{pr.url[:80]}] {e}"
                error_logs.append(msg)
                logger.warning("Relevance rejudge error: %s", msg)

            done += 1
            if done % 20 == 0:
                prisma.crawljob.update(
                    where={"id": job_id},
                    data={"donePages": done, "resultCount": updated, "errorCount": error_count},
                )

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
