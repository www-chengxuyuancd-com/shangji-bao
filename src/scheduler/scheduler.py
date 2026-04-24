"""
APScheduler 调度管理。

从数据库读取 CrawlSchedule 配置，自动创建/更新定时任务。
使用 Asia/Shanghai (CST) 时区。
"""
import logging
from datetime import timezone, timedelta

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from src.scheduler.runner import start_crawl_job

logger = logging.getLogger(__name__)

CST_TZ = "Asia/Shanghai"
CST = timezone(timedelta(hours=8))

_scheduler: BackgroundScheduler | None = None


def get_scheduler() -> BackgroundScheduler:
    global _scheduler
    if _scheduler is None:
        _scheduler = BackgroundScheduler(timezone=CST_TZ)
    return _scheduler


def _make_scheduled_crawl(schedule_id: int):
    """生成绑定了 schedule_id 的回调函数。"""
    def _do():
        from datetime import datetime
        now = datetime.now(CST).strftime("%Y-%m-%d %H:%M:%S CST")
        logger.info("=== Scheduled crawl triggered at %s (schedule_id=%s) ===", now, schedule_id)
        try:
            job_id = start_crawl_job(trigger_type="scheduled", schedule_id=schedule_id)
            logger.info("Scheduled crawl job created: %d", job_id)
        except Exception as e:
            logger.error("Failed to start scheduled crawl: %s", e)
    return _do


def sync_schedules():
    """从数据库加载调度配置，同步到 APScheduler。"""
    scheduler = get_scheduler()

    for job in scheduler.get_jobs():
        if job.id.startswith("crawl_schedule_"):
            scheduler.remove_job(job.id)

    try:
        from src.db.prisma_client import get_prisma as _get_app_prisma
        prisma = _get_app_prisma()
        schedules = prisma.crawlschedule.find_many(where={"enabled": True})
    except Exception as e:
        logger.error("sync_schedules: DB query failed: %s", e, exc_info=True)
        return

    if not schedules:
        logger.info("No enabled schedules found")
        return

    logger.info("sync_schedules: found %d enabled schedules", len(schedules))
    for sched in schedules:
        job_id = f"crawl_schedule_{sched.id}"
        try:
            trigger = _build_trigger(sched)
            if trigger:
                scheduler.add_job(
                    _make_scheduled_crawl(int(sched.id)),
                    trigger=trigger,
                    id=job_id,
                    replace_existing=True,
                )
                job = scheduler.get_job(job_id)
                next_time = (getattr(job, "next_run_time", None) or getattr(job, "next_fire_time", None)) if job else None
                next_run = next_time.strftime("%Y-%m-%d %H:%M CST") if next_time else "未知"
                logger.info("Schedule synced: [%s] %s (%s), next_run=%s",
                            sched.id, sched.name, sched.scheduleType, next_run)
            else:
                logger.warning("Schedule [%s] %s: trigger build returned None (type=%s)",
                               sched.id, sched.name, sched.scheduleType)
        except Exception as e:
            logger.error("Schedule [%s] %s: failed to add job: %s",
                         sched.id, sched.name, e, exc_info=True)


def _build_trigger(sched):
    """根据 CrawlSchedule 构建 CronTrigger（CST 时区）。"""
    hour = sched.startHour
    minute = sched.startMinute

    if sched.scheduleType == "daily":
        if sched.timesPerDay <= 1:
            return CronTrigger(hour=hour, minute=minute, timezone=CST_TZ)
        interval = 24 // sched.timesPerDay
        hours = ",".join(str((hour + i * interval) % 24) for i in range(sched.timesPerDay))
        return CronTrigger(hour=hours, minute=minute, timezone=CST_TZ)

    elif sched.scheduleType == "weekly":
        weekdays = sched.weekdays or "0"
        dow = _weekdays_to_cron(weekdays)
        return CronTrigger(day_of_week=dow, hour=hour, minute=minute, timezone=CST_TZ)

    elif sched.scheduleType == "multi_weekly":
        weekdays = sched.weekdays or "0,2,4"
        dow = _weekdays_to_cron(weekdays)
        return CronTrigger(day_of_week=dow, hour=hour, minute=minute, timezone=CST_TZ)

    elif sched.scheduleType == "multi_daily":
        if sched.timesPerDay <= 1:
            return CronTrigger(hour=hour, minute=minute, timezone=CST_TZ)
        interval = 24 // sched.timesPerDay
        hours = ",".join(str((hour + i * interval) % 24) for i in range(sched.timesPerDay))
        return CronTrigger(hour=hours, minute=minute, timezone=CST_TZ)

    logger.warning("Unknown schedule type: %s", sched.scheduleType)
    return None


def _weekdays_to_cron(weekdays_str: str) -> str:
    """将 '0,2,4' (0=周一) 转换为 APScheduler 格式 'mon,wed,fri'。"""
    mapping = {
        "0": "mon", "1": "tue", "2": "wed", "3": "thu",
        "4": "fri", "5": "sat", "6": "sun",
    }
    parts = [mapping.get(d.strip(), d.strip()) for d in weekdays_str.split(",") if d.strip()]
    return ",".join(parts)


def get_next_run_times() -> dict:
    """获取所有调度任务的下次执行时间（供页面展示），key 为 schedule_id (int)。"""
    scheduler = get_scheduler()
    result = {}
    for job in scheduler.get_jobs():
        if job.id.startswith("crawl_schedule_"):
            try:
                sched_id = int(job.id.replace("crawl_schedule_", ""))
            except ValueError:
                continue
            next_time = getattr(job, "next_run_time", None) or getattr(job, "next_fire_time", None)
            result[sched_id] = next_time.strftime("%Y-%m-%d %H:%M CST") if next_time else None
    return result


def start_scheduler():
    """启动调度器。gunicorn 使用 -w 1 确保只有一个 worker。"""
    import os
    scheduler = get_scheduler()
    if not scheduler.running:
        try:
            scheduler.start()
            logger.info("Scheduler started in pid=%d", os.getpid())
        except Exception as e:
            logger.error("Scheduler start failed: %s", e, exc_info=True)
            return
    sync_schedules()
    logger.info("Scheduler ready, jobs=%d", len(scheduler.get_jobs()))


def stop_scheduler():
    """停止调度器。"""
    scheduler = get_scheduler()
    if scheduler.running:
        scheduler.shutdown(wait=False)
        logger.info("Scheduler stopped")
