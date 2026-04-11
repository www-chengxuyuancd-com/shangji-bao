"""
APScheduler 调度管理。

从数据库读取 CrawlSchedule 配置，自动创建/更新定时任务。
"""
import logging

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from prisma import Prisma

from src.scheduler.runner import start_crawl_job

logger = logging.getLogger(__name__)

_scheduler: BackgroundScheduler | None = None


def get_scheduler() -> BackgroundScheduler:
    global _scheduler
    if _scheduler is None:
        _scheduler = BackgroundScheduler()
    return _scheduler


def _scheduled_crawl():
    """由调度器触发的抓取任务。"""
    logger.info("Scheduled crawl triggered")
    try:
        job_id = start_crawl_job(trigger_type="scheduled")
        logger.info("Scheduled crawl job created: %d", job_id)
    except Exception as e:
        logger.error("Failed to start scheduled crawl: %s", e)


def sync_schedules():
    """从数据库加载调度配置，同步到 APScheduler。"""
    scheduler = get_scheduler()

    for job in scheduler.get_jobs():
        if job.id.startswith("crawl_schedule_"):
            scheduler.remove_job(job.id)

    prisma = Prisma()
    prisma.connect()
    schedules = prisma.crawlschedule.find_many(where={"enabled": True})
    prisma.disconnect()

    for sched in schedules:
        job_id = f"crawl_schedule_{sched.id}"
        trigger = _build_trigger(sched)
        if trigger:
            scheduler.add_job(
                _scheduled_crawl,
                trigger=trigger,
                id=job_id,
                replace_existing=True,
            )
            logger.info("Schedule synced: %s (%s)", sched.name, sched.scheduleType)


def _build_trigger(sched):
    """根据 CrawlSchedule 构建 CronTrigger。"""
    hour = sched.startHour
    minute = sched.startMinute

    if sched.scheduleType == "daily":
        if sched.timesPerDay <= 1:
            return CronTrigger(hour=hour, minute=minute)
        interval = 24 // sched.timesPerDay
        hours = ",".join(str((hour + i * interval) % 24) for i in range(sched.timesPerDay))
        return CronTrigger(hour=hours, minute=minute)

    elif sched.scheduleType == "weekly":
        weekdays = sched.weekdays or "0"
        dow = _weekdays_to_cron(weekdays)
        return CronTrigger(day_of_week=dow, hour=hour, minute=minute)

    elif sched.scheduleType == "multi_weekly":
        weekdays = sched.weekdays or "0,2,4"
        dow = _weekdays_to_cron(weekdays)
        return CronTrigger(day_of_week=dow, hour=hour, minute=minute)

    elif sched.scheduleType == "multi_daily":
        if sched.timesPerDay <= 1:
            return CronTrigger(hour=hour, minute=minute)
        interval = 24 // sched.timesPerDay
        hours = ",".join(str((hour + i * interval) % 24) for i in range(sched.timesPerDay))
        return CronTrigger(hour=hours, minute=minute)

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


def start_scheduler():
    """启动调度器。"""
    scheduler = get_scheduler()
    if not scheduler.running:
        sync_schedules()
        scheduler.start()
        logger.info("Scheduler started")


def stop_scheduler():
    """停止调度器。"""
    scheduler = get_scheduler()
    if scheduler.running:
        scheduler.shutdown(wait=False)
        logger.info("Scheduler stopped")
