import json
import logging
import os
from datetime import timedelta, timezone

from flask import Flask

from src.config import get_config
from src.db.prisma_client import get_prisma, close_prisma
from src.db.mongo import close_mongo

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(name)s] %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# 静音掉这些噪声日志，避免淹没爬虫/解析的关键日志
for _noisy in ("httpx", "httpcore", "urllib3.connectionpool", "asyncio"):
    logging.getLogger(_noisy).setLevel(logging.WARNING)

CST = timezone(timedelta(hours=8))


def create_app():
    app = Flask(
        __name__,
        template_folder=os.path.join(os.path.dirname(__file__), "templates"),
        static_folder=os.path.join(os.path.dirname(__file__), "static"),
    )

    cfg = get_config()
    app.config.from_object(cfg)

    @app.template_filter("cst_time")
    def cst_time_filter(dt, fmt="%m-%d %H:%M:%S"):
        """将 UTC 时间转为 CST (UTC+8) 显示。"""
        if dt is None:
            return "-"
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(CST).strftime(fmt)

    @app.template_filter("from_json")
    def from_json_filter(s):
        try:
            return json.loads(s or "{}")
        except (json.JSONDecodeError, TypeError):
            return {}

    with app.app_context():
        get_prisma()

    # mongo raw_pages 上一次性建必要索引（首次启动几十秒，之后命中缓存）
    try:
        from src.db.mongo import ensure_raw_pages_indexes
        ensure_raw_pages_indexes()
    except Exception as e:
        logging.getLogger(__name__).warning("ensure_raw_pages_indexes failed: %s", e)

    from src.app.routes.frontend import frontend_bp
    from src.app.routes.admin import admin_bp
    from src.app.api.search import api_bp

    app.register_blueprint(frontend_bp)
    app.register_blueprint(admin_bp, url_prefix="/admin")
    app.register_blueprint(api_bp, url_prefix="/api")

    # Flask 内置开发服务器在 debug+reloader 下会启动两次进程，
    # 只有子进程（WERKZEUG_RUN_MAIN=true）才应初始化后台任务；
    # 但在 gunicorn/docker 中该变量通常不存在，也必须初始化。
    werkzeug_run_main = os.getenv("WERKZEUG_RUN_MAIN")
    should_bootstrap_background = (werkzeug_run_main == "true") or (werkzeug_run_main is None)
    if should_bootstrap_background:
        try:
            from src.scheduler.runner import fix_orphaned_jobs
            fix_orphaned_jobs()
        except Exception as e:
            import logging
            logging.getLogger(__name__).error("Failed to fix orphaned jobs: %s", e, exc_info=True)
        try:
            from src.scheduler.scheduler import start_scheduler
            start_scheduler()
        except Exception as e:
            import logging
            logging.getLogger(__name__).error("Failed to start scheduler: %s", e, exc_info=True)

    @app.teardown_appcontext
    def shutdown_db(exception=None):
        pass

    return app
