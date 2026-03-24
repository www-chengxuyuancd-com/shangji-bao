import os

from flask import Flask

from src.config import get_config
from src.db.prisma_client import get_prisma, close_prisma
from src.db.mongo import close_mongo


def create_app():
    app = Flask(
        __name__,
        template_folder=os.path.join(os.path.dirname(__file__), "templates"),
        static_folder=os.path.join(os.path.dirname(__file__), "static"),
    )

    cfg = get_config()
    app.config.from_object(cfg)

    with app.app_context():
        get_prisma()

    from src.app.routes.frontend import frontend_bp
    from src.app.routes.admin import admin_bp
    from src.app.api.search import api_bp

    app.register_blueprint(frontend_bp)
    app.register_blueprint(admin_bp, url_prefix="/admin")
    app.register_blueprint(api_bp, url_prefix="/api")

    @app.teardown_appcontext
    def shutdown_db(exception=None):
        pass

    return app
