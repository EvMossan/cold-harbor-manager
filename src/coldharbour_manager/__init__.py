from __future__ import annotations

from flask import Flask

from coldharbour_manager.web.routes import (
    ACCOUNT_BLUEPRINTS,
    accounts_bp,
    register_request_hooks,
)

__all__ = ["create_app", "accounts_bp", "ACCOUNT_BLUEPRINTS"]


def create_app() -> Flask:
    """Flask application factory."""

    app = Flask(
        __name__,
        template_folder="web/templates",
        static_folder=None,
    )

    # Install request hooks (logging, CSP) from web layer
    register_request_hooks(app)

    # Mount root index and per-destination blueprints
    app.register_blueprint(accounts_bp)
    for bp in ACCOUNT_BLUEPRINTS:
        app.register_blueprint(bp)

    return app
