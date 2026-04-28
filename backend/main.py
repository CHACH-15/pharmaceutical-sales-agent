"""
main.py
~~~~~~~
Application factory for the Wisdom Analytics API.

Entry points
─────────────
  uvicorn app.main:app --reload
  gunicorn app.main:app -k uvicorn.workers.UvicornWorker

Environment
────────────
  All configuration is loaded from .env via app.config.Settings (Pydantic).
  See config.py for the full list of settings.
"""

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.config import settings
from app.database import init_db
from app.routers import auth, admin, chat, websocket

# ── Logging ───────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

logger = logging.getLogger(__name__)


# ── Lifespan ──────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown events."""
    logger.info("🚀 Starting Wisdom Analytics API…")
    init_db()
    logger.info("✅ Database initialised.")
    yield
    logger.info("🛑 Wisdom Analytics API shutting down.")


# ── Factory ───────────────────────────────────────────────────────────────────

def create_app() -> FastAPI:
    app = FastAPI(
        title="Wisdom Agent — Hikma Analytics",
        version="2.0.0",
        docs_url="/docs",
        redoc_url="/redoc",
        lifespan=lifespan,
    )

    # ── CORS ──────────────────────────────────────────────────────────────────
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.allow_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # ── Routers ───────────────────────────────────────────────────────────────
    app.include_router(auth.router)
    app.include_router(admin.router)
    app.include_router(chat.router)
    app.include_router(websocket.router)

    # ── Health ────────────────────────────────────────────────────────────────
    @app.get("/health", tags=["meta"], summary="Liveness check")
    def health():
        return {"status": "ok", "service": "wisdom-agent", "version": "2.0.0"}

    return app


app = create_app()