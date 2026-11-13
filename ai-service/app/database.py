"""
BodhNet AI Service — app/database.py
Automated Knowledge Management and Query System
------------------------------------------------------
SQLAlchemy async engine + session factory for Oracle (Phase 3A).

Connection is configured via the environment variable BODHNET_DB_URL.
If the variable is absent, a safe local default is used — edit it to
match your Oracle XE / Oracle Free instance credentials.

Environment variable (set in your shell or a .env file):
  export BODHNET_DB_URL="oracle+oracledb_async://bodhnet:bodhnet@localhost:1521/?service_name=XEPDB1"

The `oracledb` driver runs in "thin" mode by default — no Oracle Client
libraries (Instant Client) are required on Apple Silicon.
"""

import logging
import os

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.models import Base

log = logging.getLogger("bodhnet.db")

# ---------------------------------------------------------------------------
# Connection URL — override via env-var; never hard-code credentials in prod
# ---------------------------------------------------------------------------
_DEFAULT_DB_URL = (
    "oracle+oracledb_async://bodhnet:bodhnet@localhost:1521/?service_name=FREEPDB1"
)

DATABASE_URL: str = os.environ.get("BODHNET_DB_URL", _DEFAULT_DB_URL)

# ---------------------------------------------------------------------------
# Engine — created lazily, held as module-level singleton
# ---------------------------------------------------------------------------
_engine: AsyncEngine | None = None
_session_factory: async_sessionmaker[AsyncSession] | None = None


def get_engine() -> AsyncEngine:
    """Return the shared async engine, creating it on first call."""
    global _engine
    if _engine is None:
        _engine = create_async_engine(
            DATABASE_URL,
            echo=False,          # set True to log every SQL statement
            pool_size=5,
            max_overflow=10,
            pool_pre_ping=True,  # verify connections before use (recovers from DB restarts)
        )
    return _engine


def get_session_factory() -> async_sessionmaker[AsyncSession]:
    """Return the shared async session factory."""
    global _session_factory
    if _session_factory is None:
        _session_factory = async_sessionmaker(
            bind=get_engine(),
            expire_on_commit=False,   # avoid lazy-load issues after commit in async context
            class_=AsyncSession,
        )
    return _session_factory


# ---------------------------------------------------------------------------
# Schema initialisation — called once at application startup
# ---------------------------------------------------------------------------
async def init_db() -> None:
    """
    Create all tables defined in models.py if they do not already exist.
    Safe to call on every startup — SQLAlchemy uses CREATE TABLE IF NOT EXISTS
    semantics (checkfirst=True).

    NOTE: Oracle does not support IF NOT EXISTS in older versions; SQLAlchemy
    handles this by inspecting the schema before issuing DDL, so this call
    is idempotent regardless of Oracle version.
    """
    engine = get_engine()
    try:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all, checkfirst=True)
        log.info("Oracle schema initialised — tables BODHNET_SESSIONS and BODHNET_MESSAGES are ready.")
    except Exception as exc:
        log.error(
            "Oracle schema initialisation failed: %s\n"
            "The service will start without database persistence. "
            "Fix your DB connection and restart.",
            exc,
        )
        raise


# ---------------------------------------------------------------------------
# Async context manager — use in endpoint handlers to get a scoped session
# ---------------------------------------------------------------------------
from contextlib import asynccontextmanager
from typing import AsyncGenerator


@asynccontextmanager
async def get_db_session() -> AsyncGenerator[AsyncSession, None]:
    """
    Async context manager that yields a database session and guarantees
    rollback on exception, then closes the session.

    Usage:
        async with get_db_session() as session:
            session.add(...)
            await session.commit()
    """
    factory = get_session_factory()
    async with factory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()


# ---------------------------------------------------------------------------
# Teardown — called at application shutdown
# ---------------------------------------------------------------------------
async def close_db() -> None:
    """Dispose of the connection pool gracefully."""
    global _engine
    if _engine is not None:
        await _engine.dispose()
        _engine = None
        log.info("Oracle connection pool closed.")

