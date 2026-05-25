from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from loguru import logger
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from aq_scraper.config.settings import Settings
from aq_scraper.storage.models import Base


def create_engine(settings: Settings):
    """Create an async SQLAlchemy engine from settings."""
    logger.debug("Creating async engine for {}", settings.DB_HOST)
    return create_async_engine(
        settings.async_dsn,
        echo=False,
        pool_pre_ping=True,
    )


def create_session_factory(engine) -> async_sessionmaker[AsyncSession]:
    """Create an async session factory."""
    return async_sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)


async def init_db(engine) -> None:
    """Create all tables if they do not exist."""
    logger.info("Initializing database schema...")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    logger.info("Database schema ready.")


@asynccontextmanager
async def get_session(session_factory: async_sessionmaker[AsyncSession]) -> AsyncGenerator[AsyncSession, None]:
    """Provide a transactional scope around a series of operations."""
    async with session_factory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
