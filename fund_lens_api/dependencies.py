"""FastAPI dependencies for dependency injection."""

from collections.abc import Generator
from typing import Annotated, cast

from fastapi import Depends
from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import Session, sessionmaker

from fund_lens_api.config import Settings, get_settings

# Global engine and session factory (created once at startup)
_engine: Engine | None = None
_session_factory: sessionmaker[Session] | None = None


def init_db(settings: Settings) -> None:
    """Initialize database engine and session factory.

    Should be called once at application startup.
    """
    global _engine, _session_factory

    _engine = create_engine(
        settings.database_url,
        pool_pre_ping=True,
        pool_size=5,
        max_overflow=10,
    )
    _session_factory = sessionmaker(bind=_engine, expire_on_commit=False)


def get_db() -> Generator[Session, None, None]:
    """Dependency that provides a database session.

    Yields a SQLAlchemy session and ensures it's closed after use.
    """
    if _session_factory is None:
        raise RuntimeError("Database not initialized. Call init_db() at startup.")

    # Type cast after None check - we know it's not None here
    session = cast(sessionmaker[Session], _session_factory)()
    try:
        yield session
    finally:
        session.close()


def close_db() -> None:
    """Close database connections.

    Should be called at application shutdown.
    """
    global _engine
    if _engine:
        _engine.dispose()


# Type alias for dependency injection
DBSession = Annotated[Session, Depends(get_db)]
AppSettings = Annotated[Settings, Depends(get_settings)]
