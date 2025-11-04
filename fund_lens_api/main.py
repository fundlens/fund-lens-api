"""Main FastAPI application entry point."""

from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
# noinspection PyProtectedMember
from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded

from fund_lens_api.config import get_settings
from fund_lens_api.dependencies import close_db, init_db
from fund_lens_api.rate_limit import limiter
from fund_lens_api.routers import candidate, committee, contribution, contributor, metadata, race, state


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
    """Application lifespan context manager.

    Handles startup and shutdown events.
    """
    # Startup
    app_settings = get_settings()
    init_db(app_settings)
    yield
    # Shutdown
    close_db()


# Initialize FastAPI app
settings = get_settings()

app = FastAPI(
    title=settings.app_name,
    version=settings.app_version,
    description=settings.app_description,
    lifespan=lifespan,
)

# Add rate limiter to app state
app.state.limiter = limiter  # type: ignore[attr-defined]
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)  # type: ignore[arg-type]

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=settings.cors_allow_credentials,
    allow_methods=settings.cors_allow_methods,
    allow_headers=settings.cors_allow_headers,
)

# Register routers
app.include_router(candidate.router)
app.include_router(contributor.router)
app.include_router(committee.router)
app.include_router(contribution.router)
app.include_router(metadata.router)
app.include_router(race.router)
app.include_router(state.router)


@app.get("/", tags=["health"])
def root() -> dict[str, str]:
    """Root endpoint with API information."""
    return {
        "name": settings.app_name,
        "version": settings.app_version,
        "description": settings.app_description,
        "docs_url": "/docs",
        "openapi_url": "/openapi.json",
    }


@app.get("/health", tags=["health"])
def health_check() -> dict[str, str]:
    """Health check endpoint."""
    return {"status": "healthy"}
