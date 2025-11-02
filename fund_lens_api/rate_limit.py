"""Rate limiting utilities for API endpoints."""

from typing import Any

from fastapi import Request
from slowapi import Limiter
from slowapi.util import get_remote_address

from fund_lens_api.config import get_settings

settings = get_settings()

# Initialize rate limiter (shared across app)
limiter = Limiter(key_func=get_remote_address, default_limits=[settings.rate_limit_default])


def get_limiter(request: Request) -> Any:
    """Get the rate limiter from app state."""
    return request.app.state.limiter


# Pre-configured rate limit strings for different endpoint types
RATE_LIMIT_DEFAULT = settings.rate_limit_default
RATE_LIMIT_SEARCH = settings.rate_limit_search
RATE_LIMIT_STATS = settings.rate_limit_stats
