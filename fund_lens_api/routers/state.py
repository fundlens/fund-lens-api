"""API routes for state-level resources."""

from fastapi import APIRouter, HTTPException, Query, Request
from typing import Annotated

from fund_lens_api.dependencies import DBSession
from fund_lens_api.rate_limit import RATE_LIMIT_DEFAULT, limiter
from fund_lens_api.schemas.state import StateSummary
from fund_lens_api.services.state import StateService

router = APIRouter(prefix="/states", tags=["states"])


# noinspection PyUnusedLocal
@router.get("/{state}/summary", response_model=StateSummary)
@limiter.limit(RATE_LIMIT_DEFAULT)
def get_state_summary(
    request: Request,
    db: DBSession,
    state: str,
    top_n: Annotated[int, Query(ge=1, le=20, description="Number of top fundraisers per race")] = 5,
) -> StateSummary:
    """Get comprehensive summary of campaign finance data for a state.

    Returns state-level statistics including:
    - Total number of candidates (total and active)
    - Total amount raised across all candidates
    - Total number of contributions
    - Number of unique contributors
    - Race summaries grouped by office type (House, Senate, President, etc.)
    - Top fundraising candidates for each race type
    - District information for House races

    Examples:
    - `/states/MD/summary` - Get summary for Maryland with top 5 fundraisers per race
    - `/states/CA/summary?top_n=10` - Get summary for California with top 10 fundraisers per race
    """
    summary = StateService.get_state_summary(db, state.upper(), top_n)
    if not summary:
        raise HTTPException(status_code=404, detail=f"No data found for state: {state}")
    return summary
