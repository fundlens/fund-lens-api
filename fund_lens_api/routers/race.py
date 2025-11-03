"""API routes for race resources."""

from typing import Annotated

from fastapi import APIRouter, HTTPException, Query, Request

from fund_lens_api.dependencies import DBSession
from fund_lens_api.rate_limit import RATE_LIMIT_DEFAULT, limiter
from fund_lens_api.schemas.race import (
    HouseRaceResponse,
    PresidentialRaceResponse,
    SenateRaceResponse,
)
from fund_lens_api.services.race import RaceService

router = APIRouter(prefix="/races", tags=["races"])


# noinspection PyUnusedLocal
@router.get("/presidential", response_model=PresidentialRaceResponse)
@limiter.limit(RATE_LIMIT_DEFAULT)
def get_presidential_race(
    request: Request,
    db: DBSession,
    include_stats: Annotated[
        bool, Query(description="Include fundraising statistics for each candidate")
    ] = True,
) -> PresidentialRaceResponse:
    """Get information about the Presidential race.

    Returns all Presidential candidates with optional fundraising statistics
    and race-level aggregated data.

    Examples:
    - `/races/presidential` - Get race with stats
    - `/races/presidential?include_stats=false` - Get race without stats
    """
    return RaceService.get_presidential_race(db, include_stats=include_stats)


# noinspection PyUnusedLocal
@router.get("/{state}/senate", response_model=SenateRaceResponse)
@limiter.limit(RATE_LIMIT_DEFAULT)
def get_senate_race(
    request: Request,
    db: DBSession,
    state: str,
    include_stats: Annotated[
        bool, Query(description="Include fundraising statistics for each candidate")
    ] = True,
) -> SenateRaceResponse:
    """Get information about a Senate race in a specific state.

    Returns all Senate candidates for the state with optional fundraising statistics
    and race-level aggregated data.

    Examples:
    - `/races/MD/senate` - Get Maryland Senate race with stats
    - `/races/VA/senate?include_stats=false` - Get Virginia Senate race without stats
    """
    result = RaceService.get_senate_race(db, state.upper(), include_stats=include_stats)
    if not result:
        raise HTTPException(
            status_code=404, detail=f"No Senate race found for state {state}"
        )
    return result


# noinspection PyUnusedLocal
@router.get("/{state}/house/{district}", response_model=HouseRaceResponse)
@limiter.limit(RATE_LIMIT_DEFAULT)
def get_house_race(
    request: Request,
    db: DBSession,
    state: str,
    district: str,
    include_stats: Annotated[
        bool, Query(description="Include fundraising statistics for each candidate")
    ] = True,
) -> HouseRaceResponse:
    """Get information about a House race in a specific state and district.

    Returns all House candidates for the state and district with optional fundraising
    statistics and race-level aggregated data.

    Examples:
    - `/races/MD/house/08` - Get Maryland 8th district race with stats
    - `/races/CA/house/12?include_stats=false` - Get California 12th district race without stats
    """
    result = RaceService.get_house_race(
        db, state.upper(), district, include_stats=include_stats
    )
    if not result:
        raise HTTPException(
            status_code=404,
            detail=f"No House race found for state {state}, district {district}",
        )
    return result
