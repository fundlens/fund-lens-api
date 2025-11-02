"""API routes for committee resources."""

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query, Request

from fund_lens_api.dependencies import DBSession
from fund_lens_api.rate_limit import RATE_LIMIT_DEFAULT, RATE_LIMIT_SEARCH, RATE_LIMIT_STATS, limiter
from fund_lens_api.schemas.committee import (
    CommitteeDetail,
    CommitteeFilters,
    CommitteeList,
    CommitteeStats,
)
from fund_lens_api.schemas.common import PaginatedResponse, PaginationParams, create_pagination_meta
from fund_lens_api.services.committee import CommitteeService

router = APIRouter(prefix="/committees", tags=["committees"])


# noinspection PyUnusedLocal
@router.get("", response_model=PaginatedResponse[CommitteeList])
@limiter.limit(RATE_LIMIT_DEFAULT)
def list_committees(
        request: Request,
        db: DBSession,
        pagination: Annotated[PaginationParams, Depends()],
        filters: Annotated[CommitteeFilters, Depends()],
) -> PaginatedResponse[CommitteeList]:
    """List committees with pagination and filtering.

    Supports filtering by:
    - state: Two-letter state code
    - committee_type: Committee type (e.g., CANDIDATE, PAC, PARTY, SUPER_PAC)
    - party: Political party
    - is_active: Active status
    - candidate_id: Associated candidate ID
    """
    committees, total_count = CommitteeService.list_committees(
        db=db,
        filters=filters,
        offset=pagination.offset,
        limit=pagination.page_size,
    )

    return PaginatedResponse(
        items=[CommitteeList.model_validate(c) for c in committees],
        meta=create_pagination_meta(
            page=pagination.page,
            page_size=pagination.page_size,
            total_items=total_count,
        ),
    )


# noinspection PyUnusedLocal
@router.get("/search", response_model=PaginatedResponse[CommitteeList])
@limiter.limit(RATE_LIMIT_SEARCH)
def search_committees(
        request: Request,
        db: DBSession,
        q: Annotated[str, Query(min_length=1, max_length=500, description="Search query")],
        pagination: Annotated[PaginationParams, Depends()],
) -> PaginatedResponse[CommitteeList]:
    """Search committees by name.

    Performs case-insensitive partial matching on committee names.
    """
    committees, total_count = CommitteeService.search_committees(
        db=db,
        search_query=q,
        offset=pagination.offset,
        limit=pagination.page_size,
    )

    return PaginatedResponse(
        items=[CommitteeList.model_validate(c) for c in committees],
        meta=create_pagination_meta(
            page=pagination.page,
            page_size=pagination.page_size,
            total_items=total_count,
        ),
    )


# noinspection PyUnusedLocal
@router.get("/by-candidate/{candidate_id}", response_model=list[CommitteeList])
@limiter.limit(RATE_LIMIT_DEFAULT)
def list_committees_by_candidate(
        request: Request,
        db: DBSession,
        candidate_id: int,
) -> list[CommitteeList]:
    """Get all committees associated with a specific candidate."""
    committees = CommitteeService.get_committees_by_candidate(db, candidate_id)
    return [CommitteeList.model_validate(c) for c in committees]


# noinspection PyUnusedLocal
@router.get("/by-state/{state}", response_model=list[CommitteeList])
@limiter.limit(RATE_LIMIT_DEFAULT)
def list_committees_by_state(
        request: Request,
        db: DBSession,
        state: str,
) -> list[CommitteeList]:
    """Get all committees for a specific state."""
    committees = CommitteeService.get_committees_by_state(db, state.upper())
    return [CommitteeList.model_validate(c) for c in committees]


# noinspection PyUnusedLocal
@router.get("/{committee_id}", response_model=CommitteeDetail)
@limiter.limit(RATE_LIMIT_DEFAULT)
def get_committee(
        request: Request,
        db: DBSession,
        committee_id: int,
) -> CommitteeDetail:
    """Get detailed information for a specific committee."""
    committee = CommitteeService.get_committee_by_id(db, committee_id)
    if not committee:
        raise HTTPException(status_code=404, detail="Committee not found")
    return CommitteeDetail.model_validate(committee)


# noinspection PyUnusedLocal
@router.get("/{committee_id}/stats", response_model=CommitteeStats)
@limiter.limit(RATE_LIMIT_STATS)
def get_committee_stats(
        request: Request,
        db: DBSession,
        committee_id: int,
) -> CommitteeStats:
    """Get aggregated statistics for a committee.

    Includes:
    - Total number of contributions received
    - Total amount received
    - Number of unique contributors
    - Average contribution amount
    """
    stats = CommitteeService.get_committee_stats(db, committee_id)
    if not stats:
        raise HTTPException(status_code=404, detail="Committee not found")
    return stats
