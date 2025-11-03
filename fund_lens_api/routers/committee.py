"""API routes for committee resources."""

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query, Request

from fund_lens_api.dependencies import DBSession
from fund_lens_api.rate_limit import RATE_LIMIT_DEFAULT, RATE_LIMIT_SEARCH, RATE_LIMIT_STATS, limiter
from fund_lens_api.schemas.candidate import CandidateList
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
        state: Annotated[str | None, Query(max_length=2, description="Filter by state")] = None,
        committee_type: Annotated[
            str | None, Query(description="Filter by committee type (PAC, PARTY, CANDIDATE, etc.)")
        ] = None,
        party: Annotated[
            str | None, Query(description="Filter by party (DEM, REP, etc.)")
        ] = None,
        is_active: Annotated[bool | None, Query(description="Filter by active status")] = None,
) -> PaginatedResponse[CommitteeList]:
    """Search committees by name with advanced filtering.

    Performs case-insensitive partial matching on committee names with optional filters.

    Examples:
    - `/committees/search?q=Victory&state=MD`
    - `/committees/search?q=PAC&committee_type=PAC&party=DEM`
    - `/committees/search?q=Fund&is_active=true`
    """
    committees, total_count = CommitteeService.search_committees_enhanced(
        db=db,
        search_query=q,
        state=state.upper() if state else None,
        committee_type=committee_type.upper() if committee_type else None,
        party=party.upper() if party else None,
        is_active=is_active,
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
        include_candidate: Annotated[
            bool, Query(description="Include associated candidate details")
        ] = True,
) -> CommitteeDetail:
    """Get detailed information for a specific committee.

    By default includes associated candidate details if the committee
    is linked to a candidate.
    """
    result = CommitteeService.get_committee_by_id(db, committee_id, include_candidate)
    if not result:
        raise HTTPException(status_code=404, detail="Committee not found")

    committee, candidate = result

    # Build the response with candidate if available
    committee_dict = {
        "id": committee.id,
        "name": committee.name,
        "committee_type": committee.committee_type,
        "party": committee.party,
        "state": committee.state,
        "city": committee.city,
        "is_active": committee.is_active,
        "candidate_id": committee.candidate_id,
        "fec_committee_id": committee.fec_committee_id,
        "state_committee_id": committee.state_committee_id,
        "created_at": committee.created_at,
        "updated_at": committee.updated_at,
        "candidate": CandidateList.model_validate(candidate) if candidate else None,
    }

    return CommitteeDetail(**committee_dict)


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
