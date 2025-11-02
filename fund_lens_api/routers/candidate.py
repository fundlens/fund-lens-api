"""API routes for candidate resources."""

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query, Request

from fund_lens_api.dependencies import DBSession
from fund_lens_api.rate_limit import RATE_LIMIT_DEFAULT, RATE_LIMIT_SEARCH, RATE_LIMIT_STATS, limiter
from fund_lens_api.schemas.candidate import (
    CandidateDetail,
    CandidateFilters,
    CandidateList,
    CandidateStats,
)
from fund_lens_api.schemas.common import PaginatedResponse, PaginationParams, create_pagination_meta
from fund_lens_api.services.candidate import CandidateService

router = APIRouter(prefix="/candidates", tags=["candidates"])


# noinspection PyUnusedLocal
@router.get("", response_model=PaginatedResponse[CandidateList])
@limiter.limit(RATE_LIMIT_DEFAULT)
def list_candidates(
        request: Request,
        db: DBSession,
        pagination: Annotated[PaginationParams, Depends()],
        filters: Annotated[CandidateFilters, Depends()],
) -> PaginatedResponse[CandidateList]:
    """List candidates with pagination and filtering.

    Supports filtering by:
    - state: Two-letter state code (e.g., MD, VA)
    - office: Office type (e.g., HOUSE, SENATE, PRESIDENT)
    - party: Political party
    - district: Congressional district
    - is_active: Active status
    """
    candidates, total_count = CandidateService.list_candidates(
        db=db,
        filters=filters,
        offset=pagination.offset,
        limit=pagination.page_size,
    )

    return PaginatedResponse(
        items=[CandidateList.model_validate(c) for c in candidates],
        meta=create_pagination_meta(
            page=pagination.page,
            page_size=pagination.page_size,
            total_items=total_count,
        ),
    )


# noinspection PyUnusedLocal
@router.get("/search", response_model=PaginatedResponse[CandidateList])
@limiter.limit(RATE_LIMIT_SEARCH)
def search_candidates(
        request: Request,
        db: DBSession,
        q: Annotated[str, Query(min_length=1, max_length=500, description="Search query")],
        pagination: Annotated[PaginationParams, Depends()],
) -> PaginatedResponse[CandidateList]:
    """Search candidates by name.

    Performs case-insensitive partial matching on candidate names.
    """
    candidates, total_count = CandidateService.search_candidates(
        db=db,
        search_query=q,
        offset=pagination.offset,
        limit=pagination.page_size,
    )

    return PaginatedResponse(
        items=[CandidateList.model_validate(c) for c in candidates],
        meta=create_pagination_meta(
            page=pagination.page,
            page_size=pagination.page_size,
            total_items=total_count,
        ),
    )


# noinspection PyUnusedLocal
@router.get("/states", response_model=list[str])
@limiter.limit(RATE_LIMIT_DEFAULT)
def list_states_with_candidates(request: Request, db: DBSession) -> list[str]:
    """Get list of states that have candidates.

    Useful for building state-based navigation.
    """
    return CandidateService.get_states_with_candidates(db)


# noinspection PyUnusedLocal
@router.get("/by-state/{state}", response_model=list[CandidateList])
@limiter.limit(RATE_LIMIT_DEFAULT)
def list_candidates_by_state(
        request: Request,
        db: DBSession,
        state: str,
) -> list[CandidateList]:
    """Get all candidates for a specific state.

    Results are ordered by office and then by name.
    """
    candidates = CandidateService.get_candidates_by_state(db, state.upper())
    return [CandidateList.model_validate(c) for c in candidates]


# noinspection PyUnusedLocal
@router.get("/{candidate_id}", response_model=CandidateDetail)
@limiter.limit(RATE_LIMIT_DEFAULT)
def get_candidate(
        request: Request,
        db: DBSession,
        candidate_id: int,
) -> CandidateDetail:
    """Get detailed information for a specific candidate."""
    candidate = CandidateService.get_candidate_by_id(db, candidate_id)
    if not candidate:
        raise HTTPException(status_code=404, detail="Candidate not found")
    return CandidateDetail.model_validate(candidate)


# noinspection PyUnusedLocal
@router.get("/{candidate_id}/stats", response_model=CandidateStats)
@limiter.limit(RATE_LIMIT_STATS)
def get_candidate_stats(
        request: Request,
        db: DBSession,
        candidate_id: int,
) -> CandidateStats:
    """Get aggregated statistics for a candidate.

    Includes:
    - Total number of contributions
    - Total amount raised
    - Number of unique contributors
    - Average contribution amount
    """
    stats = CandidateService.get_candidate_stats(db, candidate_id)
    if not stats:
        raise HTTPException(status_code=404, detail="Candidate not found")
    return stats
