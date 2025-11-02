"""API routes for contributor resources."""

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query, Request

from fund_lens_api.dependencies import DBSession
from fund_lens_api.rate_limit import RATE_LIMIT_DEFAULT, RATE_LIMIT_SEARCH, RATE_LIMIT_STATS, limiter
from fund_lens_api.schemas.common import PaginatedResponse, PaginationParams, create_pagination_meta
from fund_lens_api.schemas.contributor import (
    ContributorDetail,
    ContributorFilters,
    ContributorList,
    ContributorStats,
)
from fund_lens_api.services.contributor import ContributorService

router = APIRouter(prefix="/contributors", tags=["contributors"])


# noinspection PyUnusedLocal
@router.get("", response_model=PaginatedResponse[ContributorList])
@limiter.limit(RATE_LIMIT_DEFAULT)
def list_contributors(
        request: Request,
        db: DBSession,
        pagination: Annotated[PaginationParams, Depends()],
        filters: Annotated[ContributorFilters, Depends()],
) -> PaginatedResponse[ContributorList]:
    """List contributors with pagination and filtering.

    Supports filtering by:
    - state: Two-letter state code
    - city: City name (exact match)
    - entity_type: Entity type (e.g., INDIVIDUAL, COMMITTEE, ORG)
    - employer: Employer name (partial match)
    - occupation: Occupation (partial match)
    """
    contributors, total_count = ContributorService.list_contributors(
        db=db,
        filters=filters,
        offset=pagination.offset,
        limit=pagination.page_size,
    )

    return PaginatedResponse(
        items=[ContributorList.model_validate(c) for c in contributors],
        meta=create_pagination_meta(
            page=pagination.page,
            page_size=pagination.page_size,
            total_items=total_count,
        ),
    )


# noinspection PyUnusedLocal
@router.get("/search", response_model=PaginatedResponse[ContributorList])
@limiter.limit(RATE_LIMIT_SEARCH)
def search_contributors(
        request: Request,
        db: DBSession,
        q: Annotated[str, Query(min_length=1, max_length=500, description="Search query")],
        pagination: Annotated[PaginationParams, Depends()],
) -> PaginatedResponse[ContributorList]:
    """Search contributors by name.

    Performs case-insensitive partial matching on contributor names.
    """
    contributors, total_count = ContributorService.search_contributors(
        db=db,
        search_query=q,
        offset=pagination.offset,
        limit=pagination.page_size,
    )

    return PaginatedResponse(
        items=[ContributorList.model_validate(c) for c in contributors],
        meta=create_pagination_meta(
            page=pagination.page,
            page_size=pagination.page_size,
            total_items=total_count,
        ),
    )


# noinspection PyUnusedLocal
@router.get("/top", response_model=list[dict])
@limiter.limit(RATE_LIMIT_DEFAULT)
def get_top_contributors(
        request: Request,
        db: DBSession,
        limit: Annotated[int, Query(ge=1, le=100)] = 10,
        state: Annotated[str | None, Query(max_length=2)] = None,
) -> list[dict]:
    """Get top contributors by total contribution amount.

    Optionally filter by state.
    Returns contributors with their total contribution amounts.
    """
    top_contributors = ContributorService.get_top_contributors(db, limit=limit, state=state)

    return [
        {
            "contributor": ContributorList.model_validate(contributor),
            "total_amount": total_amount,
        }
        for contributor, total_amount in top_contributors
    ]


# noinspection PyUnusedLocal
@router.get("/{contributor_id}", response_model=ContributorDetail)
@limiter.limit(RATE_LIMIT_DEFAULT)
def get_contributor(
        request: Request,
        db: DBSession,
        contributor_id: int,
) -> ContributorDetail:
    """Get detailed information for a specific contributor."""
    contributor = ContributorService.get_contributor_by_id(db, contributor_id)
    if not contributor:
        raise HTTPException(status_code=404, detail="Contributor not found")
    return ContributorDetail.model_validate(contributor)


# noinspection PyUnusedLocal
@router.get("/{contributor_id}/stats", response_model=ContributorStats)
@limiter.limit(RATE_LIMIT_STATS)
def get_contributor_stats(
        request: Request,
        db: DBSession,
        contributor_id: int,
) -> ContributorStats:
    """Get aggregated statistics for a contributor.

    Includes:
    - Total number of contributions made
    - Total amount contributed
    - Number of unique recipients
    - Average contribution amount
    - First and last contribution dates
    """
    stats = ContributorService.get_contributor_stats(db, contributor_id)
    if not stats:
        raise HTTPException(status_code=404, detail="Contributor not found")
    return stats
