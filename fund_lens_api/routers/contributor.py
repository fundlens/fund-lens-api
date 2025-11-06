"""API routes for contributor resources."""

from datetime import date
from decimal import Decimal
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query, Request

from fund_lens_api.dependencies import DBSession
from fund_lens_api.rate_limit import RATE_LIMIT_DEFAULT, RATE_LIMIT_SEARCH, RATE_LIMIT_STATS, limiter
from fund_lens_api.schemas.common import PaginatedResponse, PaginationParams, create_pagination_meta
from fund_lens_api.schemas.contributor import (
    ContributorContributionsResponse,
    ContributorDetail,
    ContributorFilters,
    ContributorList,
    ContributorRecipientsResponse,
    ContributorSearchAggregated,
    ContributorStats,
    ContributorsByCandidateResponse,
    ContributorsByCommitteeResponse,
    TopContributor,
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
        state: Annotated[str | None, Query(max_length=2, description="Filter by state")] = None,
        entity_type: Annotated[
            str | None, Query(description="Filter by entity type (IND, PAC, ORG, etc.)")
        ] = None,
        employer: Annotated[
            str | None, Query(description="Filter by employer (partial match)")
        ] = None,
        occupation: Annotated[
            str | None, Query(description="Filter by occupation (partial match)")
        ] = None,
) -> PaginatedResponse[ContributorList]:
    """Search contributors by name with advanced filtering.

    Performs case-insensitive partial matching on contributor names with optional filters.

    Examples:
    - `/contributors/search?q=Smith&state=MD`
    - `/contributors/search?q=John&entity_type=IND&employer=Google`
    - `/contributors/search?q=PAC&entity_type=PAC`
    """
    contributors, total_count = ContributorService.search_contributors_enhanced(
        db=db,
        search_query=q,
        state=state.upper() if state else None,
        entity_type=entity_type.upper() if entity_type else None,
        employer=employer,
        occupation=occupation,
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
@router.get("/search/aggregated", response_model=PaginatedResponse[ContributorSearchAggregated])
@limiter.limit(RATE_LIMIT_SEARCH)
def search_contributors_aggregated(
    request: Request,
    db: DBSession,
    q: Annotated[str, Query(min_length=1, max_length=500, description="Search query")],
    pagination: Annotated[PaginationParams, Depends()],
    state: Annotated[str | None, Query(max_length=2, description="Filter by state")] = None,
    entity_type: Annotated[
        str | None, Query(description="Filter by entity type (INDIVIDUAL, COMMITTEE, ORG)")
    ] = None,
    min_amount: Annotated[
        Decimal | None, Query(ge=0, description="Minimum total contribution amount")
    ] = None,
    max_amount: Annotated[
        Decimal | None, Query(ge=0, description="Maximum total contribution amount")
    ] = None,
    date_from: Annotated[date | None, Query(description="Contributions from date")] = None,
    date_to: Annotated[date | None, Query(description="Contributions to date")] = None,
    sort_by: Annotated[
        str,
        Query(
            description="Sort by: name, total_amount, contribution_count, unique_recipients, first_date, last_date"
        ),
    ] = "total_amount",
    order: Annotated[str, Query(description="Sort order: asc, desc")] = "desc",
) -> PaginatedResponse[ContributorSearchAggregated]:
    """Search contributors by name with aggregated statistics across all recipients.

    This endpoint returns contributors matching the search query with their aggregated
    contribution statistics across all candidates and committees they've contributed to.

    This provides a comprehensive view of each contributor's total activity.

    Examples:
    - `/contributors/search/aggregated?q=John&state=MD`
    - `/contributors/search/aggregated?q=Smith&min_amount=1000&sort_by=total_amount`
    - `/contributors/search/aggregated?q=PAC&entity_type=COMMITTEE&sort_by=contribution_count`
    """
    # Validate sort_by and order
    valid_sort_by = {
        "name",
        "total_amount",
        "contribution_count",
        "unique_recipients",
        "first_date",
        "last_date",
    }
    if sort_by not in valid_sort_by:
        raise HTTPException(
            status_code=400,
            detail=f"sort_by must be one of: {', '.join(valid_sort_by)}",
        )

    valid_order = {"asc", "desc"}
    if order not in valid_order:
        raise HTTPException(status_code=400, detail="order must be 'asc' or 'desc'")

    contributors, total_count = ContributorService.search_contributors_with_aggregations(
        db=db,
        search_query=q,
        state=state.upper() if state else None,
        entity_type=entity_type.upper() if entity_type else None,
        min_amount=min_amount,
        max_amount=max_amount,
        date_from=date_from,
        date_to=date_to,
        sort_by=sort_by,  # type: ignore
        order=order,  # type: ignore
        offset=pagination.offset,
        limit=pagination.page_size,
    )

    return PaginatedResponse(
        items=contributors,
        meta=create_pagination_meta(
            page=pagination.page,
            page_size=pagination.page_size,
            total_items=total_count,
        ),
    )


# noinspection PyUnusedLocal
@router.get("/top", response_model=PaginatedResponse[TopContributor])
@limiter.limit(RATE_LIMIT_DEFAULT)
def get_top_contributors(
        request: Request,
        db: DBSession,
        limit: Annotated[int, Query(ge=1, le=1000, description="Maximum number of contributors to return")] = 10,
        state: Annotated[str | None, Query(max_length=2, description="Filter by state (two-letter code)")] = None,
        entity_type: Annotated[str | None, Query(description="Filter by entity type (IND, ORG, PAC, etc.)")] = None,
) -> PaginatedResponse[TopContributor]:
    """Get top contributors by total contribution amount.

    Optionally filter by state and/or entity type.
    Returns contributors with their total contribution amounts and counts.

    Examples:
    - `/contributors/top?limit=25`
    - `/contributors/top?limit=25&entity_type=IND`
    - `/contributors/top?limit=25&state=MD`
    - `/contributors/top?limit=25&entity_type=ORG&state=VA`
    """
    # Normalize filters
    normalized_state = state.upper() if state else None
    normalized_entity_type = entity_type.upper() if entity_type else None

    # Get top contributors
    top_contributors = ContributorService.get_top_contributors(
        db,
        limit=limit,
        state=normalized_state,
        entity_type=normalized_entity_type,
    )

    # Get total count
    total_count = ContributorService.count_top_contributors(
        db,
        state=normalized_state,
        entity_type=normalized_entity_type,
    )

    # Build response items
    items = [
        TopContributor(
            contributor=ContributorList.model_validate(contributor),
            total_amount=total_amount,
            contribution_count=contribution_count,
            unique_recipients=unique_recipients,
        )
        for contributor, total_amount, contribution_count, unique_recipients in top_contributors
    ]

    return PaginatedResponse(
        items=items,
        meta=create_pagination_meta(
            page=1,
            page_size=limit,
            total_items=total_count,
        ),
    )


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


# noinspection PyUnusedLocal
@router.get("/{contributor_id}/contributions", response_model=ContributorContributionsResponse)
@limiter.limit(RATE_LIMIT_DEFAULT)
def get_contributor_contributions(
        request: Request,
        db: DBSession,
        contributor_id: int,
        page_size: Annotated[int, Query(ge=1, le=1000, description="Number of contributions to return")] = 1000,
        sort_by: Annotated[str, Query(pattern="^(recipient|date|amount)$", description="Sort column: recipient, date, or amount")] = "amount",
        sort_direction: Annotated[str, Query(pattern="^(asc|desc)$", description="Sort direction: asc or desc")] = "desc",
        start_date: Annotated[date | None, Query(description="Filter contributions from this date (inclusive, format: YYYY-MM-DD)")] = None,
        end_date: Annotated[date | None, Query(description="Filter contributions until this date (inclusive, format: YYYY-MM-DD)")] = None,
) -> ContributorContributionsResponse:
    """Get all contributions made by a specific contributor.

    Returns individual contributions with sorting, pagination, and optional date range filtering,
    including committee information for each contribution.

    Examples:
    - `/contributors/123/contributions` - Get first 100 contributions (default: sorted by amount desc)
    - `/contributors/123/contributions?page_size=50` - Get first 50 contributions
    - `/contributors/123/contributions?sort_by=recipient&sort_direction=asc` - Sort by committee name A-Z
    - `/contributors/123/contributions?sort_by=date&sort_direction=desc` - Sort by most recent date
    - `/contributors/123/contributions?start_date=2024-01-01&end_date=2024-12-31` - Filter by date range
    """
    # Verify contributor exists
    contributor = ContributorService.get_contributor_by_id(db, contributor_id)
    if not contributor:
        raise HTTPException(status_code=404, detail="Contributor not found")

    contributions = ContributorService.get_contributor_contributions(
        db,
        contributor_id,
        limit=page_size,
        sort_by=sort_by,
        sort_direction=sort_direction,
        start_date=start_date,
        end_date=end_date,
    )

    return ContributorContributionsResponse(contributions=contributions)


# noinspection PyUnusedLocal
@router.get("/{contributor_id}/recipients", response_model=ContributorRecipientsResponse)
@limiter.limit(RATE_LIMIT_DEFAULT)
def get_contributor_recipients(
    request: Request,
    db: DBSession,
    contributor_id: int,
    sort_by: Annotated[
        str,
        Query(
            pattern="^(committee_name|total_amount|contribution_count|first_date|last_date)$",
            description="Sort by: committee_name, total_amount, contribution_count, first_date, last_date",
        ),
    ] = "total_amount",
    sort_direction: Annotated[
        str,
        Query(pattern="^(asc|desc)$", description="Sort direction: asc or desc"),
    ] = "desc",
) -> ContributorRecipientsResponse:
    """Get pre-aggregated recipient data for a contributor.

    This endpoint provides recipient committees grouped and aggregated for a specific contributor.
    Instead of fetching thousands of individual contributions and grouping them client-side,
    this endpoint returns pre-aggregated recipient data with contribution counts and totals.

    This significantly improves performance for contributor detail pages by reducing:
    - Number of API requests (from many to one)
    - Payload size (from 1000s of records to 5-10 aggregated records)
    - Client-side processing (no grouping needed)
    """
    # Verify contributor exists
    contributor = ContributorService.get_contributor_by_id(db, contributor_id)
    if not contributor:
        raise HTTPException(status_code=404, detail="Contributor not found")

    # Get aggregated recipients
    recipients, total_count = ContributorService.get_contributor_recipients(
        db,
        contributor_id,
        sort_by=sort_by,
        sort_direction=sort_direction,
    )

    return ContributorRecipientsResponse(
        recipients=recipients,
        meta={"total_count": total_count},
    )


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
@router.get("/by-candidate/{candidate_id}", response_model=ContributorsByCandidateResponse)
@limiter.limit(RATE_LIMIT_DEFAULT)
def get_contributors_by_candidate(
    request: Request,
    db: DBSession,
    candidate_id: int,
    page: Annotated[int, Query(ge=1, description="Page number")] = 1,
    page_size: Annotated[int, Query(ge=1, le=1000, description="Items per page (max 1000)")] = 25,
    sort_by: Annotated[
        str,
        Query(
            description="Sort by: total_amount, contribution_count, name, first_date, last_date"
        ),
    ] = "total_amount",
    order: Annotated[str, Query(description="Sort order: asc, desc")] = "desc",
    min_amount: Annotated[
        Decimal | None, Query(ge=0, description="Minimum total contribution amount")
    ] = None,
    max_amount: Annotated[
        Decimal | None, Query(ge=0, description="Maximum total contribution amount")
    ] = None,
    state: Annotated[str | None, Query(max_length=2, description="Filter by state")] = None,
    entity_type: Annotated[
        str | None, Query(description="Filter by entity type (IND, ORG, etc.)")
    ] = None,
    date_from: Annotated[date | None, Query(description="Contributions from date")] = None,
    date_to: Annotated[date | None, Query(description="Contributions to date")] = None,
    search: Annotated[str | None, Query(description="Search contributor names")] = None,
    include_contributions: Annotated[
        bool, Query(description="Include individual contribution details")
    ] = False,
) -> ContributorsByCandidateResponse:
    """Get all contributors to a specific candidate with aggregated statistics.

    This endpoint returns contributors grouped by their total contributions to the candidate,
    with optional filtering, sorting, and pagination. Can optionally include individual
    contribution details for each contributor.

    Examples:
    - Get top 25 contributors: `/contributors/by-candidate/183?page=1&page_size=25`
    - Filter by state: `/contributors/by-candidate/183?state=MD`
    - Include contributions: `/contributors/by-candidate/183?include_contributions=true`
    - Date range: `/contributors/by-candidate/183?date_from=2024-01-01&date_to=2024-12-31`
    """
    # Validate sort_by and order
    valid_sort_by = {"total_amount", "contribution_count", "name", "first_date", "last_date"}
    if sort_by not in valid_sort_by:
        raise HTTPException(
            status_code=400,
            detail=f"sort_by must be one of: {', '.join(valid_sort_by)}",
        )

    valid_order = {"asc", "desc"}
    if order not in valid_order:
        raise HTTPException(status_code=400, detail="order must be 'asc' or 'desc'")

    # Calculate offset
    offset = (page - 1) * page_size

    result = ContributorService.get_contributors_by_candidate(
        db=db,
        candidate_id=candidate_id,
        include_contributions=include_contributions,
        sort_by=sort_by,  # type: ignore
        order=order,  # type: ignore
        min_amount=min_amount,
        max_amount=max_amount,
        state=state.upper() if state else None,
        entity_type=entity_type.upper() if entity_type else None,
        date_from=date_from,
        date_to=date_to,
        search=search,
        offset=offset,
        limit=page_size,
    )

    if not result:
        raise HTTPException(status_code=404, detail="Candidate not found")

    return result


# noinspection PyUnusedLocal
@router.get("/by-committee/{committee_id}", response_model=ContributorsByCommitteeResponse)
@limiter.limit(RATE_LIMIT_DEFAULT)
def get_contributors_by_committee(
    request: Request,
    db: DBSession,
    committee_id: int,
    page: Annotated[int, Query(ge=1, description="Page number")] = 1,
    page_size: Annotated[int, Query(ge=1, le=1000, description="Items per page (max 1000)")] = 25,
    sort_by: Annotated[
        str,
        Query(
            description="Sort by: total_amount, contribution_count, name, first_date, last_date"
        ),
    ] = "total_amount",
    order: Annotated[str, Query(description="Sort order: asc, desc")] = "desc",
    min_amount: Annotated[
        Decimal | None, Query(ge=0, description="Minimum total contribution amount")
    ] = None,
    max_amount: Annotated[
        Decimal | None, Query(ge=0, description="Maximum total contribution amount")
    ] = None,
    state: Annotated[str | None, Query(max_length=2, description="Filter by state")] = None,
    entity_type: Annotated[
        str | None, Query(description="Filter by entity type (IND, ORG, etc.)")
    ] = None,
    date_from: Annotated[date | None, Query(description="Contributions from date")] = None,
    date_to: Annotated[date | None, Query(description="Contributions to date")] = None,
    search: Annotated[str | None, Query(description="Search contributor names")] = None,
    include_contributions: Annotated[
        bool, Query(description="Include individual contribution details")
    ] = False,
) -> ContributorsByCommitteeResponse:
    """Get all contributors to a specific committee with aggregated statistics.

    This endpoint returns contributors grouped by their total contributions to the committee,
    with optional filtering, sorting, and pagination. Can optionally include individual
    contribution details for each contributor.

    Examples:
    - Get top 25 contributors: `/contributors/by-committee/456?page=1&page_size=25`
    - Filter by state: `/contributors/by-committee/456?state=MD`
    - Include contributions: `/contributors/by-committee/456?include_contributions=true`
    - Date range: `/contributors/by-committee/456?date_from=2024-01-01&date_to=2024-12-31`
    """
    # Validate sort_by and order
    valid_sort_by = {"total_amount", "contribution_count", "name", "first_date", "last_date"}
    if sort_by not in valid_sort_by:
        raise HTTPException(
            status_code=400,
            detail=f"sort_by must be one of: {', '.join(valid_sort_by)}",
        )

    valid_order = {"asc", "desc"}
    if order not in valid_order:
        raise HTTPException(status_code=400, detail="order must be 'asc' or 'desc'")

    # Calculate offset
    offset = (page - 1) * page_size

    result = ContributorService.get_contributors_by_committee(
        db=db,
        committee_id=committee_id,
        include_contributions=include_contributions,
        sort_by=sort_by,  # type: ignore
        order=order,  # type: ignore
        min_amount=min_amount,
        max_amount=max_amount,
        state=state.upper() if state else None,
        entity_type=entity_type.upper() if entity_type else None,
        date_from=date_from,
        date_to=date_to,
        search=search,
        offset=offset,
        limit=page_size,
    )

    if not result:
        raise HTTPException(status_code=404, detail="Committee not found")

    return result
