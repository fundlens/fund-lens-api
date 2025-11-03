"""API routes for candidate resources."""

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query, Request

from fund_lens_api.dependencies import DBSession
from fund_lens_api.field_selector import apply_field_selection, parse_fields_param
from fund_lens_api.rate_limit import RATE_LIMIT_DEFAULT, RATE_LIMIT_SEARCH, RATE_LIMIT_STATS, limiter
from fund_lens_api.schemas.candidate import (
    BatchDetailsRequest,
    BatchDetailsResponse,
    BulkStatsRequest,
    BulkStatsResponse,
    CandidateDetail,
    CandidateFilters,
    CandidateList,
    CandidateStats,
    CandidateWithStats,
)
from fund_lens_api.schemas.common import PaginatedResponse, PaginationParams, create_pagination_meta
from fund_lens_api.services.candidate import CandidateService

router = APIRouter(prefix="/candidates", tags=["candidates"])


# noinspection PyUnusedLocal
@router.get("", response_model=PaginatedResponse[CandidateList] | PaginatedResponse[dict])
@limiter.limit(RATE_LIMIT_DEFAULT)
def list_candidates(
        request: Request,
        db: DBSession,
        pagination: Annotated[PaginationParams, Depends()],
        filters: Annotated[CandidateFilters, Depends()],
        fields: Annotated[
            str | None,
            Query(
                description="Comma-separated list of fields to return (e.g., 'id,name,party')"
            ),
        ] = None,
) -> PaginatedResponse[CandidateList] | PaginatedResponse[dict]:
    """List candidates with pagination and filtering.

    Supports filtering by:
    - state: Two-letter state code (e.g., MD, VA)
    - office: Office type (e.g., HOUSE, SENATE, PRESIDENT)
    - party: Political party
    - district: Congressional district
    - is_active: Active status

    Field selection:
    - Use the 'fields' parameter to select specific fields (e.g., ?fields=id,name,party)
    - Omit the 'fields' parameter to get all fields
    """
    candidates, total_count = CandidateService.list_candidates(
        db=db,
        filters=filters,
        offset=pagination.offset,
        limit=pagination.page_size,
    )

    # Apply field selection if requested
    field_set = parse_fields_param(fields)
    if field_set:
        items = apply_field_selection(
            [CandidateList.model_validate(c) for c in candidates], field_set
        )
    else:
        items = [CandidateList.model_validate(c) for c in candidates]

    return PaginatedResponse(
        items=items,
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
        pagination: Annotated[PaginationParams, Depends()],
        q: Annotated[str, Query(min_length=1, max_length=500, description="Search query")],
        state: Annotated[str | None, Query(description="Filter by state (two-letter code)")] = None,
        office: Annotated[
            str | None, Query(description="Filter by offices (comma-separated: H,S)")
        ] = None,
        party: Annotated[
            str | None, Query(description="Filter by parties (comma-separated: DEM,REP)")
        ] = None,
        is_active: Annotated[bool | None, Query(description="Filter by active status")] = None,
        has_fundraising: Annotated[
            bool, Query(description="Only include candidates with contributions")
        ] = False,
) -> PaginatedResponse[CandidateList]:
    """Search candidates by name with advanced filtering.

    Performs case-insensitive partial matching on candidate names with optional filters.

    Examples:
    - `/candidates/search?q=John&state=MD&office=H&party=DEM`
    - `/candidates/search?q=Smith&has_fundraising=true&is_active=true`
    """
    # Parse comma-separated offices
    offices = None
    if office:
        offices = [o.strip().upper() for o in office.split(",") if o.strip()]

    # Parse comma-separated parties
    parties = None
    if party:
        parties = [p.strip().upper() for p in party.split(",") if p.strip()]

    candidates, total_count = CandidateService.search_candidates(
        db=db,
        search_query=q,
        state=state.upper() if state else None,
        offices=offices,
        parties=parties,
        is_active=is_active,
        has_fundraising=has_fundraising,
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
@router.get("/by-state/{state}", response_model=PaginatedResponse[CandidateWithStats] | PaginatedResponse[dict])
@limiter.limit(RATE_LIMIT_DEFAULT)
def list_candidates_by_state(
        request: Request,
        db: DBSession,
        state: str,
        pagination: Annotated[PaginationParams, Depends()],
        office: Annotated[
            str | None, Query(description="Filter by offices (comma-separated: H,S)")
        ] = None,
        party: Annotated[
            str | None, Query(description="Filter by parties (comma-separated: DEM,REP)")
        ] = None,
        district: Annotated[str | None, Query(description="Filter by district (for House races)")] = None,
        is_active: Annotated[bool | None, Query(description="Filter by active status")] = None,
        has_fundraising: Annotated[
            bool, Query(description="Only include candidates with contributions")
        ] = False,
        include_stats: Annotated[bool, Query(description="Include fundraising statistics")] = False,
        sort_by: Annotated[
            str, Query(description="Sort by: name, total_amount, total_contributions")
        ] = "name",
        order: Annotated[str, Query(description="Sort order: asc, desc")] = "desc",
        fields: Annotated[
            str | None,
            Query(
                description="Comma-separated fields (e.g., 'id,name,stats.total_amount'). Supports nested fields."
            ),
        ] = None,
) -> PaginatedResponse[CandidateWithStats] | PaginatedResponse[dict]:
    """Get candidates for a specific state with enhanced filtering and sorting.

    Supports:
    - Multiple office filtering (H,S for House and Senate)
    - Multiple party filtering (DEM,REP for Democrats and Republicans)
    - District filtering (for House races)
    - Active status filtering
    - Filter candidates with fundraising only
    - Including fundraising statistics (eliminates N+1 query problem)
    - Sorting by name or fundraising metrics (asc/desc)
    - Pagination with limit and offset

    Examples:
    - `/candidates/by-state/MD?office=H,S&party=DEM,REP&include_stats=true`
    - `/candidates/by-state/MD?has_fundraising=true&sort_by=total_amount&order=desc&limit=10`
    - `/candidates/by-state/MD?is_active=true&include_stats=true&limit=50&offset=0`
    """
    # Parse comma-separated offices
    offices = None
    if office:
        offices = [o.strip().upper() for o in office.split(",") if o.strip()]

    # Parse comma-separated parties
    parties = None
    if party:
        parties = [p.strip().upper() for p in party.split(",") if p.strip()]

    # Validate sort_by parameter
    if sort_by not in ("name", "total_amount", "total_contributions"):
        raise HTTPException(
            status_code=400,
            detail="sort_by must be one of: name, total_amount, total_contributions",
        )

    # Validate order parameter
    if order not in ("asc", "desc"):
        raise HTTPException(status_code=400, detail="order must be 'asc' or 'desc'")

    candidates, total_count = CandidateService.get_candidates_by_state_with_options(
        db=db,
        state=state.upper(),
        offices=offices,
        parties=parties,
        district=district,
        is_active=is_active,
        has_fundraising=has_fundraising,
        include_stats=include_stats,
        sort_by=sort_by,  # type: ignore
        order=order,  # type: ignore
        limit=pagination.page_size,
        offset=pagination.offset,
        return_count=True,
    )

    # Apply field selection if requested
    field_set = parse_fields_param(fields)
    if field_set:
        items = apply_field_selection(candidates, field_set)
    else:
        items = candidates

    return PaginatedResponse(
        items=items,
        meta=create_pagination_meta(
            page=pagination.page,
            page_size=pagination.page_size,
            total_items=total_count,
        ),
    )


# noinspection PyUnusedLocal
@router.get("/by-state/{state}/us-house", response_model=PaginatedResponse[CandidateWithStats])
@limiter.limit(RATE_LIMIT_DEFAULT)
def list_house_candidates_by_state(
        request: Request,
        db: DBSession,
        state: str,
        pagination: Annotated[PaginationParams, Depends()],
        district: Annotated[
            str | None, Query(description="Filter by districts (comma-separated: 01,02,03)")
        ] = None,
        party: Annotated[
            str | None, Query(description="Filter by parties (comma-separated: DEM,REP)")
        ] = None,
        is_active: Annotated[bool | None, Query(description="Filter by active status")] = None,
        has_fundraising: Annotated[
            bool, Query(description="Only include candidates with contributions")
        ] = False,
        include_stats: Annotated[bool, Query(description="Include fundraising statistics")] = False,
        sort_by: Annotated[
            str, Query(description="Sort by: name, total_amount, total_contributions")
        ] = "name",
        order: Annotated[str, Query(description="Sort order: asc, desc")] = "desc",
) -> PaginatedResponse[CandidateWithStats]:
    """Get US House candidates for a specific state with filtering.

    Automatically filters to office='H' (House).

    Examples:
    - `/candidates/by-state/MD/us-house?district=01,02,03&include_stats=true`
    - `/candidates/by-state/MD/us-house?party=DEM&has_fundraising=true&sort_by=total_amount`
    """
    # Parse comma-separated districts
    districts = None
    if district:
        districts = [d.strip() for d in district.split(",") if d.strip()]

    # Parse comma-separated parties
    parties = None
    if party:
        parties = [p.strip().upper() for p in party.split(",") if p.strip()]

    # Validate parameters
    if sort_by not in ("name", "total_amount", "total_contributions"):
        raise HTTPException(
            status_code=400,
            detail="sort_by must be one of: name, total_amount, total_contributions",
        )
    if order not in ("asc", "desc"):
        raise HTTPException(status_code=400, detail="order must be 'asc' or 'desc'")

    # For pagination to work correctly, only support single district filtering
    # Multiple districts would require client-side filtering which breaks pagination
    district_filter = None
    if districts and len(districts) == 1:
        district_filter = districts[0]
    elif districts and len(districts) > 1:
        raise HTTPException(
            status_code=400,
            detail="Multiple district filtering not supported with pagination. Please filter by a single district.",
        )

    candidates, total_count = CandidateService.get_candidates_by_state_with_options(
        db=db,
        state=state.upper(),
        offices=["H"],  # Force House
        parties=parties,
        district=district_filter,
        is_active=is_active,
        has_fundraising=has_fundraising,
        include_stats=include_stats,
        sort_by=sort_by,  # type: ignore
        order=order,  # type: ignore
        limit=pagination.page_size,
        offset=pagination.offset,
        return_count=True,
    )

    return PaginatedResponse(
        items=candidates,
        meta=create_pagination_meta(
            page=pagination.page,
            page_size=pagination.page_size,
            total_items=total_count,
        ),
    )


# noinspection PyUnusedLocal
@router.get("/by-state/{state}/us-senate", response_model=PaginatedResponse[CandidateWithStats])
@limiter.limit(RATE_LIMIT_DEFAULT)
def list_senate_candidates_by_state(
        request: Request,
        db: DBSession,
        state: str,
        pagination: Annotated[PaginationParams, Depends()],
        party: Annotated[
            str | None, Query(description="Filter by parties (comma-separated: DEM,REP)")
        ] = None,
        is_active: Annotated[bool | None, Query(description="Filter by active status")] = None,
        has_fundraising: Annotated[
            bool, Query(description="Only include candidates with contributions")
        ] = False,
        include_stats: Annotated[bool, Query(description="Include fundraising statistics")] = False,
        sort_by: Annotated[
            str, Query(description="Sort by: name, total_amount, total_contributions")
        ] = "name",
        order: Annotated[str, Query(description="Sort order: asc, desc")] = "desc",
) -> PaginatedResponse[CandidateWithStats]:
    """Get US Senate candidates for a specific state with filtering.

    Automatically filters to office='S' (Senate).

    Examples:
    - `/candidates/by-state/MD/us-senate?include_stats=true&sort_by=total_amount`
    - `/candidates/by-state/MD/us-senate?party=DEM,REP&has_fundraising=true`
    """
    # Parse comma-separated parties
    parties = None
    if party:
        parties = [p.strip().upper() for p in party.split(",") if p.strip()]

    # Validate parameters
    if sort_by not in ("name", "total_amount", "total_contributions"):
        raise HTTPException(
            status_code=400,
            detail="sort_by must be one of: name, total_amount, total_contributions",
        )
    if order not in ("asc", "desc"):
        raise HTTPException(status_code=400, detail="order must be 'asc' or 'desc'")

    candidates, total_count = CandidateService.get_candidates_by_state_with_options(
        db=db,
        state=state.upper(),
        offices=["S"],  # Force Senate
        parties=parties,
        district=None,
        is_active=is_active,
        has_fundraising=has_fundraising,
        include_stats=include_stats,
        sort_by=sort_by,  # type: ignore
        order=order,  # type: ignore
        limit=pagination.page_size,
        offset=pagination.offset,
        return_count=True,
    )

    return PaginatedResponse(
        items=candidates,
        meta=create_pagination_meta(
            page=pagination.page,
            page_size=pagination.page_size,
            total_items=total_count,
        ),
    )


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


# noinspection PyUnusedLocal
@router.post("/stats/bulk", response_model=BulkStatsResponse)
@limiter.limit(RATE_LIMIT_STATS)
def get_bulk_candidate_stats(
    request: Request,
    db: DBSession,
    bulk_request: BulkStatsRequest,
) -> BulkStatsResponse:
    """Get aggregated statistics for multiple candidates in a single request.

    This endpoint allows fetching stats for up to 100 candidates at once,
    eliminating the N+1 query problem when displaying lists of candidates
    with their statistics.

    Example request body:
    ```json
    {
      "candidate_ids": [183, 118, 149, 193, 133]
    }
    ```

    Example response:
    ```json
    {
      "stats": {
        "183": {
          "candidate_id": 183,
          "total_contributions": 11754,
          "total_amount": 2445037.24,
          "unique_contributors": 1976,
          "avg_contribution": 208.02
        },
        "118": {
          "candidate_id": 118,
          "total_contributions": 2015,
          "total_amount": 583362.98,
          "unique_contributors": 388,
          "avg_contribution": 289.51
        }
      }
    }
    ```
    """
    stats_dict = CandidateService.get_bulk_candidate_stats(db, bulk_request.candidate_ids)
    return BulkStatsResponse(stats=stats_dict)


# noinspection PyUnusedLocal
@router.post("/details/batch", response_model=BatchDetailsResponse)
@limiter.limit(RATE_LIMIT_DEFAULT)
def get_batch_candidate_details(
    request: Request,
    db: DBSession,
    batch_request: BatchDetailsRequest,
) -> BatchDetailsResponse:
    """Get full details for multiple candidates in a single request.

    This endpoint allows fetching complete candidate information for up to 100
    candidates at once, optionally including their fundraising statistics.

    This eliminates the N+1 query problem when displaying lists of candidates
    that need full details.

    Example request body:
    ```json
    {
      "ids": [183, 118, 149, 193, 133],
      "include_stats": true
    }
    ```

    Example response:
    ```json
    {
      "candidates": [
        {
          "id": 183,
          "name": "RASKIN, JAMIE",
          "office": "H",
          "state": "MD",
          "district": "08",
          "party": "DEM",
          "is_active": true,
          "stats": {
            "candidate_id": 183,
            "total_contributions": 11754,
            "total_amount": 2445037.24,
            "unique_contributors": 1976,
            "avg_contribution": 208.02
          }
        }
      ]
    }
    ```

    Note: The response preserves the order of IDs from the request.
    Missing or invalid IDs are silently skipped.
    """
    candidates = CandidateService.get_batch_candidate_details(
        db, batch_request.ids, batch_request.include_stats
    )
    return BatchDetailsResponse(candidates=candidates)
