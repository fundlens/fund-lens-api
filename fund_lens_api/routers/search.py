"""API routes for unified search across multiple resource types."""

from typing import Annotated

from fastapi import APIRouter, Depends, Query, Request

from fund_lens_api.dependencies import DBSession
from fund_lens_api.rate_limit import RATE_LIMIT_SEARCH, limiter
from fund_lens_api.schemas.candidate import CandidateList
from fund_lens_api.schemas.committee import CommitteeList
from fund_lens_api.schemas.common import PaginatedResponse, UnifiedSearchResponse, create_pagination_meta
from fund_lens_api.schemas.contributor import ContributorList
from fund_lens_api.services.candidate import CandidateService
from fund_lens_api.services.committee import CommitteeService
from fund_lens_api.services.contributor import ContributorService

router = APIRouter(prefix="/search", tags=["search"])


# noinspection PyUnusedLocal
@router.get("", response_model=UnifiedSearchResponse)
@limiter.limit(RATE_LIMIT_SEARCH)
def unified_search(
    request: Request,
    db: DBSession,
    q: Annotated[str, Query(min_length=1, description="Search query")] = "",
    categories: Annotated[
        str | None,
        Query(
            description="Comma-separated list of categories to search (candidates,contributors,committees). "
            "If not specified, searches all categories."
        ),
    ] = None,
    page_size: Annotated[
        int, Query(ge=1, le=1000, description="Number of results per category")
    ] = 10,
) -> UnifiedSearchResponse:
    """Unified search endpoint that searches across multiple resource types.

    This endpoint reduces the need for multiple API calls by allowing the client to search
    candidates, contributors, and committees in a single request.

    Example:
    - /search?q=smith - Searches all categories
    - /search?q=smith&categories=candidates,contributors - Searches only specified categories
    - /search?q=smith&page_size=5 - Returns up to 5 results per category
    """
    # Parse categories (default to all if not specified)
    if categories:
        category_list = [c.strip().lower() for c in categories.split(",")]
    else:
        category_list = ["candidates", "contributors", "committees"]

    response = UnifiedSearchResponse()

    # Search candidates if requested
    if "candidates" in category_list:
        candidates, total_candidates = CandidateService.search_candidates(
            db, search_query=q, offset=0, limit=page_size
        )
        response.candidates = PaginatedResponse(
            items=[CandidateList.model_validate(c) for c in candidates],
            meta=create_pagination_meta(
                page=1, page_size=page_size, total_items=total_candidates
            ),
        )

    # Search contributors if requested
    if "contributors" in category_list:
        contributors, total_contributors = ContributorService.search_contributors(
            db, search_query=q, offset=0, limit=page_size
        )
        response.contributors = PaginatedResponse(
            items=[ContributorList.model_validate(c) for c in contributors],
            meta=create_pagination_meta(
                page=1, page_size=page_size, total_items=total_contributors
            ),
        )

    # Search committees if requested
    if "committees" in category_list:
        committees, total_committees = CommitteeService.search_committees(
            db, search_query=q, offset=0, limit=page_size
        )
        response.committees = PaginatedResponse(
            items=[CommitteeList.model_validate(c) for c in committees],
            meta=create_pagination_meta(
                page=1, page_size=page_size, total_items=total_committees
            ),
        )

    return response
