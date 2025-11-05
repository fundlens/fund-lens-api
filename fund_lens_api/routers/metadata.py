"""API routes for metadata resources."""

from typing import Annotated

from fastapi import APIRouter, Depends, Query, Request, Response

from fund_lens_api.dependencies import DBSession
from fund_lens_api.rate_limit import RATE_LIMIT_DEFAULT, limiter
from fund_lens_api.schemas.metadata import (
    CommitteeTypeMetadata,
    EntityTypeMetadata,
    OfficeMetadata,
    StateMetadata,
)
from fund_lens_api.services.metadata import MetadataService

router = APIRouter(prefix="/metadata", tags=["metadata"])

# Cache duration for metadata endpoints (1 hour)
METADATA_CACHE_MAX_AGE = 3600


# noinspection PyUnusedLocal
@router.get("/states", response_model=list[StateMetadata])
@limiter.limit(RATE_LIMIT_DEFAULT)
def get_all_states(
        request: Request,
        response: Response,
        db: DBSession,
        include_names: Annotated[bool, Query(description="Include full state names")] = True,
) -> list[StateMetadata]:
    """Get all unique states across all resources.

    Returns states that appear in contributors, committees, or candidates.
    """
    # Set cache headers
    response.headers["Cache-Control"] = f"public, max-age={METADATA_CACHE_MAX_AGE}"

    # Get states from all resources
    contributor_states = set(MetadataService.get_contributor_states(db))
    committee_states = set(MetadataService.get_committee_states(db))
    candidate_states = set(MetadataService.get_candidate_states(db))

    # Combine and sort
    all_states = sorted(contributor_states | committee_states | candidate_states)

    return [
        StateMetadata(
            code=state,
            name=MetadataService.get_state_name(state),
        )
        for state in all_states
    ]


# noinspection PyUnusedLocal
@router.get("/contributors/states", response_model=list[str] | list[StateMetadata])
@limiter.limit(RATE_LIMIT_DEFAULT)
def get_contributor_states(
        request: Request,
        response: Response,
        db: DBSession,
        include_names: Annotated[bool, Query(description="Include full state names")] = False,
) -> list[str] | list[StateMetadata]:
    """Get list of unique states that have contributors.

    Returns:
    - If include_names=false: ["AL", "AK", "AZ", ...]
    - If include_names=true: [{"code": "AL", "name": "Alabama"}, ...]
    """
    # Set cache headers
    response.headers["Cache-Control"] = f"public, max-age={METADATA_CACHE_MAX_AGE}"

    if include_names:
        return MetadataService.get_contributor_states_with_names(db)
    return MetadataService.get_contributor_states(db)


# noinspection PyUnusedLocal
@router.get("/contributors/entity-types", response_model=list[str] | list[EntityTypeMetadata])
@limiter.limit(RATE_LIMIT_DEFAULT)
def get_contributor_entity_types(
        request: Request,
        response: Response,
        db: DBSession,
        include_labels: Annotated[bool, Query(description="Include descriptive labels")] = False,
) -> list[str] | list[EntityTypeMetadata]:
    """Get list of unique contributor entity types.

    Returns:
    - If include_labels=false: ["IND", "ORG", "PAC", ...]
    - If include_labels=true: [{"code": "IND", "label": "Individual"}, ...]
    """
    # Set cache headers
    response.headers["Cache-Control"] = f"public, max-age={METADATA_CACHE_MAX_AGE}"

    if include_labels:
        return MetadataService.get_contributor_entity_types_with_labels(db)
    return MetadataService.get_contributor_entity_types(db)


# noinspection PyUnusedLocal
@router.get("/committees/states", response_model=list[str] | list[StateMetadata])
@limiter.limit(RATE_LIMIT_DEFAULT)
def get_committee_states(
        request: Request,
        response: Response,
        db: DBSession,
        include_names: Annotated[bool, Query(description="Include full state names")] = False,
) -> list[str] | list[StateMetadata]:
    """Get list of unique states that have committees.

    Returns:
    - If include_names=false: ["AL", "AK", "AZ", ...]
    - If include_names=true: [{"code": "AL", "name": "Alabama"}, ...]
    """
    # Set cache headers
    response.headers["Cache-Control"] = f"public, max-age={METADATA_CACHE_MAX_AGE}"

    if include_names:
        return MetadataService.get_committee_states_with_names(db)
    return MetadataService.get_committee_states(db)


# noinspection PyUnusedLocal
@router.get("/committees/types", response_model=list[str] | list[CommitteeTypeMetadata])
@limiter.limit(RATE_LIMIT_DEFAULT)
def get_committee_types(
        request: Request,
        response: Response,
        db: DBSession,
        include_labels: Annotated[bool, Query(description="Include descriptive labels")] = False,
) -> list[str] | list[CommitteeTypeMetadata]:
    """Get list of unique committee types.

    Returns:
    - If include_labels=false: ["H", "S", "P", ...]
    - If include_labels=true: [{"code": "H", "label": "House"}, ...]
    """
    # Set cache headers
    response.headers["Cache-Control"] = f"public, max-age={METADATA_CACHE_MAX_AGE}"

    if include_labels:
        return MetadataService.get_committee_types_with_labels(db)
    return MetadataService.get_committee_types(db)


# noinspection PyUnusedLocal
@router.get("/candidates/states", response_model=list[str] | list[StateMetadata])
@limiter.limit(RATE_LIMIT_DEFAULT)
def get_candidate_states(
        request: Request,
        response: Response,
        db: DBSession,
        include_names: Annotated[bool, Query(description="Include full state names")] = False,
) -> list[str] | list[StateMetadata]:
    """Get list of unique states that have candidates.

    Returns:
    - If include_names=false: ["AL", "AK", "AZ", ...]
    - If include_names=true: [{"code": "AL", "name": "Alabama"}, ...]
    """
    # Set cache headers
    response.headers["Cache-Control"] = f"public, max-age={METADATA_CACHE_MAX_AGE}"

    if include_names:
        return MetadataService.get_candidate_states_with_names(db)
    return MetadataService.get_candidate_states(db)


# noinspection PyUnusedLocal
@router.get("/candidates/offices", response_model=list[str] | list[OfficeMetadata])
@limiter.limit(RATE_LIMIT_DEFAULT)
def get_candidate_offices(
        request: Request,
        response: Response,
        db: DBSession,
        include_labels: Annotated[bool, Query(description="Include descriptive labels")] = False,
) -> list[str] | list[OfficeMetadata]:
    """Get list of unique candidate offices.

    Returns:
    - If include_labels=false: ["H", "S", "P"]
    - If include_labels=true: [{"code": "H", "label": "U.S. House"}, ...]
    """
    # Set cache headers
    response.headers["Cache-Control"] = f"public, max-age={METADATA_CACHE_MAX_AGE}"

    if include_labels:
        return MetadataService.get_candidate_offices_with_labels(db)
    return MetadataService.get_candidate_offices(db)