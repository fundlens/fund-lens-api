"""Pydantic schemas for candidate resources."""

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field


class CandidateBase(BaseModel):
    """Base candidate fields shared across schemas."""

    id: int = Field(description="Internal candidate ID")
    name: str = Field(description="Candidate full name")
    office: str = Field(description="Office sought (e.g., HOUSE, SENATE, PRESIDENT)")
    state: str | None = Field(description="State (two-letter code)")
    district: str | None = Field(description="District (for House races)")
    party: str | None = Field(description="Political party affiliation")
    is_active: bool = Field(description="Whether candidate is currently active")

    model_config = ConfigDict(from_attributes=True)


class CandidateList(CandidateBase):
    """Candidate summary for list views."""

    pass


class CandidateDetail(CandidateBase):
    """Detailed candidate information."""

    fec_candidate_id: str | None = Field(description="FEC candidate ID")
    state_candidate_id: str | None = Field(description="State-level candidate ID")
    created_at: datetime = Field(description="Record creation timestamp")
    updated_at: datetime = Field(description="Record update timestamp")


class CandidateFilters(BaseModel):
    """Query parameters for filtering candidates."""

    state: str | None = Field(default=None, description="Filter by state (two-letter code)")
    office: str | None = Field(default=None, description="Filter by office")
    party: str | None = Field(default=None, description="Filter by party")
    is_active: bool | None = Field(default=None, description="Filter by active status")
    district: str | None = Field(default=None, description="Filter by district")
    level: str | None = Field(default=None, description="Filter by level (federal or state)")

    def to_filter_dict(self) -> dict[str, str | bool]:
        """Convert to dict of non-None filters for SQLAlchemy queries.

        Note: 'level' is excluded as it requires special handling (IN clause).
        """
        return {k: v for k, v in self.model_dump().items() if v is not None and k != 'level'}


class CandidateStats(BaseModel):
    """Aggregated statistics for a candidate."""

    candidate_id: int = Field(description="Candidate ID")
    total_contributions: int = Field(description="Total number of contributions received")
    total_amount: float = Field(description="Total amount raised")
    unique_contributors: int = Field(description="Number of unique contributors")
    avg_contribution: float = Field(description="Average contribution amount")

    model_config = ConfigDict(from_attributes=True)


class CandidateWithStats(CandidateBase):
    """Candidate with optional embedded statistics."""

    stats: CandidateStats | None = Field(
        default=None, description="Aggregated fundraising statistics"
    )


class BulkStatsRequest(BaseModel):
    """Request schema for bulk stats endpoint."""

    candidate_ids: list[int] = Field(
        description="List of candidate IDs to fetch stats for", min_length=1, max_length=100
    )


class BulkStatsResponse(BaseModel):
    """Response schema for bulk stats endpoint."""

    stats: dict[int, CandidateStats] = Field(
        description="Dictionary mapping candidate_id to stats"
    )


class BatchDetailsRequest(BaseModel):
    """Request schema for batch candidate details endpoint."""

    ids: list[int] = Field(
        description="List of candidate IDs to fetch details for",
        min_length=1,
        max_length=100,
    )
    include_stats: bool = Field(
        default=False, description="Whether to include fundraising statistics"
    )


class BatchDetailsResponse(BaseModel):
    """Response schema for batch candidate details endpoint."""

    candidates: list[CandidateWithStats] = Field(
        description="List of candidates with optional stats"
    )
