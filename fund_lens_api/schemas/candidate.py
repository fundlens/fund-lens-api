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

    def to_filter_dict(self) -> dict[str, str | bool]:
        """Convert to dict of non-None filters for SQLAlchemy queries."""
        return {k: v for k, v in self.model_dump().items() if v is not None}


class CandidateStats(BaseModel):
    """Aggregated statistics for a candidate."""

    candidate_id: int = Field(description="Candidate ID")
    total_contributions: int = Field(description="Total number of contributions received")
    total_amount: float = Field(description="Total amount raised")
    unique_contributors: int = Field(description="Number of unique contributors")
    avg_contribution: float = Field(description="Average contribution amount")

    model_config = ConfigDict(from_attributes=True)
