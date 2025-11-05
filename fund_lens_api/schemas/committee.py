"""Pydantic schemas for committee resources."""

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field

from fund_lens_api.schemas.candidate import CandidateList


class CommitteeBase(BaseModel):
    """Base committee fields shared across schemas."""

    id: int = Field(description="Internal committee ID")
    name: str = Field(description="Committee name")
    committee_type: str = Field(description="Committee type (e.g., CANDIDATE, PAC, PARTY)")
    party: str | None = Field(description="Political party affiliation")
    state: str | None = Field(description="State (two-letter code)")
    city: str | None = Field(description="City")
    is_active: bool = Field(description="Whether committee is currently active")

    model_config = ConfigDict(from_attributes=True)


class CommitteeList(CommitteeBase):
    """Committee summary for list views."""

    candidate_id: int | None = Field(description="Associated candidate ID (if applicable)")


class CommitteeDetail(CommitteeList):
    """Detailed committee information."""

    fec_committee_id: str | None = Field(description="FEC committee ID")
    state_committee_id: str | None = Field(description="State-level committee ID")
    created_at: datetime = Field(description="Record creation timestamp")
    updated_at: datetime = Field(description="Record update timestamp")
    candidate: CandidateList | None = Field(
        default=None, description="Associated candidate details (if applicable)"
    )


class CommitteeFilters(BaseModel):
    """Query parameters for filtering committees."""

    state: str | None = Field(default=None, description="Filter by state (two-letter code)")
    committee_type: str | None = Field(default=None, description="Filter by committee type")
    party: str | None = Field(default=None, description="Filter by party")
    is_active: bool | None = Field(default=None, description="Filter by active status")
    candidate_id: int | None = Field(default=None, description="Filter by associated candidate ID")
    min_total_received: float | None = Field(
        default=None,
        ge=0,
        description="Minimum total amount received (filters by stats.total_amount_received)"
    )

    def to_filter_dict(self) -> dict[str, str | bool | int]:
        """Convert to dict of non-None filters for SQLAlchemy queries.

        Note: 'min_total_received' is excluded as it requires special handling.
        """
        return {k: v for k, v in self.model_dump().items() if v is not None and k != 'min_total_received'}


class CommitteeStats(BaseModel):
    """Aggregated statistics for a committee."""

    committee_id: int = Field(description="Committee ID")
    total_contributions_received: int = Field(description="Total contributions received")
    total_amount_received: float = Field(description="Total amount received")
    unique_contributors: int = Field(description="Number of unique contributors")
    avg_contribution: float = Field(description="Average contribution amount")

    model_config = ConfigDict(from_attributes=True)


class CommitteeWithStats(CommitteeList):
    """Committee with optional embedded statistics."""

    stats: CommitteeStats | None = Field(
        default=None, description="Aggregated fundraising statistics"
    )
