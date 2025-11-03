"""Pydantic schemas for contributor resources."""

from datetime import date, datetime
from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field


class ContributorBase(BaseModel):
    """Base contributor fields shared across schemas."""

    id: int = Field(description="Internal contributor ID")
    name: str = Field(description="Contributor full name")
    first_name: str | None = Field(description="First name")
    last_name: str | None = Field(description="Last name")
    city: str | None = Field(description="City")
    state: str | None = Field(description="State (two-letter code)")
    zip: str | None = Field(description="ZIP code (5 digits)")
    entity_type: str = Field(description="Entity type (e.g., INDIVIDUAL, COMMITTEE, ORG)")

    model_config = ConfigDict(from_attributes=True)


class ContributorList(ContributorBase):
    """Contributor summary for list views."""

    employer: str | None = Field(description="Employer name")
    occupation: str | None = Field(description="Occupation")


class ContributorDetail(ContributorList):
    """Detailed contributor information."""

    match_confidence: float | None = Field(description="Deduplication confidence score (0.0-1.0)")
    created_at: datetime = Field(description="Record creation timestamp")
    updated_at: datetime = Field(description="Record update timestamp")


class ContributorFilters(BaseModel):
    """Query parameters for filtering contributors."""

    state: str | None = Field(default=None, description="Filter by state (two-letter code)")
    city: str | None = Field(default=None, description="Filter by city")
    entity_type: str | None = Field(default=None, description="Filter by entity type")
    employer: str | None = Field(default=None, description="Filter by employer (partial match)")
    occupation: str | None = Field(default=None, description="Filter by occupation (partial match)")

    def to_filter_dict(self) -> dict[str, str]:
        """Convert to dict of non-None filters for SQLAlchemy queries."""
        return {k: v for k, v in self.model_dump().items() if v is not None}


class ContributorStats(BaseModel):
    """Aggregated statistics for a contributor."""

    contributor_id: int = Field(description="Contributor ID")
    total_contributions: int = Field(description="Total number of contributions made")
    total_amount: float = Field(description="Total amount contributed")
    unique_recipients: int = Field(description="Number of unique recipients")
    avg_contribution: float = Field(description="Average contribution amount")
    first_contribution_date: datetime | None = Field(description="Date of first contribution")
    last_contribution_date: datetime | None = Field(description="Date of most recent contribution")

    model_config = ConfigDict(from_attributes=True)


# New schemas for by-candidate/by-committee endpoints


class ContributionSimple(BaseModel):
    """Simplified contribution record for nested display."""

    id: int = Field(description="Contribution ID")
    contribution_date: date = Field(description="Contribution date", alias="date")
    amount: Decimal = Field(description="Contribution amount")
    contribution_type: str = Field(description="Contribution type")

    model_config = ConfigDict(from_attributes=True, populate_by_name=True)


class ContributorWithAggregates(BaseModel):
    """Contributor with aggregated stats and optional contribution details."""

    contributor_id: int = Field(description="Contributor ID")
    contributor_name: str = Field(description="Contributor name")
    city: str | None = Field(description="City")
    state: str | None = Field(description="State")
    zip: str | None = Field(description="ZIP code")
    entity_type: str = Field(description="Entity type")
    employer: str | None = Field(description="Employer")
    occupation: str | None = Field(description="Occupation")
    total_amount: Decimal = Field(description="Total amount contributed to this recipient")
    contribution_count: int = Field(description="Number of contributions to this recipient")
    first_contribution_date: date | None = Field(description="First contribution date")
    last_contribution_date: date | None = Field(description="Last contribution date")
    contributions: list[ContributionSimple] | None = Field(
        default=None, description="Individual contributions (if requested)"
    )


class CandidateSummary(BaseModel):
    """Candidate summary for response headers."""

    id: int
    name: str
    office: str
    state: str | None
    district: str | None
    party: str | None

    model_config = ConfigDict(from_attributes=True)


class CommitteeSummary(BaseModel):
    """Committee summary for response headers."""

    id: int
    name: str
    committee_type: str
    state: str | None
    candidate_id: int | None
    candidate_name: str | None = None

    model_config = ConfigDict(from_attributes=True)


class ContributorsSummary(BaseModel):
    """Summary statistics for contributors response."""

    total_contributors: int = Field(description="Total number of unique contributors")
    total_amount_raised: Decimal = Field(description="Total amount raised")
    total_contributions: int = Field(description="Total number of contributions")
    first_contribution: date | None = Field(description="Date of first contribution")
    last_contribution: date | None = Field(description="Date of last contribution")


class DateRange(BaseModel):
    """Date range for summary statistics."""

    first_contribution: date | None
    last_contribution: date | None


class ContributorsByCandidateResponse(BaseModel):
    """Response for contributors by candidate endpoint."""

    candidate: CandidateSummary
    summary: ContributorsSummary
    contributors: list[ContributorWithAggregates]
    meta: dict  # Pagination metadata


class ContributorsByCommitteeResponse(BaseModel):
    """Response for contributors by committee endpoint."""

    committee: CommitteeSummary
    summary: ContributorsSummary
    contributors: list[ContributorWithAggregates]
    meta: dict  # Pagination metadata


class ContributorSearchAggregated(BaseModel):
    """Aggregated contributor search result."""

    contributor_id: int = Field(description="Contributor ID")
    contributor_name: str = Field(description="Contributor name")
    city: str | None = Field(description="City")
    state: str | None = Field(description="State")
    zip: str | None = Field(description="ZIP code")
    entity_type: str = Field(description="Entity type")
    employer: str | None = Field(description="Employer")
    occupation: str | None = Field(description="Occupation")
    total_amount: Decimal = Field(description="Total amount contributed across all recipients")
    contribution_count: int = Field(description="Total number of contributions")
    unique_recipients: int = Field(description="Number of unique recipients")
    unique_candidates: int = Field(description="Number of unique candidates")
    unique_committees: int = Field(description="Number of unique committees")
    first_contribution_date: date | None = Field(description="First contribution date")
    last_contribution_date: date | None = Field(description="Last contribution date")
