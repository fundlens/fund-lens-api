"""Pydantic schemas for contributor resources."""

from datetime import datetime

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
