"""Pydantic schemas for contribution resources."""

from datetime import date, datetime
from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field


class ContributionBase(BaseModel):
    """Base contribution fields shared across schemas."""

    id: int = Field(description="Internal contribution ID")
    contribution_date: date = Field(description="Date of contribution")
    amount: Decimal = Field(description="Contribution amount")
    contribution_type: str = Field(description="Contribution type (e.g., DIRECT, EARMARKED)")
    election_type: str | None = Field(description="Election type (e.g., PRIMARY, GENERAL)")
    election_year: int = Field(description="Election year")
    election_cycle: int = Field(description="Election cycle")

    model_config = ConfigDict(from_attributes=True)


class ContributionList(ContributionBase):
    """Contribution summary for list views."""

    contributor_id: int = Field(description="Contributor ID")
    recipient_committee_id: int = Field(description="Recipient committee ID")
    recipient_candidate_id: int | None = Field(description="Recipient candidate ID (if applicable)")
    source_system: str = Field(description="Source system (e.g., FEC, MD_STATE)")


class ContributionDetail(ContributionList):
    """Detailed contribution information."""

    source_transaction_id: str = Field(description="Original transaction ID from source system")
    memo_text: str | None = Field(description="Memo text")
    created_at: datetime = Field(description="Record creation timestamp")
    updated_at: datetime = Field(description="Record update timestamp")


class ContributionWithRelations(ContributionDetail):
    """Contribution with nested contributor, committee, and candidate information."""

    contributor_name: str = Field(description="Contributor name")
    contributor_city: str | None = Field(description="Contributor city")
    contributor_state: str | None = Field(description="Contributor state")

    committee_name: str = Field(description="Committee name")
    committee_type: str = Field(description="Committee type")

    candidate_name: str | None = Field(description="Candidate name (if applicable)")
    candidate_office: str | None = Field(description="Candidate office (if applicable)")
    candidate_state: str | None = Field(description="Candidate state (if applicable)")


class ContributionFilters(BaseModel):
    """Query parameters for filtering contributions."""

    contributor_id: int | None = Field(default=None, description="Filter by contributor ID")
    recipient_committee_id: int | None = Field(
        default=None, description="Filter by recipient committee ID"
    )
    recipient_candidate_id: int | None = Field(
        default=None, description="Filter by recipient candidate ID"
    )
    contribution_type: str | None = Field(default=None, description="Filter by contribution type")
    election_type: str | None = Field(default=None, description="Filter by election type")
    election_year: int | None = Field(default=None, description="Filter by election year")
    election_cycle: int | None = Field(default=None, description="Filter by election cycle")
    source_system: str | None = Field(default=None, description="Filter by source system")

    # Date range filters
    start_date: date | None = Field(default=None, description="Start date (inclusive)")
    end_date: date | None = Field(default=None, description="End date (inclusive)")

    # Amount range filters
    min_amount: Decimal | None = Field(default=None, ge=0, description="Minimum amount")
    max_amount: Decimal | None = Field(default=None, ge=0, description="Maximum amount")

    def to_filter_dict(self) -> dict[str, int | str]:
        """Convert to dict of non-None simple filters for SQLAlchemy queries."""
        # Exclude range filters (handled separately in service layer)
        exclude = {"start_date", "end_date", "min_amount", "max_amount"}
        return {
            k: v
            for k, v in self.model_dump().items()
            if v is not None and k not in exclude
        }


class ContributionSummary(BaseModel):
    """Aggregated contribution statistics."""

    total_contributions: int = Field(description="Total number of contributions")
    total_amount: Decimal = Field(description="Total amount")
    avg_contribution: Decimal = Field(description="Average contribution amount")
    min_contribution: Decimal = Field(description="Minimum contribution amount")
    max_contribution: Decimal = Field(description="Maximum contribution amount")

    model_config = ConfigDict(from_attributes=True)
