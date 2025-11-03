"""Pydantic schemas for state-level resources."""

from decimal import Decimal

from pydantic import BaseModel, Field

from fund_lens_api.schemas.candidate import CandidateList


class RaceSummary(BaseModel):
    """Summary of a race (office type) within a state."""

    office: str = Field(description="Office type (H, S, P, etc.)")
    districts: list[str] | None = Field(description="List of districts (for House races)")
    candidate_count: int = Field(description="Total number of candidates")
    active_candidate_count: int = Field(description="Number of active candidates")
    total_raised: Decimal = Field(description="Total amount raised across all candidates")
    top_fundraisers: list[CandidateList] = Field(
        description="Top fundraising candidates for this race"
    )


class StateSummary(BaseModel):
    """Comprehensive summary of campaign finance data for a state."""

    state: str = Field(description="Two-letter state code")
    total_candidates: int = Field(description="Total number of candidates")
    active_candidates: int = Field(description="Number of active candidates")
    total_raised: Decimal = Field(description="Total amount raised across all candidates")
    total_contributions: int = Field(description="Total number of contributions")
    unique_contributors: int = Field(description="Number of unique contributors")
    races: list[RaceSummary] = Field(description="Summary by race type")
