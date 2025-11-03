"""Pydantic schemas for race resources."""

from decimal import Decimal

from pydantic import BaseModel, Field

from fund_lens_api.schemas.candidate import CandidateStats


class RaceCandidate(BaseModel):
    """Candidate information within a race context."""

    id: int = Field(description="Candidate ID")
    name: str = Field(description="Candidate name")
    party: str | None = Field(description="Political party")
    is_active: bool = Field(description="Whether candidate is currently active")
    stats: CandidateStats | None = Field(
        default=None, description="Fundraising statistics"
    )


class RaceSummary(BaseModel):
    """Summary statistics for a race."""

    total_candidates: int = Field(description="Total number of candidates in race")
    active_candidates: int = Field(description="Number of active candidates")
    total_amount_raised: Decimal = Field(
        description="Total amount raised across all candidates"
    )
    total_contributions: int = Field(
        description="Total number of contributions to all candidates"
    )
    unique_contributors: int = Field(
        description="Number of unique contributors to this race"
    )


class SenateRaceResponse(BaseModel):
    """Response for a Senate race."""

    state: str = Field(description="State (two-letter code)")
    office: str = Field(default="S", description="Office type (Senate)")
    summary: RaceSummary = Field(description="Race summary statistics")
    candidates: list[RaceCandidate] = Field(description="Candidates in this race")


class HouseRaceResponse(BaseModel):
    """Response for a House race."""

    state: str = Field(description="State (two-letter code)")
    district: str = Field(description="Congressional district")
    office: str = Field(default="H", description="Office type (House)")
    summary: RaceSummary = Field(description="Race summary statistics")
    candidates: list[RaceCandidate] = Field(description="Candidates in this race")


class PresidentialRaceResponse(BaseModel):
    """Response for the Presidential race."""

    office: str = Field(default="P", description="Office type (President)")
    summary: RaceSummary = Field(description="Race summary statistics")
    candidates: list[RaceCandidate] = Field(description="Candidates in this race")
