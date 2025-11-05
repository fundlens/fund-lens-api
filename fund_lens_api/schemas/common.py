"""Common Pydantic schemas shared across the API."""

from datetime import date
from typing import Generic, TypeVar

from pydantic import BaseModel, ConfigDict, Field

# Generic type for paginated responses
T = TypeVar("T")


class PaginationParams(BaseModel):
    """Query parameters for pagination."""

    page: int = Field(
        default=1,
        ge=1,
        description="Page number (1-indexed)",
    )
    page_size: int = Field(
        default=50,
        ge=1,
        le=1000,
        description="Items per page (maximum: 1000)",
    )

    @property
    def offset(self) -> int:
        """Calculate SQL offset from page number."""
        return (self.page - 1) * self.page_size


class PaginationMeta(BaseModel):
    """Pagination metadata in responses."""

    page: int = Field(description="Current page number")
    page_size: int = Field(description="Items per page")
    total_items: int = Field(description="Total number of items")
    total_pages: int = Field(description="Total number of pages")
    has_next: bool = Field(description="Whether there is a next page")
    has_prev: bool = Field(description="Whether there is a previous page")


class PaginatedResponse(BaseModel, Generic[T]):
    """Generic paginated response wrapper."""

    items: list[T] = Field(description="List of items for current page")
    meta: PaginationMeta = Field(description="Pagination metadata")

    model_config = ConfigDict(from_attributes=True)


class DateRangeFilter(BaseModel):
    """Filter for date ranges."""

    start_date: date | None = Field(default=None, description="Start date (inclusive)")
    end_date: date | None = Field(default=None, description="End date (inclusive)")

    def is_empty(self) -> bool:
        """Check if filter is empty (no dates specified)."""
        return self.start_date is None and self.end_date is None


class AmountRangeFilter(BaseModel):
    """Filter for amount ranges."""

    min_amount: float | None = Field(default=None, ge=0, description="Minimum amount")
    max_amount: float | None = Field(default=None, ge=0, description="Maximum amount")

    def is_empty(self) -> bool:
        """Check if filter is empty (no amounts specified)."""
        return self.min_amount is None and self.max_amount is None


class SearchParams(BaseModel):
    """Common search parameters."""

    query: str = Field(min_length=1, max_length=500, description="Search query string")


def create_pagination_meta(
        page: int,
        page_size: int,
        total_items: int,
) -> PaginationMeta:
    """Helper to create pagination metadata."""
    total_pages = (total_items + page_size - 1) // page_size if total_items > 0 else 0

    return PaginationMeta(
        page=page,
        page_size=page_size,
        total_items=total_items,
        total_pages=total_pages,
        has_next=page < total_pages,
        has_prev=page > 1,
    )


class UnifiedSearchResponse(BaseModel):
    """Response for unified search endpoint combining multiple resource types."""

    candidates: PaginatedResponse | None = Field(
        default=None, description="Candidate search results"
    )
    contributors: PaginatedResponse | None = Field(
        default=None, description="Contributor search results"
    )
    committees: PaginatedResponse | None = Field(
        default=None, description="Committee search results"
    )

    model_config = ConfigDict(from_attributes=True)
