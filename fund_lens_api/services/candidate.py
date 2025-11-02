"""Service layer for candidate operations."""

from typing import Any, cast

from fund_lens_models.gold import GoldCandidate, GoldContribution
from sqlalchemy import func, select
from sqlalchemy.orm import Session
from sqlalchemy.orm.attributes import InstrumentedAttribute

from fund_lens_api.schemas.candidate import CandidateFilters, CandidateStats


class CandidateService:
    """Business logic for candidate operations."""

    @staticmethod
    def list_candidates(
            db: Session,
            filters: CandidateFilters,
            offset: int = 0,
            limit: int = 50,
    ) -> tuple[list[GoldCandidate], int]:
        """List candidates with filtering and pagination.

        Returns:
            Tuple of (candidates list, total count)
        """
        # Build base query
        query = select(GoldCandidate)
        count_query = select(func.count()).select_from(GoldCandidate)

        # Apply filters
        filter_dict = filters.to_filter_dict()
        for field, value in filter_dict.items():
            column = cast(InstrumentedAttribute[Any], getattr(GoldCandidate, field))
            query = query.where(column == value)
            count_query = count_query.where(column == value)

        # Get total count
        total_count = db.execute(count_query).scalar_one()

        # Apply pagination and ordering
        query = query.order_by(GoldCandidate.name).offset(offset).limit(limit)

        # Execute query
        candidates = db.execute(query).scalars().all()

        return list(candidates), total_count

    @staticmethod
    def get_candidate_by_id(db: Session, candidate_id: int) -> GoldCandidate | None:
        """Get a single candidate by ID."""
        query = select(GoldCandidate).where(GoldCandidate.id == candidate_id)
        return db.execute(query).scalar_one_or_none()

    @staticmethod
    def search_candidates(
            db: Session,
            search_query: str,
            offset: int = 0,
            limit: int = 50,
    ) -> tuple[list[GoldCandidate], int]:
        """Search candidates by name.

        Returns:
            Tuple of (candidates list, total count)
        """
        # Build search query using case-insensitive LIKE
        search_pattern = f"%{search_query}%"
        query = select(GoldCandidate).where(GoldCandidate.name.ilike(search_pattern))
        count_query = select(func.count()).select_from(GoldCandidate).where(
            GoldCandidate.name.ilike(search_pattern)
        )

        # Get total count
        total_count = db.execute(count_query).scalar_one()

        # Apply pagination and ordering
        query = query.order_by(GoldCandidate.name).offset(offset).limit(limit)

        # Execute query
        candidates = db.execute(query).scalars().all()

        return list(candidates), total_count

    @staticmethod
    def get_candidate_stats(db: Session, candidate_id: int) -> CandidateStats | None:
        """Get aggregated statistics for a candidate."""
        # Verify candidate exists
        candidate = CandidateService.get_candidate_by_id(db, candidate_id)
        if not candidate:
            return None

        # Query contribution statistics
        stats_query = select(
            func.count(GoldContribution.id).label("total_contributions"),
            func.coalesce(func.sum(GoldContribution.amount), 0).label("total_amount"),
            func.count(func.distinct(GoldContribution.contributor_id)).label(
                "unique_contributors"
            ),
            func.coalesce(func.avg(GoldContribution.amount), 0).label("avg_contribution"),
        ).where(GoldContribution.recipient_candidate_id == candidate_id)

        result = db.execute(stats_query).one()

        return CandidateStats(
            candidate_id=candidate_id,
            total_contributions=result.total_contributions,
            total_amount=float(result.total_amount),
            unique_contributors=result.unique_contributors,
            avg_contribution=float(result.avg_contribution),
        )

    @staticmethod
    def get_candidates_by_state(db: Session, state: str) -> list[GoldCandidate]:
        """Get all candidates for a given state."""
        query = select(GoldCandidate).where(GoldCandidate.state == state).order_by(
            GoldCandidate.office, GoldCandidate.name
        )
        return list(db.execute(query).scalars().all())

    @staticmethod
    def get_states_with_candidates(db: Session) -> list[str]:
        """Get list of states that have candidates."""
        query = (
            select(GoldCandidate.state)
            .where(GoldCandidate.state.isnot(None))
            .distinct()
            .order_by(GoldCandidate.state)
        )
        # Cast needed because type checker doesn't understand SQL None filter
        return cast(list[str], list(db.execute(query).scalars().all()))
