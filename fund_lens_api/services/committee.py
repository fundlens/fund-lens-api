"""Service layer for committee operations."""

from typing import Any, cast

from fund_lens_models.gold import GoldCommittee, GoldContribution
from sqlalchemy import func, select
from sqlalchemy.orm import Session
from sqlalchemy.orm.attributes import InstrumentedAttribute

from fund_lens_api.schemas.committee import CommitteeFilters, CommitteeStats


class CommitteeService:
    """Business logic for committee operations."""

    @staticmethod
    def list_committees(
            db: Session,
            filters: CommitteeFilters,
            offset: int = 0,
            limit: int = 50,
    ) -> tuple[list[GoldCommittee], int]:
        """List committees with filtering and pagination.

        Returns:
            Tuple of (committees list, total count)
        """
        # Build base query
        query = select(GoldCommittee)
        count_query = select(func.count()).select_from(GoldCommittee)

        # Apply filters
        filter_dict = filters.to_filter_dict()
        for field, value in filter_dict.items():
            column = cast(InstrumentedAttribute[Any], getattr(GoldCommittee, field))
            query = query.where(column == value)
            count_query = count_query.where(column == value)

        # Get total count
        total_count = db.execute(count_query).scalar_one()

        # Apply pagination and ordering
        query = query.order_by(GoldCommittee.name).offset(offset).limit(limit)

        # Execute query
        committees = db.execute(query).scalars().all()

        return list(committees), total_count

    @staticmethod
    def get_committee_by_id(db: Session, committee_id: int) -> GoldCommittee | None:
        """Get a single committee by ID."""
        query = select(GoldCommittee).where(GoldCommittee.id == committee_id)
        return db.execute(query).scalar_one_or_none()

    @staticmethod
    def search_committees(
            db: Session,
            search_query: str,
            offset: int = 0,
            limit: int = 50,
    ) -> tuple[list[GoldCommittee], int]:
        """Search committees by name.

        Returns:
            Tuple of (committees list, total count)
        """
        # Build search query using case-insensitive LIKE
        search_pattern = f"%{search_query}%"
        query = select(GoldCommittee).where(GoldCommittee.name.ilike(search_pattern))
        count_query = select(func.count()).select_from(GoldCommittee).where(
            GoldCommittee.name.ilike(search_pattern)
        )

        # Get total count
        total_count = db.execute(count_query).scalar_one()

        # Apply pagination and ordering
        query = query.order_by(GoldCommittee.name).offset(offset).limit(limit)

        # Execute query
        committees = db.execute(query).scalars().all()

        return list(committees), total_count

    @staticmethod
    def get_committee_stats(db: Session, committee_id: int) -> CommitteeStats | None:
        """Get aggregated statistics for a committee."""
        # Verify committee exists
        committee = CommitteeService.get_committee_by_id(db, committee_id)
        if not committee:
            return None

        # Query contribution statistics
        stats_query = select(
            func.count(GoldContribution.id).label("total_contributions_received"),
            func.coalesce(func.sum(GoldContribution.amount), 0).label("total_amount_received"),
            func.count(func.distinct(GoldContribution.contributor_id)).label(
                "unique_contributors"
            ),
            func.coalesce(func.avg(GoldContribution.amount), 0).label("avg_contribution"),
        ).where(GoldContribution.recipient_committee_id == committee_id)

        result = db.execute(stats_query).one()

        return CommitteeStats(
            committee_id=committee_id,
            total_contributions_received=result.total_contributions_received,
            total_amount_received=float(result.total_amount_received),
            unique_contributors=result.unique_contributors,
            avg_contribution=float(result.avg_contribution),
        )

    @staticmethod
    def get_committees_by_candidate(db: Session, candidate_id: int) -> list[GoldCommittee]:
        """Get all committees associated with a candidate."""
        query = select(GoldCommittee).where(GoldCommittee.candidate_id == candidate_id).order_by(
            GoldCommittee.name
        )
        return list(db.execute(query).scalars().all())

    @staticmethod
    def get_committees_by_state(db: Session, state: str) -> list[GoldCommittee]:
        """Get all committees for a given state."""
        query = select(GoldCommittee).where(GoldCommittee.state == state).order_by(
            GoldCommittee.name
        )
        return list(db.execute(query).scalars().all())
