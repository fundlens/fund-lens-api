"""Service layer for contributor operations."""

from typing import Any, cast

from fund_lens_models.gold import GoldContribution, GoldContributor
from sqlalchemy import func, select
from sqlalchemy.orm import Session
from sqlalchemy.orm.attributes import InstrumentedAttribute

from fund_lens_api.schemas.contributor import ContributorFilters, ContributorStats


class ContributorService:
    """Business logic for contributor operations."""

    @staticmethod
    def list_contributors(
            db: Session,
            filters: ContributorFilters,
            offset: int = 0,
            limit: int = 50,
    ) -> tuple[list[GoldContributor], int]:
        """List contributors with filtering and pagination.

        Returns:
            Tuple of (contributors list, total count)
        """
        # Build base query
        query = select(GoldContributor)
        count_query = select(func.count()).select_from(GoldContributor)

        # Apply exact match filters
        filter_dict = filters.to_filter_dict()
        exact_match_fields = {"state", "city", "entity_type"}

        for field in exact_match_fields:
            if field in filter_dict:
                column = cast(InstrumentedAttribute[Any], getattr(GoldContributor, field))
                query = query.where(column == filter_dict[field])
                count_query = count_query.where(column == filter_dict[field])

        # Apply partial match filters (employer, occupation)
        if filters.employer:
            query = query.where(GoldContributor.employer.ilike(f"%{filters.employer}%"))
            count_query = count_query.where(GoldContributor.employer.ilike(f"%{filters.employer}%"))

        if filters.occupation:
            query = query.where(GoldContributor.occupation.ilike(f"%{filters.occupation}%"))
            count_query = count_query.where(
                GoldContributor.occupation.ilike(f"%{filters.occupation}%")
            )

        # Get total count
        total_count = db.execute(count_query).scalar_one()

        # Apply pagination and ordering
        query = query.order_by(GoldContributor.name).offset(offset).limit(limit)

        # Execute query
        contributors = db.execute(query).scalars().all()

        return list(contributors), total_count

    @staticmethod
    def get_contributor_by_id(db: Session, contributor_id: int) -> GoldContributor | None:
        """Get a single contributor by ID."""
        query = select(GoldContributor).where(GoldContributor.id == contributor_id)
        return db.execute(query).scalar_one_or_none()

    @staticmethod
    def search_contributors(
            db: Session,
            search_query: str,
            offset: int = 0,
            limit: int = 50,
    ) -> tuple[list[GoldContributor], int]:
        """Search contributors by name.

        Returns:
            Tuple of (contributors list, total count)
        """
        # Build search query using case-insensitive LIKE
        search_pattern = f"%{search_query}%"
        query = select(GoldContributor).where(GoldContributor.name.ilike(search_pattern))
        count_query = select(func.count()).select_from(GoldContributor).where(
            GoldContributor.name.ilike(search_pattern)
        )

        # Get total count
        total_count = db.execute(count_query).scalar_one()

        # Apply pagination and ordering
        query = query.order_by(GoldContributor.name).offset(offset).limit(limit)

        # Execute query
        contributors = db.execute(query).scalars().all()

        return list(contributors), total_count

    @staticmethod
    def get_contributor_stats(db: Session, contributor_id: int) -> ContributorStats | None:
        """Get aggregated statistics for a contributor."""
        # Verify contributor exists
        contributor = ContributorService.get_contributor_by_id(db, contributor_id)
        if not contributor:
            return None

        # Query contribution statistics
        stats_query = select(
            func.count(GoldContribution.id).label("total_contributions"),
            func.coalesce(func.sum(GoldContribution.amount), 0).label("total_amount"),
            func.count(func.distinct(GoldContribution.recipient_committee_id)).label(
                "unique_recipients"
            ),
            func.coalesce(func.avg(GoldContribution.amount), 0).label("avg_contribution"),
            func.min(GoldContribution.contribution_date).label("first_contribution_date"),
            func.max(GoldContribution.contribution_date).label("last_contribution_date"),
        ).where(GoldContribution.contributor_id == contributor_id)

        result = db.execute(stats_query).one()

        return ContributorStats(
            contributor_id=contributor_id,
            total_contributions=result.total_contributions,
            total_amount=float(result.total_amount),
            unique_recipients=result.unique_recipients,
            avg_contribution=float(result.avg_contribution),
            first_contribution_date=result.first_contribution_date,
            last_contribution_date=result.last_contribution_date,
        )

    @staticmethod
    def get_top_contributors(
            db: Session,
            limit: int = 10,
            state: str | None = None,
    ) -> list[tuple[GoldContributor, float]]:
        """Get top contributors by total amount.

        Returns:
            List of (contributor, total_amount) tuples
        """
        # Build query to get contributors with their total contribution amounts
        query = (
            select(
                GoldContributor,
                func.sum(GoldContribution.amount).label("total_amount"),
            )
            .join(GoldContribution, GoldContribution.contributor_id == GoldContributor.id)
            .group_by(GoldContributor.id)
            .order_by(func.sum(GoldContribution.amount).desc())
            .limit(limit)
        )

        # Apply state filter if provided
        if state:
            query = query.where(GoldContributor.state == state)

        results = db.execute(query).all()
        return [(row[0], float(row[1])) for row in results]
