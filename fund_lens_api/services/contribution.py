"""Service layer for contribution operations."""

from typing import Any, cast

from fund_lens_models.gold import GoldCandidate, GoldCommittee, GoldContribution, GoldContributor
from sqlalchemy import func, select
from sqlalchemy.orm import Session
from sqlalchemy.orm.attributes import InstrumentedAttribute

from fund_lens_api.schemas.contribution import ContributionFilters, ContributionSummary


class ContributionService:
    """Business logic for contribution operations."""

    @staticmethod
    def list_contributions(
            db: Session,
            filters: ContributionFilters,
            offset: int = 0,
            limit: int = 50,
    ) -> tuple[list[GoldContribution], int]:
        """List contributions with filtering and pagination.

        Returns:
            Tuple of (contributions list, total count)
        """
        # Build base query
        query = select(GoldContribution)
        count_query = select(func.count()).select_from(GoldContribution)

        # Apply simple filters
        filter_dict = filters.to_filter_dict()
        for field, value in filter_dict.items():
            column = cast(InstrumentedAttribute[Any], getattr(GoldContribution, field))
            query = query.where(column == value)
            count_query = count_query.where(column == value)

        # Apply date range filters
        if filters.start_date:
            query = query.where(GoldContribution.contribution_date >= filters.start_date)
            count_query = count_query.where(GoldContribution.contribution_date >= filters.start_date)

        if filters.end_date:
            query = query.where(GoldContribution.contribution_date <= filters.end_date)
            count_query = count_query.where(GoldContribution.contribution_date <= filters.end_date)

        # Apply amount range filters
        if filters.min_amount:
            query = query.where(GoldContribution.amount >= filters.min_amount)
            count_query = count_query.where(GoldContribution.amount >= filters.min_amount)

        if filters.max_amount:
            query = query.where(GoldContribution.amount <= filters.max_amount)
            count_query = count_query.where(GoldContribution.amount <= filters.max_amount)

        # Get total count
        total_count = db.execute(count_query).scalar_one()

        # Apply pagination and ordering (most recent first)
        query = (
            query.order_by(GoldContribution.contribution_date.desc(), GoldContribution.id.desc())
            .offset(offset)
            .limit(limit)
        )

        # Execute query
        contributions = db.execute(query).scalars().all()

        return list(contributions), total_count

    @staticmethod
    def get_contribution_by_id(db: Session, contribution_id: int) -> GoldContribution | None:
        """Get a single contribution by ID."""
        query = select(GoldContribution).where(GoldContribution.id == contribution_id)
        return db.execute(query).scalar_one_or_none()

    @staticmethod
    def get_contribution_with_relations(
            db: Session, contribution_id: int
    ) -> tuple[GoldContribution, GoldContributor, GoldCommittee, GoldCandidate | None] | None:
        """Get contribution with related entities (contributor, committee, candidate).

        Returns:
            Tuple of (contribution, contributor, committee, candidate) or None if not found
        """
        # Get the contribution
        contribution = ContributionService.get_contribution_by_id(db, contribution_id)
        if not contribution:
            return None

        # Get related entities
        contributor = db.execute(
            select(GoldContributor).where(GoldContributor.id == contribution.contributor_id)
        ).scalar_one()

        committee = db.execute(
            select(GoldCommittee).where(GoldCommittee.id == contribution.recipient_committee_id)
        ).scalar_one()

        candidate = None
        if contribution.recipient_candidate_id:
            candidate = db.execute(
                select(GoldCandidate).where(
                    GoldCandidate.id == contribution.recipient_candidate_id
                )
            ).scalar_one_or_none()

        return contribution, contributor, committee, candidate

    @staticmethod
    def get_contribution_summary(db: Session, filters: ContributionFilters) -> ContributionSummary:
        """Get aggregated statistics for contributions matching filters."""
        # Build base query with filters
        query = select(GoldContribution)

        # Apply simple filters
        filter_dict = filters.to_filter_dict()
        for field, value in filter_dict.items():
            column = cast(InstrumentedAttribute[Any], getattr(GoldContribution, field))
            query = query.where(column == value)

        # Apply date range filters
        if filters.start_date:
            query = query.where(GoldContribution.contribution_date >= filters.start_date)

        if filters.end_date:
            query = query.where(GoldContribution.contribution_date <= filters.end_date)

        # Apply amount range filters
        if filters.min_amount:
            query = query.where(GoldContribution.amount >= filters.min_amount)

        if filters.max_amount:
            query = query.where(GoldContribution.amount <= filters.max_amount)

        # Get statistics
        stats_query = select(
            func.count(GoldContribution.id).label("total_contributions"),
            func.coalesce(func.sum(GoldContribution.amount), 0).label("total_amount"),
            func.coalesce(func.avg(GoldContribution.amount), 0).label("avg_contribution"),
            func.coalesce(func.min(GoldContribution.amount), 0).label("min_contribution"),
            func.coalesce(func.max(GoldContribution.amount), 0).label("max_contribution"),
        ).select_from(query.subquery())

        result = db.execute(stats_query).one()

        return ContributionSummary(
            total_contributions=result.total_contributions,
            total_amount=result.total_amount,
            avg_contribution=result.avg_contribution,
            min_contribution=result.min_contribution,
            max_contribution=result.max_contribution,
        )

    @staticmethod
    def get_contributions_by_contributor(
            db: Session,
            contributor_id: int,
            offset: int = 0,
            limit: int = 50,
    ) -> tuple[list[GoldContribution], int]:
        """Get all contributions from a specific contributor."""
        query = select(GoldContribution).where(GoldContribution.contributor_id == contributor_id)
        count_query = select(func.count()).select_from(GoldContribution).where(
            GoldContribution.contributor_id == contributor_id
        )

        total_count = db.execute(count_query).scalar_one()

        query = (
            query.order_by(GoldContribution.contribution_date.desc())
            .offset(offset)
            .limit(limit)
        )

        contributions = db.execute(query).scalars().all()
        return list(contributions), total_count

    @staticmethod
    def get_contributions_by_committee(
            db: Session,
            committee_id: int,
            offset: int = 0,
            limit: int = 50,
    ) -> tuple[list[GoldContribution], int]:
        """Get all contributions to a specific committee."""
        query = select(GoldContribution).where(
            GoldContribution.recipient_committee_id == committee_id
        )
        count_query = select(func.count()).select_from(GoldContribution).where(
            GoldContribution.recipient_committee_id == committee_id
        )

        total_count = db.execute(count_query).scalar_one()

        query = (
            query.order_by(GoldContribution.contribution_date.desc())
            .offset(offset)
            .limit(limit)
        )

        contributions = db.execute(query).scalars().all()
        return list(contributions), total_count

    @staticmethod
    def get_contributions_by_candidate(
            db: Session,
            candidate_id: int,
            offset: int = 0,
            limit: int = 50,
    ) -> tuple[list[GoldContribution], int]:
        """Get all contributions to a specific candidate."""
        query = select(GoldContribution).where(
            GoldContribution.recipient_candidate_id == candidate_id
        )
        count_query = select(func.count()).select_from(GoldContribution).where(
            GoldContribution.recipient_candidate_id == candidate_id
        )

        total_count = db.execute(count_query).scalar_one()

        query = (
            query.order_by(GoldContribution.contribution_date.desc())
            .offset(offset)
            .limit(limit)
        )

        contributions = db.execute(query).scalars().all()
        return list(contributions), total_count
