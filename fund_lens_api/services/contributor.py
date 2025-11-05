"""Service layer for contributor operations."""

from datetime import date
from decimal import Decimal
from typing import Any, Literal, cast

from fund_lens_models.gold import GoldCandidate, GoldCommittee, GoldContribution, GoldContributor
from sqlalchemy import and_, func, select
from sqlalchemy.orm import Session
from sqlalchemy.orm.attributes import InstrumentedAttribute

from fund_lens_api.schemas.contributor import (
    CandidateSummary,
    CommitteeSummary,
    ContributionSimple,
    ContributionWithCommittee,
    ContributorFilters,
    ContributorSearchAggregated,
    ContributorStats,
    ContributorWithAggregates,
    ContributorsByCandidateResponse,
    ContributorsByCommitteeResponse,
    ContributorsSummary,
)


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
    def search_contributors_enhanced(
            db: Session,
            search_query: str,
            state: str | None = None,
            entity_type: str | None = None,
            employer: str | None = None,
            occupation: str | None = None,
            offset: int = 0,
            limit: int = 50,
    ) -> tuple[list[GoldContributor], int]:
        """Search contributors by name with advanced filtering.

        Args:
            db: Database session
            search_query: Name search query (partial match)
            state: Filter by state
            entity_type: Filter by entity type
            employer: Filter by employer (partial match)
            occupation: Filter by occupation (partial match)
            offset: Pagination offset
            limit: Pagination limit

        Returns:
            Tuple of (contributors list, total count)
        """
        # Build base search query using case-insensitive LIKE
        search_pattern = f"%{search_query}%"
        filters = [GoldContributor.name.ilike(search_pattern)]

        # Apply additional filters
        if state:
            filters.append(GoldContributor.state == state)
        if entity_type:
            filters.append(GoldContributor.entity_type == entity_type)
        if employer:
            filters.append(GoldContributor.employer.ilike(f"%{employer}%"))
        if occupation:
            filters.append(GoldContributor.occupation.ilike(f"%{occupation}%"))

        # Build queries
        query = select(GoldContributor).where(and_(*filters))
        count_query = select(func.count()).select_from(GoldContributor).where(and_(*filters))

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
            entity_type: str | None = None,
    ) -> list[tuple[GoldContributor, float, int, int]]:
        """Get top contributors by total amount.

        Args:
            db: Database session
            limit: Maximum number of contributors to return
            state: Optional state filter
            entity_type: Optional entity type filter

        Returns:
            List of (contributor, total_amount, contribution_count, unique_recipients) tuples
        """
        # Build query to get contributors with their total contribution amounts and counts
        query = (
            select(
                GoldContributor,
                func.sum(GoldContribution.amount).label("total_amount"),
                func.count(GoldContribution.id).label("contribution_count"),
                func.count(func.distinct(GoldContribution.recipient_committee_id)).label("unique_recipients"),
            )
            .join(GoldContribution, GoldContribution.contributor_id == GoldContributor.id)
            .group_by(GoldContributor.id)
            .order_by(func.sum(GoldContribution.amount).desc())
            .limit(limit)
        )

        # Apply filters if provided
        if state:
            query = query.where(GoldContributor.state == state)
        if entity_type:
            query = query.where(GoldContributor.entity_type == entity_type)

        results = db.execute(query).all()
        return [(row[0], float(row[1]), row[2], row[3]) for row in results]

    @staticmethod
    def count_top_contributors(
            db: Session,
            state: str | None = None,
            entity_type: str | None = None,
    ) -> int:
        """Count contributors matching the top contributors filters.

        Args:
            db: Database session
            state: Optional state filter
            entity_type: Optional entity type filter

        Returns:
            Total count of contributors matching the filters
        """
        # Build count query with same filters as get_top_contributors
        # Count distinct contributors who have made contributions
        query = (
            select(func.count(func.distinct(GoldContributor.id)))
            .select_from(GoldContributor)
            .join(GoldContribution, GoldContribution.contributor_id == GoldContributor.id)
        )

        # Apply filters if provided
        if state:
            query = query.where(GoldContributor.state == state)
        if entity_type:
            query = query.where(GoldContributor.entity_type == entity_type)

        return db.execute(query).scalar_one()

    @staticmethod
    def get_contributors_by_candidate(
        db: Session,
        candidate_id: int,
        include_contributions: bool = False,
        sort_by: Literal[
            "total_amount", "contribution_count", "name", "first_date", "last_date"
        ] = "total_amount",
        order: Literal["asc", "desc"] = "desc",
        min_amount: Decimal | None = None,
        max_amount: Decimal | None = None,
        state: str | None = None,
        entity_type: str | None = None,
        date_from: date | None = None,
        date_to: date | None = None,
        search: str | None = None,
        offset: int = 0,
        limit: int = 25,
    ) -> ContributorsByCandidateResponse | None:
        """Get contributors to a specific candidate with aggregated stats.

        Returns detailed contributor information with optional nested contributions.
        """
        # Verify candidate exists
        candidate = db.execute(
            select(GoldCandidate).where(GoldCandidate.id == candidate_id)
        ).scalar_one_or_none()
        if not candidate:
            return None

        # Build base query for aggregated contributors
        # We need to aggregate contributions by contributor for this candidate
        base_filters = [GoldContribution.recipient_candidate_id == candidate_id]

        # Apply filters
        if date_from:
            base_filters.append(GoldContribution.contribution_date >= date_from)
        if date_to:
            base_filters.append(GoldContribution.contribution_date <= date_to)

        # Build aggregation query
        agg_query = (
            select(
                GoldContributor.id.label("contributor_id"),
                GoldContributor.name.label("contributor_name"),
                GoldContributor.city,
                GoldContributor.state,
                GoldContributor.zip,
                GoldContributor.entity_type,
                GoldContributor.employer,
                GoldContributor.occupation,
                func.sum(GoldContribution.amount).label("total_amount"),
                func.count(GoldContribution.id).label("contribution_count"),
                func.min(GoldContribution.contribution_date).label("first_contribution_date"),
                func.max(GoldContribution.contribution_date).label("last_contribution_date"),
            )
            .join(GoldContribution, GoldContribution.contributor_id == GoldContributor.id)
            .where(and_(*base_filters))
            .group_by(
                GoldContributor.id,
                GoldContributor.name,
                GoldContributor.city,
                GoldContributor.state,
                GoldContributor.zip,
                GoldContributor.entity_type,
                GoldContributor.employer,
                GoldContributor.occupation,
            )
        )

        # Apply contributor filters
        if state:
            agg_query = agg_query.having(GoldContributor.state == state)
        if entity_type:
            agg_query = agg_query.having(GoldContributor.entity_type == entity_type)
        if search:
            agg_query = agg_query.having(GoldContributor.name.ilike(f"%{search}%"))

        # Create subquery for filtering and sorting
        agg_subquery = agg_query.subquery()

        # Build main query from subquery
        main_query = select(agg_subquery)

        # Apply amount filters
        if min_amount is not None:
            main_query = main_query.where(agg_subquery.c.total_amount >= min_amount)
        if max_amount is not None:
            main_query = main_query.where(agg_subquery.c.total_amount <= max_amount)

        # Apply sorting
        sort_column_map = {
            "total_amount": agg_subquery.c.total_amount,
            "contribution_count": agg_subquery.c.contribution_count,
            "name": agg_subquery.c.contributor_name,
            "first_date": agg_subquery.c.first_contribution_date,
            "last_date": agg_subquery.c.last_contribution_date,
        }
        sort_column = sort_column_map[sort_by]
        if order == "desc":
            main_query = main_query.order_by(sort_column.desc())
        else:
            main_query = main_query.order_by(sort_column.asc())

        # Get total count before pagination
        count_query = select(func.count()).select_from(main_query.subquery())
        total_count = db.execute(count_query).scalar_one()

        # Apply pagination
        main_query = main_query.offset(offset).limit(limit)

        # Execute main query
        results = db.execute(main_query).all()

        # Build contributor objects
        contributors = []
        for row in results:
            contributor_data = ContributorWithAggregates(
                contributor_id=row.contributor_id,
                contributor_name=row.contributor_name,
                city=row.city,
                state=row.state,
                zip=row.zip,
                entity_type=row.entity_type,
                employer=row.employer,
                occupation=row.occupation,
                total_amount=row.total_amount,
                contribution_count=row.contribution_count,
                first_contribution_date=row.first_contribution_date,
                last_contribution_date=row.last_contribution_date,
                contributions=None,
            )

            # Optionally fetch individual contributions
            if include_contributions:
                contrib_filters = [
                    GoldContribution.contributor_id == row.contributor_id,
                    GoldContribution.recipient_candidate_id == candidate_id,
                ]
                if date_from:
                    contrib_filters.append(GoldContribution.contribution_date >= date_from)
                if date_to:
                    contrib_filters.append(GoldContribution.contribution_date <= date_to)

                contrib_query = (
                    select(GoldContribution)
                    .where(and_(*contrib_filters))
                    .order_by(GoldContribution.contribution_date.desc())
                )
                contributions = db.execute(contrib_query).scalars().all()

                contributor_data.contributions = [
                    ContributionSimple(
                        id=c.id,
                        contribution_date=c.contribution_date,
                        amount=c.amount,
                        contribution_type=c.contribution_type,
                    )
                    for c in contributions
                ]

            contributors.append(contributor_data)

        # Get summary statistics
        summary_query = (
            select(
                func.count(func.distinct(GoldContribution.contributor_id)).label(
                    "total_contributors"
                ),
                func.coalesce(func.sum(GoldContribution.amount), 0).label("total_amount_raised"),
                func.count(GoldContribution.id).label("total_contributions"),
                func.min(GoldContribution.contribution_date).label("first_contribution"),
                func.max(GoldContribution.contribution_date).label("last_contribution"),
            )
            .where(and_(*base_filters))
        )
        summary_result = db.execute(summary_query).one()

        # Build response
        return ContributorsByCandidateResponse(
            candidate=CandidateSummary(
                id=candidate.id,
                name=candidate.name,
                office=candidate.office,
                state=candidate.state,
                district=candidate.district,
                party=candidate.party,
            ),
            summary=ContributorsSummary(
                total_contributors=summary_result.total_contributors,
                total_amount_raised=summary_result.total_amount_raised,
                total_contributions=summary_result.total_contributions,
                first_contribution=summary_result.first_contribution,
                last_contribution=summary_result.last_contribution,
            ),
            contributors=contributors,
            meta={
                "page": (offset // limit) + 1 if limit > 0 else 1,
                "page_size": limit,
                "total_items": total_count,
                "total_pages": (total_count + limit - 1) // limit if limit > 0 else 1,
                "has_next": offset + limit < total_count,
                "has_prev": offset > 0,
            },
        )

    @staticmethod
    def get_contributors_by_committee(
        db: Session,
        committee_id: int,
        include_contributions: bool = False,
        sort_by: Literal[
            "total_amount", "contribution_count", "name", "first_date", "last_date"
        ] = "total_amount",
        order: Literal["asc", "desc"] = "desc",
        min_amount: Decimal | None = None,
        max_amount: Decimal | None = None,
        state: str | None = None,
        entity_type: str | None = None,
        date_from: date | None = None,
        date_to: date | None = None,
        search: str | None = None,
        offset: int = 0,
        limit: int = 25,
    ) -> ContributorsByCommitteeResponse | None:
        """Get contributors to a specific committee with aggregated stats.

        Returns detailed contributor information with optional nested contributions.
        """
        # Verify committee exists and get candidate info if applicable
        committee_query = select(GoldCommittee).where(GoldCommittee.id == committee_id)
        committee = db.execute(committee_query).scalar_one_or_none()
        if not committee:
            return None

        # Get candidate name if applicable
        candidate_name = None
        if committee.candidate_id:
            candidate = db.execute(
                select(GoldCandidate).where(GoldCandidate.id == committee.candidate_id)
            ).scalar_one_or_none()
            if candidate:
                candidate_name = candidate.name

        # Build base query for aggregated contributors
        base_filters = [GoldContribution.recipient_committee_id == committee_id]

        # Apply filters
        if date_from:
            base_filters.append(GoldContribution.contribution_date >= date_from)
        if date_to:
            base_filters.append(GoldContribution.contribution_date <= date_to)

        # Build aggregation query (same structure as by-candidate)
        agg_query = (
            select(
                GoldContributor.id.label("contributor_id"),
                GoldContributor.name.label("contributor_name"),
                GoldContributor.city,
                GoldContributor.state,
                GoldContributor.zip,
                GoldContributor.entity_type,
                GoldContributor.employer,
                GoldContributor.occupation,
                func.sum(GoldContribution.amount).label("total_amount"),
                func.count(GoldContribution.id).label("contribution_count"),
                func.min(GoldContribution.contribution_date).label("first_contribution_date"),
                func.max(GoldContribution.contribution_date).label("last_contribution_date"),
            )
            .join(GoldContribution, GoldContribution.contributor_id == GoldContributor.id)
            .where(and_(*base_filters))
            .group_by(
                GoldContributor.id,
                GoldContributor.name,
                GoldContributor.city,
                GoldContributor.state,
                GoldContributor.zip,
                GoldContributor.entity_type,
                GoldContributor.employer,
                GoldContributor.occupation,
            )
        )

        # Apply contributor filters
        if state:
            agg_query = agg_query.having(GoldContributor.state == state)
        if entity_type:
            agg_query = agg_query.having(GoldContributor.entity_type == entity_type)
        if search:
            agg_query = agg_query.having(GoldContributor.name.ilike(f"%{search}%"))

        # Create subquery for filtering and sorting
        agg_subquery = agg_query.subquery()

        # Build main query from subquery
        main_query = select(agg_subquery)

        # Apply amount filters
        if min_amount is not None:
            main_query = main_query.where(agg_subquery.c.total_amount >= min_amount)
        if max_amount is not None:
            main_query = main_query.where(agg_subquery.c.total_amount <= max_amount)

        # Apply sorting
        sort_column_map = {
            "total_amount": agg_subquery.c.total_amount,
            "contribution_count": agg_subquery.c.contribution_count,
            "name": agg_subquery.c.contributor_name,
            "first_date": agg_subquery.c.first_contribution_date,
            "last_date": agg_subquery.c.last_contribution_date,
        }
        sort_column = sort_column_map[sort_by]
        if order == "desc":
            main_query = main_query.order_by(sort_column.desc())
        else:
            main_query = main_query.order_by(sort_column.asc())

        # Get total count before pagination
        count_query = select(func.count()).select_from(main_query.subquery())
        total_count = db.execute(count_query).scalar_one()

        # Apply pagination
        main_query = main_query.offset(offset).limit(limit)

        # Execute main query
        results = db.execute(main_query).all()

        # Build contributor objects
        contributors = []
        for row in results:
            contributor_data = ContributorWithAggregates(
                contributor_id=row.contributor_id,
                contributor_name=row.contributor_name,
                city=row.city,
                state=row.state,
                zip=row.zip,
                entity_type=row.entity_type,
                employer=row.employer,
                occupation=row.occupation,
                total_amount=row.total_amount,
                contribution_count=row.contribution_count,
                first_contribution_date=row.first_contribution_date,
                last_contribution_date=row.last_contribution_date,
                contributions=None,
            )

            # Optionally fetch individual contributions
            if include_contributions:
                contrib_filters = [
                    GoldContribution.contributor_id == row.contributor_id,
                    GoldContribution.recipient_committee_id == committee_id,
                ]
                if date_from:
                    contrib_filters.append(GoldContribution.contribution_date >= date_from)
                if date_to:
                    contrib_filters.append(GoldContribution.contribution_date <= date_to)

                contrib_query = (
                    select(GoldContribution)
                    .where(and_(*contrib_filters))
                    .order_by(GoldContribution.contribution_date.desc())
                )
                contributions = db.execute(contrib_query).scalars().all()

                contributor_data.contributions = [
                    ContributionSimple(
                        id=c.id,
                        contribution_date=c.contribution_date,
                        amount=c.amount,
                        contribution_type=c.contribution_type,
                    )
                    for c in contributions
                ]

            contributors.append(contributor_data)

        # Get summary statistics
        summary_query = (
            select(
                func.count(func.distinct(GoldContribution.contributor_id)).label(
                    "total_contributors"
                ),
                func.coalesce(func.sum(GoldContribution.amount), 0).label("total_amount_raised"),
                func.count(GoldContribution.id).label("total_contributions"),
                func.min(GoldContribution.contribution_date).label("first_contribution"),
                func.max(GoldContribution.contribution_date).label("last_contribution"),
            )
            .where(and_(*base_filters))
        )
        summary_result = db.execute(summary_query).one()

        # Build response
        return ContributorsByCommitteeResponse(
            committee=CommitteeSummary(
                id=committee.id,
                name=committee.name,
                committee_type=committee.committee_type,
                state=committee.state,
                candidate_id=committee.candidate_id,
                candidate_name=candidate_name,
            ),
            summary=ContributorsSummary(
                total_contributors=summary_result.total_contributors,
                total_amount_raised=summary_result.total_amount_raised,
                total_contributions=summary_result.total_contributions,
                first_contribution=summary_result.first_contribution,
                last_contribution=summary_result.last_contribution,
            ),
            contributors=contributors,
            meta={
                "page": (offset // limit) + 1 if limit > 0 else 1,
                "page_size": limit,
                "total_items": total_count,
                "total_pages": (total_count + limit - 1) // limit if limit > 0 else 1,
                "has_next": offset + limit < total_count,
                "has_prev": offset > 0,
            },
        )

    @staticmethod
    def search_contributors_with_aggregations(
        db: Session,
        search_query: str,
        state: str | None = None,
        entity_type: str | None = None,
        min_amount: Decimal | None = None,
        max_amount: Decimal | None = None,
        date_from: date | None = None,
        date_to: date | None = None,
        sort_by: Literal[
            "name", "total_amount", "contribution_count", "unique_recipients", "first_date", "last_date"
        ] = "total_amount",
        order: Literal["asc", "desc"] = "desc",
        offset: int = 0,
        limit: int = 25,
    ) -> tuple[list[ContributorSearchAggregated], int]:
        """Search contributors by name with aggregated statistics across all recipients.

        Args:
            db: Database session
            search_query: Name search query (partial match)
            state: Filter by contributor state
            entity_type: Filter by entity type
            min_amount: Minimum total contribution amount
            max_amount: Maximum total contribution amount
            date_from: Contributions from date
            date_to: Contributions to date
            sort_by: Field to sort by
            order: Sort direction
            offset: Pagination offset
            limit: Pagination limit

        Returns:
            Tuple of (aggregated contributors list, total count)
        """
        # Build base query for contributor search
        search_pattern = f"%{search_query}%"

        # Build aggregation query
        agg_query = (
            select(
                GoldContributor.id.label("contributor_id"),
                GoldContributor.name.label("contributor_name"),
                GoldContributor.city,
                GoldContributor.state,
                GoldContributor.zip,
                GoldContributor.entity_type,
                GoldContributor.employer,
                GoldContributor.occupation,
                func.coalesce(func.sum(GoldContribution.amount), 0).label("total_amount"),
                func.count(GoldContribution.id).label("contribution_count"),
                func.count(func.distinct(GoldContribution.recipient_committee_id)).label(
                    "unique_recipients"
                ),
                func.count(func.distinct(GoldContribution.recipient_candidate_id)).label(
                    "unique_candidates"
                ),
                func.count(func.distinct(GoldContribution.recipient_committee_id)).label(
                    "unique_committees"
                ),
                func.min(GoldContribution.contribution_date).label("first_contribution_date"),
                func.max(GoldContribution.contribution_date).label("last_contribution_date"),
            )
            .outerjoin(GoldContribution, GoldContribution.contributor_id == GoldContributor.id)
            .where(GoldContributor.name.ilike(search_pattern))
            .group_by(
                GoldContributor.id,
                GoldContributor.name,
                GoldContributor.city,
                GoldContributor.state,
                GoldContributor.zip,
                GoldContributor.entity_type,
                GoldContributor.employer,
                GoldContributor.occupation,
            )
        )

        # Apply contributor filters using HAVING clauses
        if state:
            agg_query = agg_query.having(GoldContributor.state == state)
        if entity_type:
            agg_query = agg_query.having(GoldContributor.entity_type == entity_type)

        # Create subquery for amount and date filtering
        agg_subquery = agg_query.subquery()

        # Build main query from subquery
        main_query = select(agg_subquery)

        # Apply amount filters
        if min_amount is not None:
            main_query = main_query.where(agg_subquery.c.total_amount >= min_amount)
        if max_amount is not None:
            main_query = main_query.where(agg_subquery.c.total_amount <= max_amount)

        # Apply date filters (note: these filter on aggregated dates)
        if date_from:
            main_query = main_query.where(agg_subquery.c.first_contribution_date >= date_from)
        if date_to:
            main_query = main_query.where(agg_subquery.c.last_contribution_date <= date_to)

        # Get total count before pagination
        count_query = select(func.count()).select_from(main_query.subquery())
        total_count = db.execute(count_query).scalar_one()

        # Apply sorting
        sort_column_map = {
            "name": agg_subquery.c.contributor_name,
            "total_amount": agg_subquery.c.total_amount,
            "contribution_count": agg_subquery.c.contribution_count,
            "unique_recipients": agg_subquery.c.unique_recipients,
            "first_date": agg_subquery.c.first_contribution_date,
            "last_date": agg_subquery.c.last_contribution_date,
        }
        sort_column = sort_column_map[sort_by]
        if order == "desc":
            main_query = main_query.order_by(sort_column.desc().nulls_last())
        else:
            main_query = main_query.order_by(sort_column.asc().nulls_last())

        # Apply pagination
        main_query = main_query.offset(offset).limit(limit)

        # Execute query
        results = db.execute(main_query).all()

        # Build response objects
        contributors = [
            ContributorSearchAggregated(
                contributor_id=row.contributor_id,
                contributor_name=row.contributor_name,
                city=row.city,
                state=row.state,
                zip=row.zip,
                entity_type=row.entity_type,
                employer=row.employer,
                occupation=row.occupation,
                total_amount=row.total_amount,
                contribution_count=row.contribution_count,
                unique_recipients=row.unique_recipients,
                unique_candidates=row.unique_candidates,
                unique_committees=row.unique_committees,
                first_contribution_date=row.first_contribution_date,
                last_contribution_date=row.last_contribution_date,
            )
            for row in results
        ]

        return contributors, total_count

    @staticmethod
    def get_contributor_contributions(
        db: Session,
        contributor_id: int,
        limit: int = 100,
    ) -> list[ContributionWithCommittee]:
        """Get all contributions made by a specific contributor.

        Args:
            db: Database session
            contributor_id: Contributor ID
            limit: Maximum number of contributions to return

        Returns:
            List of contributions with committee information
        """
        # Query contributions with committee information
        query = (
            select(
                GoldContribution.id,
                GoldContribution.contributor_id,
                GoldContribution.recipient_committee_id,
                GoldCommittee.name.label("committee_name"),
                GoldCommittee.committee_type,
                GoldCommittee.state.label("committee_state"),
                GoldCommittee.party.label("committee_party"),
                GoldContribution.amount,
                GoldContribution.contribution_date,
                GoldContribution.contribution_type,
            )
            .join(
                GoldCommittee,
                GoldContribution.recipient_committee_id == GoldCommittee.id,
            )
            .where(GoldContribution.contributor_id == contributor_id)
            .order_by(GoldContribution.amount.desc())
            .limit(limit)
        )

        results = db.execute(query).all()

        return [
            ContributionWithCommittee(
                id=row.id,
                contributor_id=row.contributor_id,
                recipient_committee_id=row.recipient_committee_id,
                committee_name=row.committee_name,
                committee_type=row.committee_type,
                committee_state=row.committee_state,
                committee_party=row.committee_party,
                amount=row.amount,
                contribution_date=row.contribution_date,
                contribution_type=row.contribution_type,
            )
            for row in results
        ]
