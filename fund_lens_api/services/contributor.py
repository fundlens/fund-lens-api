"""Service layer for contributor operations."""

from datetime import date
from decimal import Decimal
from typing import Any, Literal, cast

from fund_lens_models.gold import GoldCandidate, GoldCommittee, GoldContribution, GoldContributor
from sqlalchemy import and_, func, literal_column, select, text
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
        """Get aggregated statistics for a contributor.

        Uses mv_contributor_stats materialized view for performance.
        Stats exclude earmarked contributions for accurate totals.
        """
        # Verify contributor exists
        contributor = ContributorService.get_contributor_by_id(db, contributor_id)
        if not contributor:
            return None

        # Query from materialized view for fast lookup
        stats_query = text("""
            SELECT total_contributions, total_amount, unique_recipients, avg_contribution,
                   first_contribution_date, last_contribution_date
            FROM mv_contributor_stats
            WHERE contributor_id = :contributor_id
        """)

        result = db.execute(stats_query, {"contributor_id": contributor_id}).one_or_none()

        if not result:
            # Contributor exists but has no contributions (or all are earmarked)
            return ContributorStats(
                contributor_id=contributor_id,
                total_contributions=0,
                total_amount=0.0,
                unique_recipients=0,
                avg_contribution=0.0,
                first_contribution_date=None,
                last_contribution_date=None,
            )

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

        Uses mv_contributor_stats materialized view for performance.

        Args:
            db: Database session
            limit: Maximum number of contributors to return
            state: Optional state filter
            entity_type: Optional entity type filter

        Returns:
            List of (contributor, total_amount, contribution_count, unique_recipients) tuples
        """
        from sqlalchemy import text

        # Build raw SQL query using materialized view
        # This is much faster than aggregating 14M contributions
        sql = """
            SELECT
                gc.id, gc.name, gc.city, gc.state, gc.zip,
                gc.employer, gc.occupation, gc.entity_type, gc.match_confidence,
                mv.total_amount, mv.total_contributions, mv.unique_recipients
            FROM mv_contributor_stats mv
            JOIN gold_contributor gc ON gc.id = mv.contributor_id
            WHERE 1=1
        """
        params: dict[str, Any] = {"limit": limit}

        if state:
            sql += " AND gc.state = :state"
            params["state"] = state
        if entity_type:
            sql += " AND gc.entity_type = :entity_type"
            params["entity_type"] = entity_type

        sql += " ORDER BY mv.total_amount DESC LIMIT :limit"

        results = db.execute(text(sql), params).fetchall()

        # Convert to expected format: (GoldContributor, total_amount, contribution_count, unique_recipients)
        output = []
        for row in results:
            contributor = GoldContributor(
                id=row.id,
                name=row.name,
                city=row.city,
                state=row.state,
                zip=row.zip,
                employer=row.employer,
                occupation=row.occupation,
                entity_type=row.entity_type,
                match_confidence=row.match_confidence,
            )
            output.append((contributor, float(row.total_amount), row.total_contributions, row.unique_recipients))

        return output

    @staticmethod
    def count_top_contributors(
            db: Session,
            state: str | None = None,
            entity_type: str | None = None,
    ) -> int:
        """Count contributors matching the top contributors filters.

        Uses mv_contributor_stats materialized view for performance.

        Args:
            db: Database session
            state: Optional state filter
            entity_type: Optional entity type filter

        Returns:
            Total count of contributors matching the filters
        """
        from sqlalchemy import text

        # Build raw SQL query using materialized view
        sql = """
            SELECT COUNT(*)
            FROM mv_contributor_stats mv
            JOIN gold_contributor gc ON gc.id = mv.contributor_id
            WHERE 1=1
        """
        params: dict[str, Any] = {}

        if state:
            sql += " AND gc.state = :state"
            params["state"] = state
        if entity_type:
            sql += " AND gc.entity_type = :entity_type"
            params["entity_type"] = entity_type

        result = db.execute(text(sql), params).scalar_one()
        return result

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
        Uses mv_contributor_candidate_stats materialized view for fast aggregation
        when no date filters are applied.
        """
        # Verify candidate exists
        candidate = db.execute(
            select(GoldCandidate).where(GoldCandidate.id == candidate_id)
        ).scalar_one_or_none()
        if not candidate:
            return None

        # Use materialized view when no date filters (much faster)
        use_materialized_view = date_from is None and date_to is None

        if use_materialized_view:
            # Build query using mv_contributor_candidate_stats
            mv_subquery = (
                select(
                    literal_column("contributor_id").label("contributor_id"),
                    literal_column("contribution_count").label("contribution_count"),
                    literal_column("total_amount").label("total_amount"),
                    literal_column("first_contribution_date").label("first_contribution_date"),
                    literal_column("last_contribution_date").label("last_contribution_date"),
                )
                .select_from(text("mv_contributor_candidate_stats"))
                .where(text("candidate_id = :candidate_id"))
                .subquery()
            )

            # Join with contributor details
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
                    mv_subquery.c.total_amount,
                    mv_subquery.c.contribution_count,
                    mv_subquery.c.first_contribution_date,
                    mv_subquery.c.last_contribution_date,
                )
                .join(mv_subquery, GoldContributor.id == mv_subquery.c.contributor_id)
            )

            # Apply contributor filters
            if state:
                agg_query = agg_query.where(GoldContributor.state == state)
            if entity_type:
                agg_query = agg_query.where(GoldContributor.entity_type == entity_type)
            if search:
                agg_query = agg_query.where(GoldContributor.name.ilike(f"%{search}%"))
            if min_amount is not None:
                agg_query = agg_query.where(mv_subquery.c.total_amount >= min_amount)
            if max_amount is not None:
                agg_query = agg_query.where(mv_subquery.c.total_amount <= max_amount)

            # Apply sorting
            sort_column_map = {
                "total_amount": mv_subquery.c.total_amount,
                "contribution_count": mv_subquery.c.contribution_count,
                "name": GoldContributor.name,
                "first_date": mv_subquery.c.first_contribution_date,
                "last_date": mv_subquery.c.last_contribution_date,
            }
            sort_column = sort_column_map[sort_by]
            if order == "desc":
                agg_query = agg_query.order_by(sort_column.desc())
            else:
                agg_query = agg_query.order_by(sort_column.asc())

            # Get total count
            count_sql = text("""
                SELECT COUNT(*) FROM mv_contributor_candidate_stats mv
                JOIN gold_contributor gc ON gc.id = mv.contributor_id
                WHERE mv.candidate_id = :candidate_id
            """)
            params: dict[str, Any] = {"candidate_id": candidate_id}

            # Add filter conditions to count
            count_conditions = []
            if state:
                count_conditions.append("gc.state = :state")
                params["state"] = state
            if entity_type:
                count_conditions.append("gc.entity_type = :entity_type")
                params["entity_type"] = entity_type
            if search:
                count_conditions.append("gc.name ILIKE :search")
                params["search"] = f"%{search}%"
            if min_amount is not None:
                count_conditions.append("mv.total_amount >= :min_amount")
                params["min_amount"] = min_amount
            if max_amount is not None:
                count_conditions.append("mv.total_amount <= :max_amount")
                params["max_amount"] = max_amount

            if count_conditions:
                count_sql = text(f"""
                    SELECT COUNT(*) FROM mv_contributor_candidate_stats mv
                    JOIN gold_contributor gc ON gc.id = mv.contributor_id
                    WHERE mv.candidate_id = :candidate_id AND {' AND '.join(count_conditions)}
                """)

            total_count = db.execute(count_sql, params).scalar_one()

            # Apply pagination and execute
            agg_query = agg_query.offset(offset).limit(limit)
            results = db.execute(agg_query, {"candidate_id": candidate_id}).all()

            # Get summary from materialized view
            summary_sql = text("""
                SELECT
                    COUNT(*) as total_contributors,
                    COALESCE(SUM(total_amount), 0) as total_amount_raised,
                    COALESCE(SUM(contribution_count), 0) as total_contributions,
                    MIN(first_contribution_date) as first_contribution,
                    MAX(last_contribution_date) as last_contribution
                FROM mv_contributor_candidate_stats
                WHERE candidate_id = :candidate_id
            """)
            summary_result = db.execute(summary_sql, {"candidate_id": candidate_id}).one()

        else:
            # Fall back to real-time aggregation when date filters are applied
            # Include conduit filtering for consistency with materialized view
            base_filters = [
                GoldContribution.recipient_candidate_id == candidate_id,
                GoldContribution.is_earmark_receipt == False,  # noqa: E712
                ~GoldContribution.source_transaction_id.like("%E"),
                ~func.upper(func.coalesce(GoldContribution.memo_text, "")).like("%EARMARK%"),
                ~func.upper(func.coalesce(GoldContribution.memo_text, "")).like("%CONDUIT%"),
                ~func.upper(func.coalesce(GoldContribution.memo_text, "")).like("%ATTRIBUTION BELOW%"),
            ]
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

            # Get total count
            count_query = select(func.count()).select_from(main_query.subquery())
            total_count = db.execute(count_query).scalar_one()

            # Apply pagination
            main_query = main_query.offset(offset).limit(limit)
            results = db.execute(main_query).all()

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

        # Build contributor objects
        contributors = []
        contributor_ids = [row.contributor_id for row in results]

        # Batch fetch contributions if needed (avoid N+1 queries)
        contributions_by_contributor: dict[int, list] = {}
        if include_contributions and contributor_ids:
            contrib_filters = [
                GoldContribution.contributor_id.in_(contributor_ids),
                GoldContribution.recipient_candidate_id == candidate_id,
            ]
            if date_from:
                contrib_filters.append(GoldContribution.contribution_date >= date_from)
            if date_to:
                contrib_filters.append(GoldContribution.contribution_date <= date_to)

            contrib_query = (
                select(GoldContribution)
                .where(and_(*contrib_filters))
                .order_by(
                    GoldContribution.contributor_id,
                    GoldContribution.contribution_date.desc()
                )
            )
            all_contributions = db.execute(contrib_query).scalars().all()

            for c in all_contributions:
                if c.contributor_id not in contributions_by_contributor:
                    contributions_by_contributor[c.contributor_id] = []
                contributions_by_contributor[c.contributor_id].append(c)

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

            if include_contributions:
                contribs = contributions_by_contributor.get(row.contributor_id, [])
                contributor_data.contributions = [
                    ContributionSimple(
                        id=c.id,
                        contribution_date=c.contribution_date,
                        amount=c.amount,
                        contribution_type=c.contribution_type,
                    )
                    for c in contribs
                ]

            contributors.append(contributor_data)

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
        Uses mv_contributor_committee_stats materialized view for fast aggregation
        when no date filters are applied.
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

        # Use materialized view when no date filters (much faster)
        use_materialized_view = date_from is None and date_to is None

        if use_materialized_view:
            # Build query using mv_contributor_committee_stats
            mv_subquery = (
                select(
                    literal_column("contributor_id").label("contributor_id"),
                    literal_column("contribution_count").label("contribution_count"),
                    literal_column("total_amount").label("total_amount"),
                    literal_column("first_contribution_date").label("first_contribution_date"),
                    literal_column("last_contribution_date").label("last_contribution_date"),
                )
                .select_from(text("mv_contributor_committee_stats"))
                .where(text("committee_id = :committee_id"))
                .subquery()
            )

            # Join with contributor details
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
                    mv_subquery.c.total_amount,
                    mv_subquery.c.contribution_count,
                    mv_subquery.c.first_contribution_date,
                    mv_subquery.c.last_contribution_date,
                )
                .join(mv_subquery, GoldContributor.id == mv_subquery.c.contributor_id)
            )

            # Apply contributor filters
            if state:
                agg_query = agg_query.where(GoldContributor.state == state)
            if entity_type:
                agg_query = agg_query.where(GoldContributor.entity_type == entity_type)
            if search:
                agg_query = agg_query.where(GoldContributor.name.ilike(f"%{search}%"))
            if min_amount is not None:
                agg_query = agg_query.where(mv_subquery.c.total_amount >= min_amount)
            if max_amount is not None:
                agg_query = agg_query.where(mv_subquery.c.total_amount <= max_amount)

            # Apply sorting
            sort_column_map = {
                "total_amount": mv_subquery.c.total_amount,
                "contribution_count": mv_subquery.c.contribution_count,
                "name": GoldContributor.name,
                "first_date": mv_subquery.c.first_contribution_date,
                "last_date": mv_subquery.c.last_contribution_date,
            }
            sort_column = sort_column_map[sort_by]
            if order == "desc":
                agg_query = agg_query.order_by(sort_column.desc())
            else:
                agg_query = agg_query.order_by(sort_column.asc())

            # Get total count
            count_sql = text("""
                SELECT COUNT(*) FROM mv_contributor_committee_stats mv
                JOIN gold_contributor gc ON gc.id = mv.contributor_id
                WHERE mv.committee_id = :committee_id
            """)
            params: dict[str, Any] = {"committee_id": committee_id}

            # Add filter conditions to count
            count_conditions = []
            if state:
                count_conditions.append("gc.state = :state")
                params["state"] = state
            if entity_type:
                count_conditions.append("gc.entity_type = :entity_type")
                params["entity_type"] = entity_type
            if search:
                count_conditions.append("gc.name ILIKE :search")
                params["search"] = f"%{search}%"
            if min_amount is not None:
                count_conditions.append("mv.total_amount >= :min_amount")
                params["min_amount"] = min_amount
            if max_amount is not None:
                count_conditions.append("mv.total_amount <= :max_amount")
                params["max_amount"] = max_amount

            if count_conditions:
                count_sql = text(f"""
                    SELECT COUNT(*) FROM mv_contributor_committee_stats mv
                    JOIN gold_contributor gc ON gc.id = mv.contributor_id
                    WHERE mv.committee_id = :committee_id AND {' AND '.join(count_conditions)}
                """)

            total_count = db.execute(count_sql, params).scalar_one()

            # Apply pagination and execute
            agg_query = agg_query.offset(offset).limit(limit)
            results = db.execute(agg_query, {"committee_id": committee_id}).all()

            # Get summary from materialized view
            summary_sql = text("""
                SELECT
                    COUNT(*) as total_contributors,
                    COALESCE(SUM(total_amount), 0) as total_amount_raised,
                    COALESCE(SUM(contribution_count), 0) as total_contributions,
                    MIN(first_contribution_date) as first_contribution,
                    MAX(last_contribution_date) as last_contribution
                FROM mv_contributor_committee_stats
                WHERE committee_id = :committee_id
            """)
            summary_result = db.execute(summary_sql, {"committee_id": committee_id}).one()

        else:
            # Fall back to real-time aggregation when date filters are applied
            base_filters = [GoldContribution.recipient_committee_id == committee_id]
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

            # Get total count
            count_query = select(func.count()).select_from(main_query.subquery())
            total_count = db.execute(count_query).scalar_one()

            # Apply pagination
            main_query = main_query.offset(offset).limit(limit)
            results = db.execute(main_query).all()

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

        # Build contributor objects
        contributors = []
        contributor_ids = [row.contributor_id for row in results]

        # Batch fetch contributions if needed (avoid N+1 queries)
        contributions_by_contributor: dict[int, list] = {}
        if include_contributions and contributor_ids:
            contrib_filters = [
                GoldContribution.contributor_id.in_(contributor_ids),
                GoldContribution.recipient_committee_id == committee_id,
            ]
            if date_from:
                contrib_filters.append(GoldContribution.contribution_date >= date_from)
            if date_to:
                contrib_filters.append(GoldContribution.contribution_date <= date_to)

            contrib_query = (
                select(GoldContribution)
                .where(and_(*contrib_filters))
                .order_by(
                    GoldContribution.contributor_id,
                    GoldContribution.contribution_date.desc()
                )
            )
            all_contributions = db.execute(contrib_query).scalars().all()

            for c in all_contributions:
                if c.contributor_id not in contributions_by_contributor:
                    contributions_by_contributor[c.contributor_id] = []
                contributions_by_contributor[c.contributor_id].append(c)

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

            if include_contributions:
                contribs = contributions_by_contributor.get(row.contributor_id, [])
                contributor_data.contributions = [
                    ContributionSimple(
                        id=c.id,
                        contribution_date=c.contribution_date,
                        amount=c.amount,
                        contribution_type=c.contribution_type,
                    )
                    for c in contribs
                ]

            contributors.append(contributor_data)

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
        sort_by: str = "amount",
        sort_direction: str = "desc",
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> list[ContributionWithCommittee]:
        """Get all contributions made by a specific contributor.

        Args:
            db: Database session
            contributor_id: Contributor ID
            limit: Maximum number of contributions to return
            sort_by: Column to sort by (recipient, date, or amount)
            sort_direction: Sort direction (asc or desc)
            start_date: Optional start date filter (inclusive)
            end_date: Optional end date filter (inclusive)

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
        )

        # Apply date range filters if provided
        if start_date:
            query = query.where(GoldContribution.contribution_date >= start_date)
        if end_date:
            query = query.where(GoldContribution.contribution_date <= end_date)

        # Map sort_by values to actual column references
        sort_column_map = {
            "recipient": GoldCommittee.name,
            "date": GoldContribution.contribution_date,
            "amount": GoldContribution.amount,
        }

        # Get the column to sort by
        sort_column = sort_column_map.get(sort_by, GoldContribution.amount)

        # Apply sort direction
        if sort_direction == "asc":
            query = query.order_by(sort_column.asc())
        else:
            query = query.order_by(sort_column.desc())

        # Apply limit
        query = query.limit(limit)

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

    @staticmethod
    def get_contributor_recipients(
        db: Session,
        contributor_id: int,
        sort_by: str = "total_amount",
        sort_direction: str = "desc",
    ) -> tuple[list[Any], int]:
        """Get pre-aggregated recipient data for a contributor.

        Uses mv_contributor_committee_stats materialized view for performance.
        Stats exclude earmarked contributions for accurate totals.

        Args:
            db: Database session
            contributor_id: Contributor ID
            sort_by: Field to sort by (total_amount, contribution_count, committee_name, first_date, last_date)
            sort_direction: Sort direction (asc, desc)

        Returns:
            Tuple of (recipients list, total count)
        """
        from fund_lens_api.schemas.contributor import ContributorRecipient

        # Build query using mv_contributor_committee_stats materialized view
        # This is much faster than aggregating contributions on the fly
        sort_column_sql = {
            "committee_name": "gc.name",
            "total_amount": "mv.total_amount",
            "contribution_count": "mv.contribution_count",
            "first_date": "mv.first_contribution_date",
            "last_date": "mv.last_contribution_date",
        }

        sort_col = sort_column_sql.get(sort_by, "mv.total_amount")
        sort_dir = "ASC" if sort_direction == "asc" else "DESC"

        query_sql = text(f"""
            SELECT
                gc.id AS committee_id,
                gc.name AS committee_name,
                gc.committee_type,
                gc.state AS committee_state,
                gc.party AS committee_party,
                mv.contribution_count,
                mv.total_amount,
                mv.first_contribution_date,
                mv.last_contribution_date
            FROM mv_contributor_committee_stats mv
            JOIN gold_committee gc ON gc.id = mv.committee_id
            WHERE mv.contributor_id = :contributor_id
            ORDER BY {sort_col} {sort_dir}
        """)

        results = db.execute(query_sql, {"contributor_id": contributor_id}).all()

        # Get total count from materialized view
        count_sql = text("""
            SELECT COUNT(*)
            FROM mv_contributor_committee_stats
            WHERE contributor_id = :contributor_id
        """)
        total_count = db.execute(count_sql, {"contributor_id": contributor_id}).scalar_one()

        # Convert to ContributorRecipient objects
        recipients = [
            ContributorRecipient(
                committee_id=row.committee_id,
                committee_name=row.committee_name,
                committee_type=row.committee_type,
                committee_state=row.committee_state,
                committee_party=row.committee_party,
                contribution_count=row.contribution_count,
                total_amount=row.total_amount,
                first_contribution_date=row.first_contribution_date,
                last_contribution_date=row.last_contribution_date,
            )
            for row in results
        ]

        return recipients, total_count
