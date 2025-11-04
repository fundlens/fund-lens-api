"""Service layer for committee operations."""

from typing import Any, cast

from fund_lens_models.gold import GoldCandidate, GoldCommittee, GoldContribution
from sqlalchemy import and_, desc, func, nullslast, select
from sqlalchemy.orm import Session, joinedload
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
            include_stats: bool = False,
            sort_by: str = "name",
            order: str = "asc",
    ) -> tuple[list[GoldCommittee] | list[tuple[GoldCommittee, dict]], int]:
        """List committees with filtering and pagination.

        Args:
            db: Database session
            filters: Committee filters
            offset: Pagination offset
            limit: Pagination limit
            include_stats: Include aggregated statistics for each committee
            sort_by: Sort by field (name, total_received, total_contributions)
            order: Sort order (asc, desc)

        Returns:
            Tuple of (committees list, total count)
            If include_stats=True, committees are tuples of (committee, stats_dict)
        """
        # Build base query
        filter_dict = filters.to_filter_dict()

        if include_stats:
            # Build stats subquery
            stats_subquery = (
                select(
                    GoldContribution.recipient_committee_id.label("committee_id"),
                    func.count(GoldContribution.id).label("total_contributions_received"),
                    func.coalesce(func.sum(GoldContribution.amount), 0).label("total_amount_received"),
                    func.count(func.distinct(GoldContribution.contributor_id)).label(
                        "unique_contributors"
                    ),
                    func.coalesce(func.avg(GoldContribution.amount), 0).label("avg_contribution"),
                )
                .group_by(GoldContribution.recipient_committee_id)
                .subquery()
            )

            # Main query with LEFT JOIN to stats
            query = (
                select(
                    GoldCommittee,
                    stats_subquery.c.total_contributions_received,
                    stats_subquery.c.total_amount_received,
                    stats_subquery.c.unique_contributors,
                    stats_subquery.c.avg_contribution,
                )
                .outerjoin(stats_subquery, GoldCommittee.id == stats_subquery.c.committee_id)
            )

            # Apply filters
            for field, value in filter_dict.items():
                column = cast(InstrumentedAttribute[Any], getattr(GoldCommittee, field))
                query = query.where(column == value)

            # Get total count (without stats join)
            count_query = select(func.count()).select_from(GoldCommittee)
            for field, value in filter_dict.items():
                column = cast(InstrumentedAttribute[Any], getattr(GoldCommittee, field))
                count_query = count_query.where(column == value)
            total_count = db.execute(count_query).scalar_one()

            # Apply sorting
            if sort_by == "total_received":
                # Sort by total amount received
                if order == "desc":
                    query = query.order_by(
                        nullslast(desc(stats_subquery.c.total_amount_received)),
                        GoldCommittee.name.asc(),
                    )
                else:
                    query = query.order_by(
                        nullslast(stats_subquery.c.total_amount_received),
                        GoldCommittee.name.asc(),
                    )
            elif sort_by == "total_contributions":
                # Sort by total number of contributions
                if order == "desc":
                    query = query.order_by(
                        nullslast(desc(stats_subquery.c.total_contributions_received)),
                        GoldCommittee.name.asc(),
                    )
                else:
                    query = query.order_by(
                        nullslast(stats_subquery.c.total_contributions_received),
                        GoldCommittee.name.asc(),
                    )
            else:
                # Sort by name (default)
                if order == "desc":
                    query = query.order_by(GoldCommittee.name.desc())
                else:
                    query = query.order_by(GoldCommittee.name.asc())

            # Apply pagination
            query = query.offset(offset).limit(limit)

            # Execute query
            results = db.execute(query).all()

            # Build response with committees and stats
            committees_with_stats = []
            for row in results:
                committee = row[0]
                stats_dict = None
                if row[1] is not None:  # If stats exist
                    stats_dict = {
                        "total_contributions_received": row[1],
                        "total_amount_received": row[2],
                        "unique_contributors": row[3],
                        "avg_contribution": row[4],
                    }
                committees_with_stats.append((committee, stats_dict))

            return committees_with_stats, total_count

        else:
            # Regular query without stats
            query = select(GoldCommittee)
            count_query = select(func.count()).select_from(GoldCommittee)

            # Apply filters
            for field, value in filter_dict.items():
                column = cast(InstrumentedAttribute[Any], getattr(GoldCommittee, field))
                query = query.where(column == value)
                count_query = count_query.where(column == value)

            # Get total count
            total_count = db.execute(count_query).scalar_one()

            # Apply sorting (only name sorting is supported without stats)
            if order == "desc":
                query = query.order_by(GoldCommittee.name.desc())
            else:
                query = query.order_by(GoldCommittee.name.asc())

            # Apply pagination
            query = query.offset(offset).limit(limit)

            # Execute query
            committees = db.execute(query).scalars().all()

            return list(committees), total_count

    @staticmethod
    def get_committee_by_id(
        db: Session, committee_id: int, include_candidate: bool = False
    ) -> tuple[GoldCommittee, GoldCandidate | None] | None:
        """Get a single committee by ID.

        Args:
            db: Database session
            committee_id: Committee ID to fetch
            include_candidate: Whether to include associated candidate details

        Returns:
            Tuple of (committee, candidate) if include_candidate=True, otherwise (committee, None)
            Returns None if committee not found
        """
        if include_candidate:
            # Query with LEFT JOIN to get candidate if it exists
            query = (
                select(GoldCommittee, GoldCandidate)
                .outerjoin(GoldCandidate, GoldCommittee.candidate_id == GoldCandidate.id)
                .where(GoldCommittee.id == committee_id)
            )
            result = db.execute(query).one_or_none()
            if not result:
                return None
            return result[0], result[1]
        else:
            # Simple query without candidate
            query = select(GoldCommittee).where(GoldCommittee.id == committee_id)
            committee = db.execute(query).scalar_one_or_none()
            if not committee:
                return None
            return committee, None

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
    def search_committees_enhanced(
            db: Session,
            search_query: str,
            state: str | None = None,
            committee_type: str | None = None,
            party: str | None = None,
            is_active: bool | None = None,
            offset: int = 0,
            limit: int = 50,
    ) -> tuple[list[GoldCommittee], int]:
        """Search committees by name with advanced filtering.

        Args:
            db: Database session
            search_query: Name search query (partial match)
            state: Filter by state
            committee_type: Filter by committee type
            party: Filter by party
            is_active: Filter by active status
            offset: Pagination offset
            limit: Pagination limit

        Returns:
            Tuple of (committees list, total count)
        """
        # Build base search query using case-insensitive LIKE
        search_pattern = f"%{search_query}%"
        filters = [GoldCommittee.name.ilike(search_pattern)]

        # Apply additional filters
        if state:
            filters.append(GoldCommittee.state == state)
        if committee_type:
            filters.append(GoldCommittee.committee_type == committee_type)
        if party:
            filters.append(GoldCommittee.party == party)
        if is_active is not None:
            filters.append(GoldCommittee.is_active == is_active)

        # Build queries
        query = select(GoldCommittee).where(and_(*filters))
        count_query = select(func.count()).select_from(GoldCommittee).where(and_(*filters))

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
        result = CommitteeService.get_committee_by_id(db, committee_id)
        if not result:
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
