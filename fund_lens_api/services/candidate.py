"""Service layer for candidate operations."""

from typing import Any, Literal, cast

from fund_lens_models.gold import GoldCandidate, GoldContribution
from sqlalchemy import func, select
from sqlalchemy.orm import Session
from sqlalchemy.orm.attributes import InstrumentedAttribute

from fund_lens_api.schemas.candidate import CandidateFilters, CandidateStats, CandidateWithStats


class CandidateService:
    """Business logic for candidate operations."""

    @staticmethod
    def list_candidates(
            db: Session,
            filters: CandidateFilters,
            offset: int = 0,
            limit: int = 50,
            include_stats: bool = False,
    ) -> tuple[list[CandidateWithStats], int]:
        """List candidates with filtering and pagination.

        Args:
            db: Database session
            filters: Candidate filters
            offset: Pagination offset
            limit: Pagination limit
            include_stats: Whether to include fundraising statistics

        Returns:
            Tuple of (candidates list with optional stats, total count)
        """
        # Build stats subquery if filtering by min_total_amount
        stats_subquery = None
        if include_stats and filters.min_total_amount is not None:
            stats_subquery = (
                select(
                    GoldContribution.recipient_candidate_id.label("candidate_id"),
                    func.count(GoldContribution.id).label("total_contributions"),
                    func.coalesce(func.sum(GoldContribution.amount), 0).label("total_amount"),
                    func.count(func.distinct(GoldContribution.contributor_id)).label(
                        "unique_contributors"
                    ),
                    func.coalesce(func.avg(GoldContribution.amount), 0).label("avg_contribution"),
                )
                .group_by(GoldContribution.recipient_candidate_id)
                .subquery()
            )

        # Build base query
        if stats_subquery is not None:
            # Join with stats for filtering
            query = (
                select(GoldCandidate)
                .join(stats_subquery, GoldCandidate.id == stats_subquery.c.candidate_id)
                .where(stats_subquery.c.total_amount >= filters.min_total_amount)
            )
            count_query = (
                select(func.count())
                .select_from(GoldCandidate)
                .join(stats_subquery, GoldCandidate.id == stats_subquery.c.candidate_id)
                .where(stats_subquery.c.total_amount >= filters.min_total_amount)
            )
        else:
            query = select(GoldCandidate)
            count_query = select(func.count()).select_from(GoldCandidate)

        # Apply standard filters (excludes 'level' and 'min_total_amount' which need special handling)
        filter_dict = filters.to_filter_dict()
        for field, value in filter_dict.items():
            column = cast(InstrumentedAttribute[Any], getattr(GoldCandidate, field))
            query = query.where(column == value)
            count_query = count_query.where(column == value)

        # Handle level filter (federal vs state)
        if filters.level:
            level = filters.level.lower()
            if level == 'federal':
                # Federal offices: House (H), Senate (S), President (P)
                query = query.where(GoldCandidate.office.in_(['H', 'S', 'P']))
                count_query = count_query.where(GoldCandidate.office.in_(['H', 'S', 'P']))
            elif level == 'state':
                # State offices: everything else
                query = query.where(GoldCandidate.office.not_in(['H', 'S', 'P']))
                count_query = count_query.where(GoldCandidate.office.not_in(['H', 'S', 'P']))

        # Get total count (reflects all filters including min_total_amount)
        total_count = db.execute(count_query).scalar_one()

        # Apply pagination and ordering
        query = query.order_by(GoldCandidate.name).offset(offset).limit(limit)

        # Execute query
        candidates = list(db.execute(query).scalars().all())

        # Build CandidateWithStats objects
        if include_stats:
            # Fetch stats for all candidates in bulk
            candidate_ids = [c.id for c in candidates]
            stats_map = CandidateService.get_bulk_candidate_stats(db, candidate_ids)

            candidates_with_stats = [
                CandidateWithStats(
                    id=c.id,
                    name=c.name,
                    office=c.office,
                    state=c.state,
                    district=c.district,
                    party=c.party,
                    is_active=c.is_active,
                    stats=stats_map.get(c.id),
                )
                for c in candidates
            ]
        else:
            candidates_with_stats = [
                CandidateWithStats(
                    id=c.id,
                    name=c.name,
                    office=c.office,
                    state=c.state,
                    district=c.district,
                    party=c.party,
                    is_active=c.is_active,
                    stats=None,
                )
                for c in candidates
            ]

        return candidates_with_stats, total_count

    @staticmethod
    def get_candidate_by_id(db: Session, candidate_id: int) -> GoldCandidate | None:
        """Get a single candidate by ID."""
        query = select(GoldCandidate).where(GoldCandidate.id == candidate_id)
        return db.execute(query).scalar_one_or_none()

    @staticmethod
    def search_candidates(
            db: Session,
            search_query: str,
            state: str | None = None,
            offices: list[str] | None = None,
            parties: list[str] | None = None,
            is_active: bool | None = None,
            has_fundraising: bool = False,
            offset: int = 0,
            limit: int = 50,
    ) -> tuple[list[GoldCandidate], int]:
        """Search candidates by name with optional filtering.

        Returns:
            Tuple of (candidates list, total count)
        """
        # Build search query using case-insensitive LIKE
        search_pattern = f"%{search_query}%"

        # Base filters
        filters = [GoldCandidate.name.ilike(search_pattern)]

        # Apply additional filters
        if state:
            filters.append(GoldCandidate.state == state)
        if offices:
            filters.append(GoldCandidate.office.in_(offices))
        if parties:
            filters.append(GoldCandidate.party.in_(parties))
        if is_active is not None:
            filters.append(GoldCandidate.is_active == is_active)

        # If has_fundraising is requested, we need a subquery
        if has_fundraising:
            # Get candidate IDs with contributions
            candidates_with_contributions = (
                select(GoldContribution.recipient_candidate_id)
                .where(GoldContribution.recipient_candidate_id.isnot(None))
                .group_by(GoldContribution.recipient_candidate_id)
                .having(func.sum(GoldContribution.amount) > 0)
                .scalar_subquery()
            )
            filters.append(GoldCandidate.id.in_(candidates_with_contributions))

        # Build queries
        query = select(GoldCandidate).where(*filters)
        count_query = select(func.count()).select_from(GoldCandidate).where(*filters)

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
    def get_bulk_candidate_stats(
        db: Session, candidate_ids: list[int]
    ) -> dict[int, CandidateStats]:
        """Get aggregated statistics for multiple candidates in a single query.

        Args:
            db: Database session
            candidate_ids: List of candidate IDs to fetch stats for

        Returns:
            Dictionary mapping candidate_id to CandidateStats
        """
        if not candidate_ids:
            return {}

        # Query contribution statistics for all candidates at once
        stats_query = (
            select(
                GoldContribution.recipient_candidate_id,
                func.count(GoldContribution.id).label("total_contributions"),
                func.coalesce(func.sum(GoldContribution.amount), 0).label("total_amount"),
                func.count(func.distinct(GoldContribution.contributor_id)).label(
                    "unique_contributors"
                ),
                func.coalesce(func.avg(GoldContribution.amount), 0).label("avg_contribution"),
            )
            .where(GoldContribution.recipient_candidate_id.in_(candidate_ids))
            .group_by(GoldContribution.recipient_candidate_id)
        )

        results = db.execute(stats_query).all()

        # Build stats dictionary
        stats_dict = {}
        for row in results:
            stats_dict[row.recipient_candidate_id] = CandidateStats(
                candidate_id=row.recipient_candidate_id,
                total_contributions=row.total_contributions,
                total_amount=float(row.total_amount),
                unique_contributors=row.unique_contributors,
                avg_contribution=float(row.avg_contribution),
            )

        # For candidates with no contributions, add zero stats
        for candidate_id in candidate_ids:
            if candidate_id not in stats_dict:
                stats_dict[candidate_id] = CandidateStats(
                    candidate_id=candidate_id,
                    total_contributions=0,
                    total_amount=0.0,
                    unique_contributors=0,
                    avg_contribution=0.0,
                )

        return stats_dict

    @staticmethod
    def get_candidates_by_state(db: Session, state: str) -> list[GoldCandidate]:
        """Get all candidates for a given state."""
        query = select(GoldCandidate).where(GoldCandidate.state == state).order_by(
            GoldCandidate.office, GoldCandidate.name
        )
        return list(db.execute(query).scalars().all())

    @staticmethod
    def get_candidates_by_state_with_options(
        db: Session,
        state: str,
        offices: list[str] | None = None,
        parties: list[str] | None = None,
        district: str | None = None,
        is_active: bool | None = None,
        has_fundraising: bool = False,
        include_stats: bool = False,
        sort_by: Literal["name", "total_amount", "total_contributions"] = "name",
        order: Literal["asc", "desc"] = "desc",
        limit: int | None = None,
        offset: int = 0,
        return_count: bool = False,
    ) -> list[CandidateWithStats] | tuple[list[CandidateWithStats], int]:
        """Get candidates for a state with optional stats and filtering.

        Args:
            db: Database session
            state: Two-letter state code
            offices: Filter by offices (e.g., ['H', 'S'])
            parties: Filter by parties (e.g., ['DEM', 'REP'])
            district: Filter by district (for House races)
            is_active: Filter by active status
            has_fundraising: Only include candidates with contributions
            include_stats: Whether to include fundraising statistics
            sort_by: Sort order (name, total_amount, total_contributions)
            order: Sort direction (asc, desc)
            limit: Maximum number of results
            offset: Number of results to skip
            return_count: Whether to return total count for pagination

        Returns:
            List of candidates with optional stats, or tuple of (candidates, total_count)
        """
        if include_stats:
            # Build query with stats aggregation
            stats_subquery = (
                select(
                    GoldContribution.recipient_candidate_id.label("candidate_id"),
                    func.count(GoldContribution.id).label("total_contributions"),
                    func.coalesce(func.sum(GoldContribution.amount), 0).label("total_amount"),
                    func.count(func.distinct(GoldContribution.contributor_id)).label(
                        "unique_contributors"
                    ),
                    func.coalesce(func.avg(GoldContribution.amount), 0).label("avg_contribution"),
                )
                .group_by(GoldContribution.recipient_candidate_id)
                .subquery()
            )

            # Main query with LEFT JOIN to stats
            query = (
                select(
                    GoldCandidate,
                    stats_subquery.c.total_contributions,
                    stats_subquery.c.total_amount,
                    stats_subquery.c.unique_contributors,
                    stats_subquery.c.avg_contribution,
                )
                .outerjoin(stats_subquery, GoldCandidate.id == stats_subquery.c.candidate_id)
                .where(GoldCandidate.state == state)
            )
        else:
            # Simple query without stats
            query = select(GoldCandidate).where(GoldCandidate.state == state)

        # Apply office filter (multiple values)
        if offices:
            query = query.where(GoldCandidate.office.in_(offices))

        # Apply party filter (multiple values)
        if parties:
            query = query.where(GoldCandidate.party.in_(parties))

        # Apply district filter
        if district:
            query = query.where(GoldCandidate.district == district)

        # Apply is_active filter
        if is_active is not None:
            query = query.where(GoldCandidate.is_active == is_active)

        # Apply has_fundraising filter
        if has_fundraising and include_stats:
            # Only include candidates with contributions (stats > 0)
            query = query.where(stats_subquery.c.total_amount > 0)

        # Apply sorting
        if include_stats and sort_by in ("total_amount", "total_contributions"):
            # Sort by stats fields (nulls last)
            sort_column = {
                "total_amount": stats_subquery.c.total_amount,
                "total_contributions": stats_subquery.c.total_contributions,
            }[sort_by]
            if order == "desc":
                query = query.order_by(sort_column.desc().nulls_last(), GoldCandidate.name)
            else:
                query = query.order_by(sort_column.asc().nulls_last(), GoldCandidate.name)
        else:
            # Sort by name or office+name
            if order == "desc":
                query = query.order_by(GoldCandidate.office.desc(), GoldCandidate.name.desc())
            else:
                query = query.order_by(GoldCandidate.office, GoldCandidate.name)

        # Get total count if requested (before applying offset/limit)
        total_count = 0
        if return_count:
            # Build count query with same filters
            count_query = select(func.count()).select_from(GoldCandidate).where(GoldCandidate.state == state)

            if offices:
                count_query = count_query.where(GoldCandidate.office.in_(offices))
            if parties:
                count_query = count_query.where(GoldCandidate.party.in_(parties))
            if district:
                count_query = count_query.where(GoldCandidate.district == district)
            if is_active is not None:
                count_query = count_query.where(GoldCandidate.is_active == is_active)
            if has_fundraising and include_stats:
                # For has_fundraising, we need to join with the stats subquery
                count_query = (
                    select(func.count())
                    .select_from(GoldCandidate)
                    .outerjoin(stats_subquery, GoldCandidate.id == stats_subquery.c.candidate_id)
                    .where(GoldCandidate.state == state)
                    .where(stats_subquery.c.total_amount > 0)
                )
                if offices:
                    count_query = count_query.where(GoldCandidate.office.in_(offices))
                if parties:
                    count_query = count_query.where(GoldCandidate.party.in_(parties))
                if district:
                    count_query = count_query.where(GoldCandidate.district == district)
                if is_active is not None:
                    count_query = count_query.where(GoldCandidate.is_active == is_active)

            total_count = db.execute(count_query).scalar_one()

        # Apply offset and limit
        if offset:
            query = query.offset(offset)
        if limit:
            query = query.limit(limit)

        # Execute query
        results = db.execute(query).all()

        # Build response objects
        candidates_with_stats = []
        for row in results:
            if include_stats:
                candidate = row[0]
                stats = CandidateStats(
                    candidate_id=candidate.id,
                    total_contributions=row[1] or 0,
                    total_amount=float(row[2] or 0),
                    unique_contributors=row[3] or 0,
                    avg_contribution=float(row[4] or 0),
                )
                candidate_with_stats = CandidateWithStats(
                    id=candidate.id,
                    name=candidate.name,
                    office=candidate.office,
                    state=candidate.state,
                    district=candidate.district,
                    party=candidate.party,
                    is_active=candidate.is_active,
                    stats=stats,
                )
            else:
                candidate = row if isinstance(row, GoldCandidate) else row[0]
                candidate_with_stats = CandidateWithStats(
                    id=candidate.id,
                    name=candidate.name,
                    office=candidate.office,
                    state=candidate.state,
                    district=candidate.district,
                    party=candidate.party,
                    is_active=candidate.is_active,
                    stats=None,
                )
            candidates_with_stats.append(candidate_with_stats)

        if return_count:
            return candidates_with_stats, total_count
        return candidates_with_stats

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

    @staticmethod
    def get_batch_candidate_details(
        db: Session, candidate_ids: list[int], include_stats: bool = False
    ) -> list[CandidateWithStats]:
        """Get full details for multiple candidates in a single request.

        Args:
            db: Database session
            candidate_ids: List of candidate IDs to fetch
            include_stats: Whether to include fundraising statistics

        Returns:
            List of CandidateWithStats objects
        """
        if not candidate_ids:
            return []

        # Fetch all candidates
        query = (
            select(GoldCandidate)
            .where(GoldCandidate.id.in_(candidate_ids))
            .order_by(GoldCandidate.name)
        )
        candidates = list(db.execute(query).scalars().all())

        # Build candidate map for quick lookup
        candidate_map = {c.id: c for c in candidates}

        # Fetch stats if requested
        stats_map = {}
        if include_stats:
            stats_map = CandidateService.get_bulk_candidate_stats(db, candidate_ids)

        # Build response objects
        results = []
        for candidate_id in candidate_ids:
            candidate = candidate_map.get(candidate_id)
            if not candidate:
                continue  # Skip missing candidates

            stats = stats_map.get(candidate_id) if include_stats else None

            candidate_with_stats = CandidateWithStats(
                id=candidate.id,
                name=candidate.name,
                office=candidate.office,
                state=candidate.state,
                district=candidate.district,
                party=candidate.party,
                is_active=candidate.is_active,
                stats=stats,
            )
            results.append(candidate_with_stats)

        return results
