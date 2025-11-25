"""Service layer for race operations."""

from decimal import Decimal

from fund_lens_models.gold import GoldCandidate
from sqlalchemy import and_, select, text
from sqlalchemy.orm import Session

from fund_lens_api.schemas.candidate import CandidateStats
from fund_lens_api.schemas.race import (
    HouseRaceResponse,
    PresidentialRaceResponse,
    RaceCandidate,
    RaceSummary,
    SenateRaceResponse,
)


class RaceService:
    """Business logic for race operations."""

    @staticmethod
    def get_senate_race(
        db: Session, state: str, include_stats: bool = True
    ) -> SenateRaceResponse | None:
        """Get information about a Senate race in a specific state.

        Args:
            db: Database session
            state: Two-letter state code
            include_stats: Whether to include candidate fundraising statistics

        Returns:
            SenateRaceResponse or None if no candidates found
        """
        # Get all Senate candidates for this state
        candidates_query = (
            select(GoldCandidate)
            .where(and_(GoldCandidate.state == state, GoldCandidate.office == "S"))
            .order_by(GoldCandidate.is_active.desc(), GoldCandidate.name)
        )
        candidates = list(db.execute(candidates_query).scalars().all())

        if not candidates:
            return None

        # Get candidate IDs
        candidate_ids = [c.id for c in candidates]

        # Get stats for all candidates if requested using mv_candidate_stats
        # This view pre-filters earmarked contributions for accurate totals
        stats_map = {}
        if include_stats and candidate_ids:
            stats_query = text("""
                SELECT candidate_id, total_contributions, total_amount,
                       unique_contributors, avg_contribution
                FROM mv_candidate_stats
                WHERE candidate_id = ANY(:candidate_ids)
            """)
            stats_results = db.execute(
                stats_query, {"candidate_ids": candidate_ids}
            ).all()

            for row in stats_results:
                stats_map[row.candidate_id] = CandidateStats(
                    candidate_id=row.candidate_id,
                    total_contributions=row.total_contributions,
                    total_amount=float(row.total_amount),
                    unique_contributors=row.unique_contributors,
                    avg_contribution=float(row.avg_contribution),
                )

        # Build race summary by aggregating from mv_candidate_stats
        # This uses pre-computed stats that exclude earmarked contributions
        summary_query = text("""
            SELECT
                COUNT(DISTINCT c.id) AS total_candidates,
                COUNT(DISTINCT c.id) FILTER (WHERE c.is_active = TRUE) AS active_candidates,
                COALESCE(SUM(s.total_amount), 0) AS total_amount_raised,
                COALESCE(SUM(s.total_contributions), 0) AS total_contributions,
                COALESCE(SUM(s.unique_contributors), 0) AS unique_contributors
            FROM gold_candidate c
            LEFT JOIN mv_candidate_stats s ON s.candidate_id = c.id
            WHERE c.state = :state AND c.office = 'S'
        """)
        summary_result = db.execute(summary_query, {"state": state}).one()

        summary = RaceSummary(
            total_candidates=summary_result.total_candidates,
            active_candidates=summary_result.active_candidates or 0,
            total_amount_raised=Decimal(str(summary_result.total_amount_raised)),
            total_contributions=summary_result.total_contributions,
            unique_contributors=summary_result.unique_contributors,
        )

        # Build candidate list
        race_candidates = []
        for candidate in candidates:
            stats = stats_map.get(candidate.id) if include_stats else None
            # If no stats found but stats requested, create zero stats
            if include_stats and not stats:
                stats = CandidateStats(
                    candidate_id=candidate.id,
                    total_contributions=0,
                    total_amount=0.0,
                    unique_contributors=0,
                    avg_contribution=0.0,
                )

            race_candidates.append(
                RaceCandidate(
                    id=candidate.id,
                    name=candidate.name,
                    party=candidate.party,
                    is_active=candidate.is_active,
                    stats=stats,
                )
            )

        return SenateRaceResponse(
            state=state, summary=summary, candidates=race_candidates
        )

    @staticmethod
    def get_house_race(
        db: Session, state: str, district: str, include_stats: bool = True
    ) -> HouseRaceResponse | None:
        """Get information about a House race in a specific state and district.

        Args:
            db: Database session
            state: Two-letter state code
            district: Congressional district (e.g., "01", "02")
            include_stats: Whether to include candidate fundraising statistics

        Returns:
            HouseRaceResponse or None if no candidates found
        """
        # Get all House candidates for this state and district
        candidates_query = (
            select(GoldCandidate)
            .where(
                and_(
                    GoldCandidate.state == state,
                    GoldCandidate.office == "H",
                    GoldCandidate.district == district,
                )
            )
            .order_by(GoldCandidate.is_active.desc(), GoldCandidate.name)
        )
        candidates = list(db.execute(candidates_query).scalars().all())

        if not candidates:
            return None

        # Get candidate IDs
        candidate_ids = [c.id for c in candidates]

        # Get stats for all candidates if requested using mv_candidate_stats
        # This view pre-filters earmarked contributions for accurate totals
        stats_map = {}
        if include_stats and candidate_ids:
            stats_query = text("""
                SELECT candidate_id, total_contributions, total_amount,
                       unique_contributors, avg_contribution
                FROM mv_candidate_stats
                WHERE candidate_id = ANY(:candidate_ids)
            """)
            stats_results = db.execute(
                stats_query, {"candidate_ids": candidate_ids}
            ).all()

            for row in stats_results:
                stats_map[row.candidate_id] = CandidateStats(
                    candidate_id=row.candidate_id,
                    total_contributions=row.total_contributions,
                    total_amount=float(row.total_amount),
                    unique_contributors=row.unique_contributors,
                    avg_contribution=float(row.avg_contribution),
                )

        # Build race summary by aggregating from mv_candidate_stats
        # This uses pre-computed stats that exclude earmarked contributions
        summary_query = text("""
            SELECT
                COUNT(DISTINCT c.id) AS total_candidates,
                COUNT(DISTINCT c.id) FILTER (WHERE c.is_active = TRUE) AS active_candidates,
                COALESCE(SUM(s.total_amount), 0) AS total_amount_raised,
                COALESCE(SUM(s.total_contributions), 0) AS total_contributions,
                COALESCE(SUM(s.unique_contributors), 0) AS unique_contributors
            FROM gold_candidate c
            LEFT JOIN mv_candidate_stats s ON s.candidate_id = c.id
            WHERE c.state = :state AND c.office = 'H' AND c.district = :district
        """)
        summary_result = db.execute(
            summary_query, {"state": state, "district": district}
        ).one()

        summary = RaceSummary(
            total_candidates=summary_result.total_candidates,
            active_candidates=summary_result.active_candidates or 0,
            total_amount_raised=Decimal(str(summary_result.total_amount_raised)),
            total_contributions=summary_result.total_contributions,
            unique_contributors=summary_result.unique_contributors,
        )

        # Build candidate list
        race_candidates = []
        for candidate in candidates:
            stats = stats_map.get(candidate.id) if include_stats else None
            # If no stats found but stats requested, create zero stats
            if include_stats and not stats:
                stats = CandidateStats(
                    candidate_id=candidate.id,
                    total_contributions=0,
                    total_amount=0.0,
                    unique_contributors=0,
                    avg_contribution=0.0,
                )

            race_candidates.append(
                RaceCandidate(
                    id=candidate.id,
                    name=candidate.name,
                    party=candidate.party,
                    is_active=candidate.is_active,
                    stats=stats,
                )
            )

        return HouseRaceResponse(
            state=state, district=district, summary=summary, candidates=race_candidates
        )

    @staticmethod
    def get_presidential_race(
        db: Session, include_stats: bool = True
    ) -> PresidentialRaceResponse:
        """Get information about the Presidential race.

        Args:
            db: Database session
            include_stats: Whether to include candidate fundraising statistics

        Returns:
            PresidentialRaceResponse
        """
        # Get all Presidential candidates
        candidates_query = (
            select(GoldCandidate)
            .where(GoldCandidate.office == "P")
            .order_by(GoldCandidate.is_active.desc(), GoldCandidate.name)
        )
        candidates = list(db.execute(candidates_query).scalars().all())

        # Get candidate IDs
        candidate_ids = [c.id for c in candidates]

        # Get stats for all candidates if requested using mv_candidate_stats
        # This view pre-filters earmarked contributions for accurate totals
        stats_map = {}
        if include_stats and candidate_ids:
            stats_query = text("""
                SELECT candidate_id, total_contributions, total_amount,
                       unique_contributors, avg_contribution
                FROM mv_candidate_stats
                WHERE candidate_id = ANY(:candidate_ids)
            """)
            stats_results = db.execute(
                stats_query, {"candidate_ids": candidate_ids}
            ).all()

            for row in stats_results:
                stats_map[row.candidate_id] = CandidateStats(
                    candidate_id=row.candidate_id,
                    total_contributions=row.total_contributions,
                    total_amount=float(row.total_amount),
                    unique_contributors=row.unique_contributors,
                    avg_contribution=float(row.avg_contribution),
                )

        # Build race summary by aggregating from mv_candidate_stats
        # This uses pre-computed stats that exclude earmarked contributions
        if candidate_ids:
            summary_query = text("""
                SELECT
                    COUNT(DISTINCT c.id) AS total_candidates,
                    COUNT(DISTINCT c.id) FILTER (WHERE c.is_active = TRUE) AS active_candidates,
                    COALESCE(SUM(s.total_amount), 0) AS total_amount_raised,
                    COALESCE(SUM(s.total_contributions), 0) AS total_contributions,
                    COALESCE(SUM(s.unique_contributors), 0) AS unique_contributors
                FROM gold_candidate c
                LEFT JOIN mv_candidate_stats s ON s.candidate_id = c.id
                WHERE c.office = 'P'
            """)
            summary_result = db.execute(summary_query).one()

            summary = RaceSummary(
                total_candidates=summary_result.total_candidates,
                active_candidates=summary_result.active_candidates or 0,
                total_amount_raised=Decimal(str(summary_result.total_amount_raised)),
                total_contributions=summary_result.total_contributions,
                unique_contributors=summary_result.unique_contributors,
            )
        else:
            # No candidates, return zero stats
            summary = RaceSummary(
                total_candidates=0,
                active_candidates=0,
                total_amount_raised=Decimal("0"),
                total_contributions=0,
                unique_contributors=0,
            )

        # Build candidate list
        race_candidates = []
        for candidate in candidates:
            stats = stats_map.get(candidate.id) if include_stats else None
            # If no stats found but stats requested, create zero stats
            if include_stats and not stats:
                stats = CandidateStats(
                    candidate_id=candidate.id,
                    total_contributions=0,
                    total_amount=0.0,
                    unique_contributors=0,
                    avg_contribution=0.0,
                )

            race_candidates.append(
                RaceCandidate(
                    id=candidate.id,
                    name=candidate.name,
                    party=candidate.party,
                    is_active=candidate.is_active,
                    stats=stats,
                )
            )

        return PresidentialRaceResponse(summary=summary, candidates=race_candidates)
