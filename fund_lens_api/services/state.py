"""Service layer for state-level operations."""

from decimal import Decimal

from fund_lens_models.gold import GoldCandidate
from sqlalchemy import case, func, select, text
from sqlalchemy.orm import Session

from fund_lens_api.schemas.candidate import CandidateList
from fund_lens_api.schemas.state import RaceSummary, StateSummary


class StateService:
    """Business logic for state-level operations."""

    @staticmethod
    def get_state_summary(db: Session, state: str, top_n: int = 5) -> StateSummary | None:
        """Get comprehensive summary of campaign finance data for a state.

        Uses mv_candidate_stats materialized view for performance.
        Stats exclude earmarked contributions for accurate totals.

        Args:
            db: Database session
            state: Two-letter state code
            top_n: Number of top fundraisers to include per race

        Returns:
            StateSummary or None if state has no candidates
        """
        # Get overall state statistics (candidates only - no contributions needed)
        state_stats_query = (
            select(
                func.count(func.distinct(GoldCandidate.id)).label("total_candidates"),
                func.sum(
                    case((GoldCandidate.is_active == True, 1), else_=0)
                ).label("active_candidates"),
            )
            .where(GoldCandidate.state == state)
        )
        state_stats = db.execute(state_stats_query).one()

        if state_stats.total_candidates == 0:
            return None

        # Get contribution statistics for the state using mv_candidate_stats
        # This aggregates pre-computed stats that exclude earmarked contributions
        contrib_stats_query = text("""
            SELECT
                COALESCE(SUM(s.total_amount), 0) AS total_raised,
                COALESCE(SUM(s.total_contributions), 0) AS total_contributions,
                COALESCE(SUM(s.unique_contributors), 0) AS unique_contributors
            FROM gold_candidate c
            LEFT JOIN mv_candidate_stats s ON s.candidate_id = c.id
            WHERE c.state = :state
        """)
        contrib_stats = db.execute(contrib_stats_query, {"state": state}).one()

        # Get race summaries by office
        races_query = (
            select(GoldCandidate.office)
            .where(GoldCandidate.state == state)
            .distinct()
            .order_by(GoldCandidate.office)
        )
        offices = db.execute(races_query).scalars().all()

        races = []
        for office in offices:
            # Get race statistics (candidates only)
            race_stats_query = (
                select(
                    func.count(GoldCandidate.id).label("candidate_count"),
                    func.sum(
                        case((GoldCandidate.is_active == True, 1), else_=0)
                    ).label("active_count"),
                )
                .where(GoldCandidate.state == state)
                .where(GoldCandidate.office == office)
            )
            race_stats = db.execute(race_stats_query).one()

            # Get districts for House races
            districts = None
            if office == "H":
                districts_query = (
                    select(GoldCandidate.district)
                    .where(GoldCandidate.state == state)
                    .where(GoldCandidate.office == "H")
                    .where(GoldCandidate.district.isnot(None))
                    .distinct()
                    .order_by(GoldCandidate.district)
                )
                districts = list(db.execute(districts_query).scalars().all())

            # Get total raised for this race using mv_candidate_stats
            race_raised_query = text("""
                SELECT COALESCE(SUM(s.total_amount), 0) AS total_raised
                FROM gold_candidate c
                LEFT JOIN mv_candidate_stats s ON s.candidate_id = c.id
                WHERE c.state = :state AND c.office = :office
            """)
            race_raised = db.execute(
                race_raised_query, {"state": state, "office": office}
            ).scalar_one()

            # Get top fundraisers for this race using mv_candidate_stats
            top_fundraisers_query = text("""
                SELECT c.id, c.name, c.party, c.office, c.state, c.district,
                       c.is_active, c.fec_candidate_id,
                       COALESCE(s.total_amount, 0) AS total_amount
                FROM gold_candidate c
                LEFT JOIN mv_candidate_stats s ON s.candidate_id = c.id
                WHERE c.state = :state AND c.office = :office
                ORDER BY COALESCE(s.total_amount, 0) DESC
                LIMIT :top_n
            """)
            top_results = db.execute(
                top_fundraisers_query, {"state": state, "office": office, "top_n": top_n}
            ).all()

            # Convert to CandidateList objects
            top_fundraisers = []
            for row in top_results:
                top_fundraisers.append(
                    CandidateList(
                        id=row.id,
                        name=row.name,
                        party=row.party,
                        office=row.office,
                        state=row.state,
                        district=row.district,
                        is_active=row.is_active,
                        fec_candidate_id=row.fec_candidate_id,
                    )
                )

            races.append(
                RaceSummary(
                    office=office,
                    districts=districts,
                    candidate_count=race_stats.candidate_count,
                    active_candidate_count=race_stats.active_count,
                    total_raised=Decimal(str(race_raised)),
                    top_fundraisers=top_fundraisers,
                )
            )

        return StateSummary(
            state=state,
            total_candidates=state_stats.total_candidates,
            active_candidates=state_stats.active_candidates,
            total_raised=Decimal(str(contrib_stats.total_raised)),
            total_contributions=contrib_stats.total_contributions,
            unique_contributors=contrib_stats.unique_contributors,
            races=races,
        )
