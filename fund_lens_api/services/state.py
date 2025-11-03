"""Service layer for state-level operations."""

from decimal import Decimal

from fund_lens_models.gold import GoldCandidate, GoldContribution
from sqlalchemy import case, func, select
from sqlalchemy.orm import Session

from fund_lens_api.schemas.candidate import CandidateList
from fund_lens_api.schemas.state import RaceSummary, StateSummary


class StateService:
    """Business logic for state-level operations."""

    @staticmethod
    def get_state_summary(db: Session, state: str, top_n: int = 5) -> StateSummary | None:
        """Get comprehensive summary of campaign finance data for a state.

        Args:
            db: Database session
            state: Two-letter state code
            top_n: Number of top fundraisers to include per race

        Returns:
            StateSummary or None if state has no candidates
        """
        # Get overall state statistics
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

        # Get contribution statistics for the state
        contrib_stats_query = (
            select(
                func.coalesce(func.sum(GoldContribution.amount), 0).label("total_raised"),
                func.count(GoldContribution.id).label("total_contributions"),
                func.count(func.distinct(GoldContribution.contributor_id)).label(
                    "unique_contributors"
                ),
            )
            .join(GoldCandidate, GoldContribution.recipient_candidate_id == GoldCandidate.id)
            .where(GoldCandidate.state == state)
        )
        contrib_stats = db.execute(contrib_stats_query).one()

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
            # Get race statistics
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

            # Get total raised for this race
            race_raised_query = (
                select(
                    func.coalesce(func.sum(GoldContribution.amount), 0).label("total_raised")
                )
                .join(
                    GoldCandidate, GoldContribution.recipient_candidate_id == GoldCandidate.id
                )
                .where(GoldCandidate.state == state)
                .where(GoldCandidate.office == office)
            )
            race_raised = db.execute(race_raised_query).scalar_one()

            # Get top fundraisers for this race
            top_fundraisers_query = (
                select(
                    GoldCandidate,
                    func.coalesce(func.sum(GoldContribution.amount), 0).label(
                        "total_amount"
                    ),
                )
                .outerjoin(
                    GoldContribution, GoldContribution.recipient_candidate_id == GoldCandidate.id
                )
                .where(GoldCandidate.state == state)
                .where(GoldCandidate.office == office)
                .group_by(GoldCandidate.id)
                .order_by(func.coalesce(func.sum(GoldContribution.amount), 0).desc())
                .limit(top_n)
            )
            top_results = db.execute(top_fundraisers_query).all()
            top_fundraisers = [
                CandidateList.model_validate(result[0]) for result in top_results
            ]

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
