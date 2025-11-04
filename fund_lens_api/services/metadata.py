"""Service layer for metadata operations."""

from fund_lens_models.gold import GoldCandidate, GoldCommittee, GoldContributor
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from fund_lens_api.schemas.metadata import (
    CommitteeTypeMetadata,
    EntityTypeMetadata,
    OfficeMetadata,
    StateMetadata,
)


class MetadataService:
    """Business logic for metadata operations."""

    # State code to name mappings
    STATE_NAMES = {
        "AL": "Alabama",
        "AK": "Alaska",
        "AZ": "Arizona",
        "AR": "Arkansas",
        "CA": "California",
        "CO": "Colorado",
        "CT": "Connecticut",
        "DE": "Delaware",
        "FL": "Florida",
        "GA": "Georgia",
        "HI": "Hawaii",
        "ID": "Idaho",
        "IL": "Illinois",
        "IN": "Indiana",
        "IA": "Iowa",
        "KS": "Kansas",
        "KY": "Kentucky",
        "LA": "Louisiana",
        "ME": "Maine",
        "MD": "Maryland",
        "MA": "Massachusetts",
        "MI": "Michigan",
        "MN": "Minnesota",
        "MS": "Mississippi",
        "MO": "Missouri",
        "MT": "Montana",
        "NE": "Nebraska",
        "NV": "Nevada",
        "NH": "New Hampshire",
        "NJ": "New Jersey",
        "NM": "New Mexico",
        "NY": "New York",
        "NC": "North Carolina",
        "ND": "North Dakota",
        "OH": "Ohio",
        "OK": "Oklahoma",
        "OR": "Oregon",
        "PA": "Pennsylvania",
        "RI": "Rhode Island",
        "SC": "South Carolina",
        "SD": "South Dakota",
        "TN": "Tennessee",
        "TX": "Texas",
        "UT": "Utah",
        "VT": "Vermont",
        "VA": "Virginia",
        "WA": "Washington",
        "WV": "West Virginia",
        "WI": "Wisconsin",
        "WY": "Wyoming",
        "DC": "District of Columbia",
        "PR": "Puerto Rico",
        "VI": "Virgin Islands",
        "GU": "Guam",
        "AS": "American Samoa",
        "MP": "Northern Mariana Islands",
    }

    # Entity type to label mappings
    ENTITY_TYPE_LABELS = {
        "IND": "Individual",
        "ORG": "Organization",
        "PAC": "Political Action Committee",
        "CAN": "Candidate",
        "COM": "Committee",
        "PTY": "Party",
        "CCM": "Candidate Committee",
    }

    # Committee type to label mappings
    COMMITTEE_TYPE_LABELS = {
        "H": "House",
        "S": "Senate",
        "P": "Presidential",
        "X": "Non-Qualified",
        "Y": "Qualified",
        "Z": "National Party",
        "N": "PAC - Non-Qualified",
        "Q": "PAC - Qualified",
        "I": "Independent Expenditure",
        "O": "Super PAC",
        "U": "Single Candidate Independent Expenditure",
        "V": "PAC with Non-Contribution Account - Non-Qualified",
        "W": "PAC with Non-Contribution Account - Qualified",
        "D": "Party - Delegate",
        "E": "Party - National",
    }

    # Office to label mappings
    OFFICE_LABELS = {
        "H": "U.S. House",
        "S": "U.S. Senate",
        "P": "President",
    }

    @staticmethod
    def get_contributor_states(db: Session) -> list[str]:
        """Get list of unique states with contributors."""
        query = (
            select(GoldContributor.state)
            .where(GoldContributor.state.isnot(None))
            .distinct()
            .order_by(GoldContributor.state)
        )
        results = db.execute(query).scalars().all()
        return list(results)

    @staticmethod
    def get_contributor_states_with_names(db: Session) -> list[StateMetadata]:
        """Get list of unique states with contributors including full names."""
        states = MetadataService.get_contributor_states(db)
        return [
            StateMetadata(
                code=state,
                name=MetadataService.STATE_NAMES.get(state, state),
            )
            for state in states
        ]

    @staticmethod
    def get_contributor_entity_types(db: Session) -> list[str]:
        """Get list of unique entity types."""
        query = (
            select(GoldContributor.entity_type)
            .where(GoldContributor.entity_type.isnot(None))
            .distinct()
            .order_by(GoldContributor.entity_type)
        )
        results = db.execute(query).scalars().all()
        return list(results)

    @staticmethod
    def get_contributor_entity_types_with_labels(db: Session) -> list[EntityTypeMetadata]:
        """Get list of unique entity types with descriptive labels."""
        entity_types = MetadataService.get_contributor_entity_types(db)
        return [
            EntityTypeMetadata(
                code=entity_type,
                label=MetadataService.ENTITY_TYPE_LABELS.get(entity_type, entity_type),
            )
            for entity_type in entity_types
        ]

    @staticmethod
    def get_committee_states(db: Session) -> list[str]:
        """Get list of unique states with committees."""
        query = (
            select(GoldCommittee.state)
            .where(GoldCommittee.state.isnot(None))
            .distinct()
            .order_by(GoldCommittee.state)
        )
        results = db.execute(query).scalars().all()
        return list(results)

    @staticmethod
    def get_committee_states_with_names(db: Session) -> list[StateMetadata]:
        """Get list of unique states with committees including full names."""
        states = MetadataService.get_committee_states(db)
        return [
            StateMetadata(
                code=state,
                name=MetadataService.STATE_NAMES.get(state, state),
            )
            for state in states
        ]

    @staticmethod
    def get_committee_types(db: Session) -> list[str]:
        """Get list of unique committee types."""
        query = (
            select(GoldCommittee.committee_type)
            .where(GoldCommittee.committee_type.isnot(None))
            .distinct()
            .order_by(GoldCommittee.committee_type)
        )
        results = db.execute(query).scalars().all()
        return list(results)

    @staticmethod
    def get_committee_types_with_labels(db: Session) -> list[CommitteeTypeMetadata]:
        """Get list of unique committee types with descriptive labels."""
        committee_types = MetadataService.get_committee_types(db)
        return [
            CommitteeTypeMetadata(
                code=committee_type,
                label=MetadataService.COMMITTEE_TYPE_LABELS.get(
                    committee_type, committee_type
                ),
            )
            for committee_type in committee_types
        ]

    @staticmethod
    def get_candidate_states(db: Session) -> list[str]:
        """Get list of unique states with candidates."""
        query = (
            select(GoldCandidate.state)
            .where(GoldCandidate.state.isnot(None))
            .distinct()
            .order_by(GoldCandidate.state)
        )
        results = db.execute(query).scalars().all()
        return list(results)

    @staticmethod
    def get_candidate_states_with_names(db: Session) -> list[StateMetadata]:
        """Get list of unique states with candidates including full names."""
        states = MetadataService.get_candidate_states(db)
        return [
            StateMetadata(
                code=state,
                name=MetadataService.STATE_NAMES.get(state, state),
            )
            for state in states
        ]

    @staticmethod
    def get_candidate_offices(db: Session) -> list[str]:
        """Get list of unique candidate offices."""
        query = (
            select(GoldCandidate.office)
            .where(GoldCandidate.office.isnot(None))
            .distinct()
            .order_by(GoldCandidate.office)
        )
        results = db.execute(query).scalars().all()
        return list(results)

    @staticmethod
    def get_candidate_offices_with_labels(db: Session) -> list[OfficeMetadata]:
        """Get list of unique candidate offices with descriptive labels."""
        offices = MetadataService.get_candidate_offices(db)
        return [
            OfficeMetadata(
                code=office,
                label=MetadataService.OFFICE_LABELS.get(office, office),
            )
            for office in offices
        ]

    @staticmethod
    def get_state_name(state_code: str) -> str:
        """Get full state name from state code."""
        return MetadataService.STATE_NAMES.get(state_code, state_code)

    @staticmethod
    def get_entity_type_label(entity_type: str) -> str:
        """Get descriptive label for entity type."""
        return MetadataService.ENTITY_TYPE_LABELS.get(entity_type, entity_type)

    @staticmethod
    def get_committee_type_label(committee_type: str) -> str:
        """Get descriptive label for committee type."""
        return MetadataService.COMMITTEE_TYPE_LABELS.get(committee_type, committee_type)

    @staticmethod
    def get_office_label(office: str) -> str:
        """Get descriptive label for office."""
        return MetadataService.OFFICE_LABELS.get(office, office)