"""Pydantic schemas for metadata resources."""

from pydantic import BaseModel, ConfigDict, Field


class StateMetadata(BaseModel):
    """State metadata with descriptive label."""

    code: str = Field(description="Two-letter state code")
    name: str = Field(description="Full state name")

    model_config = ConfigDict(from_attributes=True)


class EntityTypeMetadata(BaseModel):
    """Entity type metadata with descriptive label."""

    code: str = Field(description="Entity type code")
    label: str = Field(description="Human-readable label")

    model_config = ConfigDict(from_attributes=True)


class CommitteeTypeMetadata(BaseModel):
    """Committee type metadata with descriptive label."""

    code: str = Field(description="Committee type code")
    label: str = Field(description="Human-readable label")

    model_config = ConfigDict(from_attributes=True)


class OfficeMetadata(BaseModel):
    """Office metadata with descriptive label."""

    code: str = Field(description="Office code")
    label: str = Field(description="Human-readable label")

    model_config = ConfigDict(from_attributes=True)