"""Utilities for field selection in API responses."""

from typing import Any, TypeVar

from pydantic import BaseModel

T = TypeVar("T", bound=BaseModel)


def apply_field_selection(
    data: T | list[T] | dict[str, Any], fields: set[str] | None
) -> dict[str, Any] | list[dict[str, Any]]:
    """Apply field selection to Pydantic models or dictionaries.

    Args:
        data: A Pydantic model instance, list of instances, or dict
        fields: Set of field names to include, or None to include all fields

    Returns:
        Filtered dictionary or list of dictionaries

    Examples:
        >>> model = CandidateList(id=1, name="John", ...)
        >>> apply_field_selection(model, {"id", "name"})
        {"id": 1, "name": "John"}

        >>> models = [CandidateList(...), CandidateList(...)]
        >>> apply_field_selection(models, {"id", "name"})
        [{"id": 1, "name": "John"}, {"id": 2, "name": "Jane"}]
    """
    # If no fields specified, return all fields
    if fields is None or len(fields) == 0:
        if isinstance(data, list):
            return [item.model_dump() if isinstance(item, BaseModel) else item for item in data]
        elif isinstance(data, BaseModel):
            return data.model_dump()
        else:
            return data

    # Apply field selection
    if isinstance(data, list):
        return [
            _filter_dict(
                item.model_dump() if isinstance(item, BaseModel) else item, fields
            )
            for item in data
        ]
    elif isinstance(data, BaseModel):
        return _filter_dict(data.model_dump(), fields)
    else:
        return _filter_dict(data, fields)


def _filter_dict(data: dict[str, Any], fields: set[str]) -> dict[str, Any]:
    """Filter dictionary to only include specified fields.

    Supports nested field selection using dot notation (e.g., "stats.total_amount").
    """
    result: dict[str, Any] = {}

    for field in fields:
        # Handle nested fields (e.g., "stats.total_amount")
        if "." in field:
            parts = field.split(".", 1)
            parent_field = parts[0]
            nested_field = parts[1]

            if parent_field in data:
                # Ensure parent field is in result
                if parent_field not in result:
                    parent_value = data[parent_field]
                    if isinstance(parent_value, dict):
                        result[parent_field] = {}
                    elif isinstance(parent_value, BaseModel):
                        result[parent_field] = {}
                    else:
                        result[parent_field] = parent_value
                        continue

                # Extract nested value
                parent_value = data[parent_field]
                if isinstance(parent_value, dict) and nested_field in parent_value:
                    if not isinstance(result[parent_field], dict):
                        result[parent_field] = {}
                    result[parent_field][nested_field] = parent_value[nested_field]
                elif isinstance(parent_value, BaseModel):
                    parent_dict = parent_value.model_dump()
                    if nested_field in parent_dict:
                        if not isinstance(result[parent_field], dict):
                            result[parent_field] = {}
                        result[parent_field][nested_field] = parent_dict[nested_field]
        else:
            # Simple field (no nesting)
            if field in data:
                result[field] = data[field]

    return result


def parse_fields_param(fields_param: str | None) -> set[str] | None:
    """Parse the fields query parameter into a set of field names.

    Args:
        fields_param: Comma-separated field names (e.g., "id,name,stats.total_amount")

    Returns:
        Set of field names, or None if param is empty/None

    Examples:
        >>> parse_fields_param("id,name,party")
        {"id", "name", "party"}

        >>> parse_fields_param("id,stats.total_amount")
        {"id", "stats.total_amount"}

        >>> parse_fields_param(None)
        None
    """
    if not fields_param:
        return None

    # Split by comma and strip whitespace
    fields = {f.strip() for f in fields_param.split(",") if f.strip()}

    return fields if fields else None
