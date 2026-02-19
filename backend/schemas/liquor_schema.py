"""
Liquor/Bar Insurance Field Schema
Defines all liquor coverage fields in exact order with types and validation rules
"""
from dataclasses import dataclass
from typing import List, Optional
from .property_schema import FieldType, FieldDefinition


# LIQUOR FIELDS SCHEMA - EXACT ORDER FOR GOOGLE SHEETS
LIQUOR_FIELDS_SCHEMA = [
    FieldDefinition(
        name="Each Occurrence/General Aggregate Limits",
        field_type=FieldType.TEXT,
        required=True,
        description="Per occurrence and aggregate liability limits",
        llm_hints=["Each Occurrence", "General Aggregate", "$1,000,000/$2,000,000"]
    ),
    FieldDefinition(
        name="Sales - Subject to Audit",
        field_type=FieldType.TEXT,
        required=False,
        description="Sales amount and audit requirements",
        llm_hints=["Sales", "Subject to Audit", "Revenue"]
    ),
    FieldDefinition(
        name="Assault & Battery/Firearms/Active Assailant",
        field_type=FieldType.TEXT,
        required=False,
        description="Assault, battery, firearms, and active assailant coverage",
        llm_hints=["Assault", "Battery", "Firearms", "Active Assailant"]
    ),
    FieldDefinition(
        name="Requirements",
        field_type=FieldType.TEXT,
        required=False,
        description="Special requirements or conditions",
        llm_hints=["Requirements", "Conditions", "Stipulations"]
    ),
    FieldDefinition(
        name="If any subjectivities in quote please add",
        field_type=FieldType.TEXT,
        required=False,
        description="Subjectivities and special conditions",
        llm_hints=["Subjectivities", "Conditions", "Special Notes"]
    ),
    FieldDefinition(
        name="Minimum Earned",
        field_type=FieldType.PERCENTAGE,
        required=False,
        description="Minimum earned premium percentage",
        llm_hints=["MEP", "Minimum Earned", "25%", "35%"]
    ),
    FieldDefinition(
        name="Liquor Premium",
        field_type=FieldType.CURRENCY,
        required=False,
        description="Base liquor liability premium",
        llm_hints=["Liquor Premium", "Liquor Liability Premium"]
    ),
    FieldDefinition(
        name="Total Liquor Premium",
        field_type=FieldType.CURRENCY,
        required=True,
        description="Total liquor premium including endorsements",
        llm_hints=["Total Premium", "Total Charges", "Total Premium (With/Without Terrorism)", "Total Liquor Premium"]
    ),
    FieldDefinition(
        name="Policy Premium",
        field_type=FieldType.CURRENCY,
        required=False,
        description="Overall policy premium",
        llm_hints=["Policy Premium", "Total Policy Cost"]
    ),
]


def get_liquor_field_names() -> List[str]:
    """Get list of liquor field names in schema order"""
    return [field.name for field in LIQUOR_FIELDS_SCHEMA]


def get_liquor_field_by_name(field_name: str) -> Optional[FieldDefinition]:
    """Get liquor field definition by name"""
    for field in LIQUOR_FIELDS_SCHEMA:
        if field.name == field_name:
            return field
    return None


def get_liquor_required_fields() -> List[str]:
    """Get list of required liquor field names"""
    return [field.name for field in LIQUOR_FIELDS_SCHEMA if field.required]

