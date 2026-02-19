"""
General Liability Insurance Field Schema
Defines all GL coverage fields in exact order with types and validation rules
"""
from dataclasses import dataclass
from typing import List, Optional
from .property_schema import FieldType, FieldDefinition


# GENERAL LIABILITY FIELDS SCHEMA - EXACT ORDER FOR GOOGLE SHEETS
GL_FIELDS_SCHEMA = [
    FieldDefinition(
        name="Each Occurrence/General Aggregate Limits",
        field_type=FieldType.TEXT,
        required=True,
        description="Per occurrence and aggregate liability limits",
        llm_hints=["Each Occurrence", "General Aggregate", "$1,000,000/$2,000,000"]
    ),
    FieldDefinition(
        name="Liability Deductible - Per claim or Per Occ basis",
        field_type=FieldType.CURRENCY,
        required=False,
        description="Liability deductible per claim or occurrence",
        llm_hints=["Deductible", "Per Claim", "Per Occ"]
    ),
    FieldDefinition(
        name="Hired Auto And Non-Owned Auto Liability - Without Delivery Service",
        field_type=FieldType.TEXT,
        required=False,
        description="Hired and non-owned auto liability coverage",
        llm_hints=["Hired Auto", "Non-Owned Auto", "Without Delivery"]
    ),
    FieldDefinition(
        name="Fuel Contamination coverage limits",
        field_type=FieldType.TEXT,
        required=False,
        description="Fuel contamination coverage details",
        llm_hints=["Fuel Contamination", "Pollution"]
    ),
    FieldDefinition(
        name="Vandalism coverage",
        field_type=FieldType.TEXT,
        required=False,
        description="Vandalism coverage details",
        llm_hints=["Vandalism", "Malicious Damage"]
    ),
    FieldDefinition(
        name="Garage Keepers Liability",
        field_type=FieldType.TEXT,
        required=False,
        description="Garage keepers liability coverage",
        llm_hints=["Garage Keepers", "GKL"]
    ),
    FieldDefinition(
        name="Employment Practices Liability",
        field_type=FieldType.TEXT,
        required=False,
        description="Employment practices liability coverage",
        llm_hints=["EPL", "Employment Practices"]
    ),
    FieldDefinition(
        name="Abuse & Molestation Coverage limits",
        field_type=FieldType.TEXT,
        required=False,
        description="Abuse and molestation coverage",
        llm_hints=["Abuse", "Molestation"]
    ),
    FieldDefinition(
        name="Assault & Battery Coverage limits",
        field_type=FieldType.TEXT,
        required=False,
        description="Assault and battery coverage",
        llm_hints=["Assault", "Battery"]
    ),
    FieldDefinition(
        name="Firearms/Active Assailant Coverage limits",
        field_type=FieldType.TEXT,
        required=False,
        description="Firearms and active assailant coverage",
        llm_hints=["Firearms", "Active Assailant"]
    ),
    FieldDefinition(
        name="Additional Insured",
        field_type=FieldType.TEXT,
        required=False,
        description="Additional insured parties",
        llm_hints=["Additional Insured", "Named Insured"]
    ),
    FieldDefinition(
        name="Additional Insured (Mortgagee)",
        field_type=FieldType.TEXT,
        required=False,
        description="Additional insured mortgagee",
        llm_hints=["Mortgagee", "Lender"]
    ),
    FieldDefinition(
        name="Additional Insured - Jobber",
        field_type=FieldType.TEXT,
        required=False,
        description="Additional insured jobber",
        llm_hints=["Jobber", "Distributor"]
    ),
    FieldDefinition(
        name="Exposure",
        field_type=FieldType.TEXT,
        required=False,
        description="Exposure details",
        llm_hints=["Exposure", "Risk Exposure"]
    ),
    FieldDefinition(
        name="Rating basis: If Sales - Subject to Audit",
        field_type=FieldType.TEXT,
        required=False,
        description="Rating basis and audit requirements",
        llm_hints=["Rating Basis", "Sales", "Subject to Audit"]
    ),
    FieldDefinition(
        name="Terrorism",
        field_type=FieldType.TEXT,
        required=False,
        description="Terrorism coverage",
        llm_hints=["Terrorism", "TRIA"]
    ),
    FieldDefinition(
        name="Personal and Advertising Injury Limit",
        field_type=FieldType.CURRENCY,
        required=False,
        description="Personal and advertising injury limit",
        llm_hints=["Personal Injury", "Advertising Injury"]
    ),
    FieldDefinition(
        name="Products/Completed Operations Aggregate Limit",
        field_type=FieldType.CURRENCY,
        required=False,
        description="Products and completed operations aggregate",
        llm_hints=["Products", "Completed Operations", "Aggregate"]
    ),
    FieldDefinition(
        name="Minimum Earned",
        field_type=FieldType.PERCENTAGE,
        required=False,
        description="Minimum earned premium percentage",
        llm_hints=["MEP", "Minimum Earned", "25%", "35%"]
    ),
    FieldDefinition(
        name="General Liability Premium",
        field_type=FieldType.CURRENCY,
        required=False,
        description="Base GL premium",
        llm_hints=["GL Premium", "Liability Premium"]
    ),
    FieldDefinition(
        name="Total GL Premium",
        field_type=FieldType.CURRENCY,
        required=True,
        description="Total GL premium including endorsements",
        llm_hints=["Total Premium", "Total Charges", "Total Premium (With/Without Terrorism)", "Total GL Premium", "Grand Total"]
    ),
    FieldDefinition(
        name="Policy Premium",
        field_type=FieldType.CURRENCY,
        required=False,
        description="Overall policy premium",
        llm_hints=["Policy Premium", "Total Policy Cost"]
    ),
]


def get_gl_field_names() -> List[str]:
    """Get list of GL field names in schema order"""
    return [field.name for field in GL_FIELDS_SCHEMA]


def get_gl_field_by_name(field_name: str) -> Optional[FieldDefinition]:
    """Get GL field definition by name"""
    for field in GL_FIELDS_SCHEMA:
        if field.name == field_name:
            return field
    return None


def get_gl_required_fields() -> List[str]:
    """Get list of required GL field names"""
    return [field.name for field in GL_FIELDS_SCHEMA if field.required]

