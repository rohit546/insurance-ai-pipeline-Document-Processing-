"""
Workers Compensation Insurance Field Schema
Defines all workers compensation coverage fields in exact order with types and validation rules
"""
from dataclasses import dataclass
from typing import List, Optional
from .property_schema import FieldType, FieldDefinition


# WORKERS COMPENSATION FIELDS SCHEMA - EXACT ORDER FOR GOOGLE SHEETS
WORKERS_COMP_FIELDS_SCHEMA = [
    FieldDefinition(
        name="Limits",
        field_type=FieldType.TEXT,
        required=True,
        description="Workers compensation limits per accident/policy/employee",
        llm_hints=["$1,000,000 Each Accident", "$1,000,000 Policy Limit", "$1,000,000 Each Employee", "$500,000 / $500,000 / $500,000"]
    ),
    FieldDefinition(
        name="FEIN #",
        field_type=FieldType.TEXT,
        required=False,
        description="Federal Employer Identification Number",
        llm_hints=["47-4792684", "39-4013959", "33-4251695", "FEIN", "Federal Employer Identification Number"]
    ),
    FieldDefinition(
        name="Payroll - Subject to Audit",
        field_type=FieldType.CURRENCY,
        required=False,
        description="Payroll amount subject to audit",
        llm_hints=["$36,000", "$45,000", "$30,000", "Payroll", "Subject to Audit"]
    ),
    FieldDefinition(
        name="Excluded Officer",
        field_type=FieldType.TEXT,
        required=False,
        description="Excluded officer information",
        llm_hints=["Parvez Jiwani", "Provide Details", "Details Required", "Officer decision on Inclusion / Exclusion required"]
    ),
    FieldDefinition(
        name="If Opting out from Workers Compensation Coverage",
        field_type=FieldType.TEXT,
        required=False,
        description="Opt-out information and liability statements",
        llm_hints=["By State Law in GA you are liable", "by not opting any injuries to the employees during work hours will not be covered"]
    ),
    FieldDefinition(
        name="Workers Compensation Premium",
        field_type=FieldType.CURRENCY,
        required=True,
        description="Workers compensation premium amount",
        llm_hints=["$1,500.00", "WC Premium", "Workers Comp Premium", "TOTAL excl Terrorism", "TOTAL CHARGES W/O TRIA"]
    ),
    FieldDefinition(
        name="Total Premium",
        field_type=FieldType.CURRENCY,
        required=True,
        description="Total premium including terrorism",
        llm_hints=["$3,500.00", "TOTAL incl Terrorism", "TOTAL CHARGES WITH TRIA", "Total Premium", "Annual Premium"]
    ),
    FieldDefinition(
        name="Policy Premium",
        field_type=FieldType.CURRENCY,
        required=False,
        description="Base policy premium",
        llm_hints=["$2,500.00", "Policy Premium", "Base Premium", "Workers Compensation base amount"]
    ),
]


def get_workers_comp_field_names() -> List[str]:
    """Get list of workers compensation field names in schema order"""
    return [field.name for field in WORKERS_COMP_FIELDS_SCHEMA]


def get_workers_comp_field_by_name(field_name: str) -> Optional[FieldDefinition]:
    """Get workers compensation field definition by name"""
    for field in WORKERS_COMP_FIELDS_SCHEMA:
        if field.name == field_name:
            return field
    return None


def get_workers_comp_required_fields() -> List[str]:
    """Get list of required workers compensation field names"""
    return [field.name for field in WORKERS_COMP_FIELDS_SCHEMA if field.required]

