"""
Property Insurance Field Schema
Defines all property coverage fields in exact order with types and validation rules
"""
from dataclasses import dataclass
from enum import Enum
from typing import List, Optional


class FieldType(Enum):
    """Field data types for validation and formatting"""
    TEXT = "text"
    CURRENCY = "currency"
    PERCENTAGE = "percentage"
    DATE = "date"
    PAGE_REF = "page_reference"
    BOOLEAN = "boolean"


@dataclass
class FieldDefinition:
    """Definition of a single insurance field"""
    name: str
    field_type: FieldType
    required: bool = False
    description: str = ""
    llm_hints: Optional[List[str]] = None
    
    def __post_init__(self):
        if self.llm_hints is None:
            self.llm_hints = []


# PROPERTY FIELDS SCHEMA - EXACT ORDER FOR GOOGLE SHEETS
# This is the single source of truth for field ordering
PROPERTY_FIELDS_SCHEMA = [
    FieldDefinition(
        name="Carrier",
        field_type=FieldType.TEXT,
        required=False,
        description="Insurance carrier name",
        llm_hints=["Carrier", "Insurance Company", "Insurer"]
    ),
    FieldDefinition(
        name="Policy Number",
        field_type=FieldType.TEXT,
        required=False,
        description="Policy identification number",
        llm_hints=["Policy Number", "Policy #", "Policy No"]
    ),
    FieldDefinition(
        name="Policy Period",
        field_type=FieldType.TEXT,
        required=False,
        description="Policy effective dates",
        llm_hints=["Policy Period", "Effective Date", "From/To"]
    ),
    FieldDefinition(
        name="Construction Type",
        field_type=FieldType.TEXT,
        required=True,
        description="Building construction classification",
        llm_hints=["MNC", "Frame", "Joisted Masonry", "Construction"]
    ),
    FieldDefinition(
        name="Valuation and Coinsurance",
        field_type=FieldType.TEXT,
        required=True,
        description="Valuation method and coinsurance percentage",
        llm_hints=["RC", "ACV", "90%", "Coinsurance"]
    ),
    FieldDefinition(
        name="Cosmetic Damage",
        field_type=FieldType.TEXT,
        required=False,
        description="Cosmetic damage coverage details",
        llm_hints=["Cosmetic", "Aesthetic Loss"]
    ),
    FieldDefinition(
        name="Building",
        field_type=FieldType.CURRENCY,
        required=True,
        description="Building coverage limit",
        llm_hints=["Building", "Building Coverage", "Building Limit"]
    ),
    FieldDefinition(
        name="Pumps",
        field_type=FieldType.CURRENCY,
        required=False,
        description="Pump equipment coverage",
        llm_hints=["Pumps", "Pump Equipment"]
    ),
    FieldDefinition(
        name="Canopy",
        field_type=FieldType.CURRENCY,
        required=False,
        description="Canopy structure coverage",
        llm_hints=["Canopy", "Canopy Structure"]
    ),
    FieldDefinition(
        name="ROOF EXCLUSION",
        field_type=FieldType.TEXT,
        required=False,
        description="Roof exclusion details",
        llm_hints=["Roof Exclusion", "Excluded"]
    ),
    FieldDefinition(
        name="Roof Surfacing",
        field_type=FieldType.TEXT,
        required=False,
        description="Roof surfacing coverage",
        llm_hints=["Roof Surfacing", "CP 10 36", "Form"]
    ),
    FieldDefinition(
        name="Roof Surfacing -Limitation",
        field_type=FieldType.TEXT,
        required=False,
        description="Roof surfacing limitations",
        llm_hints=["Limitations on Coverage", "Roof Surfacing"]
    ),
    FieldDefinition(
        name="Business Personal Property",
        field_type=FieldType.CURRENCY,
        required=False,
        description="Business personal property coverage",
        llm_hints=["BPP", "Business Personal Property", "Personal Property"]
    ),
    FieldDefinition(
        name="Business Income",
        field_type=FieldType.CURRENCY,
        required=False,
        description="Business income coverage",
        llm_hints=["Business Income", "BI", "Income Coverage"]
    ),
    FieldDefinition(
        name="Business Income with Extra Expense",
        field_type=FieldType.CURRENCY,
        required=False,
        description="Business income with extra expense",
        llm_hints=["Extra Expense", "BI with Extra Expense"]
    ),
    FieldDefinition(
        name="Equipment Breakdown",
        field_type=FieldType.TEXT,
        required=False,
        description="Equipment breakdown coverage",
        llm_hints=["Equipment Breakdown", "Boiler", "Machinery"]
    ),
    FieldDefinition(
        name="Outdoor Signs",
        field_type=FieldType.CURRENCY,
        required=False,
        description="Outdoor sign coverage",
        llm_hints=["Outdoor Signs", "Signs"]
    ),
    FieldDefinition(
        name="Signs Within 1,000 Feet to Premises",
        field_type=FieldType.TEXT,
        required=False,
        description="Sign location coverage details",
        llm_hints=["1,000 Feet", "Signs Within"]
    ),
    FieldDefinition(
        name="Employee Dishonesty",
        field_type=FieldType.TEXT,
        required=False,
        description="Employee dishonesty coverage",
        llm_hints=["Employee Dishonesty", "Fidelity"]
    ),
    FieldDefinition(
        name="Money & Securities",
        field_type=FieldType.TEXT,
        required=False,
        description="Money and securities coverage",
        llm_hints=["Money", "Securities", "Cash"]
    ),
    FieldDefinition(
        name="Money and Securities (Inside; Outside)",
        field_type=FieldType.TEXT,
        required=False,
        description="Inside and outside premises money coverage",
        llm_hints=["Inside", "Outside", "Premises"]
    ),
    FieldDefinition(
        name="Spoilage",
        field_type=FieldType.TEXT,
        required=False,
        description="Spoilage coverage",
        llm_hints=["Spoilage", "Refrigeration"]
    ),
    FieldDefinition(
        name="Theft",
        field_type=FieldType.TEXT,
        required=False,
        description="Theft coverage",
        llm_hints=["Theft", "Burglary"]
    ),
    FieldDefinition(
        name="Theft Sublimit",
        field_type=FieldType.CURRENCY,
        required=False,
        description="Theft sublimit amount",
        llm_hints=["Theft Sublimit", "Theft Limit"]
    ),
    FieldDefinition(
        name="Theft Deductible",
        field_type=FieldType.CURRENCY,
        required=False,
        description="Theft deductible amount",
        llm_hints=["Theft Deductible", "Deductible for Theft"]
    ),
    FieldDefinition(
        name="Windstorm or Hail",
        field_type=FieldType.TEXT,
        required=False,
        description="Windstorm or hail coverage",
        llm_hints=["Windstorm", "Hail", "Wind", "Excluded"]
    ),
    FieldDefinition(
        name="Named Storm Deductible",
        field_type=FieldType.TEXT,
        required=False,
        description="Named storm deductible",
        llm_hints=["Named Storm", "Hurricane Deductible"]
    ),
    FieldDefinition(
        name="Wind and Hail and Named Storm exclusion",
        field_type=FieldType.TEXT,
        required=False,
        description="Wind, hail, and named storm exclusions",
        llm_hints=["Exclusion", "Wind Exclusion"]
    ),
    FieldDefinition(
        name="All Other Perils Deductible",
        field_type=FieldType.CURRENCY,
        required=False,
        description="All other perils (AOP) deductible",
        llm_hints=["AOP", "All Other Perils", "Deductible"]
    ),
    FieldDefinition(
        name="Fire Station Alarm",
        field_type=FieldType.TEXT,
        required=False,
        description="Fire station alarm requirement",
        llm_hints=["Fire Station", "Alarm"]
    ),
    FieldDefinition(
        name="Burglar Alarm",
        field_type=FieldType.TEXT,
        required=False,
        description="Burglar alarm requirement",
        llm_hints=["Burglar Alarm", "Security Alarm"]
    ),
    FieldDefinition(
        name="Terrorism",
        field_type=FieldType.TEXT,
        required=False,
        description="Terrorism coverage",
        llm_hints=["Terrorism", "TRIA", "Terrorist Acts"]
    ),
    FieldDefinition(
        name="Protective Safeguards Requirements",
        field_type=FieldType.TEXT,
        required=False,
        description="Protective safeguard requirements",
        llm_hints=["Protective Safeguards", "Requirements"]
    ),
    FieldDefinition(
        name="Minimum Earned Premium (MEP)",
        field_type=FieldType.PERCENTAGE,
        required=False,
        description="Minimum earned premium percentage",
        llm_hints=["MEP", "25%", "35%", "Minimum Earned"]
    ),
    FieldDefinition(
        name="Property Premium",
        field_type=FieldType.CURRENCY,
        required=False,
        description="Base property premium (may differ from total)",
        llm_hints=["Property Premium", "Base Premium"]
    ),
    FieldDefinition(
        name="Total Property Premium",
        field_type=FieldType.CURRENCY,
        required=True,
        description="Total property premium including endorsements",
        llm_hints=["Total Premium", "Total Charges", "Premium", "Total Premium (With/Without Terrorism)", "Total Property Premium"]
    ),
    FieldDefinition(
        name="Policy Premium",
        field_type=FieldType.CURRENCY,
        required=False,
        description="Overall policy premium",
        llm_hints=["Policy Premium", "Total Policy Cost"]
    ),
]


def get_field_names() -> List[str]:
    """Get list of field names in schema order"""
    return [field.name for field in PROPERTY_FIELDS_SCHEMA]


def get_field_by_name(field_name: str) -> Optional[FieldDefinition]:
    """Get field definition by name"""
    for field in PROPERTY_FIELDS_SCHEMA:
        if field.name == field_name:
            return field
    return None


def get_required_fields() -> List[str]:
    """Get list of required field names"""
    return [field.name for field in PROPERTY_FIELDS_SCHEMA if field.required]

