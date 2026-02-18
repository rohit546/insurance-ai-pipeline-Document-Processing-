"""
LLM-Based ACORD Commercial General Liability Section Field Extraction
Extracts key fields from ACORD Commercial General Liability Section forms using GPT-4o-mini
"""

import os
import json
import sys
from pathlib import Path
from typing import Dict, Optional
from dotenv import load_dotenv
from openai import OpenAI

# Load environment variables
load_dotenv()


class ACORDGLAExtractor:
    """Extract fields from ACORD Commercial General Liability Section forms using LLM"""
    
    def __init__(self, model: str = "gpt-4o-mini"):
        """
        Initialize the extractor
        
        Args:
            model: OpenAI model to use (default: gpt-4o-mini)
        """
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            raise ValueError("OPENAI_API_KEY not found in environment variables")
        
        self.client = OpenAI(api_key=api_key)
        self.model = model
    
    def parse_dual_extraction(self, combo_text: str) -> tuple[str, str]:
        """
        Parse combo file to extract pdfplumber and PyMuPDF sections separately
        
        Args:
            combo_text: Combined extraction text with both methods
            
        Returns:
            Tuple of (pdfplumber_text, pymupdf_text)
        """
        pdfplumber_text = ""
        pymupdf_text = ""
        
        # Split by the extraction method markers
        if "--- PDFPLUMBER (Table-aware) ---" in combo_text:
            parts = combo_text.split("--- PDFPLUMBER (Table-aware) ---")
            if len(parts) > 1:
                pdfplumber_section = parts[1]
                
                # Extract pdfplumber text (everything until PyMuPDF section)
                if "--- PYMUPDF (Text layer) ---" in pdfplumber_section:
                    pdfplumber_text = pdfplumber_section.split("--- PYMUPDF (Text layer) ---")[0].strip()
                    pymupdf_text = pdfplumber_section.split("--- PYMUPDF (Text layer) ---")[1].strip()
                else:
                    pdfplumber_text = pdfplumber_section.strip()
        
        # If parsing failed, return the whole text as single source
        if not pdfplumber_text and not pymupdf_text:
            pdfplumber_text = combo_text
        
        return pdfplumber_text, pymupdf_text
    
    def create_extraction_prompt(self, pdfplumber_text: str, pymupdf_text: str = None) -> str:
        """
        Create the extraction prompt for the LLM with dual extraction validation
        
        Args:
            pdfplumber_text: Extraction text from pdfplumber (table-aware)
            pymupdf_text: Extraction text from PyMuPDF (text layer, optional)
            
        Returns:
            Formatted prompt string
        """
        if pymupdf_text:
            # Dual extraction mode - cross-validation (ACORD Commercial General Liability Section)
            prompt = """You are an expert in ACORD Commercial General Liability Section form extraction.

You are given TWO extraction sources for the SAME document:
1. **PDFPLUMBER (Table-aware)**: Preserves table structure - USE THIS AS PRIMARY SOURCE for schedule of hazards and coverage data
2. **PYMUPDF (Text layer)**: Raw text extraction - use as cross-validation/fallback

**PRIORITY**: For table extraction (especially Schedule of Hazards), prioritize pdfplumber's TABLE sections.
Cross-validate with PyMuPDF text when needed, but trust pdfplumber's structured table data first.

==================================================
CRITICAL: DATE CLARIFICATION
==================================================
- The top "DATE (MM/DD/YYYY)" is the FORM DATE (when the form was filled out). Do NOT use it as policy dates.
- Policy dates are in the "EFFECTIVE DATE" field (policy start date).

==================================================
FIELDS TO EXTRACT (ACORD Commercial General Liability Section)
==================================================
Return ONLY a valid JSON object with:

1) Top-level policy information:
- agency_name: string or null (extract from "AGENCY" field)
- agency_address: string or null (combine all address lines with commas from AGENCY section)
- agency_phone: string or null (from "PHONE" field)
- agency_fax: string or null (from "FAX" field)
- agency_email: string or null (from "E-MAIL ADDRESS" field)
- carrier: string or null (from "CARRIER" field)
- policy_number: string or null (from "POLICY NUMBER" field)
- effective_date: MM/DD/YYYY or null (from "EFFECTIVE DATE" field)
- applicant_first_named_insured: string or null (from "APPLICANT / FIRST NAMED INSURED" field)
- form_date: MM/DD/YYYY or null (from top "DATE (MM/DD/YYYY)" field - this is when form was filled, NOT policy date)

2) Coverage information:
- commercial_general_liability_checked: boolean (true if "COMMERCIAL GENERAL LIABILITY" checkbox is marked)
- claims_made: boolean or null (true if "CLAIMS MADE" is checked, false if "OCCURRENCE" is checked, null if neither)
- occurrence: boolean or null (true if "OCCURRENCE" is checked, false if "CLAIMS MADE" is checked, null if neither)
- owners_contractors_protective: boolean (true if "OWNER'S & CONTRACTOR'S PROTECTIVE" checkbox is marked)

**CRITICAL**: If "OCCURRENCE" is checked, set "occurrence": true and "claims_made": false.
If "CLAIMS MADE" is checked, set "claims_made": true and "occurrence": false.

3) Limits section:
**⛔⛔⛔ CRITICAL - YOU MUST EXTRACT ALL AMOUNTS - THEY ARE VISIBLE IN THE TEXT ⛔⛔⛔**:
**DO NOT SKIP THIS SECTION - THE AMOUNTS ARE CLEARLY VISIBLE IN THE EXTRACTION TEXT BELOW**
- The amounts ARE visible in the LIMITS section - extract them EXACTLY as they appear (with dollar sign "$" and commas)
- Do NOT skip amounts that are clearly visible in the text
- Preserve the exact format: "$ 2,000,000" (with space after $ and commas in numbers)
- If a field shows "$ " (dollar sign with space but no number), set to null

**HOW TO FIND AMOUNTS IN THE TEXT**:
- Look for patterns like "GENERAL AGGREGATE $ 2,000,000" or "GENERAL AGGREGATE" followed by "$ 2,000,000" on the same or next line
- In TABLE sections (from pdfplumber), the amounts are in structured columns - use these as PRIMARY source
- The format is always: [FIELD NAME] $ [AMOUNT] where amount has commas (e.g., "$ 2,000,000", "$ 1,000,000", "$ 100,000", "$ 5,000")
- Amounts may appear on the same line as the field name OR on the next line - look carefully
- Example patterns you'll see:
  * "GENERAL AGGREGATE $ 2,000,000"
  * "PRODUCTS & COMPLETED OPERATIONS AGGREGATE $ 2,000,000"
  * "EACH OCCURRENCE $ 1,000,000"
  * "DAMAGE TO RENTED PREMISES (each occurrence) $ 100,000"
  * "MEDICAL EXPENSE (Any one person) $ 5,000"

- general_aggregate: string or null 
  **EXTRACT FROM**: Look for "GENERAL AGGREGATE" followed by "$" and a number with commas
  **EXAMPLE**: If you see "GENERAL AGGREGATE $ 2,000,000", extract exactly "$ 2,000,000"
  **LOCATION**: In LIMITS section, usually appears as "GENERAL AGGREGATE $ 2,000,000" or in TABLE 2 as a structured row
  
- general_aggregate_applies_per: "POLICY"|"LOCATION"|"PROJECT"|"OTHER"|null 
  **EXTRACT FROM**: "LIMIT APPLIES PER:" section with checkboxes
  **CHECK**: Which checkbox is marked (X) - POLICY, LOCATION, PROJECT, or OTHER
  **EXAMPLE**: If "POLICY" checkbox has X, return "POLICY"
  
- products_completed_operations_aggregate: string or null 
  **EXTRACT FROM**: Look for "PRODUCTS & COMPLETED OPERATIONS AGGREGATE" followed by "$" and a number
  **EXAMPLE**: If you see "PRODUCTS & COMPLETED OPERATIONS AGGREGATE $ 2,000,000", extract exactly "$ 2,000,000"
  
- personal_advertising_injury: string or null 
  **EXTRACT FROM**: Look for "PERSONAL & ADVERTISING INJURY" followed by "$" and a number
  **EXAMPLE**: If you see "PERSONAL & ADVERTISING INJURY $ 1,000,000", extract exactly "$ 1,000,000"
  
- each_occurrence: string or null 
  **EXTRACT FROM**: Look for "EACH OCCURRENCE" followed by "$" and a number
  **EXAMPLE**: If you see "EACH OCCURRENCE $ 1,000,000", extract exactly "$ 1,000,000"
  
- damage_to_rented_premises: string or null 
  **EXTRACT FROM**: Look for "DAMAGE TO RENTED PREMISES (each occurrence)" followed by "$" and a number
  **EXAMPLE**: If you see "DAMAGE TO RENTED PREMISES (each occurrence) $ 100,000", extract exactly "$ 100,000"
  
- medical_expense: string or null 
  **EXTRACT FROM**: Look for "MEDICAL EXPENSE (Any one person)" followed by "$" and a number
  **EXAMPLE**: If you see "MEDICAL EXPENSE (Any one person) $ 5,000", extract exactly "$ 5,000"
  
- employee_benefits: string or null 
  **EXTRACT FROM**: Look for "EMPLOYEE BENEFITS" followed by "$"
  **IF**: Shows "$ " with no number after it → set to null
  **IF**: Shows "$ [number]" → extract exactly as shown (e.g., "$ 10,000")

**VERIFICATION CHECKLIST**:
Before returning the JSON, verify:
1. Did I find "GENERAL AGGREGATE" in the text? What amount follows it?
2. Did I find "PRODUCTS & COMPLETED OPERATIONS AGGREGATE"? What amount follows it?
3. Did I find "EACH OCCURRENCE"? What amount follows it?
4. Did I find "PERSONAL & ADVERTISING INJURY"? What amount follows it?
5. Did I find "DAMAGE TO RENTED PREMISES"? What amount follows it?
6. Did I find "MEDICAL EXPENSE"? What amount follows it?
7. If any of these show a dollar amount (like "$ 2,000,000"), I MUST include it - do NOT return null

**CRITICAL REMINDER**: The amounts ARE in the text. If you see "$ 2,000,000" or "$ 1,000,000" or "$ 100,000" or "$ 5,000" after a field name, extract it. Do NOT skip visible amounts.

4) Deductibles section:
- property_damage_deductible: string or null (from "PROPERTY DAMAGE" field)
- property_damage_per_claim: boolean or null (true if "PER CLAIM" is checked under Property Damage)
- property_damage_per_occurrence: boolean or null (true if "PER OCCURRENCE" is checked under Property Damage)
- bodily_injury_deductible: string or null (from "BODILY INJURY" field)
- bodily_injury_per_claim: boolean or null (true if "PER CLAIM" is checked under Bodily Injury)
- bodily_injury_per_occurrence: boolean or null (true if "PER OCCURRENCE" is checked under Bodily Injury)

5) Premiums section (if present):
- premises_operations_premium: string or null (from "PREMISES/OPERATIONS" field)
- products_premium: string or null (from "PRODUCTS" field)
- other_premium: string or null (from "OTHER" field)
- total_premium: string or null (from "TOTAL" field)

6) Schedule of Hazards:
- Extract ALL hazards from the "SCHEDULE OF HAZARDS" table
- Return as an array of objects under key "schedule_of_hazards"
- Each hazard object should include:
  {
    "loc_number": "string or null",  # LOC # column
    "haz_number": "string or null",   # HAZ # column
    "class_code": "string or null",   # CLASS CODE column
    "premium_basis": "string or null", # PREMIUM BASIS column (e.g., "AREA", "GrSales", "PAYROLL")
    "exposure": "string or null",     # EXPOSURE column (e.g., "2,760 Sqft", "525,000")
    "terr": "string or null",         # TERR column
    "rate_prem_ops": "string or null", # RATE (PREM/OPS) column
    "rate_products": "string or null", # RATE (PRODUCTS) column
    "premium_prem_ops": "string or null", # PREMIUM (PREM/OPS) column
    "premium_products": "string or null", # PREMIUM (PRODUCTS) column
    "classification_description": "string or null" # CLASSIFICATION DESCRIPTION below the row
  }
- **CRITICAL**: Only include hazards that have actual data (at least one field filled). Skip blank/empty rows.

7) Other coverages, restrictions and/or endorsements:
- other_coverages_restrictions_endorsements: string or null (extract text from "OTHER COVERAGES, RESTRICTIONS AND/OR ENDORSEMENTS" section)

8) Wisconsin-specific fields (if applicable):
- um_uim_coverage_is: boolean or null (true if "IS" is checked under "1. UM/UIM COVERAGE")
- um_uim_coverage_is_not_available: boolean or null (true if "IS NOT AVAILABLE" is checked)
- medical_payments_coverage_is: boolean or null (true if "IS" is checked under "2. MEDICAL PAYMENTS COVERAGE")
- medical_payments_coverage_is_not_available: boolean or null (true if "IS NOT AVAILABLE" is checked)

9) validation_notes: string (brief notes about OCR conflicts or assumptions)

**Output Format:**
{
  "agency_name": "...",
  "agency_address": "...",
  "agency_phone": "...",
  "agency_fax": "...",
  "agency_email": "...",
  "carrier": "...",
  "policy_number": "...",
  "effective_date": "MM/DD/YYYY or null",
  "applicant_first_named_insured": "...",
  "form_date": "MM/DD/YYYY or null",
  "commercial_general_liability_checked": true/false,
  "claims_made": true/false/null,
  "occurrence": true/false/null,
  "owners_contractors_protective": true/false,
  "general_aggregate": "...",
  "general_aggregate_applies_per": "POLICY|LOCATION|PROJECT|OTHER|null",
  "products_completed_operations_aggregate": "...",
  "personal_advertising_injury": "...",
  "each_occurrence": "...",
  "damage_to_rented_premises": "...",
  "medical_expense": "...",
  "employee_benefits": "...",
  "property_damage_deductible": "...",
  "property_damage_per_claim": true/false/null,
  "property_damage_per_occurrence": true/false/null,
  "bodily_injury_deductible": "...",
  "bodily_injury_per_claim": true/false/null,
  "bodily_injury_per_occurrence": true/false/null,
  "premises_operations_premium": "...",
  "products_premium": "...",
  "other_premium": "...",
  "total_premium": "...",
  "schedule_of_hazards": [
    {
      "loc_number": "...",
      "haz_number": "...",
      "class_code": "...",
      "premium_basis": "...",
      "exposure": "...",
      "terr": "...",
      "rate_prem_ops": "...",
      "rate_products": "...",
      "premium_prem_ops": "...",
      "premium_products": "...",
      "classification_description": "..."
    }
  ],
  "other_coverages_restrictions_endorsements": "...",
  "um_uim_coverage_is": true/false/null,
  "um_uim_coverage_is_not_available": true/false/null,
  "medical_payments_coverage_is": true/false/null,
  "medical_payments_coverage_is_not_available": true/false/null,
  "validation_notes": "..."
}

==================================================
EXTRACTION SOURCE 1: PDFPLUMBER (Table-aware) - PRIMARY SOURCE
==================================================
**PRIORITY**: Use the TABLE sections for Schedule of Hazards extraction.
The tables preserve structure and are more reliable than raw text.

**CRITICAL - BEFORE READING THE TEXT BELOW**:
The LIMITS section will contain text like this (these are REAL examples from the document):
- "GENERAL AGGREGATE $ 2,000,000"
- "PRODUCTS & COMPLETED OPERATIONS AGGREGATE $ 2,000,000"
- "EACH OCCURRENCE $ 1,000,000"
- "PERSONAL & ADVERTISING INJURY $ 1,000,000"
- "DAMAGE TO RENTED PREMISES (each occurrence) $ 100,000"
- "MEDICAL EXPENSE (Any one person) $ 5,000"

**YOU MUST FIND AND EXTRACT THESE AMOUNTS**. They appear in the LIMITS section. Search for the field name followed by "$" and a number with commas. Extract the exact value including the dollar sign and space.

""" + pdfplumber_text + """

==================================================
EXTRACTION SOURCE 2: PYMUPDF (Text layer) - CROSS-VALIDATION
==================================================
**USE AS**: Fallback/cross-reference when pdfplumber data is unclear or missing.

**CRITICAL - BEFORE READING THE TEXT BELOW**:
The LIMITS section will contain text like this (these are REAL examples from the document):
- "GENERAL AGGREGATE $ 2,000,000"
- "PRODUCTS & COMPLETED OPERATIONS AGGREGATE $ 2,000,000"
- "EACH OCCURRENCE $ 1,000,000"
- "PERSONAL & ADVERTISING INJURY $ 1,000,000"
- "DAMAGE TO RENTED PREMISES (each occurrence) $ 100,000"
- "MEDICAL EXPENSE (Any one person) $ 5,000"

**YOU MUST FIND AND EXTRACT THESE AMOUNTS**. They appear in the LIMITS section. Search for the field name followed by "$" and a number with commas. Extract the exact value including the dollar sign and space.

""" + pymupdf_text + """

**FINAL VERIFICATION BEFORE RETURNING JSON**:
1. Search the text above for "GENERAL AGGREGATE" - did you find "$ 2,000,000" or similar? Extract it.
2. Search for "PRODUCTS & COMPLETED OPERATIONS AGGREGATE" - did you find "$ 2,000,000" or similar? Extract it.
3. Search for "EACH OCCURRENCE" - did you find "$ 1,000,000" or similar? Extract it.
4. Search for "PERSONAL & ADVERTISING INJURY" - did you find "$ 1,000,000" or similar? Extract it.
5. Search for "DAMAGE TO RENTED PREMISES" - did you find "$ 100,000" or similar? Extract it.
6. Search for "MEDICAL EXPENSE" - did you find "$ 5,000" or similar? Extract it.

If you found any of these amounts in the text, you MUST include them in the JSON. Do NOT return null if the amount is visible in the text.

Return ONLY the JSON object now."""
        else:
            # Single extraction mode (ACORD Commercial General Liability Section)
            prompt = """You are an expert in ACORD Commercial General Liability Section form extraction.

**NOTE**: If you see TABLE sections in the text, prioritize those for Schedule of Hazards extraction as they preserve structure.

==================================================
CRITICAL: DATE CLARIFICATION
==================================================
- The top "DATE (MM/DD/YYYY)" is the FORM DATE (when the form was filled out). Do NOT use it as policy dates.
- Policy dates are in the "EFFECTIVE DATE" field (policy start date).

==================================================
FIELDS TO EXTRACT (ACORD Commercial General Liability Section)
==================================================
Return ONLY a valid JSON object with:
- agency_name, agency_address, agency_phone, agency_fax, agency_email
- carrier, policy_number, effective_date, applicant_first_named_insured, form_date
- commercial_general_liability_checked, claims_made, occurrence, owners_contractors_protective
- All limits fields (general_aggregate, products_completed_operations_aggregate, etc.)
- All deductibles fields
- Premiums fields (if present)
- schedule_of_hazards (array of hazard objects from the table)
- other_coverages_restrictions_endorsements
- Wisconsin-specific fields (if applicable)
- validation_notes

**CRITICAL - LIMITS AMOUNTS EXTRACTION**:
- The amounts ARE visible in the LIMITS section - extract them EXACTLY as they appear
- Look for patterns like "GENERAL AGGREGATE $ 2,000,000" or "EACH OCCURRENCE $ 1,000,000"
- Preserve the exact format: "$ 2,000,000" (with space after $ and commas in numbers)
- Do NOT skip amounts that are clearly visible - if you see "$ 2,000,000" or "$ 1,000,000" or "$ 100,000" or "$ 5,000" after a field name, extract it
- If a field shows "$ " (dollar sign with space but no number), set to null
- For each limits field, search for the field name followed by "$" and extract the amount exactly as shown

Follow the same JSON structure described in the dual-extraction instructions.

**CRITICAL - BEFORE READING THE TEXT BELOW**:
The LIMITS section will contain text like this (these are REAL examples from the document):
- "GENERAL AGGREGATE $ 2,000,000"
- "PRODUCTS & COMPLETED OPERATIONS AGGREGATE $ 2,000,000"
- "EACH OCCURRENCE $ 1,000,000"
- "PERSONAL & ADVERTISING INJURY $ 1,000,000"
- "DAMAGE TO RENTED PREMISES (each occurrence) $ 100,000"
- "MEDICAL EXPENSE (Any one person) $ 5,000"

**YOU MUST FIND AND EXTRACT THESE AMOUNTS**. They appear in the LIMITS section. Search for the field name followed by "$" and a number with commas. Extract the exact value including the dollar sign and space.

ACORD Form Extraction Text:
---
""" + pdfplumber_text + """
---

**FINAL VERIFICATION BEFORE RETURNING JSON**:
1. Search the text above for "GENERAL AGGREGATE" - did you find "$ 2,000,000" or similar? Extract it.
2. Search for "PRODUCTS & COMPLETED OPERATIONS AGGREGATE" - did you find "$ 2,000,000" or similar? Extract it.
3. Search for "EACH OCCURRENCE" - did you find "$ 1,000,000" or similar? Extract it.
4. Search for "PERSONAL & ADVERTISING INJURY" - did you find "$ 1,000,000" or similar? Extract it.
5. Search for "DAMAGE TO RENTED PREMISES" - did you find "$ 100,000" or similar? Extract it.
6. Search for "MEDICAL EXPENSE" - did you find "$ 5,000" or similar? Extract it.

If you found any of these amounts in the text, you MUST include them in the JSON. Do NOT return null if the amount is visible in the text.

Return ONLY the JSON object now."""
        
        return prompt
    
    def extract_fields(self, ocr_text: str, use_dual_validation: bool = True) -> Dict[str, Optional[str]]:
        """
        Extract fields from ACORD form text using LLM
        
        Args:
            ocr_text: The OCR extracted text (may be combo file with dual OCR)
            use_dual_validation: If True, parse and validate both OCR sources
            
        Returns:
            Dictionary with extracted fields
        """
        # Try to parse dual extraction if available
        pdfplumber_text, pymupdf_text = "", ""
        
        if use_dual_validation:
            pdfplumber_text, pymupdf_text = self.parse_dual_extraction(ocr_text)
            if pdfplumber_text and pymupdf_text:
                print("[OK] Detected dual extraction sources (pdfplumber + PyMuPDF) - using cross-validation")
            else:
                pdfplumber_text = ocr_text
                print("[INFO] Single extraction source detected")
        else:
            pdfplumber_text = ocr_text
        
        # Create prompt
        prompt = self.create_extraction_prompt(pdfplumber_text, pymupdf_text if pymupdf_text else None)
        
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {
                        "role": "system",
                        "content": "You are an expert insurance document analyzer. Return only valid JSON."
                    },
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                temperature=0.0,  # Deterministic output
                response_format={"type": "json_object"}
            )
            
            # Parse the response
            result_text = response.choices[0].message.content.strip()
            extracted_data = json.loads(result_text)
            
            return extracted_data
            
        except json.JSONDecodeError as e:
            print(f"[ERROR] Failed to parse LLM response as JSON: {e}")
            print(f"Response was: {result_text}")
            return {
                "policy_number": None,
                "effective_date": None,
                "applicant_first_named_insured": None,
                "error": "JSON parsing failed"
            }
        except Exception as e:
            print(f"[ERROR] Error calling LLM API: {e}")
            return {
                "policy_number": None,
                "effective_date": None,
                "applicant_first_named_insured": None,
                "error": str(e)
            }
    
    def extract_from_file(self, file_path: Path) -> Dict[str, Optional[str]]:
        """
        Extract fields from an ACORD form text file
        
        Args:
            file_path: Path to the OCR text file
            
        Returns:
            Dictionary with extracted fields
        """
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")
        
        # Read the OCR text
        with open(file_path, 'r', encoding='utf-8') as f:
            ocr_text = f.read()
        
        # Extract fields
        return self.extract_fields(ocr_text)


def main():
    """Main function to extract fields from ACORD Commercial General Liability Section form"""
    
    print("\n" + "="*80)
    print("ACORD COMMERCIAL GENERAL LIABILITY SECTION FIELD EXTRACTION (LLM-Based)")
    print("="*80)
    print()
    
    # Get input file
    if len(sys.argv) < 2:
        print("[WARNING] No input provided, using default: confianza_gla")
        base_name = "confianza_gla"
    else:
        base_name = sys.argv[1]
    
    # Carrier directory (change this to switch between usgnonop, standardop, etc.)
    carrier_dir = "standardop"
    
    # Look for the combo file (best extraction)
    # NOTE: for GLA we typically use base names like "arrr_gla" so the file becomes "arrr_gla_combo.txt"
    input_file = Path(f"{carrier_dir}/{base_name}_combo.txt")
    
    if not input_file.exists():
        # Try alternatives
        alternatives = [
            Path(f"{carrier_dir}/{base_name}1.txt"),  # pdfplumber
            Path(f"{carrier_dir}/{base_name}2.txt"),  # PyMuPDF
        ]
        for alt in alternatives:
            if alt.exists():
                input_file = alt
                break
    
    if not input_file.exists():
        print(f"[ERROR] No OCR file found for: {base_name}")
        print("   Please run cert_extract_gla.py first")
        return
    
    print(f"[FILE] Input file: {input_file}")
    print(f"   Size: {input_file.stat().st_size:,} bytes")
    
    # Check if it's a combo file (dual extraction)
    is_combo = "_combo.txt" in str(input_file)
    if is_combo:
        print(f"   Type: Dual extraction (pdfplumber + PyMuPDF)")
    else:
        print(f"   Type: Single extraction")
    print()
    
    # Initialize extractor
    try:
        extractor = ACORDGLAExtractor()
        print(f"[OK] LLM initialized: {extractor.model}\n")
    except ValueError as e:
        print(f"[ERROR] {e}")
        print("   Please add OPENAI_API_KEY to your .env file")
        return
    
    # Extract fields
    print("[EXTRACTING] Extracting fields with LLM cross-validation...\n")
    result = extractor.extract_from_file(input_file)
    
    # Display results
    print("\n" + "="*80)
    print("EXTRACTED FIELDS")
    print("="*80)
    print()
    print(json.dumps(result, indent=2))
    print()
    
    # Save results
    output_file = Path(f"{carrier_dir}/{base_name}_extracted.json")
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(result, f, indent=2)
    
    print(f"[SAVED] Results saved to: {output_file}")
    print("="*80)


if __name__ == "__main__":
    main()

