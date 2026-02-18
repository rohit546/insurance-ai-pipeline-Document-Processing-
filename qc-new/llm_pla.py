"""
LLM-Based Property Liability Application (PLA) Field Extraction
Extracts key fields from ACORD 125/140 insurance application forms using GPT-4o-mini
"""

import os
import json
import sys
from pathlib import Path
from typing import Dict, Optional, List
from dotenv import load_dotenv
from openai import OpenAI

# Load environment variables
load_dotenv()


class ApplicationExtractor:
    """Extract fields from insurance application forms using LLM"""
    
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
    
    def parse_dual_ocr(self, combo_text: str) -> tuple[str, str]:
        """
        Parse combo file to extract PDFPLUMBER/TESSERACT and PyMuPDF sections separately
        Handles multi-page documents by combining all pages
        
        Args:
            combo_text: Combined OCR text with both methods
            
        Returns:
            Tuple of (primary_text, pymupdf_text)
        """
        primary_text = ""
        pymupdf_text = ""
        
        # Collect all primary OCR (PDFPLUMBER or TESSERACT) and PYMUPDF sections
        primary_sections = []
        pymupdf_sections = []
        
        # Check for PDFPLUMBER format
        if "--- PDFPLUMBER" in combo_text:
            # Split by PDFPLUMBER markers to get all sections
            pdfplumber_parts = combo_text.split("--- PDFPLUMBER")
            for part in pdfplumber_parts[1:]:  # Skip first empty part
                if "--- PYMUPDF" in part:
                    primary_sections.append(part.split("--- PYMUPDF")[0].strip())
                    pymupdf_part = part.split("--- PYMUPDF")[1]
                    # Remove the header line "--- PYMUPDF (Text layer) ---" or similar
                    if "---" in pymupdf_part:
                        pymupdf_clean = pymupdf_part.split("---", 1)[1].strip()
                    else:
                        pymupdf_clean = pymupdf_part.strip()
                    # Remove page separators
                    pymupdf_clean = pymupdf_clean.split("PAGE")[0].strip() if "PAGE" in pymupdf_clean else pymupdf_clean
                    pymupdf_sections.append(pymupdf_clean)
                else:
                    primary_sections.append(part.strip())
        
        # Check for TESSERACT format (some files use this, like arrr_pla)
        elif "--- TESSERACT" in combo_text:
            # Split by TESSERACT markers to get all sections
            tesseract_parts = combo_text.split("--- TESSERACT")
            for i, part in enumerate(tesseract_parts[1:], 1):  # Skip first empty part
                if "--- PYMUPDF" in part:
                    # Get TESSERACT text (everything before PYMUPDF)
                    tesseract_text = part.split("--- PYMUPDF")[0].strip()
                    primary_sections.append(tesseract_text)
                    
                    # Get PYMUPDF text (everything after PYMUPDF marker)
                    pymupdf_part = part.split("--- PYMUPDF")[1]
                    # Remove the header line "--- PYMUPDF (Buffer=0) ---" or similar
                    if "---" in pymupdf_part:
                        pymupdf_clean = pymupdf_part.split("---", 1)[1].strip()
                    else:
                        pymupdf_clean = pymupdf_part.strip()
                    
                    # Stop at next TESSERACT marker (next page) or end of file
                    if i < len(tesseract_parts) - 1:
                        # There's another TESSERACT section, so stop before it
                        # But we already split by TESSERACT, so this part should be complete
                        pass
                    # Remove any trailing page separators
                    if "================================================================================\nPAGE" in pymupdf_clean:
                        pymupdf_clean = pymupdf_clean.split("================================================================================\nPAGE")[0].strip()
                    
                    pymupdf_sections.append(pymupdf_clean)
                else:
                    primary_sections.append(part.strip())
        
        # Combine all pages with clear separators
        if primary_sections:
            primary_text = "\n\n".join([s for s in primary_sections if s.strip()])
        if pymupdf_sections:
            pymupdf_text = "\n\n".join([s for s in pymupdf_sections if s.strip()])
        
        # If parsing failed, return the whole text as single source
        if not primary_text and not pymupdf_text:
            primary_text = combo_text
        
        return primary_text, pymupdf_text
    
    def create_extraction_prompt(self, pdfplumber_text: str, pymupdf_text: str = None) -> str:
        """
        Create the extraction prompt for the LLM with dual OCR validation
        
        Args:
            pdfplumber_text: OCR text from PDFPLUMBER method (table-aware)
            pymupdf_text: OCR text from PyMuPDF method (optional)
            
        Returns:
            Formatted prompt string
        """
        if pymupdf_text:
            # Dual OCR mode - cross-validation
            prompt = """You are an expert in business insurance and ACORD (Association for Cooperative Operations Research and Development) application forms. ACORD forms have a STANDARDIZED, FIXED FORMAT that never changes.

**IMPORTANT: You have TWO independent OCR extractions of the SAME document below.**

**ACORD Application Form Structure (ALWAYS THE SAME):**
ACORD 125/140 application forms follow a fixed layout structure:
- **Page 1+**: PROPERTY SECTION (ACORD 140) - Contains policy info, premises, coverages, building details
- **Page 2+**: APPLICANT INFORMATION SECTION (ACORD 125) - Contains applicant details, mailing address, contact info
- **Multiple Pages**: Can be 5-9 pages depending on number of premises and additional information

**Standard Field Locations (NEVER CHANGE):**

**PROPERTY SECTION (ACORD 140):**
1. **POLICY NUMBER** - Found in header row, labeled "POLICY NUMBER" (may be empty if new application)
2. **EFFECTIVE DATE** or **PROPOSED EFF DATE** - Found in header row or POLICY INFORMATION section, format: MM/DD/YYYY
3. **EXPIRATION DATE** or **PROPOSED EXP DATE** - Found in header row or POLICY INFORMATION section, format: MM/DD/YYYY
4. **NAMED INSURED(S)** - Found in PROPERTY SECTION header, labeled "NAMED INSURED(S)"
5. **PREMISES INFORMATION** - Multiple premises can exist (PREMISES #: 1, 2, etc.)
   - Each premise has: STREET ADDRESS, BUILDING #, BLDG DESCRIPTION
   - Location addresses are in "PREMISES INFORMATION" section
6. **SUBJECT OF INSURANCE** - Table with coverages and amounts (Building, Business Income, Equipment Breakdown, etc.)
7. **ADDITIONAL INTEREST** - Found in PROPERTY SECTION or separate ACORD 45 form
   - Can have 0, 1, 2, or more entries
   - **CRITICAL:** Generic placeholders like "TO WHOM IT MAY CONCERN" are NOT valid additional interests (count as 0)

**APPLICANT INFORMATION SECTION (ACORD 125):**
1. **NAME (First Named Insured)** - Found in APPLICANT INFORMATION section
   - This is the primary applicant name
   - May be same as NAMED INSURED(S) in PROPERTY SECTION or different
2. **MAILING ADDRESS** - Found immediately below "NAME (First Named Insured)" 
   - Full address format: "Street, City, State ZIP"
   - Combine all address lines into a single string with commas
3. **CONTACT INFORMATION** - Contact name, phone, email (if present)

**IMPORTANT DATE CLARIFICATION:**
- The **application date** appears at the very top (e.g., "DATE (MM/DD/YYYY) 11/24/2025") - DO NOT use this
- The **EFFECTIVE DATE** or **PROPOSED EFF DATE** is the policy start date - look in PROPERTY SECTION header or POLICY INFORMATION section
- The **EXPIRATION DATE** or **PROPOSED EXP DATE** comes after the effective date
- DO NOT confuse the application date with the policy effective date

**MULTIPLE PREMISES:**
- PLA forms can have multiple premises (1, 2, 3, or more)
- Each premise has its own location address
- Extract ALL premises and their addresses
- Format as array: [{"premise_number": "1", "address": "..."}, {"premise_number": "2", "address": "..."}]

Your task is to:
1. **Compare both OCR outputs** to identify discrepancies
2. **Cross-validate** the information between both sources
3. **Choose the most accurate value** when there are conflicts
4. **Handle OCR errors** intelligently using context from both sources
5. **Fill gaps** where one OCR captured data the other missed
6. **Use the standard ACORD form structure** - fields are always in the same relative positions
7. **Extract ALL premises** - don't miss any location addresses

**Fields to Extract:**
1. **Policy Number** - Found in PROPERTY SECTION header, labeled "POLICY NUMBER" (may be null if new application)
2. **Effective Date** - Found as "EFFECTIVE DATE" or "PROPOSED EFF DATE", format: MM/DD/YYYY
3. **Expiration Date** - Found as "EXPIRATION DATE" or "PROPOSED EXP DATE", format: MM/DD/YYYY
4. **Insured Name** - Found in PROPERTY SECTION as "NAMED INSURED(S)" (primary source)
   - If not found, use "NAME (First Named Insured)" from APPLICANT INFORMATION section
   - Examples: "140 N Salem LLC & Waheguru Investments, LLC", "SHELBY INVESTMENT INC DBA SHELBY EXPRESS"
   - May contain multiple names separated by "&" or semicolons
   - Extract exactly as shown (name only, not address)
5. **Mailing Address** - Found in APPLICANT INFORMATION section, below "NAME (First Named Insured)"
   - This is the mailing address where the applicant receives mail
   - Full address format: "Street, City, State ZIP"
   - Example: "5553 LEGENDS DR, BRASELTON, GA 30517-4014"
   - Combine all address lines into a single string with commas
6. **Location Addresses** - Found in "PREMISES INFORMATION" section (can be multiple)
   - These are the physical locations of the insured properties
   - Each premise has: PREMISES #, STREET ADDRESS, CITY, STATE, ZIP
   - Format as array: [{"premise_number": "1", "address": "Street, City, State ZIP"}, ...]
   - Example: [{"premise_number": "1", "address": "87 Hurricane Shoals Road Northeast, LAWRENCEVILLE, GA 30046"}, {"premise_number": "2", "address": "579 LYLE CIR, LAWRENCEVILLE, GA 30046-4563"}]
   - If only one premise, still use array format with single element
7. **Additional Interest(s)** - Found in "ADDITIONAL INTEREST" section (PROPERTY SECTION) or ACORD 45 form
   - Parties with an insurable interest (mortgagee, lender, property owner, etc.)
   - **IMPORTANT - CONDITIONAL FORMATTING BASED ON COUNT:**
     - **If 0 additional interests found** → Do NOT include any additional interest fields (no null, no empty string, DO NOT ADD THE KEYS AT ALL)
     - **If EXACTLY 1 additional interest found** → Use flat structure:
       - "additional_interest_name": "name only"
       - "additional_interest_address": "full address"
     - **If 2 OR MORE additional interests found** → Use array structure:
       - "additional_interests": [{"name": "...", "address": "..."}, {...}]
   - **CRITICAL:** "TO WHOM IT MAY CONCERN" is NOT a valid additional interest → count as 0 → OMIT FIELDS ENTIRELY
   - Extract all legitimate additional interests (can be 0, 1, 2, 3, or more)
8. **Coverages** - Found in "SUBJECT OF INSURANCE" table in PROPERTY SECTION
   - This is a table with coverage items and their insurance amounts
   - **CRITICAL - MULTIPLE PREMISES HANDLING:**
     - PLA forms have MULTIPLE premises (1, 2, 3, 4, or more)
     - Each premise has its own "SUBJECT OF INSURANCE" table with coverages
     - You MUST extract coverages from ALL premises, not just one
     - Look for "PREMISES #: 1", "PREMISES #: 2", etc. followed by "SUBJECT OF INSURANCE"
   - **CRITICAL - EXTRACT ALL COVERAGES:**
     - **YOU MUST EXTRACT EVERY SINGLE COVERAGE** that appears in ANY "SUBJECT OF INSURANCE" table
     - Do NOT skip any coverage - if it's in the table, it MUST be in your output
     - Common coverage types include: "Building", "Pumps", "Canopy", "Business Personal Property", "Business Income", "Equipment Breakdown", "Outdoor Signs", "Signs", "Wind and Hail", "Theft", "Money and Securities", "Employee Dishonesty", "Spoilage"
     - **DO NOT MISS:** Building, Pumps, Canopy, Business Personal Property, Business Income, Equipment Breakdown, Employee Dishonesty, Money and Securities, Spoilage, Outdoor Signs, Wind and Hail, Theft - extract ALL of them if they appear
     - **IMPORTANT - COVERAGE FORMAT:**
       - In OCR text, coverage names and amounts may appear on SEPARATE LINES
       - Example format you might see:
         ```
         Building
         2,000,000
         Business Income
         100,000
         Equipment Breakdown
         Included
         Wind and Hail
         Included
         Theft
         Included
         ```
       - You MUST look for coverage names followed by their amounts on the next line(s)
       - Amount can be: numeric (e.g., "2,000,000", "100,000"), "Included", or "Included Ded $X"
     - **CRITICAL EXAMPLES:**
       - If you see "Building" followed by "2,000,000" on next line → MUST extract "Building": "2,000,000"
       - If you see "Theft" followed by "Included" on next line → MUST extract "Theft": "Included"
       - If you see "Wind and Hail" followed by "Included" on next line → MUST extract "Wind and Hail": "Included"
       - If you see "Equipment Breakdown" followed by "Included" on next line → MUST extract "Equipment Breakdown": "Included"
       - If you see "Business Income" followed by "100,000" on next line → MUST extract "Business Income": "100,000"
       - Missing ANY of these is an error
   - **IMPORTANT - DYNAMIC EXTRACTION:**
     - Extract ONLY the coverages that are present in the application
     - Do NOT include coverages that are not listed
     - Each application may have different coverages (variable, not fixed)
     - **BUT:** If a coverage appears in the table, you MUST include it - no exceptions
   - **Format as a JSON object** with coverage name as key and amount as value
   - Amount values can be:
     - Numeric (e.g., "350,000", "100,000", "5,000", "150,000", "300,000", "75,000", "10,000")
     - "Included" (coverage is included without specific limit)
     - "Included Ded $2,500" or similar variations (extract exactly as shown)
     - "Actual Loss Sustained" (for business income)
   - Extract the amount exactly as shown (including commas in numbers and any additional text like "Ded $2,500")
   - **NOTE:** If same coverage appears for multiple premises with different amounts:
     - For "Building": Use the HIGHEST value (e.g., if Premise 1 has 150,000 and Premise 2 has 300,000, use 300,000)
     - For other coverages: Use the value that appears most frequently, or the highest if amounts differ
     - For "Included" coverages: Keep as "Included" (or "Included Ded $X" if specified)
   - **SEARCH STRATEGY:** 
     - Look through ALL pages for "SUBJECT OF INSURANCE" sections - they appear multiple times (once per premise)
     - Also check for "ADDITIONAL PREMISES INFORMATION" sections which may have additional coverages
     - Scan the entire document - coverages can appear on different pages
   - **EXAMPLE:** If you see:
     - Premise 1: Building 150,000, Business Income 60,000
     - Premise 2: Building 300,000, Business Income 60,000
     - Premise 3: Building 300,000, Business Income 60,000
     - Then extract: {"Building": "300,000", "Business Income": "60,000", "Equipment Breakdown": "Included", "Wind and Hail coverage": "Included"}
9. **Building Details** (Optional but useful):
   - **Construction Type** - Found in PROPERTY SECTION (e.g., "Frame", "Joisted Masonry")
   - **Year Built** - Found as "YR BUILT" (e.g., "1973", "1980")
   - **Total Area** - Found as "TOTAL AREA" (e.g., "2,760 Sqft")
   - Extract these if available, but they're optional

**Validation Rules:**
- If both sources agree → high confidence, use that value
- If sources disagree → use the one that makes more logical sense
- If one source has formatting issues → prefer the cleaner formatted value
- Dates must be in MM/DD/YYYY format
- Policy numbers should be alphanumeric (may have periods, hyphens, or be empty for new applications)
- Always extract ALL premises - don't miss any location addresses
- **CRITICAL:** Coverages appear in MULTIPLE "SUBJECT OF INSURANCE" tables (one per premise) - you MUST search through ALL pages and extract from ALL premises
- **CRITICAL:** Extract EVERY SINGLE coverage that appears in ANY "SUBJECT OF INSURANCE" table - do NOT skip Building, Pumps, Canopy, Business Personal Property, Business Income, Equipment Breakdown, Employee Dishonesty, Money and Securities, Spoilage, Outdoor Signs, Wind and Hail, Theft, or any other coverage
- If you find "Building 150,000" for Premise 1 and "Building 300,000" for Premise 2, extract "Building": "300,000" (highest value)
- **EXAMPLE:** If you see "Building 2,000,000", "Business Income 100,000", "Equipment Breakdown Included", "Wind and Hail Included", "Theft Included" in the table, you MUST include ALL of them: {"Building": "2,000,000", "Business Income": "100,000", "Equipment Breakdown": "Included", "Wind and Hail": "Included", "Theft": "Included"} - missing even one is an error

**Output Format:**
Return ONLY a valid JSON object with CONDITIONAL structure for additional interests:

**If 0 additional interests (e.g., "TO WHOM IT MAY CONCERN" or no entries):**
{
    "policy_number": "string or null",
    "effective_date": "MM/DD/YYYY or null",
    "expiration_date": "MM/DD/YYYY or null",
    "insured_name": "string or null",
    "mailing_address": "string or null",
    "location_addresses": [
        {"premise_number": "1", "address": "Street, City, State ZIP"}
    ],
    "coverages": {
        "Pumps": "75,000",
        "Canopy": "75,000",
        "Business Personal Property": "100,000",
        "Business Income": "100,000",
        "Equipment Breakdown": "Included",
        "Employee Dishonesty": "10,000",
        "Money and Securities": "10,000",
        "Spoilage": "10,000",
        "Outdoor Signs": "10,000",
        "Wind and Hail": "Included Ded $2,500"
    },
    "construction_type": "Frame or null",
    "year_built": "1973 or null",
    "total_area": "2,760 Sqft or null",
    "validation_notes": "..."
}
NOTE: NO additional_interest fields at all - the keys should NOT exist in the JSON
NOTE: coverages object should ONLY contain items that are present in the application
NOTE: location_addresses is ALWAYS an array (even if only one premise)

**If EXACTLY 1 additional interest:**
{
    "policy_number": "string or null",
    "effective_date": "MM/DD/YYYY or null",
    "expiration_date": "MM/DD/YYYY or null",
    "insured_name": "string or null",
    "mailing_address": "string or null",
    "location_addresses": [
        {"premise_number": "1", "address": "Street, City, State ZIP"},
        {"premise_number": "2", "address": "Street, City, State ZIP"}
    ],
    "coverages": {
        "Pumps": "75,000",
        "Canopy": "75,000",
        "Business Personal Property": "100,000",
        "Business Income": "100,000",
        "Equipment Breakdown": "Included"
    },
    "construction_type": "Frame or null",
    "year_built": "1980 or null",
    "total_area": "2,760 Sqft or null",
    "additional_interest_name": "string",
    "additional_interest_address": "string",
    "validation_notes": "..."
}

**If 2 OR MORE additional interests:**
{
    "policy_number": "string or null",
    "effective_date": "MM/DD/YYYY or null",
    "expiration_date": "MM/DD/YYYY or null",
    "insured_name": "string or null",
    "mailing_address": "string or null",
    "location_addresses": [
        {"premise_number": "1", "address": "Street, City, State ZIP"}
    ],
    "coverages": {
        "Pumps": "75,000",
        "Canopy": "75,000",
        "Business Personal Property": "100,000",
        "Business Income": "100,000",
        "Equipment Breakdown": "Included",
        "Outdoor Signs": "10,000"
    },
    "construction_type": "Frame or null",
    "year_built": "1973 or null",
    "total_area": "2,760 Sqft or null",
    "additional_interests": [
        {"name": "string", "address": "string"},
        {"name": "string", "address": "string"}
    ],
    "validation_notes": "..."
}

**OCR Source 1 (PDFPLUMBER/TESSERACT - Table-aware):**
---
""" + pdfplumber_text + """
---

**OCR Source 2 (PyMuPDF - Text layer):**
---
""" + pymupdf_text + """
---

**CRITICAL REMINDER:**
- Look for "SUBJECT OF INSURANCE" table in the text above
- Extract EVERY coverage you find: Building, Business Income, Equipment Breakdown, Wind and Hail, Theft, Pumps, Canopy, etc.
- Coverage names and amounts may be on separate lines - look carefully
- If you see "Building" followed by "2,000,000" → extract it
- If you see "Theft" followed by "Included" → extract it
- DO NOT skip any coverage that appears in the table

Now analyze both sources, cross-validate, and return the JSON object:"""
        else:
            # Single OCR mode
            prompt = """You are an expert in business insurance and ACORD (Association for Cooperative Operations Research and Development) application forms. ACORD forms have a STANDARDIZED, FIXED FORMAT that never changes.

**ACORD Application Form Structure (ALWAYS THE SAME):**
ACORD 125/140 application forms follow a fixed layout structure:
- **Page 1+**: PROPERTY SECTION (ACORD 140) - Contains policy info, premises, coverages, building details
- **Page 2+**: APPLICANT INFORMATION SECTION (ACORD 125) - Contains applicant details, mailing address, contact info
- **Multiple Pages**: Can be 5-9 pages depending on number of premises and additional information

**Standard Field Locations (NEVER CHANGE):**

**PROPERTY SECTION (ACORD 140):**
1. **POLICY NUMBER** - Found in header row, labeled "POLICY NUMBER" (may be empty if new application)
2. **EFFECTIVE DATE** or **PROPOSED EFF DATE** - Found in header row or POLICY INFORMATION section
3. **EXPIRATION DATE** or **PROPOSED EXP DATE** - Found in header row or POLICY INFORMATION section
4. **NAMED INSURED(S)** - Found in PROPERTY SECTION header
5. **PREMISES INFORMATION** - Multiple premises can exist (PREMISES #: 1, 2, etc.)
6. **SUBJECT OF INSURANCE** - Table with coverages and amounts
7. **ADDITIONAL INTEREST** - Found in PROPERTY SECTION or separate ACORD 45 form

**APPLICANT INFORMATION SECTION (ACORD 125):**
1. **NAME (First Named Insured)** - Found in APPLICANT INFORMATION section
2. **MAILING ADDRESS** - Found immediately below "NAME (First Named Insured)"
3. **CONTACT INFORMATION** - Contact name, phone, email (if present)

**IMPORTANT DATE CLARIFICATION:**
- The **application date** appears at the very top - DO NOT use this
- The **EFFECTIVE DATE** or **PROPOSED EFF DATE** is the policy start date
- The **EXPIRATION DATE** or **PROPOSED EXP DATE** comes after the effective date

**MULTIPLE PREMISES:**
- PLA forms can have multiple premises (1, 2, 3, or more)
- Extract ALL premises and their addresses
- Format as array: [{"premise_number": "1", "address": "..."}, ...]

**Your Task:**
Extract the following fields using the standard ACORD form structure:
1. **Policy Number** - Found in PROPERTY SECTION header (may be null if new application)
2. **Effective Date** - Found as "EFFECTIVE DATE" or "PROPOSED EFF DATE", format: MM/DD/YYYY
3. **Expiration Date** - Found as "EXPIRATION DATE" or "PROPOSED EXP DATE", format: MM/DD/YYYY
4. **Insured Name** - Found in PROPERTY SECTION as "NAMED INSURED(S)" (primary source)
   - If not found, use "NAME (First Named Insured)" from APPLICANT INFORMATION section
   - Extract exactly as shown (name only, not address)
5. **Mailing Address** - Found in APPLICANT INFORMATION section, below "NAME (First Named Insured)"
   - Full address format: "Street, City, State ZIP"
   - Combine all address lines into a single string with commas
6. **Location Addresses** - Found in "PREMISES INFORMATION" section (can be multiple)
   - Format as array: [{"premise_number": "1", "address": "Street, City, State ZIP"}, ...]
   - Always use array format even if only one premise
7. **Additional Interest(s)** - Found in "ADDITIONAL INTEREST" section
   - **IMPORTANT - CONDITIONAL FORMATTING BASED ON COUNT:**
     - **If 0 additional interests found** → Do NOT include any additional interest fields
     - **If EXACTLY 1 additional interest found** → Use flat structure:
       - "additional_interest_name": "name only"
       - "additional_interest_address": "full address"
     - **If 2 OR MORE additional interests found** → Use array structure:
       - "additional_interests": [{"name": "...", "address": "..."}, {...}]
   - **CRITICAL:** "TO WHOM IT MAY CONCERN" is NOT a valid additional interest → count as 0
8. **Coverages** - Found in "SUBJECT OF INSURANCE" table
   - **CRITICAL - MULTIPLE PREMISES HANDLING:**
     - PLA forms have MULTIPLE premises (1, 2, 3, 4, or more)
     - Each premise has its own "SUBJECT OF INSURANCE" table with coverages
     - You MUST extract coverages from ALL premises, not just one
     - Look for "PREMISES #: 1", "PREMISES #: 2", etc. followed by "SUBJECT OF INSURANCE"
     - **CRITICAL - EXTRACT ALL COVERAGES:**
     - **YOU MUST EXTRACT EVERY SINGLE COVERAGE** that appears in ANY "SUBJECT OF INSURANCE" table
     - Do NOT skip any coverage - if it's in the table, it MUST be in your output
     - Common coverage types: "Building", "Pumps", "Canopy", "Business Personal Property", "Business Income", "Equipment Breakdown", "Outdoor Signs", "Signs", "Wind and Hail", "Theft", "Money and Securities", "Employee Dishonesty", "Spoilage"
     - **DO NOT MISS:** Building, Pumps, Canopy, Business Personal Property, Business Income, Equipment Breakdown, Employee Dishonesty, Money and Securities, Spoilage, Outdoor Signs, Wind and Hail, Theft - extract ALL of them if they appear
     - **IMPORTANT - COVERAGE FORMAT:**
       - In OCR text, coverage names and amounts may appear on SEPARATE LINES
       - Example format you might see:
         ```
         Building
         2,000,000
         Business Income
         100,000
         Equipment Breakdown
         Included
         Wind and Hail
         Included
         Theft
         Included
         ```
       - You MUST look for coverage names followed by their amounts on the next line(s)
       - Amount can be: numeric (e.g., "2,000,000", "100,000"), "Included", or "Included Ded $X"
     - **CRITICAL EXAMPLES:**
       - If you see "Building" followed by "2,000,000" on next line → MUST extract "Building": "2,000,000"
       - If you see "Theft" followed by "Included" on next line → MUST extract "Theft": "Included"
       - If you see "Wind and Hail" followed by "Included" on next line → MUST extract "Wind and Hail": "Included"
       - If you see "Equipment Breakdown" followed by "Included" on next line → MUST extract "Equipment Breakdown": "Included"
       - If you see "Business Income" followed by "100,000" on next line → MUST extract "Business Income": "100,000"
       - Missing ANY of these is an error
   - Extract ONLY coverages that are present (but extract ALL that are present - don't miss any)
   - Format as JSON object: {"Coverage Name": "Amount"}
   - Amount values: numeric with commas (e.g., "350,000", "150,000", "300,000", "75,000", "10,000"), "Included", "Included Ded $2,500", or "Actual Loss Sustained"
   - Extract amounts exactly as shown (including any additional text like "Ded $2,500")
   - **NOTE:** If same coverage appears for multiple premises with different amounts, use the HIGHEST value
   - **SEARCH STRATEGY:** 
     - Look through ALL pages for "SUBJECT OF INSURANCE" sections - they appear multiple times (once per premise)
     - Also check for "ADDITIONAL PREMISES INFORMATION" sections which may have additional coverages
     - Scan the entire document - coverages can appear on different pages
9. **Building Details** (Optional):
   - "construction_type": "Frame or null"
   - "year_built": "1973 or null"
   - "total_area": "2,760 Sqft or null"

**Output Format:**
Return ONLY a valid JSON object with CONDITIONAL structure for additional interests:

**If 0 additional interests:**
{
    "policy_number": "string or null",
    "effective_date": "MM/DD/YYYY or null",
    "expiration_date": "MM/DD/YYYY or null",
    "insured_name": "string or null",
    "mailing_address": "string or null",
    "location_addresses": [
        {"premise_number": "1", "address": "Street, City, State ZIP"}
    ],
     "coverages": {
         "Pumps": "75,000",
         "Canopy": "75,000",
         "Business Personal Property": "100,000",
         "Business Income": "100,000",
         "Equipment Breakdown": "Included"
     },
    "construction_type": "Frame or null",
    "year_built": "1973 or null",
    "total_area": "2,760 Sqft or null"
}
NOTE: NO additional_interest fields at all - the keys should NOT exist in the JSON
NOTE: location_addresses is ALWAYS an array

**If EXACTLY 1 additional interest:**
{
    "policy_number": "string or null",
    "effective_date": "MM/DD/YYYY or null",
    "expiration_date": "MM/DD/YYYY or null",
    "insured_name": "string or null",
    "mailing_address": "string or null",
    "location_addresses": [
        {"premise_number": "1", "address": "Street, City, State ZIP"}
    ],
    "coverages": {
        "Pumps": "75,000",
        "Canopy": "75,000",
        "Business Personal Property": "100,000",
        "Business Income": "100,000",
        "Equipment Breakdown": "Included"
    },
    "construction_type": "Frame or null",
    "year_built": "1973 or null",
    "total_area": "2,760 Sqft or null",
    "additional_interest_name": "string",
    "additional_interest_address": "string"
}

**If 2 OR MORE additional interests:**
{
    "policy_number": "string or null",
    "effective_date": "MM/DD/YYYY or null",
    "expiration_date": "MM/DD/YYYY or null",
    "insured_name": "string or null",
    "mailing_address": "string or null",
    "location_addresses": [
        {"premise_number": "1", "address": "Street, City, State ZIP"}
    ],
    "coverages": {
        "Pumps": "75,000",
        "Canopy": "75,000",
        "Business Personal Property": "100,000",
        "Business Income": "100,000",
        "Equipment Breakdown": "Included",
        "Outdoor Signs": "10,000"
    },
    "construction_type": "Frame or null",
    "year_built": "1973 or null",
    "total_area": "2,760 Sqft or null",
    "additional_interests": [
        {"name": "string", "address": "string"},
        {"name": "string", "address": "string"}
    ]
}

**Important Guidelines:**
- Use MM/DD/YYYY format for dates
- Return ONLY the JSON object, no additional text or explanation
- If a field is not found or unclear, use null
- Be precise and extract exactly what appears on the application
- Handle OCR errors intelligently based on context
- Always extract ALL premises - location_addresses is always an array

**Application OCR Text:**
---
""" + pdfplumber_text + """
---

**CRITICAL REMINDER:**
- Look for "SUBJECT OF INSURANCE" table in the text above
- Extract EVERY coverage you find: Building, Business Income, Equipment Breakdown, Wind and Hail, Theft, Pumps, Canopy, etc.
- Coverage names and amounts may be on separate lines - look carefully
- If you see "Building" followed by "2,000,000" → extract it
- If you see "Theft" followed by "Included" → extract it
- DO NOT skip any coverage that appears in the table

Return the JSON object now:"""
        
        return prompt
    
    def extract_fields(self, ocr_text: str, use_dual_validation: bool = True) -> Dict[str, Optional[str]]:
        """
        Extract fields from application text using LLM
        
        Args:
            ocr_text: The OCR extracted text (may be combo file with dual OCR)
            use_dual_validation: If True, parse and validate both OCR sources
            
        Returns:
            Dictionary with extracted fields
        """
        # Try to parse dual OCR if available
        pdfplumber_text, pymupdf_text = "", ""
        
        if use_dual_validation:
            pdfplumber_text, pymupdf_text = self.parse_dual_ocr(ocr_text)
            if pdfplumber_text and pymupdf_text:
                print("✅ Detected dual OCR sources - using cross-validation")
            else:
                pdfplumber_text = ocr_text
                print("ℹ️  Single OCR source detected")
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
            print(f"❌ Failed to parse LLM response as JSON: {e}")
            print(f"Response was: {result_text}")
            return {
                "policy_number": None,
                "effective_date": None,
                "expiration_date": None,
                "insured_name": None,
                "mailing_address": None,
                "location_addresses": [],
                "coverages": {},
                "error": "JSON parsing failed"
            }
        except Exception as e:
            print(f"❌ Error calling LLM API: {e}")
            return {
                "policy_number": None,
                "effective_date": None,
                "expiration_date": None,
                "insured_name": None,
                "mailing_address": None,
                "location_addresses": [],
                "coverages": {},
                "error": str(e)
            }
    
    def extract_from_file(self, file_path: Path) -> Dict[str, Optional[str]]:
        """
        Extract fields from an application text file
        
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
    """Main function to extract fields from application"""
    
    print("\n" + "="*80)
    print("APPLICATION FIELD EXTRACTION (LLM-Based) - PLA")
    print("="*80)
    print()
    
    # Get input file
    if len(sys.argv) < 2:
        print("⚠️  No input provided, using default: salem_pla")
        base_name = "arrr_pla"
    else:
        base_name = sys.argv[1]
    
    # Carrier directory (change this to switch between encovaop, usgnonop, etc.)
    carrier_dir = "usgnonop"
    
    # Look for the combo file (best extraction)
    input_file = Path(f"{carrier_dir}/{base_name}_combo.txt")
    
    if not input_file.exists():
        # Try alternatives
        alternatives = [
            Path(f"{carrier_dir}/{base_name}2.txt"),  # PyMuPDF
            Path(f"{carrier_dir}/{base_name}1.txt"),  # PDFPLUMBER
            Path(f"{carrier_dir}/{base_name}.txt"),   # Single extraction
        ]
        for alt in alternatives:
            if alt.exists():
                input_file = alt
                break
    
    if not input_file.exists():
        print(f"❌ No OCR file found for: {base_name}")
        print("   Please run cert_extract_pla.py first")
        return
    
    print(f"📄 Input file: {input_file}")
    print(f"   Size: {input_file.stat().st_size:,} bytes")
    
    # Check if it's a combo file (dual OCR)
    is_combo = "_combo.txt" in str(input_file)
    if is_combo:
        print(f"   Type: Dual OCR (PDFPLUMBER + PyMuPDF)")
    else:
        print(f"   Type: Single OCR")
    print()
    
    # Initialize extractor
    try:
        extractor = ApplicationExtractor()
        print(f"✅ LLM initialized: {extractor.model}\n")
    except ValueError as e:
        print(f"❌ {e}")
        print("   Please add OPENAI_API_KEY to your .env file")
        return
    
    # Extract fields
    print("🔍 Extracting fields with LLM cross-validation...\n")
    result = extractor.extract_from_file(input_file)
    
    # Display results
    print("\n" + "="*80)
    print("EXTRACTED FIELDS")
    print("="*80)
    print()
    print(json.dumps(result, indent=2))
    print()
    
    # Save results
    output_file = Path(f"{carrier_dir}/{base_name}_extracted_real.json")
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(result, f, indent=2)
    
    print(f"💾 Results saved to: {output_file}")
    print("="*80)


if __name__ == "__main__":
    main()

