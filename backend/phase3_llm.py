"""
Phase 3: LLM Information Extraction
Extracts 34 specific property coverage fields from insurance documents using GPT.
Works with Google Cloud Storage.
Uses Joblib for parallel chunk processing.
"""
import json
import openai
import os
import re
import time
import socket
import random
from datetime import datetime
from typing import Dict, Any, List, Optional
from google.cloud import storage
from dotenv import load_dotenv
from joblib import Parallel, delayed
import gspread
from google.oauth2.service_account import Credentials
from schemas.property_schema import PROPERTY_FIELDS_SCHEMA, get_field_names, get_required_fields

load_dotenv()

BUCKET_NAME = os.getenv('BUCKET_NAME', 'mckinneysuite')

# Initialize OpenAI
openai.api_key = os.getenv('OPENAI_API_KEY')
CLOUDFLARE_GATEWAY_URL = os.getenv('CLOUDFLARE_GATEWAY_URL')  # Proxy to bypass Railway IP blocking

if not openai.api_key:
    print("[WARN] OPENAI_API_KEY not found in environment variables!")
    print("[WARN] Phase 3 LLM extraction will fail without OpenAI API key")
elif len(openai.api_key) < 20:
    print("[WARN] OPENAI_API_KEY appears invalid (too short)")
else:
    # Validate API key format (should start with sk-)
    if not openai.api_key.startswith('sk-'):
        print("[WARN] OPENAI_API_KEY format may be incorrect (should start with 'sk-')")
    else:
        print(f"[OK] OpenAI API key configured (length: {len(openai.api_key)})")
        if CLOUDFLARE_GATEWAY_URL:
            print(f"[OK] Cloudflare AI Gateway enabled - bypassing Railway IP blocking")
            print(f"[OK] Gateway URL: {CLOUDFLARE_GATEWAY_URL[:60]}...")
        else:
            print("[INFO] Direct OpenAI connection (set CLOUDFLARE_GATEWAY_URL to use proxy)")


def _get_bucket() -> storage.bucket.Bucket:
    client = storage.Client()
    return client.bucket(BUCKET_NAME)


def _create_openai_client(timeout: float = 300.0, max_retries: int = 0) -> openai.OpenAI:
    """
    Create OpenAI client with optional Cloudflare AI Gateway proxy.
    
    Cloudflare AI Gateway bypasses Railway's IP blocking (OpenAI blocks GCP IPs).
    Set CLOUDFLARE_GATEWAY_URL to: https://gateway.ai.cloudflare.com/v1/{account_id}/{gateway_name}/openai
    
    Args:
        timeout: Request timeout in seconds
        max_retries: Max retry attempts (we handle retries ourselves, so default 0)
    
    Returns:
        Configured OpenAI client
    """
    client_params = {
        "api_key": openai.api_key,
        "timeout": timeout,
        "max_retries": max_retries
    }
    
    # Use Cloudflare AI Gateway if configured (bypasses Railway IP blocking)
    if CLOUDFLARE_GATEWAY_URL:
        client_params["base_url"] = CLOUDFLARE_GATEWAY_URL
    
    return openai.OpenAI(**client_params)


def _download_text_from_gcs(bucket: storage.bucket.Bucket, blob_path: str) -> str:
    """Download text file from GCS"""
    blob = bucket.blob(blob_path)
    if not blob.exists():
        return ""
    return blob.download_as_string().decode('utf-8')


def _upload_json_to_gcs(bucket: storage.bucket.Bucket, blob_path: str, data: Dict[str, Any]) -> None:
    """Upload JSON file to GCS"""
    blob = bucket.blob(blob_path)
    blob.upload_from_string(
        json.dumps(data, indent=2, ensure_ascii=False),
        content_type='application/json'
    )
    print(f"✅ Uploaded to: gs://{BUCKET_NAME}/{blob_path}")


# Google Sheets API rate limit: 60 write requests per minute per user
SHEETS_WRITE_DELAY = 2  # seconds between write API calls

def _sheets_api_call_with_retry(func, *args, max_retries=5, **kwargs):
    """
    Execute a Google Sheets API call with retry on rate limit (429) errors.
    Uses exponential backoff: 5s, 10s, 20s, 40s, 65s
    """
    for attempt in range(max_retries):
        try:
            result = func(*args, **kwargs)
            time.sleep(SHEETS_WRITE_DELAY)  # Rate limit protection
            return result
        except gspread.exceptions.APIError as e:
            if hasattr(e, 'response') and e.response.status_code == 429:
                wait_time = min(5 * (2 ** attempt), 65)
                print(f"  ⏳ Rate limited (429). Waiting {wait_time}s before retry {attempt + 1}/{max_retries}...")
                time.sleep(wait_time)
            else:
                raise
    return func(*args, **kwargs)


def reset_user_sheet_to_template(client, username: str):
    """
    Reset user sheet to MAIN SHEET template.
    CRITICAL: This ensures consistent structure EVERY RUN with formatting preserved!
    
    Process:
    1. Get MAIN SHEET (template - never changes)
    2. Get user sheet (or create if doesn't exist)
    3. DELETE old user sheet completely
    4. DUPLICATE MAIN SHEET to user sheet (preserves formatting!)
    5. Return ready-to-write sheet
    
    Uses Google Sheets API to preserve:
    - ✅ Text values
    - ✅ Colors & formatting
    - ✅ Fonts & styles
    - ✅ Borders & shading
    - ✅ Everything!
    """
    try:
        # Open the spreadsheet
        spreadsheet = client.open("Insurance Fields Data")
        
        # Step 1: Get template sheet
        print(f"📋 Reading MAIN SHEET template (with formatting)...")
        template_sheet = spreadsheet.worksheet("MAIN SHEET")
        template_sheet_id = template_sheet.id
        print(f"✅ Template found (Sheet ID: {template_sheet_id})")
        
        # Step 2: Check if user sheet exists and delete it
        try:
            user_sheet = spreadsheet.worksheet(username)
            print(f"🗑️  Found old {username} sheet, deleting it...")
            # Delete the old sheet
            _sheets_api_call_with_retry(spreadsheet.del_worksheet, user_sheet)
            print(f"✅ Old sheet deleted")
        except gspread.exceptions.WorksheetNotFound:
            print(f"ℹ️  No existing user sheet to delete (first run)")
        except Exception as e:
            print(f"ℹ️  No existing user sheet to delete (first run)")
        
        # Step 3: Duplicate MAIN SHEET to user sheet
        # This preserves ALL formatting, colors, fonts, borders, etc!
        print(f"📋 Duplicating MAIN SHEET template with formatting...")
        
        time.sleep(SHEETS_WRITE_DELAY)  # Rate limit protection before duplicate
        
        # Use gspread to duplicate the sheet
        # This copies the entire sheet including formatting
        new_sheet = spreadsheet.duplicate_sheet(
            source_sheet_id=template_sheet_id,
            new_sheet_name=username,
            insert_sheet_index=None
        )
        
        time.sleep(SHEETS_WRITE_DELAY)  # Rate limit protection after duplicate
        
        print(f"✅ Sheet duplicated: {username}")
        print(f"✅ All formatting PRESERVED (colors, fonts, borders, etc.)")
        
        print(f"✅ Sheet '{username}' is RESET and ready for new data!")
        return new_sheet
        
    except Exception as e:
        print(f"❌ Failed to reset sheet: {e}")
        import traceback
        traceback.print_exc()
        raise


def read_combined_file_from_gcs(bucket: storage.bucket.Bucket, file_path: str) -> List[Dict[str, Any]]:
    """Read the intelligent combined file from Phase 2D"""
    try:
        content = _download_text_from_gcs(bucket, file_path)
        if not content:
            print(f"Warning: Combined file not found at {file_path}")
            return []
        
        print(f"Reading intelligent combined file from: {file_path}")
        
        # Extract all pages
        all_pages = []
        # Updated regex to handle both normal format and OCR-only format
        # Normal: PAGE X (PyMuPDF (Clean)): or PAGE X (OCR (All Pages)):
        # OCR-only: PAGE X (OCR Only):
        page_sections = re.findall(
            r'PAGE (\d+) \((?:(PyMuPDF|OCR) \(.*?\)|(OCR Only))\):.*?TEXT CONTENT:.*?------------------------------\n(.*?)\n={80}',
            content,
            re.DOTALL
        )
        
        for match in page_sections:
            page_num = match[0]
            # source can be in match[1] (normal) or match[2] (OCR Only)
            source = match[1] if match[1] else 'OCR'  # OCR Only becomes OCR
            page_text = match[3]  # Text is now in the 4th group
            all_pages.append({
                'page_num': int(page_num),
                'source': source,
                'text': page_text.strip()
            })
        
        # Sort by page number
        all_pages.sort(key=lambda x: x['page_num'])
        
        print(f"Extracted {len(all_pages)} pages from combined file")
        for page in all_pages:
            print(f"  Page {page['page_num']:2d} ({page['source']:8s}): {len(page['text']):5,} chars")
        
        return all_pages
        
    except Exception as e:
        print(f"Error reading combined file: {e}")
        return []


def create_chunks(all_pages: List[Dict[str, Any]], chunk_size: int = 4) -> List[Dict[str, Any]]:
    """Split pages into chunks of 4 pages each"""
    chunks = []
    
    for i in range(0, len(all_pages), chunk_size):
        chunk_pages = all_pages[i:i+chunk_size]
        
        # Combine text from all pages in this chunk
        chunk_text = ""
        page_nums = []
        sources = []
        
        for page in chunk_pages:
            chunk_text += f"=== PAGE {page['page_num']} ({page['source']}) ===\n"
            chunk_text += page['text'] + "\n\n"
            page_nums.append(page['page_num'])
            sources.append(page['source'])
        
        chunks.append({
            'chunk_num': len(chunks) + 1,
            'pages': chunk_pages,
            'page_nums': page_nums,
            'sources': sources,
            'text': chunk_text.strip(),
            'char_count': len(chunk_text)
        })
    
    print(f"\nCreated {len(chunks)} chunks:")
    for chunk in chunks:
        print(f"  Chunk {chunk['chunk_num']}: Pages {chunk['page_nums']} ({chunk['char_count']:,} chars)")
    
    return chunks


def _call_llm_with_retry(client: openai.OpenAI, model: str, prompt: str, max_retries: int = 10, base_delay: float = 3.0) -> Optional[str]:
    """
    Call OpenAI API with aggressive retry logic optimized for Railway.
    Railway-specific improvements:
    - More retries (10 instead of 8)
    - Longer base delay (3s instead of 2s)
    - Fresh client connection on each retry (fixes connection pooling issues)
    - Better timeout handling
    - DNS resolution test before retry
    """
    api_key = openai.api_key
    last_error = None
    
    for attempt in range(max_retries):
        try:
            # Create fresh client on each retry to avoid connection pooling issues on Railway
            if attempt > 0:
                client = _create_openai_client(
                    timeout=300.0,  # 5 minute timeout for Railway (very generous)
                    max_retries=0  # We handle retries ourselves
                )
            
            # Use OpenAI API (GPT-5 Responses API format)
            response = client.responses.create(
                model=model,
                input=prompt,
                reasoning={
                    "effort": "low"
                },
                text={
                    "verbosity": "low"
                }
            )
            
            if attempt > 0:
                print(f"  [SUCCESS] Retry {attempt} succeeded after {attempt} attempts!")
            # Extract text from GPT-5 response format
            return response.output_text.strip()
            
        except (openai.APIConnectionError, openai.APITimeoutError, ConnectionError, TimeoutError, socket.gaierror, socket.timeout) as e:
            last_error = e
            error_type = type(e).__name__
            error_msg_short = str(e)[:100]
            
            if attempt < max_retries - 1:
                # Exponential backoff with jitter: 3s, 6s, 12s, 24s, 48s, 96s, 192s, 384s, 768s, 1536s
                delay = base_delay * (2 ** attempt)
                # Add small random jitter to avoid thundering herd
                jitter = random.uniform(0.5, 1.5)
                delay = delay * jitter
                
                print(f"  [RETRY {attempt + 1}/{max_retries}] {error_type} - retrying in {delay:.1f}s... ({error_msg_short})")
                
                # Test DNS resolution before retry (Railway sometimes has DNS issues)
                if attempt % 3 == 0:  # Every 3rd retry, test DNS
                    try:
                        socket.gethostbyname('api.openai.com')
                        print(f"  [DIAGNOSTIC] DNS resolution for api.openai.com: OK")
                    except socket.gaierror:
                        print(f"  [DIAGNOSTIC] DNS resolution for api.openai.com: FAILED - Railway DNS issue")
                
                time.sleep(delay)
            else:
                error_msg = f"All {max_retries} retry attempts failed. Last error ({error_type}): {str(e)[:200]}"
                print(f"  [ERROR] {error_msg}")
                print(f"  [DIAGNOSTIC] Railway OpenAI API connection failed after {max_retries} attempts")
                print(f"  [DIAGNOSTIC] Possible causes:")
                print(f"  [DIAGNOSTIC] 1. OPENAI_API_KEY invalid/expired (check Railway env vars)")
                print(f"  [DIAGNOSTIC] 2. Railway network timeout (try increasing timeout)")
                print(f"  [DIAGNOSTIC] 3. OpenAI API rate limit (check OpenAI dashboard)")
                print(f"  [DIAGNOSTIC] 4. DNS resolution issue (Railway DNS may be slow)")
                raise ConnectionError(error_msg) from e
                
        except openai.RateLimitError as e:
            last_error = e
            if attempt < max_retries - 1:
                # Much longer delay for rate limits: 15s, 30s, 60s, 120s, etc.
                delay = base_delay * (2 ** attempt) * 5
                print(f"  [RETRY {attempt + 1}/{max_retries}] Rate limit hit, retrying in {delay:.1f}s...")
                time.sleep(delay)
            else:
                print(f"  [ERROR] Rate limit error after {max_retries} attempts: {e}")
                print(f"  [DIAGNOSTIC] OpenAI API rate limit exceeded. Wait before retrying.")
                raise
                
        except openai.AuthenticationError as e:
            # Don't retry auth errors - API key is wrong
            print(f"  [ERROR] Authentication failed - OPENAI_API_KEY is invalid: {str(e)[:200]}")
            print(f"  [DIAGNOSTIC] Check Railway environment variables - API key may be expired or incorrect")
            raise
            
        except Exception as e:
            # For other errors, log and re-raise
            error_type = type(e).__name__
            print(f"  [ERROR] Unexpected error ({error_type}): {str(e)[:200]}")
            if attempt < max_retries - 1:
                delay = base_delay * (2 ** attempt)
                print(f"  [RETRY {attempt + 1}/{max_retries}] Retrying in {delay:.1f}s...")
                time.sleep(delay)
            else:
                raise
    
    return None


def extract_with_llm(chunk: Dict[str, Any], chunk_num: int, total_chunks: int) -> Dict[str, Any]:
    """Extract information using LLM"""
    
    prompt = f"""
    Analyze the following insurance document text and extract ONLY the 34 specific property coverage fields listed below.
    
    CRITICAL: Extract ONLY these 34 fields. Do NOT create new field names or extract any other information.
    
    THE 34 SPECIFIC FIELDS TO EXTRACT (with examples):
    1. Construction Type - Look for: "FRAME", "Frame", "Joisted Masonry", "Masonry Non-Combustible"
    2. Valuation and Coinsurance - Look for: "RC, 90%", "Replacement Cost, 80%", "Actual Cash Value"
       This is the GENERAL/DEFAULT valuation policy shown as its own field
       Extract as standalone value: "RC, 90%" (WITHOUT dollar amounts or coverage names)
       Often appears in its own table row or after "Valuation and Coinsurance:" label
       CRITICAL: Valuation and Coinsurance are often in SEPARATE columns - find BOTH and COMBINE:
       - Part 1: "Valuation: RC" or "Replacement Cost" 
       - Part 2: "Coins %: 90%" or "Coinsurance: 80%"
       - COMBINE as: "RC, 90%" or "Replacement Cost, 90%"
       This field is SEPARATE from individual coverage valuations (Building, Pumps, etc.)
    3. Cosmetic Damage - Look for: "Excluded", "Included", "Cosmetic Damage is Excluded"
    4. Building - Look for: "$500,000", "$648,000 (RC, 90%)", "Coverage not required"
       CRITICAL: If in a table with Valuation and Coins % columns, include them: "$648,000 (RC, 90%)"
    5. Pumps - Look for: "$10,000.00", "$160,000 (RC, 90%)"
       CRITICAL: If in a table with Valuation and Coins % columns, include them: "$160,000 (RC, 90%)"
    6. Canopy - Look for: "$40,000", "$160,000 (RC, 90%)"
       CRITICAL: If in a table with Valuation and Coins % columns, include them: "$160,000 (RC, 90%)"
    7. ROOF EXCLUSION - Look for: "Included", "Excluded", "Cosmetic Damage is Excluded"
    8. Roof Surfacing - Look for: "ACV only applies to roofs that are more than 15 years old", "CP 10 36", "CP 10 36 applies"
       CRITICAL: Often appears in "Subject to:" sections as "CP 10 36 – Limitations on Coverage for Roof Surfacing applies"
       ALSO check endorsement/form lists for "CP 10 36 10 12" or similar codes
       Extract as: "CP 10 36 applies" or "Limitations on Coverage for Roof Surfacing applies" or the full text found
       Form codes like "CP 10 36" are VALID VALUES - DO NOT leave empty if you find them
    9. Roof Surfacing -Limitation - Look for: "ACV on Roof", "Cosmetic Damage is Excluded", "CP 10 36"
       CRITICAL: If "CP 10 36" is mentioned anywhere in Subject to/endorsements, extract it here too
       Extract as: "CP 10 36 applies" or "Limitations on Coverage for Roof Surfacing applies"
       This field and "Roof Surfacing" often have the SAME value when referencing form codes
    10. Business Personal Property - Look for: "$50,000.00", "$50,000 (RC, 90%)", "$200,000"
        If in a table with Valuation and Coins % columns, include them: "$50,000 (RC, 90%)"
    11. Business Income - Look for: "$100,000", "$120,000 (RC, 1/6)", "$100,000 (RC, 1/3)", "$100,000"
        NOTE: Business Income often has coinsurance 1/6 or 1/3 (different from 90%)
        If in a table with Valuation and Coins % columns, include them: "$100,000 (RC, 1/3)"
        Extract dollar amount even if valuation/coinsurance not shown
    12. Business Income with Extra Expense - Look for: "$100,000", "$100,000 (RC, 1/6)", "$100,000 (RC, 1/3)"
        If in a table with Valuation and Coins % columns, include them
    13. Equipment Breakdown - Look for: "Included", "$225,000"
    14. Outdoor Signs - Look for: "$10,000", "$5,000", "Included", "Deductible $250"
    15. Signs Within 1,000 Feet to Premises - Look for: any signs within 1,000 feet coverage
    16. Employee Dishonesty - Look for: "$5,000", "Included", "Not Offered"
    17. Money & Securities - Look for: "$10,000", "$5,000", "On Premises $2,500 / Off Premises $2,500"
    18. Money and Securities (Inside; Outside) - Look for: separate inside/outside limits
    19. Spoilage - Look for: "$5,000", "$10,000", "Deductible $250"
    20. Theft - Look for: "Sublimit: $5,000", "Ded: $2,500", "Sublimit $10,000"
    21. Theft Sublimit - Look for: "$5,000", "$15,000", "$10,000", "Theft Sublimit: $10,000"
        May appear in endorsement sections or main tables
    22. Theft Deductible - Look for: "$2,500", "$1,000", "$250", "Theft Deductible: $1,000"
        May appear in endorsement sections or main tables
    23. Windstorm or Hail - Look for: "$2,500", "2%", "1%", "Min Per Building", "Excluded", "$2,500 Min"
        Often in coverage tables under "Wind/Hail Ded" column
        Can be dollar amount, percentage, or "Excluded"
    24. Named Storm Deductible - Look for: any named storm deductible
    25. Wind and Hail and Named Storm exclusion - Look for: any wind/hail/named storm exclusion
    26. All Other Perils Deductible - Look for: "$2,500", "$1,000", "$5,000", "$5000"
        Often in coverage tables under "AOP Ded" column - extract the dollar amount shown
        Extract ANY dollar amount found near "AOP" or "All Other Perils"
    27. Fire Station Alarm - Look for: "$2,500.00", "Local", "Central"
    28. Burglar Alarm - Look for: "Local", "Central", "Active Central Station"
    29. Terrorism - Look for: "APPLIES", "Excluded", "Included", "Can be added"
        Also look for: "TRIA", "Subject to TRIA", "Terrorism Risk Insurance Act"
    30. Protective Safeguards Requirements - Look for: any protective safeguards requirements
    31. Minimum Earned Premium (MEP) - Look for: "25%", "MEP: 25%", "35%"
    32. Property Premium - Look for: "TOTAL CHARGES W/O TRIA $7,176.09", "W/O TRIA $7,176.09, WITH TRIA $7,441.13"
        CRITICAL: Look for "TOTAL CHARGES" or "Total Premium (With/Without Terrorism)" - NOT "Property Premium"
        "Property Premium" is base only; we need TOTAL which includes endorsements
        DO NOT extract from "Summary of Cost" section (that combines all policies - property, GL, liquor)
        Extract from property coverage section as: "W/O TRIA $7,176.09, WITH TRIA $7,441.13" or single value
    33. Total Premium (With/Without Terrorism) - Look for: "W/O TRIA $7,176.09, WITH TRIA $7,441.13"
        Same as Property Premium - look for TOTAL CHARGES, not base property premium
        DO NOT extract from "Summary of Cost" section
    34. Policy Premium - Look for: "$2,500.00", "Policy Premium", "Base Premium"
    
    EXTRACTION RULES:
    - Extract EXACTLY as written in the document
    - Look for SIMILAR PATTERNS even if exact examples don't match
    - For Dollar Amounts: Look for any dollar amounts ($X,XXX, $X,XXX.XX)
    - For Percentages: Look for any percentages (X%, X.X%)
    - For Deductibles: Look for "Deductible", "Ded", "Min", "Per" with amounts
      * Check coverage tables for columns like "Wind/Hail Ded", "AOP Ded", etc.
      * Can be: dollar amounts ($5,000), percentages (2%), or status (Excluded)
    - For Sublimits: Look for "Sublimit", "Limit", "Max" with amounts
    - For Coverage Status: Look for "Included", "Excluded", "Not Offered", "Coverage not required"
    - For "Valuation and Coinsurance" (Field #2 - standalone general field):
      * This is the GENERAL valuation policy shown as its own separate field
      * Extract as standalone: "RC, 90%" WITHOUT coverage names or dollar amounts
      * MUST extract TWO pieces and combine them:
        - Part 1 (Valuation): RC, Replacement Cost, ACV, Actual Cash Value
        - Part 2 (Coinsurance %): Look for "Coins %", "Coinsurance", or percentage (80%, 90%, 100%)
        - COMBINE as: "RC, 90%" or "Replacement Cost, 80%" - DO NOT extract just "RC" alone
      * Often in separate columns in table - find both parts and combine them
      * This is DIFFERENT from coverage-specific valuations (Building, Business Income, etc.)
    - For COVERAGE AMOUNTS (Building, Pumps, Canopy, BPP, Business Income):
      * If in a TABLE with Valuation and Coins % columns, include them: "$648,000 (RC, 90%)"
      * Example: "Building #01 $648,000 RC 90%" should extract as "$648,000 (RC, 90%)"
      * Business Income often has 1/6 or 1/3 coinsurance instead of 90%
      * If valuation columns not present, extract just the dollar amount
    - For FORM CODES (Roof Surfacing, Terrorism, Windstorm):
      * Form codes like "CP 10 36", "TRIA" are VALID VALUES
      * Often appear in "Subject to:" sections at the end of quotes
      * Extract as "CP 10 36 applies" or "TRIA" - these are complete values
    - For ENDORSEMENT SECTIONS (Theft Sublimit/Deductible, Outdoor Signs, etc.):
      * Check "Additional Endorsements" or "Additional Coverages" sections
      * Format: "Field Name: $value" (e.g., "Theft Sublimit: $10,000")
    - For PREMIUM EXTRACTION (Property Premium, Total Premium):
      * Extract ONLY "TOTAL CHARGES" or "Total Premium" from property coverage section
      * If document shows BOTH "Property Premium" ($6,303) and "Total Premium" ($7,176), extract the TOTAL
      * "Property Premium" = base coverage only; "Total Premium" = base + endorsements (we want TOTAL)
      * CRITICAL: DO NOT extract from "Summary of Cost" section at the end
      * "Summary of Cost" combines property + GL + liquor + fees = wrong value
      * Look for "TOTAL CHARGES W/O TRIA" or "Total Premium (With/Without Terrorism)" in property section
    - If field is not found, set to null
    - Do NOT hallucinate or make up values
    - Do NOT combine or modify existing values (EXCEPT for Valuation/Coinsurance and Coverage Amounts as noted above)
    - Do NOT extract administrative, financial, or policy information
    
    IMPORTANT: This is chunk {chunk_num} of {total_chunks}. This chunk contains pages {chunk['page_nums']}.
    
    CRITICAL PAGE NUMBER EXTRACTION:
    - The document text below has clear page markers: "=== PAGE X (OCR) ===" or "=== PAGE X (PyMuPDF) ==="
    - For each field you extract, find which "=== PAGE X ===" section it appears in
    - Extract the EXACT page number X from that section marker
    - Look BACKWARDS from the field to find the most recent "=== PAGE X ===" marker
    - DO NOT guess or estimate page numbers - use the exact number from the marker
    - Multiple fields can be on the same page
    
    Example: If you see:
    === PAGE 7 (OCR) ===
    Commercial Property
    Building #01: $648,000
    Construction: MNC
    
    Then "Construction Type" should have page: 7 (because it's under "=== PAGE 7 ===" marker)
    
    CRITICAL: Return ONLY valid JSON with this exact format:
    {{
        "Construction Type": {{"value": "MNC", "page": 7}},
        "Building": {{"value": "$648,000 (RC, 90%)", "page": 7}},
        "Business Income": {{"value": "$120,000 (RC, 1/6)", "page": 7}},
        "Roof Surfacing": {{"value": "CP 10 36 applies", "page": 9}},
        "Roof Surfacing -Limitation": {{"value": "Limitations on Coverage for Roof Surfacing applies", "page": 9}},
        "Windstorm or Hail": {{"value": "Excluded", "page": 7}},
        "All Other Perils Deductible": {{"value": "$5,000", "page": 7}},
        "Theft Sublimit": {{"value": "$10,000", "page": 8}},
        "Theft Deductible": {{"value": "$1,000", "page": 8}},
        "Terrorism": {{"value": "TRIA", "page": 9}},
        "Property Premium": {{"value": "W/O TRIA $7,176.09, WITH TRIA $7,441.13", "page": 9}}
    }}
    
    If a field is not found, use: {{"value": null, "page": null}}
    
    IMPORTANT: 
    - Check entire document: main tables, endorsement sections, and "Subject to:" sections
    - For Premium: Extract "TOTAL CHARGES" from property section, NOT "Summary of Cost" at end
    - If both "Property Premium" and "Total Premium" exist, extract the TOTAL (includes endorsements)
    - "Summary of Cost" section combines all policies (property + GL + liquor) - DO NOT use it
    
    Do not provide explanations, context, or any text outside the JSON object.
    
    Document text:
    {chunk['text']}
    """
    
    try:
        print(f"  Processing chunk {chunk_num} with LLM (Pages {chunk['page_nums']})...")
        
        # Use OpenAI API with retry logic optimized for Railway
        client = _create_openai_client(
            timeout=300.0,
            max_retries=0  # We handle retries ourselves
        )
        result_text = _call_llm_with_retry(client, "gpt-5-nano", prompt, max_retries=10, base_delay=3.0)
        
        if not result_text:
            print(f"  [ERROR] Empty response from LLM after retries")
            return {'_metadata': {'chunk_num': chunk_num, 'page_nums': chunk['page_nums'], 'error': 'Empty LLM response after retries'}}
        
        # Clean up markdown code blocks if present
        if result_text.startswith('```json'):
            result_text = result_text[7:]
        if result_text.startswith('```'):
            result_text = result_text[3:]
        if result_text.endswith('```'):
            result_text = result_text[:-3]
        result_text = result_text.strip()
        
        # Try to parse JSON
        try:
            result_json = json.loads(result_text)
            
            # Convert to compatible format
            converted_json = {}
            individual_page_fields = {}
            
            for field, data in result_json.items():
                if isinstance(data, dict) and 'value' in data and 'page' in data:
                    converted_json[field] = data['value']
                    if data['value'] is not None and data['page'] is not None:
                        individual_page_fields[field] = [data['page']]
                        print(f"    Found {field} on Page {data['page']}")
                else:
                    converted_json[field] = data
            
            # Add metadata
            converted_json['_metadata'] = {
                'chunk_num': chunk_num,
                'page_nums': chunk['page_nums'],
                'sources': chunk['sources'],
                'char_count': chunk['char_count'],
                'individual_page_fields': individual_page_fields
            }
            
            found_fields = len([k for k, v in converted_json.items() if v is not None and k != '_metadata'])
            print(f"  [SUCCESS] Extracted {found_fields} fields from pages {chunk['page_nums']}")
            return converted_json
            
        except json.JSONDecodeError as e:
            print(f"  [ERROR] Failed to parse JSON response")
            print(f"  Raw LLM response: {result_text[:200]}...")
            return {'_metadata': {'chunk_num': chunk_num, 'page_nums': chunk['page_nums'], 'error': f'JSON parse failed: {str(e)}'}}
            
    except Exception as e:
        print(f"  [ERROR] LLM processing failed: {e}")
        return {'_metadata': {'chunk_num': chunk_num, 'page_nums': chunk['page_nums'], 'error': str(e)}}


def merge_extraction_results(all_results: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Merge results from all chunks, prioritizing non-null values"""
    
    # Define the expected fields
    # Use schema to initialize fields in correct order
    # This ensures Google Sheets always has consistent field ordering
    expected_field_names = get_field_names()  # From schema - guaranteed order
    
    merged_result = {}
    
    # Initialize all expected fields as null (schema order preserved)
    for field_name in expected_field_names:
        merged_result[field_name] = None
    
    # Collect all unique fields found by LLM
    all_found_fields = set()
    for chunk_result in all_results:
        if '_metadata' in chunk_result and 'error' not in chunk_result['_metadata']:
            for field in chunk_result.keys():
                if field != '_metadata':
                    all_found_fields.add(field)
    
    # Add any new fields found by LLM
    for field in all_found_fields:
        if field not in merged_result:
            merged_result[field] = None
    
    # Track which specific page each field was found on
    field_sources = {}
    
    # Merge results from all chunks
    for chunk_result in all_results:
        if '_metadata' in chunk_result and 'error' in chunk_result['_metadata']:
            continue  # Skip failed chunks
            
        chunk_pages = chunk_result['_metadata']['page_nums']
        
        for field, value in chunk_result.items():
            if field == '_metadata':
                continue
                
            if value is not None and value != "" and value != "null":
                if merged_result[field] is None:
                    merged_result[field] = value
                    field_sources[field] = [chunk_pages[0]] if chunk_pages else []
                else:
                    if merged_result[field] != value:
                        print(f"  Multiple values found for {field}: '{merged_result[field]}' (pages {field_sources[field]}) and '{value}' (pages {chunk_pages})")
    
    # Add source information to merged result
    merged_result['_extraction_summary'] = {
        'total_chunks_processed': len(all_results),
        'successful_chunks': len([r for r in all_results if '_metadata' in r and 'error' not in r['_metadata']]),
        'field_sources': field_sources
    }
    
    return merged_result


def extract_company_info_only(bucket: storage.bucket.Bucket, carrier_name: str, safe_carrier_name: str, file_type: str, pdf_info: Dict) -> Dict[str, Any]:
    """
    Extract ONLY 5 company information fields from the first available PDF.
    Called once per upload, not per carrier/PDF.
    """
    try:
        print(f"\n📋 Extracting company info from {carrier_name} {file_type}...")
        
        # Find combined file
        type_short = file_type.replace('PDF', '').lower()
        combined_files = list(bucket.list_blobs(prefix=f'phase2d/results/{safe_carrier_name}_{type_short}_intelligent_combined_'))
        if not combined_files:
            print(f"⚠️  No combined file found")
            return None
        
        combined_file = sorted(combined_files, key=lambda x: x.time_created)[-1].name
        
        # Read first 2 pages only (company info is always at top)
        all_pages = read_combined_file_from_gcs(bucket, combined_file)
        if not all_pages:
            return None
        
        # Take only first 2 pages
        first_pages = all_pages[:2]
        combined_text = "\n\n".join([f"=== PAGE {p['page_num']} ===\n{p['text']}" for p in first_pages])
        
        # LLM prompt for company info ONLY (5 fields)
        prompt = f"""
Analyze the following insurance document text and extract ONLY these 5 company information fields:

FIELDS TO EXTRACT:
1. Named Insured - Look for: "Named Insured:", "Insured:", company/business name
   Example: "HYPERCITY INVESTMENTS LLC"

2. Mailing Address - Look for: "Mailing Address:", "Mailing:", address where mail is sent
   Example: "7506 MARTIN LUTHER KING BLVD HOUSTON, TX 77033"
   OR: "PO BOX 12345, ATLANTA, GA 30301"
   Extract EXACTLY as written, include full address (street/PO Box, city, state, zip) as one line

3. Location Address - Look for: "Location Address:", "Risk Location:", "Premises:", "Location:", physical business location
   Example: "7506 MARTIN LUTHER KING BLVD HOUSTON, TX 77033"
   OR: "Same as mailing" or "Same" (if document says so)
   OR: Different address than mailing
   IMPORTANT: Extract EXACTLY as written - could be same, different, or "Same as mailing"

4. Policy Term - Look for: "Policy Term:", "Policy Period:", "Term:", date range
   Example: "12/03/2025-26" or "12/01/2024 - 12/01/2025" or "12/01/2024 to 12/01/2025"
   Extract exact format shown

5. Description of Business - Look for: "Description of Business:", "Business Type:", "Operations:", "Business Description:"
   Example: "C STORE WITH GAS - 18 HOURS" or "Convenience Store with Fuel Sales"
   Extract complete description exactly as written

EXTRACTION RULES:
- Extract EXACTLY as written in the document
- Mailing Address and Location Address are SEPARATE fields - extract both independently
- If Location Address says "Same as mailing" or similar, extract that text
- If both addresses are identical, extract the full address for BOTH fields
- For Policy Term: Keep original format (MM/DD/YYYY-YY or MM/DD/YYYY - MM/DD/YYYY)
- If field not found, set to null
- Do NOT hallucinate or make up values
- These fields are typically at the TOP of page 1 or 2

Document text (first 2 pages):
{combined_text}

Return ONLY valid JSON with these 5 fields:
{{
    "Named Insured": "HYPERCITY INVESTMENTS LLC",
    "Mailing Address": "PO BOX 12345, ATLANTA, GA 30301",
    "Location Address": "7506 MARTIN LUTHER KING BLVD HOUSTON, TX 77033",
    "Policy Term": "12/03/2025-26",
    "Description of Business": "C STORE WITH GAS - 18 HOURS"
}}
"""
        
        # Use OpenAI API with retry logic optimized for Railway
        client = _create_openai_client(
            timeout=300.0,
            max_retries=0  # We handle retries ourselves
        )
        result_text = _call_llm_with_retry(client, "gpt-5-nano", prompt, max_retries=10, base_delay=3.0)
        
        if not result_text:
            print(f"  [ERROR] Empty response from LLM after retries")
            return None
        
        # Check if response is empty
        if not result_text:
            print(f"  [ERROR] Empty response from LLM")
            return None
        
        # Clean markdown
        if result_text.startswith('```json'):
            result_text = result_text[7:]
        if result_text.startswith('```'):
            result_text = result_text[3:]
        if result_text.endswith('```'):
            result_text = result_text[:-3]
        result_text = result_text.strip()
        
        # Parse JSON
        company_info = json.loads(result_text)
        
        print(f"✅ Extracted company info:")
        for key, value in company_info.items():
            if value:
                print(f"   {key}: {value}")
        
        return company_info
        
    except json.JSONDecodeError as e:
        print(f"  [ERROR] Failed to parse JSON response")
        print(f"  Raw LLM response: {result_text[:200]}...")
        return None
    except Exception as e:
        print(f"❌ Failed to extract company info: {e}")
        import traceback
        traceback.print_exc()
        return None


def save_extraction_results_to_gcs(
    bucket: storage.bucket.Bucket,
    merged_result: Dict[str, Any],
    carrier_name: str,
    safe_carrier_name: str,
    file_type: str,
    timestamp: str
) -> str:
    """Save extraction results to GCS"""
    type_short = file_type.replace('PDF', '').lower()
    final_file_path = f'phase3/results/{safe_carrier_name}_{type_short}_final_validated_fields_{timestamp}.json'
    
    # Get page sources from extraction summary
    field_sources = merged_result.get('_extraction_summary', {}).get('field_sources', {})
    
    # Create final validated fields structure
    final_fields = {}
    for field_name, llm_value in merged_result.items():
        if not field_name.startswith('_'):  # Skip metadata fields
            source_pages = field_sources.get(field_name, [])
            page_info = f"Page {source_pages[0]}" if source_pages else ""
            
            final_fields[field_name] = {
                "llm_value": llm_value,
                "vlm_value": None,  # No VLM validation
                "final_value": llm_value,  # Use LLM value as final
                "confidence": "llm_only",
                "source_page": page_info
            }
    
    _upload_json_to_gcs(bucket, final_file_path, final_fields)
    print(f"✅ Saved final validated fields to: gs://{BUCKET_NAME}/{final_file_path}")
    
    return final_file_path


def _check_if_all_carriers_complete(bucket: storage.bucket.Bucket, upload_id: str) -> bool:
    """
    Check if all carriers in this upload have completed Phase 3.
    Returns True if this is the last carrier to finish.
    """
    try:
        # Read metadata to get total carriers
        from phase1 import _read_metadata
        full_metadata = _read_metadata(bucket)
        uploads = full_metadata.get('uploads', [])
        upload_record = next((u for u in uploads if u.get('uploadId') == upload_id), None)
        
        if not upload_record:
            print(f"⚠️  Upload {upload_id} not found in metadata")
            return False
        
        carriers = upload_record.get('carriers', [])
        total_carriers = len(carriers)
        
        if total_carriers == 0:
            print(f"⚠️  No carriers found for upload {upload_id}")
            return False
        
        # Count how many carriers have completed Phase 3
        completed_count = 0
        for carrier in carriers:
            carrier_name = carrier.get('carrierName', 'Unknown')
            safe_name = carrier_name.lower().replace(" ", "_").replace("&", "and")
            
            # Check for property, liability, liquor, and workers comp final validated fields
            for file_type in ['propertyPDF', 'liabilityPDF', 'liquorPDF', 'workersCompPDF']:
                pdf_info = carrier.get(file_type)
                if not pdf_info or not pdf_info.get('path'):
                    continue
                
                # Extract timestamp from PDF path
                pdf_path = pdf_info['path']
                timestamp_match = re.search(r'_(\d{8}_\d{6})\.pdf$', pdf_path)
                if not timestamp_match:
                    continue
                
                timestamp = timestamp_match.group(1)
                type_short = file_type.replace('PDF', '').lower()
                
                # Check if Phase 3 result exists
                final_file_path = f"phase3/results/{safe_name}_{type_short}_final_validated_fields_{timestamp}.json"
                blob = bucket.blob(final_file_path)
                if blob.exists():
                    completed_count += 1
        
        # Calculate total expected files (property + liability + liquor + workers comp for each carrier)
        expected_files = 0
        for carrier in carriers:
            if carrier.get('propertyPDF') and carrier.get('propertyPDF').get('path'):
                expected_files += 1
            if carrier.get('liabilityPDF') and carrier.get('liabilityPDF').get('path'):
                expected_files += 1
            if carrier.get('liquorPDF') and carrier.get('liquorPDF').get('path'):
                expected_files += 1
            if carrier.get('workersCompPDF') and carrier.get('workersCompPDF').get('path'):
                expected_files += 1
        
        print(f"📊 Upload {upload_id}: {completed_count}/{expected_files} files completed")
        
        return completed_count == expected_files and expected_files > 0
        
    except Exception as e:
        print(f"❌ Error checking completion status: {e}")
        import traceback
        traceback.print_exc()
        return False


def process_upload_llm_extraction(upload_id: str) -> Dict[str, Any]:
    """
    Given an upload_id, read Phase 2D results from GCS,
    extract insurance fields using LLM, and save results.
    """
    if not openai.api_key:
        return {"success": False, "error": "OpenAI API key not configured. Cannot run Phase 3."}
    
    bucket = _get_bucket()
    
    # Read metadata
    from phase1 import _read_metadata
    metadata = _read_metadata(bucket)
    
    uploads: List[Dict[str, Any]] = metadata.get('uploads', [])
    record = next((u for u in uploads if u.get('uploadId') == upload_id), None)
    if record is None:
        return {"success": False, "error": f"uploadId {upload_id} not found"}
    
    all_results: List[Dict[str, Any]] = []
    
    # ====== STEP 1: Extract company info ONCE from first available PDF ======
    print("\n" + "="*80)
    print("📋 STEP 1: EXTRACTING COMPANY INFORMATION (ONE-TIME)")
    print("="*80)
    
    company_info = None
    for carrier in record.get('carriers', []):
        carrier_name = carrier.get('carrierName')
        safe_carrier_name = carrier_name.lower().replace(" ", "_").replace("&", "and")
        
        # Try property first, then GL, then liquor, then WC
        for file_type in ['propertyPDF', 'liabilityPDF', 'liquorPDF', 'workersCompPDF']:
            pdf_info = carrier.get(file_type)
            if pdf_info and pdf_info.get('path'):
                company_info = extract_company_info_only(bucket, carrier_name, safe_carrier_name, file_type, pdf_info)
                if company_info:
                    print(f"✅ Got company info from {carrier_name} {file_type}")
                    # Save company info to GCS
                    company_info_path = f"phase3/results/{upload_id}_company_info.json"
                    _upload_json_to_gcs(bucket, company_info_path, company_info)
                    break
        if company_info:
            break  # Got it, stop searching
    
    if not company_info:
        print("⚠️  Could not extract company info from any PDF")
        company_info = {}  # Empty dict
    
    print("="*80 + "\n")
    
    print("\n" + "="*80)
    print("📋 STEP 2: EXTRACTING COVERAGE FIELDS FROM ALL PDFs")
    print("="*80 + "\n")
    
    # Helper function to process a single PDF file
    def process_single_pdf_file(carrier_name, safe_carrier_name, file_type, pdf_info):
        """Process one PDF file (property/GL/liquor/workersComp) - called in parallel"""
        gs_path = pdf_info.get('path')
        if not gs_path:
            return None
        
        try:
            print(f"\n📄 Processing {carrier_name} - {file_type}...")
            # Extract timestamp from PDF path
            original_pdf_path = pdf_info.get('path')
            timestamp_match = re.search(r'_(\d{8}_\d{6})\.pdf$', original_pdf_path)
            if not timestamp_match:
                report_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            else:
                report_timestamp = timestamp_match.group(1)
            
            type_short = file_type.replace('PDF', '').lower()
            
            # Find latest intelligent combined file
            combined_files = list(bucket.list_blobs(prefix=f'phase2d/results/{safe_carrier_name}_{type_short}_intelligent_combined_'))
            if not combined_files:
                print(f"Warning: No combined file found for {carrier_name} {file_type}")
                return None
            
            # Get latest file
            combined_file = sorted(combined_files, key=lambda x: x.time_created)[-1].name
            
            # Read combined file
            all_pages = read_combined_file_from_gcs(bucket, combined_file)
            if not all_pages:
                print(f"Warning: No pages extracted from {combined_file}")
                return None
            
            # Create chunks (4 pages each)
            chunks = create_chunks(all_pages, chunk_size=4)
            
            # Process each chunk with LLM - PARALLELIZED for faster processing
            # Route to correct extractor based on file type
            print(f"  Processing {len(chunks)} chunks...")
            
            def process_single_chunk(chunk):
                """Process one chunk - called in parallel"""
                if file_type == 'liabilityPDF':
                    # Import GL-specific extractor for liability
                    from phase3_gl import extract_with_llm as extract_with_llm_gl
                    return extract_with_llm_gl(chunk, chunk['chunk_num'], len(chunks))
                elif file_type == 'liquorPDF':
                    # Import Liquor-specific extractor for liquor
                    from phase3_liqour import extract_with_llm as extract_with_llm_liquor
                    return extract_with_llm_liquor(chunk, chunk['chunk_num'], len(chunks))
                elif file_type == 'workersCompPDF':
                    # Import Workers Comp-specific extractor
                    from phase3_workers_comp import extract_with_llm as extract_with_llm_wc
                    return extract_with_llm_wc(chunk, chunk['chunk_num'], len(chunks))
                else:
                    # Use property extraction for property PDFs
                    return extract_with_llm(chunk, chunk['chunk_num'], len(chunks))
            
            # Process all chunks in parallel (n_jobs=-1 uses all available cores)
            # backend='threading' is perfect for I/O-bound LLM API calls
            chunk_results = Parallel(
                n_jobs=-1,
                backend='threading',
                verbose=5
            )(
                delayed(process_single_chunk)(chunk)
                for chunk in chunks
            )
            
            # Merge all results - route to correct merge function based on file type
            print(f"  Merging results from {len(chunk_results)} chunks...")
            if file_type == 'liabilityPDF':
                # Import GL-specific merge for liability extraction
                from phase3_gl import merge_extraction_results as merge_extraction_results_gl
                merged_result = merge_extraction_results_gl(chunk_results)
            elif file_type == 'liquorPDF':
                # Import Liquor-specific merge for liquor extraction
                from phase3_liqour import merge_extraction_results as merge_extraction_results_liquor
                merged_result = merge_extraction_results_liquor(chunk_results)
            elif file_type == 'workersCompPDF':
                # Import Workers Comp-specific merge
                from phase3_workers_comp import merge_extraction_results as merge_extraction_results_wc
                merged_result = merge_extraction_results_wc(chunk_results)
            else:
                # Use property merge for property PDFs
                merged_result = merge_extraction_results(chunk_results)
            
            # Save results to GCS
            final_path = save_extraction_results_to_gcs(bucket, merged_result, carrier_name, safe_carrier_name, file_type, report_timestamp)
            
            return {
                'carrierName': carrier_name,
                'fileType': file_type,
                'finalFields': f'gs://{BUCKET_NAME}/{final_path}',
                'totalFields': len([k for k in merged_result.keys() if not k.startswith('_')]),
                'fieldsFound': len([k for k, v in merged_result.items() if v is not None and not k.startswith('_')])
            }
        
        except Exception as e:
            print(f"❌ Error processing {carrier_name} {file_type}: {e}")
            return {
                'carrierName': carrier_name,
                'fileType': file_type,
                'error': str(e)
            }
    
    # Process each carrier
    for carrier in record.get('carriers', []):
        carrier_name = carrier.get('carrierName')
        safe_carrier_name = carrier_name.lower().replace(" ", "_").replace("&", "and")
        
        print(f"\n{'='*60}")
        print(f"🏢 Processing {carrier_name}")
        print(f"{'='*60}")
        
        # Collect all PDF files for this carrier
        pdf_tasks = []
        for file_type in ['propertyPDF', 'liabilityPDF', 'liquorPDF', 'workersCompPDF']:
            pdf_info = carrier.get(file_type)
            if pdf_info and pdf_info.get('path'):
                pdf_tasks.append((carrier_name, safe_carrier_name, file_type, pdf_info))
        
        if not pdf_tasks:
            print(f"⚠️  No PDFs found for {carrier_name}")
            continue
        
        # Process all PDFs for this carrier IN PARALLEL (aggressive multi-user + multi-PDF)
        print(f"🚀 Processing {len(pdf_tasks)} PDFs in parallel...")
        carrier_results = Parallel(
            n_jobs=-1,  # Use all available cores for maximum parallelism
            backend='threading',
            verbose=5
        )(
            delayed(process_single_pdf_file)(carrier_name, safe_carrier_name, file_type, pdf_info)
            for carrier_name, safe_carrier_name, file_type, pdf_info in pdf_tasks
        )
        
        # Add results (filter out None values from skipped files)
        all_results.extend([r for r in carrier_results if r is not None])
    
    result = {
        "success": True,
        "uploadId": upload_id,
        "results": all_results
    }
    
    # Check if all carriers in this upload have completed Phase 3
    print("\n✅ Phase 3 LLM extraction complete!")
    print("🔍 Checking if all carriers are complete...")
    
    if _check_if_all_carriers_complete(bucket, upload_id):
        print("🎉 ALL CARRIERS COMPLETE! Auto-filling sheets...")
        
        # Get username from metadata (now using username instead of userId)
        username = record.get('username', 'default')
        print(f"📋 Using user-specific sheet tab: '{username}'")
        
        # Auto-fill GL data to sheet
        try:
            from pathlib import Path
            
            # Get credentials - use environment variable first (Railway), then local paths
            creds_path = os.getenv('GOOGLE_SHEETS_CREDENTIALS') or os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
            
            if not creds_path or not os.path.exists(creds_path):
                # Fall back to local development paths
                possible_paths = [
                    '/app/credentials/gcp-credentials.json',
                    'credentials/insurance-sheets-474717-7fc3fd9736bc.json',
                    '../credentials/insurance-sheets-474717-7fc3fd9736bc.json',
                ]
                for path in possible_paths:
                    if os.path.exists(path):
                        creds_path = str(Path(path).resolve())
                        break
            
            if creds_path:
                scope = [
                    'https://www.googleapis.com/auth/spreadsheets',
                    'https://www.googleapis.com/auth/drive'
                ]
                creds = Credentials.from_service_account_file(creds_path, scopes=scope)
                client = gspread.authorize(creds)
                
                # CRITICAL: Reset user sheet to MAIN SHEET template before writing
                print(f"\n{'='*80}")
                print(f"TEMPLATE RESET PROCEDURE FOR USER: {username}")
                print(f"{'='*80}")
                sheet = reset_user_sheet_to_template(client, username)
                print(f"{'='*80}\n")
                
                # ====== Fill company information (5 fields in Column A, rows 2-6) ======
                print("  📝 Filling company information...")
                if company_info:
                    company_updates = []
                    
                    # Build each row as "Label: Value" in a single cell
                    company_fields = [
                        ("Named Insured", 2),
                        ("Mailing Address", 3),
                        ("Location Address", 4),
                        ("Policy Term", 5),
                        ("Description of Business", 6),
                    ]
                    
                    for field_name, row_num in company_fields:
                        if field_name in company_info and company_info[field_name]:
                            # Write full text "Label: Value" to cell A{row}
                            full_text = f"{field_name}: {company_info[field_name]}"
                            company_updates.append({
                                'range': f"A{row_num}",
                                'values': [[full_text]]
                            })
                    
                    # Batch update all at once (maintains sequence)
                    if company_updates:
                        _sheets_api_call_with_retry(sheet.batch_update, company_updates)
                        print(f"  ✅ Filled {len(company_updates)} company info rows")
                    else:
                        print("  ⚠️  No company info to fill")
                
                # GL Field to Row mapping (row numbers for each field)
                gl_field_rows = {
                    "Each Occurrence/General Aggregate Limits": 8,
                    "Liability Deductible - Per claim or Per Occ basis": 9,
                    "Hired Auto And Non-Owned Auto Liability - Without Delivery Service": 10,
                    "Fuel Contamination coverage limits": 11,
                    "Vandalism coverage": 12,
                    "Garage Keepers Liability": 13,
                    "Employment Practices Liability": 14,
                    "Abuse & Molestation Coverage limits": 15,
                    "Assault & Battery Coverage limits": 16,
                    "Firearms/Active Assailant Coverage limits": 17,
                    "Additional Insured": 18,
                    "Additional Insured (Mortgagee)": 19,
                    "Additional insured - Jobber": 20,
                    "Exposure": 21,
                    "Rating basis: If Sales - Subject to Audit": 22,
                    "Terrorism": 23,
                    "Personal and Advertising Injury Limit": 24,
                    "Products/Completed Operations Aggregate Limit": 25,
                    "Minimum Earned": 26,
                    "General Liability Premium": 27,
                    "Total Premium (With/Without Terrorism)": 28,
                    "Policy Premium": 29,
                    "Contaminated fuel": 30,
                    "Liquor Liability": 31,
                    "Additional Insured - Managers Or Lessors Of Premises": 32,
                }
                
                # Property Field to Row mapping
                property_field_rows = {
                    "Construction Type": 46,
                    "Valuation and Coinsurance": 47,
                    "Cosmetic Damage": 48,
                    "Building": 49,
                    "Pumps": 50,
                    "Canopy": 51,
                    "Roof Surfacing": 53,
                    "Roof Surfacing -Limitation": 54,
                    "Business Personal Property": 55,
                    "Business Income": 56,
                    "Business Income with Extra Expense": 57,
                    "Equipment Breakdown": 58,
                    "Outdoor Signs": 59,
                    "Signs Within 1,000 Feet to Premises": 60,
                    "Employee Dishonesty": 61,
                    "Money & Securities": 62,
                    "Money and Securities (Inside; Outside)": 63,
                    "Spoilage": 64,
                    "Theft": 65,
                    "Theft Sublimit": 66,
                    "Theft Deductible": 67,
                    "Windstorm or Hail Deductible": 68,
                    "Named Storm Deductible": 69,
                    "Wind and Hail and Named Storm exclusion": 70,
                    "All Other Perils Deductible": 71,
                    "Fire Station Alarm": 72,
                    "Burglar Alarm": 73,
                    "Loss Payee": 74,
                    "Forms and Exclusions": 75,
                    "Requirement: Protective Safeguards": 76,
                    "Terrorism": 77,
                    "Subjectivity:": 78,
                    "Minimum Earned": 79,
                    "Total Premium (With/Without Terrorism)": 80,
                }
                
                # Liquor Field to Row mapping
                liquor_field_rows = {
                    "Each Occurrence/General Aggregate Limits": 36,
                    "Sales - Subject to Audit": 37,
                    "Assault & Battery/Firearms/Active Assailant": 38,
                    "Requirements": 39,
                    "If any subjectivities in quote please add": 40,
                    "Minimum Earned": 41,
                    "Total Premium (With/Without Terrorism)": 42,
                    "Liquor Premium": 42,  # Same as Total Premium
                    "Policy Premium": 42,  # Same as Total Premium
                }
                
                # Workers Comp Field to Row mapping
                workers_comp_field_rows = {
                    "Limits": 86,
                    "FEIN #": 87,
                    "Payroll - Subject to Audit": 88,
                    "Excluded Officer": 89,
                    "If Opting out from Workers Compensation Coverage": 89,  # Same row as Excluded Officer
                    "Total Premium": 90,
                    "Workers Compensation Premium": 90,  # Same as Total Premium
                    "Policy Premium": 90,  # Same as Total Premium
                }
                
                # Columns for each carrier: B (Option 1), C (Option 2), D (Option 3)
                columns = ['B', 'C', 'D']
                
                # STEP 1: Clear old data from all columns (B, C, D) for GL, Property, Liquor, and Workers Comp rows
                print("  🧹 Clearing old data from columns B, C, D...")
                clear_ranges = []
                # GL rows (8-32)
                for col in columns:
                    clear_ranges.append(f"{col}8:{col}32")
                # Liquor rows (36-42)
                for col in columns:
                    clear_ranges.append(f"{col}36:{col}42")
                # Property rows (46-80)
                for col in columns:
                    clear_ranges.append(f"{col}46:{col}80")
                # Workers Comp rows (86-90)
                for col in columns:
                    clear_ranges.append(f"{col}86:{col}90")
                # Premium Breakdown rows (91-97) - clear entire section
                for col in columns:
                    clear_ranges.append(f"{col}91:{col}97")  # Premium Breakdown section (GL, Property, Umbrella, LL, WC, Total Policy Premium)
                
                # Clear all ranges in one batch call
                if clear_ranges:
                    _sheets_api_call_with_retry(sheet.batch_clear, clear_ranges)
                print("  ✅ Cleared old data from columns B, C, D")
                
                # STEP 2: Assign one column per carrier (for ALL their file types)
                updates = []
                
                # Loop through each carrier ONCE and get their column
                for carrier_index, carrier in enumerate(record.get('carriers', [])):
                    if carrier_index >= 3:  # Max 3 carriers
                        break
                    
                    carrier_name = carrier.get('carrierName', 'Unknown')
                    column = columns[carrier_index]  # This carrier's column (B, C, or D)
                    
                    # Process GL data if exists
                    if carrier.get('liabilityPDF'):
                        pdf_path = carrier['liabilityPDF']['path']
                        timestamp_match = re.search(r'_(\d{8}_\d{6})\.pdf$', pdf_path)
                        if timestamp_match:
                            timestamp = timestamp_match.group(1)
                            safe_name = carrier_name.lower().replace(" ", "_").replace("&", "and")
                            
                            # Load GL data from GCS
                            gl_file = f"phase3/results/{safe_name}_liability_final_validated_fields_{timestamp}.json"
                            blob = bucket.blob(gl_file)
                            if blob.exists():
                                gl_data = json.loads(blob.download_as_string().decode('utf-8'))
                                
                                # For row 28 (Total Premium), use priority logic to match row 91
                                for field_name, row_num in gl_field_rows.items():
                                    # Special handling for row 28 - use priority like row 91
                                    if row_num == 28:
                                        continue  # Handle row 28 separately below
                                    
                                    if field_name in gl_data:
                                        field_info = gl_data[field_name]
                                        llm_value = field_info.get("llm_value", "") if isinstance(field_info, dict) else field_info
                                        if llm_value:
                                            cell_ref = f"{column}{row_num}"
                                            updates.append({
                                                'range': cell_ref,
                                                'values': [[str(llm_value)]]
                                            })
                                
                                # Handle row 28 (Total Premium) with priority logic to match row 91
                                for field_name in ["Total Premium (With/Without Terrorism)", "Total GL Premium", "Total Premium GL (With/Without Terrorism)"]:
                                    if field_name in gl_data:
                                        field_info = gl_data[field_name]
                                        llm_value = field_info.get("llm_value", "") if isinstance(field_info, dict) else field_info
                                        if llm_value:
                                            updates.append({
                                                'range': f"{column}28",  # Total Premium row
                                                'values': [[str(llm_value)]]
                                            })
                                            break  # Stop at first match, same as row 91
                                
                                print(f"  ✓ Carrier {carrier_index + 1} ({carrier_name}) GL → Column {column}")
                    
                            # Also copy to Premium Breakdown row 91
                            for field_name in ["Total Premium (With/Without Terrorism)", "Total GL Premium", "Total Premium GL (With/Without Terrorism)"]:
                                if field_name in gl_data:
                                    field_info = gl_data[field_name]
                                    llm_value = field_info.get("llm_value", "") if isinstance(field_info, dict) else field_info
                                    if llm_value:
                                        updates.append({
                                            'range': f"{column}91",  # GL Premium row
                                            'values': [[str(llm_value)]]
                                        })
                                        break
                
                    # Process Property data if exists (SAME carrier, SAME column)
                    if carrier.get('propertyPDF'):
                        pdf_path = carrier['propertyPDF']['path']
                        timestamp_match = re.search(r'_(\d{8}_\d{6})\.pdf$', pdf_path)
                        if timestamp_match:
                            timestamp = timestamp_match.group(1)
                            safe_name = carrier_name.lower().replace(" ", "_").replace("&", "and")
                            
                            # Load Property data from GCS
                            property_file = f"phase3/results/{safe_name}_property_final_validated_fields_{timestamp}.json"
                            blob = bucket.blob(property_file)
                            if blob.exists():
                                property_data = json.loads(blob.download_as_string().decode('utf-8'))
                                
                                # For row 80 (Total Premium), use priority logic to match row 92
                                for field_name, row_num in property_field_rows.items():
                                    # Special handling for row 80 - use priority like row 92
                                    if row_num == 80:
                                        continue  # Handle row 80 separately below
                                    
                                    if field_name in property_data:
                                        field_info = property_data[field_name]
                                        llm_value = field_info.get("llm_value", "") if isinstance(field_info, dict) else field_info
                                        if llm_value:
                                            cell_ref = f"{column}{row_num}"
                                            updates.append({
                                                'range': cell_ref,
                                                'values': [[str(llm_value)]]
                                            })
                                
                                # Handle row 80 (Total Premium) with priority logic to match row 92
                                for field_name in ["Total Premium (With/Without Terrorism)", "Total Property Premium", "Total Premium Property (With/Without Terrorism)"]:
                                    if field_name in property_data:
                                        field_info = property_data[field_name]
                                        llm_value = field_info.get("llm_value", "") if isinstance(field_info, dict) else field_info
                                        if llm_value:
                                            updates.append({
                                                'range': f"{column}80",  # Total Premium row
                                                'values': [[str(llm_value)]]
                                            })
                                            break  # Stop at first match, same as row 92
                                
                                print(f"  ✓ Carrier {carrier_index + 1} ({carrier_name}) Property → Column {column}")
                            
                            # Also copy to Premium Breakdown row 92
                            for field_name in ["Total Premium (With/Without Terrorism)", "Total Property Premium", "Total Premium Property (With/Without Terrorism)"]:
                                if field_name in property_data:
                                    field_info = property_data[field_name]
                                    llm_value = field_info.get("llm_value", "") if isinstance(field_info, dict) else field_info
                                    if llm_value:
                                        updates.append({
                                            'range': f"{column}92",  # Property Premium row
                                            'values': [[str(llm_value)]]
                                        })
                                        break
                    
                    # Process Liquor data if exists (SAME carrier, SAME column)
                    if carrier.get('liquorPDF'):
                        pdf_path = carrier['liquorPDF']['path']
                        timestamp_match = re.search(r'_(\d{8}_\d{6})\.pdf$', pdf_path)
                        if timestamp_match:
                            timestamp = timestamp_match.group(1)
                            safe_name = carrier_name.lower().replace(" ", "_").replace("&", "and")
                            
                            # Load Liquor data from GCS
                            liquor_file = f"phase3/results/{safe_name}_liquor_final_validated_fields_{timestamp}.json"
                            blob = bucket.blob(liquor_file)
                            if blob.exists():
                                liquor_data = json.loads(blob.download_as_string().decode('utf-8'))
                                
                                # Use the liquor field rows mapping defined earlier
                                # For row 42 (Total Premium), use priority logic to match row 94
                                for field_name, row_num in liquor_field_rows.items():
                                    # Special handling for row 42 - use priority like row 94
                                    if row_num == 42:
                                        continue  # Handle row 42 separately below
                                    
                                    if field_name in liquor_data:
                                        field_info = liquor_data[field_name]
                                        llm_value = field_info.get("llm_value", "") if isinstance(field_info, dict) else field_info
                                        if llm_value:
                                            cell_ref = f"{column}{row_num}"
                                            updates.append({
                                                'range': cell_ref,
                                                'values': [[str(llm_value)]]
                                            })
                                
                                # Handle row 42 (Total Premium) with priority logic to match row 94
                                for field_name in ["Total Premium (With/Without Terrorism)", "Total Liquor Premium", "Liquor Premium", "Policy Premium", "Total Premium Liquor (With/Without Terrorism)"]:
                                    if field_name in liquor_data:
                                        field_info = liquor_data[field_name]
                                        llm_value = field_info.get("llm_value", "") if isinstance(field_info, dict) else field_info
                                        if llm_value:
                                            updates.append({
                                                'range': f"{column}42",  # Total Premium row
                                                'values': [[str(llm_value)]]
                                            })
                                            break  # Stop at first match, same as row 94
                                
                                print(f"  ✓ Carrier {carrier_index + 1} ({carrier_name}) Liquor → Column {column}")
                            
                            # Also copy to Premium Breakdown row 94
                            for field_name in ["Total Premium (With/Without Terrorism)", "Total Liquor Premium", "Total Premium Liquor (With/Without Terrorism)"]:
                                if field_name in liquor_data:
                                    field_info = liquor_data[field_name]
                                    llm_value = field_info.get("llm_value", "") if isinstance(field_info, dict) else field_info
                                    if llm_value:
                                        updates.append({
                                            'range': f"{column}94",  # LL premium row
                                            'values': [[str(llm_value)]]
                                        })
                                        break
                    
                    # Process Workers Comp data if exists (SAME carrier, SAME column)
                    if carrier.get('workersCompPDF'):
                        pdf_path = carrier['workersCompPDF']['path']
                        timestamp_match = re.search(r'_(\d{8}_\d{6})\.pdf$', pdf_path)
                        if timestamp_match:
                            timestamp = timestamp_match.group(1)
                            safe_name = carrier_name.lower().replace(" ", "_").replace("&", "and")
                            
                            # Load Workers Comp data from GCS
                            wc_file = f"phase3/results/{safe_name}_workerscomp_final_validated_fields_{timestamp}.json"
                            blob = bucket.blob(wc_file)
                            if blob.exists():
                                wc_data = json.loads(blob.download_as_string().decode('utf-8'))
                                
                                # Use the workers comp field rows mapping defined earlier
                                # Skip "Excluded Officer" fields - leave row 89 empty
                                fields_to_skip = ["Excluded Officer", "If Opting out from Workers Compensation Coverage"]
                                # For row 90 (Total Premium), use priority logic to match row 96
                                for field_name, row_num in workers_comp_field_rows.items():
                                    if field_name in fields_to_skip:
                                        continue  # Skip these fields, leave row 89 empty
                                    
                                    # Special handling for row 90 - use priority like row 96
                                    if row_num == 90:
                                        continue  # Handle row 90 separately below
                                    
                                    if field_name in wc_data:
                                        field_info = wc_data[field_name]
                                        llm_value = field_info.get("llm_value", "") if isinstance(field_info, dict) else field_info
                                        if llm_value:
                                            cell_ref = f"{column}{row_num}"
                                            updates.append({
                                                'range': cell_ref,
                                                'values': [[str(llm_value)]]
                                            })
                                
                                # Handle row 90 (Total Premium) with priority logic to match row 96
                                # Use EXACT same priority list as row 96 to ensure consistency
                                for field_name in ["Total Premium", "Workers Compensation Premium", "Policy Premium", "Total Premium (With/Without Terrorism)"]:
                                    if field_name in wc_data:
                                        field_info = wc_data[field_name]
                                        llm_value = field_info.get("llm_value", "") if isinstance(field_info, dict) else field_info
                                        if llm_value:
                                            updates.append({
                                                'range': f"{column}90",  # Total Premium row
                                                'values': [[str(llm_value)]]
                                            })
                                            break  # Stop at first match, same as row 96
                                 
                                print(f"  ✓ Carrier {carrier_index + 1} ({carrier_name}) Workers Comp → Column {column}")
                            
                            # Also copy to Premium Breakdown row 96
                            for field_name in ["Total Premium", "Workers Compensation Premium", "Policy Premium", "Total Premium (With/Without Terrorism)"]:
                                if field_name in wc_data:
                                    field_info = wc_data[field_name]
                                    llm_value = field_info.get("llm_value", "") if isinstance(field_info, dict) else field_info
                                    if llm_value:
                                        updates.append({
                                            'range': f"{column}96",  # WC Premium row
                                            'values': [[str(llm_value)]]
                                        })
                                        break
                
                # Batch update sheet (GL + Property + Liquor + Workers Comp + Premium Breakdown combined)
                if updates:
                    _sheets_api_call_with_retry(sheet.batch_update, updates)
                    print(f"✅ Batch updated {len(updates)} fields to sheet (GL + Property + Liquor + Workers Comp + Premium Breakdown)")
                else:
                    print("⚠️  No values to fill")
            else:
                print("⚠️  Credentials not found")
        except Exception as e:
            print(f"❌ Sheet fill failed: {e}")
            import traceback
            traceback.print_exc()
    else:
        print("⏳ Other carriers still processing. Waiting for all to complete...")
    
    return result
