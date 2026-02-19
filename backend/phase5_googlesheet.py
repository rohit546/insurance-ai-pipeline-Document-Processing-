"""
Phase 5: Google Sheets Push
Push extracted insurance fields to Google Sheets.
Works with Google Cloud Storage.
"""
import json
import gspread
from google.oauth2.service_account import Credentials
import os
import re
from datetime import datetime
from typing import Dict, Any, List
from google.cloud import storage
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()

BUCKET_NAME = os.getenv('BUCKET_NAME', 'mckinneysuite')


def _get_bucket() -> storage.bucket.Bucket:
    client = storage.Client()
    return client.bucket(BUCKET_NAME)


def _download_json_from_gcs(bucket: storage.bucket.Bucket, blob_path: str) -> Dict[str, Any]:
    """Download JSON file from GCS"""
    blob = bucket.blob(blob_path)
    if not blob.exists():
        return {}
    return json.loads(blob.download_as_string().decode('utf-8'))


def _get_credentials_path() -> str:
    """Get Google Sheets credentials path"""
    # First check environment variable (Railway deployment)
    creds_path = os.getenv('GOOGLE_SHEETS_CREDENTIALS')
    if creds_path and os.path.exists(creds_path):
        return creds_path
    
    # Try multiple local paths
    possible_paths = [
        '/app/credentials/gcp-credentials.json',
        'credentials/gcp-credentials.json',
        'credentials/insurance-sheets-474717-7fc3fd9736bc.json',
        '../credentials/insurance-sheets-474717-7fc3fd9736bc.json',
        '../insurance-sheets-474717-7fc3fd9736bc.json',
        'insurance-sheets-474717-7fc3fd9736bc.json'
    ]
    
    for path in possible_paths:
        if os.path.exists(path):
            return str(Path(path).resolve())
    
    raise Exception("Google Sheets credentials not found! Please provide credentials.json")


def push_to_sheets_from_gcs(bucket: storage.bucket.Bucket, data_path: str, sheet_name: str = "Insurance Fields Data"):
    """Push data from GCS to Google Sheets"""
    
    print("Starting Google Sheets push...")
    
    # Download JSON data from GCS
    data = _download_json_from_gcs(bucket, data_path)
    if not data:
        raise Exception(f"Failed to download data from {data_path}")
    
    print(f"✅ Loaded {len(data)} fields from GCS")
    
    # Setup Google Sheets
    scope = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]
    
    creds_path = _get_credentials_path()
    print(f"✅ Using credentials from: {creds_path}")
    
    try:
        creds = Credentials.from_service_account_file(creds_path, scopes=scope)
        client = gspread.authorize(creds)
        print("✅ Connected to Google Sheets!")
        
        # Try to open the sheet by name
        sheet = None
        try:
            print(f"🔍 Looking for sheet: {sheet_name}")
            sheet = client.open(sheet_name).sheet1
            print(f"✅ Opened existing sheet: {sheet_name}")
        except gspread.exceptions.SpreadsheetNotFound as e:
            print(f"⚠️  Sheet '{sheet_name}' not found by name. Error: {e}")
            print("   This might be a permissions issue. Trying alternative approach...")
            # If sheet not found, it might be permissions. Try to list all sheets
            try:
                spreadsheets = client.openall()
                print(f"Found {len(spreadsheets)} accessible spreadsheets")
                for ss in spreadsheets:
                    print(f"  - {ss.title}")
                    if sheet_name.lower() in ss.title.lower():
                        sheet = ss.sheet1
                        print(f"✅ Found matching sheet: {ss.title}")
                        break
                
                if not sheet:
                    print(f"❌ Could not find sheet '{sheet_name}'. Creating new one...")
                    spreadsheet = client.create(sheet_name)
                    sheet = spreadsheet.sheet1
                    print(f"✅ Created new sheet: {sheet_name} (ID: {spreadsheet.id})")
            except Exception as list_err:
                print(f"❌ Failed to list sheets: {list_err}")
                raise
        
        if not sheet:
            raise Exception(f"Could not open or create sheet '{sheet_name}'")
        
        # Clear and push data
        sheet.clear()
        print("✅ Cleared existing data")
        
        # Prepare all data at once (BATCH UPDATE)
        all_rows = []
        
        # Header row
        all_rows.append(["Field Name", "LLM Value", "VLM Value", "Final Value", "Confidence", "Source Page"])
        
        # Data rows
        for field_name, field_data in data.items():
            row = [
                field_name,
                field_data.get('llm_value', 'null'),
                field_data.get('vlm_value', 'null'),
                field_data.get('final_value', 'null'),
                field_data.get('confidence', 'llm_only'),
                field_data.get('source_page', '')
            ]
            all_rows.append(row)
        
        print(f"📝 Prepared {len(all_rows)} rows (1 header + {len(all_rows)-1} data)")
        
        # Push ALL data in ONE API call (no rate limit issues!)
        print(f"📤 Pushing data to Google Sheets...")
        update_response = sheet.update('A1', all_rows)  # Returns Response object
        print(f"✅ Google Sheets update response: {update_response}")
        print(f"✅ Added {len(all_rows)} rows in single batch update")
        
        print(f"✅ DONE! Pushed {len(all_rows)} rows to Google Sheets!")
        print(f"✅ Check your Google Sheet: {sheet_name}")
        
        return len(all_rows)  # Return the count, not the response object
        
    except Exception as e:
        print(f"❌ ERROR in push_to_sheets_from_gcs: {type(e).__name__}: {e}")
        raise


def process_upload_googlesheets_push(upload_id: str, sheet_name: str = "Insurance Fields Data") -> Dict[str, Any]:
    """
    Given an upload_id, read Phase 3 results from GCS,
    and push to Google Sheets.
    """
    bucket = _get_bucket()
    
    # Read metadata
    from phase1 import _read_metadata
    metadata = _read_metadata(bucket)
    
    uploads: List[Dict[str, Any]] = metadata.get('uploads', [])
    record = next((u for u in uploads if u.get('uploadId') == upload_id), None)
    if record is None:
        return {"success": False, "error": f"uploadId {upload_id} not found"}
    
    all_results: List[Dict[str, Any]] = []
    
    for carrier in record.get('carriers', []):
        carrier_name = carrier.get('carrierName')
        safe_carrier_name = carrier_name.lower().replace(" ", "_").replace("&", "and")
        
        for file_type in ['propertyPDF', 'liabilityPDF', 'liquorPDF']:
            pdf_info = carrier.get(file_type)
            if not pdf_info:
                continue
            
            gs_path = pdf_info.get('path')
            if not gs_path:
                continue
            
            try:
                # Extract timestamp from PDF path
                original_pdf_path = pdf_info.get('path')
                timestamp_match = re.search(r'_(\d{8}_\d{6})\.pdf$', original_pdf_path)
                if not timestamp_match:
                    report_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                else:
                    report_timestamp = timestamp_match.group(1)
                
                type_short = file_type.replace('PDF', '').lower()
                
                # Find latest final validated fields file
                final_files = list(bucket.list_blobs(prefix=f'phase3/results/{safe_carrier_name}_{type_short}_final_validated_fields_'))
                if not final_files:
                    print(f"Warning: No final validated fields found for {carrier_name} {file_type}")
                    continue
                
                # Get latest file
                final_file = sorted(final_files, key=lambda x: x.time_created)[-1].name
                
                # Push to Google Sheets
                print(f"📤 About to push {carrier_name} {file_type} from {final_file}")
                rows_pushed = push_to_sheets_from_gcs(bucket, final_file, sheet_name)
                print(f"📊 Successfully pushed {rows_pushed} rows")
                
                all_results.append({
                    'carrierName': carrier_name,
                    'fileType': file_type,
                    'rowsPushed': rows_pushed,
                    'dataSource': f'gs://{BUCKET_NAME}/{final_file}'
                })
                
            except Exception as e:
                print(f"❌ Error processing {carrier_name} {file_type}: {type(e).__name__}: {e}")
                import traceback
                traceback.print_exc()
                all_results.append({
                    'carrierName': carrier_name,
                    'fileType': file_type,
                    'error': str(e)
                })
    
    return {
        "success": True,
        "uploadId": upload_id,
        "results": all_results
    }


def _get_all_unique_fields(all_carrier_data: Dict[str, Dict[str, Any]], carrier_names: List[str], insurance_type: str) -> List[str]:
    """
    Get all unique fields across all carriers for a specific insurance type, preserving order.
    Returns fields in the order they appear in the first carrier that has data.
    """
    all_fields = []
    for carrier_name in carrier_names:
        carrier_data = all_carrier_data.get(carrier_name, {})
        type_data = carrier_data.get(insurance_type)
        if type_data:
            for field_name in type_data.keys():
                if field_name not in all_fields:
                    all_fields.append(field_name)
    return all_fields


def _apply_sheet_formatting(sheet, all_rows: List[List[str]], has_property: bool, has_liability: bool, has_liquor: bool) -> None:
    """Apply formatting to section headers in Google Sheets"""
    try:
        # Get spreadsheet object for batch formatting
        spreadsheet = sheet.spreadsheet
        
        # Calculate which rows are section headers by scanning all_rows
        current_row_idx = 0
        requests = []
        
        # Row 0: Company header - Green background, white text, bold, centered
        if len(all_rows) > 0:
            requests.append({
                'repeatCell': {
                    'range': {
                        'sheetId': 0,
                        'startRowIndex': 0,
                        'endRowIndex': 1,
                        'startColumnIndex': 0,
                        'endColumnIndex': 10
                    },
                    'cell': {
                        'userEnteredFormat': {
                            'backgroundColor': {'red': 0.2, 'green': 0.6, 'blue': 0.2},
                            'textFormat': {'foregroundColor': {'red': 1.0, 'green': 1.0, 'blue': 1.0}, 'bold': True},
                            'horizontalAlignment': 'CENTER',
                            'wrapStrategy': 'WRAP'
                        }
                    },
                    'fields': 'userEnteredFormat(backgroundColor,textFormat,horizontalAlignment,wrapStrategy)'
                }
            })
            current_row_idx = 3  # Skip company header, empty row, and section title row
        
        # Property section header (row 2 = index 2)
        if has_property and len(all_rows) > 2:
            requests.append({
                'repeatCell': {
                    'range': {
                        'sheetId': 0,
                        'startRowIndex': 2,
                        'endRowIndex': 3,
                        'startColumnIndex': 0,
                        'endColumnIndex': 10
                    },
                    'cell': {
                        'userEnteredFormat': {
                            'backgroundColor': {'red': 0.0, 'green': 0.0, 'blue': 0.0},
                            'textFormat': {'foregroundColor': {'red': 1.0, 'green': 1.0, 'blue': 1.0}, 'bold': True},
                            'horizontalAlignment': 'CENTER'
                        }
                    },
                    'fields': 'userEnteredFormat(backgroundColor,textFormat,horizontalAlignment)'
                }
            })
            
            # Find Property column headers row
            for i in range(3, min(20, len(all_rows))):
                row = all_rows[i]
                if row and len(row) > 0 and row[0] == "Field Name":
                    requests.append({
                        'repeatCell': {
                            'range': {
                                'sheetId': 0,
                                'startRowIndex': i,
                                'endRowIndex': i + 1,
                                'startColumnIndex': 0,
                                'endColumnIndex': 10
                            },
                            'cell': {
                                'userEnteredFormat': {
                                    'backgroundColor': {'red': 0.9, 'green': 0.9, 'blue': 0.9},
                                    'textFormat': {'bold': True}
                                }
                            },
                            'fields': 'userEnteredFormat(backgroundColor,textFormat)'
                        }
                    })
                    break
        
        # Liability section header - scan for it
        if has_liability:
            for i in range(5, min(100, len(all_rows))):
                row = all_rows[i]
                if row and len(row) > 0 and any(x and 'General Liability' in str(x) for x in row):
                    requests.append({
                        'repeatCell': {
                            'range': {
                                'sheetId': 0,
                                'startRowIndex': i,
                                'endRowIndex': i + 1,
                                'startColumnIndex': 0,
                                'endColumnIndex': 10
                            },
                            'cell': {
                                'userEnteredFormat': {
                                    'backgroundColor': {'red': 0.0, 'green': 0.0, 'blue': 0.0},
                                    'textFormat': {'foregroundColor': {'red': 1.0, 'green': 1.0, 'blue': 1.0}, 'bold': True},
                                    'horizontalAlignment': 'CENTER'
                                }
                            },
                            'fields': 'userEnteredFormat(backgroundColor,textFormat,horizontalAlignment)'
                        }
                    })
                    # Find Liability column headers
                    for j in range(i + 1, min(i + 10, len(all_rows))):
                        row2 = all_rows[j]
                        if row2 and len(row2) > 0 and row2[0] == "Field Name":
                            requests.append({
                                'repeatCell': {
                                    'range': {
                                        'sheetId': 0,
                                        'startRowIndex': j,
                                        'endRowIndex': j + 1,
                                        'startColumnIndex': 0,
                                        'endColumnIndex': 10
                                    },
                                    'cell': {
                                        'userEnteredFormat': {
                                            'backgroundColor': {'red': 0.9, 'green': 0.9, 'blue': 0.9},
                                            'textFormat': {'bold': True}
                                        }
                                    },
                                    'fields': 'userEnteredFormat(backgroundColor,textFormat)'
                                }
                            })
                            break
                    break
        
        # Liquor section header - scan for it
        if has_liquor:
            for i in range(5, min(100, len(all_rows))):
                row = all_rows[i]
                if row and len(row) > 0 and any(x and 'Liquor' in str(x) for x in row):
                    requests.append({
                        'repeatCell': {
                            'range': {
                                'sheetId': 0,
                                'startRowIndex': i,
                                'endRowIndex': i + 1,
                                'startColumnIndex': 0,
                                'endColumnIndex': 10
                            },
                            'cell': {
                                'userEnteredFormat': {
                                    'backgroundColor': {'red': 0.0, 'green': 0.0, 'blue': 0.0},
                                    'textFormat': {'foregroundColor': {'red': 1.0, 'green': 1.0, 'blue': 1.0}, 'bold': True},
                                    'horizontalAlignment': 'CENTER'
                                }
                            },
                            'fields': 'userEnteredFormat(backgroundColor,textFormat,horizontalAlignment)'
                        }
                    })
                    # Find Liquor column headers
                    for j in range(i + 1, min(i + 10, len(all_rows))):
                        row2 = all_rows[j]
                        if row2 and len(row2) > 0 and row2[0] == "Field Name":
                            requests.append({
                                'repeatCell': {
                                    'range': {
                                        'sheetId': 0,
                                        'startRowIndex': j,
                                        'endRowIndex': j + 1,
                                        'startColumnIndex': 0,
                                        'endColumnIndex': 10
                                    },
                                    'cell': {
                                        'userEnteredFormat': {
                                            'backgroundColor': {'red': 0.9, 'green': 0.9, 'blue': 0.9},
                                            'textFormat': {'bold': True}
                                        }
                                    },
                                    'fields': 'userEnteredFormat(backgroundColor,textFormat)'
                                }
                            })
                            break
                    break
        
        # Apply all formats in batch
        if requests:
            spreadsheet.batch_update({'requests': requests})
            
    except Exception as e:
        print(f"⚠️  Warning: Could not apply formatting: {e}")
        import traceback
        traceback.print_exc()
        # Don't fail the whole process if formatting fails


def finalize_upload_to_sheets(upload_id: str, sheet_name: str = "Insurance Fields Data") -> Dict[str, Any]:
    """
    Finalize upload: Load ALL carriers from this upload, fill into MAIN SHEET template.
    
    TEMPLATE-BASED APPROACH:
    1. Duplicate MAIN SHEET template to user-specific tab (preserves formatting, logos, disclaimers)
    2. Clear only data cells (not static content)
    3. Fill values into exact row/column positions using field→row mappings
    4. Up to 3 carriers in columns B, C, D
    
    Layout (from MAIN SHEET template):
    - Row 1: Company Header (Mckinney & Co. Insurance)
    - Rows 2-6: Named Insured, Mailing Address, Location, Policy Term, Description
    - Row 7: Section headers with carrier name columns
    - Rows 8-32: Liability Coverages
    - Rows 36-42: Liquor Liability
    - Rows 46-80: Property Coverages
    - Rows 86-90: Workers Compensation
    - Rows 91-97: Premium Breakdown
    """
    print(f"\n{'='*80}")
    print(f"FINALIZING UPLOAD: {upload_id}")
    print(f"{'='*80}")
    
    bucket = _get_bucket()
    
    # 1. Load upload metadata
    metadata_path = f"metadata/uploads/{upload_id}.json"
    print(f"📂 Loading metadata from: {metadata_path}")
    
    from phase1 import _read_metadata
    full_metadata = _read_metadata(bucket)
    uploads = full_metadata.get('uploads', [])
    upload_record = next((u for u in uploads if u.get('uploadId') == upload_id), None)
    
    if not upload_record:
        return {"success": False, "error": f"Upload {upload_id} not found in metadata"}
    
    carriers = upload_record.get('carriers', [])
    if not carriers:
        return {"success": False, "error": "No carriers found in upload"}
    
    carrier_names = [c.get('carrierName', 'Unknown') for c in carriers]
    print(f"📦 Found {len(carriers)} carriers: {', '.join(carrier_names)}")
    
    # 2. Load ALL carrier data (property + liability + liquor + workers comp)
    all_carrier_data = {}
    
    for carrier in carriers:
        carrier_name = carrier.get('carrierName', 'Unknown')
        safe_name = carrier_name.lower().replace(" ", "_").replace("&", "and")
        
        all_carrier_data[carrier_name] = {
            'property': None,
            'liability': None,
            'liquor': None,
            'workerscomp': None
        }
        
        # Check for property, liability, liquor, and workers comp files
        for file_type in ['propertyPDF', 'liabilityPDF', 'liquorPDF', 'workersCompPDF']:
            pdf_info = carrier.get(file_type)
            if not pdf_info or not pdf_info.get('path'):
                continue
            
            # Extract timestamp from PDF path
            pdf_path = pdf_info['path']
            timestamp_match = re.search(r'_(\d{8}_\d{6})\.pdf$', pdf_path)
            if not timestamp_match:
                print(f"⚠️  Could not extract timestamp from {pdf_path}")
                continue
            
            timestamp = timestamp_match.group(1)
            type_short = file_type.replace('PDF', '').lower()
            # Handle workersCompPDF -> workerscomp
            if type_short == 'workerscomp':
                type_short = 'workerscomp'
            
            # Construct path to final validated fields
            final_file_path = f"phase3/results/{safe_name}_{type_short}_final_validated_fields_{timestamp}.json"
            
            try:
                data = _download_json_from_gcs(bucket, final_file_path)
                if data:
                    all_carrier_data[carrier_name][type_short] = data
                    print(f"  ✅ Loaded {type_short} data for {carrier_name} ({len(data)} fields)")
                else:
                    print(f"  ⚠️  No data found for {carrier_name} {type_short}")
            except Exception as e:
                print(f"  ❌ Failed to load {carrier_name} {type_short}: {e}")
    
    # 3. Setup Google Sheets
    print(f"\n🔗 Connecting to Google Sheets...")
    scope = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]
    
    creds_path = _get_credentials_path()
    print(f"✅ Using credentials from: {creds_path}")
    
    try:
        creds = Credentials.from_service_account_file(creds_path, scopes=scope)
        client = gspread.authorize(creds)
        print("✅ Connected to Google Sheets!")
        
        # Get username from metadata for user-specific tab
        username = upload_record.get('username', 'default')
        print(f"📋 Using user-specific sheet tab: '{username}'")
        
        # 4. Open spreadsheet and select user-specific tab
        sheet = None
        try:
            print(f"🔍 Looking for spreadsheet: {sheet_name}")
            spreadsheet = client.open(sheet_name)
            print(f"✅ Opened existing spreadsheet: {sheet_name}")
            
            # Try to open user-specific tab
            try:
                sheet = spreadsheet.worksheet(username)
                print(f"✅ Opened user tab: {username}")
            except gspread.exceptions.WorksheetNotFound:
                print(f"⚠️  User tab '{username}' not found. Falling back to MAIN SHEET")
                sheet = spreadsheet.sheet1
                
        except gspread.exceptions.SpreadsheetNotFound:
            print(f"⚠️  Spreadsheet not found, trying alternative approach...")
            spreadsheets = client.openall()
            for ss in spreadsheets:
                if sheet_name.lower() in ss.title.lower():
                    spreadsheet = ss
                    print(f"✅ Found matching spreadsheet: {ss.title}")
                    
                    # Try to open user-specific tab
                    try:
                        sheet = spreadsheet.worksheet(username)
                        print(f"✅ Opened user tab: {username}")
                    except gspread.exceptions.WorksheetNotFound:
                        print(f"⚠️  User tab '{username}' not found. Falling back to MAIN SHEET")
                        sheet = spreadsheet.sheet1
                    break
        
        if not sheet:
            raise Exception(f"Could not open sheet or find tab '{username}' in '{sheet_name}'")
        
        # 5. CRITICAL: Reset user sheet to MAIN SHEET template (preserves formatting!)
        from phase3_llm import reset_user_sheet_to_template
        print(f"\n{'='*80}")
        print(f"TEMPLATE RESET PROCEDURE FOR USER: {username}")
        print(f"{'='*80}")
        sheet = reset_user_sheet_to_template(client, username)
        print(f"{'='*80}\n")
        
        # 6. Use row mappings to fill data into exact template positions
        import json
        
        # GL Field to Row mapping (matches MAIN SHEET template)
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
        
        # Property Field to Row mapping (matches MAIN SHEET template)
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
          
        # Liquor Field to Row mapping (matches MAIN SHEET template)
        liquor_field_rows = {
            "Each Occurrence/General Aggregate Limits": 36,
            "Sales - Subject to Audit": 37,
            "Assault & Battery/Firearms/Active Assailant": 38,
            "Requirements": 39,
            "If any subjectivities in quote please add": 40,
            "Minimum Earned": 41,
            "Total Premium (With/Without Terrorism)": 42,
            "Liquor Premium": 42,
            "Policy Premium": 42,
        }
        
        # Workers Comp Field to Row mapping (matches MAIN SHEET template)
        workers_comp_field_rows = {
            "Limits": 86,
            "FEIN #": 87,
            "Payroll - Subject to Audit": 88,
            "Excluded Officer": 89,
            "If Opting out from Workers Compensation Coverage": 89,
            "Total Premium": 90,
            "Workers Compensation Premium": 90,
            "Policy Premium": 90,
        }
        
        # Columns for each carrier: B (Option 1), C (Option 2), D (Option 3)
        columns = ['B', 'C', 'D']
        
        # STEP 1: Clear old data from data columns (B, C, D) — NOT static content
        # PRESERVE: Row 70-75 Column B (merged cells with template text)
        print("  🧹 Clearing old data from columns B, C, D...")
        clear_ranges = []
        for col in columns:
            clear_ranges.append(f"{col}8:{col}32")   # GL rows
            clear_ranges.append(f"{col}36:{col}42")  # Liquor rows
            
            # Property rows - but preserve specific cells in Column B
            if col == 'B':
                # Column B: Clear 46-69, 76-79, 80 (skip 70-75 to preserve merged cells)
                clear_ranges.append(f"{col}46:{col}69")
                # SKIP: B70-B75 (preserve merged cells: Wind/Hail exclusion, Fire Alarm, Burglar Alarm)
                clear_ranges.append(f"{col}76:{col}79")
                clear_ranges.append(f"{col}80:{col}80")
            else:
                # Columns C and D: Clear all 46-80
                clear_ranges.append(f"{col}46:{col}80")
            
            clear_ranges.append(f"{col}86:{col}90")  # Workers Comp rows
            clear_ranges.append(f"{col}91:{col}97")  # Premium Breakdown rows
        
        if clear_ranges:
            sheet.batch_clear(clear_ranges)
        print("  ✅ Cleared old data (preserved B70-B75 merged cells with template text)")
        
        # STEP 1.5: Update column headers (Row 7) with actual carrier names
        print("  📝 Updating column headers with carrier names...")
        header_updates = []
        
        for carrier_index, carrier in enumerate(carriers):
            if carrier_index >= 3:  # Max 3 carriers
                break
            
            carrier_name = carrier.get('carrierName', f'Option {carrier_index + 1}')
            column = columns[carrier_index]
            
            header_updates.append({
                'range': f"{column}7",
                'values': [[carrier_name]]
            })
            print(f"    ✏ Row 7, Column {column}: '{carrier_name}'")
        
        if header_updates:
            sheet.batch_update(header_updates)
            print(f"  ✅ Updated {len(header_updates)} column headers in row 7")
        else:
            print("  ⚠️  No carrier names to update")
        
        # STEP 2: Fill company information (rows 2-6)
        print("  📝 Filling company information...")
        company_info = None
        company_info_path = f"phase3/results/{upload_id}_company_info.json"
        print(f"  🔍 Looking for company info at: {company_info_path}")
        
        blob = bucket.blob(company_info_path)
        if blob.exists():
            print(f"  ✅ Found company info file")
            try:
                company_info = json.loads(blob.download_as_string().decode('utf-8'))
                print(f"  ✅ Loaded company info: {list(company_info.keys()) if company_info else 'empty'}")
            except Exception as e:
                print(f"  ❌ Failed to parse company info: {e}")
        else:
            print(f"  ⚠️  Company info file NOT FOUND at: {company_info_path}")
            # List what files actually exist in phase3/results/ for this upload
            print(f"  🔍 Checking what Phase 3 files exist for upload {upload_id}...")
            phase3_files = list(bucket.list_blobs(prefix=f'phase3/results/{upload_id}'))
            if phase3_files:
                print(f"  📂 Found {len(phase3_files)} Phase 3 files for this upload:")
                for f in phase3_files[:10]:
                    print(f"    - {f.name}")
            else:
                print(f"  ⚠️  No Phase 3 files found with prefix: phase3/results/{upload_id}")
        
        if company_info:
            company_updates = []
            company_fields = [
                ("Named Insured", 2),
                ("Mailing Address", 3),
                ("Location Address", 4),
                ("Policy Term", 5),
                ("Description of Business", 6),
            ]
            
            for field_name, row_num in company_fields:
                if field_name in company_info and company_info[field_name]:
                    full_text = f"{field_name}: {company_info[field_name]}"
                    company_updates.append({
                        'range': f"A{row_num}",
                        'values': [[full_text]]
                    })
            
            if company_updates:
                sheet.batch_update(company_updates)
                print(f"  ✅ Filled {len(company_updates)} company info rows")
        else:
            print("  ⚠️  No company info to fill")
        
        # STEP 3: Fill data for each carrier into template positions
        updates = []
        
        print(f"  📈 Preparing field updates for {len(carriers)} carriers...")
        print(f"  📦 Loaded carrier data: {list(all_carrier_data.keys())}")
        
        for carrier_index, carrier in enumerate(carriers):
            if carrier_index >= 3:  # Max 3 carriers
                break
            
            carrier_name = carrier.get('carrierName', 'Unknown')
            column = columns[carrier_index]  # B, C, or D
            
            print(f"  🔍 Processing carrier {carrier_index + 1}: {carrier_name} → Column {column}")
            
            # Process GL data
            if carrier.get('liabilityPDF') and carrier_name in all_carrier_data and all_carrier_data[carrier_name].get('liability'):
                gl_data = all_carrier_data[carrier_name]['liability']
                print(f"    ✏ GL data found: {len(gl_data)} fields")
                gl_updates_before = len(updates)
                
                for field_name, row_num in gl_field_rows.items():
                    if row_num == 28:
                        continue  # Handle Total Premium separately
                    
                    if field_name in gl_data:
                        field_info = gl_data[field_name]
                        llm_value = field_info.get("llm_value", "") if isinstance(field_info, dict) else field_info
                        if llm_value:
                            updates.append({
                                'range': f"{column}{row_num}",
                                'values': [[str(llm_value)]]
                            })
                
                # Handle row 28 (Total Premium) — multiple possible field names
                for field_name in ["Total Premium (With/Without Terrorism)", "Total GL Premium", "Total Premium GL (With/Without Terrorism)"]:
                    if field_name in gl_data:
                        field_info = gl_data[field_name]
                        llm_value = field_info.get("llm_value", "") if isinstance(field_info, dict) else field_info
                        if llm_value:
                            updates.append({
                                'range': f"{column}28",
                                'values': [[str(llm_value)]]
                            })
                            break
                
                # Also copy GL premium to Premium Breakdown row 91
                for field_name in ["Total Premium (With/Without Terrorism)", "Total GL Premium", "Total Premium GL (With/Without Terrorism)"]:
                    if field_name in gl_data:
                        field_info = gl_data[field_name]
                        llm_value = field_info.get("llm_value", "") if isinstance(field_info, dict) else field_info
                        if llm_value:
                            updates.append({
                                'range': f"{column}91",
                                'values': [[str(llm_value)]]
                            })
                            break
                
                gl_updates_added = len(updates) - gl_updates_before
                print(f"    ✏ Added {gl_updates_added} GL fields to updates")
            else:
                if carrier.get('liabilityPDF'):
                    print(f"    ⚠️  GL PDF exists but no GL data loaded for {carrier_name}")
                else:
                    print(f"    - No GL PDF for {carrier_name}")
            
            # Process Property data
            if carrier.get('propertyPDF') and carrier_name in all_carrier_data and all_carrier_data[carrier_name].get('property'):
                property_data = all_carrier_data[carrier_name]['property']
                print(f"    ✏ Property data found: {len(property_data)} fields")
                prop_updates_before = len(updates)
                
                for field_name, row_num in property_field_rows.items():
                    if row_num == 80:
                        continue  # Handle Total Premium separately
                    
                    if field_name in property_data:
                        field_info = property_data[field_name]
                        llm_value = field_info.get("llm_value", "") if isinstance(field_info, dict) else field_info
                        if llm_value:
                            updates.append({
                                'range': f"{column}{row_num}",
                                'values': [[str(llm_value)]]
                            })
                
                # Handle row 80 (Total Property Premium)
                for field_name in ["Total Premium (With/Without Terrorism)", "Total Property Premium", "Total Premium Property (With/Without Terrorism)"]:
                    if field_name in property_data:
                        field_info = property_data[field_name]
                        llm_value = field_info.get("llm_value", "") if isinstance(field_info, dict) else field_info
                        if llm_value:
                            updates.append({
                                'range': f"{column}80",
                                'values': [[str(llm_value)]]
                            })
                            break
                
                # Also copy Property premium to Premium Breakdown row 92
                for field_name in ["Total Premium (With/Without Terrorism)", "Total Property Premium", "Total Premium Property (With/Without Terrorism)"]:
                    if field_name in property_data:
                        field_info = property_data[field_name]
                        llm_value = field_info.get("llm_value", "") if isinstance(field_info, dict) else field_info
                        if llm_value:
                            updates.append({
                                'range': f"{column}92",
                                'values': [[str(llm_value)]]
                            })
                            break
                
                prop_updates_added = len(updates) - prop_updates_before
                print(f"    ✏ Added {prop_updates_added} Property fields to updates")
            else:
                if carrier.get('propertyPDF'):
                    print(f"    ⚠️  Property PDF exists but no Property data loaded for {carrier_name}")
                else:
                    print(f"    - No Property PDF for {carrier_name}")
            
            # Process Liquor data
            if carrier.get('liquorPDF') and carrier_name in all_carrier_data and all_carrier_data[carrier_name].get('liquor'):
                liquor_data = all_carrier_data[carrier_name]['liquor']
                print(f"    ✏ Liquor data found: {len(liquor_data)} fields")
                liq_updates_before = len(updates)
                
                for field_name, row_num in liquor_field_rows.items():
                    if row_num == 42:
                        continue  # Handle Total Premium separately
                    
                    if field_name in liquor_data:
                        field_info = liquor_data[field_name]
                        llm_value = field_info.get("llm_value", "") if isinstance(field_info, dict) else field_info
                        if llm_value:
                            updates.append({
                                'range': f"{column}{row_num}",
                                'values': [[str(llm_value)]]
                            })
                
                # Handle row 42 (Total Liquor Premium)
                for field_name in ["Total Premium (With/Without Terrorism)", "Total Liquor Premium", "Liquor Premium", "Policy Premium", "Total Premium Liquor (With/Without Terrorism)"]:
                    if field_name in liquor_data:
                        field_info = liquor_data[field_name]
                        llm_value = field_info.get("llm_value", "") if isinstance(field_info, dict) else field_info
                        if llm_value:
                            updates.append({
                                'range': f"{column}42",
                                'values': [[str(llm_value)]]
                            })
                            break
                
                # Also copy Liquor premium to Premium Breakdown row 94
                for field_name in ["Total Premium (With/Without Terrorism)", "Total Liquor Premium", "Total Premium Liquor (With/Without Terrorism)"]:
                    if field_name in liquor_data:
                        field_info = liquor_data[field_name]
                        llm_value = field_info.get("llm_value", "") if isinstance(field_info, dict) else field_info
                        if llm_value:
                            updates.append({
                                'range': f"{column}94",
                                'values': [[str(llm_value)]]
                            })
                            break
                
                liq_updates_added = len(updates) - liq_updates_before
                print(f"    ✏ Added {liq_updates_added} Liquor fields to updates")
            else:
                if carrier.get('liquorPDF'):
                    print(f"    ⚠️  Liquor PDF exists but no Liquor data loaded for {carrier_name}")
                else:
                    print(f"    - No Liquor PDF for {carrier_name}")
            
            # Process Workers Comp data
            if carrier.get('workersCompPDF') and carrier_name in all_carrier_data and all_carrier_data[carrier_name].get('workerscomp'):
                workerscomp_data = all_carrier_data[carrier_name]['workerscomp']
                print(f"    ✏ Workers Comp data found: {len(workerscomp_data)} fields")
                wc_updates_before = len(updates)
                
                for field_name, row_num in workers_comp_field_rows.items():
                    if row_num == 90:
                        continue  # Handle Total Premium separately
                    
                    if field_name in workerscomp_data:
                        field_info = workerscomp_data[field_name]
                        llm_value = field_info.get("llm_value", "") if isinstance(field_info, dict) else field_info
                        if llm_value:
                            updates.append({
                                'range': f"{column}{row_num}",
                                'values': [[str(llm_value)]]
                            })
                
                # Handle row 90 (Total Workers Comp Premium)
                for field_name in ["Total Premium", "Workers Compensation Premium", "Policy Premium"]:
                    if field_name in workerscomp_data:
                        field_info = workerscomp_data[field_name]
                        llm_value = field_info.get("llm_value", "") if isinstance(field_info, dict) else field_info
                        if llm_value:
                            updates.append({
                                'range': f"{column}90",
                                'values': [[str(llm_value)]]
                            })
                            break
                
                # Also copy Workers Comp premium to Premium Breakdown row 95
                for field_name in ["Total Premium", "Workers Compensation Premium"]:
                    if field_name in workerscomp_data:
                        field_info = workerscomp_data[field_name]
                        llm_value = field_info.get("llm_value", "") if isinstance(field_info, dict) else field_info
                        if llm_value:
                            updates.append({
                                'range': f"{column}95",
                                'values': [[str(llm_value)]]
                            })
                            break
                
                wc_updates_added = len(updates) - wc_updates_before
                print(f"    ✏ Added {wc_updates_added} Workers Comp fields to updates")
            else:
                if carrier.get('workersCompPDF'):
                    print(f"    ⚠️  Workers Comp PDF exists but no Workers Comp data loaded for {carrier_name}")
                else:
                    print(f"    - No Workers Comp PDF for {carrier_name}")
        
        # STEP 4: Batch update all data
        print(f"\n  📈 Total updates prepared: {len(updates)}")
        if updates:
            # Show sample of what will be updated
            print(f"  📝 Sample updates (first 5):")
            for u in updates[:5]:
                print(f"    - {u['range']}: {u['values'][0][0][:50] if u['values'] and u['values'][0] else 'empty'}...")
            
            sheet.batch_update(updates)
            print(f"✅ Batch updated {len(updates)} fields to sheet")
        else:
            print("⚠️  No values to fill")
            print("  🔍 Debugging why updates are empty:")
            print(f"    - Carriers in metadata: {[c.get('carrierName') for c in carriers]}")
            print(f"    - Carriers with loaded data: {list(all_carrier_data.keys())}")
            for carrier in carriers:
                carrier_name = carrier.get('carrierName', 'Unknown')
                print(f"    - {carrier_name}:")
                print(f"      * Has liabilityPDF: {bool(carrier.get('liabilityPDF'))}")
                print(f"      * Has propertyPDF: {bool(carrier.get('propertyPDF'))}")
                print(f"      * Has liquorPDF: {bool(carrier.get('liquorPDF'))}")
                if carrier_name in all_carrier_data:
                    print(f"      * Loaded data: {list(all_carrier_data[carrier_name].keys())}")
                else:
                    print(f"      * ⚠️  No data loaded for this carrier")
        
        # Get Google Sheet URL
        sheet_url = f"https://docs.google.com/spreadsheets/d/{sheet.spreadsheet.id}/edit#gid={sheet.id}"
        
        print(f"\n{'='*80}")
        print(f"✅ FINALIZATION COMPLETE!")
        print(f"{'='*80}")
        print(f"✅ Upload ID: {upload_id}")
        print(f"✅ Carriers: {', '.join(carrier_names)}")
        print(f"✅ Fields updated: {len(updates)}")
        print(f"✅ Sheet: {sheet_name}")
        print(f"✅ User tab: {username}")
        print(f"🔗 Google Sheet URL: {sheet_url}")
        print(f"{'='*80}\n")
        
        has_property = any(all_carrier_data[c].get('property') for c in carrier_names if c in all_carrier_data)
        has_liability = any(all_carrier_data[c].get('liability') for c in carrier_names if c in all_carrier_data)
        has_liquor = any(all_carrier_data[c].get('liquor') for c in carrier_names if c in all_carrier_data)
        
        return {
            "success": True,
            "uploadId": upload_id,
            "carriers": carrier_names,
            "fieldsUpdated": len(updates),
            "sheetName": sheet_name,
            "username": username,
            "sheetUrl": sheet_url,
            "sections": {
                "property": has_property,
                "liability": has_liability,
                "liquor": has_liquor
            }
        }
        
    except Exception as e:
        print(f"\n❌ ERROR in finalize_upload_to_sheets: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
        return {
            "success": False,
            "error": str(e)
        }


if __name__ == "__main__":
    # For testing - read from local file
    import gspread
    from google.oauth2.service_account import Credentials
    
    data_path = "results/final_validated_fields.json"
    if os.path.exists(data_path):
        with open(data_path, 'r') as f:
            data = json.load(f)
        
        # Setup Google Sheets
        scope = [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
        ]
        
        creds_path = _get_credentials_path()
        creds = Credentials.from_service_account_file(creds_path, scopes=scope)
        client = gspread.authorize(creds)
        
        # Open the sheet
        sheet = client.open("Insurance Fields Data").sheet1
        sheet.clear()
        
        # Prepare data
        all_rows = [["Field Name", "LLM Value", "VLM Value", "Final Value", "Confidence", "Source Page"]]
        
        for field_name, field_data in data.items():
            row = [
                field_name,
                field_data.get('llm_value', 'null'),
                field_data.get('vlm_value', 'null'),
                field_data.get('final_value', 'null'),
                field_data.get('confidence', 'llm_only'),
                field_data.get('source_page', '')
            ]
            all_rows.append(row)
        
        update_response = sheet.update('A1', all_rows)  # Returns Response object
        print(f"✅ Pushed {len(all_rows)} rows to Google Sheets!")
    else:
        print(f"Error: {data_path} not found!")
