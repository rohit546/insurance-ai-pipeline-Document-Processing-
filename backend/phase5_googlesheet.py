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
import time
from datetime import datetime
from typing import Dict, Any, List, Optional
from google.cloud import storage
from dotenv import load_dotenv
from pathlib import Path
from schemas.property_schema import PROPERTY_FIELDS_SCHEMA, get_field_names
from schemas.gl_schema import GL_FIELDS_SCHEMA, get_gl_field_names
from schemas.liquor_schema import LIQUOR_FIELDS_SCHEMA, get_liquor_field_names

load_dotenv()

BUCKET_NAME = os.getenv('BUCKET_NAME', 'mckinneysuite')
COVERSHEET_DATABASE_URL = os.getenv('COVERSHEET_DATABASE_URL')

# Google Sheets API rate limit: 60 write requests per minute per user
# We add delays between calls and retry on 429 errors
SHEETS_WRITE_DELAY = 2  # seconds between write API calls

def _sheets_api_call_with_retry(func, *args, max_retries=5, **kwargs):
    """
    Execute a Google Sheets API call with retry on rate limit (429) errors.
    Uses exponential backoff: 5s, 10s, 20s, 40s, 60s
    """
    for attempt in range(max_retries):
        try:
            result = func(*args, **kwargs)
            time.sleep(SHEETS_WRITE_DELAY)  # Rate limit protection
            return result
        except gspread.exceptions.APIError as e:
            if hasattr(e, 'response') and e.response.status_code == 429:
                wait_time = min(5 * (2 ** attempt), 65)  # 5, 10, 20, 40, 65 seconds
                print(f"  ⏳ Rate limited (429). Waiting {wait_time}s before retry {attempt + 1}/{max_retries}...")
                time.sleep(wait_time)
            else:
                raise
    # Final attempt without catching
    return func(*args, **kwargs)


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


class SheetBuilder:
    """
    Robust Google Sheets builder with row tracking and dynamic formatting.
    Builds sheets incrementally while tracking exact row positions.
    """
    
    def __init__(self, sheet, carriers: List[str]):
        self.sheet = sheet
        self.spreadsheet = sheet.spreadsheet
        self.carriers = carriers
        self.current_row = 0  # 0-indexed, will be incremented as we add rows
        self.format_requests = []  # Batch formatting requests
        self.all_rows = []  # Collect all rows for batch write
        
    def add_company_header(self, company_name: str = "Mckinney & Co. Insurance"):
        """Add company header row with green formatting"""
        self.all_rows.append([company_name])
        
        # Format company header (green background, white text, bold, centered)
        self.format_requests.append({
            'repeatCell': {
                'range': {
                    'sheetId': 0,
                    'startRowIndex': self.current_row,
                    'endRowIndex': self.current_row + 1,
                    'startColumnIndex': 0,
                    'endColumnIndex': max(len(self.carriers) * 2 + 1, 10)  # Dynamic columns
                },
                'cell': {
                    'userEnteredFormat': {
                        'backgroundColor': {'red': 0.2, 'green': 0.6, 'blue': 0.2},
                        'textFormat': {
                            'foregroundColor': {'red': 1.0, 'green': 1.0, 'blue': 1.0},
                            'bold': True,
                            'fontSize': 14
                        },
                        'horizontalAlignment': 'CENTER',
                        'wrapStrategy': 'WRAP'
                    }
                },
                'fields': 'userEnteredFormat'
            }
        })
        
        self.current_row += 1
        self.all_rows.append([])  # Empty row for spacing
        self.current_row += 1
        
    def add_section(self, section_name: str, field_schema: List, carrier_data: Dict[str, Dict[str, Any]]):
        """
        Add a complete section (Property/GL/Liquor) with schema-based ordering.
        
        Args:
            section_name: Display name for section (e.g., "Property Coverages")
            field_schema: Schema list (PROPERTY_FIELDS_SCHEMA, GL_FIELDS_SCHEMA, etc.)
            carrier_data: Dict[carrier_name][field_name] = field_data
        """
        # Section header row (black background, white text)
        self.all_rows.append([section_name])
        
        self.format_requests.append({
            'repeatCell': {
                'range': {
                    'sheetId': 0,
                    'startRowIndex': self.current_row,
                    'endRowIndex': self.current_row + 1,
                    'startColumnIndex': 0,
                    'endColumnIndex': max(len(self.carriers) * 2 + 1, 10)
                },
                'cell': {
                    'userEnteredFormat': {
                        'backgroundColor': {'red': 0.0, 'green': 0.0, 'blue': 0.0},
                        'textFormat': {
                            'foregroundColor': {'red': 1.0, 'green': 1.0, 'blue': 1.0},
                            'bold': True,
                            'fontSize': 13
                        },
                        'horizontalAlignment': 'CENTER'
                    }
                },
                'fields': 'userEnteredFormat'
            }
        })
        
        self.current_row += 1
        self.all_rows.append([])  # Empty row for spacing
        self.current_row += 1
        
        # Column headers (gray background, bold)
        headers = ["Field Name"]
        for carrier in self.carriers:
            headers.extend([f"LLM Value ({carrier})", f"Source Page ({carrier})"])
        self.all_rows.append(headers)
        
        self.format_requests.append({
            'repeatCell': {
                'range': {
                    'sheetId': 0,
                    'startRowIndex': self.current_row,
                    'endRowIndex': self.current_row + 1,
                    'startColumnIndex': 0,
                    'endColumnIndex': len(headers)
                },
                'cell': {
                    'userEnteredFormat': {
                        'backgroundColor': {'red': 0.9, 'green': 0.9, 'blue': 0.9},
                        'textFormat': {'bold': True, 'fontSize': 11},
                        'horizontalAlignment': 'LEFT',
                        'wrapStrategy': 'WRAP'
                    }
                },
                'fields': 'userEnteredFormat'
            }
        })
        
        self.current_row += 1
        
        # Data rows (using SCHEMA ORDER - guaranteed consistency!)
        for field_def in field_schema:
            row = [field_def.name]
            
            for carrier in self.carriers:
                carrier_fields = carrier_data.get(carrier, {})
                if carrier_fields and field_def.name in carrier_fields:
                    field_data = carrier_fields[field_def.name]
                    llm_value = field_data.get('llm_value', '')
                    source_page = field_data.get('source_page', '')
                    
                    # Handle None values
                    if llm_value is None:
                        llm_value = ''
                    if source_page is None:
                        source_page = ''
                    
                    row.extend([str(llm_value), str(source_page)])
                else:
                    row.extend(['', ''])
            
            self.all_rows.append(row)
            self.current_row += 1
        
        # Add spacing after section
        self.all_rows.append([])
        self.current_row += 1
        self.all_rows.append([])
        self.current_row += 1
    
    def write_all_data(self):
        """Write all collected rows to sheet in one batch"""
        if self.all_rows:
            print(f"📤 Writing {len(self.all_rows)} rows to Google Sheets...")
            self.sheet.update('A1', self.all_rows)
            print(f"✅ Data written successfully!")
    
    def apply_all_formatting(self):
        """Apply all collected formatting requests in one batch"""
        if self.format_requests:
            print(f"🎨 Applying {len(self.format_requests)} formatting requests...")
            self.spreadsheet.batch_update({'requests': self.format_requests})
            print(f"✅ Formatting applied successfully!")


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


def _get_field(data: dict, field_name: str) -> str:
    """Extract llm_value from a field dict, or the value directly if it's a string."""
    if not data or field_name not in data:
        return ""
    val = data[field_name]
    if isinstance(val, dict):
        return str(val.get("llm_value", "") or "")
    return str(val or "")


def _build_extracted_data(carriers: list, all_carrier_data: dict) -> dict:
    """
    Build a flat extractedData structure from all_carrier_data for frontend DB storage.
    Maps internal field names → camelCase keys matching the frontend schema.
    """
    carrier_list = []

    for carrier in carriers:
        carrier_name = carrier.get("carrierName", "Unknown")
        cd = all_carrier_data.get(carrier_name, {})
        gl = cd.get("liability") or {}
        prop = cd.get("property") or {}
        liq = cd.get("liquor") or {}
        wc = cd.get("workerscomp") or {}

        carrier_list.append({
            "carrierName": carrier_name,

            # General Liability
            "glEachOccurrenceLimits":        _get_field(gl, "Each Occurrence/General Aggregate Limits"),
            "glLiabilityDeductible":         _get_field(gl, "Liability Deductible - Per claim or Per Occ basis"),
            "glHiredAutoNonOwned":           _get_field(gl, "Hired Auto And Non-Owned Auto Liability - Without Delivery Service"),
            "glFuelContamination":           _get_field(gl, "Fuel Contamination coverage limits"),
            "glVandalism":                   _get_field(gl, "Vandalism coverage"),
            "glGarageKeepers":               _get_field(gl, "Garage Keepers Liability"),
            "glEmploymentPractices":         _get_field(gl, "Employment Practices Liability"),
            "glAbuseMolestation":            _get_field(gl, "Abuse & Molestation Coverage limits"),
            "glAssaultBattery":              _get_field(gl, "Assault & Battery Coverage limits"),
            "glFirearmsActiveAssailant":     _get_field(gl, "Firearms/Active Assailant Coverage limits"),
            "glAdditionalInsured":           _get_field(gl, "Additional Insured"),
            "glAdditionalInsuredMortgagee":  _get_field(gl, "Additional Insured (Mortgagee)"),
            "glAdditionalInsuredJobber":     _get_field(gl, "Additional insured - Jobber"),
            "glExposure":                    _get_field(gl, "Exposure"),
            "glRatingBasis":                 _get_field(gl, "Rating basis: If Sales - Subject to Audit"),
            "glTerrorism":                   _get_field(gl, "Terrorism"),
            "glPersonalAdvertisingInjury":   _get_field(gl, "Personal and Advertising Injury Limit"),
            "glProductsCompletedOps":        _get_field(gl, "Products/Completed Operations Aggregate Limit"),
            "glMinimumEarned":               _get_field(gl, "Minimum Earned"),
            "glGeneralLiabilityPremium":     _get_field(gl, "General Liability Premium"),
            "glTotalPremium":                _get_field(gl, "Total Premium (With/Without Terrorism)"),
            "glPolicyPremium":               _get_field(gl, "Policy Premium"),
            "glContaminatedFuel":            _get_field(gl, "Contaminated fuel"),
            "glLiquorLiability":             _get_field(gl, "Liquor Liability"),
            "glAdditionalInsuredManagers":   _get_field(gl, "Additional Insured - Managers Or Lessors Of Premises"),

            # Liquor Liability
            "llEachOccurrenceLimits":        _get_field(liq, "Each Occurrence/General Aggregate Limits"),
            "llSalesSubjectAudit":           _get_field(liq, "Sales - Subject to Audit"),
            "llAssaultBatteryFirearms":      _get_field(liq, "Assault & Battery/Firearms/Active Assailant"),
            "llRequirements":                _get_field(liq, "Requirements"),
            "llSubjectivities":              _get_field(liq, "If any subjectivities in quote please add"),
            "llMinimumEarned":               _get_field(liq, "Minimum Earned"),
            "llTotalPremium":                _get_field(liq, "Total Premium (With/Without Terrorism)") or _get_field(liq, "Liquor Premium"),

            # Property Coverages
            "propConstructionType":          _get_field(prop, "Construction Type"),
            "propValuationCoinsurance":      _get_field(prop, "Valuation and Coinsurance"),
            "propCosmeticDamage":            _get_field(prop, "Cosmetic Damage"),
            "propBuilding":                  _get_field(prop, "Building"),
            "propPumps":                     _get_field(prop, "Pumps"),
            "propCanopy":                    _get_field(prop, "Canopy"),
            "propRoofSurfacing":             _get_field(prop, "Roof Surfacing"),
            "propRoofSurfacingLimitation":   _get_field(prop, "Roof Surfacing -Limitation"),
            "propBusinessPersonalProperty":  _get_field(prop, "Business Personal Property"),
            "propBusinessIncome":            _get_field(prop, "Business Income"),
            "propBusinessIncomeExtraExpense":_get_field(prop, "Business Income with Extra Expense"),
            "propEquipmentBreakdown":        _get_field(prop, "Equipment Breakdown"),
            "propOutdoorSigns":              _get_field(prop, "Outdoor Signs"),
            "propSignsWithin1000ft":         _get_field(prop, "Signs Within 1,000 Feet to Premises"),
            "propEmployeeDishonesty":        _get_field(prop, "Employee Dishonesty"),
            "propMoneySecurities":           _get_field(prop, "Money & Securities"),
            "propMoneySecuritiesInsideOutside": _get_field(prop, "Money and Securities (Inside; Outside)"),
            "propSpoilage":                  _get_field(prop, "Spoilage"),
            "propTheft":                     _get_field(prop, "Theft"),
            "propTheftSublimit":             _get_field(prop, "Theft Sublimit"),
            "propTheftDeductible":           _get_field(prop, "Theft Deductible"),
            "propWindstormHailDeductible":   _get_field(prop, "Windstorm or Hail Deductible"),
            "propNamedStormDeductible":      _get_field(prop, "Named Storm Deductible"),
            "propWindHailNamedStormExclusion": _get_field(prop, "Wind and Hail and Named Storm exclusion"),
            "propAllOtherPerilsDeductible":  _get_field(prop, "All Other Perils Deductible"),
            "propFireStationAlarm":          _get_field(prop, "Fire Station Alarm"),
            "propBurglarAlarm":              _get_field(prop, "Burglar Alarm"),
            "propLossPayee":                 _get_field(prop, "Loss Payee"),
            "propFormsExclusions":           _get_field(prop, "Forms and Exclusions"),
            "propProtectiveSafeguards":      _get_field(prop, "Requirement: Protective Safeguards"),
            "propTerrorism":                 _get_field(prop, "Terrorism"),
            "propSubjectivity":              _get_field(prop, "Subjectivity:"),
            "propMinimumEarned":             _get_field(prop, "Minimum Earned"),
            "propTotalPremium":              _get_field(prop, "Total Premium (With/Without Terrorism)"),

            # Workers Compensation
            "wcLimits":                      _get_field(wc, "Limits"),
            "wcFein":                        _get_field(wc, "FEIN #"),
            "wcPayrollSubjectAudit":         _get_field(wc, "Payroll - Subject to Audit"),
            "wcIncludedExcludedOfficers":    _get_field(wc, "Excluded Officer"),
            "wcTotalPremium":                _get_field(wc, "Total Premium") or _get_field(wc, "Workers Compensation Premium"),

            # Premium Breakdown
            "premiumGl":           _get_field(gl, "Total Premium (With/Without Terrorism)") or _get_field(gl, "General Liability Premium"),
            "premiumProperty":     _get_field(prop, "Total Premium (With/Without Terrorism)"),
            "premiumLiquor":       _get_field(liq, "Total Premium (With/Without Terrorism)") or _get_field(liq, "Liquor Premium"),
            "premiumWc":           _get_field(wc, "Total Premium") or _get_field(wc, "Workers Compensation Premium"),
        })

    return {"carriers": carrier_list}


def save_summary_to_database(
    submission_id: str,
    upload_id: str,
    created_by: str,
    sheet_url: str,
    extracted_data: Dict[str, Any]
) -> Optional[str]:
    """
    Save extracted summary data to the coversheet app's PostgreSQL database.
    Only called when submissionId is present (uploads from the other app).
    
    Populates individual JSONB columns (one per field) with carrier-keyed values,
    e.g. gl_each_occurrence_limits = {"AAA": "$1,000,000 / $2,000,000"}
    
    Returns the row id on success, None on failure.
    """
    if not COVERSHEET_DATABASE_URL:
        print("⚠️  COVERSHEET_DATABASE_URL not set, skipping DB save")
        return None
    
    try:
        import psycopg2
        
        conn = psycopg2.connect(COVERSHEET_DATABASE_URL)
        cur = conn.cursor()
        
        # Get carrier names from extracted data
        carriers = extracted_data.get('carriers', [])
        carrier_1_name = carriers[0].get('carrierName') if len(carriers) > 0 else None
        carrier_2_name = carriers[1].get('carrierName') if len(carriers) > 1 else None
        carrier_3_name = carriers[2].get('carrierName') if len(carriers) > 2 else None
        
        # Map camelCase extractedData keys → snake_case DB columns
        # Each JSONB column stores {carrierName: value} for all carriers
        FIELD_TO_COLUMN = {
            # General Liability
            "glEachOccurrenceLimits":       "gl_each_occurrence_limits",
            "glLiabilityDeductible":        "gl_liability_deductible",
            "glHiredAutoNonOwned":          "gl_hired_auto_non_owned",
            "glFuelContamination":          "gl_fuel_contamination",
            "glVandalism":                  "gl_vandalism",
            "glGarageKeepers":              "gl_garage_keepers",
            "glEmploymentPractices":        "gl_employment_practices",
            "glAbuseMolestation":           "gl_abuse_molestation",
            "glAssaultBattery":             "gl_assault_battery",
            "glFirearmsActiveAssailant":    "gl_firearms_active_assailant",
            "glAdditionalInsured":          "gl_additional_insured",
            "glAdditionalInsuredMortgagee": "gl_additional_insured_mortgagee",
            "glAdditionalInsuredJobber":    "gl_additional_insured_jobber",
            "glExposure":                   "gl_exposure",
            "glRatingBasis":                "gl_rating_basis",
            "glTerrorism":                  "gl_terrorism",
            "glPersonalAdvertisingInjury":  "gl_personal_advertising_injury",
            "glProductsCompletedOps":       "gl_products_completed_ops",
            "glMinimumEarned":              "gl_minimum_earned",
            "glTotalPremium":               "gl_total_premium",
            "glPolicyPremium":              "gl_policy_premium",
            "glContaminatedFuel":           "gl_contaminated_fuel",
            # Liquor Liability
            "llEachOccurrenceLimits":       "ll_each_occurrence_limits",
            "llSalesSubjectAudit":          "ll_sales_subject_audit",
            "llAssaultBatteryFirearms":     "ll_assault_battery_firearms",
            "llRequirements":               "ll_requirements",
            "llSubjectivities":             "ll_subjectivities",
            "llMinimumEarned":              "ll_minimum_earned",
            "llTotalPremium":               "ll_total_premium",
            # Property
            "propConstructionType":         "prop_construction_type",
            "propValuationCoinsurance":     "prop_valuation_coinsurance",
            "propCosmeticDamage":           "prop_cosmetic_damage",
            "propBuilding":                 "prop_building",
            "propPumps":                    "prop_pumps",
            "propCanopy":                   "prop_canopy",
            "propRoofSurfacing":            "prop_roof_surfacing",
            "propRoofSurfacingLimitation":  "prop_roof_surfacing_limitation",
            "propBusinessPersonalProperty": "prop_business_personal_property",
            "propBusinessIncome":           "prop_business_income",
            "propBusinessIncomeExtraExpense": "prop_business_income_extra_expense",
            "propEquipmentBreakdown":       "prop_equipment_breakdown",
            "propOutdoorSigns":             "prop_outdoor_signs",
            "propSignsWithin1000ft":        "prop_signs_within_1000ft",
            "propEmployeeDishonesty":       "prop_employee_dishonesty",
            "propMoneySecurities":          "prop_money_securities",
            "propMoneySecuritiesInsideOutside": "prop_money_securities_inside_outside",
            "propSpoilage":                 "prop_spoilage",
            "propTheft":                    "prop_theft",
            "propTheftSublimit":            "prop_theft_sublimit",
            "propTheftDeductible":          "prop_theft_deductible",
            "propWindstormHailDeductible":  "prop_windstorm_hail_deductible",
            "propNamedStormDeductible":     "prop_named_storm_deductible",
            "propWindHailNamedStormExclusion": "prop_wind_hail_named_storm_exclusion",
            "propAllOtherPerilsDeductible": "prop_all_other_perils_deductible",
            "propFireStationAlarm":         "prop_fire_station_alarm",
            "propBurglarAlarm":             "prop_burglar_alarm",
            "propLossPayee":               "prop_loss_payee",
            "propFormsExclusions":         "prop_forms_exclusions",
            "propProtectiveSafeguards":    "prop_protective_safeguards",
            "propTerrorism":               "prop_terrorism",
            "propSubjectivity":            "prop_subjectivity",
            "propMinimumEarned":           "prop_minimum_earned",
            "propTotalPremium":            "prop_total_premium",
            # Workers Compensation
            "wcLimits":                    "wc_limits",
            "wcFein":                      "wc_fein",
            "wcPayrollSubjectAudit":       "wc_payroll_subject_audit",
            "wcIncludedExcludedOfficers":  "wc_included_excluded_officers",
            "wcTotalPremium":              "wc_total_premium",
            # Premium Breakdown
            "premiumGl":                   "premium_gl",
            "premiumProperty":             "premium_property",
            "premiumLiquor":               "premium_liquor",
            "premiumWc":                   "premium_wc",
        }
        
        # Build per-column JSONB values: {carrierName: fieldValue} across all carriers
        column_values = {}
        for carrier in carriers:
            cname = carrier.get('carrierName', 'Unknown')
            for camel_key, db_col in FIELD_TO_COLUMN.items():
                val = carrier.get(camel_key, '')
                if val:  # Only include non-empty values
                    if db_col not in column_values:
                        column_values[db_col] = {}
                    column_values[db_col][cname] = val
        
        # Build dynamic INSERT with all populated columns
        base_columns = [
            'submission_id', 'upload_id', 'sheet_url', 'created_by',
            'carrier_1_name', 'carrier_2_name', 'carrier_3_name',
            'raw_ai_response'
        ]
        base_values = [
            submission_id, upload_id, sheet_url, created_by,
            carrier_1_name, carrier_2_name, carrier_3_name,
            json.dumps(extracted_data)
        ]
        
        # Add all field columns that have data
        field_columns = []
        field_values = []
        for db_col, val_dict in column_values.items():
            field_columns.append(db_col)
            field_values.append(json.dumps(val_dict))
        
        all_columns = base_columns + field_columns
        all_values = base_values + field_values
        
        placeholders = ', '.join(['%s'] * len(all_values))
        columns_str = ', '.join(all_columns)
        
        # Build ON CONFLICT UPDATE for all columns
        update_parts = []
        for col in all_columns:
            if col != 'submission_id':  # Don't update the conflict key
                update_parts.append(f"{col} = EXCLUDED.{col}")
        update_parts.append("updated_at = NOW()")
        update_str = ', '.join(update_parts)
        
        sql = f"""
            INSERT INTO submission_summaries ({columns_str})
            VALUES ({placeholders})
            ON CONFLICT (submission_id) 
            DO UPDATE SET {update_str}
            RETURNING id
        """
        
        cur.execute(sql, all_values)
        
        result = cur.fetchone()
        conn.commit()
        cur.close()
        conn.close()
        
        row_id = str(result[0]) if result else None
        field_count = len(field_columns)
        print(f"✅ Saved summary to coversheet DB (id: {row_id}, {field_count} field columns populated)")
        return row_id
        
    except Exception as e:
        print(f"⚠️  Failed to save to coversheet DB: {e}")
        import traceback
        traceback.print_exc()
        return None


def finalize_upload_to_sheets(upload_id: str, sheet_name: str = "Insurance Fields Data") -> Dict[str, Any]:
    """
    Finalize upload: Load ALL carriers from this upload, build side-by-side layout, push ONCE.
    This prevents individual carriers from overwriting each other.
    
    Layout:
    - Company Header: "Mckinney & Co. Insurance"
    - Property Section: All carriers side-by-side
    - Liability Section: All carriers side-by-side (if any)
    - Liquor Section: All carriers side-by-side (if any)
    
    Each section has:
    - Section header (e.g., "Property Coverages")
    - Column headers: Field Name | LLM Value (Carrier1) | Source Page (Carrier1) | ...
    - Data rows: Field values for each carrier
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
    
    # 2. Load ALL carrier data (property + liability + liquor)
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
        
        # 6. Use the SAME row mappings and logic as phase3_llm.py
        import json
        
        # GL Field to Row mapping (same as phase3_llm.py)
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
        
        # Property Field to Row mapping (same as phase3_llm.py)
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
          
        # Liquor Field to Row mapping (same as phase3_llm.py)
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
        
        # Workers Comp Field to Row mapping (same as phase3_llm.py)
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
        
        # STEP 1: Clear old data from all columns (B, C, D)
        # PRESERVE: Row 70-75 Column B (merged cells with template text)
        print("  🧹 Clearing old data from columns B, C, D...")
        clear_ranges = []
        for col in columns:
            clear_ranges.append(f"{col}8:{col}32")   # GL rows
            clear_ranges.append(f"{col}36:{col}42")  # Liquor rows
            
            # Property rows - but preserve specific cells in Column B
            if col == 'B':
                # Column B: Clear 46-69, 76-79, 80 (skip 70-75 to preserve merged cells with template text)
                clear_ranges.append(f"{col}46:{col}69")  # 46-69
                # SKIP: B70-B75 (preserve merged cells: Wind/Hail exclusion, Fire Alarm, Burglar Alarm)
                clear_ranges.append(f"{col}76:{col}79")  # 76-79
                clear_ranges.append(f"{col}80:{col}80")  # Row 80
            else:
                # Columns C and D: Clear all 46-80
                clear_ranges.append(f"{col}46:{col}80")
            
            clear_ranges.append(f"{col}86:{col}90")  # Workers Comp rows
            clear_ranges.append(f"{col}91:{col}97")  # Premium Breakdown rows
        
        if clear_ranges:
            _sheets_api_call_with_retry(sheet.batch_clear, clear_ranges)
        print("  ✅ Cleared old data (preserved B70-B75 merged cells with template text)")
        
        # STEP 1.5: Update column headers (Row 7) with actual carrier names
        print("  📝 Updating column headers with carrier names...")
        header_updates = []
        
        # Row 7 has headers: Column A = "Liability Coverages", Columns B/C/D = "Option 1/2/3"
        # We'll update columns B, C, D (which correspond to carrier indices 0, 1, 2)
        for carrier_index, carrier in enumerate(carriers):
            if carrier_index >= 3:  # Max 3 carriers
                break
            
            carrier_name = carrier.get('carrierName', f'Option {carrier_index + 1}')
            column = columns[carrier_index]  # B, C, or D
            
            header_updates.append({
                'range': f"{column}7",
                'values': [[carrier_name]]
            })
            print(f"    ✓ Row 7, Column {column}: '{carrier_name}'")
        
        if header_updates:
            _sheets_api_call_with_retry(sheet.batch_update, header_updates)
            print(f"  ✅ Updated {len(header_updates)} column headers in row 7")
        else:
            print("  ⚠️  No carrier names to update")
        
        # STEP 2: Fill company information (rows 2-6)
        print("  📝 Filling company information...")
        # Extract company info from first available carrier
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
                for f in phase3_files[:10]:  # Show first 10
                    print(f"    - {f.name}")
            else:
                print(f"  ⚠️  No Phase 3 files found with prefix: phase3/results/{upload_id}")
                # Check if Phase 3 ran at all
                all_phase3 = list(bucket.list_blobs(prefix='phase3/results/'))
                recent_files = sorted(all_phase3, key=lambda x: x.time_created, reverse=True)[:5]
                if recent_files:
                    print(f"  📂 Recent Phase 3 files (last 5):")
                    for f in recent_files:
                        print(f"    - {f.name} (created: {f.time_created})")
        
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
                _sheets_api_call_with_retry(sheet.batch_update, company_updates)
                print(f"  ✅ Filled {len(company_updates)} company info rows")
        else:
            print("  ⚠️  No company info to fill")
        
        # STEP 3: Fill data for each carrier (same logic as phase3_llm.py)
        updates = []
        
        print(f"  📊 Preparing field updates for {len(carriers)} carriers...")
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
                print(f"    ✓ GL data found: {len(gl_data)} fields")
                gl_updates_before = len(updates)
                
                for field_name, row_num in gl_field_rows.items():
                    if row_num == 28:
                        continue  # Handle separately
                    
                    if field_name in gl_data:
                        field_info = gl_data[field_name]
                        llm_value = field_info.get("llm_value", "") if isinstance(field_info, dict) else field_info
                        if llm_value:
                            updates.append({
                                'range': f"{column}{row_num}",
                                'values': [[str(llm_value)]]
                            })
                
                # Handle row 28 (Total Premium)
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
                
                # Also copy to Premium Breakdown row 91
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
                print(f"    ✓ Added {gl_updates_added} GL fields to updates")
            else:
                if carrier.get('liabilityPDF'):
                    print(f"    ⚠️  GL PDF exists but no GL data loaded for {carrier_name}")
                else:
                    print(f"    - No GL PDF for {carrier_name}")
            
            # Process Property data
            if carrier.get('propertyPDF') and carrier_name in all_carrier_data and all_carrier_data[carrier_name].get('property'):
                property_data = all_carrier_data[carrier_name]['property']
                print(f"    ✓ Property data found: {len(property_data)} fields")
                prop_updates_before = len(updates)
                
                for field_name, row_num in property_field_rows.items():
                    if row_num == 80:
                        continue  # Handle separately
                    
                    if field_name in property_data:
                        field_info = property_data[field_name]
                        llm_value = field_info.get("llm_value", "") if isinstance(field_info, dict) else field_info
                        if llm_value:
                            updates.append({
                                'range': f"{column}{row_num}",
                                'values': [[str(llm_value)]]
                            })
                
                # Handle row 80 (Total Premium)
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
                
                # Also copy to Premium Breakdown row 92
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
                print(f"    ✓ Added {prop_updates_added} Property fields to updates")
            else:
                if carrier.get('propertyPDF'):
                    print(f"    ⚠️  Property PDF exists but no Property data loaded for {carrier_name}")
                else:
                    print(f"    - No Property PDF for {carrier_name}")
            
            # Process Liquor data
            if carrier.get('liquorPDF') and carrier_name in all_carrier_data and all_carrier_data[carrier_name].get('liquor'):
                liquor_data = all_carrier_data[carrier_name]['liquor']
                print(f"    ✓ Liquor data found: {len(liquor_data)} fields")
                liq_updates_before = len(updates)
                
                for field_name, row_num in liquor_field_rows.items():
                    if row_num == 42:
                        continue  # Handle separately
                    
                    if field_name in liquor_data:
                        field_info = liquor_data[field_name]
                        llm_value = field_info.get("llm_value", "") if isinstance(field_info, dict) else field_info
                        if llm_value:
                            updates.append({
                                'range': f"{column}{row_num}",
                                'values': [[str(llm_value)]]
                            })
                
                # Handle row 42 (Total Premium)
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
                
                # Also copy to Premium Breakdown row 94
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
                print(f"    ✓ Added {liq_updates_added} Liquor fields to updates")
            else:
                if carrier.get('liquorPDF'):
                    print(f"    ⚠️  Liquor PDF exists but no Liquor data loaded for {carrier_name}")
                else:
                    print(f"    - No Liquor PDF for {carrier_name}")
            
            # Process Workers Comp data
            if carrier.get('workersCompPDF') and carrier_name in all_carrier_data and all_carrier_data[carrier_name].get('workerscomp'):
                workerscomp_data = all_carrier_data[carrier_name]['workerscomp']
                print(f"    ✓ Workers Comp data found: {len(workerscomp_data)} fields")
                wc_updates_before = len(updates)
                
                for field_name, row_num in workers_comp_field_rows.items():
                    if row_num == 90:
                        continue  # Handle separately
                    
                    if field_name in workerscomp_data:
                        field_info = workerscomp_data[field_name]
                        llm_value = field_info.get("llm_value", "") if isinstance(field_info, dict) else field_info
                        if llm_value:
                            updates.append({
                                'range': f"{column}{row_num}",
                                'values': [[str(llm_value)]]
                            })
                
                # Handle row 90 (Total Premium) - multiple field names map here
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
                
                # Also copy to Premium Breakdown row 95 (Workers Comp)
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
                print(f"    ✓ Added {wc_updates_added} Workers Comp fields to updates")
            else:
                if carrier.get('workersCompPDF'):
                    print(f"    ⚠️  Workers Comp PDF exists but no Workers Comp data loaded for {carrier_name}")
                else:
                    print(f"    - No Workers Comp PDF for {carrier_name}")
        
        # STEP 4: Batch update all data
        print(f"\n  📊 Total updates prepared: {len(updates)}")
        if updates:
            # Show sample of what will be updated
            print(f"  📝 Sample updates (first 5):")
            for u in updates[:5]:
                print(f"    - {u['range']}: {u['values'][0][0][:50] if u['values'] and u['values'][0] else 'empty'}...")
            
            _sheets_api_call_with_retry(sheet.batch_update, updates)
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
        
        # Build flat extractedData for frontend DB storage
        extracted_data = _build_extracted_data(carriers, all_carrier_data)
        
        # Save to coversheet app's PostgreSQL database (if submissionId provided)
        submission_id = upload_record.get('submissionId')
        created_by = upload_record.get('createdBy', username)
        db_row_id = None
        
        if submission_id:
            print(f"\n📦 Saving to coversheet database (submissionId: {submission_id})...")
            db_row_id = save_summary_to_database(
                submission_id=submission_id,
                upload_id=upload_id,
                created_by=created_by,
                sheet_url=sheet_url,
                extracted_data=extracted_data
            )
        else:
            print(f"\nℹ️  No submissionId in metadata, skipping coversheet DB save")
        
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
            },
            "extractedData": extracted_data,
            "dbSaved": db_row_id is not None,
            "submissionId": submission_id
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
