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
    # Try multiple paths
    possible_paths = [
        'credentials/insurance-sheets-474717-7fc3fd9736bc.json',
        '../credentials/insurance-sheets-474717-7fc3fd9736bc.json',
        '../insurance-sheets-474717-7fc3fd9736bc.json',
        'insurance-sheets-474717-7fc3fd9736bc.json'
    ]
    
    for path in possible_paths:
        if os.path.exists(path):
            return str(Path(path).resolve())
    
    # If not found, try to get from environment
    creds_path = os.getenv('GOOGLE_SHEETS_CREDENTIALS')
    if creds_path and os.path.exists(creds_path):
        return creds_path
    
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
            'liquor': None
        }
        
        # Check for property, liability, and liquor files
        for file_type in ['propertyPDF', 'liabilityPDF', 'liquorPDF']:
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
        
        # 4. Open/create sheet
        sheet = None
        try:
            print(f"🔍 Looking for sheet: {sheet_name}")
            sheet = client.open(sheet_name).sheet1
            print(f"✅ Opened existing sheet: {sheet_name}")
        except gspread.exceptions.SpreadsheetNotFound:
            print(f"⚠️  Sheet not found, trying alternative approach...")
            spreadsheets = client.openall()
            for ss in spreadsheets:
                if sheet_name.lower() in ss.title.lower():
                    sheet = ss.sheet1
                    print(f"✅ Found matching sheet: {ss.title}")
                    break
            
            if not sheet:
                print(f"📝 Creating new sheet: {sheet_name}")
                spreadsheet = client.create(sheet_name)
                sheet = spreadsheet.sheet1
                print(f"✅ Created new sheet: {sheet_name}")
        
        if not sheet:
            raise Exception(f"Could not open or create sheet '{sheet_name}'")
        
        # 5. Clear sheet ONCE
        sheet.clear()
        print("✅ Cleared existing data")
        
        # 6. Build complete layout
        print(f"\n📊 Building side-by-side layout...")
        all_rows = []
        
        # Company header
        all_rows.append(["Mckinney & Co. Insurance"])
        
        # PROPERTY SECTION
        has_property = any(all_carrier_data[c].get('property') for c in carrier_names if c in all_carrier_data)
        
        if has_property:
            print(f"  📋 Building Property section...")
            all_rows.append(["Property Coverages"])
            all_rows.append(["=" * 20])
            all_rows.append([])  # Spacing
            
            # Property column headers
            property_header = ["Field Name"]
            for carrier_name in carrier_names:
                property_header.extend([f"LLM Value ({carrier_name})", f"Source Page ({carrier_name})"])
            all_rows.append(property_header)
            
            # Property data rows
            property_fields = _get_all_unique_fields(all_carrier_data, carrier_names, 'property')
            print(f"    Found {len(property_fields)} unique property fields")
            
            for field_name in property_fields:
                row = [field_name]
                for carrier_name in carrier_names:
                    property_data = all_carrier_data.get(carrier_name, {}).get('property', {})
                    if property_data and field_name in property_data:
                        field_data = property_data[field_name]
                        row.extend([
                            field_data.get('llm_value', ''),
                            field_data.get('source_page', '')
                        ])
                    else:
                        row.extend(['', ''])
                all_rows.append(row)
            
            all_rows.append([])  # Spacing
            all_rows.append([])
        
        # LIABILITY SECTION
        has_liability = any(all_carrier_data[c].get('liability') for c in carrier_names if c in all_carrier_data)
        
        if has_liability:
            print(f"  📋 Building Liability section...")
            all_rows.append(["General Liability Coverages"])
            all_rows.append(["=" * 20])
            all_rows.append([])
            
            # Liability column headers
            liability_header = ["Field Name"]
            for carrier_name in carrier_names:
                liability_header.extend([f"LLM Value ({carrier_name})", f"Source Page ({carrier_name})"])
            all_rows.append(liability_header)
            
            # Liability data rows
            liability_fields = _get_all_unique_fields(all_carrier_data, carrier_names, 'liability')
            print(f"    Found {len(liability_fields)} unique liability fields")
            
            for field_name in liability_fields:
                row = [field_name]
                for carrier_name in carrier_names:
                    liability_data = all_carrier_data.get(carrier_name, {}).get('liability', {})
                    if liability_data and field_name in liability_data:
                        field_data = liability_data[field_name]
                        row.extend([
                            field_data.get('llm_value', ''),
                            field_data.get('source_page', '')
                        ])
                    else:
                        row.extend(['', ''])
                all_rows.append(row)
            
            all_rows.append([])  # Spacing
            all_rows.append([])
        
        # LIQUOR SECTION
        has_liquor = any(all_carrier_data[c].get('liquor') for c in carrier_names if c in all_carrier_data)
        
        if has_liquor:
            print(f"  📋 Building Liquor section...")
            all_rows.append(["Liquor/Bar Insurance Coverages"])
            all_rows.append(["=" * 20])
            all_rows.append([])
            
            # Liquor column headers
            liquor_header = ["Field Name"]
            for carrier_name in carrier_names:
                liquor_header.extend([f"LLM Value ({carrier_name})", f"Source Page ({carrier_name})"])
            all_rows.append(liquor_header)
            
            # Liquor data rows
            liquor_fields = _get_all_unique_fields(all_carrier_data, carrier_names, 'liquor')
            print(f"    Found {len(liquor_fields)} unique liquor fields")
            
            for field_name in liquor_fields:
                row = [field_name]
                for carrier_name in carrier_names:
                    liquor_data = all_carrier_data.get(carrier_name, {}).get('liquor', {})
                    if liquor_data and field_name in liquor_data:
                        field_data = liquor_data[field_name]
                        row.extend([
                            field_data.get('llm_value', ''),
                            field_data.get('source_page', '')
                        ])
                    else:
                        row.extend(['', ''])
                all_rows.append(row)
            
            all_rows.append([])  # Spacing
            all_rows.append([])
        
        # 7. Push EVERYTHING in ONE batch
        print(f"\n📤 Pushing {len(all_rows)} rows to Google Sheets...")
        update_response = sheet.update('A1', all_rows)
        print(f"✅ Google Sheets update response: {update_response}")
        
        # 8. Apply formatting (headers, colors, etc.)
        print(f"\n🎨 Applying formatting to headers...")
        _apply_sheet_formatting(sheet, all_rows, has_property, has_liability, has_liquor)
        print(f"✅ Formatting applied!")
        
        print(f"\n{'='*80}")
        print(f"✅ FINALIZATION COMPLETE!")
        print(f"{'='*80}")
        print(f"✅ Upload ID: {upload_id}")
        print(f"✅ Carriers: {', '.join(carrier_names)}")
        print(f"✅ Total rows: {len(all_rows)}")
        print(f"✅ Sheet: {sheet_name}")
        print(f"{'='*80}\n")
        
        return {
            "success": True,
            "uploadId": upload_id,
            "carriers": carrier_names,
            "rows": len(all_rows),
            "sheetName": sheet_name,
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
