"""
Phase 2D: Intelligent Combination
Combines the best text from each page based on smart selection results
from Phase 2C, creating the final combined file for LLM extraction.
Works with Google Cloud Storage.
"""
import json
import os
import re
from datetime import datetime
from typing import Dict, Any, List
from google.cloud import storage
from dotenv import load_dotenv

load_dotenv()

BUCKET_NAME = os.getenv('BUCKET_NAME', 'deployment')


def _get_bucket() -> storage.bucket.Bucket:
    client = storage.Client()
    return client.bucket(BUCKET_NAME)


def _download_text_from_gcs(bucket: storage.bucket.Bucket, blob_path: str) -> str:
    """Download text file from GCS"""
    blob = bucket.blob(blob_path)
    if not blob.exists():
        return ""
    return blob.download_as_string().decode('utf-8')


def _upload_text_to_gcs(bucket: storage.bucket.Bucket, blob_path: str, content: str) -> None:
    """Upload text file to GCS"""
    blob = bucket.blob(blob_path)
    blob.upload_from_string(content, content_type='text/plain')
    print(f"✅ Uploaded to: gs://{BUCKET_NAME}/{blob_path}")


def _blob_path_from_gs_uri(gs_uri: str) -> str:
    prefix = f"gs://{BUCKET_NAME}/"
    if gs_uri.startswith(prefix):
        return gs_uri[len(prefix):]
    return gs_uri


def read_smart_selection_results_from_gcs(bucket: storage.bucket.Bucket, file_path: str) -> Dict[str, Any]:
    """Read smart selection results from GCS JSON file"""
    try:
        content = _download_text_from_gcs(bucket, file_path)
        if not content:
            print(f"Warning: Smart selection results not found at {file_path}")
            return {}
        
        selection_results = json.loads(content)
        print(f"Found selection results for {len(selection_results)} pages from {file_path}")
        return selection_results
    except Exception as e:
        print(f"Error reading smart selection results: {e}")
        return {}


def read_pymupdf_clean_pages_from_gcs(bucket: storage.bucket.Bucket, file_path: str) -> Dict[int, str]:
    """Read PyMuPDF clean pages from GCS"""
    try:
        content = _download_text_from_gcs(bucket, file_path)
        if not content:
            print(f"Warning: PyMuPDF clean pages not found at {file_path}")
            return {}
        
        clean_pages = {}
        # Extract clean pages
        page_sections = re.findall(r'PAGE (\d+):.*?TEXT CONTENT:\n(.*?)\n={80}', content, re.DOTALL)
        
        for page_num, page_text in page_sections:
            clean_pages[int(page_num)] = page_text.strip()
        
        print(f"Found {len(clean_pages)} PyMuPDF clean pages from {file_path}: {list(clean_pages.keys())}")
        return clean_pages
    except Exception as e:
        print(f"Error reading PyMuPDF clean pages: {e}")
        return {}


def read_ocr_all_pages_from_gcs(bucket: storage.bucket.Bucket, file_path: str) -> Dict[int, str]:
    """Read OCR all pages from GCS"""
    try:
        content = _download_text_from_gcs(bucket, file_path)
        if not content:
            print(f"Warning: OCR results not found at {file_path}")
            return {}
        
        ocr_pages = {}
        # Extract OCR pages
        page_sections = re.findall(r'PAGE (\d+):.*?OCR EXTRACTED TEXT:.*?----------------------------------------\n(.*?)\n={80}', content, re.DOTALL)
        
        for page_num, page_text in page_sections:
            ocr_pages[int(page_num)] = page_text.strip()
        
        print(f"Found {len(ocr_pages)} OCR pages from {file_path}: {list(ocr_pages.keys())}")
        return ocr_pages
    except Exception as e:
        print(f"Error reading OCR pages: {e}")
        return {}


def create_intelligent_combined_file(
    bucket: storage.bucket.Bucket,
    selection_results: Dict[str, Any],
    pymupdf_pages: Dict[int, str],
    ocr_pages: Dict[int, str],
    carrier_name: str,
    safe_carrier_name: str,
    file_type: str,
    timestamp: str
) -> str:
    """Create final combined file with best text from each page"""
    type_short = file_type.replace('PDF', '').lower()
    combined_file_path = f'phase2d/results/{safe_carrier_name}_{type_short}_intelligent_combined_{timestamp}.txt'
    
    print("PHASE 2D: INTELLIGENT COMBINING")
    print("=" * 80)
    
    report_lines = []
    report_lines.append("INTELLIGENT COMBINED PDF EXTRACTION RESULTS - ALL PAGES")
    report_lines.append("=" * 80)
    report_lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    report_lines.append(f"Carrier: {carrier_name}")
    report_lines.append(f"File Type: {file_type.upper()}")
    report_lines.append(f"Method: Smart LLM Selection + Intelligent Combining")
    report_lines.append(f"Total Pages: {len(selection_results)}")
    report_lines.append("=" * 80)
    report_lines.append("")
    
    # Count selections by source
    pymupdf_count = len([s for s in selection_results.values() if s['selected_source'] == 'PyMuPDF'])
    ocr_count = len([s for s in selection_results.values() if s['selected_source'] == 'OCR'])
    
    report_lines.append("INTELLIGENT SELECTION SUMMARY:")
    report_lines.append("-" * 40)
    report_lines.append(f"PyMuPDF Selected: {pymupdf_count} pages")
    report_lines.append(f"OCR Selected: {ocr_count} pages")
    report_lines.append(f"Total Pages: {len(selection_results)} pages")
    report_lines.append("=" * 80)
    report_lines.append("")
    
    # Process each page in order
    for page_num_str in sorted(selection_results.keys(), key=int):
        page_num = int(page_num_str)
        selection = selection_results[page_num_str]
        selected_source = selection['selected_source']
        reason = selection['reason']
        confidence = selection['confidence']
        
        # Get the selected text
        if selected_source == 'PyMuPDF':
            page_text = pymupdf_pages.get(page_num, '')
            source_info = f"PyMuPDF (Clean)"
        else:  # OCR
            page_text = ocr_pages.get(page_num, '')
            source_info = f"OCR (All Pages)"
        
        report_lines.append(f"PAGE {page_num} ({source_info}):")
        report_lines.append("-" * 50)
        report_lines.append(f"Selected Source: {selected_source}")
        report_lines.append(f"Reason: {reason}")
        report_lines.append(f"Confidence: {confidence}")
        report_lines.append(f"Characters: {len(page_text):,}")
        # Count lines (avoid backslash in f-string)
        num_lines = len([line for line in page_text.split(chr(10)) if line.strip()])
        report_lines.append(f"Lines: {num_lines}")
        report_lines.append("")
        report_lines.append("TEXT CONTENT:")
        report_lines.append("-" * 30)
        report_lines.append(page_text)
        report_lines.append("=" * 80)
        report_lines.append("")
    
    report_content = "\n".join(report_lines)
    
    _upload_text_to_gcs(bucket, combined_file_path, report_content)
    print(f"✅ Saved intelligent combined file to: gs://{BUCKET_NAME}/{combined_file_path}")
    
    return combined_file_path


def process_upload_intelligent_combination(upload_id: str) -> Dict[str, Any]:
    """
    Given an upload_id, read Phase 2C results from GCS,
    read corresponding PyMuPDF and OCR files,
    and create intelligent combined files.
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
                
                # Find latest smart selection JSON file
                selection_files = list(bucket.list_blobs(prefix=f'phase2c/results/{safe_carrier_name}_{type_short}_smart_selection_'))
                if not selection_files:
                    print(f"Warning: No smart selection results found for {carrier_name} {file_type}")
                    continue
                
                # Get latest file
                selection_file = sorted(selection_files, key=lambda x: x.time_created)[-1].name
                
                # Read smart selection results
                selection_results = read_smart_selection_results_from_gcs(bucket, selection_file)
                
                # Find corresponding Phase 1 and Phase 2 files
                pymupdf_files = list(bucket.list_blobs(prefix=f'phase1/results/{safe_carrier_name}_{type_short}_pymupdf_clean_pages_only_'))
                ocr_files = list(bucket.list_blobs(prefix=f'phase2/results/{safe_carrier_name}_{type_short}_ocr_all_pages_'))
                
                if not pymupdf_files or not ocr_files:
                    print(f"Warning: Missing Phase 1 or Phase 2 results for {carrier_name} {file_type}")
                    continue
                
                # Get latest files
                pymupdf_file = sorted(pymupdf_files, key=lambda x: x.time_created)[-1].name
                ocr_file = sorted(ocr_files, key=lambda x: x.time_created)[-1].name
                
                # Read both text sources
                pymupdf_pages = read_pymupdf_clean_pages_from_gcs(bucket, pymupdf_file)
                ocr_pages = read_ocr_all_pages_from_gcs(bucket, ocr_file)
                
                # Create intelligent combined file
                combined_path = create_intelligent_combined_file(
                    bucket, selection_results, pymupdf_pages, ocr_pages,
                    carrier_name, safe_carrier_name, file_type, report_timestamp
                )
                
                all_results.append({
                    'carrierName': carrier_name,
                    'fileType': file_type,
                    'combinedFile': f'gs://{BUCKET_NAME}/{combined_path}',
                    'totalPages': len(selection_results),
                    'pymupdfSelected': len([s for s in selection_results.values() if s['selected_source'] == 'PyMuPDF']),
                    'ocrSelected': len([s for s in selection_results.values() if s['selected_source'] == 'OCR'])
                })
                
            except Exception as e:
                print(f"Error processing {carrier_name} {file_type}: {e}")
                all_results.append({
                    'carrierName': carrier_name,
                    'fileType': file_type,
                    'error': str(e)
                })
    
    result = {
        "success": True,
        "uploadId": upload_id,
        "results": all_results
    }
    
    # Automatically trigger Phase 3 LLM extraction after intelligent combination completes
    try:
        print("\n✅ Phase 2D Intelligent Combination complete. Starting Phase 3 LLM extraction...")
        from phase3_llm import process_upload_llm_extraction
        llm_result = process_upload_llm_extraction(upload_id)
        if llm_result.get('success'):
            print("✅ Phase 3 LLM extraction complete!")
        else:
            print(f"Warning: Phase 3 had issues: {llm_result.get('error')}")
    except Exception as e:
        print(f"Warning: Phase 3 LLM extraction failed: {e}")
    
    return result
