"""
Phase 2C: Smart Selection (Rule-Based)
Uses simple rule-based selection: Always prefers OCR (NanoNets) over PyMuPDF.
Fallback to PyMuPDF only if OCR is unavailable.
Works with Google Cloud Storage.
No LLM calls - faster and cheaper.
"""
import openai
import json
import os
import re
from datetime import datetime
from typing import Dict, Any, List
from google.cloud import storage
from dotenv import load_dotenv

load_dotenv()

BUCKET_NAME = os.getenv('BUCKET_NAME', 'deployment')

# Initialize OpenAI
openai.api_key = os.getenv('OPENAI_API_KEY')

if not openai.api_key:
    print("Warning: OPENAI_API_KEY not found in environment variables!")
    print("Phase 2C Smart Selection will fail without OpenAI API key")


def _get_bucket() -> storage.bucket.Bucket:
    client = storage.Client()
    return client.bucket(BUCKET_NAME)


def _download_text_from_gcs(bucket: storage.bucket.Bucket, blob_path: str) -> str:
    """Download text file from GCS"""
    blob = bucket.blob(blob_path)
    return blob.download_as_string().decode('utf-8')


def _upload_json_to_gcs(bucket: storage.bucket.Bucket, blob_path: str, data: dict) -> None:
    """Upload JSON file to GCS"""
    blob = bucket.blob(blob_path)
    blob.upload_from_string(
        json.dumps(data, indent=2),
        content_type='application/json'
    )


def read_pymupdf_clean_pages_from_gcs(bucket: storage.bucket.Bucket, file_path: str) -> Dict[int, Dict[str, str]]:
    """Read PyMuPDF clean pages from GCS file"""
    try:
        content = _download_text_from_gcs(bucket, file_path)
        clean_pages = {}
        
        # Extract clean pages
        page_sections = re.findall(r'PAGE (\d+):.*?TEXT CONTENT:\n(.*?)\n={80}', content, re.DOTALL)
        
        for page_num, page_text in page_sections:
            clean_pages[int(page_num)] = {
                'text': page_text.strip(),
                'source': 'PyMuPDF'
            }
        
        print(f"Found {len(clean_pages)} PyMuPDF clean pages: {list(clean_pages.keys())}")
        return clean_pages
    except Exception as e:
        print(f"Error reading PyMuPDF clean pages: {e}")
        return {}


def read_ocr_all_pages_from_gcs(bucket: storage.bucket.Bucket, file_path: str) -> Dict[int, Dict[str, str]]:
    """Read OCR all pages from GCS file"""
    try:
        content = _download_text_from_gcs(bucket, file_path)
        ocr_pages = {}
        
        # Extract OCR pages
        page_sections = re.findall(r'PAGE (\d+):.*?OCR EXTRACTED TEXT:.*?----------------------------------------\n(.*?)\n={80}', content, re.DOTALL)
        
        for page_num, page_text in page_sections:
            ocr_pages[int(page_num)] = {
                'text': page_text.strip(),
                'source': 'OCR'
            }
        
        print(f"Found {len(ocr_pages)} OCR pages: {list(ocr_pages.keys())}")
        return ocr_pages
    except Exception as e:
        print(f"Error reading OCR pages: {e}")
        return {}


def get_all_page_numbers(pymupdf_pages: Dict[int, Dict], ocr_pages: Dict[int, Dict]) -> List[int]:
    """Get all unique page numbers from both sources"""
    all_pages = set(pymupdf_pages.keys()) | set(ocr_pages.keys())
    return sorted(list(all_pages))


def create_selection_prompt(page_num: int, pymupdf_text: str, ocr_text: str) -> str:
    """Create prompt for LLM to select best text source"""
    
    # Truncate texts to focus on key information (first 300 chars)
    pymupdf_preview = pymupdf_text[:300] if pymupdf_text else "NOT AVAILABLE"
    ocr_preview = ocr_text[:300] if ocr_text else "NOT AVAILABLE"
    
    # Add truncation notice if text was cut
    pymupdf_notice = "\n[... text truncated for analysis ...]" if len(pymupdf_text) > 3000 else ""
    ocr_notice = "\n[... text truncated for analysis ...]" if len(ocr_text) > 3000 else ""
    
    prompt = f"""
You are a text quality analyzer for insurance documents. Compare two text extractions for Page {page_num} and decide which one is better.

CRITICAL INSURANCE DOCUMENT CRITERIA:
1. DATA COMPLETENESS - Which text contains actual data values vs blank fields?
2. DATA CORRECTNESS - Which text has accurate data without OCR errors?
3. READABILITY - Which text is more readable and coherent?

IMPORTANT: For insurance documents, DATA COMPLETENESS AND CORRECTNESS are the most critical factors. 
- A form with blank fields (like "Account No. ______________________") is WORSE than a form with actual data values
- A form with incorrect OCR data (like "Account No. 8l7553.l" instead of "817553.1") is WORSE than a form with correct data
- Choose the source that provides the most complete AND accurate data

PYMUPDF EXTRACTION (Page {page_num}):
{pymupdf_preview}{pymupdf_notice}

OCR EXTRACTION (Page {page_num}):
{ocr_preview}{ocr_notice}

SELECTION RULES:
- If PyMuPDF text is NOT AVAILABLE, choose OCR
- If OCR text is NOT AVAILABLE, choose PyMuPDF  
- If PyMuPDF has blank fields (like "______________________") and OCR has actual data, choose OCR
- If OCR has obvious errors and PyMuPDF has correct data, choose PyMuPDF
- If both have data, choose the more complete AND accurate one

SPECIFIC RED FLAGS TO AVOID:
- Blank account numbers, policy numbers, or dollar amounts
- Forms with empty fields where data should be
- OCR character recognition errors (0/O, 1/I, 6/G, 8/B, etc.)

Return ONLY a JSON response with this exact format:
{{
    "page": {page_num},
    "selected_source": "PyMuPDF" or "OCR",
    "reason": "Brief explanation focusing on data completeness and correctness",
    "confidence": "high" or "medium" or "low"
}}

Do not provide any other text, only the JSON response.
"""
    
    return prompt


def select_best_source_with_llm(page_num: int, pymupdf_text: str, ocr_text: str) -> Dict[str, Any]:
    """Use GPT-3.5 to select best text source for a page"""
    
    prompt = create_selection_prompt(page_num, pymupdf_text, ocr_text)
    
    try:
        print(f"  Analyzing Page {page_num} with GPT-3.5...")
        
        # Use OpenAI API
        if not openai.api_key:
            print("  [ERROR] OpenAI API key not configured")
            return None
        client = openai.OpenAI(api_key=openai.api_key)
        response = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": "You are a text quality analyzer specializing in insurance documents. Your primary focus is DATA COMPLETENESS AND CORRECTNESS - actual data values that are accurate are more important than clean formatting. Watch for OCR character recognition errors. Return ONLY valid JSON responses."},
                {"role": "user", "content": prompt}
            ],
            max_tokens=1000,
            temperature=0.1
        )
        
        result_text = response.choices[0].message.content.strip()
        
        # Clean up markdown code blocks if present
        if result_text.startswith('```json'):
            result_text = result_text[7:]
        if result_text.startswith('```'):
            result_text = result_text[3:]
        if result_text.endswith('```'):
            result_text = result_text[:-3]
        result_text = result_text.strip()
        
        # Parse JSON response
        try:
            result_json = json.loads(result_text)
            return result_json
        except json.JSONDecodeError as e:
            print(f"  [ERROR] Failed to parse JSON response: {e}")
            print(f"  Raw response: {result_text[:100]}...")
            return None
            
    except Exception as e:
        print(f"  [ERROR] LLM processing failed: {e}")
        return None


def process_all_pages_selection(pymupdf_pages: Dict[int, Dict], ocr_pages: Dict[int, Dict]) -> Dict[int, Dict]:
    """
    Process smart selection for all pages using simple rule-based selection.
    Always prefers OCR (NanoNets) over PyMuPDF, with PyMuPDF as fallback if OCR unavailable.
    No LLM calls - faster and cheaper.
    """
    print("PHASE 2C: SMART SELECTION (Rule-Based)")
    print("=" * 80)
    
    all_pages = get_all_page_numbers(pymupdf_pages, ocr_pages)
    print(f"Processing {len(all_pages)} pages for smart selection")
    print("Selection Rule: Always prefer OCR (NanoNets), fallback to PyMuPDF if OCR unavailable")
    print("=" * 80)
    
    selection_results = {}
    
    for page_num in all_pages:
        print(f"\nProcessing Page {page_num}...")
        
        # Get texts from both sources
        pymupdf_text = pymupdf_pages.get(page_num, {}).get('text', '')
        ocr_text = ocr_pages.get(page_num, {}).get('text', '')
        
        # Simple rule: Always prefer OCR if available (NanoNets is better quality)
        # Fallback to PyMuPDF only if OCR is not available
        if ocr_text and len(ocr_text.strip()) > 0:
            # OCR available - use it (NanoNets provides better quality)
            selection_results[page_num] = {
                "page": page_num,
                "selected_source": "OCR",
                "reason": "OCR (NanoNets) selected - provides better quality with structured data extraction",
                "confidence": "high"
            }
            print(f"  [SUCCESS] Selected OCR - NanoNets provides better quality")
        elif pymupdf_text and len(pymupdf_text.strip()) > 0:
            # OCR not available, use PyMuPDF as fallback
            selection_results[page_num] = {
                "page": page_num,
                "selected_source": "PyMuPDF",
                "reason": "Fallback: OCR not available, using PyMuPDF",
                "confidence": "medium"
            }
            print(f"  [FALLBACK] Selected PyMuPDF (OCR not available)")
        else:
            # Neither source available
            print(f"  [ERROR] No text available for Page {page_num} from either source")
    
    return selection_results


def save_selection_results_to_gcs(bucket: storage.bucket.Bucket, carrier_name: str, safe_carrier_name: str, file_type: str, timestamp: str, selection_results: Dict[int, Dict]) -> None:
    """Save smart selection results to GCS"""
    type_short = file_type.replace('PDF', '').lower()
    selection_file_path = f'phase2c/results/{safe_carrier_name}_{type_short}_smart_selection_{timestamp}.json'
    
    _upload_json_to_gcs(bucket, selection_file_path, selection_results)
    print(f"✅ Saved smart selection results to: gs://{BUCKET_NAME}/{selection_file_path}")


def process_upload_smart_selection(upload_id: str, pymupdf_file: str, ocr_file: str) -> Dict[str, Any]:
    """
    Process smart selection for a single file pair (PyMuPDF + OCR).
    Returns selection results.
    """
    bucket = _get_bucket()
    
    # Read both source files
    pymupdf_pages = read_pymupdf_clean_pages_from_gcs(bucket, pymupdf_file)
    ocr_pages = read_ocr_all_pages_from_gcs(bucket, ocr_file)
    
    if not pymupdf_pages and not ocr_pages:
        print("Error: No pages found from either source!")
        return {"success": False, "error": "No pages found"}
    
    # Process smart selection
    selection_results = process_all_pages_selection(pymupdf_pages, ocr_pages)
    
    return {
        "success": True,
        "selection_results": selection_results
    }


def process_upload_smart_selection_analysis(upload_id: str) -> Dict[str, Any]:
    """
    Given an upload_id, read Phase 1 and Phase 2 results from GCS,
    perform smart selection using rule-based selection (always prefer OCR), and save results.
    Automatically called after Phase 2 OCR.
    No LLM calls - faster and cheaper.
    """
    bucket = _get_bucket()
    
    # Read metadata
    from phase1 import _read_metadata
    metadata = _read_metadata(bucket)
    
    uploads: List[Dict[str, Any]] = metadata.get('uploads', [])
    record = next((u for u in uploads if u.get('uploadId') == upload_id), None)
    if record is None:
        return {"success": False, "error": f"uploadId {upload_id} not found"}
    
    results: List[Dict[str, Any]] = []
    
    for carrier in record.get('carriers', []):
        carrier_name = carrier.get('carrierName')
        files_analysis: List[Dict[str, Any]] = []
        
        for file_type in ['propertyPDF', 'liabilityPDF', 'liquorPDF']:
            pdf_info = carrier.get(file_type)
            if not pdf_info:
                continue
            
            gs_path = pdf_info.get('path')
            if not gs_path:
                continue
            
            try:
                # Find corresponding Phase 1 and Phase 2 results in GCS
                # Phase 1 file: phase1/results/{carrier}_{type}_pymupdf_clean_pages_only_{timestamp}.txt
                # Phase 2 file: phase2/results/{carrier}_{type}_ocr_all_pages_{timestamp}.txt
                
                # Get latest files for this carrier+type combination
                # Since we just saved them, we can construct the path
                safe_carrier_name = carrier_name.lower().replace(" ", "_").replace("&", "and")
                type_short = file_type.replace('PDF', '').lower()
                
                # Find latest Phase 1 and Phase 2 files
                pymupdf_files = list(bucket.list_blobs(prefix=f'phase1/results/{safe_carrier_name}_{type_short}_pymupdf_clean_pages_only_'))
                ocr_files = list(bucket.list_blobs(prefix=f'phase2/results/{safe_carrier_name}_{type_short}_ocr_all_pages_'))
                
                if not pymupdf_files or not ocr_files:
                    print(f"Warning: Missing Phase 1 or Phase 2 results for {carrier_name} {file_type}")
                    continue
                
                # Get latest files (most recently uploaded)
                pymupdf_file = sorted(pymupdf_files, key=lambda x: x.time_created)[-1].name
                ocr_file = sorted(ocr_files, key=lambda x: x.time_created)[-1].name
                
                # Process smart selection
                selection_result = process_upload_smart_selection(upload_id, pymupdf_file, ocr_file)
                
                if selection_result.get('success'):
                    selection_results = selection_result['selection_results']
                    
                    files_analysis.append({
                        'type': file_type,
                        'selection_results': selection_results,
                        'pymupdf_source': f'gs://{BUCKET_NAME}/{pymupdf_file}',
                        'ocr_source': f'gs://{BUCKET_NAME}/{ocr_file}'
                    })
                    
                    # Save to GCS
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    save_selection_results_to_gcs(bucket, carrier_name, safe_carrier_name, file_type, timestamp, selection_results)
                
            except Exception as e:
                files_analysis.append({
                    'type': file_type,
                    'error': str(e)
                })
        
        results.append({
            'carrierName': carrier_name,
            'files': files_analysis,
        })
    
    # Prepare result
    result = {
        'success': True,
        'uploadId': upload_id,
        'carriers': results,
    }
    
    # Automatically trigger Phase 2D Intelligent Combination after smart selection completes
    try:
        print("\n✅ Phase 2C Smart Selection complete. Starting Phase 2D Intelligent Combination...")
        from phase2d_intelligent_combination import process_upload_intelligent_combination
        combination_result = process_upload_intelligent_combination(upload_id)
        if combination_result.get('success'):
            print("✅ Phase 2D Intelligent Combination complete!")
        else:
            print(f"Warning: Phase 2D had issues: {combination_result.get('error')}")
    except Exception as e:
        print(f"Warning: Phase 2D Intelligent Combination failed: {e}")
    
    return result
