"""
Phase 3 GL: LLM Information Extraction for General Liability
Extracts 22 specific general liability coverage fields from insurance documents using GPT.
Works with Google Cloud Storage.
Uses Joblib for parallel chunk processing.
"""
import json
import openai
import os
import re
from datetime import datetime
from typing import Dict, Any, List
from google.cloud import storage
from dotenv import load_dotenv
from joblib import Parallel, delayed
from schemas.gl_schema import GL_FIELDS_SCHEMA, get_gl_field_names, get_gl_required_fields

load_dotenv()

BUCKET_NAME = os.getenv('BUCKET_NAME', 'mckinneysuite')

# Initialize OpenAI
openai.api_key = os.getenv('OPENAI_API_KEY')

if not openai.api_key:
    print("Warning: OPENAI_API_KEY not found in environment variables!")
    print("Phase 3 GL LLM extraction will fail without OpenAI API key")


def _get_bucket() -> storage.bucket.Bucket:
    client = storage.Client()
    return client.bucket(BUCKET_NAME)


def _download_text_from_gcs(bucket: storage.bucket.Bucket, blob_path: str) -> str:
    """Download text file from GCS"""
    blob = bucket.blob(blob_path)
    if not blob.exists():
        return ""
    return blob.download_as_string().decode('utf-8')


def _download_json_from_gcs(bucket: storage.bucket.Bucket, blob_path: str) -> Dict[str, Any]:
    """Download JSON file from GCS"""
    blob = bucket.blob(blob_path)
    if not blob.exists():
        return {}
    return json.loads(blob.download_as_string().decode('utf-8'))


def _upload_json_to_gcs(bucket: storage.bucket.Bucket, blob_path: str, data: Dict[str, Any]) -> None:
    """Upload JSON file to GCS"""
    blob = bucket.blob(blob_path)
    blob.upload_from_string(
        json.dumps(data, indent=2, ensure_ascii=False),
        content_type='application/json'
    )
    print(f"✅ Uploaded to: gs://{BUCKET_NAME}/{blob_path}")


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
        page_sections = re.findall(
            r'PAGE (\d+) \((PyMuPDF|OCR) \(.*?\)\):.*?TEXT CONTENT:.*?------------------------------\n(.*?)\n={80}',
            content,
            re.DOTALL
        )
        
        for page_num, source, page_text in page_sections:
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

def extract_with_llm(chunk: Dict[str, Any], chunk_num: int, total_chunks: int) -> Dict[str, Any]:
    """Extract information using LLM with your exact prompt"""
    
    prompt = f"""
    Analyze the following general liability insurance document text and extract ONLY the 22 specific general liability coverage fields listed below.
    
    CRITICAL: Extract ONLY these 22 fields. Do NOT create new field names or extract any other information.
    
    THE 22 SPECIFIC FIELDS TO EXTRACT (with examples of what to look for):
    1. Each Occurrence/General Aggregate Limits - Look for: "$1,000,000 / $2,000,000", "$1M / $2M", any occurrence/aggregate limits with dollar amounts and "/" separator
    2. Liability Deductible - Per claim or Per Occ basis - Look for: "$0", "$500", "$1,000", "Per claim", "Per Occurrence", any liability deductible amount
    3. Hired Auto And Non-Owned Auto Liability - Without Delivery Service - Look for: "Included", "Excluded", "$1,000,000 / $1,000,000", any hired/non-owned auto coverage
    4. Fuel Contamination coverage limits - Look for: "Each Customer's Auto Limit $1,000", "Aggregate Limit $5,000", any fuel contamination coverage
    5. Vandalism coverage - Look for: any vandalism coverage details
    6. Garage Keepers Liability - Look for: "Limit: $60,000", "Comprehensive Deductible: $500", "Collision Deductible: $500", any garage keepers liability
    7. Employment Practices Liability - Look for: "Each Claim Limit $25,000", "Aggregate Limit $25,000", any employment practices liability
    8. Abuse & Molestation Coverage limits - Look for: "Excluded", "Included", "Exclusion - Abuse or Molestation", any abuse & molestation coverage status
    9. Assault & Battery Coverage limits - Look for: "$100,000 / $200,000", "Not Excluded", "Excluded", "Limited Coverage - Assault or Battery", any assault & battery coverage
    10. Firearms/Active Assailant Coverage limits - Look for: "Not Excluded", "Excluded", any firearms/active assailant coverage
    11. Additional Insured - Look for: "786 ALLGOOD ROAD LLC", "C/O GIL MOOR", specific company names and addresses, any additional insured details
    12. Additional Insured (Mortgagee) - Look for: "FIRST HORIZON BANK", "NORTHEAST BANK", "PO BOX", specific bank names and addresses, any mortgagee additional insured details
    13. Additional Insured - Jobber - Look for: "Premier Petroleum", any jobber additional insured details
    14. Exposure - Look for: "Inside Sales: $400,000", "Gasoline Gallons: 400,000", any exposure details with sales and gallons
    15. Rating basis: If Sales - Subject to Audit - Look for: "Sales $300,000", "Gasoline 48,000 Gallons", "Area: 1,600 Sqft", any rating basis information
    16. Terrorism - Look for: "Excluded", "Can be added with additional premium", "Excluded - Can be Added With Additional Premium", any terrorism coverage status
    17. Personal and Advertising Injury Limit - Look for: "$1,000,000", "Excluded", any personal and advertising injury limit
    18. Products/Completed Operations Aggregate Limit - Look for: "Excluded", any products/completed operations aggregate limit
    19. Minimum Earned - Look for: "25%", "MEP: 25%", "35%", any minimum earned premium percentage
    20. General Liability Premium - Look for: "$1,200.00", "GL Premium", "Liability Premium", "TOTAL excl Terrorism", "TOTAL CHARGES W/O TRIA", any GL premium amount (PRIORITY: Look for "TOTAL excl Terrorism" or "TOTAL CHARGES W/O TRIA" first)
    21. Total Premium (With/Without Terrorism) - Look for: "TOTAL CHARGES W/O TRIA $7,176.09, TOTAL CHARGES WITH TRIA $7,441.13", "TOTAL excl Terrorism $2,019.68, TOTAL incl Terrorism $2,123.68", "Total Premium", "Annual Premium", any total premium amount (EXTRACT BOTH VALUES if available: "Without Terrorism: $X,XXX.XX, With Terrorism: $X,XXX.XX")
    22. Policy Premium - Look for: "$2,500.00", "Policy Premium", "Base Premium", "General Liability" base amount, any policy premium amount
    
    EXTRACTION RULES:
    - Extract EXACTLY as written in the document
    - Look for SIMILAR PATTERNS even if exact examples don't match
    - For Limits: Look for dollar amounts with "/" separator (e.g., "$X,XXX,XXX / $X,XXX,XXX")
    - For Dollar Amounts: Look for any dollar amounts ($X,XXX, $X,XXX.XX, $XXX,XXX)
    - For Coverage Status: Look for "Included", "Excluded", "Not Excluded"
    - For Deductibles: Look for "Per claim", "Per Occurrence", "Per Occ" with amounts
    - For Additional Insured: Extract complete details including names and addresses
    - For Rating Basis: Extract complete sales/area/gasoline information
    - For Multi-line Values: Extract everything related to that field, preserve line breaks
    - For Complex Values: Extract the complete text block for that field
    - CRITICAL: Do NOT extract "See Carrier Quote" or "See Quote" - extract the ACTUAL VALUES
    - CRITICAL: Look for the actual dollar amounts, limits, and specific details
    - CRITICAL: If you see a table with columns, extract the values from the appropriate column
    - If field is not found, set to null
    - Do NOT hallucinate or make up values
    - Do NOT combine or modify existing values
    - If you see variations not in examples, still extract them exactly as written
    - Do NOT extract administrative, financial, or policy information
    - Do NOT create new field names
    - Do NOT extract policy numbers or legal disclosures
    - Note: Some quotes may have multiple columns (2-3 carriers), extract values for EACH column as separate entries when applicable
    
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
    General Liability
    Each Occurrence: $1,000,000
    General Aggregate: $2,000,000
    
    Then those limits should have page: 7 (because they're under "=== PAGE 7 ===" marker)
    
    CRITICAL: Return ONLY valid JSON with this exact format:
    {{
        "Each Occurrence/General Aggregate Limits": {{"value": "$1,000,000 / $2,000,000", "page": 5}},
        "Liability Deductible - Per claim or Per Occ basis": {{"value": "$0", "page": 5}},
        "Hired Auto And Non-Owned Auto Liability - Without Delivery Service": {{"value": "Included", "page": 5}},
        "Fuel Contamination coverage limits": {{"value": "Each Customer's Auto Limit $1,000, Aggregate Limit $5,000", "page": 3}},
        "Vandalism coverage": {{"value": null, "page": null}},
        "Garage Keepers Liability": {{"value": "Limit: $60,000, Comprehensive Deductible: $500, Collision Deductible: $500", "page": 3}},
        "Employment Practices Liability": {{"value": "Each Claim Limit $25,000, Aggregate Limit $25,000", "page": 3}},
        "Abuse & Molestation Coverage limits": {{"value": null, "page": null}},
        "Assault & Battery Coverage limits": {{"value": "$100,000 / $200,000", "page": 5}},
        "Firearms/Active Assailant Coverage limits": {{"value": "Not Excluded", "page": 5}},
        "Additional Insured": {{"value": "786 ALLGOOD ROAD LLC C/O GIL MOOR 786 ALLGOOD RD MARIETTA GA 30062", "page": 3}},
        "Additional Insured (Mortgagee)": {{"value": "FIRST HORIZON BANK PO BOX 132 MEMPHIS TN 38101", "page": 3}},
        "Additional Insured - Jobber": {{"value": "Premier Petroleum", "page": 3}},
        "Exposure": {{"value": "Inside Sales: $400,000, Gasoline Gallons: 400,000", "page": 3}},
        "Rating basis: If Sales - Subject to Audit": {{"value": "Sales $300,000, Gasoline 48,000 Gallons", "page": 3}},
        "Terrorism": {{"value": "Excluded, Can be added with additional premium", "page": 3}},
        "Personal and Advertising Injury Limit": {{"value": "$1,000,000", "page": 5}},
        "Products/Completed Operations Aggregate Limit": {{"value": "Excluded", "page": 5}},
        "Minimum Earned": {{"value": "25%", "page": 3}},
        "General Liability Premium": {{"value": "$1,200.00", "page": 3}},
        "Total Premium (With/Without Terrorism)": {{"value": "Without Terrorism: $1,200.00, With Terrorism: $1,300.00", "page": 3}},
        "Policy Premium": {{"value": "$2,500.00", "page": 3}}
    }}
    
    PAGE DETECTION RULES:
    - Look for "Page X" markers in the text above each field
    - Use the nearest page number found above the field
    - If no page number found, use null for page
    - Multiple fields can share the same page number
    - Extract the actual page number from the text (e.g., "Page 3" = page 3)
    
    If a field is not found, use: {{"value": null, "page": null}}
    Do not provide explanations, context, or any text outside the JSON object.
    
    Document text:
    {chunk['text']}
    """
    
    try:
        print(f"  Processing chunk {chunk_num} with LLM (Pages {chunk['page_nums']})...")
        
        # Use OpenAI API (GPT-5 Responses API format)
        client = openai.OpenAI(api_key=openai.api_key)
        response = client.responses.create(
            model="gpt-5",
            input=prompt,
            reasoning={
                "effort": "low"
            },
            text={
                "verbosity": "low"
            }
        )
        
        result_text = response.output_text.strip()
        
        # Check if response is empty
        if not result_text:
            print(f"  [ERROR] Empty response from LLM")
            return {'_metadata': {'chunk_num': chunk_num, 'page_nums': chunk['page_nums'], 'error': 'Empty LLM response'}}
        
        # Clean up markdown code blocks if present
        if result_text.startswith('```json'):
            result_text = result_text[7:]  # Remove ```json
        if result_text.startswith('```'):
            result_text = result_text[3:]   # Remove ```
        if result_text.endswith('```'):
            result_text = result_text[:-3]  # Remove trailing ```
        result_text = result_text.strip()
        
        # Try to parse JSON
        try:
            result_json = json.loads(result_text)
            
            # Convert new format to old format for compatibility
            converted_json = {}
            individual_page_fields = {}
            
            for field, data in result_json.items():
                if isinstance(data, dict) and 'value' in data and 'page' in data:
                    # New format: {"value": "FRAME", "page": 5}
                    converted_json[field] = data['value']
                    if data['value'] is not None and data['page'] is not None:
                        individual_page_fields[field] = [data['page']]
                        print(f"    Found {field} on Page {data['page']}")
                else:
                    # Old format: direct value
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
    
    # Use schema to initialize GL fields in correct order
    # This ensures Google Sheets always has consistent field ordering
    expected_field_names = get_gl_field_names()  # From GL schema - guaranteed order
    
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
                # If field already has a value, keep the first non-null one
                if merged_result[field] is None:
                    merged_result[field] = value
                    # Store the specific page where this field was found
                    # For now, use the first page of the chunk, but this should be more precise
                    field_sources[field] = [chunk_pages[0]] if chunk_pages else []
                else:
                    # If we have multiple values, note the conflict
                    if merged_result[field] != value:
                        print(f"  Multiple values found for {field}: '{merged_result[field]}' (pages {field_sources[field]}) and '{value}' (pages {chunk_pages})")
    
    # Add source information to merged result
    merged_result['_extraction_summary'] = {
        'total_chunks_processed': len(all_results),
        'successful_chunks': len([r for r in all_results if '_metadata' in r and 'error' not in r['_metadata']]),
        'field_sources': field_sources
    }
    
    return merged_result

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


def _check_if_all_carriers_complete_gl(bucket: storage.bucket.Bucket, upload_id: str) -> bool:
    """
    Check if all carriers in this upload have completed Phase 3 GL.
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
        
        if len(carriers) == 0:
            print(f"⚠️  No carriers found for upload {upload_id}")
            return False
        
        # Count how many carriers have completed Phase 3 GL
        completed_count = 0
        for carrier in carriers:
            carrier_name = carrier.get('carrierName', 'Unknown')
            safe_name = carrier_name.lower().replace(" ", "_").replace("&", "and")
            
            # Check for GL final validated fields
            if carrier.get('liabilityPDF'):
                pdf_info = carrier.get('liabilityPDF')
                if not pdf_info or not pdf_info.get('path'):
                    continue
                
                # Extract timestamp from PDF path
                pdf_path = pdf_info['path']
                timestamp_match = re.search(r'_(\d{8}_\d{6})\.pdf$', pdf_path)
                if not timestamp_match:
                    continue
                
                timestamp = timestamp_match.group(1)
                
                # Check if Phase 3 GL result exists
                final_file_path = f"phase3/results/{safe_name}_liability_final_validated_fields_{timestamp}.json"
                blob = bucket.blob(final_file_path)
                if blob.exists():
                    completed_count += 1
        
        # Count expected GL files
        expected_files = 0
        for carrier in carriers:
            if carrier.get('liabilityPDF') and carrier.get('liabilityPDF').get('path'):
                expected_files += 1
        
        print(f"📊 Upload {upload_id}: {completed_count}/{expected_files} GL files completed")
        
        return completed_count == expected_files and expected_files > 0
        
    except Exception as e:
        print(f"❌ Error checking GL completion status: {e}")
        import traceback
        traceback.print_exc()
        return False

def process_upload_llm_extraction_gl(upload_id: str) -> Dict[str, Any]:
    """
    Given an upload_id, read Phase 2D results from GCS for General Liability,
    extract insurance fields using LLM, and save results.
    """
    if not openai.api_key:
        return {"success": False, "error": "OpenAI API key not configured. Cannot run Phase 3 GL."}
    
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
        
        # Only process liability PDFs for GL
        pdf_info = carrier.get('liabilityPDF')
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
            
            # Find latest intelligent combined file for liability
            combined_files = list(bucket.list_blobs(prefix=f'phase2d/results/{safe_carrier_name}_liability_intelligent_combined_'))
            if not combined_files:
                print(f"Warning: No combined file found for {carrier_name} liability")
                continue
            
            # Get latest file
            combined_file = sorted(combined_files, key=lambda x: x.time_created)[-1].name
            
            # Read combined file
            all_pages = read_combined_file_from_gcs(bucket, combined_file)
            if not all_pages:
                print(f"Warning: No pages extracted from {combined_file}")
                continue
            
            # Create chunks (4 pages each)
            chunks = create_chunks(all_pages, chunk_size=4)
            
            # Process each chunk with LLM - PARALLELIZED for faster processing
            print(f"\nProcessing {len(chunks)} GL chunks in parallel...")
            
            def process_single_chunk(chunk):
                """Process one chunk - called in parallel"""
                print(f"  Processing GL Chunk {chunk['chunk_num']}/{len(chunks)}...")
                return extract_with_llm(chunk, chunk['chunk_num'], len(chunks))
            
            # Process all chunks in parallel (n_jobs=-1 uses all available cores)
            chunk_results = Parallel(
                n_jobs=-1,
                backend='threading',
                verbose=5
            )(
                delayed(process_single_chunk)(chunk)
                for chunk in chunks
            )
            
            # Merge all results
            print(f"\nMerging results from {len(chunk_results)} chunks...")
            merged_result = merge_extraction_results(chunk_results)
            
            # Save results to GCS
            final_path = save_extraction_results_to_gcs(bucket, merged_result, carrier_name, safe_carrier_name, 'liabilityPDF', report_timestamp)
            
            all_results.append({
                'carrierName': carrier_name,
                'fileType': 'liabilityPDF',
                'finalFields': f'gs://{BUCKET_NAME}/{final_path}',
                'totalFields': len([k for k in merged_result.keys() if not k.startswith('_')]),
                'fieldsFound': len([k for k, v in merged_result.items() if v is not None and not k.startswith('_')])
            })
            
        except Exception as e:
            print(f"Error processing {carrier_name} liability: {e}")
            all_results.append({
                'carrierName': carrier_name,
                'fileType': 'liabilityPDF',
                'error': str(e)
            })
    
    result = {
        "success": True,
        "uploadId": upload_id,
        "results": all_results
    }
    
    # Check if all carriers in this upload have completed Phase 3 GL
    print("\n✅ Phase 3 GL LLM extraction complete!")
    print("🔍 Checking if all carriers are complete...")
    
    if _check_if_all_carriers_complete_gl(bucket, upload_id):
        print("🎉 ALL GL CARRIERS COMPLETE! Auto-filling sheet...")
        try:
            import gspread
            from google.oauth2.service_account import Credentials
            from pathlib import Path
            
            # Get credentials
            possible_paths = [
                'credentials/insurance-sheets-474717-7fc3fd9736bc.json',
                '../credentials/insurance-sheets-474717-7fc3fd9736bc.json',
            ]
            creds_path = None
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
                sheet = client.open("Insurance Fields Data").sheet1
                
                # Field to Cell mapping for GL
                field_mapping = {
                    "Each Occurrence/General Aggregate Limits": "B8",
                    "Liability Deductible - Per claim or Per Occ basis": "B9",
                    "Hired Auto And Non-Owned Auto Liability - Without Delivery Service": "B10",
                    "Fuel Contamination coverage limits": "B11",
                    "Vandalism coverage": "B12",
                    "Garage Keepers Liability": "B13",
                    "Employment Practices Liability": "B14",
                    "Abuse & Molestation Coverage limits": "B15",
                    "Assault & Battery Coverage limits": "B16",
                    "Firearms/Active Assailant Coverage limits": "B17",
                    "Additional Insured": "B18",
                    "Additional Insured (Mortgagee)": "B19",
                    "Additional insured - Jobber": "B20",
                    "Exposure": "B21",
                    "Rating basis: If Sales - Subject to Audit": "B22",
                    "Terrorism": "B23",
                    "Personal and Advertising Injury Limit": "B24",
                    "Products/Completed Operations Aggregate Limit": "B25",
                    "Minimum Earned": "B26",
                    "General Liability Premium": "B27",
                    "Total Premium (With/Without Terrorism)": "B28",
                    "Policy Premium": "B29",
                    "Contaminated fuel": "B30",
                    "Liquor Liability": "B31",
                    "Additional Insured - Managers Or Lessors Of Premises": "B32",
                }
                
                # Load GL extracted data from GCS
                carriers = record.get('carriers', [])
                for carrier in carriers:
                    if carrier.get('liabilityPDF'):
                        carrier_name = carrier.get('carrierName', 'Unknown')
                        pdf_path = carrier['liabilityPDF']['path']
                        timestamp_match = re.search(r'_(\d{8}_\d{6})\.pdf$', pdf_path)
                        if timestamp_match:
                            timestamp = timestamp_match.group(1)
                            safe_name = carrier_name.lower().replace(" ", "_").replace("&", "and")
                            
                            # Load GL data from GCS
                            gl_file = f"phase3/results/{safe_name}_liability_final_validated_fields_{timestamp}.json"
                            gl_data = _download_json_from_gcs(bucket, gl_file)
                            
                            if gl_data:
                                # Build batch update from extracted data
                                updates = []
                                for field_name, cell_ref in field_mapping.items():
                                    if field_name in gl_data:
                                        field_info = gl_data[field_name]
                                        llm_value = field_info.get("llm_value", "") if isinstance(field_info, dict) else field_info
                                        if llm_value:
                                            updates.append({
                                                'range': cell_ref,
                                                'values': [[str(llm_value)]]
                                            })
                                
                                # Single batch update - only values, no formatting
                                if updates:
                                    sheet.batch_update(updates)
                                    print(f"✅ Batch updated {len(updates)} GL fields to sheet")
                                    result['sheets_push'] = {"success": True, "fields_filled": len(updates)}
                                else:
                                    print("⚠️  No GL values to fill")
                            else:
                                print(f"⚠️  No GL data found at {gl_file}")
                        break
            else:
                print("⚠️  Credentials not found")
        except Exception as e:
            print(f"❌ Sheet fill failed: {e}")
            import traceback
            traceback.print_exc()
    else:
        print("⏳ Other GL carriers still processing. Waiting for all to complete...")
    
    return result
