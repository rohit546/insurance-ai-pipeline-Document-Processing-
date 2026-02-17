"""
Phase 3 Liquor: LLM Information Extraction for Liquor/Bar Insurance
Extracts 9 specific liquor coverage fields from insurance documents using GPT.
Works with Google Cloud Storage.
"""
import json
import openai
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
    print("Phase 3 Liquor LLM extraction will fail without OpenAI API key")


def _get_bucket() -> storage.bucket.Bucket:
    client = storage.Client()
    return client.bucket(BUCKET_NAME)


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

def extract_with_llm(chunk, chunk_num, total_chunks):
    """Extract information using LLM with your exact prompt"""
    
    prompt = f"""
    Analyze the following liquor/bar insurance document text and extract ONLY the 6 specific liquor coverage fields listed below.
    
    CRITICAL: Extract ONLY these 6 fields. Do NOT create new field names or extract any other information.
    
    THE 6 SPECIFIC FIELDS TO EXTRACT (with examples of what to look for):
    1. Each Occurrence/General Aggregate Limits - Look for: "$1,000,000 / $2,000,000", "$1,000,000 / $1,000,000", "Limit of Insurance: $1,000,000 Each Common Cause and $2,000,000 Aggregate", any occurrence/aggregate limits with dollar amounts and "/" separator
    2. Sales - Subject to Audit - Look for: "$52,000", "$60,000", "$80,000", "$93,000", "Based upon Estimated Annual and Alcohol Receipts of: $52,000", any sales amount subject to audit
    3. Assault & Battery/Firearms/Active Assailant - Look for: "Not Excluded", "Follows GL", "NOT EXCLUDING ASSAULT & BATTERY", "Excluded", "Included", any assault/battery/firearms/active assailant coverage status
    4. Requirements - Look for: "Active liquor license", "Training to staff serving alcohol", "Age verification", "minors are excluded", any requirements listed
    5. If any subjectivities in quote please add - Look for: "The establishment ceases the sale of alcohol daily by", "Approved by State / County / City", "approved represented time", any subjectivities or conditions
    6. Minimum Earned - Look for: "25%", "MEP: 25%", "35%", any minimum earned premium percentage
    7. Liquor Premium - Look for: "$800.00", "Liquor Premium", "Bar Premium", "TOTAL excl Terrorism", "TOTAL CHARGES W/O TRIA", any liquor premium amount (PRIORITY: Look for "TOTAL excl Terrorism" or "TOTAL CHARGES W/O TRIA" first)
    8. Total Premium (With/Without Terrorism) - Look for: "TOTAL CHARGES W/O TRIA $7,176.09, TOTAL CHARGES WITH TRIA $7,441.13", "TOTAL excl Terrorism $2,019.68, TOTAL incl Terrorism $2,123.68", "Total Premium", "Annual Premium", any total premium amount (EXTRACT BOTH VALUES if available: "Without Terrorism: $X,XXX.XX, With Terrorism: $X,XXX.XX")
    9. Policy Premium - Look for: "$2,500.00", "Policy Premium", "Base Premium", "Liquor" base amount, any policy premium amount
    
    EXTRACTION RULES:
    - Extract EXACTLY as written in the document
    - Look for SIMILAR PATTERNS even if exact examples don't match
    - For Limits: Look for dollar amounts with "/" separator (e.g., "$X,XXX,XXX / $X,XXX,XXX")
    - For Dollar Amounts: Look for any dollar amounts ($X,XXX, $X,XXX.XX, $XXX,XXX)
    - For Coverage Status: Look for "Excluded", "Not Excluded", "Included"
    - For Requirements: Extract ALL requirements listed, preserve line breaks and formatting
    - For Subjectivities: Extract complete text block for that field, preserve formatting
    - For Percentages: Look for any percentages (X%, X.X%, "MEP: X%")
    - For Multi-line Values: Extract everything related to that field, preserve line breaks
    - For Complex Values: Extract the complete text block for that field
    - If field is not found, set to null
    - Do NOT hallucinate or make up values
    - Do NOT combine or modify existing values
    - If you see variations not in examples, still extract them exactly as written
    - Do NOT extract administrative, financial, or policy information
    - Do NOT create new field names
    - Do NOT extract policy numbers or legal disclosures
    - Note: Some quotes may have multiple columns (2 carriers), extract values for EACH column as separate entries when applicable
    
    IMPORTANT: This is chunk {chunk_num} of {total_chunks}. This chunk contains pages {chunk['page_nums']}. 
    
    For each field you find, look for the nearest page number in the text above it (e.g., "Page 3", "Page 5"). 
    Use the actual page number from the text. Multiple fields can be on the same page.
    
    CRITICAL: Return ONLY valid JSON with this exact format:
    {{
        "Each Occurrence/General Aggregate Limits": {{"value": "$1,000,000 / $2,000,000", "page": 5}},
        "Sales - Subject to Audit": {{"value": "$60,000", "page": 5}},
        "Assault & Battery/Firearms/Active Assailant": {{"value": "Excluded", "page": 5}},
        "Requirements": {{"value": "Active liquor license, Training to staff serving alcohol, Age verification (minors are excluded)", "page": 5}},
        "If any subjectivities in quote please add": {{"value": "The establishment ceases the sale of alcohol daily by the represented time. Approved by State / County / City", "page": 3}},
        "Minimum Earned": {{"value": "25%", "page": 3}},
        "Liquor Premium": {{"value": "$800.00", "page": 3}},
        "Total Premium (With/Without Terrorism)": {{"value": "Without Terrorism: $800.00, With Terrorism: $900.00", "page": 3}},
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

def merge_extraction_results(all_results):
    """Merge results from all chunks, prioritizing non-null values"""
    
    # Define the expected fields for LIQUOR INSURANCE
    expected_fields = [
        "Each Occurrence/General Aggregate Limits",
        "Sales - Subject to Audit",
        "Assault & Battery/Firearms/Active Assailant",
        "Requirements",
        "If any subjectivities in quote please add",
        "Minimum Earned",
        "Liquor Premium",
        "Total Premium (With/Without Terrorism)",
        "Policy Premium"
    ]
    
    merged_result = {}
    
    # Initialize all expected fields as null
    for field in expected_fields:
        merged_result[field] = None
    
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


def _check_if_all_carriers_complete_liquor(bucket: storage.bucket.Bucket, upload_id: str) -> bool:
    """
    Check if all carriers in this upload have completed Phase 3 Liquor.
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
        
        # Count how many carriers have completed Phase 3 Liquor
        completed_count = 0
        for carrier in carriers:
            carrier_name = carrier.get('carrierName', 'Unknown')
            safe_name = carrier_name.lower().replace(" ", "_").replace("&", "and")
            
            # Check for liquor final validated fields
            if carrier.get('liquorPDF'):
                pdf_info = carrier.get('liquorPDF')
                if not pdf_info or not pdf_info.get('path'):
                    continue
                
                # Extract timestamp from PDF path
                pdf_path = pdf_info['path']
                timestamp_match = re.search(r'_(\d{8}_\d{6})\.pdf$', pdf_path)
                if not timestamp_match:
                    continue
                
                timestamp = timestamp_match.group(1)
                
                # Check if Phase 3 Liquor result exists
                final_file_path = f"phase3/results/{safe_name}_liquor_final_validated_fields_{timestamp}.json"
                blob = bucket.blob(final_file_path)
                if blob.exists():
                    completed_count += 1
        
        # Count expected liquor files
        expected_files = 0
        for carrier in carriers:
            if carrier.get('liquorPDF') and carrier.get('liquorPDF').get('path'):
                expected_files += 1
        
        print(f"📊 Upload {upload_id}: {completed_count}/{expected_files} Liquor files completed")
        
        return completed_count == expected_files and expected_files > 0
        
    except Exception as e:
        print(f"❌ Error checking Liquor completion status: {e}")
        import traceback
        traceback.print_exc()
        return False


def process_upload_llm_extraction_liquor(upload_id: str) -> Dict[str, Any]:
    """
    Given an upload_id, read Phase 2D results from GCS,
    extract liquor insurance fields using LLM, and save results.
    """
    if not openai.api_key:
        return {"success": False, "error": "OpenAI API key not configured. Cannot run Phase 3 Liquor."}
    
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
        
        # Process liquor PDF
        pdf_info = carrier.get('liquorPDF')
        if pdf_info:
            gs_path = pdf_info.get('path')
            if gs_path:
                try:
                    # Extract timestamp from PDF path
                    original_pdf_path = pdf_info.get('path')
                    timestamp_match = re.search(r'_(\d{8}_\d{6})\.pdf$', original_pdf_path)
                    if not timestamp_match:
                        report_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    else:
                        report_timestamp = timestamp_match.group(1)
                    
                    # Find latest intelligent combined file
                    combined_files = list(bucket.list_blobs(prefix=f'phase2d/results/{safe_carrier_name}_liquor_intelligent_combined_'))
                    if not combined_files:
                        print(f"Warning: No combined file found for {carrier_name} liquor")
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
                    
                    # Process each chunk with LLM
                    chunk_results = []
                    for chunk in chunks:
                        print(f"\nProcessing Chunk {chunk['chunk_num']}/{len(chunks)}...")
                        result = extract_with_llm(chunk, chunk['chunk_num'], len(chunks))
                        chunk_results.append(result)
                    
                    # Merge all results
                    print(f"\nMerging results from {len(chunk_results)} chunks...")
                    merged_result = merge_extraction_results(chunk_results)
                    
                    # Save results to GCS
                    final_path = save_extraction_results_to_gcs(bucket, merged_result, carrier_name, safe_carrier_name, 'liquorPDF', report_timestamp)
                    
                    all_results.append({
                        'carrierName': carrier_name,
                        'fileType': 'liquorPDF',
                        'finalFields': f'gs://{BUCKET_NAME}/{final_path}',
                        'totalFields': len([k for k in merged_result.keys() if not k.startswith('_')]),
                        'fieldsFound': len([k for k, v in merged_result.items() if v is not None and not k.startswith('_')])
                    })
                    
                except Exception as e:
                    print(f"Error processing {carrier_name} liquor: {e}")
                    all_results.append({
                        'carrierName': carrier_name,
                        'fileType': 'liquorPDF',
                        'error': str(e)
                    })
    
    result = {
        "success": True,
        "uploadId": upload_id,
        "results": all_results
    }
    
    # Check if all carriers in this upload have completed Phase 3 Liquor
    print("\n✅ Phase 3 Liquor LLM extraction complete!")
    print("🔍 Checking if all carriers are complete...")
    
    if _check_if_all_carriers_complete_liquor(bucket, upload_id):
        print("🎉 ALL LIQUOR CARRIERS COMPLETE! Auto-triggering Google Sheets finalization...")
        try:
            from phase5_googlesheet import finalize_upload_to_sheets
            sheets_result = finalize_upload_to_sheets(upload_id)
            if sheets_result.get('success'):
                print("✅ Google Sheets finalization complete!")
                result['sheets_push'] = sheets_result
            else:
                print(f"⚠️  Google Sheets finalization had issues: {sheets_result.get('error')}")
                result['sheets_push_error'] = sheets_result.get('error')
        except Exception as e:
            print(f"❌ Google Sheets finalization failed: {e}")
            import traceback
            traceback.print_exc()
            result['sheets_push_error'] = str(e)
    else:
        print("⏳ Other Liquor carriers still processing. Waiting for all to complete...")
        print("💡 Or manually run: /finalize-upload/{uploadId}")
    
    return result
