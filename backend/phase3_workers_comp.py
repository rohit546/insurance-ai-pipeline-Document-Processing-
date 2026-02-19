"""
Phase 3 Workers Comp: LLM Information Extraction for Workers Compensation
Extracts 8 specific workers compensation coverage fields from insurance documents using GPT.
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
from schemas.workers_comp_schema import WORKERS_COMP_FIELDS_SCHEMA, get_workers_comp_field_names, get_workers_comp_required_fields

load_dotenv()

BUCKET_NAME = os.getenv('BUCKET_NAME', 'mckinneysuite')

# Initialize OpenAI
openai.api_key = os.getenv('OPENAI_API_KEY')

if not openai.api_key:
    print("Warning: OPENAI_API_KEY not found in environment variables!")
    print("Phase 3 Workers Comp LLM extraction will fail without OpenAI API key")


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
    """Extract information using LLM with Workers Comp prompt"""
    
    prompt = f"""
    Analyze the following workers compensation insurance document text and extract ONLY the 8 specific workers compensation coverage fields listed below.
    
    CRITICAL: Extract ONLY these 8 fields. Do NOT create new field names or extract any other information.
    
    THE 8 SPECIFIC FIELDS TO EXTRACT (with examples of what to look for):
    1. Limits - Look for: "$1,000,000 Each Accident", "$1,000,000 Policy Limit", "$1,000,000 Each Employee", "$500,000 / $500,000 / $500,000", "$1,000,000 / $1,000,000 / $1,000,000", any workers compensation limits with dollar amounts
    2. FEIN # - Look for: "47-4792684", "39-4013959", "33-4251695", any Federal Employer Identification Numbers (FEIN)
    3. Payroll - Subject to Audit - Look for: "$36,000", "$45,000", "$30,000", any payroll amounts subject to audit
    4. Excluded Officer - Look for: "Parvez Jiwani", "Provide Details", "Details Required", "Officer decision on Inclusion / Exclusion required", any excluded officer information
    5. If Opting out from Workers Compensation Coverage - Look for: "By State Law in GA you are liable --- by not opting any injuries to the employees during work hours will not be covered", any opt-out information or liability statements
    6. Workers Compensation Premium - Look for: "$1,500.00", "WC Premium", "Workers Comp Premium", "TOTAL excl Terrorism", "TOTAL CHARGES W/O TRIA", any workers compensation premium amount (PRIORITY: Look for "TOTAL excl Terrorism" or "TOTAL CHARGES W/O TRIA" first)
    7. Total Premium - Look for: "$3,500.00", "TOTAL incl Terrorism", "TOTAL CHARGES WITH TRIA", "Total Premium", "Annual Premium", any total premium amount
    8. Policy Premium - Look for: "$2,500.00", "Policy Premium", "Base Premium", "Workers Compensation" base amount, any policy premium amount
    
    EXTRACTION RULES:
    - Extract EXACTLY as written in the document
    - Look for SIMILAR PATTERNS even if exact examples don't match
    - For Limits: Look for dollar amounts with "/" separator (e.g., "$X,XXX,XXX / $X,XXX,XXX")
    - For FEIN #: Look for numeric patterns like "XX-XXXXXXX" (Federal Employer Identification Numbers)
    - For Dollar Amounts: Look for any dollar amounts ($X,XXX, $X,XXX.XX, $XXX,XXX)
    - For Officer Information: Extract complete officer details, names, and exclusion/inclusion status
    - For Opt-out Information: Extract complete liability statements and opt-out conditions
    - For Multi-line Values: Extract everything related to that field, preserve line breaks
    - For Complex Values: Extract the complete text block for that field
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
    
    CRITICAL: Return ONLY valid JSON with this exact format:
    {{
        "Limits": {{"value": "$1,000,000 Each Accident, $1,000,000 Policy Limit, $1,000,000 Each Employee", "page": 5}},
        "FEIN #": {{"value": "47-4792684", "page": 5}},
        "Payroll - Subject to Audit": {{"value": "$36,000", "page": 5}},
        "Excluded Officer": {{"value": "Parvez Jiwani", "page": 5}},
        "If Opting out from Workers Compensation Coverage": {{"value": "By State Law in GA you are liable --- by not opting any injuries to the employees during work hours will not be covered", "page": 3}},
        "Workers Compensation Premium": {{"value": "$1,500.00", "page": 3}},
        "Total Premium": {{"value": "$3,500.00", "page": 3}},
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
    
    # Use schema to initialize Workers Comp fields in correct order
    # This ensures Google Sheets always has consistent field ordering
    expected_field_names = get_workers_comp_field_names()  # From Workers Comp schema - guaranteed order
    
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

