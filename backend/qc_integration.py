"""
QC System Integration Module
Orchestrates the QC pipeline: OCR → Regex Extraction → LLM Field Extraction
"""
import json
import os
import sys
import tempfile
from pathlib import Path
from datetime import datetime
from google.cloud import storage
from dotenv import load_dotenv

load_dotenv()

# Set Google credentials
google_cloud_credentials = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
if google_cloud_credentials:
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = google_cloud_credentials

# Initialize GCS
client = storage.Client()
bucket_name = os.getenv('BUCKET_NAME', 'deployment')
bucket = client.get_bucket(bucket_name)


def download_pdf_from_gcs(gcs_path: str, local_path: str) -> bool:
    """
    Download a PDF from GCS to local filesystem
    
    Args:
        gcs_path: GCS path (gs://bucket/path/file.pdf)
        local_path: Local filesystem path
    
    Returns:
        True if successful, False otherwise
    """
    try:
        # Extract path from gs://bucket/path format
        if gcs_path.startswith('gs://'):
            path_parts = gcs_path.replace('gs://', '').split('/', 1)
            blob_path = path_parts[1] if len(path_parts) > 1 else path_parts[0]
        else:
            blob_path = gcs_path
        
        blob = bucket.blob(blob_path)
        blob.download_to_filename(local_path)
        print(f"  ✓ Downloaded: {gcs_path} → {local_path}")
        return True
    except Exception as e:
        print(f"  ❌ Download failed: {gcs_path}: {e}")
        return False


def upload_json_to_gcs(data: dict, gcs_path: str) -> bool:
    """
    Upload JSON data to GCS
    
    Args:
        data: Dictionary to upload as JSON
        gcs_path: GCS path (e.g., "phase3/results/file.json")
    
    Returns:
        True if successful, False otherwise
    """
    try:
        blob = bucket.blob(gcs_path)
        blob.upload_from_string(
            json.dumps(data, indent=2),
            content_type='application/json'
        )
        print(f"  ✓ Uploaded JSON: {gcs_path}")
        return True
    except Exception as e:
        print(f"  ❌ Upload failed: {gcs_path}: {e}")
        return False


def process_qc_extraction(upload_id: str, policy_pdf_path: str, username: str) -> dict:
    """
    Process QC pipeline: OCR → Regex Extraction → LLM Field Extraction
    
    Args:
        upload_id: Unique ID for this QC upload
        policy_pdf_path: GCS path to policy PDF
        username: Username for tracking
    
    Returns:
        Result dict with:
        {
            "success": bool,
            "upload_id": str,
            "ocr_output": str (GCS path),
            "extraction_results": dict (GCS path),
            "llm_results": dict (GCS path),
            "error": str (if failed)
        }
    """
    temp_dir = None
    
    try:
        print(f"\n🔄 Starting QC pipeline for upload: {upload_id}")
        print(f"   User: {username}")
        
        # Create temporary directory for processing
        temp_dir = tempfile.mkdtemp(prefix=f"qc_{upload_id}_")
        print(f"   Temp dir: {temp_dir}")
        
        # ====== STEP 1: Download policy PDF ======
        print(f"\n📥 Step 1: Downloading policy PDF...")
        local_policy_path = os.path.join(temp_dir, "policy.pdf")
        
        if not download_pdf_from_gcs(policy_pdf_path, local_policy_path):
            raise Exception("Failed to download policy PDF")
        
        # ====== STEP 2: OCR Extraction ======
        print(f"\n📄 Step 2: Running OCR extraction...")
        try:
            # Add qc-system to path to import modules
            qc_system_path = os.path.join(os.path.dirname(__file__), '..', 'qc-system')
            if qc_system_path not in sys.path:
                sys.path.insert(0, qc_system_path)
            
            from extract_policy_ocr import extract_pdf_text
            
            ocr_output_path = os.path.join(temp_dir, "ocr_output.txt")
            extract_pdf_text(local_policy_path, ocr_output_path)
            
            # Read OCR output
            with open(ocr_output_path, 'r', encoding='utf-8') as f:
                ocr_text = f.read()
            
            print(f"  ✓ OCR completed. Output length: {len(ocr_text)} chars")
            
            # Upload OCR output to GCS
            ocr_gcs_path = f"qc/uploads/{upload_id}/ocr_output.txt"
            blob = bucket.blob(ocr_gcs_path)
            blob.upload_from_string(ocr_text, content_type='text/plain')
            print(f"  ✓ Uploaded OCR output: {ocr_gcs_path}")
            
        except Exception as e:
            print(f"  ❌ OCR extraction failed: {e}")
            import traceback
            traceback.print_exc()
            raise Exception(f"OCR extraction failed: {e}")
        
        # ====== STEP 3: Regex Extraction (Heading Extraction) ======
        print(f"\n🔍 Step 3: Running regex extraction...")
        try:
            from qc_heading_extraction import PolicyPageExtractor
            from dataclasses import asdict
            
            # Extract sections from OCR text
            extractor = PolicyPageExtractor(ocr_text, "policy.pdf")
            extraction_results = extractor.process_all_headings()
            
            print(f"  ✓ Regex extraction completed")
            print(f"    - Coverage types found: {list(extraction_results.keys())}")
            
            # Convert dataclasses to dict for JSON serialization
            serializable_results = {}
            for coverage, section in extraction_results.items():
                if section is not None:
                    serializable_results[coverage] = asdict(section)
                else:
                    serializable_results[coverage] = None
            
            # Upload extraction results to GCS
            extraction_gcs_path = f"qc/uploads/{upload_id}/extraction_results.json"
            upload_json_to_gcs(serializable_results, extraction_gcs_path)
            
        except Exception as e:
            print(f"  ❌ Regex extraction failed: {e}")
            import traceback
            traceback.print_exc()
            raise Exception(f"Regex extraction failed: {e}")
        
        # ====== STEP 4: LLM Field Extraction ======
        print(f"\n🤖 Step 4: Running LLM field extraction...")
        try:
            from llm_field_extraction import extract_fields_with_llm, get_llm_client
            
            # Initialize LLM client
            client = get_llm_client()
            print(f"  ✓ Initialized GPT-5 nano client")
            
            # Extract fields for each coverage type
            llm_results = {
                "upload_id": upload_id,
                "coverage_types": {}
            }
            
            # Process GL and Property (main coverage types for QC)
            for coverage in ["GL", "PROPERTY"]:
                if coverage in extraction_results:
                    section = extraction_results[coverage]
                    if section is not None:
                        # Get content from the section
                        content = section.content
                        
                        print(f"  → Processing {coverage} ({len(content)} chars)...")
                        
                        # Extract fields using LLM
                        fields = extract_fields_with_llm(client, coverage, content)
                        llm_results["coverage_types"][coverage] = fields
                        
                        print(f"    ✓ Extracted {len(fields)} fields")
            
            print(f"  ✓ LLM extraction completed")
            print(f"    - Coverage types: {list(llm_results['coverage_types'].keys())}")
            
            # Upload LLM results to GCS
            llm_gcs_path = f"qc/uploads/{upload_id}/llm_results.json"
            upload_json_to_gcs(llm_results, llm_gcs_path)
            
        except Exception as e:
            print(f"  ❌ LLM extraction failed: {e}")
            import traceback
            traceback.print_exc()
            raise Exception(f"LLM extraction failed: {e}")
        
        # ====== STEP 5: Extract Certificate Fields ======
        print(f"\n📜 Step 5: Extracting certificate fields...")
        try:
            certificate_fields = {
                "property": {},
                "gl": {}
            }
            
            # Extract Property Certificate if exists
            property_cert_path = f"qc/uploads/{upload_id}/property_cert.pdf"
            if bucket.blob(property_cert_path).exists():
                local_prop_cert = os.path.join(temp_dir, "property_cert.pdf")
                if download_pdf_from_gcs(property_cert_path, local_prop_cert):
                    print(f"  → Extracting Property Certificate...")
                    certificate_fields["property"] = extract_certificate_fields(
                        local_prop_cert,
                        "PROPERTY"
                    )
            
            # Extract GL Certificate if exists
            gl_cert_path = f"qc/uploads/{upload_id}/gl_cert.pdf"
            if bucket.blob(gl_cert_path).exists():
                local_gl_cert = os.path.join(temp_dir, "gl_cert.pdf")
                if download_pdf_from_gcs(gl_cert_path, local_gl_cert):
                    print(f"  → Extracting GL Certificate...")
                    certificate_fields["gl"] = extract_certificate_fields(
                        local_gl_cert,
                        "GL"
                    )
            
            # Upload certificate extraction results
            cert_extraction_gcs_path = f"qc/uploads/{upload_id}/certificate_extraction.json"
            upload_json_to_gcs(certificate_fields, cert_extraction_gcs_path)
            
        except Exception as e:
            print(f"  ⚠️  Certificate extraction warning (non-critical): {e}")
            certificate_fields = {"property": {}, "gl": {}}
        
        # ====== STEP 6: Compare Policy vs Certificate and Flag ======
        print(f"\n🔴 Step 6: Comparing and flagging fields...")
        try:
            flagged_results = {}
            
            # Compare each coverage type
            for coverage_type in ["PROPERTY", "GL"]:
                coverage_key = coverage_type.lower()
                
                policy_data = llm_results["coverage_types"].get(coverage_type, {})
                cert_data = certificate_fields.get(coverage_key, {})
                
                if policy_data:
                    flagged_results[coverage_key] = compare_and_flag_fields(
                        policy_data,
                        cert_data
                    )
                    
                    # Count matches/mismatches
                    matches = sum(1 for f in flagged_results[coverage_key].values() if f.get("color") == "green")
                    mismatches = sum(1 for f in flagged_results[coverage_key].values() if f.get("color") == "red")
                    not_in_cert = sum(1 for f in flagged_results[coverage_key].values() if f.get("color") == "black")
                    
                    print(f"  {coverage_type}: {matches}✅ {mismatches}❌ {not_in_cert}⚫")
            
            # Upload flagged results to GCS
            flagged_gcs_path = f"qc/uploads/{upload_id}/flagged_results.json"
            upload_json_to_gcs(flagged_results, flagged_gcs_path)
            
        except Exception as e:
            print(f"  ❌ Comparison failed: {e}")
            import traceback
            traceback.print_exc()
            raise Exception(f"Comparison failed: {e}")
        
        # ====== Success ======
        print(f"\n✅ QC pipeline completed successfully")
        
        return {
            "success": True,
            "upload_id": upload_id,
            "username": username,
            "ocr_output_path": ocr_gcs_path,
            "extraction_results_path": extraction_gcs_path,
            "llm_results_path": llm_gcs_path,
            "flagged_results_path": flagged_gcs_path,
            "processed_at": datetime.now().isoformat(),
            "coverage_types": list(llm_results.get('coverage_types', {}).keys())
        }
    
    except Exception as e:
        print(f"\n❌ QC pipeline failed: {e}")
        import traceback
        traceback.print_exc()
        
        return {
            "success": False,
            "upload_id": upload_id,
            "error": str(e)
        }
    
    finally:
        # Clean up temporary files
        if temp_dir and os.path.exists(temp_dir):
            import shutil
            try:
                shutil.rmtree(temp_dir)
                print(f"\n🗑️  Cleaned up temp directory: {temp_dir}")
            except Exception as e:
                print(f"\n⚠️  Failed to clean temp directory: {e}")


def extract_certificate_fields(cert_pdf_path: str, cert_type: str):
    """
    Extract fields from certificate PDF using OCR + LLM
    
    Args:
        cert_pdf_path: Local path to certificate PDF
        cert_type: "PROPERTY" or "GL"
    
    Returns:
        Dict with extracted certificate fields
    """
    try:
        from extract_policy_ocr import extract_pdf_text
        from llm_field_extraction import extract_fields_with_llm, get_llm_client
        
        # 1. OCR the certificate
        cert_ocr_path = cert_pdf_path.replace('.pdf', '_ocr.txt')
        extract_pdf_text(cert_pdf_path, cert_ocr_path)
        
        with open(cert_ocr_path, 'r', encoding='utf-8') as f:
            cert_ocr = f.read()
        
        print(f"  ✓ OCR complete for {cert_type} certificate ({len(cert_ocr)} chars)")
        
        # 2. Extract fields using LLM
        client = get_llm_client()
        cert_fields = extract_fields_with_llm(client, cert_type, cert_ocr)
        
        print(f"  ✓ LLM extraction complete for {cert_type} ({len(cert_fields)} fields)")
        
        # Clean up temp OCR file
        if os.path.exists(cert_ocr_path):
            os.remove(cert_ocr_path)
        
        return cert_fields
    
    except Exception as e:
        print(f"  ❌ Error extracting {cert_type} certificate: {e}")
        import traceback
        traceback.print_exc()
        return {}


def compare_and_flag_fields(policy_fields: dict, cert_fields: dict) -> dict:
    """
    Compare policy fields vs certificate fields and assign color flags
    Recursively handles nested objects (dicts)
    
    Args:
        policy_fields: Extracted fields from policy
        cert_fields: Extracted fields from certificate
    
    Returns:
        Dict with policy fields + color flags (flattened with nested keys)
    """
    from difflib import SequenceMatcher
    
    def compare_values(policy_val, cert_val):
        """Helper to compare two values and return color"""
        if policy_val is None or policy_val == "":
            return "black"
        
        if cert_val is None:
            return "black"
        
        if cert_val == "" and policy_val != "":
            return "black"
        
        # Normalize strings
        if isinstance(policy_val, str) and isinstance(cert_val, str):
            policy_str = policy_val.strip().upper()
            cert_str = cert_val.strip().upper()
            
            if policy_str == cert_str:
                return "green"
            
            # Fuzzy match for typos
            ratio = SequenceMatcher(None, policy_str, cert_str).ratio()
            return "green" if ratio > 0.95 else "red"
        
        elif isinstance(policy_val, (int, float)) and isinstance(cert_val, (int, float)):
            return "green" if policy_val == cert_val else "red"
        
        elif isinstance(policy_val, str) or isinstance(cert_val, str):
            # Mixed types, compare as strings
            return "green" if str(policy_val).strip().upper() == str(cert_val).strip().upper() else "red"
        
        else:
            return "red"
    
    def flatten_and_flag(obj, prefix="", cert_obj=None):
        """Recursively flatten objects and assign colors"""
        result = {}
        
        if cert_obj is None:
            cert_obj = {}
        
        for key, policy_value in obj.items():
            if policy_value is None or policy_value == "":
                continue
            
            new_key = f"{prefix} > {key}" if prefix else key
            cert_value = cert_obj.get(key) if isinstance(cert_obj, dict) else None
            
            # Only recurse if value is a dict AND not empty
            if isinstance(policy_value, dict) and len(policy_value) > 0:
                nested_result = flatten_and_flag(
                    policy_value,
                    new_key,
                    cert_value if isinstance(cert_value, dict) else {}
                )
                result.update(nested_result)
            else:
                # Leaf value (string, number, list, or empty dict) - compare and flag
                color = compare_values(policy_value, cert_value)
                result[new_key] = {
                    "value": policy_value,
                    "color": color
                }
        
        return result
    
    flagged = flatten_and_flag(policy_fields, "", cert_fields)
    return flagged


def get_qc_results(upload_id: str) -> dict:
    """
    Retrieve QC results from GCS for a given upload_id
    
    Args:
        upload_id: Unique QC upload ID
    
    Returns:
        Dict with OCR, extraction, LLM, flagged results, and certificate URLs
    """
    try:
        results = {
            "upload_id": upload_id,
            "ocr_output": None,
            "extraction_results": None,
            "llm_results": None,
            "flagged_results": None,
            "certificates": {
                "property": None,
                "gl": None
            }
        }
        
        print(f"📥 Retrieving QC results for upload: {upload_id}")
        
        # Try to download OCR output
        try:
            ocr_path = f"qc/uploads/{upload_id}/ocr_output.txt"
            blob = bucket.blob(ocr_path)
            if blob.exists():
                results["ocr_output"] = blob.download_as_string().decode('utf-8')[:10000]  # First 10k chars
                print(f"✓ OCR output retrieved: {ocr_path}")
        except Exception as e:
            print(f"⚠️  OCR output not found: {e}")
        
        # Try to download extraction results
        try:
            extraction_path = f"qc/uploads/{upload_id}/extraction_results.json"
            blob = bucket.blob(extraction_path)
            if blob.exists():
                results["extraction_results"] = json.loads(blob.download_as_string().decode('utf-8'))
                print(f"✓ Extraction results retrieved: {extraction_path}")
        except Exception as e:
            print(f"⚠️  Extraction results not found: {e}")
        
        # Try to download LLM results
        try:
            llm_path = f"qc/uploads/{upload_id}/llm_results.json"
            blob = bucket.blob(llm_path)
            if blob.exists():
                results["llm_results"] = json.loads(blob.download_as_string().decode('utf-8'))
                print(f"✓ LLM results retrieved: {llm_path}")
        except Exception as e:
            print(f"⚠️  LLM results not found: {e}")
        
        # Try to download flagged results (color-coded for UI) - PRIORITY
        try:
            flagged_path = f"qc/uploads/{upload_id}/flagged_results.json"
            blob = bucket.blob(flagged_path)
            if blob.exists():
                flagged_data = json.loads(blob.download_as_string().decode('utf-8'))
                results["flagged_results"] = flagged_data
                num_fields = sum(len(v) for v in flagged_data.values() if isinstance(v, dict))
                print(f"✓ Flagged results retrieved: {flagged_path} ({num_fields} fields)")
            else:
                print(f"⚠️  Flagged results not found at: {flagged_path} (still processing?)")
        except Exception as e:
            print(f"❌ Error retrieving flagged results: {e}")
            import traceback
            traceback.print_exc()
        
        # Check for certificate PDFs
        property_cert_path = f"qc/uploads/{upload_id}/property_cert.pdf"
        if bucket.blob(property_cert_path).exists():
            results["certificates"]["property"] = f"/qc-cert/{upload_id}/property"
        
        gl_cert_path = f"qc/uploads/{upload_id}/gl_cert.pdf"
        if bucket.blob(gl_cert_path).exists():
            results["certificates"]["gl"] = f"/qc-cert/{upload_id}/gl"
        
        return results
    
    except Exception as e:
        print(f"❌ Failed to get QC results: {e}")
        return {"error": str(e)}


def generate_signed_urls(upload_id: str) -> dict:
    """
    Generate signed URLs for certificate PDFs
    
    Args:
        upload_id: Unique QC upload ID
    
    Returns:
        Dict with signed URLs for property and GL certificates
    """
    try:
        from datetime import timedelta
        
        signed_urls = {
            "property": None,
            "gl": None
        }
        
        # Generate signed URL for property certificate
        try:
            property_cert_path = f"qc/uploads/{upload_id}/property_cert.pdf"
            blob = bucket.blob(property_cert_path)
            # Reload blob metadata to check existence
            if blob.exists():
                url = blob.generate_signed_url(
                    version="v4",
                    expiration=timedelta(hours=24),
                    method="GET"
                )
                signed_urls["property"] = url
                print(f"✓ Generated signed URL for property certificate")
            else:
                print(f"⊘ Property certificate not found: {property_cert_path}")
        except Exception as e:
            print(f"⚠️  Failed to generate property cert URL: {e}")
        
        # Generate signed URL for GL certificate
        try:
            gl_cert_path = f"qc/uploads/{upload_id}/gl_cert.pdf"
            blob = bucket.blob(gl_cert_path)
            # Reload blob metadata to check existence
            if blob.exists():
                url = blob.generate_signed_url(
                    version="v4",
                    expiration=timedelta(hours=24),
                    method="GET"
                )
                signed_urls["gl"] = url
                print(f"✓ Generated signed URL for GL certificate")
            else:
                print(f"⊘ GL certificate not found: {gl_cert_path}")
        except Exception as e:
            print(f"⚠️  Failed to generate GL cert URL: {e}")
        
        return signed_urls
    
    except Exception as e:
        print(f"❌ Failed to generate signed URLs: {e}")
        return {"property": None, "gl": None}

