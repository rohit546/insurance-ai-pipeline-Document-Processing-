from fastapi import FastAPI, UploadFile, File, HTTPException, Header, Form, Request
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
from google.cloud import storage
import os
from tasks import celery_app
import tempfile
import secrets
from datetime import datetime
import time
from google.api_core.exceptions import ServiceUnavailable, InternalServerError
from auth import register, login
from database import get_all_users, user_exists_by_email, create_user, get_user
from upload_handler import process_carrier_uploads, get_upload_history
from dotenv import load_dotenv
import os
from phase1 import process_upload_lengths, process_upload_quality_analysis
# from phase2_ocr import process_upload_ocr_analysis
from phase2_ocr_nano import process_upload_ocr_analysis
from system_resources import probe_resources

load_dotenv()

# Set Google credentials - check multiple possible locations
google_cloud_credentials = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')

# If not set, check common locations
if not google_cloud_credentials:
    # Check Railway deployment path
    railway_creds = '/app/credentials/insurance-sheets-474717-7fc3fd9736bc.json'
    if os.path.exists(railway_creds):
        google_cloud_credentials = railway_creds
    else:
        # Check local development path
        local_creds = os.path.join(os.path.dirname(__file__), '..', 'credentials', 'insurance-sheets-474717-7fc3fd9736bc.json')
        local_creds = os.path.abspath(local_creds)
        if os.path.exists(local_creds):
            google_cloud_credentials = local_creds

# Set the environment variable if we found credentials
if google_cloud_credentials:
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = google_cloud_credentials
    print(f"[OK] Using Google credentials from: {google_cloud_credentials}")
else:
    print("[WARN] GOOGLE_APPLICATION_CREDENTIALS not found")

app = FastAPI()

# Add CORS middleware to allow frontend to connect
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow all origins (for ngrok + Vercel)
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize storage client - make it resilient to avoid import failures
bucket_name = os.getenv('BUCKET_NAME', 'mckinneysuite')
_client = None
_bucket = None

def get_storage_client():
    """Get or create Google Cloud Storage client"""
    global _client
    if _client is None:
        if not google_cloud_credentials or not os.path.exists(google_cloud_credentials):
            raise Exception("Google Cloud credentials not found. Please set GOOGLE_APPLICATION_CREDENTIALS.")
        _client = storage.Client()
    return _client

def get_bucket():
    """Get or create Google Cloud Storage bucket"""
    global _bucket
    if _bucket is None:
        client = get_storage_client()
        _bucket = client.get_bucket(bucket_name)
    return _bucket

# Try to initialize at import time, but don't fail if it doesn't work
try:
    if google_cloud_credentials and os.path.exists(google_cloud_credentials):
        _client = storage.Client()
        _bucket = _client.get_bucket(bucket_name)
        print("[OK] Storage client initialized in app.py")
    else:
        print("[WARN] Storage client will be initialized lazily in app.py")
except Exception as e:
    print(f"[WARN] Could not initialize storage client in app.py: {e}")
    print("   Storage will be initialized lazily when needed")

# For backward compatibility
client = _client
bucket = _bucket

@app.get("/")
def read_root():
    return {"message": "Hello, World! Insurance PDF Analysis API"}

@app.post("/register/")
def register_endpoint(email: str = Form(None), username: str = Form(None), password: str = Form(...)):
    """Register new user with email/username"""
    user_identifier = email or username
    if not user_identifier:
        raise HTTPException(status_code=400, detail="Email or username is required")
    result = register(user_identifier, password)
    if "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])
    return result

@app.post("/login/")
def login_endpoint(email: str = Form(None), username: str = Form(None), password: str = Form(...)):
    """Login user with email/username"""
    user_identifier = email or username
    if not user_identifier:
        raise HTTPException(status_code=400, detail="Email or username is required")
    result = login(user_identifier, password)
    if "error" in result:
        raise HTTPException(status_code=401, detail=result["error"])
    return result

@app.get("/health")
def health_check():
    return {"status": "healthy"}

@app.get("/test-celery")
def test_celery():
    """Test Celery connectivity and task queuing"""
    try:
        from tasks import celery_app, example_task
        
        # Check if Celery app is configured
        broker_url = celery_app.conf.get('broker_url', 'Not set')
        result_backend = celery_app.conf.get('result_backend', 'Not set')
        
        # Try to queue a simple test task
        try:
            task = example_task.delay("test")
            task_id = task.id if task else None
            
            return {
                "celery_configured": True,
                "broker_url": broker_url[:50] + "..." if len(str(broker_url)) > 50 else broker_url,
                "result_backend": result_backend[:50] + "..." if len(str(result_backend)) > 50 else result_backend,
                "test_task_queued": task_id is not None,
                "test_task_id": task_id,
                "message": "Celery appears to be working" if task_id else "Warning: Task queued but no ID returned"
            }
        except Exception as task_error:
            return {
                "celery_configured": True,
                "broker_url": broker_url[:50] + "..." if len(str(broker_url)) > 50 else broker_url,
                "result_backend": result_backend[:50] + "..." if len(str(result_backend)) > 50 else result_backend,
                "test_task_queued": False,
                "test_task_id": None,
                "error": str(task_error),
                "message": "Celery is configured but task queuing failed"
            }
    except Exception as e:
        return {
            "celery_configured": False,
            "error": str(e),
            "message": "Celery is not properly configured"
        }


@app.get("/system/resources")
def system_resources():
    """
    Debug endpoint: report effective CPU/RAM limits inside container (cgroups),
    CPU affinity, and key env knobs (Celery concurrency, Joblib threads).
    """
    return {"success": True, "resources": probe_resources()}

@app.get("/test-openai")
async def test_openai():
    """Test OpenAI API connectivity from Railway - diagnostic endpoint"""
    import socket
    import openai
    from openai import OpenAI
    
    cloudflare_gateway = os.getenv('CLOUDFLARE_GATEWAY_URL')
    
    results = {
        "dns_test": None,
        "api_connection_test": None,
        "responses_api_test": None,
        "api_key_set": bool(os.getenv('OPENAI_API_KEY')),
        "api_key_length": len(os.getenv('OPENAI_API_KEY', '')),
        "railway_environment": bool(os.getenv('RAILWAY_ENVIRONMENT') or os.getenv('RAILWAY_SERVICE_NAME')),
        "environment": os.getenv('RAILWAY_ENVIRONMENT', 'unknown'),
        "cloudflare_gateway_enabled": bool(cloudflare_gateway),
        "cloudflare_gateway_url": cloudflare_gateway[:60] + "..." if cloudflare_gateway and len(cloudflare_gateway) > 60 else cloudflare_gateway
    }
    
    # Test DNS resolution
    try:
        ip_address = socket.gethostbyname('api.openai.com')
        results["dns_test"] = f"OK - Resolved to {ip_address}"
    except socket.gaierror as e:
        results["dns_test"] = f"FAILED: DNS resolution error - {str(e)}"
    except Exception as e:
        results["dns_test"] = f"FAILED: {type(e).__name__} - {str(e)}"
    
    # Test API connection (without making actual request)
    try:
        api_key = os.getenv('OPENAI_API_KEY')
        if not api_key:
            results["api_connection_test"] = "SKIPPED: No API key set" 
        else:
            # Try to create client and test connection
            client_params = {
                "api_key": api_key,
                "timeout": 10.0,
                "max_retries": 0
            }
            if cloudflare_gateway:
                client_params["base_url"] = cloudflare_gateway
            
            client = OpenAI(**client_params)
            # Try a minimal request to test connectivity
            try:
                # Test with a simple API call - models.list() doesn't take limit parameter
                response = client.models.list(timeout=10.0)
                # Convert to list to check if we got data
                models_list = list(response)
                # If we get here, connection works!
                results["api_connection_test"] = f"SUCCESS: Can connect to OpenAI API (found {len(models_list)} models)"
            except openai.APIConnectionError as e:
                results["api_connection_test"] = f"FAILED: Connection error - {str(e)[:200]}"
            except openai.APITimeoutError as e:
                results["api_connection_test"] = f"FAILED: Timeout error - {str(e)[:200]}"
            except openai.AuthenticationError as e:
                results["api_connection_test"] = f"AUTH_ERROR: API key invalid - {str(e)[:200]}"
            except (ConnectionError, TimeoutError, socket.timeout) as e:
                results["api_connection_test"] = f"FAILED: Network error - {type(e).__name__}: {str(e)[:200]}"
            except Exception as e:
                results["api_connection_test"] = f"ERROR: {type(e).__name__} - {str(e)[:200]}"
    except Exception as e:
        results["api_connection_test"] = f"FAILED: {type(e).__name__} - {str(e)[:200]}"
    
    # Test the actual Responses API endpoint used in phase3_llm.py
    try:
        api_key = os.getenv('OPENAI_API_KEY')
        if api_key:
            client_params = {
                "api_key": api_key,
                "timeout": 30.0,
                "max_retries": 0
            }
            if cloudflare_gateway:
                client_params["base_url"] = cloudflare_gateway
            
            client = OpenAI(**client_params)
            # Test the chat.completions.create() endpoint (what phase3_llm.py uses)
            try:
                response = client.chat.completions.create(
                    model="gpt-4o-mini",
                    messages=[
                        {"role": "user", "content": "Say 'test' if you can read this."}
                    ],
                    timeout=30.0
                )
                content = response.choices[0].message.content
                results["responses_api_test"] = f"SUCCESS: Chat API works - Response: {content[:50]}"
            except openai.APIConnectionError as e:
                results["responses_api_test"] = f"FAILED: Connection error - {str(e)[:200]}"
            except openai.APITimeoutError as e:
                results["responses_api_test"] = f"FAILED: Timeout - {str(e)[:200]}"
            except openai.AuthenticationError as e:
                results["responses_api_test"] = f"AUTH_ERROR: {str(e)[:200]}"
            except AttributeError as e:
                results["responses_api_test"] = f"API_NOT_AVAILABLE: chat.completions.create() not found - {str(e)[:200]}"
            except Exception as e:
                results["responses_api_test"] = f"ERROR: {type(e).__name__} - {str(e)[:200]}"
        else:
            results["responses_api_test"] = "SKIPPED: No API key"
    except Exception as e:
        results["responses_api_test"] = f"FAILED: {type(e).__name__} - {str(e)[:200]}"
    
    return results

@app.post("/upload-quotes/")
async def upload_quotes(
    request: Request,
    carriers_json: str = Form(...),
    files: list = File(...)
):
    """
    Upload multiple carrier quotes
    
    Form data:
    - carriers_json: JSON string with carrier names
    - files: List of PDF files (property1, liability1, property2, liability2, ...)
    
    Headers:
    - X-User-ID: User ID for routing to user-specific sheet tab
    
    Example:
    {
      "carriers": [
        {"name": "State Farm"},
        {"name": "Allstate"}
      ]
    }
    """
    try:
        import json
        
        # Extract username from headers or use default
        username = request.headers.get('X-User-ID', 'default')
        print(f"📝 Processing upload for user: {username}")
        
        # Parse carriers data
        carriers_info = json.loads(carriers_json)
        carriers = carriers_info.get("carriers", [])
        
        if not carriers:
            raise HTTPException(status_code=400, detail="No carriers provided")
        
        # Files can be 0 to 4 per carrier (property, liability, liquor, workersComp) - completely optional
        min_files = 0
        max_files = len(carriers) * 4
        
        if len(files) < min_files or len(files) > max_files:
            raise HTTPException(
                status_code=400,
                detail=f"Expected 0-{max_files} files for {len(carriers)} carriers, got {len(files)}"
            )
        
        # Process files for each carrier
        carriers_data = []
        
        # Initialize all carriers with None
        for carrier in carriers:
            carriers_data.append({
                "carrierName": carrier.get("name", f"Carrier_{len(carriers_data)+1}"),
                "propertyPDF": None,
                "liabilityPDF": None,
                "liquorPDF": None,
                "workersCompPDF": None
            })
        
        # Get file metadata list
        form_data = await request.form()
        file_metadata_list = form_data.getlist('file_metadata')
        
        # Process each file with its metadata
        for idx, file in enumerate(files):
            if idx < len(file_metadata_list):
                try:
                    metadata = json.loads(file_metadata_list[idx])
                    carrier_index = metadata.get('carrierIndex')
                    file_type = metadata.get('type')
                    
                    # Read file
                    file_content = await file.read()
                    
                    # Assign to correct carrier and type
                    if 0 <= carrier_index < len(carriers_data):
                        if file_type == 'property':
                            carriers_data[carrier_index]['propertyPDF'] = file_content
                        elif file_type == 'liability':
                            carriers_data[carrier_index]['liabilityPDF'] = file_content
                        elif file_type == 'liquor':
                            carriers_data[carrier_index]['liquorPDF'] = file_content
                        elif file_type == 'workersComp':
                            carriers_data[carrier_index]['workersCompPDF'] = file_content
                except Exception as e:
                    print(f"Error processing file metadata: {e}")
                    raise HTTPException(status_code=400, detail=f"Error processing file: {str(e)}")
        
        # Process uploads (username already extracted from headers above)
        result = process_carrier_uploads(carriers_data, username)
        
        if not result["success"]:
            raise HTTPException(status_code=500, detail=result.get("message"))
        
        return result
    
    except HTTPException:
        raise
    except Exception as e:
        print(f"ERROR in upload_quotes: {str(e)}")
        import traceback
        print("Full traceback:")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Server error: {str(e)}")

@app.get("/upload-history/")
def get_history(user_id: str = None):
    """
    Get upload history for a user or all uploads
    """
    try:
        result = get_upload_history(user_id)
        if not result["success"]:
            raise HTTPException(status_code=500, detail=result.get("error"))
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/confirm-upload/")
def confirm_upload(
    uploadId: str = Form(...)
):
    """
    Confirm upload execution
    
    This endpoint is called after the user reviews the uploaded files
    and confirms they want to proceed.
    """
    try:
        return {
            "success": True,
            "message": f"Upload confirmed",
            "uploadId": uploadId
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/phase1/process")
def process_phase1(uploadId: str):
    try:
        result = process_upload_lengths(uploadId)
        if not result.get("success"):
            raise HTTPException(status_code=404, detail=result.get("error", "Unknown error"))
        return result
    except HTTPException:
        raise
    except Exception as e:
        print(f"ERROR in process_phase1: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Server error: {str(e)}")

@app.get("/phase1/quality-analysis")
def analyze_quality(uploadId: str):
    """
    Queue Phase 1 quality analysis task.
    Files are already uploaded to GCS - this just queues the processing.
    Returns immediately so users don't wait.
    Processing happens in background queue (one at a time).
    """
    try:
        from tasks import process_phase1_task
        # Queue the task - files are already in GCS from upload step
        # User ID is stored in metadata and will be retrieved during processing
        task = process_phase1_task.delay(uploadId)
        print(f"✅ Phase 1 queued for upload: {uploadId}, Task ID: {task.id}")
        return {
            "success": True,
            "message": f"Processing queued. Your upload will be processed shortly.",
            "uploadId": uploadId,
            "taskId": task.id,
            "status": "queued"
        }
    except Exception as e:
        print(f"ERROR in analyze_quality: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Server error: {str(e)}")

@app.get("/phase2/ocr-analysis")
def analyze_ocr(uploadId: str):
    """
    Run OCR on all PDF pages using Tesseract.
    Can be called manually or automatically triggered after Phase 1.
    """
    try:
        result = process_upload_ocr_analysis(uploadId)
        if not result.get("success"):
            raise HTTPException(status_code=404, detail=result.get("error", "Unknown error"))
        return result
    except HTTPException:
        raise
    except Exception as e:
        print(f"ERROR in analyze_ocr: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Server error: {str(e)}")


@app.get("/phase2d/intelligent-combination")
def intelligent_combination(uploadId: str):
    """
    Create intelligent combined file from Phase 2C smart selection results.
    Automatically triggered after Phase 2C completes.
    """
    try:
        from phase2d_intelligent_combination import process_upload_intelligent_combination
        result = process_upload_intelligent_combination(uploadId)
        if not result.get("success"):
            raise HTTPException(status_code=404, detail=result.get("error", "Unknown error"))
        return result
    except HTTPException:
        raise
    except Exception as e:
        print(f"ERROR in intelligent_combination: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Server error: {str(e)}")


@app.get("/phase3/llm-extraction")
def llm_extraction(uploadId: str):
    """
    Extract insurance fields using GPT from Phase 2D intelligent combined file.
    Automatically triggered after Phase 2D completes.
    """
    try:
        from phase3_llm import process_upload_llm_extraction
        result = process_upload_llm_extraction(uploadId)
        if not result.get("success"):
            raise HTTPException(status_code=404, detail=result.get("error", "Unknown error"))
        return result
    except HTTPException:
        raise
    except Exception as e:
        print(f"ERROR in llm_extraction: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Server error: {str(e)}")


@app.get("/phase5/googlesheets-push")
def googlesheets_push(uploadId: str, sheetName: str = "Insurance Fields Data"):
    """
    DEPRECATED: Use /finalize-upload instead.
    This endpoint pushes individual carriers (causes overwriting).
    """
    try:
        from phase5_googlesheet import process_upload_googlesheets_push
        result = process_upload_googlesheets_push(uploadId, sheetName)
        if not result.get("success"):
            raise HTTPException(status_code=404, detail=result.get("error", "Unknown error"))
        return result
    except HTTPException:
        raise
    except Exception as e:
        print(f"ERROR in googlesheets_push: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Server error: {str(e)}")


@app.get("/upload-status/{upload_id}")
def get_upload_status(upload_id: str):
    """
    Check if all phases (OCR + LLM extraction) are complete for an upload.
    Returns:
        - processing: true if still processing, false if complete
        - ready: true if finalize-upload can be called
        - phase3_complete: true if Phase 3 LLM extraction is done
    """
    try:
        from google.cloud import storage
        import re
        
        # Get bucket
        bucket_name = os.getenv('BUCKET_NAME', 'mckinneysuite')
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        
        # Read metadata to get carriers
        from phase1 import _read_metadata
        full_metadata = _read_metadata(bucket)
        uploads = full_metadata.get('uploads', [])
        upload_record = next((u for u in uploads if u.get('uploadId') == upload_id), None)
        
        if not upload_record:
            raise HTTPException(status_code=404, detail=f"Upload {upload_id} not found")
        
        carriers = upload_record.get('carriers', [])
        if not carriers:
            raise HTTPException(status_code=404, detail="No carriers found in upload")
        
        # Count expected Phase 3 results and how many are complete
        expected_files = 0
        completed_files = 0
        
        for carrier in carriers:
            carrier_name = carrier.get('carrierName', 'Unknown')
            safe_name = carrier_name.lower().replace(" ", "_").replace("&", "and")
            
            # Check each file type (property, liability, liquor, workersComp)
            for file_type in ['propertyPDF', 'liabilityPDF', 'liquorPDF', 'workersCompPDF']:
                pdf_info = carrier.get(file_type)
                if not pdf_info or not pdf_info.get('path'):
                    continue
                
                expected_files += 1
                
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
                    completed_files += 1
        
        is_complete = completed_files == expected_files and expected_files > 0
        
        return {
            "success": True,
            "uploadId": upload_id,
            "processing": not is_complete,
            "ready": is_complete,
            "phase3_complete": is_complete,
            "completed_files": completed_files,
            "expected_files": expected_files,
            "carriers": len(carriers)
        }
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"ERROR in get_upload_status: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Server error: {str(e)}")


@app.get("/finalize-upload")
def finalize_upload(uploadId: str, sheetName: str = "Insurance Fields Data"):
    """
    Finalize upload: Push ALL carriers to Google Sheets in side-by-side format.
    Should be called AFTER all carriers complete Phase 3.
    This prevents individual carriers from overwriting each other.
    """
    try:
        from phase5_googlesheet import finalize_upload_to_sheets
        result = finalize_upload_to_sheets(uploadId, sheetName)
        if not result.get("success"):
            raise HTTPException(status_code=500, detail=result.get("error", "Unknown error"))
        return result
    except HTTPException:
        raise
    except Exception as e:
        print(f"ERROR in finalize_upload: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Server error: {str(e)}")


# ============================================================
# QC Endpoints (Unified Certificate + Policy)
# ============================================================


def upload_with_retry(blob, content: bytes, content_type: str, max_retries: int = 3):
    """
    Upload to GCS with retry logic for SSL/network errors
    """
    import ssl
    from google.api_core.exceptions import ServiceUnavailable, InternalServerError
    
    for attempt in range(max_retries):
        try:
            blob.upload_from_string(content, content_type=content_type)
            return True
        except (ssl.SSLError, ssl.SSLEOFError, ServiceUnavailable, InternalServerError) as e:
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt  # Exponential backoff: 1s, 2s, 4s
                print(f"⚠️ Upload attempt {attempt + 1} failed (SSL/Network error): {e}")
                print(f"   Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                print(f"❌ Upload failed after {max_retries} attempts: {e}")
                raise
        except Exception as e:
            # For other errors, don't retry
            print(f"❌ Upload failed with non-retryable error: {e}")
            raise
    return False

@app.post("/qc-new/upload-unified")
async def qc_new_upload_unified(
    pl_certificate_pdf: UploadFile = File(None, description="PL Certificate PDF"),
    gl_certificate_pdf: UploadFile = File(None, description="GL Certificate PDF"),
    acord_certificate_pdf: UploadFile = File(None, description="ACORD Certificate PDF (Property)"),
    gl_acord_certificate_pdf: UploadFile = File(None, description="GL ACORD Certificate PDF"),
    policy_pdf: UploadFile = File(..., description="Policy PDF"),
    username: str = Form(...),
):
    """
    Unified QC upload: PL certificate + GL certificate + policy together.
    Runs cert extraction for both PL and GL, then policy validation against both.
    
    At least one certificate (PL or GL) must be provided.
    """
    print(f"🔵 [QC UPLOAD] ========== REQUEST RECEIVED ==========")
    print(f"🔵 [QC UPLOAD] Endpoint: /qc-new/upload-unified")
    print(f"🔵 [QC UPLOAD] Username: {username}")
    
    try:
        # Get bucket (lazy initialization)
        bucket = get_bucket()
        print(f"🔵 [QC UPLOAD] Bucket initialized successfully")
        
        # Validate that at least one primary certificate is provided (ACORD is optional for now)
        if not pl_certificate_pdf and not gl_certificate_pdf:
            print(f"🔴 [QC UPLOAD] ERROR: No certificates provided")
            raise HTTPException(
                status_code=400,
                detail="At least one certificate (PL or GL) must be provided",
            )
        
        # Generate upload ID
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        upload_id = f"qcnew_{timestamp}_{secrets.token_hex(6)}"
        
        has_pl = pl_certificate_pdf is not None
        has_gl = gl_certificate_pdf is not None
        has_acord = acord_certificate_pdf is not None
        has_gl_acord = gl_acord_certificate_pdf is not None
        print(f"📥 Unified QC upload: {upload_id} by {username}")
        print(f"   PL Certificate: {'✅' if has_pl else '❌'}")
        print(f"   GL Certificate: {'✅' if has_gl else '❌'}")
        print(f"   ACORD Certificate (Property): {'✅' if has_acord else '❌'}")
        print(f"   GL ACORD Certificate: {'✅' if has_gl_acord else '❌'}")
        print(f"   Policy: ✅")
        
        uploaded_files = {}
        
        # Upload PL certificate PDF to GCS with retry (if provided)
        if pl_certificate_pdf:
            pl_cert_blob_name = f"qc-new/uploads/{upload_id}/pl_certificate.pdf"
            blob_pl_cert = bucket.blob(pl_cert_blob_name)
            pl_cert_contents = await pl_certificate_pdf.read()
            upload_with_retry(blob_pl_cert, pl_cert_contents, "application/pdf")
            uploaded_files["pl_certificate"] = pl_cert_blob_name
            print(f"✅ PL Cert uploaded: {pl_cert_blob_name}")
        
        # Upload GL certificate PDF to GCS with retry (if provided)
        if gl_certificate_pdf:
            gl_cert_blob_name = f"qc-new/uploads/{upload_id}/gl_certificate.pdf"
            blob_gl_cert = bucket.blob(gl_cert_blob_name)
            gl_cert_contents = await gl_certificate_pdf.read()
            upload_with_retry(blob_gl_cert, gl_cert_contents, "application/pdf")
            uploaded_files["gl_certificate"] = gl_cert_blob_name
            print(f"✅ GL Cert uploaded: {gl_cert_blob_name}")

        # Upload ACORD certificate PDF to GCS with retry (if provided)
        if acord_certificate_pdf:
            acord_blob_name = f"qc-new/uploads/{upload_id}/acord_certificate.pdf"
            blob_acord_cert = bucket.blob(acord_blob_name)
            acord_contents = await acord_certificate_pdf.read()
            upload_with_retry(blob_acord_cert, acord_contents, "application/pdf")
            uploaded_files["acord_certificate"] = acord_blob_name
            print(f"✅ ACORD Cert uploaded: {acord_blob_name}")

        # Upload GL ACORD certificate PDF to GCS with retry (if provided)
        if gl_acord_certificate_pdf:
            gl_acord_blob_name = f"qc-new/uploads/{upload_id}/gl_acord_certificate.pdf"
            blob_gl_acord_cert = bucket.blob(gl_acord_blob_name)
            gl_acord_contents = await gl_acord_certificate_pdf.read()
            upload_with_retry(blob_gl_acord_cert, gl_acord_contents, "application/pdf")
            uploaded_files["gl_acord_certificate"] = gl_acord_blob_name
            print(f"✅ GL ACORD Cert uploaded: {gl_acord_blob_name}")
        
        # Upload policy PDF to GCS with retry
        policy_blob_name = f"qc-new/uploads/{upload_id}/policy.pdf"
        blob_policy = bucket.blob(policy_blob_name)
        policy_contents = await policy_pdf.read()
        upload_with_retry(blob_policy, policy_contents, "application/pdf")
        uploaded_files["policy"] = policy_blob_name
        print(f"✅ Policy uploaded: {policy_blob_name}")
        
        # Queue unified task
        try:
            from tasks import process_qc_new_unified_task
            print(f"📤 Attempting to queue Celery task for upload: {upload_id}")
            
            task = process_qc_new_unified_task.delay(
                pl_cert_blob_name=uploaded_files.get("pl_certificate"),
                gl_cert_blob_name=uploaded_files.get("gl_certificate"),
                policy_blob_name=uploaded_files["policy"],
                upload_id=upload_id,
                acord_cert_blob_name=uploaded_files.get("acord_certificate"),
                gl_acord_cert_blob_name=uploaded_files.get("gl_acord_certificate"),
            )
            
            if not task or not task.id:
                print(f"⚠️  WARNING: Task queued but no task ID returned!")
                raise Exception("Failed to queue Celery task - no task ID returned")
            
            print(f"✅ Successfully queued unified task: {task.id}")
            
            return {
                "success": True,
                "upload_id": upload_id,
                "task_id": task.id,
                "message": "QC processing started",
                "has_pl": has_pl,
                "has_gl": has_gl,
                "has_acord": has_acord,
            }
        except Exception as task_error:
            print(f"❌ Failed to queue Celery task: {task_error}")
            import traceback
            traceback.print_exc()
            # Still return success with upload_id so files are saved, but warn about task
            return {
                "success": True,
                "upload_id": upload_id,
                "task_id": None,
                "message": "Files uploaded but task queueing failed. Check Celery worker.",
                "warning": f"Task queue error: {str(task_error)}",
                "has_pl": has_pl,
                "has_gl": has_gl,
                "has_acord": has_acord,
            }
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Unified QC upload failed: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/qc-new/unified-results/{upload_id}")
async def qc_new_unified_results(upload_id: str):
    """
    Get unified QC results (merged JSON with cert, core, coverage validations).
    """
    try:
        print(f"📊 Fetching unified results for: {upload_id}")
        
        results = {
            "upload_id": upload_id,
            "merged": None,
            "success": False,
        }
        
        # Try to fetch merged result
        try:
            merged_path = f"qc-new/uploads/{upload_id}/merged_result.json"
            blob = bucket.blob(merged_path)
            if blob.exists():
                import json
                merged_data = json.loads(blob.download_as_text(encoding="utf-8"))
                results["merged"] = merged_data
                results["success"] = True
                print(f"✅ Found merged result for {upload_id}")
                return results
        except Exception as e:
            print(f"⚠️  Merged fetch failed for {upload_id}: {e}")
        
        # If not ready yet
        return {
            "success": False,
            "error": "Results not ready yet. Try again in a few seconds.",
            "upload_id": upload_id,
        }
        
    except Exception as e:
        print(f"❌ Unified results fetch failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/qc-new/pdf/{upload_id}/{pdf_type}")
async def qc_new_get_pdf(upload_id: str, pdf_type: str):
    """
    Serve the uploaded certificate or policy PDF for viewing.
    pdf_type: 'pl_certificate', 'gl_certificate', 'acord_certificate', 'certificate' (legacy), or 'policy'
    """
    try:
        # Map pdf_type to blob path
        if pdf_type == "pl_certificate":
            blob_path = f"qc-new/uploads/{upload_id}/pl_certificate.pdf"
            filename = "pl_certificate.pdf"
        elif pdf_type == "gl_certificate":
            blob_path = f"qc-new/uploads/{upload_id}/gl_certificate.pdf"
            filename = "gl_certificate.pdf"
        elif pdf_type == "acord_certificate":
            blob_path = f"qc-new/uploads/{upload_id}/acord_certificate.pdf"
            filename = "acord_certificate.pdf"
        elif pdf_type == "gl_acord_certificate":
            blob_path = f"qc-new/uploads/{upload_id}/gl_acord_certificate.pdf"
            filename = "gl_acord_certificate.pdf"
        elif pdf_type == "certificate":
            # Legacy support: try pl_certificate first, then fallback to old path
            blob_path = f"qc-new/uploads/{upload_id}/pl_certificate.pdf"
            blob = bucket.blob(blob_path)
            if not blob.exists():
                blob_path = f"qc-new/uploads/{upload_id}/certificate.pdf"
            filename = "certificate.pdf"
        elif pdf_type == "policy":
            blob_path = f"qc-new/uploads/{upload_id}/policy.pdf"
            filename = "policy.pdf"
        else:
            raise HTTPException(
                status_code=400,
                detail="Invalid PDF type. Must be 'pl_certificate', 'gl_certificate', 'acord_certificate', 'gl_acord_certificate', 'certificate', or 'policy'",
            )
        
        blob = bucket.blob(blob_path)
        
        if not blob.exists():
            raise HTTPException(status_code=404, detail=f"{pdf_type.capitalize()} PDF not found")
        
        # Download PDF content
        pdf_content = blob.download_as_bytes()
        
        # Return as PDF response
        from fastapi.responses import Response
        return Response(
            content=pdf_content,
            media_type="application/pdf",
            headers={
                "Content-Disposition": f"inline; filename={filename}"
            }
        )
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ PDF fetch failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/feedback/send/")
async def send_feedback(
    request: Request,
    subject: str = Form(...),
    message: str = Form(...)
):
    """
    Send feedback email to mudassir@mckinneyandco.com and rohit@mckinneyandco.com
    
    Form data:
    - subject: Feedback subject
    - message: Feedback message
    
    Headers:
    - X-User-ID: Username submitting feedback
    """
    try:
        from email_service import send_feedback_email
        
        username = request.headers.get('X-User-ID', 'Anonymous')
        
        if not subject or len(subject.strip()) < 3:
            raise HTTPException(
                status_code=400, 
                detail="Subject must be at least 3 characters long"
            )
        
        if not message or len(message.strip()) < 10:
            raise HTTPException(
                status_code=400, 
                detail="Message must be at least 10 characters long"
            )
        
        result = send_feedback_email(
            subject=subject.strip(),
            message=message.strip(),
            sender_username=username
        )
        
        if result['success']:
            print(f"✅ Feedback submitted by {username}: {subject[:50]}...")
            return {
                "success": True,
                "message": "Thank you for your feedback! We'll get back to you soon."
            }
        else:
            raise HTTPException(status_code=500, detail=result['message'])
            
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Error processing feedback: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))