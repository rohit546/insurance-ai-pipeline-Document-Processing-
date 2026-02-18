from celery import Celery
from dotenv import load_dotenv
import os
from google.cloud import storage
from pathlib import Path
import time
import ssl
from google.api_core.exceptions import ServiceUnavailable, InternalServerError
from requests.exceptions import ConnectionError as RequestsConnectionError
from cpu_allocator import allocate_cpu_for_task

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

# Create Celery app - make initialization resilient to avoid import failures
celery_app = Celery(__name__)

# Try to load config, but don't fail if Redis is unavailable at import time
# This allows the app to start even if Redis connection fails initially
try:
    celery_app.config_from_object('celery_config')
    print("[OK] Celery configuration loaded successfully")
except Exception as e:
    print(f"[WARN] Could not load Celery config (Redis may be unavailable): {e}")
    print("   Celery will use default configuration. Tasks may not work until Redis is available.")
    # Set minimal config to allow app to start
    celery_app.conf.update(
        broker_url='redis://localhost:6379/0',
        result_backend='redis://localhost:6379/0',
        task_serializer='json',
        accept_content=['json'],
        result_serializer='json',
    )

# Initialize Google Cloud Storage (lazy initialization to avoid import-time errors)
_client = None
_bucket = None
bucket_name = os.getenv('BUCKET_NAME', 'mckinneysuite')

def get_storage_client():
    """Get or create Google Cloud Storage client"""
    global _client
    if _client is None:
        _client = storage.Client()
    return _client

def get_bucket():
    """Get or create Google Cloud Storage bucket"""
    global _bucket
    if _bucket is None:
        client = get_storage_client()
        _bucket = client.get_bucket(bucket_name)
    return _bucket

# For backward compatibility, create client and bucket at module level
# but only if credentials are available - don't fail if this doesn't work
# Storage will be initialized lazily when actually needed
client = None
bucket = None

try:
    if google_cloud_credentials and os.path.exists(google_cloud_credentials):
        # Try to initialize, but don't fail if it doesn't work
        try:
            client = get_storage_client()
            bucket = get_bucket()
            print("[OK] Storage client initialized successfully")
        except Exception as storage_error:
            print(f"[WARN] Could not initialize storage client: {storage_error}")
            print("   Storage will be initialized lazily when needed")
            client = None
            bucket = None
    else:
        print("[WARN] Storage client will be initialized lazily (credentials not found)")
except Exception as e:
    print(f"[WARN] Error checking credentials: {e}")
    print("   Storage will be initialized lazily when needed")
    client = None
    bucket = None

@celery_app.task
def example_task(message):
    """Example task that can be called from your FastAPI app"""
    print(f"Processing: {message}")
    return f"Task completed: {message}"

@celery_app.task(
    name="tasks.process_phase1_task",
    queue="summary",
    max_retries=0,
    acks_late=True,
    reject_on_worker_lost=True,
)
def process_phase1_task(upload_id: str):
    """
    Background task to process Phase 1 (quality analysis) for an upload.
    Queues the entire pipeline so only one processes at a time.
    Files are already uploaded to GCS before this task runs.
    """
    try:
        from phase1 import process_upload_quality_analysis
        print(f"📦 Starting Phase 1 task for upload: {upload_id}")
        result = process_upload_quality_analysis(upload_id)
        
        if result.get('success'):
            print(f"✅ Phase 1 task completed for upload: {upload_id}")
        else:
            print(f"❌ Phase 1 task failed for upload: {upload_id}: {result.get('error')}")
        
        return result
    except Exception as e:
        print(f"❌ Phase 1 task error for upload: {upload_id}: {e}")
        import traceback
        traceback.print_exc()
        return {
            'success': False,
            'error': str(e)
        }

@celery_app.task(
    name="tasks.process_ocr_task",
    queue='summary',
    max_retries=0,
    acks_late=True,
    reject_on_worker_lost=True
)
def process_ocr_task(upload_id: str):
    """
    Background task to process OCR on all PDFs for an upload (Summary feature).
    Runs in 'summary' queue with dynamic CPU allocation (2 CPU if QC active, 8 CPU if idle).
    Returns immediately to caller, OCR runs in background.
    """
    try:
        # ========== CPU ALLOCATION ==========
        # Allocate CPU cores for this Summary task (dynamic: 2 CPU if QC active, 8 CPU if idle)
        summary_cpu_cores = allocate_cpu_for_task('summary')
        print(f"[Summary Task] Allocated {summary_cpu_cores} CPU cores for Summary processing")
        # ====================================
        
        # from phase2_ocr import process_upload_ocr_analysis
        from phase2_ocr_nano import process_upload_ocr_analysis
        print(f"📦 Starting OCR task for upload: {upload_id}")
        result = process_upload_ocr_analysis(upload_id)
        
        if result.get('success'):
            print(f"✅ OCR task completed for upload: {upload_id}")
        else:
            print(f"❌ OCR task failed for upload: {upload_id}: {result.get('error')}")
        
        return result
    except Exception as e:
        print(f"❌ OCR task error for upload: {upload_id}: {e}")
        import traceback
        traceback.print_exc()
        return {
            'success': False,
            'error': str(e)
        }

@celery_app.task(
    name="tasks.process_qc_new_unified_task",
    queue='qc',
    max_retries=0,
    acks_late=True,
    reject_on_worker_lost=True
)
def process_qc_new_unified_task(
    pl_cert_blob_name: str = None,
    gl_cert_blob_name: str = None,
    policy_blob_name: str = None,
    upload_id: str = None,
    cert_blob_name: str = None,
    coverage_type: str = None,
    acord_cert_blob_name: str = None,
    gl_acord_cert_blob_name: str = None,
):
    """
    Unified QC task: runs cert extraction + policy validation for PL and/or GL.
    Runs in 'qc' queue with priority CPU allocation (6 cores on Railway).
    
    Args:
        pl_cert_blob_name: GCS blob name for PL certificate PDF (optional)
        gl_cert_blob_name: GCS blob name for GL certificate PDF (optional)
        policy_blob_name: GCS blob name for policy PDF (required)
        upload_id: Unique upload ID (e.g., qcnew_20241216_123456_abc123)
        cert_blob_name: Legacy parameter for backward compatibility (maps to pl_cert_blob_name)
        coverage_type: Legacy parameter (ignored, determined from certificates)
    
    Returns:
        {"success": True, "upload_id": ..., "result": {...merged validation...}}
    """
    import tempfile
    import shutil
    import json
    from pathlib import Path
    from qc_new_unified_pipeline import run_unified_qc_pl_gl
    
    # ========== CPU ALLOCATION ==========
    # Allocate CPU cores for this QC task (6 CPU on Railway)
    qc_cpu_cores = allocate_cpu_for_task('qc')
    print(f"[QC Task] Allocated {qc_cpu_cores} CPU cores for QC processing")
    # ====================================
    
    # Legacy support: if cert_blob_name is provided, treat it as PL certificate
    if cert_blob_name and not pl_cert_blob_name:
        pl_cert_blob_name = cert_blob_name
    
    # Validate that at least one primary certificate is provided (ACORD is optional)
    if not pl_cert_blob_name and not gl_cert_blob_name:
        return {
            "success": False,
            "error": "At least one certificate (PL or GL) must be provided",
            "upload_id": upload_id,
        }
    
    if not policy_blob_name:
        return {
            "success": False,
            "error": "Policy PDF is required",
            "upload_id": upload_id,
        }
    
    temp_dir = Path(tempfile.mkdtemp(prefix="qc_unified_"))
    
    try:
        print(f"🚀 Starting unified QC task for upload: {upload_id}")
        print(f"   PL Certificate: {'✅' if pl_cert_blob_name else '❌'}")
        print(f"   GL Certificate: {'✅' if gl_cert_blob_name else '❌'}")
        print(f"   ACORD Certificate (Property): {'✅' if acord_cert_blob_name else '❌'}")
        print(f"   GL ACORD Certificate: {'✅' if gl_acord_cert_blob_name else '❌'}")
        print(f"   Policy: ✅")
        
        # Get bucket (lazy initialization)
        bucket = get_bucket()
        
        # Download policy PDF
        policy_pdf = temp_dir / "policy.pdf"
        print(f"📥 Downloading policy: {policy_blob_name}")
        blob_policy = bucket.blob(policy_blob_name)
        blob_policy.download_to_filename(str(policy_pdf))
        
        # Download PL certificate if provided
        pl_cert_pdf = None
        if pl_cert_blob_name:
            pl_cert_pdf = temp_dir / "pl_certificate.pdf"
            print(f"📥 Downloading PL certificate: {pl_cert_blob_name}")
            blob_pl_cert = bucket.blob(pl_cert_blob_name)
            blob_pl_cert.download_to_filename(str(pl_cert_pdf))
        
        # Download GL certificate if provided
        gl_cert_pdf = None
        if gl_cert_blob_name:
            gl_cert_pdf = temp_dir / "gl_certificate.pdf"
            print(f"📥 Downloading GL certificate: {gl_cert_blob_name}")
            blob_gl_cert = bucket.blob(gl_cert_blob_name)
            blob_gl_cert.download_to_filename(str(gl_cert_pdf))
        
        # Download ACORD certificate if provided
        acord_cert_pdf = None
        if acord_cert_blob_name:
            acord_cert_pdf = temp_dir / "acord_certificate.pdf"
            print(f"📥 Downloading ACORD certificate: {acord_cert_blob_name}")
            blob_acord_cert = bucket.blob(acord_cert_blob_name)
            blob_acord_cert.download_to_filename(str(acord_cert_pdf))

        # Download GL ACORD certificate if provided
        gl_acord_cert_pdf = None
        if gl_acord_cert_blob_name:
            gl_acord_cert_pdf = temp_dir / "gl_acord_certificate.pdf"
            print(f"📥 Downloading GL ACORD certificate: {gl_acord_cert_blob_name}")
            blob_gl_acord_cert = bucket.blob(gl_acord_cert_blob_name)
            blob_gl_acord_cert.download_to_filename(str(gl_acord_cert_pdf))

        # Run unified pipeline
        print("🔄 Running unified QC pipeline (PL + GL)...")
        result = run_unified_qc_pl_gl(
            pl_cert_pdf_path=str(pl_cert_pdf) if pl_cert_pdf else None,
            gl_cert_pdf_path=str(gl_cert_pdf) if gl_cert_pdf else None,
            policy_pdf_path=str(policy_pdf),
            upload_id=upload_id,
            acord_cert_pdf_path=str(acord_cert_pdf) if acord_cert_pdf else None,
            gl_acord_cert_pdf_path=str(gl_acord_cert_pdf) if gl_acord_cert_pdf else None,
        )
        
        # Upload result JSON to GCS with retry
        result_json_path = temp_dir / "merged_result.json"
        with open(result_json_path, "w") as f:
            json.dump(result, f, indent=2)
        
        result_blob_name = f"qc-new/uploads/{upload_id}/merged_result.json"
        blob_result = bucket.blob(result_blob_name)
        
        # Upload with retry logic for SSL/network errors
        max_retries = 3
        for attempt in range(max_retries):
            try:
                blob_result.upload_from_filename(str(result_json_path))
                print(f"✅ Uploaded merged result to: {result_blob_name}")
                break
            except (ssl.SSLError, ssl.SSLEOFError, ServiceUnavailable, InternalServerError, RequestsConnectionError, Exception) as e:
                if attempt < max_retries - 1:
                    wait_time = 2 ** attempt  # Exponential backoff: 1s, 2s, 4s
                    print(f"⚠️ Result upload attempt {attempt + 1} failed (SSL/Network error): {e}")
                    print(f"   Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    print(f"❌ Result upload failed after {max_retries} attempts: {e}")
                    raise
        
        return {
            "success": True,
            "upload_id": upload_id,
            "result": result,
        }
        
    except Exception as e:
        print(f"❌ Unified QC task error for upload: {upload_id}: {e}")
        import traceback
        traceback.print_exc()
        return {
            "success": False,
            "error": str(e),
            "upload_id": upload_id,
        }
    finally:
        try:
            shutil.rmtree(temp_dir)
        except Exception:
            pass