from fastapi import FastAPI, UploadFile, File, HTTPException, Header, Form, Request
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
from google.cloud import storage
import os
from tasks import celery_app
import tempfile
from auth import register, login
from database import get_all_users, user_exists_by_email, create_user, get_user
from upload_handler import process_carrier_uploads, get_upload_history
from dotenv import load_dotenv
import os
from phase1 import process_upload_lengths, process_upload_quality_analysis
# from phase2_ocr import process_upload_ocr_analysis
from phase2_ocr_nano import process_upload_ocr_analysis

load_dotenv()

google_cloud_credentials = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
if google_cloud_credentials:
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = google_cloud_credentials

app = FastAPI()

# Add CORS middleware to allow frontend to connect
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow all origins (for ngrok + Vercel)
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

client = storage.Client()
bucket_name = os.getenv('BUCKET_NAME', 'deployment')
bucket = client.get_bucket(bucket_name)

@app.get("/")
def read_root():
    return {"message": "Hello, World! Insurance PDF Analysis API"}

@app.post("/register/")
def register_endpoint(email: str = Form(...), password: str = Form(...)):
    """Register new user"""
    result = register(email, password)
    if "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])
    return result

@app.post("/login/")
def login_endpoint(email: str = Form(...), password: str = Form(...)):
    """Login user"""
    result = login(email, password)
    if "error" in result:
        raise HTTPException(status_code=401, detail=result["error"])
    return result

@app.get("/health")
def health_check():
    return {"status": "healthy"}

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
        
        # Parse carriers data
        carriers_info = json.loads(carriers_json)
        carriers = carriers_info.get("carriers", [])
        
        if not carriers:
            raise HTTPException(status_code=400, detail="No carriers provided")
        
        # Files can be 0 to 3 per carrier (property, liability, liquor) - completely optional
        min_files = 0
        max_files = len(carriers) * 3
        
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
                "liquorPDF": None
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
                except Exception as e:
                    print(f"Error processing file metadata: {e}")
                    raise HTTPException(status_code=400, detail=f"Error processing file: {str(e)}")
        
        # Get user ID (for now, use a default)
        user_id = "user_1"  # This should come from authenticated user
        
        # Process uploads
        result = process_carrier_uploads(carriers_data, user_id)
        
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

@app.get("/upload-status/{uploadId}")
def get_upload_status(uploadId: str):
    """
    Check processing status for an upload.
    Returns ready=True when all carriers have completed Phase 3 (LLM extraction).
    The frontend polls this endpoint every 3 seconds from the confirmed page.
    """
    try:
        import json
        import re
        from upload_handler import load_metadata

        # Find the upload in metadata
        metadata = load_metadata()
        uploads = metadata.get('uploads', [])
        upload_record = next((u for u in uploads if u.get('uploadId') == uploadId), None)

        if not upload_record:
            raise HTTPException(status_code=404, detail=f"Upload {uploadId} not found")

        carriers = upload_record.get('carriers', [])

        # Count expected and completed files
        expected_files = 0
        completed_files = 0

        for carrier in carriers:
            carrier_name = carrier.get('carrierName', 'Unknown')
            safe_name = carrier_name.lower().replace(" ", "_").replace("&", "and")

            for file_type in ['propertyPDF', 'liabilityPDF', 'liquorPDF']:
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

                # Check if Phase 3 result exists in GCS
                final_file_path = f"phase3/results/{safe_name}_{type_short}_final_validated_fields_{timestamp}.json"
                blob = bucket.blob(final_file_path)
                if blob.exists():
                    completed_files += 1

        ready = (completed_files == expected_files and expected_files > 0)

        return {
            "ready": ready,
            "completed_files": completed_files,
            "expected_files": expected_files,
            "uploadId": uploadId,
            "message": "All files processed" if ready else f"Processing: {completed_files}/{expected_files} files complete"
        }

    except HTTPException:
        raise
    except Exception as e:
        print(f"ERROR in get_upload_status: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Server error: {str(e)}")


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
    Analyze PDF quality using PyMuPDF - extracts text and classifies pages
    as CLEAN, PROBLEM, or BORDERLINE based on quality metrics.
    Automatically triggers Phase 2 OCR after completion.
    """
    try:
        result = process_upload_quality_analysis(uploadId)
        if not result.get("success"):
            raise HTTPException(status_code=404, detail=result.get("error", "Unknown error"))
        return result
    except HTTPException:
        raise
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