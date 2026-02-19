"""
Multi-Carrier PDF Upload Handler
Handles uploading multiple carrier quotes to Google Cloud Storage
"""
import json
import os
from datetime import datetime
from typing import List, Dict, Any
from google.cloud import storage
from dotenv import load_dotenv

load_dotenv()

# Initialize Google Cloud Storage
google_cloud_credentials = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
if google_cloud_credentials:
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = google_cloud_credentials

client = storage.Client()
BUCKET_NAME = os.getenv('BUCKET_NAME', 'mckinneysuite')
PDF_FOLDER = 'pdf'
METADATA_FILE = f'{PDF_FOLDER}/uploads_metadata.json'

bucket = client.bucket(BUCKET_NAME)


def get_unique_filename(carrier_name: str, pdf_type: str) -> str:
    """
    Generate unique filename for PDF
    Format: carriername_type_timestamp.pdf
    Example: state_farm_property_20241029_103000.pdf
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_carrier_name = carrier_name.lower().replace(" ", "_").replace("&", "and")
    return f"{safe_carrier_name}_{pdf_type}_{timestamp}.pdf"


def upload_pdf_to_gcs(file_content: bytes, filename: str) -> str:
    """
    Upload PDF file to Google Cloud Storage
    Returns: GCS path (gs://bucket/path)
    """
    blob_path = f"{PDF_FOLDER}/{filename}"
    blob = bucket.blob(blob_path)
    
    blob.upload_from_string(
        file_content,
        content_type='application/pdf'
    )
    
    gcs_path = f"gs://{BUCKET_NAME}/{blob_path}"
    return gcs_path


def load_metadata() -> Dict[str, Any]:
    """
    Load existing metadata from GCS
    If file doesn't exist, return empty structure
    """
    try:
        blob = bucket.blob(METADATA_FILE)
        if blob.exists():
            content = blob.download_as_string().decode('utf-8')
            return json.loads(content)
    except Exception as e:
        print(f"Error loading metadata: {e}")
    
    return {"uploads": []}


def save_metadata(metadata: Dict[str, Any]) -> None:
    """
    Save metadata to GCS
    """
    blob = bucket.blob(METADATA_FILE)
    blob.upload_from_string(
        json.dumps(metadata, indent=2),
        content_type='application/json'
    )


def process_carrier_uploads(
    carriers_data: List[Dict[str, Any]],
    username: str
) -> Dict[str, Any]:
    """
    Process multiple carriers and upload their PDFs
    
    Args:
        carriers_data: List of carrier dicts with:
            - carrierName: str
            - propertyPDF: bytes (file content)
            - propertyFilename: str
            - liabilityPDF: bytes (file content)
            - liabilityFilename: str
            - liquorPDF: bytes (file content)
            - liquorFilename: str
            - workersCompPDF: bytes (file content)
            - workersCompFilename: str
        username: Username for tracking and sheet routing
    
    Returns:
        Success response with all file paths and metadata
    """
    
    upload_id = f"upload_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    uploaded_carriers = []
    
    try:
        # Process each carrier
        for carrier in carriers_data:
            carrier_name = carrier['carrierName']
            
            carrier_info = {
                "carrierName": carrier_name,
                "propertyPDF": None,
                "liabilityPDF": None,
                "liquorPDF": None,
                "workersCompPDF": None
            }
            
            # Upload Property PDF if provided
            if carrier.get('propertyPDF'):
                property_filename = get_unique_filename(carrier_name, 'property')
                property_path = upload_pdf_to_gcs(
                    carrier['propertyPDF'],
                    property_filename
                )
                carrier_info["propertyPDF"] = {
                    "filename": property_filename,
                    "path": property_path,
                    "size": len(carrier['propertyPDF']),
                    "uploadedAt": datetime.now().isoformat()
                }
            
            # Upload Liability PDF if provided
            if carrier.get('liabilityPDF'):
                liability_filename = get_unique_filename(carrier_name, 'liability')
                liability_path = upload_pdf_to_gcs(
                    carrier['liabilityPDF'],
                    liability_filename
                )
                carrier_info["liabilityPDF"] = {
                    "filename": liability_filename,
                    "path": liability_path,
                    "size": len(carrier['liabilityPDF']),
                    "uploadedAt": datetime.now().isoformat()
                }
            
            # Upload Liquor PDF if provided
            if carrier.get('liquorPDF'):
                liquor_filename = get_unique_filename(carrier_name, 'liquor')
                liquor_path = upload_pdf_to_gcs(
                    carrier['liquorPDF'],
                    liquor_filename
                )
                carrier_info["liquorPDF"] = {
                    "filename": liquor_filename,
                    "path": liquor_path,
                    "size": len(carrier['liquorPDF']),
                    "uploadedAt": datetime.now().isoformat()
                }
            
            # Upload Workers Comp PDF if provided
            if carrier.get('workersCompPDF'):
                workerscomp_filename = get_unique_filename(carrier_name, 'workerscomp')
                workerscomp_path = upload_pdf_to_gcs(
                    carrier['workersCompPDF'],
                    workerscomp_filename
                )
                carrier_info["workersCompPDF"] = {
                    "filename": workerscomp_filename,
                    "path": workerscomp_path,
                    "size": len(carrier['workersCompPDF']),
                    "uploadedAt": datetime.now().isoformat()
                }
            
            uploaded_carriers.append(carrier_info)
        
        # Update metadata file
        metadata = load_metadata()
        
        # Count total files uploaded (some may be None)
        total_files = sum(
            (1 if c.get("propertyPDF") else 0) + (1 if c.get("liabilityPDF") else 0)
            for c in uploaded_carriers
        )
        
        upload_record = {
            "uploadId": upload_id,
            "userId": username,
            "username": username,
            "uploadedAt": datetime.now().isoformat(),
            "totalCarriers": len(uploaded_carriers),
            "totalFiles": total_files,
            "carriers": uploaded_carriers
        }
        
        metadata["uploads"].append(upload_record)
        save_metadata(metadata)
        
        # Return success response
        return {
            "success": True,
            "uploadId": upload_id,
            "totalCarriers": len(uploaded_carriers),
            "totalFiles": total_files,
            "carriers": uploaded_carriers,
            "uploadedAt": datetime.now().isoformat(),
            "message": f"Successfully uploaded {len(uploaded_carriers)} carriers with {total_files} PDF files"
        }
    
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "message": "Failed to upload carriers"
        }


def get_upload_history(user_id: str = None):
    """
    Get upload history for a user or all uploads
    """
    try:
        metadata = load_metadata()
        
        if user_id:
            uploads = [u for u in metadata.get("uploads", []) if u.get("userId") == user_id]
        else:
            uploads = metadata.get("uploads", [])
        
        return {
            "success": True,
            "uploads": uploads
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }

def add_username_to_upload(upload_id: str, user_id: str, username: str):
    """
    Add username and user_id to an existing upload record
    """
    try:
        metadata = load_metadata()
        uploads = metadata.get("uploads", [])
        
        # Find the upload by ID and add username and user_id
        for upload in uploads:
            if upload.get("uploadId") == upload_id:
                upload["userId"] = user_id
                upload["username"] = username
                upload["confirmedAt"] = datetime.now().isoformat()
                break
        
        metadata["uploads"] = uploads
        save_metadata(metadata)
        
        return {
            "success": True,
            "message": f"Upload confirmed for {username} ({user_id})"
        }
    except Exception as e:
        return {
            "success": False,
            "message": str(e)
        }
