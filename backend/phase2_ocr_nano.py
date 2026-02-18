"""
Phase 2: OCR Extraction
After Phase 1 identifies problem pages, OCR extracts text from all pages
Works with Google Cloud Storage
"""
import fitz
from docstrange import DocumentExtractor
import os
import json
import re
import time
import tempfile
from datetime import datetime
from typing import Dict, Any, List
from google.cloud import storage

BUCKET_NAME = os.getenv('BUCKET_NAME', 'mckinneysuite')
PDF_FOLDER = 'pdf'
METADATA_FILE = f'{PDF_FOLDER}/uploads_metadata.json'

# Initialize NanoNets OCR extractor
EXTRACTOR = DocumentExtractor(api_key="bdee3d34-b8db-11f0-bd7c-dece98018c81", model="nanonets-ocr-s")


def _get_bucket() -> storage.bucket.Bucket:
    client = storage.Client()
    return client.bucket(BUCKET_NAME)


def _blob_path_from_gs_uri(gs_uri: str) -> str:
    # gs://deployment/pdf/filename.pdf -> pdf/filename.pdf
    prefix = f"gs://{BUCKET_NAME}/"
    if gs_uri.startswith(prefix):
        return gs_uri[len(prefix):]
    return gs_uri  # assume already relative


def _download_bytes(bucket: storage.bucket.Bucket, blob_path: str) -> bytes:
    blob = bucket.blob(blob_path)
    return blob.download_as_bytes()


def _upload_text_to_gcs(bucket: storage.bucket.Bucket, file_path: str, content: str) -> None:
    """Upload text file to GCS"""
    blob = bucket.blob(file_path)
    blob.upload_from_string(content, content_type='text/plain')


def analyze_ocr_quality(text: str) -> Dict[str, Any]:
    """Analyze OCR text quality"""
    metrics = {
        'total_chars': len(text),
        'readable_words': len([word for word in text.split() if len(word) > 2 and word.isalpha()]),
        'lines': len([line for line in text.split('\n') if line.strip()]),
        'confidence_score': 0
    }
    
    # Calculate confidence score
    confidence = 100
    
    # Penalty for very short text
    if metrics['total_chars'] < 100:
        confidence -= 30
    elif metrics['total_chars'] < 500:
        confidence -= 15
    
    # Penalty for very few readable words
    if metrics['readable_words'] < 20:
        confidence -= 40
    elif metrics['readable_words'] < 50:
        confidence -= 20
    
    # Bonus for good text length
    if metrics['total_chars'] > 1000:
        confidence += 10
    if metrics['readable_words'] > 100:
        confidence += 10
    
    metrics['confidence_score'] = max(confidence, 0)
    
    return metrics


def extract_with_nanonets_ocr(pdf_bytes: bytes, page_num: int) -> Dict[str, Any]:
    """Extract text using NanoNets OCR from PDF bytes
    
    Thread-safe: Uses tempfile which generates unique file names automatically.
    Each concurrent request gets its own unique temp file, preventing conflicts.
    """
    import threading
    temp_image_path = None
    doc = None
    try:
        print(f"  Converting page {page_num} to image...")
        
        # Open PDF and get page
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        page = doc[page_num - 1]  # PyMuPDF uses 0-based indexing
        
        # Convert page to image (2.0x zoom for better table detection)
        mat = fitz.Matrix(2.0, 2.0)
        pix = page.get_pixmap(matrix=mat, alpha=False)
        
        # Create unique temp file for the image
        # Generate unique filename and let pix.save() create the file
        process_id = os.getpid()
        thread_id = threading.get_ident()
        
        # Use system temp directory (handles concurrent users properly)
        temp_dir = tempfile.gettempdir()
        
        # Generate unique filename WITHOUT pre-creating the file
        # This lets pix.save() create and manage the file properly
        unique_name = f"nanonets_ocr_{process_id}_{thread_id}_page{page_num}_{int(time.time()*1000000)}.png"
        temp_image_path = os.path.join(temp_dir, unique_name)
        
        # Save image to temp file
        # Let pix.save() create and write the file - it handles this better than we can
        pix.save(temp_image_path)
        
        # CRITICAL: Release pixmap resources immediately after saving
        # This ensures the file is fully written and released before NanoNets accesses it
        pix = None  # Release pixmap reference
        
        # Debug: Verify temp file was created and has content
        if os.path.exists(temp_image_path):
            file_size = os.path.getsize(temp_image_path)
            print(f"    [DEBUG] Temp image created: {temp_image_path} ({file_size} bytes)")
            if file_size == 0:
                print(f"    [WARNING] Temp image file is EMPTY! This might cause OCR to fail.")
        else:
            print(f"    [ERROR] Temp image file was NOT created: {temp_image_path}")
        
        # Close PDF document and release resources before calling NanoNets
        doc.close()
        doc = None  # Mark as closed
        
        # Small delay to ensure all file handles are fully released on Windows
        time.sleep(0.2)
        
        print(f"  Running NanoNets OCR on page {page_num}...")
        
        page_text = ""
        ocr_success = False
        ocr_error = None
        
        try:
            # Extract text using NanoNets
            result = EXTRACTOR.extract(temp_image_path)
            
            # Wait a bit for processing (especially for larger pages)
            time.sleep(1)
            
            # Extract markdown content
            markdown_content = result.extract_markdown()
            
            if markdown_content and len(markdown_content.strip()) > 0:
                page_text = markdown_content.strip()
                ocr_success = True
                print(f"    [SUCCESS] NanoNets OCR successful ({len(page_text)} chars)")
            else:
                ocr_error = "Empty OCR result"
                print(f"    [WARNING] NanoNets OCR returned empty content")
                
        except Exception as e:
            error_str = str(e)
            # Check if this is a file deletion/permission error (likely from NanoNets cleanup)
            # If OCR actually succeeded before cleanup failed, we should still mark as success if we got content
            if "cannot remove file" in error_str.lower() or "permission denied" in error_str.lower():
                # This might be a cleanup error, but OCR may have actually worked
                # Check if we can extract any result despite the error
                try:
                    # Try to get the result again if possible, or check if we got content
                    if 'result' in locals() and hasattr(result, 'extract_markdown'):
                        markdown_content = result.extract_markdown()
                        if markdown_content and len(markdown_content.strip()) > 0:
                            page_text = markdown_content.strip()
                            ocr_success = True
                            print(f"    [SUCCESS] NanoNets OCR successful ({len(page_text)} chars) (cleanup warning ignored)")
                        else:
                            ocr_error = "Empty OCR result"
                            print(f"    [WARNING] NanoNets OCR returned empty content")
                    else:
                        ocr_error = f"OCR extraction failed: {error_str}"
                        print(f"    [ERROR] NanoNets OCR extraction failed: {e}")
                except:
                    ocr_error = f"OCR failed: {error_str}"
                    print(f"    [ERROR] NanoNets OCR failed: {e}")
            else:
                # Real OCR error
                ocr_error = f"OCR extraction failed: {error_str}"
                print(f"    [ERROR] NanoNets OCR failed: {e}")
        
        # Analyze OCR quality
        metrics = analyze_ocr_quality(page_text)
        
        return {
            'text': page_text,
            'metrics': metrics,
            'success': ocr_success,
            'error': None if ocr_success else (ocr_error or "Empty or failed OCR result"),
            'temp_file_path': temp_image_path  # Return temp file path for later cleanup
        }
        
    except Exception as e:
        print(f"  [ERROR] OCR failed on page {page_num}: {e}")
        return {
            'text': '',
            'metrics': {'total_chars': 0, 'readable_words': 0, 'lines': 0, 'confidence_score': 0},
            'success': False,
            'error': str(e),
            'temp_file_path': temp_image_path  # Still return temp file path for cleanup
        }
    finally:
        # Ensure PDF document is closed
        if doc:
            try:
                doc.close()
            except:
                pass
        # NOTE: Temp file cleanup is deferred until all pages are processed
        # This avoids Windows file locking issues during OCR processing


def process_all_pages_with_ocr(pdf_bytes: bytes, total_pages: int) -> Dict[str, Any]:
    """Process ALL pages with OCR"""
    print("PHASE 2: OCR EXTRACTION - ALL PAGES")
    print("=" * 80)
    print(f"Processing {total_pages} pages with NanoNets OCR")
    print("=" * 80)
    
    results = {
        'successful_pages': [],
        'failed_pages': [],
        'all_results': {},
        'total_pages': total_pages
    }
    
    all_pages = list(range(1, total_pages + 1))
    temp_files_to_cleanup = []  # Track all temp files for cleanup at the end
    
    try:
        for page_num in all_pages:
            print(f"\nProcessing Page {page_num}...")
            
            # Extract text with OCR
            ocr_result = extract_with_nanonets_ocr(pdf_bytes, page_num)
            
            # Collect temp file path for later cleanup
            if ocr_result.get('temp_file_path'):
                temp_files_to_cleanup.append(ocr_result['temp_file_path'])
            
            # Store results (without temp_file_path in the stored result)
            result_to_store = {k: v for k, v in ocr_result.items() if k != 'temp_file_path'}
            results['all_results'][page_num] = result_to_store
            
            if ocr_result['success']:
                results['successful_pages'].append({
                    'page_num': page_num,
                    'text': ocr_result['text'],
                    'metrics': ocr_result['metrics']
                })
                
                metrics = ocr_result['metrics']
                print(f"  [SUCCESS] - {metrics['total_chars']} chars, {metrics['readable_words']} words, {metrics['confidence_score']:.1f}% confidence")
            else:
                results['failed_pages'].append({
                    'page_num': page_num,
                    'error': ocr_result['error']
                })
                print(f"  [FAILED] - {ocr_result['error']}")
    finally:
        # Clean up all temp files at the end when all pages are processed
        # This avoids Windows file locking issues during OCR processing
        if temp_files_to_cleanup:
            print(f"\n🧹 Cleaning up {len(temp_files_to_cleanup)} temporary files...")
            cleanup_count = 0
            for temp_file_path in temp_files_to_cleanup:
                if temp_file_path and os.path.exists(temp_file_path):
                    try:
                        # Add a small delay to ensure file handles are released
                        time.sleep(0.1)
                        os.remove(temp_file_path)
                        cleanup_count += 1
                    except Exception as e:
                        # Log but don't fail - temp files will be cleaned by OS later
                        print(f"    [WARNING] Could not delete temp file {os.path.basename(temp_file_path)}: {e}")
            
            if cleanup_count == len(temp_files_to_cleanup):
                print(f"✅ Successfully cleaned up {cleanup_count} temporary files")
            elif cleanup_count > 0:
                print(f"⚠️  Cleaned up {cleanup_count}/{len(temp_files_to_cleanup)} temporary files (OS will handle the rest)")
            else:
                print(f"⚠️  Could not clean up temp files (OS will handle cleanup automatically)")
    
    return results


def save_ocr_results_to_gcs(bucket: storage.bucket.Bucket, carrier_name: str, safe_carrier_name: str, file_type: str, timestamp: str, results: Dict[str, Any]) -> None:
    """Save OCR results to GCS with file type in filename"""
    # Convert file_type: 'propertyPDF' -> 'property', 'liabilityPDF' -> 'liability'
    type_short = file_type.replace('PDF', '').lower()
    ocr_file_path = f'phase2/results/{safe_carrier_name}_{type_short}_ocr_all_pages_{timestamp}.txt'
    
    report_lines = []
    report_lines.append("OCR EXTRACTION RESULTS - ALL PAGES")
    report_lines.append("=" * 80)
    report_lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    report_lines.append(f"Carrier: {carrier_name}")
    report_lines.append(f"Method: NanoNets OCR (2.0x zoom)")
    report_lines.append(f"Total Pages: {len(results['successful_pages'])}")
    report_lines.append(f"Success Rate: {len(results['successful_pages'])}/{len(results['all_results'])} ({len(results['successful_pages'])/len(results['all_results'])*100:.1f}%)")
    report_lines.append("=" * 80)
    report_lines.append("")
    
    for page_result in results['successful_pages']:
        page_num = page_result['page_num']
        report_lines.append(f"PAGE {page_num}:")
        report_lines.append("-" * 40)
        report_lines.append(f"Total Characters: {page_result['metrics']['total_chars']}")
        report_lines.append(f"Readable Words: {page_result['metrics']['readable_words']}")
        report_lines.append(f"Lines: {page_result['metrics']['lines']}")
        report_lines.append(f"Confidence Score: {page_result['metrics']['confidence_score']:.1f}%")
        report_lines.append("")
        report_lines.append("OCR EXTRACTED TEXT:")
        report_lines.append("-" * 40)
        report_lines.append(page_result['text'])
        report_lines.append("=" * 80)
        report_lines.append("")
    
    report_content = "\n".join(report_lines)
    _upload_text_to_gcs(bucket, ocr_file_path, report_content)
    
    print(f"✅ Saved OCR results to: gs://{BUCKET_NAME}/{ocr_file_path}")


def process_upload_ocr_analysis(upload_id: str) -> Dict[str, Any]:
    """
    Given an upload_id, read metadata, fetch PDFs from GCS, run OCR on all pages.
    Automatically called after Phase 1 quality analysis.
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
            
            blob_path = _blob_path_from_gs_uri(gs_path)
            
            try:
                # Download PDF bytes
                pdf_bytes = _download_bytes(bucket, blob_path)
                
                # Open PDF to get total pages
                doc = fitz.open(stream=pdf_bytes, filetype="pdf")
                total_pages = doc.page_count
                doc.close()
                
                # Process all pages with OCR
                ocr_results = process_all_pages_with_ocr(pdf_bytes, total_pages)
                
                files_analysis.append({
                    'type': file_type,
                    'path': gs_path,
                    'total_pages': total_pages,
                    'successful_pages': len(ocr_results.get('successful_pages', [])),
                    'failed_pages': len(ocr_results.get('failed_pages', [])),
                    'success_rate': f"{len(ocr_results['successful_pages'])}/{total_pages}",
                    'ocr_results': ocr_results
                })
                
                # Save OCR results to GCS
                safe_carrier_name = carrier_name.lower().replace(" ", "_").replace("&", "and")
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                save_ocr_results_to_gcs(bucket, carrier_name, safe_carrier_name, file_type, timestamp, ocr_results)
                
            except Exception as e:
                files_analysis.append({
                    'type': file_type,
                    'path': gs_path,
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
    
    # Automatically trigger Phase 2C Smart Selection after OCR completes
    try:
        print("\n✅ Phase 2 OCR complete. Starting Phase 2C Smart Selection...")
        from phase2c_smart_selection import process_upload_smart_selection_analysis
        selection_result = process_upload_smart_selection_analysis(upload_id)
        if selection_result.get('success'):
            print("✅ Phase 2C Smart Selection complete!")
        else:
            print(f"Warning: Phase 2C had issues: {selection_result.get('error')}")
    except Exception as e:
        print(f"Warning: Phase 2C Smart Selection failed: {e}")
    
    return result
