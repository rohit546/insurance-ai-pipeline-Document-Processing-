"""
Phase 2: OCR Extraction
After Phase 1 identifies problem pages, OCR extracts text from all pages
Works with Google Cloud Storage
Uses Joblib for parallel processing
"""
import fitz
import pytesseract
import os
import json
import re
from datetime import datetime
from typing import Dict, Any, List
from PIL import Image
import io
from google.cloud import storage
from joblib import Parallel, delayed

BUCKET_NAME = os.getenv('BUCKET_NAME', 'mckinneysuite')
PDF_FOLDER = 'pdf'
METADATA_FILE = f'{PDF_FOLDER}/uploads_metadata.json'


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


def extract_with_tesseract_ocr(pdf_bytes: bytes, page_num: int) -> Dict[str, Any]:
    """Extract text using Tesseract OCR from PDF bytes"""
    try:
        print(f"  Converting page {page_num} to image...")
        
        # Open PDF and get page
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        page = doc[page_num - 1]  # PyMuPDF uses 0-based indexing
        
        # Convert page to image (2.0x zoom for better table detection)
        mat = fitz.Matrix(2.0, 2.0)
        pix = page.get_pixmap(matrix=mat)
        img_data = pix.tobytes("png")
        
        # Create PIL Image from bytes
        image = Image.open(io.BytesIO(img_data))
        
        print(f"  Running Tesseract OCR on page {page_num}...")
        
        # Try multiple OCR configurations with fallback
        configs = [
            '--oem 3 --psm 6',           # Best for tables
            '--oem 3 --psm 3',           # Fallback for mixed content
            ''                           # Basic fallback
        ]
        
        page_text = ""
        ocr_success = False
        
        for i, config in enumerate(configs):
            try:
                if config:
                    print(f"    Trying OCR config {i+1}: {config}")
                    page_text = pytesseract.image_to_string(image, config=config)
                else:
                    print(f"    Trying basic OCR (fallback)")
                    page_text = pytesseract.image_to_string(image)
                
                # Check if we got meaningful text
                if len(page_text.strip()) > 50:
                    ocr_success = True
                    print(f"    [SUCCESS] OCR successful with config {i+1}")
                    break
                else:
                    print(f"    [FAILED] Config {i+1} produced insufficient text")
                    
            except Exception as e:
                print(f"    [ERROR] Config {i+1} failed: {e}")
                continue
        
        if not ocr_success:
            print(f"    [FAILED] All OCR configurations failed")
            page_text = ""
        
        doc.close()
        
        # Analyze OCR quality
        metrics = analyze_ocr_quality(page_text)
        
        return {
            'text': page_text,
            'metrics': metrics,
            'success': True,
            'error': None
        }
        
    except Exception as e:
        print(f"  [ERROR] OCR failed on page {page_num}: {e}")
        return {
            'text': '',
            'metrics': {'total_chars': 0, 'readable_words': 0, 'confidence_score': 0},
            'success': False,
            'error': str(e)
        }


def process_all_pages_with_ocr(pdf_bytes: bytes, total_pages: int, n_jobs: int = 2) -> Dict[str, Any]:
    """
    Process ALL pages with Tesseract OCR - PARALLELIZED with Joblib
    
    Args:
        pdf_bytes: PDF file bytes
        total_pages: Total number of pages
        n_jobs: Number of parallel workers (default: 4, -1 for all cores)
    """
    print("PHASE 2: OCR EXTRACTION - ALL PAGES (PARALLEL)")
    print("=" * 80)
    print(f"Processing {total_pages} pages with Tesseract OCR")
    print(f"Parallel Workers: {n_jobs}")
    print("=" * 80)
    
    def process_single_page(page_num):
        """Process single page - called in parallel by Joblib"""
        print(f"\nProcessing Page {page_num}...")
        ocr_result = extract_with_tesseract_ocr(pdf_bytes, page_num)
        return page_num, ocr_result
    
    # Process all pages in parallel using Joblib
    # backend='threading' is perfect for I/O-bound tasks
    # Tesseract is CPU-bound, but threading still works due to image I/O
    results_list = Parallel(
        n_jobs=n_jobs,
        backend='threading',
        verbose=5
    )(
        delayed(process_single_page)(page_num)
        for page_num in range(1, total_pages + 1)
    )
    
    # Organize results
    results = {
        'successful_pages': [],
        'failed_pages': [],
        'all_results': {},
        'total_pages': total_pages
    }
    
    # Process results from parallel execution
    for page_num, ocr_result in results_list:
        # Store results
        results['all_results'][page_num] = ocr_result
        
        if ocr_result['success']:
            results['successful_pages'].append({
                'page_num': page_num,
                'text': ocr_result['text'],
                'metrics': ocr_result['metrics']
            })
            metrics = ocr_result['metrics']
            print(f"  ✅ Page {page_num} SUCCESS - {metrics['total_chars']} chars, {metrics['readable_words']} words, {metrics['confidence_score']:.1f}% confidence")
        else:
            results['failed_pages'].append({
                'page_num': page_num,
                'error': ocr_result['error']
            })
            print(f"  ❌ Page {page_num} FAILED - {ocr_result['error']}")
    
    print(f"\n✅ Parallel Tesseract OCR processing complete: {len(results['successful_pages'])}/{total_pages} pages successful")
    
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
    report_lines.append(f"Method: Tesseract OCR (2.0x zoom)")
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
