import fitz
import os
from datetime import datetime
from typing import Dict, Any, List
from google.cloud import storage
import io
from phase1_pymudf import analyze_text_quality, classify_page_quality

# NOTE: PyMuPDF detailed text extraction and quality analysis is now disabled.
# Phase 1 now only counts total pages and returns minimal structure.
# Phase 2C Smart Selection always prefers OCR (NanoNets) anyway,
# so detailed PyMuPDF processing was unnecessary and wasting time.

BUCKET_NAME = os.getenv('BUCKET_NAME', 'mckinneysuite')
PDF_FOLDER = 'pdf'
METADATA_FILE = f'{PDF_FOLDER}/uploads_metadata.json'


def _get_bucket() -> storage.bucket.Bucket:
    client = storage.Client()
    return client.bucket(BUCKET_NAME)


def _read_metadata(bucket: storage.bucket.Bucket) -> Dict[str, Any]:
    blob = bucket.blob(METADATA_FILE)
    if not blob.exists():
        raise FileNotFoundError(f"{METADATA_FILE} not found in bucket {BUCKET_NAME}")
    content = blob.download_as_string().decode('utf-8')
    import json as _json
    return _json.loads(content)


def _blob_path_from_gs_uri(gs_uri: str) -> str:
    # gs://deployment/pdf/filename.pdf -> pdf/filename.pdf
    prefix = f"gs://{BUCKET_NAME}/"
    if gs_uri.startswith(prefix):
        return gs_uri[len(prefix):]
    return gs_uri  # assume already relative


def _download_bytes(bucket: storage.bucket.Bucket, blob_path: str) -> bytes:
    blob = bucket.blob(blob_path)
    return blob.download_as_bytes()


# Quality analysis functions imported from phase1_pymudf.py


def _extract_and_analyze_pdf(pdf_bytes: bytes) -> Dict[str, Any]:
    """
    DEPRECATED: PyMuPDF text extraction disabled - using OCR only.
    Keeping function signature for structure compatibility.
    
    This function now only counts total pages and returns minimal structure.
    Phase 2C Smart Selection always prefers OCR anyway, so empty PyMuPDF
    analysis is unnecessary and wastes processing time.
    """
    try:
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        total_pages = doc.page_count
        doc.close()
        
        # Return minimal structure without processing individual pages
        # Phase 2C will always select OCR anyway, so empty PyMuPDF data is fine
        return {
            'clean_pages': [],
            'problem_pages': [],
            'borderline_pages': [],
            'all_metrics': {},
            'total_pages': total_pages
        }
        
    except Exception as e:
        return {
            'error': str(e),
            'total_pages': 0,
            'clean_pages': [],
            'problem_pages': [],
            'borderline_pages': []
        }


def process_upload_lengths(upload_id: str) -> Dict[str, Any]:
    """
    Given an upload_id, read metadata and compute byte lengths for any PDFs present
    per carrier. Returns a structured dict suitable for JSON response.
    """
    bucket = _get_bucket()
    metadata = _read_metadata(bucket)

    uploads: List[Dict[str, Any]] = metadata.get('uploads', [])
    record = next((u for u in uploads if u.get('uploadId') == upload_id), None)
    if record is None:
        return {"success": False, "error": f"uploadId {upload_id} not found"}

    results: List[Dict[str, Any]] = []
    for carrier in record.get('carriers', []):
        carrier_name = carrier.get('carrierName')
        files_info: List[Dict[str, Any]] = []
        for file_type in ['propertyPDF', 'liabilityPDF', 'liquorPDF']:
            pdf_info = carrier.get(file_type)
            if not pdf_info:
                continue
            gs_path = pdf_info.get('path')
            if not gs_path:
                continue
            blob_path = _blob_path_from_gs_uri(gs_path)
            try:
                length_bytes = len(_download_bytes(bucket, blob_path))
                files_info.append({
                    'type': file_type,
                    'path': gs_path,
                    'length': length_bytes,
                })
            except Exception as e:
                files_info.append({
                    'type': file_type,
                    'path': gs_path,
                    'error': str(e),
                })
        results.append({
            'carrierName': carrier_name,
            'files': files_info,
        })

    return {
        'success': True,
        'uploadId': upload_id,
        'carriers': results,
    }


def _save_quality_results_to_gcs(bucket: storage.bucket.Bucket, upload_id: str, results: Dict[str, Any], analysis_data: Dict[str, Any]) -> None:
    """Save quality analysis results to GCS phase1/results folder - one file per carrier per file type"""
    
    # Save one file per carrier per file type
    for carrier in analysis_data.get('carriers', []):
        carrier_name = carrier.get('carrierName', 'Unknown')
        # Sanitize carrier name same way as PDF uploads
        safe_carrier_name = carrier_name.lower().replace(" ", "_").replace("&", "and")
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        try:
            _save_report_txt(bucket, carrier_name, safe_carrier_name, timestamp, carrier)
        except Exception as e:
            print(f"Warning: Failed to save report.txt for {carrier_name}: {e}")
        
        # Save clean pages one file per file type
        for file_data in carrier.get('files', []):
            try:
                _save_clean_pages_txt(bucket, carrier_name, safe_carrier_name, timestamp, file_data)
            except Exception as e:
                print(f"Warning: Failed to save clean_pages.txt for {carrier_name} {file_data.get('type')}: {e}")


def _save_report_txt(bucket: storage.bucket.Bucket, carrier_name: str, safe_carrier_name: str, timestamp: str, carrier_data: Dict[str, Any]) -> None:
    """Save phase1_report.txt to GCS - one file per carrier"""
    report_path = f'phase1/results/{safe_carrier_name}_phase1_report_{timestamp}.txt'
    
    report_lines = []
    report_lines.append("PHASE 1 SCREENING REPORT - PYMUPDF ANALYSIS")
    report_lines.append("=" * 80)
    report_lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    report_lines.append(f"Carrier: {carrier_name}")
    report_lines.append("=" * 80)
    
    for file_data in carrier_data.get('files', []):
        file_type = file_data.get('type', 'unknown')
        total_pages = file_data.get('total_pages', 0)
        clean_count = file_data.get('clean_pages', 0)
        problem_count = file_data.get('problem_pages', 0)
        borderline_count = file_data.get('borderline_pages', 0)
        
        report_lines.append(f"\n{file_type.upper()}:")
        report_lines.append(f"Total Pages: {total_pages}")
        report_lines.append(f"Clean Pages: {clean_count}")
        report_lines.append(f"Problem Pages: {problem_count}")
        report_lines.append(f"Borderline Pages: {borderline_count}")
        
        # Detailed metrics by page
        page_details = file_data.get('page_details', {})
        if page_details:
            report_lines.append("\nDETAILED METRICS BY PAGE:")
            report_lines.append("-" * 50)
            for page_num, metrics in page_details.items():
                clean_pages = file_data.get('clean_page_numbers', [])
                problem_pages = file_data.get('problem_page_numbers', [])
                
                if page_num in clean_pages:
                    quality = "CLEAN"
                elif page_num in problem_pages:
                    quality = "PROBLEM"
                else:
                    quality = "BORDERLINE"
                
                report_lines.append(
                    f"Page {page_num:2d}: {quality:12s} | "
                    f"{metrics['readable_words']:3d} words | "
                    f"{metrics['cid_codes']:3d} (cid:XX) | "
                    f"{metrics['confidence_score']:5.1f}% confidence | "
                    f"{metrics['gibberish_ratio']:5.1%} gibberish"
                )
    
    report_content = "\n".join(report_lines)
    
    blob = bucket.blob(report_path)
    blob.upload_from_string(report_content, content_type='text/plain')
    
    print(f"✅ Saved report to: gs://{BUCKET_NAME}/{report_path}")


def _save_clean_pages_txt(bucket: storage.bucket.Bucket, carrier_name: str, safe_carrier_name: str, timestamp: str, file_data: Dict[str, Any]) -> None:
    """Save pymupdf_clean_pages_only.txt to GCS - one file per file type"""
    file_type = file_data.get('type', 'unknown')
    type_short = file_type.replace('PDF', '').lower()
    clean_pages_path = f'phase1/results/{safe_carrier_name}_{type_short}_pymupdf_clean_pages_only_{timestamp}.txt'
    
    report_lines = []
    report_lines.append("PYMUPDF CLEAN PAGES ONLY - FOR SMART SELECTION")
    report_lines.append("=" * 80)
    report_lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    report_lines.append(f"Carrier: {carrier_name}")
    report_lines.append(f"File Type: {file_type.upper()}")
    report_lines.append("=" * 80)
    report_lines.append("")
    
    # Summary of clean pages
    total_pages = file_data.get('total_pages', 0)
    clean_count = file_data.get('clean_pages', 0)
    clean_pages_nums = file_data.get('clean_page_numbers', [])
    
    report_lines.append(f"Total Pages: {total_pages}")
    report_lines.append(f"Clean Pages: {clean_count}")
    report_lines.append(f"Clean Page Numbers: {clean_pages_nums}")
    report_lines.append("")
    
    # List clean pages with full text content
    clean_pages_data = file_data.get('clean_pages_data', [])
    page_details = file_data.get('page_details', {})
    for page_num in clean_pages_nums:
        if page_num in page_details:
            metrics = page_details[page_num]
            
            # Find text from clean_pages_data
            page_text = ""
            for page_result in clean_pages_data:
                if page_result.get('page_num') == page_num:
                    page_text = page_result.get('text', '')
                    break
            
            report_lines.append(f"PAGE {page_num}:")
            report_lines.append("-" * 40)
            report_lines.append(f"Quality: CLEAN")
            report_lines.append(f"Confidence: {metrics['confidence_score']:.1f}%")
            report_lines.append(f"Readable Words: {metrics['readable_words']}")
            report_lines.append(f"Total Characters: {metrics['total_chars']}")
            report_lines.append(f"(cid:XX) Codes: {metrics['cid_codes']}")
            report_lines.append(f"Gibberish Ratio: {metrics['gibberish_ratio']:.2%}")
            report_lines.append("")
            report_lines.append("TEXT CONTENT:")
            report_lines.append(page_text)
            report_lines.append("=" * 80)
            report_lines.append("")
    
    report_content = "\n".join(report_lines)
    
    blob = bucket.blob(clean_pages_path)
    blob.upload_from_string(report_content, content_type='text/plain')
    
    print(f"✅ Saved clean pages summary to: gs://{BUCKET_NAME}/{clean_pages_path}")


def process_upload_quality_analysis(upload_id: str) -> Dict[str, Any]:
    """
    Given an upload_id, read metadata, fetch PDFs from GCS, and analyze quality.
    Returns structured JSON with page analysis results.
    """
    bucket = _get_bucket()
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
                
                # Analyze quality
                analysis = _extract_and_analyze_pdf(pdf_bytes)
                
                files_analysis.append({
                    'type': file_type,
                    'path': gs_path,
                    'length_bytes': len(pdf_bytes),
                    'total_pages': analysis.get('total_pages', 0),
                    'clean_pages': len(analysis.get('clean_pages', [])),
                    'problem_pages': len(analysis.get('problem_pages', [])),
                    'borderline_pages': len(analysis.get('borderline_pages', [])),
                    'quality_summary': {
                        'total': analysis.get('total_pages', 0),
                        'clean': len(analysis.get('clean_pages', [])),
                        'problem': len(analysis.get('problem_pages', [])),
                        'borderline': len(analysis.get('borderline_pages', []))
                    },
                    'page_details': analysis.get('all_metrics', {}),
                    'clean_page_numbers': [p['page_num'] for p in analysis.get('clean_pages', [])],
                    'problem_page_numbers': [p['page_num'] for p in analysis.get('problem_pages', [])],
                    'borderline_page_numbers': [p['page_num'] for p in analysis.get('borderline_pages', [])],
                    'clean_pages_data': analysis.get('clean_pages', [])  # Include full data with text for Phase 2C
                })
                
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
    
    # Save to GCS results folder (pass both result and results for analysis)
    try:
        _save_quality_results_to_gcs(bucket, upload_id, result, {'carriers': results})
    except Exception as e:
        print(f"Warning: Failed to save results to GCS: {e}")
    
    # Automatically trigger Phase 2 OCR after Phase 1 completes (background task)
    try:
        print("\n✅ Phase 1 complete. Queueing Phase 2 OCR task...")
        from tasks import process_ocr_task
        process_ocr_task.delay(upload_id)  # Fire and forget - runs in background
        print("✅ Phase 2 OCR queued for background processing!")
    except Exception as e:
        print(f"Warning: Failed to queue OCR task: {e}")
    
    return result



