"""
QC New Unified Pipeline: Certificate + Policy validation in one go.
Runs cert extraction, then policy validation against cert ground truth.
"""
import sys
import os
from pathlib import Path
import json
import tempfile
import shutil
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional, Tuple

# Add qc-new to path
# In Docker: /app/qc_new_unified_pipeline.py and /app/qc-new/
# Locally: backend/qc_new_unified_pipeline.py and qc-new/
# Try both paths to support both environments
current_dir = Path(__file__).parent  # /app or backend/
# First try: same directory (for Docker: /app/qc-new)
qc_new_dir = current_dir / "qc-new"
if not qc_new_dir.exists():
    # Fallback: parent directory (for local: deployment2/qc-new)
    qc_new_dir = current_dir.parent / "qc-new"
if qc_new_dir.exists():
    sys.path.insert(0, str(qc_new_dir))
    print(f"[OK] Added qc-new to Python path: {qc_new_dir}")
else:
    print(f"[WARN] qc-new directory not found. Tried: {current_dir / 'qc-new'} and {current_dir.parent / 'qc-new'}")

# Define repo_root for debug directory paths (same logic as qc_new_dir)
# In Docker: /app, Locally: parent of backend/
repo_root = current_dir if (current_dir / "qc-new").exists() else current_dir.parent

from llm_pl import CertificateExtractor as PLCertificateExtractor
from llm_gl import CertificateExtractor as GLCertificateExtractor
from llm_pla import ApplicationExtractor as AcordCertificateExtractor
from llm_gla import ACORDGLAExtractor
from llm_pl_pol import PolicyValidator
from pl_cov_declarations import DeclarationsCoverageValidator
from pl_cov_perils import PerilsCoverageValidator
from pl_cov_crime_extensions import CrimeExtensionsCoverageValidator
from pl_cov_additional_interests import AdditionalInterestsCoverageValidator
from llm_gl_pol_cov import GLLimitsValidator
from cert_extract_pl import extract_tesseract, extract_pymupdf, combine_extractions
from cert_extract_pla import (
    extract_with_pdfplumber as acord_extract_with_pdfplumber,
    extract_pymupdf as acord_extract_pymupdf,
    extract_tesseract as acord_extract_tesseract,
    combine_extractions as acord_combine_extractions,
)
# Import GL ACORD extraction functions
try:
    from cert_extract_gla import (
        extract_with_pdfplumber as gl_acord_extract_with_pdfplumber,
        extract_pymupdf as gl_acord_extract_pymupdf,
        combine_extractions as gl_acord_combine_extractions,
    )
    # For GL ACORD, we'll use PL ACORD's Tesseract function if needed
    gl_acord_extract_tesseract = acord_extract_tesseract
except ImportError as e:
    print(f"⚠️ Warning: Could not import GL ACORD extraction functions: {e}")
    gl_acord_extract_with_pdfplumber = None
    gl_acord_extract_pymupdf = None
    gl_acord_extract_tesseract = None
    gl_acord_combine_extractions = None
# Import GL extraction functions
try:
    from cert_extract_gl import extract_pymupdf as gl_extract_pymupdf, extract_with_pdfplumber as gl_extract_with_pdfplumber, combine_extractions as gl_combine_extractions
except ImportError as e:
    print(f"⚠️ Warning: Could not import GL extraction functions: {e}")
    gl_extract_pymupdf = None
    gl_extract_with_pdfplumber = None
    gl_combine_extractions = None
from policy_extract import extract_tesseract as policy_extract_tesseract, extract_pymupdf as policy_extract_pymupdf
from policy_filter import PolicyPageExtractor
from combine_extractions import combine_extraction_files


def run_unified_qc(cert_pdf_path: str, policy_pdf_path: str, coverage_type: str = "property", upload_id: str = None) -> dict:
    """
    Run full QC: extract cert fields, then validate policy against cert.
    
    FULL PIPELINE:
    1. Certificate: OCR (Tesseract + PyMuPDF) → Combine → LLM extract fields
    2. Policy: OCR (Tesseract + PyMuPDF) → Filter → Combine → LLM validate
    
    Args:
        cert_pdf_path: Path to certificate PDF
        policy_pdf_path: Path to policy PDF
        coverage_type: "property" or "gl"
    
    Returns:
        {
            "certificate": {...cert fields...},
            "core_validations": {...core field validations...},
            "coverage_validations": {...coverage validations...},
            "summary": {...match/mismatch/not_found counts...}
        }
    """
    import os
    cpu_count = os.cpu_count() or 1
    joblib_threads = int(os.environ.get('JOBLIB_MAX_NUM_THREADS', cpu_count))
    
    print("\n" + "=" * 80)
    print("[QC UNIFIED PIPELINE]")
    print("=" * 80)
    print(f"System CPU Cores: {cpu_count}")
    print(f"JOBLIB_MAX_NUM_THREADS: {joblib_threads}")
    print(f"Allocated Workers: {joblib_threads}")
    print(f"Task Type: QC (Priority allocation)")
    print("=" * 80 + "\n")
    
    temp_dir = Path(tempfile.mkdtemp(prefix="qc_unified_"))
    
    try:
        # ========== STEP 1: Certificate OCR + Extract ==========
        print("[QC Unified] Step 1/5: Running Certificate OCR (Tesseract + PyMuPDF)...")
        cert_pdf = Path(cert_pdf_path)
        
        # OCR: Tesseract
        cert_tess_path = temp_dir / "cert1.txt"
        extract_tesseract(cert_pdf, cert_tess_path, n_jobs=-1)
        
        # OCR: PyMuPDF
        cert_pymupdf_path = temp_dir / "cert2.txt"
        extract_pymupdf(cert_pdf, cert_pymupdf_path, use_ocr=True, force_ocr=False)
        
        # Combine both OCR sources
        cert_combo_path = temp_dir / "cert_combo.txt"
        combine_extractions(cert_tess_path, cert_pymupdf_path, cert_combo_path, interleave_pages=True)
        
        # LLM: Extract certificate fields
        print("[QC Unified] Step 2/5: Extracting certificate fields with LLM...")
        with open(cert_combo_path, 'r', encoding='utf-8') as f:
            cert_text = f.read()
        cert_extractor = PLCertificateExtractor()  # Upgraded to powerful model
        cert_fields = cert_extractor.extract_fields(cert_text, use_dual_validation=True)
        print(f"[QC Unified] Certificate extracted: {len(cert_fields)} fields")
        
        # Save cert fields to temp JSON (needed by validators)
        cert_json_path = temp_dir / "cert.json"
        with open(cert_json_path, 'w', encoding='utf-8') as f:
            json.dump(cert_fields, f, indent=2)
        
        # ========== STEP 2: Policy OCR + Filter + Combine ==========
        print("[QC Unified] Step 3/5: Running Policy OCR (Tesseract + PyMuPDF)...")
        policy_pdf = Path(policy_pdf_path)
        
        # OCR: Tesseract
        policy_tess_path = temp_dir / "policy1.txt"
        policy_extract_tesseract(policy_pdf, policy_tess_path, n_jobs=-1, max_pages=None)
        
        # OCR: PyMuPDF
        policy_pymupdf_path = temp_dir / "policy2.txt"
        policy_extract_pymupdf(policy_pdf, policy_pymupdf_path, use_ocr=True, force_ocr=False, max_pages=None)
        
        # Filter: Keep only pages with dollar amounts
        print("[QC Unified] Step 3b/5: Filtering policy pages with dollar amounts...")
        policy_fil1_path = temp_dir / "policy_fil1.txt"
        policy_fil2_path = temp_dir / "policy_fil2.txt"
        
        # Filter Tesseract
        with open(policy_tess_path, 'r', encoding='utf-8') as f:
            policy_tess_text = f.read()
        extractor1 = PolicyPageExtractor(policy_tess_text, str(policy_tess_path))
        filtered1 = extractor1.extract_filtered_pages()
        with open(policy_fil1_path, 'w', encoding='utf-8') as f:
            f.write(filtered1)
        
        # Filter PyMuPDF
        with open(policy_pymupdf_path, 'r', encoding='utf-8') as f:
            policy_pymupdf_text = f.read()
        extractor2 = PolicyPageExtractor(policy_pymupdf_text, str(policy_pymupdf_path))
        filtered2 = extractor2.extract_filtered_pages()
        with open(policy_fil2_path, 'w', encoding='utf-8') as f:
            f.write(filtered2)
        
        # Combine both filtered OCR sources
        policy_combo_path = temp_dir / "policy_combo.txt"
        combine_extraction_files(str(policy_fil1_path), str(policy_fil2_path), str(policy_combo_path), interleave_pages=True)
        
        print(f"[QC Unified] Policy combined: {policy_combo_path.stat().st_size / 1024:.1f} KB")
        
        # ========== SAVE FILES LOCALLY FOR DEBUGGING ==========
        if upload_id:
            debug_dir = repo_root / "qc-new" / "debug" / upload_id
            debug_dir.mkdir(parents=True, exist_ok=True)
            
            # Save policy combo text (the main file sent to LLM)
            local_policy_combo = debug_dir / "policy_combo.txt"
            shutil.copy2(policy_combo_path, local_policy_combo)
            print(f"[QC Unified] 💾 Saved policy text to: {local_policy_combo}")
            
            # Save certificate JSON
            local_cert_json = debug_dir / "cert.json"
            shutil.copy2(cert_json_path, local_cert_json)
            print(f"[QC Unified] 💾 Saved certificate JSON to: {local_cert_json}")
            
            # Save intermediate OCR files for debugging
            local_policy_tess = debug_dir / "policy_tesseract.txt"
            local_policy_pymupdf = debug_dir / "policy_pymupdf.txt"
            local_cert_combo = debug_dir / "cert_combo.txt"
            
            shutil.copy2(policy_tess_path, local_policy_tess)
            shutil.copy2(policy_pymupdf_path, local_policy_pymupdf)
            shutil.copy2(cert_combo_path, local_cert_combo)
            
            print(f"[QC Unified] 💾 Saved debug files to: {debug_dir}")
        
        # ========== STEP 3: Core field validation ==========
        print("[QC Unified] Step 4/5: Running core field validation...")
        core_validator = PolicyValidator()  # Upgraded to powerful model
        core_validation = core_validator.validate_policy(str(cert_json_path), str(policy_combo_path))
        
        # ========== STEP 4: Coverage validation (4 modular validators - PARALLEL) ==========
        print("[QC Unified] Step 5/5: Running coverage validation in parallel (Declarations + Perils + Crime/Extensions + Additional Interests)...")

        import os
        # Default to previous behavior: run 4 coverage validators in parallel.
        # You can override this via env var to reduce OpenAI rate-limit pressure.
        validator_workers = int(os.getenv("QC_VALIDATOR_MAX_WORKERS", "4") or "4")
        validator_workers = max(1, min(4, validator_workers))

        def _fallback_validator_result(error: str) -> dict:
            # Keep shape flexible: downstream uses .get(..., []) for lists and .get("summary", {}) for summary
            return {
                "error": error,
                "summary": {"matched": 0, "mismatched": 0, "not_found": 0},
            }

        def _safe_load_json(path: Path, validator_name: str) -> dict:
            try:
                if not path.exists():
                    return _fallback_validator_result(f"{validator_name} produced no output file: {path}")
                with open(path, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception as e:
                return _fallback_validator_result(f"{validator_name} output read failed: {e}")
        
        # Define output paths
        declarations_output_path = temp_dir / "declarations_validation.json"
        perils_output_path = temp_dir / "perils_validation.json"
        crime_output_path = temp_dir / "crime_extensions_validation.json"
        additional_interests_output_path = temp_dir / "additional_interests_validation.json"
        
        # Define validator tasks
        def run_declarations():
            print("[QC Unified]   ⚡ Running Declarations validator (Building, BPP, Business Income, etc.)...")
            validator = DeclarationsCoverageValidator()  # Upgraded to powerful model
            try:
                validator.validate_declarations(str(cert_json_path), str(policy_combo_path), str(declarations_output_path))
                return ("declarations", _safe_load_json(declarations_output_path, "declarations"))
            except Exception as e:
                return ("declarations", _fallback_validator_result(str(e)))
        
        def run_perils():
            print("[QC Unified]   ⚡ Running Perils validator (Theft, Wind/Hail)...")
            validator = PerilsCoverageValidator()
            try:
                validator.validate_perils(str(cert_json_path), str(policy_combo_path), str(perils_output_path))
                return ("perils", _safe_load_json(perils_output_path, "perils"))
            except Exception as e:
                return ("perils", _fallback_validator_result(str(e)))
        
        def run_crime():
            print("[QC Unified]   ⚡ Running Crime/Extensions validator (Money & Securities, Employee Dishonesty)...")
            validator = CrimeExtensionsCoverageValidator()
            try:
                validator.validate_crime_extensions(str(cert_json_path), str(policy_combo_path), str(crime_output_path))
                return ("crime", _safe_load_json(crime_output_path, "crime_extensions"))
            except Exception as e:
                return ("crime", _fallback_validator_result(str(e)))
        
        def run_additional_interests():
            print("[QC Unified]   ⚡ Running Additional Interests validator (Mortgagee, Loss Payee, Additional Insured)...")
            validator = AdditionalInterestsCoverageValidator()
            try:
                validator.validate_additional_interests(str(cert_json_path), str(policy_combo_path), str(additional_interests_output_path))
                return ("additional_interests", _safe_load_json(additional_interests_output_path, "additional_interests"))
            except Exception as e:
                return ("additional_interests", _fallback_validator_result(str(e)))
        
        # Run all four validators in parallel
        results_dict = {}
        errors: dict = {}
        with ThreadPoolExecutor(max_workers=validator_workers) as executor:
            # Submit all tasks
            futures = {
                executor.submit(run_declarations): "declarations",
                executor.submit(run_perils): "perils",
                executor.submit(run_crime): "crime",
                executor.submit(run_additional_interests): "additional_interests"
            }
            
            # Collect results as they complete
            for future in as_completed(futures):
                validator_name = futures[future]
                try:
                    result_type, result_data = future.result()
                    results_dict[result_type] = result_data
                    print(f"[QC Unified]   ✓ {validator_name.capitalize()} validator completed")
                except Exception as e:
                    print(f"[QC Unified]   ❌ {validator_name.capitalize()} validator failed: {e}")
                    errors[validator_name] = str(e)
                    results_dict[validator_name] = _fallback_validator_result(str(e))
        
        # Extract results
        declarations_results = results_dict.get("declarations", _fallback_validator_result("declarations missing"))
        perils_results = results_dict.get("perils", _fallback_validator_result("perils missing"))
        crime_results = results_dict.get("crime", _fallback_validator_result("crime missing"))
        additional_interests_results = results_dict.get("additional_interests", _fallback_validator_result("additional_interests missing"))
        
        if errors:
            print(f"[QC Unified]   ⚠️ Some validators failed but QC will return partial results: {errors}")
        else:
            print("[QC Unified]   ✓ All validators completed successfully!")
        
        # Merge results from all four coverage validators
        result = {
            "certificate": cert_fields,
            "core_validations": core_validation.get("validation_results", {}),
            "validator_errors": errors,
            "coverage_validations": {
                # From Declarations validator
                "building": declarations_results.get("building_validations", []),
                "bpp": declarations_results.get("bpp_validations", []),
                "business_income": declarations_results.get("business_income_validations", []),
                "equipment_breakdown": declarations_results.get("equipment_breakdown_validations", []),
                "outdoor_signs": declarations_results.get("outdoor_signs_validations", []),
                "pumps_canopy": declarations_results.get("pumps_canopy_validations", []),
                # From Perils validator
                "theft": perils_results.get("theft_validations", []),
                "wind_hail": perils_results.get("wind_hail_validations", []),
                # From Crime/Extensions validator
                "money_and_securities": crime_results.get("money_securities_validations", []),
                "employee_dishonesty": crime_results.get("employee_dishonesty_validations", []),
                # From Additional Interests validator
                "additional_interests": additional_interests_results.get("additional_interests_validations", []),
            },
            "summary": {
                "core": core_validation.get("summary", {}),
                "coverage": {
                    # Merge summaries from all four validators
                    "declarations": declarations_results.get("summary", {}),
                    "perils": perils_results.get("summary", {}),
                    "crime_extensions": crime_results.get("summary", {}),
                    "additional_interests": additional_interests_results.get("summary", {}),
                }
            }
        }
        
        print("[QC Unified] ✓ Validation complete")
        return result
        
    finally:
        # Cleanup temp files
        try:
            shutil.rmtree(temp_dir)
        except Exception:
            pass


def run_unified_qc_pl_gl(
    pl_cert_pdf_path: str = None,
    gl_cert_pdf_path: str = None,
    policy_pdf_path: str = None,
    upload_id: str = None,
    acord_cert_pdf_path: str = None,
    gl_acord_cert_pdf_path: str = None,
) -> dict:
    """
    Run unified QC for both PL and GL certificates with shared policy.
    
    FULL PIPELINE:
    1. PL Certificate: OCR (Tesseract + PyMuPDF) → Combine → LLM extract fields → Validate
    2. GL Certificate: Extract (pdfplumber + PyMuPDF) → Combine → LLM extract fields → Validate
    3. Policy: OCR (Tesseract + PyMuPDF) → Filter → Combine → Shared for both validations
    
    Args:
        pl_cert_pdf_path: Path to PL certificate PDF (optional)
        gl_cert_pdf_path: Path to GL certificate PDF (optional)
        policy_pdf_path: Path to policy PDF (required)
        upload_id: Upload ID for debugging
    
    Returns:
        {
            "pl_certificate": {...PL cert fields...} or null,
            "gl_certificate": {...GL cert fields...} or null,
            "pl_validations": {...PL validations...} or null,
            "gl_validations": {...GL validations...} or null,
            "summary": {...combined summary...}
        }
    """
    if not pl_cert_pdf_path and not gl_cert_pdf_path:
        # For now, ACORD is an optional extra certificate; we still require PL or GL
        raise ValueError("At least one certificate (PL or GL) must be provided")
    if not policy_pdf_path:
        raise ValueError("Policy PDF is required")
    
    import os
    cpu_count = os.cpu_count() or 1
    joblib_threads = int(os.environ.get('JOBLIB_MAX_NUM_THREADS', cpu_count))
    
    print("\n" + "=" * 80)
    print("[QC UNIFIED PIPELINE - PL + GL]")
    print("=" * 80)
    print(f"System CPU Cores: {cpu_count}")
    print(f"JOBLIB_MAX_NUM_THREADS: {joblib_threads}")
    print(f"Allocated Workers: {joblib_threads}")
    print(f"Task Type: QC (Priority allocation)")
    print("=" * 80 + "\n")

    # Default to previous behavior: run 4 PL coverage validators in parallel.
    # You can override this via env var to reduce OpenAI rate-limit pressure.
    validator_workers = int(os.getenv("QC_VALIDATOR_MAX_WORKERS", "4") or "4")
    validator_workers = max(1, min(4, validator_workers))

    def _fallback_validator_result(error: str) -> dict:
        return {
            "error": error,
            "summary": {"matched": 0, "mismatched": 0, "not_found": 0},
        }

    def _safe_load_json(path: Path, validator_name: str) -> dict:
        try:
            if not path.exists():
                return _fallback_validator_result(f"{validator_name} produced no output file: {path}")
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            return _fallback_validator_result(f"{validator_name} output read failed: {e}")
    
    temp_dir = Path(tempfile.mkdtemp(prefix="qc_unified_pl_gl_"))
    
    try:
        result = {
            "pl_certificate": None,
            "gl_certificate": None,
            "acord_certificate": None,
            "pl_validations": None,
            "gl_validations": None,
            "summary": {},
        }

        # Keep copies of PL validation results for later ACORD comparisons
        pl_core_validation_raw = None
        pl_coverage_validations_raw = None
        
        # ========== STEP 1: Process Policy (Shared) ==========
        print("[QC Unified PL+GL] Step 1/6: Processing Policy (shared for both)...")
        policy_pdf = Path(policy_pdf_path)
        
        # OCR: Tesseract (optional - may not be available)
        policy_tess_path = temp_dir / "policy1.txt"
        print("[QC Unified PL+GL] Attempting Tesseract extraction...")
        tess_success = policy_extract_tesseract(policy_pdf, policy_tess_path, n_jobs=-1, max_pages=None)
        if tess_success:
            print("[QC Unified PL+GL] ✅ Tesseract extraction succeeded")
        else:
            print("[QC Unified PL+GL] ⚠️  Tesseract extraction skipped or failed (using PyMuPDF only)")
        
        # OCR: PyMuPDF (required - should always work)
        policy_pymupdf_path = temp_dir / "policy2.txt"
        print("[QC Unified PL+GL] Running PyMuPDF extraction...")
        pymupdf_success = policy_extract_pymupdf(policy_pdf, policy_pymupdf_path, use_ocr=True, force_ocr=False, max_pages=None)
        
        if not pymupdf_success:
            raise Exception("PyMuPDF extraction failed - this is required for QC processing")
        
        # Filter: Keep only pages with dollar amounts
        print("[QC Unified PL+GL] Step 1b/6: Filtering policy pages with dollar amounts...")
        policy_fil1_path = temp_dir / "policy_fil1.txt"
        policy_fil2_path = temp_dir / "policy_fil2.txt"
        
        # Filter Tesseract (only if extraction succeeded)
        filtered1 = ""
        if tess_success and policy_tess_path.exists():
            try:
                with open(policy_tess_path, 'r', encoding='utf-8') as f:
                    policy_tess_text = f.read()
                extractor1 = PolicyPageExtractor(policy_tess_text, str(policy_tess_path))
                filtered1 = extractor1.extract_filtered_pages()
                with open(policy_fil1_path, 'w', encoding='utf-8') as f:
                    f.write(filtered1)
            except Exception as e:
                print(f"[QC Unified PL+GL] ⚠️  Error filtering Tesseract extraction: {e}")
                filtered1 = ""
        else:
            print("[QC Unified PL+GL] ⚠️  Tesseract extraction not available - using PyMuPDF only")
        
        # Filter PyMuPDF (required)
        filtered2 = ""
        if pymupdf_success and policy_pymupdf_path.exists():
            try:
                with open(policy_pymupdf_path, 'r', encoding='utf-8') as f:
                    policy_pymupdf_text = f.read()
                extractor2 = PolicyPageExtractor(policy_pymupdf_text, str(policy_pymupdf_path))
                filtered2 = extractor2.extract_filtered_pages()
                with open(policy_fil2_path, 'w', encoding='utf-8') as f:
                    f.write(filtered2)
            except Exception as e:
                print(f"[QC Unified PL+GL] ⚠️  Error filtering PyMuPDF extraction: {e}")
                filtered2 = ""
        else:
            raise Exception(f"PyMuPDF extraction file not found: {policy_pymupdf_path}")
        
        # Check if filtering returned empty - if so, use unfiltered text as fallback
        use_filtered = True
        if not filtered1.strip() and not filtered2.strip():
            print("[QC Unified PL+GL] ⚠️  Filter returned empty - using unfiltered text as fallback")
            use_filtered = False
        
        # Combine both OCR sources (filtered or unfiltered)
        policy_combo_path = temp_dir / "policy_combo.txt"
        if use_filtered:
            combine_extraction_files(str(policy_fil1_path), str(policy_fil2_path), str(policy_combo_path), interleave_pages=True)
        else:
            # Fallback: combine unfiltered sources
            combine_extraction_files(str(policy_tess_path), str(policy_pymupdf_path), str(policy_combo_path), interleave_pages=True)
        print(f"[QC Unified PL+GL] Policy combined: {policy_combo_path.stat().st_size / 1024:.1f} KB")
        
        # ========== STEP 2: Process PL Certificate (if provided) ==========
        pl_result = None
        if pl_cert_pdf_path:
            print("[QC Unified PL+GL] Step 2/6: Processing PL Certificate...")
            pl_cert_pdf = Path(pl_cert_pdf_path)
            
            # OCR: Tesseract
            pl_cert_tess_path = temp_dir / "pl_cert1.txt"
            extract_tesseract(pl_cert_pdf, pl_cert_tess_path, n_jobs=-1)
            
            # OCR: PyMuPDF
            pl_cert_pymupdf_path = temp_dir / "pl_cert2.txt"
            extract_pymupdf(pl_cert_pdf, pl_cert_pymupdf_path, use_ocr=True, force_ocr=False)
            
            # Combine both OCR sources
            pl_cert_combo_path = temp_dir / "pl_cert_combo.txt"
            combine_extractions(pl_cert_tess_path, pl_cert_pymupdf_path, pl_cert_combo_path, interleave_pages=True)
            
            # LLM: Extract PL certificate fields
            print("[QC Unified PL+GL] Step 3/6: Extracting PL certificate fields with LLM...")
            with open(pl_cert_combo_path, 'r', encoding='utf-8') as f:
                pl_cert_text = f.read()
            pl_extractor = PLCertificateExtractor()  # Uses default model
            pl_cert_fields = pl_extractor.extract_fields(pl_cert_text, use_dual_validation=True)
            print(f"[QC Unified PL+GL] PL Certificate extracted: {len(pl_cert_fields)} fields")
            
            # Save PL cert fields to temp JSON
            pl_cert_json_path = temp_dir / "pl_cert.json"
            with open(pl_cert_json_path, 'w', encoding='utf-8') as f:
                json.dump(pl_cert_fields, f, indent=2)
            
            result["pl_certificate"] = pl_cert_fields
            
            # Run PL validations (reuse policy_combo_path)
            print("[QC Unified PL+GL] Step 4/6: Running PL validations...")
            # Core field validation
            core_validator = PolicyValidator()  # Uses default model
            core_validation = core_validator.validate_policy(str(pl_cert_json_path), str(policy_combo_path))
            
            # Coverage validations (parallel)
            print("[QC Unified PL+GL] Running PL coverage validations in parallel...")
            declarations_output_path = temp_dir / "pl_declarations_validation.json"
            perils_output_path = temp_dir / "pl_perils_validation.json"
            crime_output_path = temp_dir / "pl_crime_extensions_validation.json"
            additional_interests_output_path = temp_dir / "pl_additional_interests_validation.json"
            
            def run_pl_declarations():
                validator = DeclarationsCoverageValidator()
                try:
                    validator.validate_declarations(str(pl_cert_json_path), str(policy_combo_path), str(declarations_output_path))
                    return ("declarations", _safe_load_json(declarations_output_path, "pl_declarations"))
                except Exception as e:
                    return ("declarations", _fallback_validator_result(str(e)))
            
            def run_pl_perils():
                validator = PerilsCoverageValidator()
                try:
                    validator.validate_perils(str(pl_cert_json_path), str(policy_combo_path), str(perils_output_path))
                    return ("perils", _safe_load_json(perils_output_path, "pl_perils"))
                except Exception as e:
                    return ("perils", _fallback_validator_result(str(e)))
            
            def run_pl_crime():
                validator = CrimeExtensionsCoverageValidator()
                try:
                    validator.validate_crime_extensions(str(pl_cert_json_path), str(policy_combo_path), str(crime_output_path))
                    return ("crime", _safe_load_json(crime_output_path, "pl_crime_extensions"))
                except Exception as e:
                    return ("crime", _fallback_validator_result(str(e)))
            
            def run_pl_additional_interests():
                validator = AdditionalInterestsCoverageValidator()
                try:
                    validator.validate_additional_interests(str(pl_cert_json_path), str(policy_combo_path), str(additional_interests_output_path))
                    return ("additional_interests", _safe_load_json(additional_interests_output_path, "pl_additional_interests"))
                except Exception as e:
                    return ("additional_interests", _fallback_validator_result(str(e)))
            
            # Run all four validators in parallel
            pl_results_dict = {}
            pl_validator_errors: dict = {}
            with ThreadPoolExecutor(max_workers=validator_workers) as executor:
                futures = {
                    executor.submit(run_pl_declarations): "declarations",
                    executor.submit(run_pl_perils): "perils",
                    executor.submit(run_pl_crime): "crime",
                    executor.submit(run_pl_additional_interests): "additional_interests"
                }
                
                for future in as_completed(futures):
                    validator_name = futures[future]
                    try:
                        result_type, result_data = future.result()
                        pl_results_dict[result_type] = result_data
                        print(f"[QC Unified PL+GL]   ✓ PL {validator_name.capitalize()} validator completed")
                    except Exception as e:
                        print(f"[QC Unified PL+GL]   ❌ PL {validator_name.capitalize()} validator failed: {e}")
                        pl_validator_errors[validator_name] = str(e)
                        pl_results_dict[validator_name] = _fallback_validator_result(str(e))
            
            declarations_results = pl_results_dict.get("declarations", _fallback_validator_result("pl_declarations missing"))
            perils_results = pl_results_dict.get("perils", _fallback_validator_result("pl_perils missing"))
            crime_results = pl_results_dict.get("crime", _fallback_validator_result("pl_crime missing"))
            additional_interests_results = pl_results_dict.get("additional_interests", _fallback_validator_result("pl_additional_interests missing"))
            
            pl_core_validation_raw = core_validation

            pl_coverage_validations_raw = {
                "declarations": declarations_results,
                "perils": perils_results,
                "crime": crime_results,
                "additional_interests": additional_interests_results,
            }

            result["pl_validations"] = {
                "core_validations": core_validation.get("validation_results", {}),
                "validator_errors": pl_validator_errors,
                "coverage_validations": {
                    "building": declarations_results.get("building_validations", []),
                    "bpp": declarations_results.get("bpp_validations", []),
                    "business_income": declarations_results.get("business_income_validations", []),
                    "equipment_breakdown": declarations_results.get("equipment_breakdown_validations", []),
                    "outdoor_signs": declarations_results.get("outdoor_signs_validations", []),
                    "pumps_canopy": declarations_results.get("pumps_canopy_validations", []),
                    "theft": perils_results.get("theft_validations", []),
                    "wind_hail": perils_results.get("wind_hail_validations", []),
                    "money_and_securities": crime_results.get("money_securities_validations", []),
                    "employee_dishonesty": crime_results.get("employee_dishonesty_validations", []),
                    "additional_interests": additional_interests_results.get("additional_interests_validations", []),
                },
                "summary": {
                    "core": core_validation.get("summary", {}),
                    "coverage": {
                        "declarations": declarations_results.get("summary", {}),
                        "perils": perils_results.get("summary", {}),
                        "crime_extensions": crime_results.get("summary", {}),
                        "additional_interests": additional_interests_results.get("summary", {}),
                    }
                }
            }
        
        # ========== STEP 2B: Extract Policy Core Fields for GL ACORD (if PL cert not provided but GL ACORD is) ==========
        if not pl_core_validation_raw and gl_acord_cert_pdf_path and policy_pdf_path:
            # If PL cert not provided but GL ACORD is, we still need policy core fields for comparison
            print("[QC Unified PL+GL] PL cert not provided, but extracting policy core fields for GL ACORD comparison...")
            try:
                # Create a minimal certificate JSON with just the structure needed
                minimal_cert_json_path = temp_dir / "minimal_cert.json"
                minimal_cert = {
                    "policy_number": None,
                    "effective_date": None,
                    "expiration_date": None,
                    "insured_name": None,
                    "mailing_address": None,
                    "location_address": None,
                }
                with open(minimal_cert_json_path, "w", encoding="utf-8") as f:
                    json.dump(minimal_cert, f, indent=2)
                
                # Run core validation to extract policy fields (certificate values will be None, but policy values will be extracted)
                core_validator = PolicyValidator()
                core_validation = core_validator.validate_policy(str(minimal_cert_json_path), str(policy_combo_path))
                pl_core_validation_raw = core_validation
                print("[QC Unified PL+GL] ✅ Policy core fields extracted for GL ACORD comparison")
            except Exception as e:
                print(f"[QC Unified PL+GL] ⚠️  Failed to extract policy core fields: {e}")
                pl_core_validation_raw = None
        
        # ========== STEP 3: Process GL Certificate (if provided) ==========
        gl_result = None
        if gl_cert_pdf_path:
            print("[QC Unified PL+GL] Step 5/6: Processing GL Certificate...")
            gl_cert_pdf = Path(gl_cert_pdf_path)
            
            # Extract: pdfplumber (table-aware)
            gl_cert_pdfplumber_path = temp_dir / "gl_cert1.txt"
            if gl_extract_with_pdfplumber:
                gl_extract_with_pdfplumber(gl_cert_pdf, gl_cert_pdfplumber_path)
            else:
                raise ImportError("GL extraction functions not available. Please ensure cert_extract_gl module is accessible.")
            
            # Extract: PyMuPDF (text layer)
            gl_cert_pymupdf_path = temp_dir / "gl_cert2.txt"
            if gl_extract_pymupdf:
                gl_extract_pymupdf(gl_cert_pdf, gl_cert_pymupdf_path)
            else:
                raise ImportError("GL extraction functions not available. Please ensure cert_extract_gl module is accessible.")
            
            # Combine both extraction sources
            gl_cert_combo_path = temp_dir / "gl_cert_combo.txt"
            if gl_combine_extractions:
                gl_combine_extractions(gl_cert_pdfplumber_path, gl_cert_pymupdf_path, gl_cert_combo_path, interleave_pages=True)
            else:
                raise ImportError("GL extraction functions not available. Please ensure cert_extract_gl module is accessible.")
            
            # LLM: Extract GL certificate fields
            print("[QC Unified PL+GL] Step 6/6: Extracting GL certificate fields with LLM...")
            with open(gl_cert_combo_path, 'r', encoding='utf-8') as f:
                gl_cert_text = f.read()
            gl_extractor = GLCertificateExtractor()  # Uses default model
            gl_cert_fields = gl_extractor.extract_fields(gl_cert_text)
            print(f"[QC Unified PL+GL] GL Certificate extracted: {len(gl_cert_fields)} fields")
            
            # Save GL cert fields to temp JSON
            gl_cert_json_path = temp_dir / "gl_cert.json"
            with open(gl_cert_json_path, 'w', encoding='utf-8') as f:
                json.dump(gl_cert_fields, f, indent=2)
            
            result["gl_certificate"] = gl_cert_fields
            
            # Run GL validations
            print("[QC Unified PL+GL] Running GL validations...")
            gl_validator = GLLimitsValidator()  # Uses default model
            gl_validation_output = temp_dir / "gl_validation.json"
            gl_validator.validate_limits(str(gl_cert_json_path), str(policy_combo_path), str(gl_validation_output))
            
            # Load GL validation results
            with open(gl_validation_output, 'r', encoding='utf-8') as f:
                gl_validation_data = json.load(f)
            
            # GL validator returns limit validations at top level, structure them properly
            result["gl_validations"] = {
                "core_validations": {},  # GL doesn't have core validations like PL
                "address_validations": gl_validation_data.get("address_validations", []),
                "coverage_presence_validations": gl_validation_data.get("coverage_presence_validations", []),
                "coverage_validations": {
                    "cgl_limit_validations": gl_validation_data.get("cgl_limit_validations", []),
                    "umbrella_limit_validations": gl_validation_data.get("umbrella_limit_validations", []),
                    "epl_limit_validations": gl_validation_data.get("epl_limit_validations", []),
                    "liquor_limit_validations": gl_validation_data.get("liquor_limit_validations", []),
                },
                "summary": gl_validation_data.get("summary", {})
            }
        
        # ========== STEP 4: Process ACORD Certificate (if provided) ==========
        if acord_cert_pdf_path:
            print("[QC Unified PL+GL] Processing ACORD Certificate (optional)...")
            acord_pdf = Path(acord_cert_pdf_path)

            # Extract: pdfplumber (table-aware)
            acord_pdfplumber_path = temp_dir / "acord_cert1.txt"
            acord_extract_with_pdfplumber(acord_pdf, acord_pdfplumber_path)

            # Extract: PyMuPDF (text layer)
            acord_pymupdf_path = temp_dir / "acord_cert2.txt"
            acord_extract_pymupdf(acord_pdf, acord_pymupdf_path, use_ocr=True, force_ocr=False)

            # Extract: Tesseract (OCR)
            acord_tesseract_path = temp_dir / "acord_cert3.txt"
            acord_extract_tesseract(acord_pdf, acord_tesseract_path, n_jobs=-1)

            # Combine all three extraction sources
            acord_combo_path = temp_dir / "acord_cert_combo.txt"
            acord_combine_extractions(
                acord_pdfplumber_path,
                acord_pymupdf_path,
                acord_tesseract_path,
                acord_combo_path,
                interleave_pages=True,
            )

            # LLM: Extract ACORD certificate fields
            print("[QC Unified PL+GL] Extracting ACORD certificate fields with LLM...")
            with open(acord_combo_path, "r", encoding="utf-8") as f:
                acord_cert_text = f.read()
            acord_extractor = AcordCertificateExtractor()
            acord_cert_fields = acord_extractor.extract_fields(acord_cert_text)
            print(f"[QC Unified PL+GL] ACORD Certificate extracted: {len(acord_cert_fields)} fields")

            result["acord_certificate"] = acord_cert_fields

            # Save ACORD certificate JSON for debugging if upload_id provided
            if upload_id:
                debug_dir = repo_root / "qc-new" / "debug" / upload_id
                debug_dir.mkdir(parents=True, exist_ok=True)
                local_acord_cert_json = debug_dir / "acord_cert.json"
                with open(local_acord_cert_json, "w", encoding="utf-8") as f:
                    json.dump(acord_cert_fields, f, indent=2)
                print(f"[QC Unified PL+GL] 💾 Saved ACORD certificate JSON to: {local_acord_cert_json}")

        # ========== STEP 4B: Process GL ACORD Certificate (if provided) ==========
        if gl_acord_cert_pdf_path:
            print("[QC Unified PL+GL] Processing GL ACORD Certificate (optional)...")
            gl_acord_pdf = Path(gl_acord_cert_pdf_path)

            # Extract: pdfplumber (table-aware)
            gl_acord_pdfplumber_path = temp_dir / "gl_acord_cert1.txt"
            if gl_acord_extract_with_pdfplumber:
                gl_acord_extract_with_pdfplumber(gl_acord_pdf, gl_acord_pdfplumber_path)
            else:
                print("⚠️ GL ACORD pdfplumber extraction not available")

            # Extract: PyMuPDF (text layer)
            gl_acord_pymupdf_path = temp_dir / "gl_acord_cert2.txt"
            if gl_acord_extract_pymupdf:
                gl_acord_extract_pymupdf(gl_acord_pdf, gl_acord_pymupdf_path)
            else:
                print("⚠️ GL ACORD PyMuPDF extraction not available")

            # Extract: Tesseract (OCR) - reuse PL ACORD's Tesseract function
            gl_acord_tesseract_path = temp_dir / "gl_acord_cert3.txt"
            if gl_acord_extract_tesseract:
                gl_acord_extract_tesseract(gl_acord_pdf, gl_acord_tesseract_path, n_jobs=-1)
            else:
                print("⚠️ GL ACORD Tesseract extraction not available")

            # Combine extraction sources (GL ACORD combine_extractions takes 2 files: pdfplumber and pymupdf)
            gl_acord_combo_path = temp_dir / "gl_acord_cert_combo.txt"
            if gl_acord_combine_extractions:
                # For GL ACORD, combine pdfplumber and pymupdf
                if gl_acord_pdfplumber_path.exists() and gl_acord_pymupdf_path.exists():
                    gl_acord_combine_extractions(
                        gl_acord_pdfplumber_path,
                        gl_acord_pymupdf_path,
                        gl_acord_combo_path,
                        interleave_pages=True,
                    )
                elif gl_acord_pdfplumber_path.exists():
                    shutil.copy(gl_acord_pdfplumber_path, gl_acord_combo_path)
                elif gl_acord_pymupdf_path.exists():
                    shutil.copy(gl_acord_pymupdf_path, gl_acord_combo_path)
            else:
                # Fallback: use pdfplumber if available
                if gl_acord_pdfplumber_path.exists():
                    shutil.copy(gl_acord_pdfplumber_path, gl_acord_combo_path)
                elif gl_acord_pymupdf_path.exists():
                    shutil.copy(gl_acord_pymupdf_path, gl_acord_combo_path)

            # LLM: Extract GL ACORD certificate fields
            if gl_acord_combo_path.exists():
                print("[QC Unified PL+GL] Extracting GL ACORD certificate fields with LLM...")
                with open(gl_acord_combo_path, "r", encoding="utf-8") as f:
                    gl_acord_cert_text = f.read()
                gl_acord_extractor = ACORDGLAExtractor()
                gl_acord_cert_fields = gl_acord_extractor.extract_fields(gl_acord_cert_text)
                print(f"[QC Unified PL+GL] GL ACORD Certificate extracted: {len(gl_acord_cert_fields)} fields")

                result["gl_acord_certificate"] = gl_acord_cert_fields

                # Save GL ACORD certificate JSON for debugging if upload_id provided
                if upload_id:
                    debug_dir = repo_root / "qc-new" / "debug" / upload_id
                    debug_dir.mkdir(parents=True, exist_ok=True)
                    local_gl_acord_cert_json = debug_dir / "gl_acord_cert.json"
                    with open(local_gl_acord_cert_json, "w", encoding="utf-8") as f:
                        json.dump(gl_acord_cert_fields, f, indent=2)
                    print(f"[QC Unified PL+GL] 💾 Saved GL ACORD certificate JSON to: {local_gl_acord_cert_json}")

        # ========== Helper functions for smart comparison (with LLM fallback) ==========
        # Initialize LLM client for fallback comparisons (if available)
        llm_client = None
        try:
            from openai import OpenAI
            api_key = os.getenv("OPENAI_API_KEY")
            if api_key:
                llm_client = OpenAI(api_key=api_key)
                print("[QC Unified PL+GL] ✅ LLM client initialized for smart comparisons")
        except Exception as e:
            print(f"[QC Unified PL+GL] ⚠️  LLM client not available: {e}")

        def _smart_normalize_value(val, field_type: Optional[str] = None) -> Optional[str]:
            """
            Smart normalization that handles dates, amounts, and names.
            Returns normalized value or None.
            """
            if val is None:
                return None
            s = str(val).strip()
            if not s:
                return None
            
            # Try date normalization first
            # Pattern for MM/DD/YYYY or M/D/YYYY
            date_pattern1 = r'(\d{1,2})/(\d{1,2})/(\d{4})'
            match1 = re.search(date_pattern1, s)
            if match1:
                month, day, year = match1.groups()
                return f"{year}{month.zfill(2)}{day.zfill(2)}"  # YYYYMMDD
            
            # Pattern for "Month DD, YYYY" or "Month D, YYYY"
            month_names = {
                'january': '01', 'february': '02', 'march': '03', 'april': '04',
                'may': '05', 'june': '06', 'july': '07', 'august': '08',
                'september': '09', 'october': '10', 'november': '11', 'december': '12'
            }
            date_pattern2 = r'([a-z]+)\s+(\d{1,2}),?\s+(\d{4})'
            match2 = re.search(date_pattern2, s.lower())
            if match2:
                month_name, day, year = match2.groups()
                month = month_names.get(month_name)
                if month:
                    return f"{year}{month}{day.zfill(2)}"
            
            # Try amount extraction (for coverage limits)
            # Look for patterns like "$ 10,000", "10,000", "Limit $10,000", "$10,000 any one premises"
            amount_pattern = r'\$?\s*(\d{1,3}(?:,\d{3})*(?:\.\d+)?)'
            amount_match = re.search(amount_pattern, s)
            if amount_match:
                amount_str = amount_match.group(1).replace(',', '')
                return amount_str  # Return numeric value only
            
            # For names and other text: normalize but keep structure
            # Remove extra spaces, normalize case, remove special chars but keep words
            name_cleaned = re.sub(r'\s+', ' ', s.upper().strip())
            name_cleaned = re.sub(r'[&,;]', ' ', name_cleaned)
            name_cleaned = ' '.join(name_cleaned.split())
            return name_cleaned

        def _llm_compare_values(field_name: str, values: list[str], client) -> Tuple[bool, str]:
            """
            Use LLM to compare values when regex normalization fails.
            Returns (is_match: bool, explanation: str)
            """
            if not values or len(values) < 2:
                return True, "Only one value present"
            
            # Filter out None/empty values
            non_empty_values = [v for v in values if v and str(v).strip()]
            if len(non_empty_values) < 2:
                return True, "Insufficient values to compare"
            
            prompt = f"""You are comparing insurance document field values. Determine if these values represent the SAME information, despite formatting differences.

Field: {field_name}
Values to compare:
{chr(10).join(f"- {i+1}. {v}" for i, v in enumerate(non_empty_values))}

Examples of MATCHES:
- "September 16, 2025" = "09/16/2025" (same date, different format)
- "Limit $10,000" = "10,000" (same amount, different format)
- "WESTSIDE MART LLC DBA WESTSIDE SMOKE & VAPE" contains "WESTSIDE MART LLC" (truncated but same entity)
- "$ 100,000 any one premises" = "$ 100,000" (same amount, extra descriptive text)
- "$ 5,000 any one person" = "$ 5,000" (same amount, extra descriptive text)

Return ONLY a JSON object:
{{
    "match": true/false,
    "reason": "brief explanation"
}}"""

            try:
                response = client.chat.completions.create(
                    model="gpt-4o-mini",  # Use cheap model for simple comparisons
                    messages=[
                        {"role": "system", "content": "You are an expert at comparing insurance document values. Return only valid JSON."},
                        {"role": "user", "content": prompt}
                    ],
                    temperature=0.1,
                    response_format={"type": "json_object"}
                )
                result = json.loads(response.choices[0].message.content)
                return result.get("match", False), result.get("reason", "")
            except Exception as e:
                print(f"      [WARNING] LLM comparison failed: {e}, falling back to strict match")
                return False, "LLM comparison failed"

        def _three_way_status_with_llm_fallback(
            policy_val, cert_val, acord_val, 
            field_name: str = None,
            use_llm_fallback: bool = True
        ) -> str:
            """
            Three-way comparison with smart normalization and LLM fallback.
            Requires ALL THREE values to be present and match for MATCH status.
            """
            # Normalize values (handles "N/A", None, empty strings)
            def _is_empty_or_na(val):
                """Check if value is empty, None, or "N/A"."""
                if val is None:
                    return True
                s = str(val).strip().upper()
                return not s or s == "N/A" or s == "NULL" or s == "NONE"
            
            # Check if values are empty/N/A
            policy_empty = _is_empty_or_na(policy_val)
            cert_empty = _is_empty_or_na(cert_val)
            acord_empty = _is_empty_or_na(acord_val)
            
            # If all three are empty/N/A, return NOT_FOUND
            if policy_empty and cert_empty and acord_empty:
                return "NOT_FOUND"
            
            # If any one is empty/N/A while others have values, it's a MISMATCH
            # (All three must be present and match for MATCH status)
            if policy_empty or cert_empty or acord_empty:
                return "MISMATCH"
            
            # All three have values - now check if they match
            # First try smart normalization
            norm_policy = _smart_normalize_value(policy_val, field_name)
            norm_cert = _smart_normalize_value(cert_val, field_name)
            norm_acord = _smart_normalize_value(acord_val, field_name)
            
            # Check if all normalized values match
            if norm_policy is not None and norm_cert is not None and norm_acord is not None:
                if len(set([norm_policy, norm_cert, norm_acord])) == 1:
                    return "MATCH"
            
            # If normalization didn't match, try LLM fallback (if enabled and client available)
            # Only use LLM if all three values are present (we already checked above)
            original_vals = [policy_val, cert_val, acord_val]
            if use_llm_fallback and llm_client and len(original_vals) == 3:
                print(f"      [INFO] Regex normalization didn't match for {field_name}, trying LLM comparison...")
                is_match, reason = _llm_compare_values(field_name or "field", original_vals, llm_client)
                if is_match:
                    print(f"      [INFO] LLM determined values match: {reason}")
                    return "MATCH"
                else:
                    print(f"      [INFO] LLM determined values don't match: {reason}")
            
            return "MISMATCH"

        # ========== STEP 5: Build PL vs Policy vs ACORD comparisons (core fields + coverages) ==========
        if (
            result.get("pl_certificate")
            and result.get("acord_certificate")
            and pl_core_validation_raw
            and isinstance(pl_core_validation_raw.get("validation_results"), dict)
        ):
            print("[QC Unified PL+GL] Building PL vs Policy vs ACORD core field comparisons...")

            # Use the smart comparison function with LLM fallback
            def _three_way_status(policy_val, pl_val, acord_val, field_name: str = None):
                """Return MATCH if all present values agree, MISMATCH if any conflict, NOT_FOUND if all missing."""
                return _three_way_status_with_llm_fallback(
                    policy_val, pl_val, acord_val,
                    field_name=field_name,
                    use_llm_fallback=True
                )

            def _norm_name(name: str) -> str:
                """Normalize coverage names for loose matching (used for ACORD coverages)."""
                if not name:
                    return ""
                s = str(name).strip().lower()
                # Strip parenthetical suffixes like "(1/6)", "(1 of 6)", etc.
                s = re.sub(r'\s*\([^)]*\)\s*', '', s)
                # Remove common suffixes that don't affect the core coverage name
                # e.g., "Business Income with Extra Expense" -> "Business Income"
                common_suffixes = [
                    r'\s+with\s+extra\s+expense',
                    r'\s+and\s+extra\s+expense',
                    r'\s+extra\s+expense',
                    r'\s+with\s+extended\s+period',
                    r'\s+extended\s+period',
                ]
                for suffix in common_suffixes:
                    s = re.sub(suffix, '', s, flags=re.IGNORECASE)
                # Keep only alphanumeric characters
                return "".join(ch for ch in s if ch.isalnum())
            
            def _find_acord_coverage(pl_cov_name: str, acord_norm_map: dict) -> tuple:
                """
                Find matching ACORD coverage for a PL coverage name.
                Tries multiple matching strategies for flexibility.
                
                Returns:
                    (acord_display_name, acord_value) or (None, None)
                """
                if not pl_cov_name:
                    return None, None
                
                norm_pl_name = _norm_name(pl_cov_name)
                if not norm_pl_name:
                    return None, None
                
                # Strategy 1: Exact normalized match
                if norm_pl_name in acord_norm_map:
                    return acord_norm_map[norm_pl_name]
                
                # Strategy 2: Partial match - check if normalized PL name contains or is contained in ACORD names
                # This handles cases like "Business Income with Extra Expense" matching "Business Income"
                for acord_norm, (acord_display, acord_val) in acord_norm_map.items():
                    # Check if PL name contains ACORD name (e.g., "businessincomewithextraexpense" contains "businessincome")
                    if norm_pl_name.startswith(acord_norm) or acord_norm.startswith(norm_pl_name):
                        # Additional check: ensure it's a meaningful match (at least 8 chars overlap)
                        min_len = min(len(norm_pl_name), len(acord_norm))
                        if min_len >= 8:  # Require at least 8 characters for partial match
                            return acord_display, acord_val
                
                # Strategy 3: Extract core coverage name and try again
                # For "Business Income with Extra Expense", try matching just "Business Income"
                core_terms = [
                    "businessincome", "building", "businesspersonalproperty", "bpp",
                    "equipmentbreakdown", "outdoorsigns", "windhail", "windstormhail",
                    "theft", "moneysecurities", "employeedishonesty", "spoilage",
                    "pumps", "canopy"
                ]
                for core_term in core_terms:
                    if core_term in norm_pl_name:
                        if core_term in acord_norm_map:
                            return acord_norm_map[core_term]
                        # Also try partial match with core term
                        for acord_norm, (acord_display, acord_val) in acord_norm_map.items():
                            if core_term in acord_norm or acord_norm in core_term:
                                return acord_display, acord_val
                
                return None, None

            core_field_results = {}
            validation_results = pl_core_validation_raw.get("validation_results", {})
            acord_data = result.get("acord_certificate") or {}

            for field_name, field_data in validation_results.items():
                policy_val = field_data.get("policy_value")
                pl_val = field_data.get("certificate_value")
                acord_val = acord_data.get(field_name)

                status = _three_way_status(policy_val, pl_val, acord_val, field_name=field_name)

                # Simple notes to indicate which values differ when mismatched
                if status == "MISMATCH":
                    notes_parts = []
                    if _smart_normalize_value(pl_val) != _smart_normalize_value(policy_val):
                        notes_parts.append("Certificate value differs from policy")
                    if _smart_normalize_value(acord_val) != _smart_normalize_value(policy_val):
                        notes_parts.append("ACORD value differs from policy")
                    if _smart_normalize_value(acord_val) != _smart_normalize_value(pl_val):
                        notes_parts.append("ACORD value differs from certificate")
                    notes = "; ".join(notes_parts) or "Values do not all match."
                elif status == "NOT_FOUND":
                    notes = "All three sources are missing or empty for this field."
                else:
                    notes = "Policy, certificate, and ACORD values all agree (after normalization)."

                core_field_results[field_name] = {
                    "policy_value": policy_val,
                    "certificate_value": pl_val,
                    "acord_value": acord_val,
                    "status": status,
                    "notes": notes,
                }

            acord_pl_comparisons = result.get("acord_pl_comparisons") or {}
            acord_pl_comparisons["core_fields"] = core_field_results

            # Build coverage-level PL vs Policy vs ACORD comparisons based on PL coverage validators
            coverage_comparisons = []

            # Prepare ACORD coverages map (normalized name -> (display_name, value))
            acord_coverages = (acord_data.get("coverages") or {}) if isinstance(acord_data, dict) else {}
            acord_norm_map = {}
            for cov_name, cov_val in acord_coverages.items():
                norm = _norm_name(cov_name)
                if norm:
                    acord_norm_map[norm] = (cov_name, cov_val)

            def _extract_coverage_from_item(item: dict) -> tuple:
                """Derive a generic coverage name and values from a PL coverage validation item."""
                # Try declarations-style fields
                for name_key, cert_key, policy_key in [
                    ("cert_building_name", "cert_building_value", "policy_building_value"),
                    ("cert_bpp_name", "cert_bpp_value", "policy_bpp_value"),
                    ("cert_bi_name", "cert_bi_value", "policy_bi_value"),
                    ("cert_eb_name", "cert_eb_value", "policy_eb_value"),
                    ("cert_os_name", "cert_os_value", "policy_os_value"),
                    ("cert_pc_name", "cert_pc_value", "policy_pc_value"),
                    ("cert_theft_name", "cert_theft_value", "policy_theft_value"),
                    ("cert_wind_hail_name", "cert_wind_hail_value", "policy_wind_hail_value"),
                    ("cert_ms_name", "cert_ms_value", "policy_ms_value"),
                    ("cert_ed_name", "cert_ed_value", "policy_ed_value"),
                ]:
                    cert_val = item.get(cert_key)
                    policy_val = item.get(policy_key) or item.get("policy_ms_split") if policy_key == "policy_ms_value" else item.get(policy_key)
                    if cert_val is not None or policy_val is not None:
                        name = item.get(name_key) or "Coverage"
                        return name, cert_val, policy_val

                # Fallback to generic fields if present
                if "coverage_name" in item or "cert_value" in item or "policy_value" in item:
                    name = item.get("coverage_name") or item.get("cert_building_name") or "Coverage"
                    return name, item.get("cert_value"), item.get("policy_value")

                return None, None, None

            pl_cov_validations = (
                result.get("pl_validations", {}).get("coverage_validations", {}) if result.get("pl_validations") else {}
            )

            for coverage_group, items in pl_cov_validations.items():
                if not isinstance(items, list):
                    continue
                for item in items:
                    if not isinstance(item, dict):
                        continue

                    cov_name, cert_val, policy_val = _extract_coverage_from_item(item)
                    if not cov_name:
                        continue

                    # Use flexible matching to find ACORD coverage
                    _, acord_val = _find_acord_coverage(cov_name, acord_norm_map)

                    status = _three_way_status(policy_val, cert_val, acord_val, field_name=cov_name)

                    if status == "MISMATCH":
                        notes_parts = []
                        if _smart_normalize_value(cert_val) != _smart_normalize_value(policy_val):
                            notes_parts.append("Certificate vs policy differ")
                        if _smart_normalize_value(acord_val) != _smart_normalize_value(policy_val):
                            notes_parts.append("ACORD vs policy differ")
                        if _smart_normalize_value(acord_val) != _smart_normalize_value(cert_val):
                            notes_parts.append("ACORD vs certificate differ")
                        notes = "; ".join(notes_parts) or "Values do not all match."
                    elif status == "NOT_FOUND":
                        notes = "All three sources are missing or empty for this coverage, or ACORD coverage not found."
                    else:
                        notes = "Policy, certificate, and ACORD values all agree (after normalization)."

                    coverage_comparisons.append(
                        {
                            "coverage_group": coverage_group,
                            "coverage_name": cov_name,
                            "policy_value": policy_val,
                            "certificate_value": cert_val,
                            "acord_value": acord_val,
                            "pl_policy_status": item.get("status"),
                            "status": status,
                            "notes": notes,
                            "validator_notes": item.get("notes"),  # Detailed validator notes from original validation
                            "evidence_declarations": item.get("evidence_declarations") or item.get("evidence_causes_of_loss"),
                            "evidence_endorsements": item.get("evidence_endorsements") or item.get("evidence_deductible_or_endorsement") or item.get("evidence_exclusions"),
                        }
                    )

            if coverage_comparisons:
                acord_pl_comparisons["coverages"] = coverage_comparisons

            result["acord_pl_comparisons"] = acord_pl_comparisons

        # ========== STEP 5B: Build GL vs Policy vs GL ACORD comparisons (core fields + coverages) ==========
        if (
            result.get("gl_certificate")
            and result.get("gl_acord_certificate")
            and result.get("gl_validations")
        ):
            print("[QC Unified PL+GL] Building GL vs Policy vs GL ACORD comparisons...")

            gl_cert_data = result.get("gl_certificate") or {}
            gl_acord_data = result.get("gl_acord_certificate") or {}
            gl_validations = result.get("gl_validations") or {}
            # Use PL core validations for policy core fields (since PL and GL share the same policy)
            pl_core_validation_raw = result.get("pl_core_validation_raw") or {}
            pl_core_validations = pl_core_validation_raw.get("validation_results", {}) if isinstance(pl_core_validation_raw, dict) else {}

            # Use the smart comparison function with LLM fallback
            def _three_way_status(policy_val, gl_val, gl_acord_val, field_name: str = None):
                """Return MATCH if all present values agree, MISMATCH if any conflict, NOT_FOUND if all missing."""
                return _three_way_status_with_llm_fallback(
                    policy_val, gl_val, gl_acord_val,
                    field_name=field_name,
                    use_llm_fallback=True
                )

            # Core fields comparison
            gl_core_field_results = {}
            
            # Map GL certificate fields to comparison fields
            core_field_mapping = {
                "policy_number": ("policy_number", "policy_number", "policy_number"),
                "effective_date": ("effective_date", "effective_date", "effective_date"),
                "expiration_date": ("expiration_date", "expiration_date", None),  # GL ACORD may not have expiration
                "insured_name": ("insured_name", "applicant_first_named_insured", "applicant_first_named_insured"),
            }
            
            for field_name, (gl_cert_key, gl_acord_key, policy_key) in core_field_mapping.items():
                gl_val = gl_cert_data.get(gl_cert_key) if gl_cert_key else None
                gl_acord_val = gl_acord_data.get(gl_acord_key) if gl_acord_key else None
                
                # Get policy value from GL validations or PL core validations
                # (PL and GL share the same policy, so PL core validations have policy core fields)
                policy_val = None
                if policy_key:
                    # First try PL core validations (they have policy core fields like dates, insured name)
                    if pl_core_validations and policy_key in pl_core_validations:
                        field_data = pl_core_validations[policy_key]
                        if isinstance(field_data, dict):
                            policy_val = field_data.get("policy_value")
                        # Handle case where field_data might be a string or other type
                        elif field_data is not None:
                            policy_val = field_data
                    
                    # If not found in PL core validations, try GL validations
                    if not policy_val:
                        # Check coverage_presence_validations for policy_number
                        if policy_key == "policy_number":
                            for cov_presence in gl_validations.get("coverage_presence_validations", []):
                                if isinstance(cov_presence, dict) and "policy_policy_number" in cov_presence:
                                    policy_val = cov_presence.get("policy_policy_number")
                                    break
                        # Note: GL validations don't extract effective_date, expiration_date, or insured_name
                        # These must come from PL core validations
                    
                    # Debug: log if policy_val is still None
                    if not policy_val and field_name in ["effective_date", "expiration_date", "insured_name"]:
                        print(f"      [WARNING] Policy value for {field_name} not found in pl_core_validations. Available keys: {list(pl_core_validations.keys()) if pl_core_validations else 'None'}")
                
                status = _three_way_status(policy_val, gl_val, gl_acord_val, field_name=field_name)
                
                if status == "MISMATCH":
                    notes_parts = []
                    if _smart_normalize_value(gl_val) != _smart_normalize_value(policy_val) and policy_val:
                        notes_parts.append("Certificate value differs from policy")
                    if _smart_normalize_value(gl_acord_val) != _smart_normalize_value(policy_val) and policy_val:
                        notes_parts.append("GL ACORD value differs from policy")
                    if _smart_normalize_value(gl_acord_val) != _smart_normalize_value(gl_val):
                        notes_parts.append("GL ACORD value differs from certificate")
                    notes = "; ".join(notes_parts) or "Values do not all match."
                elif status == "NOT_FOUND":
                    notes = "All three sources are missing or empty for this field."
                else:
                    notes = "Policy, certificate, and GL ACORD values all agree (after normalization)."

                gl_core_field_results[field_name] = {
                    "policy_value": policy_val,
                    "certificate_value": gl_val,
                    "gl_acord_value": gl_acord_val,
                    "status": status,
                    "notes": notes,
                }

            # Coverage limits comparison
            gl_coverage_comparisons = []
            
            # Extract GL certificate CGL limits
            gl_cgl_limits = {}
            if gl_cert_data.get("coverages", {}).get("commercial_general_liability", {}).get("limits"):
                gl_cgl_limits = gl_cert_data["coverages"]["commercial_general_liability"]["limits"]
            
            # Extract GL ACORD limits
            gl_acord_limits_map = {
                "general_aggregate": gl_acord_data.get("general_aggregate"),
                "each_occurrence": gl_acord_data.get("each_occurrence"),
                "personal_advertising_injury": gl_acord_data.get("personal_advertising_injury"),
                "damage_to_rented_premises": gl_acord_data.get("damage_to_rented_premises"),
                "medical_expense": gl_acord_data.get("medical_expense"),
                "products_completed_operations_aggregate": gl_acord_data.get("products_completed_operations_aggregate"),
            }
            
            # Map GL cert limit keys to standard names
            gl_cert_limit_map = {
                "general_aggregate": gl_cgl_limits.get("general_aggregate"),
                "each_occurrence": gl_cgl_limits.get("each_occurrence"),
                "personal_advertising_injury": gl_cgl_limits.get("personal_adv_injury"),
                "damage_to_rented_premises": gl_cgl_limits.get("damage_to_rented_premises"),
                "medical_expense": gl_cgl_limits.get("med_exp"),
                "products_completed_operations_aggregate": gl_cgl_limits.get("products_comp_op_agg"),
            }
            
            # Get policy values from GL validations
            gl_cov_validations = gl_validations.get("coverage_validations", {}).get("cgl_limit_validations", [])
            
            for limit_name in gl_acord_limits_map.keys():
                gl_cert_val = gl_cert_limit_map.get(limit_name)
                gl_acord_val = gl_acord_limits_map.get(limit_name)
                
                # Find policy value from GL validations
                # GL validations use cert_limit_key and policy_value fields
                policy_val = None
                # Map our limit_name to cert_limit_key values used in GL validations
                limit_key_map = {
                    "general_aggregate": "general_aggregate",
                    "each_occurrence": "each_occurrence",
                    "personal_advertising_injury": "personal_adv_injury",
                    "damage_to_rented_premises": "damage_to_rented_premises",
                    "medical_expense": "med_exp",
                    "products_completed_operations_aggregate": "products_comp_op_agg",
                }
                expected_cert_limit_key = limit_key_map.get(limit_name)
                
                matched_cov_val = None
                if expected_cert_limit_key:
                    for cov_val in gl_cov_validations:
                        if isinstance(cov_val, dict):
                            # Match by cert_limit_key, then get policy_value
                            cert_limit_key = cov_val.get("cert_limit_key")
                            if cert_limit_key == expected_cert_limit_key:
                                policy_val = cov_val.get("policy_value")
                                matched_cov_val = cov_val  # Store the matched validator item
                                break  # Found it, stop searching
                
                status = _three_way_status(policy_val, gl_cert_val, gl_acord_val, field_name=limit_name)
                
                if status == "MISMATCH":
                    notes_parts = []
                    if _smart_normalize_value(gl_cert_val) != _smart_normalize_value(policy_val) and policy_val:
                        notes_parts.append("Certificate vs policy differ")
                    if _smart_normalize_value(gl_acord_val) != _smart_normalize_value(policy_val) and policy_val:
                        notes_parts.append("GL ACORD vs policy differ")
                    if _smart_normalize_value(gl_acord_val) != _smart_normalize_value(gl_cert_val):
                        notes_parts.append("GL ACORD vs certificate differ")
                    notes = "; ".join(notes_parts) or "Values do not all match."
                elif status == "NOT_FOUND":
                    notes = "All three sources are missing or empty for this coverage limit."
                else:
                    notes = "Policy, certificate, and GL ACORD values all agree (after normalization)."

                gl_coverage_comparisons.append({
                    "coverage_name": limit_name.replace("_", " ").title(),
                    "policy_value": policy_val,
                    "certificate_value": gl_cert_val,
                    "gl_acord_value": gl_acord_val,
                    "status": status,
                    "notes": notes,
                    "validator_notes": matched_cov_val.get("notes") if matched_cov_val else None,  # Detailed validator notes
                    "evidence": matched_cov_val.get("evidence") if matched_cov_val else None,  # Evidence from validator
                })

            # Store GL ACORD comparisons
            gl_acord_comparisons = {
                "core_fields": gl_core_field_results,
                "coverages": gl_coverage_comparisons,
            }
            result["gl_acord_comparisons"] = gl_acord_comparisons

        # ========== STEP 6: Store pl_core_validation_raw for GL ACORD comparisons ==========
        # Store pl_core_validation_raw so it's available for GL ACORD comparisons
        if pl_core_validation_raw:
            result["pl_core_validation_raw"] = pl_core_validation_raw
        
        # ========== STEP 7: Merge Summary ==========
        result["summary"] = {
            "pl": result["pl_validations"]["summary"] if result["pl_validations"] else None,
            "gl": result["gl_validations"]["summary"] if result["gl_validations"] else None,
        }
        
        # Save debug files if upload_id provided
        if upload_id:
            debug_dir = repo_root / "qc-new" / "debug" / upload_id
            debug_dir.mkdir(parents=True, exist_ok=True)
            
            # Save policy combo text
            local_policy_combo = debug_dir / "policy_combo.txt"
            shutil.copy2(policy_combo_path, local_policy_combo)
            print(f"[QC Unified PL+GL] 💾 Saved policy text to: {local_policy_combo}")
            
            # Save certificates if processed
            if pl_cert_pdf_path:
                local_pl_cert_json = debug_dir / "pl_cert.json"
                shutil.copy2(pl_cert_json_path, local_pl_cert_json)
                print(f"[QC Unified PL+GL] 💾 Saved PL certificate JSON to: {local_pl_cert_json}")
            
            if gl_cert_pdf_path:
                local_gl_cert_json = debug_dir / "gl_cert.json"
                shutil.copy2(gl_cert_json_path, local_gl_cert_json)
                print(f"[QC Unified PL+GL] 💾 Saved GL certificate JSON to: {local_gl_cert_json}")
        
        print("[QC Unified PL+GL] ✓ Validation complete")
        return result
        
    finally:
        # Cleanup temp files
        try:
            shutil.rmtree(temp_dir)
        except Exception:
            pass


if __name__ == "__main__":
    import sys
    if len(sys.argv) < 3:
        print("Usage: python qc_new_unified_pipeline.py <cert_pdf> <policy_pdf> [coverage_type]")
        sys.exit(1)
    
    cert_pdf = sys.argv[1]
    policy_pdf = sys.argv[2]
    coverage = sys.argv[3] if len(sys.argv) > 3 else "property"
    
    result = run_unified_qc(cert_pdf, policy_pdf, coverage)
    print(json.dumps(result, indent=2))

