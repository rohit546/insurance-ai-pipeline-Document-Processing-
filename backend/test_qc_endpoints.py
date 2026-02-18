"""
Test QC Endpoints
Tests /upload-qc and /qc-results endpoints
"""
import requests
import json
import time
import sys
from pathlib import Path

# Backend URL
BASE_URL = "http://localhost:8000"

def test_health():
    """Check if backend is running"""
    print("🔍 Checking backend health...\n")
    try:
        response = requests.get(f"{BASE_URL}/health", timeout=5)
        print(f"Status: {response.status_code}")
        print(f"Response: {response.json()}\n")
        return response.status_code == 200
    except requests.exceptions.ConnectionError:
        print("❌ Backend is not running. Please start it first:\n")
        print("   cd backend")
        print("   python -m uvicorn app:app --reload\n")
        return False

def test_qc_upload():
    """Test /upload-qc endpoint"""
    print("📤 Testing /upload-qc endpoint...\n")
    
    # Use divine_cna_Package.pdf for testing
    policy_pdf_path = Path("../policy/divine_cna_Package.pdf")
    
    if not policy_pdf_path.exists():
        print(f"❌ Test file not found: {policy_pdf_path}")
        print(f"   Absolute path: {policy_pdf_path.absolute()}\n")
        return None
    
    print(f"📄 Using test file: {policy_pdf_path}")
    print(f"   File size: {policy_pdf_path.stat().st_size} bytes\n")
    
    try:
        with open(policy_pdf_path, 'rb') as f:
            files = {'policy_pdf': f}
            data = {'username': 'test_user'}
            
            print("Sending request...")
            response = requests.post(f"{BASE_URL}/upload-qc/", files=files, data=data, timeout=30)
        
        print(f"Status: {response.status_code}")
        result = response.json()
        print(f"Response:\n{json.dumps(result, indent=2)}\n")
        
        if response.status_code == 200 and result.get('success'):
            return result.get('upload_id')
        else:
            print(f"❌ Upload failed: {result.get('message', 'Unknown error')}\n")
            return None
    
    except requests.exceptions.ConnectionError:
        print("❌ Cannot connect to backend\n")
        return None
    except Exception as e:
        print(f"❌ Error: {e}\n")
        import traceback
        traceback.print_exc()
        return None

def test_qc_results(upload_id):
    """Test /qc-results endpoint"""
    print(f"📥 Testing /qc-results/{upload_id} endpoint...\n")
    
    try:
        response = requests.get(f"{BASE_URL}/qc-results/{upload_id}", timeout=30)
        
        print(f"Status: {response.status_code}")
        result = response.json()
        print(f"Response:\n{json.dumps(result, indent=2)}\n")
        
        return response.status_code == 200
    
    except requests.exceptions.ConnectionError:
        print("❌ Cannot connect to backend\n")
        return False
    except Exception as e:
        print(f"❌ Error: {e}\n")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("=" * 70)
    print("QC ENDPOINTS TEST")
    print("=" * 70 + "\n")
    
    # Step 1: Check health
    if not test_health():
        sys.exit(1)
    
    # Step 2: Upload QC
    upload_id = test_qc_upload()
    
    if not upload_id:
        print("❌ QC upload failed. Aborting.\n")
        sys.exit(1)
    
    print("=" * 70)
    print("⏳ Waiting for processing to start (5 seconds)...\n")
    time.sleep(5)
    
    # Step 3: Get results
    print("=" * 70)
    success = test_qc_results(upload_id)
    
    if success:
        print("=" * 70)
        print("✅ All tests passed!")
        print("=" * 70)
    else:
        print("=" * 70)
        print("⚠️  Results check incomplete (still processing or error)")
        print("=" * 70)

