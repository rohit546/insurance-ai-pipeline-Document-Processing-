#!/usr/bin/env python3
"""
Helper script to encode GCP credentials JSON to base64 for Railway deployment.
Usage: python encode_credentials.py path/to/credentials.json
"""
import sys
import base64
import json
import os

def encode_credentials(json_file_path):
    """Encode GCP credentials JSON file to base64."""
    try:
        # Read JSON file
        if not os.path.exists(json_file_path):
            print(f"❌ Error: File not found: {json_file_path}")
            return None
        
        with open(json_file_path, 'r', encoding='utf-8') as f:
            json_content = f.read()
        
        # Validate JSON
        try:
            json.loads(json_content)
        except json.JSONDecodeError as e:
            print(f"❌ Error: Invalid JSON file: {e}")
            return None
        
        # Encode to base64
        json_bytes = json_content.encode('utf-8')
        base64_encoded = base64.b64encode(json_bytes).decode('utf-8')
        
        # Display results
        print("=" * 80)
        print("✅ GCP Credentials Encoded Successfully!")
        print("=" * 80)
        print(f"\n📁 Source file: {json_file_path}")
        print(f"📏 Base64 length: {len(base64_encoded)} characters")
        print("\n" + "=" * 80)
        print("📋 Copy this entire string to Railway variable GCP_CREDENTIALS_BASE64:")
        print("=" * 80)
        print(base64_encoded)
        print("=" * 80)
        
        # Save to file
        output_file = json_file_path.replace('.json', '_base64.txt')
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(base64_encoded)
        print(f"\n💾 Also saved to: {output_file}")
        
        # Extract bucket name hint if available
        try:
            creds = json.loads(json_content)
            project_id = creds.get('project_id', 'N/A')
            print(f"\n💡 Project ID: {project_id}")
            print(f"💡 Make sure BUCKET_NAME matches your GCS bucket name!")
        except:
            pass
        
        return base64_encoded
        
    except Exception as e:
        print(f"❌ Error encoding credentials: {e}")
        return None

def main():
    if len(sys.argv) < 2:
        print("=" * 80)
        print("GCP Credentials Base64 Encoder")
        print("=" * 80)
        print("\nUsage:")
        print(f"  python {sys.argv[0]} <path-to-credentials.json>")
        print("\nExample:")
        print(f"  python {sys.argv[0]} railway-gcp-credentials.json")
        print("\nThis will:")
        print("  1. Read your GCP service account JSON file")
        print("  2. Encode it to base64")
        print("  3. Display the encoded string (copy to Railway)")
        print("  4. Save to a .txt file")
        sys.exit(1)
    
    json_file = sys.argv[1]
    encode_credentials(json_file)

if __name__ == "__main__":
    main()
