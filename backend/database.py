"""
Simple database storage
Uses username for authentication, with email backward compatibility
"""
import json
import os
from datetime import datetime
from google.cloud import storage
from dotenv import load_dotenv

load_dotenv()

# Set Google credentials - check multiple possible locations
google_cloud_credentials = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')

# If not set, check common locations
if not google_cloud_credentials:
    # Check Railway deployment path
    railway_creds = '/app/credentials/gcp-credentials.json'
    if os.path.exists(railway_creds):
        google_cloud_credentials = railway_creds
    else:
        # Check the old credentials filename
        railway_creds_old = '/app/credentials/insurance-sheets-474717-7fc3fd9736bc.json'
        if os.path.exists(railway_creds_old):
            google_cloud_credentials = railway_creds_old

# Set the environment variable if we found credentials
if google_cloud_credentials:
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = google_cloud_credentials
    print(f"[OK] Using Google credentials from: {google_cloud_credentials}")
else:
    print("[WARN] GOOGLE_APPLICATION_CREDENTIALS not found")

# Initialize storage client - make it resilient to avoid import failures
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

# Try to initialize at import time, but don't fail
try:
    if google_cloud_credentials and os.path.exists(google_cloud_credentials):
        _client = storage.Client()
        _bucket = _client.get_bucket(bucket_name)
        print("[OK] Storage client initialized in database.py")
    else:
        print("[WARN] Storage client will be initialized lazily in database.py")
except Exception as e:
    print(f"[WARN] Could not initialize storage client in database.py: {e}")
    _client = None
    _bucket = None

# For backward compatibility
client = _client
bucket = _bucket

USERS_FILE = 'metadata/users.json'

def get_all_users():
    try:
        b = get_bucket()  # Use lazy initialization
        blob = b.blob(USERS_FILE)
        if not blob.exists():
            return {}
        content = blob.download_as_string().decode('utf-8')
        return json.loads(content)
    except:
        return {}

def save_users(users_dict):
    b = get_bucket()  # Use lazy initialization
    blob = b.blob(USERS_FILE)
    blob.upload_from_string(
        json.dumps(users_dict, indent=2),
        content_type='application/json'
    )

def get_user(user_id: str):
    users = get_all_users()
    return users.get(user_id)

def user_exists_by_username(username: str):
    """Check if a user exists by username"""
    users = get_all_users()
    for user_data in users.values():
        if user_data.get('username') == username:
            return True, username
    # Also check by email for backward compatibility
    for user_id, user_data in users.items():
        if user_data.get('email') == username:
            return True, user_id
    return False, None

def user_exists_by_email(email: str):
    """Check if a user exists by email (backward compatibility)"""
    users = get_all_users()
    for user_id, user_data in users.items():
        if user_data.get('email') == email:
            return True, user_id
    return False, None

def create_user(username: str, password: str):
    users = get_all_users()

    # Use username as the key
    users[username] = {
        "username": username,
        "password": password,
        "created_at": datetime.now().isoformat()
    }

    save_users(users)
    return username