"""
Simple database storage
"""
import json
import os
from datetime import datetime
from google.cloud import storage
from dotenv import load_dotenv

load_dotenv()

google_cloud_credentials = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
if google_cloud_credentials:
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = google_cloud_credentials

client = storage.Client()
bucket = client.get_bucket(os.getenv('BUCKET_NAME', 'mckinneysuite'))

USERS_FILE = 'metadata/users.json'

def get_all_users():
    try:
        blob = bucket.blob(USERS_FILE)
        if not blob.exists():
            return {}
        content = blob.download_as_string().decode('utf-8')
        return json.loads(content)
    except:
        return {}

def save_users(users_dict):
    blob = bucket.blob(USERS_FILE)
    blob.upload_from_string(
        json.dumps(users_dict, indent=2),
        content_type='application/json'
    )

def get_user(user_id: str):
    users = get_all_users()
    return users.get(user_id)

def user_exists_by_email(email: str):
    users = get_all_users()
    for user_id, user_data in users.items():
        if user_data.get('email') == email:
            return True, user_id
    return False, None

def create_user(email: str, password: str):
    users = get_all_users()
    user_id = f"user_{len(users) + 1}"
    
    users[user_id] = {
        "email": email,
        "password": password,
        "created_at": datetime.now().isoformat()
    }
    
    save_users(users)
    return user_id