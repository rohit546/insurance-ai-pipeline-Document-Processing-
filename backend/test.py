from dotenv import load_dotenv
import os
from google.cloud import storage

load_dotenv()

# Set Google credentials from .env file
google_cloud_credentials = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
if google_cloud_credentials:
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = google_cloud_credentials

def upload_pdf_to_cloud_storage():
    # Initialize the storage client
    client = storage.Client()
    
    # You'll need to specify your bucket name
    bucket_name = os.getenv('BUCKET_NAME', 'mckinneysuite')  # Replace with your bucket name
    bucket = client.bucket(bucket_name)
    
    # Local file path
    local_file_path = '../docs/joneal.pdf'
    blob_name = 'Credentials/joneal.pdf'  # Upload to Credentials folder in the bucket
    
    # Create a blob and upload the file
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(local_file_path)
    
    print(f'File {local_file_path} uploaded to {bucket_name}/{blob_name}')
    return f'gs://{bucket_name}/{blob_name}'

if __name__ == "__main__":
    upload_pdf_to_cloud_storage()
