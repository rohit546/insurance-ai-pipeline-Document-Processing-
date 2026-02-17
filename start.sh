#!/bin/bash
set -e

# Create credentials directory first
mkdir -p /app/credentials

# Decode GCP credentials from base64 environment variable
if [ -n "$GCP_CREDENTIALS_BASE64" ]; then
    echo "Decoding GCP credentials from environment variable..."
    echo "$GCP_CREDENTIALS_BASE64" | base64 -d > /app/credentials/gcp-credentials.json
    export GOOGLE_APPLICATION_CREDENTIALS=/app/credentials/gcp-credentials.json
    echo "GCP credentials written to $GOOGLE_APPLICATION_CREDENTIALS"
elif [ -n "$GOOGLE_APPLICATION_CREDENTIALS" ]; then
    echo "Using existing GOOGLE_APPLICATION_CREDENTIALS: $GOOGLE_APPLICATION_CREDENTIALS"
else
    echo "ERROR: No GCP credentials found! Set GCP_CREDENTIALS_BASE64 in Railway environment variables."
    exit 1
fi

# Verify the credentials file exists
if [ ! -f "$GOOGLE_APPLICATION_CREDENTIALS" ]; then
    echo "ERROR: Credentials file not found at $GOOGLE_APPLICATION_CREDENTIALS"
    exit 1
fi

echo "Credentials file verified at $GOOGLE_APPLICATION_CREDENTIALS"

# Start the application
# Use PORT env var (Railway injects this) or default to 8000
echo "Starting uvicorn on port ${PORT:-8000}..."
exec uvicorn app:app --host 0.0.0.0 --port ${PORT:-8000}
