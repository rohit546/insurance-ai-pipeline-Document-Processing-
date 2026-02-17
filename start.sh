#!/bin/bash

# Decode GCP credentials from base64 environment variable
if [ -n "$GCP_CREDENTIALS_BASE64" ]; then
    echo "Decoding GCP credentials from environment variable..."
    echo "$GCP_CREDENTIALS_BASE64" | base64 -d > /app/credentials/gcp-credentials.json
    export GOOGLE_APPLICATION_CREDENTIALS=/app/credentials/gcp-credentials.json
    echo "GCP credentials written to $GOOGLE_APPLICATION_CREDENTIALS"
elif [ -n "$GOOGLE_APPLICATION_CREDENTIALS" ]; then
    echo "Using existing GOOGLE_APPLICATION_CREDENTIALS: $GOOGLE_APPLICATION_CREDENTIALS"
else
    echo "WARNING: No GCP credentials found! Set GCP_CREDENTIALS_BASE64 or GOOGLE_APPLICATION_CREDENTIALS"
fi

# Create credentials directory if it doesn't exist
mkdir -p /app/credentials

# Re-decode after mkdir (in case it was created after the first decode)
if [ -n "$GCP_CREDENTIALS_BASE64" ]; then
    echo "$GCP_CREDENTIALS_BASE64" | base64 -d > /app/credentials/gcp-credentials.json
    export GOOGLE_APPLICATION_CREDENTIALS=/app/credentials/gcp-credentials.json
fi

# Start the application
# Use PORT env var (Railway injects this) or default to 8000
exec uvicorn app:app --host 0.0.0.0 --port ${PORT:-8000}
