#!/bin/bash
# Startup script for Railway deployment
# Writes credentials file from environment variable if provided

# Create credentials directory if it doesn't exist
mkdir -p /app/credentials

# Write credentials from environment variable (Railway Secret)
# Support both base64-encoded and raw JSON formats
if [ -n "$GCP_CREDENTIALS_BASE64" ]; then
  echo "$GCP_CREDENTIALS_BASE64" | base64 -d > /app/credentials/gcp-credentials.json
  export GOOGLE_APPLICATION_CREDENTIALS=/app/credentials/gcp-credentials.json
  export GOOGLE_SHEETS_CREDENTIALS=/app/credentials/gcp-credentials.json
  echo "[OK] Credentials file created from GCP_CREDENTIALS_BASE64"
  echo "[OK] GOOGLE_APPLICATION_CREDENTIALS set to: $GOOGLE_APPLICATION_CREDENTIALS"
elif [ -n "$GOOGLE_CREDENTIALS_JSON" ]; then
  echo "$GOOGLE_CREDENTIALS_JSON" > /app/credentials/gcp-credentials.json
  export GOOGLE_APPLICATION_CREDENTIALS=/app/credentials/gcp-credentials.json
  export GOOGLE_SHEETS_CREDENTIALS=/app/credentials/gcp-credentials.json
  echo "[OK] Credentials file created from GOOGLE_CREDENTIALS_JSON"
  echo "[OK] GOOGLE_APPLICATION_CREDENTIALS set to: $GOOGLE_APPLICATION_CREDENTIALS"
else
  echo "[WARN] No GCP credentials environment variable set (GCP_CREDENTIALS_BASE64 or GOOGLE_CREDENTIALS_JSON)"
fi

# Start Celery workers with queue-based routing
# --without-mingle disables neighbor discovery (prevents Redis timeout during startup)
# Worker 1: QC queue (max 1 concurrent QC task)
echo "[OK] Starting Celery QC worker (queue: qc, concurrency: 1)..."
celery -A tasks worker --loglevel=info --concurrency=1 --queues=qc --without-mingle --without-gossip -n qc_worker@%h &
QC_WORKER_PID=$!

# Worker 2: Summary queue (max 1 concurrent Summary task)
echo "[OK] Starting Celery Summary worker (queue: summary, concurrency: 1)..."
celery -A tasks worker --loglevel=info --concurrency=1 --queues=summary --without-mingle --without-gossip -n summary_worker@%h &
SUMMARY_WORKER_PID=$!

echo "[OK] Queue-based worker architecture initialized:"
echo "  - QC Worker: queue=qc, concurrency=1"
echo "  - Summary Worker: queue=summary, concurrency=1"
echo "  - Total workers: 2"

# Function to cleanup on exit
cleanup() {
    echo "[INFO] Shutting down..."
    kill $QC_WORKER_PID 2>/dev/null
    kill $SUMMARY_WORKER_PID 2>/dev/null
    exit 0
}
trap cleanup SIGTERM SIGINT

# Start the FastAPI application (foreground - this keeps the container alive)
echo "[OK] Starting FastAPI application..."
exec uvicorn app:app --host 0.0.0.0 --port ${PORT:-8000}

