FROM python:3.11-slim

# Install system dependencies for PyMuPDF and Tesseract OCR
RUN apt-get update && apt-get install -y \
    libgl1 \
    libglib2.0-0 \
    tesseract-ocr \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy requirements first (for better caching)
COPY backend/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy backend code to root of /app (not /app/backend/)
COPY backend/ .

# Copy qc-new directory for QC system (standalone feature)
COPY qc-new/ /app/qc-new/

# Copy startup script and make it executable
COPY backend/start.sh /app/start.sh
RUN chmod +x /app/start.sh

# Create credentials directory (credentials file will be created from env var at runtime)
RUN mkdir -p ./credentials

# Environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

# Railway uses PORT env var (set dynamically)
EXPOSE 8000

# Use startup script (handles credentials and starts app)
CMD ["/app/start.sh"]