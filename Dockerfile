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

# Copy startup script
COPY start.sh .
RUN chmod +x start.sh

# Create credentials directory
RUN mkdir -p ./credentials

# Environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

EXPOSE 8000

# Start via startup script (handles credential decoding + server start)
CMD ["./start.sh"]