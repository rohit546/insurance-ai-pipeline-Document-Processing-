# End-to-End Project Analysis: Insurance PDF Processing System

## Executive Summary

This is a **full-stack insurance document processing system** that automates the extraction and analysis of insurance quotes from multiple carriers. The system processes PDF documents through a multi-phase pipeline, extracts structured data using OCR and LLM technologies, and outputs results to Google Sheets.

**Project Name:** Mckinney and Co Insurance PDF Analysis System  
**Architecture:** Microservices (FastAPI + Next.js + Celery + Redis)  
**Deployment:** Docker Compose (local) / AWS EC2 (production)  
**Storage:** Google Cloud Storage  
**Processing:** Multi-phase pipeline with OCR (NanoNets) and LLM (OpenAI GPT)

---

## 1. Architecture Overview

### 1.1 System Components

```
┌─────────────────────────────────────────────────────────────┐
│                    FRONTEND (Next.js)                        │
│  - React 19.2.0 + TypeScript                                │
│  - Tailwind CSS 4                                           │
│  - Client-side routing & authentication                     │
└──────────────────────┬──────────────────────────────────────┘
                       │ HTTP/REST API
┌──────────────────────▼──────────────────────────────────────┐
│              BACKEND (FastAPI)                              │
│  - Python 3.11                                              │
│  - FastAPI 0.104.1                                          │
│  - RESTful API endpoints                                     │
│  - Authentication (simple password-based)                    │
└──────────────────────┬──────────────────────────────────────┘
                       │
        ┌──────────────┼──────────────┐
        │              │              │
┌───────▼──────┐ ┌─────▼──────┐ ┌────▼─────────────┐
│   CELERY     │ │   REDIS    │ │  GOOGLE CLOUD    │
│   WORKER     │ │  (Broker)  │ │    STORAGE       │
│              │ │            │ │                  │
│ - OCR Tasks  │ │ - Queue    │ │ - PDF Storage    │
│ - Background │ │ - Results  │ │ - Metadata       │
│   Processing │ │            │ │ - Results        │
└──────────────┘ └────────────┘ └──────────────────┘
```

### 1.2 Technology Stack

**Backend:**
- **FastAPI** - REST API framework
- **Celery** - Distributed task queue
- **Redis** - Message broker and result backend
- **PyMuPDF (fitz)** - PDF text extraction
- **NanoNets OCR** - Optical Character Recognition
- **OpenAI GPT** - LLM-based field extraction
- **Google Cloud Storage** - File storage
- **Google Sheets API** - Data export
- **JWT** - Authentication (imported but not fully implemented)

**Frontend:**
- **Next.js 16.0.1** - React framework
- **React 19.2.0** - UI library
- **TypeScript** - Type safety
- **Tailwind CSS 4** - Styling

**Infrastructure:**
- **Docker** - Containerization
- **Docker Compose** - Multi-container orchestration
- **AWS EC2** - Production deployment target

---

## 2. Data Flow & Processing Pipeline

### 2.1 Complete User Journey

```
1. USER REGISTRATION/LOGIN
   ↓
2. UPLOAD MULTIPLE CARRIER PDFs
   - Property PDFs (optional)
   - Liability PDFs (optional)
   - Liquor PDFs (optional)
   ↓
3. PHASE 1: PDF Quality Analysis
   - Count total pages
   - Extract text using PyMuPDF (minimal, deprecated)
   - Classify pages (CLEAN/PROBLEM/BORDERLINE)
   ↓
4. PHASE 2: OCR Extraction (Background Task)
   - Convert PDF pages to images
   - Run NanoNets OCR on all pages
   - Extract text with confidence scores
   ↓
5. PHASE 2C: Smart Selection
   - Rule-based selection: Always prefer OCR over PyMuPDF
   - Fallback to PyMuPDF if OCR unavailable
   ↓
6. PHASE 2D: Intelligent Combination
   - Combine best text from each page
   - Create unified document for LLM processing
   ↓
7. PHASE 3: LLM Field Extraction
   - Split combined document into chunks (4 pages each)
   - Extract 34 insurance fields using GPT
   - Merge chunk results into final JSON
   ↓
8. PHASE 5: Google Sheets Export
   - Push extracted fields to Google Sheets
   - Side-by-side comparison format for multiple carriers
```

### 2.2 Phase-by-Phase Breakdown

#### **Phase 1: PDF Quality Analysis** (`phase1.py`)

**Purpose:** Initial PDF assessment and page counting

**Key Functions:**
- `process_upload_lengths()` - Calculate PDF byte sizes
- `process_upload_quality_analysis()` - Analyze PDF quality (deprecated)

**Output:**
- Total page count per PDF
- File size information
- Quality metrics (minimal, as PyMuPDF processing is disabled)

**Auto-triggers:** Phase 2 OCR (via Celery task)

**Files:**
- `backend/phase1.py`
- `backend/phase1_pymudf.py` (helper functions)

---

#### **Phase 2: OCR Extraction** (`phase2_ocr_nano.py`)

**Purpose:** Extract text from all PDF pages using OCR

**Technology:** NanoNets OCR API (`docstrange` library)

**Key Functions:**
- `extract_with_nanonets_ocr()` - Extract text from single page
- `process_upload_ocr_analysis()` - Process all PDFs for an upload

**Process:**
1. Download PDF from GCS
2. Convert each page to image (2.0x zoom)
3. Call NanoNets OCR API
4. Analyze OCR quality (confidence scoring)
5. Save results to GCS

**Output:**
- OCR text per page
- Confidence scores
- Quality metrics

**Storage:** `results/{uploadId}/{carrier}/{type}/ocr_all_pages.txt`

**Auto-triggers:** Phase 2C Smart Selection

**Files:**
- `backend/phase2_ocr_nano.py`
- `backend/tasks.py` (Celery task wrapper)

---

#### **Phase 2C: Smart Selection** (`phase2c_smart_selection.py`)

**Purpose:** Select best text source (OCR vs PyMuPDF) for each page

**Strategy:** Rule-based (always prefer OCR, fallback to PyMuPDF)

**Key Functions:**
- `process_upload_smart_selection_analysis()` - Main processing function
- `create_selection_prompt()` - LLM prompt creation (not used in rule-based mode)

**Process:**
1. Read OCR results from Phase 2
2. Read PyMuPDF results from Phase 1 (if available)
3. For each page: Select OCR if available, else PyMuPDF
4. Save selection decisions to JSON

**Output:**
- JSON mapping: `{page_num: selected_source}`

**Storage:** `results/{uploadId}/{carrier}/{type}/smart_selection.json`

**Auto-triggers:** Phase 2D Intelligent Combination

**Files:**
- `backend/phase2c_smart_selection.py`

---

#### **Phase 2D: Intelligent Combination** (`phase2d_intelligent_combination.py`)

**Purpose:** Create unified document from selected text sources

**Key Functions:**
- `process_upload_intelligent_combination()` - Combine pages into single file
- `read_smart_selection_results_from_gcs()` - Load selection decisions

**Process:**
1. Read smart selection results
2. For each page, extract text from selected source (OCR or PyMuPDF)
3. Combine all pages into formatted text file
4. Include metadata (page number, source, text content)

**Output:**
- Combined text file with all pages
- Format: `PAGE X (SOURCE): TEXT CONTENT`

**Storage:** `results/{uploadId}/{carrier}/{type}/intelligent_combined.txt`

**Auto-triggers:** Phase 3 LLM Extraction

**Files:**
- `backend/phase2d_intelligent_combination.py`

---

#### **Phase 3: LLM Field Extraction** (`phase3_llm.py`)

**Purpose:** Extract structured insurance data using GPT

**Technology:** OpenAI GPT API

**Key Functions:**
- `process_upload_llm_extraction()` - Main extraction function
- `create_chunks()` - Split pages into 4-page chunks
- `extract_fields_from_chunk()` - Extract fields from chunk using GPT

**Extracted Fields (34 total):**
1. Property Coverage Type
2. Building Coverage Limit
3. Personal Property Coverage Limit
4. Business Income Coverage Limit
5. Deductible Amount
6. Coinsurance Percentage
7. ... (29 more fields)

**Process:**
1. Read intelligent combined file
2. Split into chunks (4 pages each)
3. For each chunk:
   - Create GPT prompt with field definitions
   - Call GPT API
   - Parse JSON response
4. Merge chunk results
5. Handle duplicates and conflicts
6. Save final JSON

**Output:**
- JSON with 34 extracted fields per carrier/file type

**Storage:** `results/{uploadId}/{carrier}/{type}/extracted_fields.json`

**Files:**
- `backend/phase3_llm.py`
- `backend/phase3_gl.py` (General Liability variant)
- `backend/phase3_liqour.py` (Liquor Liability variant)

---

#### **Phase 5: Google Sheets Export** (`phase5_googlesheet.py`)

**Purpose:** Export extracted data to Google Sheets

**Key Functions:**
- `finalize_upload_to_sheets()` - Push all carriers to sheets
- `push_to_sheets_from_gcs()` - Push single carrier data

**Process:**
1. Load extracted fields JSON for all carriers
2. Create/Open Google Sheet
3. Format data in side-by-side columns (one per carrier)
4. Write headers and data
5. Format cells (bold headers, number formatting)

**Output:**
- Google Sheet with extracted fields
- Side-by-side comparison format

**Files:**
- `backend/phase5_googlesheet.py`

---

## 3. Backend Architecture

### 3.1 API Endpoints (`app.py`)

**Authentication:**
- `POST /register/` - User registration
- `POST /login/` - User login

**Upload:**
- `POST /upload-quotes/` - Upload multiple carrier PDFs
- `GET /upload-history/` - Get upload history
- `POST /confirm-upload/` - Confirm upload execution

**Processing:**
- `GET /phase1/process` - Process PDF lengths
- `GET /phase1/quality-analysis` - Analyze PDF quality
- `GET /phase2/ocr-analysis` - Run OCR analysis
- `GET /phase2d/intelligent-combination` - Combine text sources
- `GET /phase3/llm-extraction` - Extract fields using LLM
- `GET /phase5/googlesheets-push` - Push to Google Sheets (deprecated)
- `GET /finalize-upload` - Finalize and push all carriers

**Health:**
- `GET /` - Root endpoint
- `GET /health` - Health check

### 3.2 Database Layer (`database.py`)

**Storage:** Google Cloud Storage (JSON files)

**Data Structures:**
- `metadata/users.json` - User accounts
- `pdf/uploads_metadata.json` - Upload records

**Functions:**
- `get_all_users()` - Load all users
- `create_user()` - Create new user
- `user_exists_by_email()` - Check email existence
- `get_user()` - Get user by ID

**Note:** No password hashing - passwords stored in plain text (security issue)

### 3.3 Authentication (`auth.py`)

**Implementation:** Simple password-based (no JWT, no hashing)

**Functions:**
- `register()` - Register new user
- `login()` - Authenticate user

**Security Issues:**
- Passwords stored in plain text
- No JWT tokens (session management unclear)
- No password strength requirements

### 3.4 Upload Handler (`upload_handler.py`)

**Purpose:** Handle multi-carrier PDF uploads

**Key Functions:**
- `process_carrier_uploads()` - Process and upload PDFs
- `get_upload_history()` - Retrieve upload history
- `upload_pdf_to_gcs()` - Upload PDF to Google Cloud Storage

**Process:**
1. Receive carrier data with PDFs
2. Generate unique filenames
3. Upload PDFs to GCS
4. Create upload metadata record
5. Save metadata to GCS

**Storage Structure:**
```
pdf/
  ├── {carrier_name}_{type}_{timestamp}.pdf
  └── uploads_metadata.json
```

### 3.5 Celery Configuration (`celery_config.py`)

**Broker:** Redis
**Result Backend:** Redis

**Settings:**
- Task timeout: 30 minutes (hard), 25 minutes (soft)
- Worker concurrency: 2
- Result expiration: 1 hour

**Tasks:**
- `process_ocr_task()` - Background OCR processing

---

## 4. Frontend Architecture

### 4.1 Application Structure

```
frontend/
├── app/
│   ├── page.tsx              # Landing page
│   ├── layout.tsx            # Root layout
│   ├── login/
│   │   └── page.tsx          # Login page
│   ├── register/
│   │   └── page.tsx          # Registration page
│   ├── dashboard/
│   │   └── page.tsx          # Dashboard (upload interface)
│   ├── summary/
│   │   ├── page.tsx          # Upload summary & confirmation
│   │   └── confirmed/
│   │       └── page.tsx      # Confirmation success page
│   └── homepage/
│       └── page.tsx          # Homepage
├── context/
│   └── AuthContext.tsx       # Authentication context
└── public/                   # Static assets
```

### 4.2 Key Components

**AuthContext (`context/AuthContext.tsx`):**
- Manages user authentication state
- Provides `login()`, `register()`, `logout()` functions
- Stores user in localStorage
- API URL detection (localhost vs production)

**Summary Page (`app/summary/page.tsx`):**
- Multi-carrier upload interface
- File upload for Property/Liability/Liquor PDFs
- Form validation
- Upload execution
- Phase 1 quality analysis trigger

**Dashboard (`app/dashboard/page.tsx`):**
- Main navigation hub
- Upload history display

### 4.3 API Integration

**Base URL:**
- Development: `http://localhost:8000`
- Production: `https://insurance-backend.duckdns.org`

**Key API Calls:**
- Upload: `POST /upload-quotes/`
- Quality Analysis: `GET /phase1/quality-analysis?uploadId=...`
- Upload History: `GET /upload-history/`

---

## 5. Data Storage & Persistence

### 5.1 Google Cloud Storage Structure

```
gs://deployment/
├── pdf/
│   ├── {carrier}_{type}_{timestamp}.pdf
│   └── uploads_metadata.json
├── metadata/
│   └── users.json
└── results/
    └── {uploadId}/
        └── {carrier}/
            └── {type}/
                ├── ocr_all_pages.txt
                ├── smart_selection.json
                ├── intelligent_combined.txt
                └── extracted_fields.json
```

### 5.2 Metadata Files

**`pdf/uploads_metadata.json`:**
```json
{
  "uploads": [
    {
      "uploadId": "upload_20241029_103000",
      "userId": "user_1",
      "uploadedAt": "2024-10-29T10:30:00",
      "totalCarriers": 2,
      "totalFiles": 4,
      "carriers": [
        {
          "carrierName": "State Farm",
          "propertyPDF": {
            "filename": "state_farm_property_20241029_103000.pdf",
            "path": "gs://deployment/pdf/...",
            "size": 1234567,
            "uploadedAt": "2024-10-29T10:30:00"
          },
          "liabilityPDF": {...}
        }
      ]
    }
  ]
}
```

**`metadata/users.json`:**
```json
{
  "user_1": {
    "email": "user@example.com",
    "password": "plaintext_password",
    "created_at": "2024-10-29T10:00:00"
  }
}
```

---

## 6. Deployment Architecture

### 6.1 Docker Compose Setup (`docker-compose.yml`)

**Services:**
1. **Redis** - Message broker (port 6380)
2. **Backend** - FastAPI server (port 8000)
3. **Celery Worker** - Background task processor

**Networks:**
- `insurance-network` (bridge)

**Volumes:**
- `redis_data` - Redis persistence
- Code volumes for hot-reload

**Environment Variables:**
- `REDIS_URL` - Redis connection
- `GOOGLE_APPLICATION_CREDENTIALS` - GCP credentials path
- `BUCKET_NAME` - GCS bucket name
- `OPENAI_API_KEY` - OpenAI API key

### 6.2 Dockerfile

**Base Image:** `python:3.11-slim`

**Dependencies:**
- System: `libgl1`, `libglib2.0-0` (for PyMuPDF)
- Python: From `requirements.txt`

**Startup:** `start.sh` script
- Decodes GCP credentials from base64
- Starts uvicorn server

### 6.3 Production Deployment (AWS EC2)

**Target:** EC2 t2.micro (free tier)

**Requirements:**
- Docker & Docker Compose installed
- Environment variables configured
- GCP credentials available
- Domain/DNS configured (duckdns.org)

---

## 7. Security Analysis

### 7.1 Security Issues

**Critical:**
1. **Plain Text Passwords** - Passwords stored without hashing
2. **No JWT Tokens** - Authentication state unclear
3. **CORS Wildcard** - `allow_origins=["*"]` allows any origin
4. **No Rate Limiting** - API endpoints unprotected
5. **API Keys in Code** - NanoNets API key hardcoded

**Medium:**
1. **No Input Validation** - File uploads not validated
2. **No File Size Limits** - Potential DoS
3. **No Authentication on Endpoints** - Most endpoints unprotected
4. **Credentials in Environment** - GCP credentials in env vars

**Low:**
1. **Error Messages** - May leak internal details
2. **No HTTPS Enforcement** - HTTP allowed

### 7.2 Recommendations

1. Implement password hashing (bcrypt/argon2)
2. Add JWT-based authentication
3. Restrict CORS to specific origins
4. Add rate limiting (slowapi)
5. Move API keys to secure vault
6. Add file validation (type, size, content)
7. Implement endpoint authentication middleware
8. Add request logging and monitoring

---

## 8. Performance Analysis

### 8.1 Bottlenecks

1. **OCR Processing** - Sequential page processing (could be parallelized)
2. **LLM API Calls** - Sequential chunk processing
3. **File Downloads** - Multiple GCS downloads per phase
4. **No Caching** - Repeated GCS reads

### 8.2 Optimization Opportunities

1. **Parallel OCR** - Process multiple pages concurrently
2. **Batch LLM Calls** - Process multiple chunks in parallel
3. **Caching Layer** - Redis cache for frequently accessed data
4. **Streaming** - Stream large files instead of loading entirely
5. **Connection Pooling** - Reuse GCS client connections

---

## 9. Error Handling & Resilience

### 9.1 Current State

**Error Handling:**
- Try-catch blocks in most functions
- HTTP exceptions for API errors
- Error messages returned to frontend

**Issues:**
- No retry logic for API calls (OCR, LLM)
- No circuit breakers
- No dead letter queue for failed tasks
- Errors may be lost in background tasks

### 9.2 Recommendations

1. Add retry logic with exponential backoff
2. Implement circuit breakers for external APIs
3. Add dead letter queue for failed Celery tasks
4. Implement comprehensive logging
5. Add error monitoring (Sentry, etc.)

---

## 10. Testing & Quality Assurance

### 10.1 Current State

**Test Files Found:**
- `backend/test_auth.py` - Authentication tests
- `backend/test.py` - General tests

**Coverage:** Unknown (no test reports found)

### 10.2 Recommendations

1. Add unit tests for each phase
2. Add integration tests for API endpoints
3. Add end-to-end tests for full pipeline
4. Add load testing for concurrent uploads
5. Implement CI/CD pipeline

---

## 11. Code Quality & Maintainability

### 11.1 Strengths

- Clear separation of concerns (phases)
- Well-documented functions
- Consistent naming conventions
- Modular design

### 11.2 Areas for Improvement

1. **Code Duplication** - Similar code across phases
2. **Magic Numbers** - Hardcoded values (chunk size, timeouts)
3. **Error Messages** - Inconsistent error handling
4. **Type Hints** - Incomplete type annotations
5. **Configuration** - Hardcoded values should be configurable

---

## 12. Dependencies & External Services

### 12.1 External Dependencies

**APIs:**
- **NanoNets OCR** - OCR processing (API key: `bdee3d34-b8db-11f0-bd7c-dece98018c81`)
- **OpenAI GPT** - LLM field extraction
- **Google Cloud Storage** - File storage
- **Google Sheets API** - Data export

**Libraries:**
- FastAPI, Celery, Redis
- PyMuPDF, docstrange (NanoNets)
- gspread (Google Sheets)
- Next.js, React, TypeScript

### 12.2 Version Management

**Python:** 3.11
**Node.js:** Not specified (Next.js 16 requires Node 18+)

**Dependency Files:**
- `backend/requirements.txt` - Python dependencies
- `frontend/package.json` - Node dependencies

---

## 13. Business Logic & Domain Model

### 13.1 Core Entities

**User:**
- ID, Email, Password, Created At

**Upload:**
- Upload ID, User ID, Timestamp, Carriers, Files

**Carrier:**
- Name, Property PDF, Liability PDF, Liquor PDF

**Processing Result:**
- Upload ID, Carrier, File Type, Phase Results

### 13.2 Insurance Fields Extracted

**34 Fields Total:**
- Coverage types and limits
- Deductibles and coinsurance
- Premium amounts
- Policy dates
- Additional coverages

**Field Types:**
- Property Coverage
- General Liability
- Liquor Liability

---

## 14. Workflow & State Management

### 14.1 Processing States

1. **Uploaded** - Files uploaded to GCS
2. **Phase 1 Complete** - Quality analysis done
3. **Phase 2 Complete** - OCR extraction done
4. **Phase 2C Complete** - Smart selection done
5. **Phase 2D Complete** - Intelligent combination done
6. **Phase 3 Complete** - LLM extraction done
7. **Phase 5 Complete** - Google Sheets export done

**Note:** No explicit state machine - states implicit in file existence

### 14.2 State Persistence

- States stored as files in GCS
- No database tracking of processing state
- Frontend polls for completion (no WebSocket)

---

## 15. Monitoring & Observability

### 15.1 Current State

**Logging:**
- Print statements for debugging
- No structured logging
- No log aggregation

**Metrics:**
- No metrics collection
- No performance monitoring
- No error tracking

### 15.2 Recommendations

1. Implement structured logging (Python `logging` module)
2. Add request/response logging middleware
3. Add performance metrics (processing time per phase)
4. Implement health check endpoints
5. Add monitoring dashboard (Grafana, etc.)

---

## 16. Scalability Considerations

### 16.1 Current Limitations

1. **Single Celery Worker** - Concurrency limited to 2 tasks
2. **Sequential Processing** - Phases run sequentially
3. **No Load Balancing** - Single backend instance
4. **File Size Limits** - No explicit limits

### 16.2 Scaling Strategies

1. **Horizontal Scaling** - Multiple Celery workers
2. **Parallel Processing** - Process multiple carriers concurrently
3. **Load Balancer** - Multiple backend instances
4. **Queue Partitioning** - Separate queues per phase
5. **Caching** - Redis cache for frequently accessed data

---

## 17. Cost Analysis

### 17.1 External Service Costs

**NanoNets OCR:**
- API key hardcoded (likely paid plan)
- Cost per page processed

**OpenAI GPT:**
- API calls per chunk
- Cost depends on model and tokens

**Google Cloud Storage:**
- Storage costs for PDFs and results
- API call costs

**Google Sheets API:**
- API quota limits
- No direct cost (within quota)

### 17.2 Optimization Opportunities

1. Cache OCR results to avoid reprocessing
2. Optimize LLM prompts to reduce tokens
3. Compress stored files
4. Implement data retention policies

---

## 18. Future Enhancements

### 18.1 Recommended Features

1. **Real-time Progress Updates** - WebSocket for live status
2. **Batch Processing** - Process multiple uploads
3. **Field Validation** - Validate extracted fields
4. **Comparison View** - Side-by-side carrier comparison
5. **Export Options** - CSV, Excel, JSON exports
6. **User Management** - Admin panel, user roles
7. **Audit Logging** - Track all user actions
8. **Notification System** - Email/SMS on completion

### 18.2 Technical Improvements

1. **API Versioning** - Version API endpoints
2. **GraphQL** - Consider GraphQL for flexible queries
3. **Microservices** - Split phases into separate services
4. **Event-Driven Architecture** - Use message queues for phases
5. **Database Migration** - Move from JSON files to PostgreSQL

---

## 19. Conclusion

### 19.1 System Strengths

- **Comprehensive Pipeline** - Well-structured multi-phase processing
- **Modern Stack** - FastAPI, Next.js, Docker
- **Cloud-Native** - GCS integration, scalable architecture
- **Automated Workflow** - Auto-triggering between phases

### 19.2 Critical Issues

- **Security** - Plain text passwords, no authentication middleware
- **Error Handling** - Limited retry logic, error recovery
- **Monitoring** - No observability, difficult to debug
- **Testing** - Minimal test coverage

### 19.3 Priority Actions

1. **Immediate:** Fix security issues (password hashing, authentication)
2. **Short-term:** Add error handling, retry logic, logging
3. **Medium-term:** Implement testing, monitoring, performance optimization
4. **Long-term:** Scale architecture, add features, improve UX

---

## 20. File Structure Reference

```
Deployment/
├── backend/
│   ├── app.py                          # FastAPI main application
│   ├── auth.py                         # Authentication logic
│   ├── database.py                     # Database operations (GCS JSON)
│   ├── upload_handler.py               # PDF upload handling
│   ├── tasks.py                        # Celery task definitions
│   ├── celery_config.py                # Celery configuration
│   ├── phase1.py                       # Phase 1: PDF quality analysis
│   ├── phase1_pymudf.py               # PyMuPDF helper functions
│   ├── phase2_ocr_nano.py             # Phase 2: OCR extraction (NanoNets)
│   ├── phase2c_smart_selection.py     # Phase 2C: Smart text selection
│   ├── phase2d_intelligent_combination.py  # Phase 2D: Text combination
│   ├── phase3_llm.py                   # Phase 3: LLM field extraction
│   ├── phase3_gl.py                    # Phase 3: GL variant
│   ├── phase3_liqour.py                # Phase 3: Liquor variant
│   ├── phase5_googlesheet.py          # Phase 5: Google Sheets export
│   ├── multi_carrier_property.py      # Multi-carrier processing
│   ├── multi_carrier_gl.py            # Multi-carrier GL processing
│   ├── requirements.txt                # Python dependencies
│   ├── test_auth.py                    # Authentication tests
│   └── test.py                         # General tests
├── frontend/
│   ├── app/                            # Next.js app directory
│   │   ├── page.tsx                    # Landing page
│   │   ├── layout.tsx                 # Root layout
│   │   ├── login/page.tsx             # Login page
│   │   ├── register/page.tsx          # Registration page
│   │   ├── dashboard/page.tsx          # Dashboard
│   │   ├── summary/page.tsx            # Upload summary
│   │   └── summary/confirmed/page.tsx  # Confirmation page
│   ├── context/
│   │   └── AuthContext.tsx             # Auth context provider
│   ├── package.json                    # Node dependencies
│   └── tsconfig.json                   # TypeScript config
├── docker-compose.yml                  # Docker Compose configuration
├── Dockerfile                          # Docker image definition
├── start.sh                            # Startup script
├── generate_key.py                     # Key generation utility
└── tokenizer.py                        # Token utility
```

---

**Document Generated:** 2024  
**Last Updated:** Based on current codebase analysis  
**Version:** 1.0
