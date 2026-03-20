# Railway Deployment Guide - GCP Setup

## Quick Setup Steps

### Step 1: Get Your GCP Service Account Credentials

1. **Go to Google Cloud Console**
   - Visit: https://console.cloud.google.com/
   - Select your project (or create one)

2. **Create/Select Service Account**
   - Navigate to: **IAM & Admin** → **Service Accounts**
   - Click **Create Service Account** (or select existing one)
   - Name: `railway-insurance-app` (or any name)
   - Click **Create and Continue**

3. **Grant Permissions**
   - Add these roles:
     - **Storage Admin** (for GCS bucket access)
     - **Storage Object Admin** (for reading/writing files)
   - Click **Continue** → **Done**

4. **Create JSON Key**
   - Click on your service account
   - Go to **Keys** tab
   - Click **Add Key** → **Create new key**
   - Select **JSON** format
   - Click **Create**
   - **Save the downloaded JSON file** (e.g., `railway-gcp-credentials.json`)

### Step 2: Verify Your GCS Bucket

1. **Check Bucket Name**
   - Go to **Cloud Storage** → **Buckets**
   - Note your bucket name (e.g., `my-insurance-bucket`)

2. **Grant Service Account Access**
   - Click on your bucket
   - Go to **Permissions** tab
   - Click **Grant Access**
   - Add your service account email (from Step 1)
   - Role: **Storage Admin** or **Storage Object Admin**
   - Click **Save**

### Step 3: Encode Credentials to Base64

**On Windows (PowerShell):**
```powershell
# Navigate to folder with your JSON file
cd C:\path\to\your\credentials

# Encode to base64
$content = Get-Content -Path "railway-gcp-credentials.json" -Raw -Encoding UTF8
$bytes = [System.Text.Encoding]::UTF8.GetBytes($content)
$base64 = [System.Convert]::ToBase64String($bytes)
$base64 | Out-File -FilePath "credentials-base64.txt" -Encoding UTF8

# Display (copy this entire output)
Write-Host $base64
```

**On Mac/Linux:**
```bash
# Encode to base64
base64 -i railway-gcp-credentials.json -o credentials-base64.txt

# Or display directly
base64 -i railway-gcp-credentials.json
```

**Online Tool (Alternative):**
- Visit: https://www.base64encode.org/
- Paste your JSON file content
- Click **Encode**
- Copy the result

### Step 4: Enable Google Sheets API (Optional but Recommended)

If you want to use Phase 5 (Google Sheets export):

1. **Enable Google Sheets API**
   - Go to: https://console.cloud.google.com/apis/library/sheets.googleapis.com
   - Click **Enable**

2. **Enable Google Drive API** (required for creating sheets)
   - Go to: https://console.cloud.google.com/apis/library/drive.googleapis.com
   - Click **Enable**

3. **Use Same Service Account**
   - The same service account JSON from Step 1 can be used
   - Make sure it has these roles:
     - **Storage Admin** (for GCS)
     - **Service Account User** (for Sheets API)

**Note:** Google Sheets credentials are optional. If you don't set them up, Phase 5 will fail, but other phases will work fine.

### Step 5: Set Railway Environment Variables

1. **Go to Railway Dashboard**
   - Open your project: https://railway.app/
   - Select your service

2. **Add Environment Variables**
   - Go to **Variables** tab
   - Click **+ New Variable** for each:

   **Required Variables:**
   
   | Variable Name | Value | Description |
   |-------------|-------|-------------|
   | `GCP_CREDENTIALS_BASE64` | `[Your base64 encoded JSON]` | Your entire base64 string from Step 3 |
   | `BUCKET_NAME` | `your-bucket-name` | Your GCS bucket name (without gs://) |
   | `OPENAI_API_KEY` | `sk-...` | Your OpenAI API key |
   | `REDIS_URL` | `redis://...` | Railway Redis URL (if using Railway Redis) |

   **Optional Variables (for Google Sheets):**
   
   | Variable Name | Value | Description |
   |-------------|-------|-------------|
   | `GOOGLE_SHEETS_CREDENTIALS` | `/app/credentials/gcp-credentials.json` | Path to credentials (same as GCP) |
   | `PORT` | `8000` | Railway sets this automatically |
   | `PYTHONPATH` | `/app` | Already set in Dockerfile |

   **Important Notes:**
   - `BUCKET_NAME` must match your GCS bucket name exactly (case-sensitive)
   - The same `GCP_CREDENTIALS_BASE64` works for both GCS and Google Sheets
   - `REDIS_URL` is only needed if you're using Railway's Redis service

3. **Save Variables**
   - Click **Save** after adding each variable
   - Railway will automatically redeploy

### Step 6: Verify Deployment

1. **Check Logs**
   - Go to **Deployments** tab
   - Click on latest deployment
   - Check logs for:
     ```
     ✅ GCP credentials written to /app/credentials/gcp-credentials.json
     ✅ Credentials file verified
     ✅ Starting uvicorn on port 8000...
     ```

2. **Test Health Endpoint**
   - Get your Railway URL (e.g., `https://your-app.up.railway.app`)
   - Visit: `https://your-app.up.railway.app/health`
   - Should return: `{"status": "healthy"}`

---

## Troubleshooting

### Error: "No GCP credentials found"

**Solution:**
- Verify `GCP_CREDENTIALS_BASE64` is set in Railway
- Check that the base64 string is complete (no line breaks)
- Ensure you copied the ENTIRE base64 string

### Error: "Credentials file not found"

**Solution:**
- The base64 decode might have failed
- Re-encode your JSON file
- Make sure JSON is valid (no extra characters)

### Error: "Permission denied" or "Bucket not found"

**Solution:**
- Verify `BUCKET_NAME` matches exactly (case-sensitive)
- Check service account has Storage Admin role
- Verify service account email is added to bucket permissions

### Error: "Invalid credentials"

**Solution:**
- Regenerate service account key
- Make sure you're using the correct project's credentials
- Verify JSON file is not corrupted

---

## Environment Variables Reference

### Required Variables

```bash
# GCP Credentials (base64 encoded JSON)
GCP_CREDENTIALS_BASE64=eyJ0eXBlIjoic2VydmljZV9hY2NvdW50Ii...

# Your GCS Bucket Name
BUCKET_NAME=my-insurance-bucket

# OpenAI API Key
OPENAI_API_KEY=sk-proj-...

# Redis URL (if using Railway Redis)
REDIS_URL=redis://default:password@redis.railway.internal:6379
```

### How Variables Are Used

1. **`GCP_CREDENTIALS_BASE64`**
   - Decoded by `start.sh` script
   - Saved to `/app/credentials/gcp-credentials.json`
   - Used by Google Cloud Storage client

2. **`BUCKET_NAME`**
   - Used throughout all phase modules
   - Default: `deployment` (if not set)
   - Must match your actual bucket name

3. **`OPENAI_API_KEY`**
   - Used in Phase 3 (LLM extraction)
   - Required for field extraction

4. **`REDIS_URL`**
   - Used by Celery for task queue
   - Format: `redis://[password]@[host]:[port]`

---

## Quick Checklist

- [ ] GCP Service Account created
- [ ] Service Account has Storage Admin role
- [ ] JSON key downloaded
- [ ] JSON key encoded to base64
- [ ] `GCP_CREDENTIALS_BASE64` set in Railway
- [ ] `BUCKET_NAME` set in Railway (matches your bucket exactly)
- [ ] `OPENAI_API_KEY` set in Railway
- [ ] Service account added to bucket permissions
- [ ] Google Sheets API enabled (optional)
- [ ] Google Drive API enabled (optional, for Sheets)
- [ ] Deployment successful (check logs)
- [ ] Health endpoint returns `{"status": "healthy"}`

---

## Security Best Practices

1. **Never commit credentials to Git**
   - JSON files are in `.gitignore`
   - Base64 strings should only be in Railway

2. **Use separate service accounts**
   - One for development
   - One for production

3. **Rotate keys regularly**
   - Regenerate service account keys periodically
   - Update Railway variables when rotated

4. **Limit permissions**
   - Only grant necessary roles
   - Use least privilege principle

---

## Testing Your Setup

After deployment, test with:

```bash
# Health check
curl https://your-app.up.railway.app/health

# Root endpoint
curl https://your-app.up.railway.app/

# Test upload (requires authentication)
curl -X POST https://your-app.up.railway.app/upload-quotes/ \
  -F "carriers_json={\"carriers\":[{\"name\":\"Test\"}]}" \
  -F "files=@test.pdf"
```

---

## Need Help?

If you encounter issues:

1. Check Railway logs for specific error messages
2. Verify all environment variables are set correctly
3. Test GCP credentials locally first:
   ```bash
   export GOOGLE_APPLICATION_CREDENTIALS="path/to/credentials.json"
   python -c "from google.cloud import storage; print(storage.Client().get_bucket('your-bucket-name'))"
   ```
