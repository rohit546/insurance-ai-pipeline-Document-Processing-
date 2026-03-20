# Railway Quick Setup - Fix GCP Credentials Error

## 🚨 Current Error
```
ERROR: No GCP credentials found! Set GCP_CREDENTIALS_BASE64 in Railway environment variables.
```

## ✅ Quick Fix (5 Minutes)

### 1. Get Your GCP Credentials JSON

**Option A: You already have a JSON file**
- Use that file, skip to step 2

**Option B: Create new service account**
1. Go to: https://console.cloud.google.com/iam-admin/serviceaccounts
2. Click **Create Service Account**
3. Name: `railway-app`
4. Grant role: **Storage Admin**
5. Click **Keys** → **Add Key** → **Create new key** → **JSON**
6. Download the JSON file

### 2. Encode to Base64

**Windows PowerShell:**
```powershell
# Replace with your file path
$file = "C:\path\to\your\credentials.json"
$content = Get-Content $file -Raw -Encoding UTF8
$bytes = [System.Text.Encoding]::UTF8.GetBytes($content)
[System.Convert]::ToBase64String($bytes)
```

**Or use Python script:**
```bash
python encode_credentials.py your-credentials.json
```

**Or online:**
- Go to: https://www.base64encode.org/
- Paste your JSON content
- Copy the result

### 3. Set Railway Variables

Go to Railway → Your Service → **Variables** tab:

| Variable | Value |
|----------|-------|
| `GCP_CREDENTIALS_BASE64` | `[Paste entire base64 string here]` |
| `BUCKET_NAME` | `your-actual-bucket-name` |
| `OPENAI_API_KEY` | `sk-proj-...` |

**Important:**
- `BUCKET_NAME` must match your GCS bucket name exactly
- Copy the ENTIRE base64 string (it's very long)
- No spaces or line breaks

### 4. Verify Bucket Permissions

1. Go to: https://console.cloud.google.com/storage/buckets
2. Click your bucket
3. Go to **Permissions** tab
4. Add your service account email
5. Role: **Storage Admin**

### 5. Redeploy

Railway will auto-redeploy. Check logs - you should see:
```
✅ GCP credentials written to /app/credentials/gcp-credentials.json
✅ Credentials file verified
✅ Starting uvicorn on port 8000...
```

---

## 🔍 Verify It Works

Visit: `https://your-app.up.railway.app/health`

Should return: `{"status": "healthy"}`

---

## ❓ Still Having Issues?

**Error: "Credentials file not found"**
- Base64 string might be incomplete
- Re-encode your JSON file
- Make sure you copied the ENTIRE string

**Error: "Permission denied"**
- Check bucket name matches exactly (case-sensitive)
- Verify service account has Storage Admin role
- Check service account is added to bucket permissions

**Error: "Bucket not found"**
- Double-check `BUCKET_NAME` variable
- Verify bucket exists in GCP Console
- Check you're using the correct GCP project

---

## 📝 What Each Variable Does

- **`GCP_CREDENTIALS_BASE64`**: Your GCP service account credentials (base64 encoded)
- **`BUCKET_NAME`**: Your Google Cloud Storage bucket name (where PDFs are stored)
- **`OPENAI_API_KEY`**: Required for Phase 3 (LLM field extraction)
- **`REDIS_URL`**: Only needed if using Railway Redis (for Celery tasks)

---

## 🎯 Minimum Required Variables

For basic functionality, you need:
1. ✅ `GCP_CREDENTIALS_BASE64`
2. ✅ `BUCKET_NAME`
3. ✅ `OPENAI_API_KEY`

That's it! The rest are optional.
