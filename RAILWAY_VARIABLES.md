# Railway Environment Variables - Ready to Copy

## ✅ Your Configuration

Based on your GCP setup:
- **Project ID:** `pdf-generator-477915`
- **Bucket Name:** `mckinney_suite_documents`
- **Service Account:** `mckinney-documents@pdf-generator-477915.iam.gserviceaccount.com`

---

## 📋 Copy These Variables to Railway

Go to Railway → Your Service → **Variables** tab → Add these:

### 1. GCP Credentials (Base64 Encoded)

**Variable Name:** `GCP_CREDENTIALS_BASE64`

**Variable Value:** (Copy this ENTIRE string - it's very long, no line breaks)

**⚠️ IMPORTANT: Copy ONLY the base64 string below. NO quotes, NO backticks, NO apostrophes - just the raw string!**

```
ewogICJ0eXBlIjogInNlcnZpY2VfYWNjb3VudCIsCiAgInByb2plY3RfaWQiOiAicGRmLWdlbmVyYXRvci00Nzc5MTUiLAogICJwcml2YXRlX2tleV9pZCI6ICJhYzhmODJkZDc2MDMwZmE2NjRlYzhkMGJkNjkxNGE1NTU4ZmUwMzM4IiwKICAicHJpdmF0ZV9rZXkiOiAiLS0tLS1CRUdJTiBQUklWQVRFIEtFWS0tLS0tXG5NSUlFdndJQkFEQU5CZ2txaGtpRzl3MEJBUUVGQUFTQ0JLa3dnZ1NsQWdFQUFvSUJBUURkTXpOY25TZ3pWeVViXG5KTnd3N0duNzBtRitXVEhZMW9PWURsRS95VXh3Mmp3NXRCTExGTlJ2UXc0aTlTSXliWE05UkRwOHJDT1FweHgyXG5IWlAzcjBoeGwxa0FzMURvc1VYTTVrZUo2eWE4akRIL28xajdkQnRVbTdjUHVlVDNiRjFSMDBDQWxDMGRYVnFlXG53djNJOVhROTlXRldidjdoUWVrR2ZabW5hbU1nMXhmaTRpQkw1THJRRk4wN01NUDVEWmk0RWFNUnBhdjlydjh2XG5DWjhFZVdRVjlYYnFMV3V4OGFoQW0reVZqemdibStONnBlaThSVTZQT0wzSCtWSXFjdmZZbnFkdktSeDIyZ0UvXG5ieUpiMTNmREFWNVZQZHJPZno1eXhRMk5oQkhiOE1oTHZScU13ckRFWkE2NjVDRUdlWTNYeTlGclVVOTdlejkwXG4zTGwrZHFUWkFnTUJBQUVDZ2dFQUI2bFFQREdYMWpqWU5IRHVUT3ZjREp0eTVURWthTzhTOGI5bXFCelZ4SzRtXG5Bdk4wQUtOcHlKS1RXOFZVNE9NcHhVZnNnZkY4djFFTDRuOXRDY0pTaVdNejgxVkNvZCtGbkRSaHBRZ1RCQWhDXG5WQ2t3Zk0wYjRUbmZlVFlPL2c3ZVZtWXhYM2NUMTlzcUs3WnptMFdnQ1ljb0lGL1BuNTc0S0dqQnovQ0s3R2t6XG5aQjUvc0VtWmQ3amZLR2NvaGUzSW8wdi9XakZ6NDJrZkp2K0twTFB2Z2RJVktsQ1VwbjZFeU1sTUJMTnJMMDVrXG5Pc0EvS3k5cnQxcEc5Y0luZDR2MGpyZFNiZDR4VElxSXVwc3FSTEFVd0tJUi9hZUZtWkhDSzNoeGxhREVWVmluXG44K1hHRTJEZTVTVERCQjJYd2NHYTZZYytTc2piQUcxVVQ4TVZjVjNLelFLQmdRRHdZMHNoQXhiYXZwc28yeG5BXG5oRElDbFdRV0oweFAyRnpCcjdsQVFCZU9LQ0FhTDFMaXAvU1orR3EzUGFYMW0ySXc2VVdQaXptanNXcndLWHpuXG5TbkxQNDZwbmx3RUF1QzFnWkpQZG8rbnBXM20rTUNQNlBzUXpNcEpweUh0RnlIRVJtSjVBalkrSWVHV1NKeHdlXG5RdmFNblJsSWlHYitZemllRStDT1NzZ0I2d0tCZ1FEcmtPTnBiekJKdE9xcGVmdTlSdTZYdEkyOEhqQXlSTVBqXG51MGFNS0xqQlJ0enVLU3Uxd0w2YkdYRGhGanY0bHQ3QnpzN2tXQTFjS1ZEZWtCUU0yYkJHaytlK3hhUlUrWWlSXG40V0VDUEdlbHJmY2xUclhWVXc1QTdiemhrSisyRWtKdUkrdkQ2bmJ5d1cwbGk2b0wxc2w2Yzl2M05uS3hGRjR3XG5PdGRqd1ovL1N3S0JnUUNUK1N0SUdQUzZsbFpBb2c2S0dWWUhqUnBkSUxleTlzYXlXNDgzWTd5Y1AwcnprMm44XG5KOU5EN0UwVW14aEx4ZWg5bnpxNGo4VmRaRnNsbHdSU0E5d0U2R24rOWs4aHVENEdkaU1uYmowUTdzUlVOS0lRXG42dzR2VGRRZTFkQThOcFNUZGxVRCs1LzlLOCtxVmZUVEUvbGN1Vi9VN0ZJcUNiM1NZUDM3MlVDaEhRS0JnUUNMXG5XUG90dEloc0VZbC9GNW5ETGVLViswaWNyMzd6UFpwamJMVWUxRGYybldTenZjY05qU2N6dUtqOWZabWNSQ01oXG5vcVRnanZYWVB0aUh6OU1NaHZtdnhtNmdlMm5xbW5JZldhTjVIeUp3NzZmemVjdzJsUnNwYlhqK05mOUVSU2ptXG4zbmpwUEJtQklNcmdHdTVNY3BKY3pZeWhnS1AzL0lSN0kxT212ek5XWVFLQmdRQ0h6UWFXRkVHSk1NeGRCbWY5XG5BVzAvTkVPcDIwenJUODBDakdTdmdubSt4WjAzSldHT29ZSTM3SlhhZGU1eUk1MllOb25sa2hrZkZRNjlvSm9NXG5PdEs2cU5Nc1BudWE1MVBUZUFyNEgyaDBtZjZCN2piZURqK0N0Y1pFWG1SUy9vWVoxTGZXT2hQc21BdlJCZ3FtXG5vTlhnKzFyTktJZk9aVlZlek5DYW1RdllMdz09XG4tLS0tLUVORCBQUklWQVRFIEtFWS0tLS0tXG4iLAogICJjbGllbnRfZW1haWwiOiAibWNraW5uZXktZG9jdW1lbnRzQHBkZi1nZW5lcmF0b3ItNDc3OTE1LmlhbS5nc2VydmljZWFjY291bnQuY29tIiwKICAiY2xpZW50X2lkIjogIjEwOTY2MDc5ODIyODMxMjM1NDU0OSIsCiAgImF1dGhfdXJpIjogImh0dHBzOi8vYWNjb3VudHMuZ29vZ2xlLmNvbS9vL29hdXRoMi9hdXRoIiwKICAidG9rZW5fdXJpIjogImh0dHBzOi8vb2F1dGgyLmdvb2dsZWFwaXMuY29tL3Rva2VuIiwKICAiYXV0aF9wcm92aWRlcl94NTA5X2NlcnRfdXJsIjogImh0dHBzOi8vd3d3Lmdvb2dsZWFwaXMuY29tL29hdXRoMi92MS9jZXJ0cyIsCiAgImNsaWVudF94NTA5X2NlcnRfdXJsIjogImh0dHBzOi8vd3d3Lmdvb2dsZWFwaXMuY29tL3JvYm90L3YxL21ldGFkYXRhL3g1MDkvbWNraW5uZXktZG9jdW1lbnRzJTQwcGRmLWdlbmVyYXRvci00Nzc5MTUuaWFtLmdzZXJ2aWNlYWNjb3VudC5jb20iLAogICJ1bml2ZXJzZV9kb21haW4iOiAiZ29vZ2xlYXBpcy5jb20iCn0K
```

**What to copy:** Start from `ewogICJ0eXBlIjogInNlcnZpY2VfYWNjb3VudCIs...` and copy everything until `...Z29vZ2xlYXBpcy5jb20iCn0K`

**What NOT to copy:** The triple backticks (```) around it - those are just markdown formatting!

---

### 2. Bucket Name

**Variable Name:** `BUCKET_NAME`

**Variable Value:**
```
mckinney_suite_documents
```

**⚠️ Important:** This must match exactly (case-sensitive). From your screenshot, your bucket is `mckinney_suite_documents`.

---

### 3. OpenAI API Key

**Variable Name:** `OPENAI_API_KEY`

**Variable Value:** (Your OpenAI API key)
```
sk-proj-...
```

---

### 4. Redis URL (If Using Railway Redis)

**Variable Name:** `REDIS_URL`

**Variable Value:** (Get from Railway Redis service)
```
redis://default:password@redis.railway.internal:6379
```

**Note:** Only needed if you're using Railway's Redis service for Celery. If not using Celery tasks, you can skip this.

---

## ✅ Verification Checklist

After setting variables:

- [ ] `GCP_CREDENTIALS_BASE64` - Base64 string copied completely (no line breaks)
- [ ] `BUCKET_NAME` - Set to `mckinney_suite_documents` (exact match)
- [ ] `OPENAI_API_KEY` - Your OpenAI key set
- [ ] Service account has Storage Admin role in GCP
- [ ] Service account added to bucket permissions

---

## 🔍 Verify Bucket Permissions

1. Go to: https://console.cloud.google.com/storage/buckets/mckinney_suite_documents
2. Click **Permissions** tab
3. Verify `mckinney-documents@pdf-generator-477915.iam.gserviceaccount.com` is listed
4. If not, click **Grant Access** and add it with **Storage Admin** role

---

## 🚀 After Setting Variables

Railway will automatically redeploy. Check logs - you should see:

```
✅ GCP credentials written to /app/credentials/gcp-credentials.json
✅ Credentials file verified at /app/credentials/gcp-credentials.json
✅ Starting uvicorn on port 8000...
```

Then test: `https://your-app.up.railway.app/health`

Should return: `{"status": "healthy"}`

---

## 🆘 Troubleshooting

**Still getting "No GCP credentials found"?**
- Make sure you copied the ENTIRE base64 string (it's very long)
- No spaces or line breaks in the variable value
- Check Railway logs for exact error

**"Bucket not found" or "Permission denied"?**
- Double-check `BUCKET_NAME` is exactly `mckinney_suite_documents`
- Verify service account has Storage Admin role
- Check service account is added to bucket permissions
