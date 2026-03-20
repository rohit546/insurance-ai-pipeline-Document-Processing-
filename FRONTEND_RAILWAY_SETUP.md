# Frontend Setup for Railway Backend

## ✅ Backend Status

Your backend is running successfully at:
**https://insurance-ai-pipeline-document-processing-production.up.railway.app**

## 🔧 Frontend Configuration Updated

I've updated your frontend to use the Railway backend URL. The changes are in:

1. ✅ `frontend/context/AuthContext.tsx` - Updated API URL
2. ✅ `frontend/app/summary/page.tsx` - Updated API URL  
3. ✅ `frontend/app/homepage/page.tsx` - Updated API URL

## 🚀 How to Use Frontend

### Option 1: Run Frontend Locally (Development)

```bash
cd frontend
npm install
npm run dev
```

Then open: **http://localhost:3000**

The frontend will automatically:
- Use `http://localhost:8000` when running locally
- Use Railway URL when deployed to production

### Option 2: Deploy Frontend to Vercel (Recommended)

1. **Push your code to GitHub** (if not already)
   ```bash
   git add .
   git commit -m "Update frontend to use Railway backend"
   git push
   ```

2. **Deploy to Vercel:**
   - Go to: https://vercel.com
   - Import your GitHub repository
   - Set root directory to: `frontend`
   - Deploy

3. **Vercel will automatically:**
   - Detect Next.js
   - Build and deploy
   - Frontend will use Railway backend automatically (not localhost)

## 🧪 Test the Connection

### Test Backend Directly:
```bash
curl https://insurance-ai-pipeline-document-processing-production.up.railway.app/health
```

Should return: `{"status": "healthy"}`

### Test from Frontend:
1. Open your frontend (localhost:3000 or Vercel URL)
2. Try to register/login
3. Check browser console for any CORS errors

## 🔍 Troubleshooting

### CORS Errors?
The backend already allows all origins (`allow_origins=["*"]`), so CORS should work fine.

### Connection Refused?
- Check Railway logs to ensure backend is running
- Verify the Railway URL is correct
- Make sure Railway service is not sleeping (free tier sleeps after inactivity)

### Frontend Still Using Old URL?
- Clear browser cache
- Hard refresh: `Ctrl+Shift+R` (Windows) or `Cmd+Shift+R` (Mac)
- Check browser console for errors

## 📝 Current Configuration

**Backend URL:** `https://insurance-ai-pipeline-document-processing-production.up.railway.app`

**Frontend Logic:**
- If `hostname === 'localhost'` → Use `http://localhost:8000`
- Otherwise → Use Railway URL

This means:
- ✅ Local development → Connects to localhost backend
- ✅ Production (Vercel) → Connects to Railway backend

## 🎯 Next Steps

1. **Test locally:**
   ```bash
   cd frontend
   npm run dev
   ```
   Visit http://localhost:3000 and test login/register

2. **Deploy to Vercel** (optional but recommended):
   - Get free hosting
   - Automatic HTTPS
   - Better performance

3. **Verify everything works:**
   - Register a new user
   - Login
   - Upload PDFs
   - Check Railway logs for processing

---

## 🆘 Need Help?

If you encounter issues:
1. Check Railway logs for backend errors
2. Check browser console for frontend errors
3. Verify Railway URL is accessible: `curl https://insurance-ai-pipeline-document-processing-production.up.railway.app/health`
