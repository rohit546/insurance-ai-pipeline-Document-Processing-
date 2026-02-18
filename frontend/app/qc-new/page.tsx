"use client";
import { useState, useCallback, useEffect } from "react";
import { useRouter } from "next/navigation";
import { useAuth } from "@/context/AuthContext";

export default function QCNewPage() {
  const router = useRouter();
  const { user } = useAuth();
  const [plCertificateFile, setPlCertificateFile] = useState<File | null>(null);
  const [glCertificateFile, setGlCertificateFile] = useState<File | null>(null);
  const [acordCertificateFile, setAcordCertificateFile] = useState<File | null>(null);
  const [glAcordCertificateFile, setGlAcordCertificateFile] = useState<File | null>(null);
  const [policyFile, setPolicyFile] = useState<File | null>(null);
  const [uploading, setUploading] = useState(false);
  const [uploadId, setUploadId] = useState<string>("");
  const [error, setError] = useState<string>("");
  const [results, setResults] = useState<any>(null);
  const [fetchingResults, setFetchingResults] = useState(false);
  const [apiUrl, setApiUrl] = useState<string>('http://localhost:8000');

  // Set API URL on client side only (works for both local and Vercel)
  useEffect(() => {
    const isVercel = typeof window !== 'undefined' && window.location.hostname !== 'localhost';
    const url = isVercel
      ? (process.env.NEXT_PUBLIC_API_URL || 'https://deployment-production-7739.up.railway.app')
      : 'http://localhost:8000';
    setApiUrl(url);
  }, []);

  const handlePlCertDrop = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    const file = e.dataTransfer.files[0];
    if (file?.type === "application/pdf") {
      setPlCertificateFile(file);
      setError("");
    }
  }, []);

  const handleGlCertDrop = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    const file = e.dataTransfer.files[0];
    if (file?.type === "application/pdf") {
      setGlCertificateFile(file);
      setError("");
    }
  }, []);

  const handleAcordCertDrop = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    const file = e.dataTransfer.files[0];
    if (file?.type === "application/pdf") {
      setAcordCertificateFile(file);
      setError("");
    }
  }, []);

  const handleGlAcordCertDrop = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    const file = e.dataTransfer.files[0];
    if (file?.type === "application/pdf") {
      setGlAcordCertificateFile(file);
      setError("");
    }
  }, []);

  const handlePolicyDrop = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    const file = e.dataTransfer.files[0];
    if (file?.type === "application/pdf") {
      setPolicyFile(file);
      setError("");
    }
  }, []);

  const handleUpload = async () => {
    if (!plCertificateFile && !glCertificateFile) {
      // ACORD is optional for now and does not replace PL/GL requirement
      setError("Please upload at least one certificate (PL or GL) and the policy PDF");
      return;
    }

    if (!policyFile) {
      setError("Please upload the policy PDF");
      return;
    }

    setUploading(true);
    setError("");
    setResults(null);

    try {
      const formData = new FormData();
      if (plCertificateFile) {
        formData.append("pl_certificate_pdf", plCertificateFile);
      }
      if (glCertificateFile) {
        formData.append("gl_certificate_pdf", glCertificateFile);
      }
      if (acordCertificateFile) {
        formData.append("acord_certificate_pdf", acordCertificateFile);
      }
      if (glAcordCertificateFile) {
        formData.append("gl_acord_certificate_pdf", glAcordCertificateFile);
      }
      formData.append("policy_pdf", policyFile);
      formData.append("username", user?.username || "user");

      // Add timeout for large files (matching summary page pattern)
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), 120000); // 120 seconds

      try {
        const response = await fetch(`${apiUrl}/qc-new/upload-unified`, {
          method: "POST",
          body: formData,
          headers: {
            'ngrok-skip-browser-warning': 'true',
            'X-User-ID': user?.username || 'user',
          },
          signal: controller.signal,
        });

        clearTimeout(timeoutId);

        // Parse response with better error handling (matching summary page)
        let data;
        try {
          data = await response.json();
        } catch (parseError) {
          const text = await response.text();
          console.error('Failed to parse JSON. Response text:', text);
          console.error('Response status:', response.status);
          throw new Error(`Invalid response from server (${response.status}): ${text}`);
        }

        if (!response.ok) {
          console.error('Backend error - Status:', response.status);
          console.error('Backend error - Response:', data);
          throw new Error(data.message || data.error || data.detail || `Upload failed (${response.status})`);
        }
        
        // Verify we got the required data
        if (!data.upload_id) {
          throw new Error("Server did not return upload_id. Task may not have been queued.");
        }
        
        if (!data.task_id) {
          console.warn("Warning: Server did not return task_id. Celery task may not have been queued.");
        }
        
        const newUploadId = data.upload_id;
        setUploadId(newUploadId);
        setError("");
        
        console.log("✅ QC upload successful:", {
          upload_id: newUploadId,
          task_id: data.task_id,
          message: data.message
        });
        
        // Auto-redirect to results page
        router.push(`/qc-new-results?id=${newUploadId}`);
      } catch (fetchError: any) {
        clearTimeout(timeoutId);
        if (fetchError.name === 'AbortError') {
          throw new Error('Upload timeout: Large files may take up to 2 minutes to process. Please try again or use smaller files.');
        }
        throw fetchError;
      }
    } catch (err: any) {
      const errorMessage = err.message || "Failed to upload files";
      setError(errorMessage);
      console.error('Upload error:', err);
      setUploading(false);
    }
  };

  const fetchResults = async () => {
    if (!uploadId) {
      setError("No upload ID available");
      return;
    }

    setFetchingResults(true);
    setError("");

    try {
      const response = await fetch(`${apiUrl}/qc-new/unified-results/${uploadId}`);
      
      if (!response.ok) {
        const err = await response.json();
        throw new Error(err.detail || "Failed to fetch results");
      }

      const data = await response.json();
      setResults(data);
    } catch (err: any) {
      setError(err.message || "Failed to fetch results");
    } finally {
      setFetchingResults(false);
    }
  };

  return (
    <div className="min-h-screen bg-gradient-to-br from-blue-50 via-white to-purple-50 p-8">
      <div className="max-w-5xl mx-auto">
        {/* Header */}
        <div className="mb-8">
          <button
            onClick={() => router.push("/dashboard")}
            className="mb-4 text-blue-600 hover:text-blue-800 flex items-center gap-2"
          >
            ← Back to Dashboard
          </button>
          <div className="text-center">
            <h1 className="text-4xl font-bold text-gray-800 mb-2">📋 QC Review System</h1>
            <p className="text-gray-600">Upload PL certificate, GL certificate, and policy for quality control review</p>
            <p className="text-sm text-gray-500 mt-2">At least one certificate (PL or GL) is required</p>
          </div>
        </div>

        {/* Upload Boxes */}
        <div className="space-y-6 mb-6">
          {/* PL Certificate Upload */}
          <div
            onDrop={handlePlCertDrop}
            onDragOver={(e) => e.preventDefault()}
            className="bg-white rounded-xl shadow-md p-8 border-2 border-dashed border-gray-300 hover:border-blue-400 transition cursor-pointer"
          >
            <div className="text-center">
              <div className="text-5xl mb-4">🏠</div>
              <h3 className="text-xl font-semibold mb-2">PL CERTIFICATE PDF (Optional)</h3>
              <p className="text-gray-600 mb-4">
                {plCertificateFile ? plCertificateFile.name : "Drop PL certificate PDF here or click to browse"}
              </p>
              <input
                type="file"
                accept="application/pdf"
                onChange={(e) => {
                  const file = e.target.files?.[0];
                  if (file) setPlCertificateFile(file);
                }}
                className="hidden"
                id="pl-cert-upload"
              />
              <label
                htmlFor="pl-cert-upload"
                className="inline-block px-6 py-2 bg-blue-500 text-white rounded-lg hover:bg-blue-600 cursor-pointer"
              >
                Choose PL Certificate
              </label>
            </div>
          </div>

          {/* GL Certificate Upload */}
          <div
            onDrop={handleGlCertDrop}
            onDragOver={(e) => e.preventDefault()}
            className="bg-white rounded-xl shadow-md p-8 border-2 border-dashed border-gray-300 hover:border-green-400 transition cursor-pointer"
          >
            <div className="text-center">
              <div className="text-5xl mb-4">⚖️</div>
              <h3 className="text-xl font-semibold mb-2">GL CERTIFICATE PDF (Optional)</h3>
              <p className="text-gray-600 mb-4">
                {glCertificateFile ? glCertificateFile.name : "Drop GL certificate PDF here or click to browse"}
              </p>
              <input
                type="file"
                accept="application/pdf"
                onChange={(e) => {
                  const file = e.target.files?.[0];
                  if (file) setGlCertificateFile(file);
                }}
                className="hidden"
                id="gl-cert-upload"
              />
              <label
                htmlFor="gl-cert-upload"
                className="inline-block px-6 py-2 bg-green-500 text-white rounded-lg hover:bg-green-600 cursor-pointer"
              >
                Choose GL Certificate
              </label>
            </div>
          </div>

          {/* ACORD Certificate Upload (Optional) */}
          <div
            onDrop={handleAcordCertDrop}
            onDragOver={(e) => e.preventDefault()}
            className="bg-white rounded-xl shadow-md p-8 border-2 border-dashed border-gray-300 hover:border-indigo-400 transition cursor-pointer"
          >
            <div className="text-center">
              <div className="text-5xl mb-4">📄</div>
              <h3 className="text-xl font-semibold mb-2">ACORD CERTIFICATE PDF (Property - Optional)</h3>
              <p className="text-gray-600 mb-4">
                {acordCertificateFile ? acordCertificateFile.name : "Drop ACORD certificate PDF here or click to browse"}
              </p>
              <input
                type="file"
                accept="application/pdf"
                onChange={(e) => {
                  const file = e.target.files?.[0];
                  if (file) setAcordCertificateFile(file);
                }}
                className="hidden"
                id="acord-cert-upload"
              />
              <label
                htmlFor="acord-cert-upload"
                className="inline-block px-6 py-2 bg-indigo-500 text-white rounded-lg hover:bg-indigo-600 cursor-pointer"
              >
                Choose ACORD Certificate
              </label>
            </div>
          </div>

          {/* GL ACORD Certificate Upload (Optional) */}
          <div
            onDrop={handleGlAcordCertDrop}
            onDragOver={(e) => e.preventDefault()}
            className="bg-white rounded-xl shadow-md p-8 border-2 border-dashed border-gray-300 hover:border-teal-400 transition cursor-pointer"
          >
            <div className="text-center">
              <div className="text-5xl mb-4">📋</div>
              <h3 className="text-xl font-semibold mb-2">GL ACORD CERTIFICATE PDF (Optional)</h3>
              <p className="text-gray-600 mb-4">
                {glAcordCertificateFile ? glAcordCertificateFile.name : "Drop GL ACORD certificate PDF here or click to browse"}
              </p>
              <input
                type="file"
                accept="application/pdf"
                onChange={(e) => {
                  const file = e.target.files?.[0];
                  if (file) setGlAcordCertificateFile(file);
                }}
                className="hidden"
                id="gl-acord-cert-upload"
              />
              <label
                htmlFor="gl-acord-cert-upload"
                className="inline-block px-6 py-2 bg-teal-500 text-white rounded-lg hover:bg-teal-600 cursor-pointer"
              >
                Choose GL ACORD Certificate
              </label>
            </div>
          </div>

          {/* Policy Upload */}
          <div
            onDrop={handlePolicyDrop}
            onDragOver={(e) => e.preventDefault()}
            className="bg-white rounded-xl shadow-md p-8 border-2 border-dashed border-gray-300 hover:border-purple-400 transition cursor-pointer"
          >
            <div className="text-center">
              <div className="text-5xl mb-4">📄</div>
              <h3 className="text-xl font-semibold mb-2">POLICY PDF</h3>
              <p className="text-gray-600 mb-4">
                {policyFile ? policyFile.name : "Drop PDF here or click to browse"}
              </p>
              <input
                type="file"
                accept="application/pdf"
                onChange={(e) => {
                  const file = e.target.files?.[0];
                  if (file) setPolicyFile(file);
                }}
                className="hidden"
                id="policy-upload"
              />
              <label
                htmlFor="policy-upload"
                className="inline-block px-6 py-2 bg-purple-500 text-white rounded-lg hover:bg-purple-600 cursor-pointer"
              >
                Choose File
              </label>
            </div>
          </div>
        </div>

        {/* Start QC Button */}
        <button
          onClick={handleUpload}
          disabled={uploading || (!plCertificateFile && !glCertificateFile) || !policyFile}
          className="w-full py-4 bg-gradient-to-r from-blue-500 to-purple-500 text-white text-lg font-semibold rounded-xl shadow-lg hover:shadow-xl disabled:opacity-50 disabled:cursor-not-allowed transition"
        >
          {uploading ? "▶ Processing..." : "▶ START QC REVIEW"}
        </button>

        {/* Upload ID Display */}
        {uploadId && (
          <div className="mt-6 bg-green-50 border border-green-200 rounded-lg p-4">
            <p className="text-green-800">
              ✅ Upload successful! ID: <strong>{uploadId}</strong>
            </p>
            <button
              onClick={fetchResults}
              disabled={fetchingResults}
              className="mt-3 px-6 py-2 bg-green-600 text-white rounded-lg hover:bg-green-700 disabled:opacity-50"
            >
              {fetchingResults ? "Fetching..." : "Fetch Results"}
            </button>
          </div>
        )}

        {/* Error Display */}
        {error && (
          <div className="mt-6 bg-red-50 border border-red-200 rounded-lg p-4">
            <p className="text-red-800">❌ {error}</p>
          </div>
        )}

        {/* Results Display */}
        {results && (
          <div className="mt-6 bg-white rounded-xl shadow-lg p-6">
            <h2 className="text-2xl font-bold mb-4">QC Results</h2>
            
            {/* Summary */}
            {results.summary && (
              <div className="mb-6 p-4 bg-gray-50 rounded-lg">
                <h3 className="font-semibold mb-2">Summary</h3>
                <div className="grid grid-cols-2 gap-4">
                  <div>
                    <p className="text-sm text-gray-600">Core Fields</p>
                    <p className="text-lg">
                      <span className="text-green-600">✓ {results.summary.core?.match || 0}</span> / 
                      <span className="text-red-600"> ✗ {results.summary.core?.mismatch || 0}</span> / 
                      <span className="text-yellow-600"> ? {results.summary.core?.not_found || 0}</span>
                    </p>
                  </div>
                  <div>
                    <p className="text-sm text-gray-600">Coverages</p>
                    <p className="text-lg">
                      <span className="text-green-600">✓ {results.summary.coverage?.match || 0}</span> / 
                      <span className="text-red-600"> ✗ {results.summary.coverage?.mismatch || 0}</span> / 
                      <span className="text-yellow-600"> ? {results.summary.coverage?.not_found || 0}</span>
                    </p>
                  </div>
                </div>
              </div>
            )}

            {/* Full Results JSON */}
            <details className="mt-4">
              <summary className="cursor-pointer font-semibold text-blue-600 hover:text-blue-800">
                View Full JSON Results
              </summary>
              <pre className="mt-2 p-4 bg-gray-100 rounded-lg overflow-auto text-xs">
                {JSON.stringify(results, null, 2)}
              </pre>
            </details>
          </div>
        )}
      </div>
    </div>
  );
}

