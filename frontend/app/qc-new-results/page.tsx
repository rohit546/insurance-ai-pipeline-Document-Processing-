"use client";
import { useEffect, useState, Suspense } from "react";
import { useRouter, useSearchParams } from "next/navigation";

function QCNewResultsContent() {
  const router = useRouter();
  const searchParams = useSearchParams();
  const uploadId = searchParams.get("id");

  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string>("");
  const [results, setResults] = useState<any>(null);
  const [pdfScale, setPdfScale] = useState(0.88);
  // PDF viewer state - must be declared before any early returns
  const [activePdf, setActivePdf] = useState<"pl" | "gl" | "acord" | "gl_acord" | "policy" | "legacy">("policy");
  const [apiUrl, setApiUrl] = useState<string>('');
  const [statusMessage, setStatusMessage] = useState<string>('Initializing...');

  // Set API URL on client side only (works for both local and Vercel)
  useEffect(() => {
    const isVercel = typeof window !== 'undefined' && window.location.hostname !== 'localhost';
    const url = isVercel
      ? (process.env.NEXT_PUBLIC_API_URL || 'https://deployment-production-7739.up.railway.app')
      : 'http://localhost:8000';
    console.log(`[QC Results] API URL set to: ${url} (isVercel: ${isVercel})`);
    setApiUrl(url);
  }, []);

  useEffect(() => {
    // CRITICAL: Don't fetch if we already have results loaded
    if (results) {
      return;
    }
    
    if (!uploadId || !apiUrl) {
      if (!uploadId) {
        setError("No upload ID provided");
        setLoading(false);
      }
      return;
    }

    let retryCount = 0;
    const maxRetries = 600; // 600 retries * 3 seconds = 30 minutes max
    let timeoutId: NodeJS.Timeout | null = null;
    let isCancelled = false; // Track if effect was cancelled

    // Fetch results with retry logic
    const fetchResults = async () => {
      // Double-check: if results were loaded or cancelled, stop
      if (isCancelled || results) {
        return;
      }
      
      try {
        setStatusMessage(`Checking results... (attempt ${retryCount + 1}/${maxRetries})`);
        
        console.log(`[QC Results] Fetching: ${apiUrl}/qc-new/unified-results/${uploadId}`);
        const response = await fetch(`${apiUrl}/qc-new/unified-results/${uploadId}`, {
          headers: {
            'ngrok-skip-browser-warning': 'true',
          },
        });
        console.log(`[QC Results] Response status: ${response.status} ${response.statusText}`);
        
        // Check again after fetch (component might have unmounted or results loaded)
        if (isCancelled || results) {
          return;
        }
        
        if (!response.ok) {
          // Try to parse error response
          let errorMsg = `HTTP ${response.status}`;
          try {
            const errorData = await response.json();
            errorMsg = errorData.detail || errorData.error || errorMsg;
          } catch {
            // If JSON parse fails, use status text
            errorMsg = response.statusText || errorMsg;
          }
          
          // For 404 or 500, retry (might be processing)
          if ((response.status === 404 || response.status === 500) && retryCount < maxRetries && !isCancelled) {
            retryCount++;
            setStatusMessage(`Results not ready yet... (${retryCount}/${maxRetries})`);
            timeoutId = setTimeout(fetchResults, 3000);
            return;
          }
          
          throw new Error(`Failed to fetch results: ${errorMsg}`);
        }

        const data = await response.json();
        
        // Check again after parsing (results might have been set)
        if (isCancelled || results) {
          return;
        }
        
        if (!data.success || !data.merged) {
          // Check if it's the "not ready" case or if there's an error
          if (data.error && data.error.includes("not ready")) {
            // Results not ready, retry after 3 seconds (with limit)
            if (retryCount < maxRetries && !isCancelled) {
              retryCount++;
              setStatusMessage(`Processing... (${retryCount}/${maxRetries})`);
              timeoutId = setTimeout(fetchResults, 3000);
              return;
            } else {
              // Max retries reached
              setError("Results are taking longer than expected (30+ minutes). The QC task may have failed. Please check Railway logs or try uploading again.");
              setLoading(false);
              return;
            }
          } else {
            // Some other error occurred
            throw new Error(data.error || "Unknown error occurred");
          }
        }

        // Success! We have results - STOP POLLING
        setResults(data.merged || data);
        setLoading(false);
        // Don't schedule any more retries
        return;
      } catch (err: any) {
        console.error(`[QC Results] Fetch error:`, err);
        // Network errors or other failures
        // Only retry if we don't have results yet and not cancelled
        if (retryCount < maxRetries && !isCancelled && !results) {
          retryCount++;
          setStatusMessage(`Connection error, retrying... (${retryCount}/${maxRetries})`);
          timeoutId = setTimeout(fetchResults, 3000);
        } else {
          // Only set error if we don't already have results
          if (!results) {
            console.error(`[QC Results] Final error after ${retryCount} retries:`, err);
            setError(err.message || "Failed to load results after multiple attempts. Please check if the backend is running on Railway.");
            setLoading(false);
          }
        }
      }
    };

    fetchResults();

    // Cleanup function to clear timeout on unmount or when dependencies change
    return () => {
      isCancelled = true; // Mark as cancelled
      if (timeoutId) {
        clearTimeout(timeoutId);
      }
    };
  }, [uploadId, apiUrl, results]);

  // Update activePdf when results change
  useEffect(() => {
    if (results) {
      const hasPlGlFormat = results.pl_certificate !== undefined || results.gl_certificate !== undefined;
      const hasPl = !!results.pl_certificate || !!results.pl_validations;
      const hasGl = !!results.gl_certificate || !!results.gl_validations;
      const hasAcord = !!results.acord_certificate;
      const hasGlAcord = !!results.gl_acord_certificate;
      const hasLegacyCert = !!results.certificate && !hasPlGlFormat;
      
      // Set initial PDF view based on available certificates
      if (hasPl) {
        setActivePdf("pl");
      } else if (hasGl) {
        setActivePdf("gl");
      } else if (hasAcord) {
        setActivePdf("acord");
      } else if (hasLegacyCert) {
        setActivePdf("legacy");
      } else {
        setActivePdf("policy");
      }
    }
  }, [results]);

  const handleZoomIn = () => setPdfScale((prev) => Math.min(prev + 0.1, 2.0));
  const handleZoomOut = () => setPdfScale((prev) => Math.max(prev - 0.1, 0.3));

  if (loading) {
    return (
      <div className="min-h-screen bg-gradient-to-br from-blue-50 via-white to-purple-50 flex items-center justify-center">
        <div className="text-center">
          <div className="inline-block animate-spin rounded-full h-16 w-16 border-t-4 border-b-4 border-blue-500 mb-4"></div>
          <h2 className="text-2xl font-semibold text-gray-800 mb-2">⏳ Processing your documents...</h2>
          <p className="text-gray-600 mb-2">{statusMessage}</p>
          <p className="text-sm text-gray-500">This may take 1-30 minutes. Please wait.</p>
          <p className="text-sm text-blue-600 mt-4">💡 The system is performing OCR extraction and LLM analysis on your policy PDF.</p>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="min-h-screen bg-gradient-to-br from-blue-50 via-white to-purple-50 p-8">
        <div className="max-w-2xl mx-auto">
          <div className="bg-red-50 border border-red-200 rounded-lg p-6">
            <h2 className="text-xl font-semibold text-red-800 mb-2">❌ Error</h2>
            <p className="text-red-700">{error}</p>
            <button
              onClick={() => router.push("/dashboard")}
              className="mt-4 px-4 py-2 bg-red-600 text-white rounded hover:bg-red-700"
            >
              ← Back to Dashboard
            </button>
          </div>
        </div>
      </div>
    );
  }

  if (!results) {
    return (
      <div className="min-h-screen bg-gradient-to-br from-blue-50 via-white to-purple-50 p-8">
        <div className="max-w-2xl mx-auto">
          <div className="bg-yellow-50 border border-yellow-200 rounded-lg p-6">
            <h2 className="text-xl font-semibold text-yellow-800 mb-2">⏳ Results not ready yet</h2>
            <p className="text-yellow-700">Please wait while we process your documents...</p>
          </div>
        </div>
      </div>
    );
  }

  // Handle both new (PL+GL) and legacy (single certificate) formats
  const hasPlGlFormat = results.pl_certificate !== undefined || results.gl_certificate !== undefined;
  
  const plCertData = results.pl_certificate || {};
  const glCertData = results.gl_certificate || {};
  const acordCertData = results.acord_certificate || {};
  const certData = results.certificate || plCertData || {}; // Legacy support
  
  const plCoreValidations = results.pl_validations?.core_validations || {};
  const plCoverageValidations = results.pl_validations?.coverage_validations || {};
  // Extract additional_interests separately for dedicated display
  const plAdditionalInterests = plCoverageValidations?.additional_interests || [];
  // Create a filtered version without additional_interests for the regular coverage loop
  const plCoverageValidationsFiltered = { ...plCoverageValidations };
  delete plCoverageValidationsFiltered.additional_interests;
  const glCoreValidations = results.gl_validations?.core_validations || {};
  const glAddressValidations = results.gl_validations?.address_validations || [];
  const glCoveragePresenceValidations = results.gl_validations?.coverage_presence_validations || [];
  const glCoverageValidations = results.gl_validations?.coverage_validations || {};
  const acordPlCoreComparisons =
    results.acord_pl_comparisons?.core_fields || {};
  const acordPlCoverageComparisons =
    results.acord_pl_comparisons?.coverages || [];
  const glAcordCertData = results.gl_acord_certificate || {};
  const glAcordCoreComparisons =
    results.gl_acord_comparisons?.core_fields || {};
  const glAcordCoverageComparisons =
    results.gl_acord_comparisons?.coverages || [];
  
  // Legacy support
  const coreValidations = results.core_validations || plCoreValidations || {};
  const coverageValidations = results.coverage_validations || plCoverageValidations || {};
  
  // Determine which certificates are available
  const hasPl = !!results.pl_certificate || !!results.pl_validations;
  const hasGl = !!results.gl_certificate || !!results.gl_validations;
  const hasAcord = !!results.acord_certificate;
  const hasGlAcord = !!results.gl_acord_certificate;
  const hasLegacyCert = !!results.certificate && !hasPlGlFormat;

  return (
    <div className="min-h-screen bg-gray-100">
      {/* Header */}
      <div className="bg-white shadow-sm border-b">
        <div className="max-w-[98%] mx-auto px-4 py-4 flex items-center justify-between">
          <div className="flex items-center gap-4">
            <button
              onClick={() => router.push("/dashboard")}
              className="text-blue-600 hover:text-blue-800 font-medium"
            >
              ← Back to Dashboard
            </button>
            <div className="h-6 w-px bg-gray-300"></div>
            <h1 className="text-xl font-bold text-gray-800">📋 QC Review Results</h1>
            <span className="text-sm text-gray-500">Upload ID: {uploadId}</span>
          </div>
          <div className="flex gap-2">
            <button className="px-4 py-2 bg-blue-600 text-white rounded hover:bg-blue-700 flex items-center gap-2">
              📥 Download Report
            </button>
            <button
              onClick={() => router.push("/qc-new")}
              className="px-4 py-2 bg-gray-200 text-gray-700 rounded hover:bg-gray-300"
            >
              ← New Upload
            </button>
          </div>
        </div>
      </div>

      {/* Main Content: Split View */}
      <div className="flex h-[calc(100vh-80px)]">
        {/* Left: PDF Viewer with Tabs */}
        <div className="w-1/2 bg-gray-800 flex flex-col">
          {/* PDF Tabs and Controls */}
          <div className="bg-gray-900 px-4 py-3">
            {/* Tabs */}
            <div className="flex gap-2 mb-3">
              {hasPl && (
                <button
                  onClick={() => setActivePdf("pl")}
                  className={`px-4 py-2 rounded-t-lg font-medium transition ${
                    activePdf === "pl"
                      ? "bg-blue-600 text-white"
                      : "bg-gray-700 text-gray-300 hover:bg-gray-600"
                  }`}
                >
                  🏠 PL Certificate
                </button>
              )}
              {hasGl && (
                <button
                  onClick={() => setActivePdf("gl")}
                  className={`px-4 py-2 rounded-t-lg font-medium transition ${
                    activePdf === "gl"
                      ? "bg-green-600 text-white"
                      : "bg-gray-700 text-gray-300 hover:bg-gray-600"
                  }`}
                >
                  ⚖️ GL Certificate
                </button>
              )}
              {hasLegacyCert && (
                <button
                  onClick={() => setActivePdf("legacy")}
                  className={`px-4 py-2 rounded-t-lg font-medium transition ${
                    activePdf === "legacy"
                      ? "bg-blue-600 text-white"
                      : "bg-gray-700 text-gray-300 hover:bg-gray-600"
                  }`}
                >
                  📄 Certificate
                </button>
              )}
              {hasAcord && (
                <button
                  onClick={() => setActivePdf("acord")}
                  className={`px-4 py-2 rounded-t-lg font-medium transition ${
                    activePdf === "acord"
                      ? "bg-indigo-600 text-white"
                      : "bg-gray-700 text-gray-300 hover:bg-gray-600"
                  }`}
                >
                  📄 ACORD Certificate
                </button>
              )}
              {hasGlAcord && (
                <button
                  onClick={() => setActivePdf("gl_acord")}
                  className={`px-4 py-2 rounded-t-lg font-medium transition ${
                    activePdf === "gl_acord"
                      ? "bg-teal-600 text-white"
                      : "bg-gray-700 text-gray-300 hover:bg-gray-600"
                  }`}
                >
                  📋 GL ACORD Certificate
                </button>
              )}
              <button
                onClick={() => setActivePdf("policy")}
                className={`px-4 py-2 rounded-t-lg font-medium transition ${
                  activePdf === "policy"
                    ? "bg-purple-600 text-white"
                    : "bg-gray-700 text-gray-300 hover:bg-gray-600"
                }`}
              >
                📄 Policy
              </button>
            </div>
            
            {/* Zoom Controls */}
            <div className="flex items-center justify-between">
              <span className="text-white font-medium text-sm">
                {activePdf === "pl" && "🏠 PL Certificate"}
                {activePdf === "gl" && "⚖️ GL Certificate"}
                {activePdf === "legacy" && "📄 Certificate"}
                {activePdf === "acord" && "📄 ACORD Certificate"}
                {activePdf === "gl_acord" && "📋 GL ACORD Certificate"}
                {activePdf === "policy" && "📄 Policy"}
              </span>
              <div className="flex items-center gap-2">
                <button
                  onClick={handleZoomOut}
                  className="px-3 py-1 bg-gray-700 text-white rounded hover:bg-gray-600"
                >
                  -
                </button>
                <span className="text-white text-sm">{Math.round(pdfScale * 100)}%</span>
                <button
                  onClick={handleZoomIn}
                  className="px-3 py-1 bg-gray-700 text-white rounded hover:bg-gray-600"
                >
                  +
                </button>
              </div>
            </div>
          </div>

          {/* PDF Display */}
          <div className="flex-1 overflow-auto p-4 flex items-start justify-center">
            <iframe
              src={`${apiUrl}/qc-new/pdf/${uploadId}/${
                activePdf === "pl"
                  ? "pl_certificate"
                  : activePdf === "gl"
                  ? "gl_certificate"
                  : activePdf === "acord"
                  ? "acord_certificate"
                  : activePdf === "gl_acord"
                  ? "gl_acord_certificate"
                  : activePdf === "legacy"
                  ? "certificate"
                  : "policy"
              }#zoom=${Math.round(pdfScale * 100)}`}
              className="bg-white shadow-lg"
              style={{
                width: "100%",
                height: "100%",
                border: "none",
              }}
              title={`${activePdf === "pl" ? "PL Certificate" : activePdf === "gl" ? "GL Certificate" : activePdf === "legacy" ? "Certificate" : "Policy"} PDF`}
            />
          </div>
        </div>

        {/* Right: Fetched Information */}
        <div className="w-1/2 bg-white overflow-auto">
          <div className="p-8">
            <h2 className="text-3xl font-bold text-gray-800 mb-8">Validation Results</h2>

            {/* PL Results Section */}
            {hasPl && (
              <div className="mb-10">
                <h3 className="text-2xl font-bold text-blue-800 mb-6 pb-3 border-b-2 border-blue-300">🏠 PL Certificate Results</h3>
                
                {/* Only show 2-way comparison (PL vs Policy) if 3-way comparison data is NOT present */}
                {Object.keys(acordPlCoreComparisons).length === 0 && acordPlCoverageComparisons.length === 0 && (
                  <>
                    {/* PL Core Validations */}
                    {Object.keys(plCoreValidations).length > 0 && (
                      <div className="mb-8">
                        <h4 className="text-xl font-bold text-gray-800 mb-4">✅ Core Field Validation</h4>
                        <div className="space-y-5">
                          {Object.entries(plCoreValidations).map(([field, data]: [string, any]) => (
                            <ValidationRow
                              key={field}
                              label={field.replace(/_/g, " ").toUpperCase()}
                              certValue={data.certificate_value}
                              policyValue={data.policy_value}
                              status={data.status}
                              evidence={data.evidence}
                            />
                          ))}
                        </div>
                      </div>
                    )}

                    {/* PL Coverage Validations */}
                    {Object.keys(plCoverageValidationsFiltered).length > 0 && (
                      <div className="mb-8">
                        <h4 className="text-xl font-bold text-gray-800 mb-4">🏢 Coverage Validation</h4>
                        <div className="space-y-6">
                          {Object.entries(plCoverageValidationsFiltered).map(([coverage, items]: [string, any]) => {
                            if (!Array.isArray(items) || items.length === 0) return null;
                            return (
                              <div key={coverage} className="border-2 rounded-lg p-6 bg-blue-50 shadow-sm">
                                <h5 className="text-lg font-bold text-gray-800 mb-4 capitalize">
                                  {coverage.replace(/_/g, " ")}
                                </h5>
                                <div className="space-y-4">
                                  {items.map((item: any, idx: number) => (
                                    <CoverageItem key={idx} item={item} />
                                  ))}
                                </div>
                              </div>
                            );
                          })}
                        </div>
                      </div>
                    )}

                    {/* Additional Interests Validation - Show in 2-way comparison (Policy + Cert only) */}
                    {plAdditionalInterests.length > 0 && (
                      <div className="mb-8">
                        <h4 className="text-xl font-bold text-gray-800 mb-4">👥 Additional Interests Validation</h4>
                        <div className="space-y-6">
                          {plAdditionalInterests.map((interest: any, idx: number) => (
                            <CoverageItem key={`additional-interest-2way-${idx}`} item={interest} />
                          ))}
                        </div>
                      </div>
                    )}
                    {plAdditionalInterests.length === 0 && (
                      <div className="mb-8">
                        <h4 className="text-xl font-bold text-gray-800 mb-4">👥 Additional Interests Validation</h4>
                        <div className="bg-gray-50 border-2 border-gray-200 rounded-lg p-6">
                          <p className="text-gray-600">No additional interests found on certificate.</p>
                        </div>
                      </div>
                    )}

                  </>
                )}

              </div>
            )}

            {/* GL Results Section */}
            {hasGl && (
              <div className="mb-10">
                <h3 className="text-2xl font-bold text-green-800 mb-6 pb-3 border-b-2 border-green-300">⚖️ GL Certificate Results</h3>
                
                {/* GL Certificate Extracted Fields */}
                {glCertData && Object.keys(glCertData).length > 0 && (
                  <div className="mb-8">
                    <h4 className="text-xl font-bold text-gray-800 mb-4">📋 Certificate Information</h4>
                    <div className="bg-green-50 border-2 border-green-200 rounded-lg p-6 space-y-3">
                      {glCertData.policy_number && (
                        <div className="flex justify-between">
                          <span className="font-semibold text-gray-700">Policy Number:</span>
                          <span className="text-gray-900 font-bold">{glCertData.policy_number}</span>
                        </div>
                      )}
                      {glCertData.effective_date && (
                        <div className="flex justify-between">
                          <span className="font-semibold text-gray-700">Effective Date:</span>
                          <span className="text-gray-900 font-bold">{glCertData.effective_date}</span>
                        </div>
                      )}
                      {glCertData.expiration_date && (
                        <div className="flex justify-between">
                          <span className="font-semibold text-gray-700">Expiration Date:</span>
                          <span className="text-gray-900 font-bold">{glCertData.expiration_date}</span>
                        </div>
                      )}
                      {glCertData.insured_name && (
                        <div className="flex justify-between">
                          <span className="font-semibold text-gray-700">Insured Name:</span>
                          <span className="text-gray-900 font-bold">{glCertData.insured_name}</span>
                        </div>
                      )}
                      {glCertData.location_address && (
                        <div className="flex justify-between">
                          <span className="font-semibold text-gray-700">Location Address:</span>
                          <span className="text-gray-900 font-bold">{glCertData.location_address}</span>
                        </div>
                      )}
                      {glCertData.mailing_address && (
                        <div className="flex justify-between">
                          <span className="font-semibold text-gray-700">Mailing Address:</span>
                          <span className="text-gray-900 font-bold">{glCertData.mailing_address}</span>
                        </div>
                      )}
                    </div>
                  </div>
                )}

                {/* Only show 2-way GL comparisons if 3-way GL ACORD comparison data is NOT present */}
                {Object.keys(glAcordCoreComparisons).length === 0 && glAcordCoverageComparisons.length === 0 && (
                  <>
                    {/* GL Address Validations */}
                    {glAddressValidations.length > 0 && (
                      <div className="mb-8">
                        <h4 className="text-xl font-bold text-gray-800 mb-4">📍 Address Validation</h4>
                        <div className="space-y-5">
                          {glAddressValidations.map((item: any, idx: number) => (
                            <ValidationRow
                              key={idx}
                              label={item.address_type?.replace(/_/g, " ").toUpperCase() || "Address"}
                              certValue={item.cert_value || "N/A"}
                              policyValue={item.policy_value || "Not found"}
                              status={item.status}
                              evidence={item.evidence}
                            />
                          ))}
                        </div>
                      </div>
                    )}

                    {/* GL Coverage Presence Validations */}
                    {glCoveragePresenceValidations.length > 0 && (
                      <div className="mb-8">
                        <h4 className="text-xl font-bold text-gray-800 mb-4">✅ Coverage Presence Validation</h4>
                        <div className="space-y-5">
                          {glCoveragePresenceValidations.map((item: any, idx: number) => (
                            <div key={idx} className="border-2 rounded-lg p-6 bg-green-50 shadow-sm">
                              <div className="flex justify-between items-start mb-4">
                                <div>
                                  <div className="font-bold text-gray-800 text-xl mb-2">{item.coverage_name}</div>
                                  <div className="text-sm text-gray-600">
                                    Certificate Policy #: <span className="font-semibold">{item.cert_policy_number}</span>
                                  </div>
                                  {item.policy_policy_number && (
                                    <div className="text-sm text-gray-600">
                                      Policy Policy #: <span className="font-semibold">{item.policy_policy_number}</span>
                                    </div>
                                  )}
                                </div>
                                <span className={`px-6 py-3 rounded-full text-xl font-bold ${
                                  item.status === "PRESENT" ? "bg-green-100 text-green-700" : 
                                  "bg-red-100 text-red-700"
                                }`}>
                                  {item.status}
                                </span>
                              </div>
                              {item.evidence && (
                                <div className="mt-4 pt-4 border-t-2 border-gray-300">
                                  <div className="text-gray-700 font-bold text-lg mb-2">📄 Evidence:</div>
                                  <div className="text-gray-900 text-base leading-relaxed bg-blue-50 p-4 rounded-lg">
                                    {item.evidence}
                                  </div>
                                </div>
                              )}
                              {item.notes && (
                                <div className="mt-3 text-sm text-gray-600 italic">{item.notes}</div>
                              )}
                            </div>
                          ))}
                        </div>
                      </div>
                    )}

                    {/* GL Coverage Validations */}
                    {Object.keys(glCoverageValidations).length > 0 && (
                      <div className="mb-8">
                        <h4 className="text-xl font-bold text-gray-800 mb-4">🏢 Coverage Validation</h4>
                        <div className="space-y-6">
                          {Object.entries(glCoverageValidations).map(([coverage, items]: [string, any]) => {
                            if (!Array.isArray(items) || items.length === 0) return null;
                            return (
                              <div key={coverage} className="border-2 rounded-lg p-6 bg-green-50 shadow-sm">
                                <h5 className="text-lg font-bold text-gray-800 mb-4 capitalize">
                                  {coverage.replace(/_/g, " ")}
                                </h5>
                                <div className="space-y-4">
                                  {items.map((item: any, idx: number) => (
                                    <CoverageItem key={idx} item={item} />
                                  ))}
                                </div>
                              </div>
                            );
                          })}
                        </div>
                      </div>
                    )}
                  </>
                )}
              </div>
            )}

            {/* ACORD Certificate Extracted Fields (no comparisons yet) */}
            {hasAcord && (
              <div className="mb-10">
                <h3 className="text-2xl font-bold text-indigo-800 mb-6 pb-3 border-b-2 border-indigo-300">
                  📄 ACORD Certificate (Extracted Fields)
                </h3>
                {acordCertData && Object.keys(acordCertData).length > 0 ? (
                  <div className="mb-8">
                    <div className="bg-indigo-50 border-2 border-indigo-200 rounded-lg p-6 space-y-3">
                      {"policy_number" in acordCertData && (
                        <div className="flex justify-between">
                          <span className="font-semibold text-gray-700">Policy Number:</span>
                          <span className="text-gray-900 font-bold">
                            {acordCertData.policy_number ?? "N/A"}
                          </span>
                        </div>
                      )}
                      {"effective_date" in acordCertData && (
                        <div className="flex justify-between">
                          <span className="font-semibold text-gray-700">Effective Date:</span>
                          <span className="text-gray-900 font-bold">
                            {acordCertData.effective_date ?? "N/A"}
                          </span>
                        </div>
                      )}
                      {"expiration_date" in acordCertData && (
                        <div className="flex justify-between">
                          <span className="font-semibold text-gray-700">Expiration Date:</span>
                          <span className="text-gray-900 font-bold">
                            {acordCertData.expiration_date ?? "N/A"}
                          </span>
                        </div>
                      )}
                      {"insured_name" in acordCertData && (
                        <div className="flex justify-between">
                          <span className="font-semibold text-gray-700">Insured Name:</span>
                          <span className="text-gray-900 font-bold">
                            {acordCertData.insured_name ?? "N/A"}
                          </span>
                        </div>
                      )}
                      {"mailing_address" in acordCertData && (
                        <div className="flex justify-between">
                          <span className="font-semibold text-gray-700">Mailing Address:</span>
                          <span className="text-gray-900 font-bold max-w-md text-right">
                            {acordCertData.mailing_address ?? "N/A"}
                          </span>
                        </div>
                      )}
                      {"location_address" in acordCertData && (
                        <div className="flex justify-between">
                          <span className="font-semibold text-gray-700">Location Address:</span>
                          <span className="text-gray-900 font-bold max-w-md text-right">
                            {acordCertData.location_address ?? "N/A"}
                          </span>
                        </div>
                      )}
                    </div>
                    {acordCertData.coverages && typeof acordCertData.coverages === "object" && (
                      <div className="mt-6">
                        <h4 className="text-xl font-bold text-gray-800 mb-3">
                          🛡️ Coverages (from ACORD)
                        </h4>
                        <div className="bg-white border border-gray-200 rounded-lg divide-y divide-gray-100">
                          {Object.entries(acordCertData.coverages).map(
                            ([name, value]: [string, any]) => (
                              <div
                                key={name}
                                className="flex justify-between items-center px-4 py-3"
                              >
                                <span className="font-semibold text-gray-700">
                                  {name}
                                </span>
                                <span className="font-bold text-gray-900">
                                  {String(value)}
                                </span>
                              </div>
                            )
                          )}
                        </div>
                      </div>
                    )}
                  </div>
                ) : (
                  <p className="text-gray-600">
                    No ACORD fields were extracted for this upload.
                  </p>
                )}

                {/* PL vs Policy vs ACORD comparisons for core fields */}
                {Object.keys(acordPlCoreComparisons).length > 0 && (
                  <div className="mt-10">
                    <h4 className="text-xl font-bold text-gray-800 mb-4">
                      🔍 PL vs Policy vs ACORD (Core Fields)
                    </h4>
                    <div className="space-y-5">
                      {Object.entries(acordPlCoreComparisons).map(
                        ([field, data]: [string, any]) => (
                          <div
                            key={field}
                            className="border-2 rounded-lg p-6 bg-white shadow-sm"
                          >
                            <div className="flex justify-between items-start mb-4">
                              <div>
                                <div className="font-bold text-gray-800 text-xl mb-1">
                                  {field.replace(/_/g, " ").toUpperCase()}
                                </div>
                              </div>
                              <span
                                className={`px-4 py-2 rounded-full text-sm font-bold ${
                                  data.status === "MATCH"
                                    ? "bg-green-100 text-green-700"
                                    : data.status === "MISMATCH" || data.status === "NOT_FOUND"
                                    ? "bg-red-100 text-red-700"
                                    : "bg-yellow-100 text-yellow-700"
                                }`}
                              >
                                {data.status}
                              </span>
                            </div>
                            <div className="grid grid-cols-3 gap-4 text-sm">
                              <div>
                                <div className="text-gray-500 font-semibold mb-1">
                                  Policy
                                </div>
                                <div className="text-gray-900 font-bold break-words">
                                  {data.policy_value ?? "N/A"}
                                </div>
                              </div>
                              <div>
                                <div className="text-gray-500 font-semibold mb-1">
                                  PL Certificate
                                </div>
                                <div className="text-gray-900 font-bold break-words">
                                  {data.certificate_value ?? "N/A"}
                                </div>
                              </div>
                              <div>
                                <div className="text-gray-500 font-semibold mb-1">
                                  ACORD
                                </div>
                                <div className="text-gray-900 font-bold break-words">
                                  {data.acord_value ?? "N/A"}
                                </div>
                              </div>
                            </div>
                            {data.notes && (
                              <div className="mt-4 text-xs text-gray-600 italic">
                                {data.notes}
                              </div>
                            )}
                          </div>
                        )
                      )}
                    </div>
                  </div>
                )}

                {/* PL vs Policy vs ACORD comparisons for coverage limits */}
                {acordPlCoverageComparisons.length > 0 && (
                  <div className="mt-10">
                    <h4 className="text-xl font-bold text-gray-800 mb-4">
                      🛡️ PL vs Policy vs ACORD (Coverages)
                    </h4>
                    <div className="space-y-5">
                      {acordPlCoverageComparisons.map((item: any, idx: number) => (
                        <div
                          key={`${item.coverage_group}-${idx}`}
                          className="border-2 rounded-lg p-6 bg-white shadow-sm"
                        >
                          <div className="flex justify-between items-start mb-4">
                            <div>
                              <div className="font-bold text-gray-800 text-xl mb-1">
                                {item.coverage_name || "Coverage"}
                              </div>
                              <div className="text-xs uppercase tracking-wide text-gray-400">
                                {item.coverage_group?.replace(/_/g, " ")}
                              </div>
                            </div>
                            <span
                              className={`px-4 py-2 rounded-full text-sm font-bold ${
                                item.status === "MATCH"
                                  ? "bg-green-100 text-green-700"
                                  : item.status === "MISMATCH" || item.status === "NOT_FOUND"
                                  ? "bg-red-100 text-red-700"
                                  : "bg-yellow-100 text-yellow-700"
                              }`}
                            >
                              {item.status}
                            </span>
                          </div>
                          <div className="grid grid-cols-3 gap-4 text-sm">
                            <div>
                              <div className="text-gray-500 font-semibold mb-1">
                                Policy
                              </div>
                              <div className="text-gray-900 font-bold break-words">
                                {item.policy_value ?? "N/A"}
                              </div>
                            </div>
                            <div>
                              <div className="text-gray-500 font-semibold mb-1">
                                PL Certificate
                              </div>
                              <div className="text-gray-900 font-bold break-words">
                                {item.certificate_value ?? "N/A"}
                              </div>
                            </div>
                            <div>
                              <div className="text-gray-500 font-semibold mb-1">
                                ACORD
                              </div>
                              <div className="text-gray-900 font-bold break-words">
                                {item.acord_value ?? "N/A"}
                              </div>
                            </div>
                          </div>
                          
                          {/* Comparison Notes (3-way match/mismatch summary) */}
                          {item.notes && (
                            <div className="mt-4 p-4 bg-gray-50 border border-gray-200 rounded-lg">
                              <div className="text-gray-700 font-semibold text-base mb-2">📊 Comparison Notes:</div>
                              <div className="text-gray-900 text-sm">{item.notes}</div>
                            </div>
                          )}
                          
                          {/* Detailed Validator Notes (from original validation) */}
                          {item.validator_notes && item.validator_notes !== "null" && (
                            <div className="mt-4 p-4 bg-blue-50 border border-blue-200 rounded-lg">
                              <div className="text-blue-700 font-semibold text-base mb-2">📝 Validation Details:</div>
                              <div className="text-blue-900 text-sm">{item.validator_notes}</div>
                            </div>
                          )}
                          
                          {/* Evidence Section */}
                          {(item.evidence_declarations || item.evidence_endorsements) && (
                            <div className="mt-4 pt-4 border-t-2 border-gray-300">
                              <div className="text-gray-700 font-bold text-lg mb-3">📄 Evidence Found:</div>
                              {item.evidence_declarations && item.evidence_declarations !== "null" && (
                                <div className="bg-blue-50 p-4 rounded-lg border-l-4 border-blue-500 mb-2">
                                  <div className="text-blue-900 font-bold text-base mb-2">📄 From Declarations:</div>
                                  <div className="text-gray-900 text-sm whitespace-pre-wrap">{item.evidence_declarations}</div>
                                </div>
                              )}
                              {item.evidence_endorsements && item.evidence_endorsements !== "null" && (
                                <div className="bg-purple-50 p-4 rounded-lg border-l-4 border-purple-500">
                                  <div className="text-purple-900 font-bold text-base mb-2">📋 From Endorsement:</div>
                                  <div className="text-gray-900 text-sm whitespace-pre-wrap">{item.evidence_endorsements}</div>
                                </div>
                              )}
                            </div>
                          )}
                        </div>
                      ))}
                    </div>
                  </div>
                )}

                {/* Additional Interests Validation - Dedicated Section (Always shown, regardless of ACORD data) */}
                {plAdditionalInterests.length > 0 && (
                  <div className="mb-8">
                    <h4 className="text-xl font-bold text-gray-800 mb-4">👥 Additional Interests Validation</h4>
                    <div className="space-y-6">
                      {plAdditionalInterests.map((interest: any, idx: number) => (
                        <CoverageItem key={`additional-interest-${idx}`} item={interest} />
                      ))}
                    </div>
                  </div>
                )}
                {plAdditionalInterests.length === 0 && (
                  <div className="mb-8">
                    <h4 className="text-xl font-bold text-gray-800 mb-4">👥 Additional Interests Validation</h4>
                    <div className="bg-gray-50 border-2 border-gray-200 rounded-lg p-6">
                      <p className="text-gray-600">No additional interests found on certificate.</p>
                    </div>
                  </div>
                )}
              </div>
            )}

            {/* GL ACORD Certificate Extracted Fields */}
            {hasGlAcord && (
              <div className="mb-10">
                <h3 className="text-2xl font-bold text-teal-800 mb-6 pb-3 border-b-2 border-teal-300">
                  📋 GL ACORD Certificate (Extracted Fields)
                </h3>
                {glAcordCertData && Object.keys(glAcordCertData).length > 0 ? (
                  <div className="mb-8">
                    <div className="bg-teal-50 border-2 border-teal-200 rounded-lg p-6 space-y-3">
                      {"policy_number" in glAcordCertData && (
                        <div className="flex justify-between">
                          <span className="font-semibold text-gray-700">Policy Number:</span>
                          <span className="text-gray-900 font-bold">
                            {glAcordCertData.policy_number ?? "N/A"}
                          </span>
                        </div>
                      )}
                      {"effective_date" in glAcordCertData && (
                        <div className="flex justify-between">
                          <span className="font-semibold text-gray-700">Effective Date:</span>
                          <span className="text-gray-900 font-bold">
                            {glAcordCertData.effective_date ?? "N/A"}
                          </span>
                        </div>
                      )}
                      {"applicant_first_named_insured" in glAcordCertData && (
                        <div className="flex justify-between">
                          <span className="font-semibold text-gray-700">Applicant/First Named Insured:</span>
                          <span className="text-gray-900 font-bold">
                            {glAcordCertData.applicant_first_named_insured ?? "N/A"}
                          </span>
                        </div>
                      )}
                      {"general_aggregate" in glAcordCertData && (
                        <div className="flex justify-between">
                          <span className="font-semibold text-gray-700">General Aggregate:</span>
                          <span className="text-gray-900 font-bold">
                            {glAcordCertData.general_aggregate ?? "N/A"}
                          </span>
                        </div>
                      )}
                      {"each_occurrence" in glAcordCertData && (
                        <div className="flex justify-between">
                          <span className="font-semibold text-gray-700">Each Occurrence:</span>
                          <span className="text-gray-900 font-bold">
                            {glAcordCertData.each_occurrence ?? "N/A"}
                          </span>
                        </div>
                      )}
                    </div>
                  </div>
                ) : (
                  <div className="text-gray-500 italic">No GL ACORD data available</div>
                )}
              </div>
            )}

            {/* GL vs Policy vs GL ACORD comparisons for core fields */}
            {hasGlAcord && Object.keys(glAcordCoreComparisons).length > 0 && (
              <div className="mb-10">
                <h3 className="text-2xl font-bold text-teal-800 mb-6 pb-3 border-b-2 border-teal-300">
                  🔍 GL vs Policy vs GL ACORD (Core Fields)
                </h3>
                <div className="space-y-5">
                  {Object.entries(glAcordCoreComparisons).map(([field, data]: [string, any]) => (
                    <div
                      key={field}
                      className="border-2 rounded-lg p-6 bg-white shadow-sm"
                    >
                      <div className="flex justify-between items-start mb-4">
                        <div className="font-bold text-gray-800 text-xl">
                          {field.replace(/_/g, " ").toUpperCase()}
                        </div>
                        <span
                          className={`px-4 py-2 rounded-full text-sm font-bold ${
                            data.status === "MATCH"
                              ? "bg-green-100 text-green-700"
                              : data.status === "MISMATCH" || data.status === "NOT_FOUND"
                              ? "bg-red-100 text-red-700"
                              : "bg-yellow-100 text-yellow-700"
                          }`}
                        >
                          {data.status}
                        </span>
                      </div>
                      <div className="space-y-3">
                        <div className="flex justify-between">
                          <span className="text-gray-600 font-semibold">Policy:</span>
                          <span className="text-gray-900 font-bold">
                            {data.policy_value || "N/A"}
                          </span>
                        </div>
                        <div className="flex justify-between">
                          <span className="text-gray-600 font-semibold">GL Certificate:</span>
                          <span className="text-gray-900 font-bold">
                            {data.certificate_value || "N/A"}
                          </span>
                        </div>
                        <div className="flex justify-between">
                          <span className="text-gray-600 font-semibold">GL ACORD:</span>
                          <span className="text-gray-900 font-bold">
                            {data.gl_acord_value || "N/A"}
                          </span>
                        </div>
                        {data.notes && (
                          <div className="mt-3 pt-3 border-t border-gray-200">
                            <div className="text-sm text-gray-600">{data.notes}</div>
                          </div>
                        )}
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            )}

            {/* GL vs Policy vs GL ACORD comparisons for coverage limits */}
            {hasGlAcord && glAcordCoverageComparisons.length > 0 && (
              <div className="mb-10">
                <h3 className="text-2xl font-bold text-teal-800 mb-6 pb-3 border-b-2 border-teal-300">
                  🛡️ GL vs Policy vs GL ACORD (Coverages)
                </h3>
                <div className="space-y-5">
                  {glAcordCoverageComparisons.map((item: any, idx: number) => (
                    <div
                      key={idx}
                      className="border-2 rounded-lg p-6 bg-white shadow-sm"
                    >
                      <div className="flex justify-between items-start mb-4">
                        <div className="font-bold text-gray-800 text-xl">
                          {item.coverage_name}
                        </div>
                        <span
                          className={`px-4 py-2 rounded-full text-sm font-bold ${
                            item.status === "MATCH"
                              ? "bg-green-100 text-green-700"
                              : item.status === "MISMATCH" || item.status === "NOT_FOUND"
                              ? "bg-red-100 text-red-700"
                              : "bg-yellow-100 text-yellow-700"
                          }`}
                        >
                          {item.status}
                        </span>
                      </div>
                      <div className="space-y-3">
                        <div className="flex justify-between">
                          <span className="text-gray-600 font-semibold">Policy:</span>
                          <span className="text-gray-900 font-bold">
                            {item.policy_value || "N/A"}
                          </span>
                        </div>
                        <div className="flex justify-between">
                          <span className="text-gray-600 font-semibold">GL Certificate:</span>
                          <span className="text-gray-900 font-bold">
                            {item.certificate_value || "N/A"}
                          </span>
                        </div>
                        <div className="flex justify-between">
                          <span className="text-gray-600 font-semibold">GL ACORD:</span>
                          <span className="text-gray-900 font-bold">
                            {item.gl_acord_value || "N/A"}
                          </span>
                        </div>
                        
                        {/* Comparison Notes (3-way match/mismatch summary) */}
                        {item.notes && (
                          <div className="mt-4 p-4 bg-gray-50 border border-gray-200 rounded-lg">
                            <div className="text-gray-700 font-semibold text-base mb-2">📊 Comparison Notes:</div>
                            <div className="text-gray-900 text-sm">{item.notes}</div>
                          </div>
                        )}
                        
                        {/* Detailed Validator Notes (from original GL validation) */}
                        {item.validator_notes && item.validator_notes !== "null" && (
                          <div className="mt-4 p-4 bg-teal-50 border border-teal-200 rounded-lg">
                            <div className="text-teal-700 font-semibold text-base mb-2">📝 Validation Details:</div>
                            <div className="text-teal-900 text-sm">{item.validator_notes}</div>
                          </div>
                        )}
                        
                        {/* Evidence Section */}
                        {item.evidence && item.evidence !== "null" && (
                          <div className="mt-4 pt-4 border-t-2 border-gray-300">
                            <div className="text-gray-700 font-bold text-lg mb-3">📄 Evidence Found:</div>
                            <div className="bg-teal-50 p-4 rounded-lg border-l-4 border-teal-500">
                              <div className="text-teal-900 font-bold text-base mb-2">📄 From Policy:</div>
                              <div className="text-gray-900 text-sm whitespace-pre-wrap">{item.evidence}</div>
                            </div>
                          </div>
                        )}
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            )}

            {/* Legacy Format Support */}
            {!hasPlGlFormat && (
              <>
                {/* Core Validations */}
                {Object.keys(coreValidations).length > 0 && (
                  <div className="mb-10">
                    <h3 className="text-2xl font-bold text-gray-800 mb-6 pb-3 border-b-2 border-gray-300">✅ Core Field Validation</h3>
                    <div className="space-y-5">
                      {Object.entries(coreValidations).map(([field, data]: [string, any]) => (
                        <ValidationRow
                          key={field}
                          label={field.replace(/_/g, " ").toUpperCase()}
                          certValue={data.certificate_value}
                          policyValue={data.policy_value}
                          status={data.status}
                          evidence={data.evidence}
                        />
                      ))}
                    </div>
                  </div>
                )}

                {/* Coverage Validations */}
                {Object.keys(coverageValidations).length > 0 && (
                  <div className="mb-10">
                    <h3 className="text-2xl font-bold text-gray-800 mb-6 pb-3 border-b-2 border-gray-300">🏢 Coverage Validation</h3>
                    <div className="space-y-6">
                      {Object.entries(coverageValidations).map(([coverage, items]: [string, any]) => {
                        if (!Array.isArray(items) || items.length === 0) return null;
                        return (
                          <div key={coverage} className="border-2 rounded-lg p-6 bg-gray-50 shadow-sm">
                            <h4 className="text-xl font-bold text-gray-800 mb-4 capitalize">
                              {coverage.replace(/_/g, " ")}
                            </h4>
                            <div className="space-y-4">
                              {items.map((item: any, idx: number) => (
                                <CoverageItem key={idx} item={item} />
                              ))}
                            </div>
                          </div>
                        );
                      })}
                    </div>
                  </div>
                )}
              </>
            )}

            {/* Summary */}
            {results.summary && (
              <div className="mt-8 bg-blue-50 border border-blue-200 rounded-lg p-4">
                <h3 className="text-lg font-semibold text-blue-800 mb-2">📊 Summary</h3>
                {hasPlGlFormat ? (
                  <>
                    {results.summary.pl && (
                      <div className="mb-3">
                        <p className="text-sm font-semibold text-blue-800 mb-1">🏠 PL Certificate:</p>
                        {results.summary.pl.core && (
                          <p className="text-sm text-gray-700 ml-4">
                            <strong>Core:</strong> {results.summary.pl.core.match || 0} matched, {results.summary.pl.core.mismatch || 0} mismatched, {results.summary.pl.core.not_found || 0} not found
                          </p>
                        )}
                        {results.summary.pl.coverage && (
                          <p className="text-sm text-gray-700 ml-4">
                            <strong>Coverage:</strong> See coverage sections above
                          </p>
                        )}
                      </div>
                    )}
                    {results.summary.gl && (
                      <div>
                        <p className="text-sm font-semibold text-green-800 mb-1">⚖️ GL Certificate:</p>
                        {results.summary.gl.total_limits !== undefined && (
                          <p className="text-sm text-gray-700 ml-4">
                            <strong>Limits:</strong> {results.summary.gl.matched || 0} matched, {results.summary.gl.mismatched || 0} mismatched, {results.summary.gl.not_found || 0} not found (Total: {results.summary.gl.total_limits || 0})
                          </p>
                        )}
                        {results.summary.gl.total_cgl_limits !== undefined && results.summary.gl.total_cgl_limits > 0 && (
                          <p className="text-sm text-gray-700 ml-4">
                            <strong>CGL Limits:</strong> {results.summary.gl.total_cgl_limits}
                          </p>
                        )}
                        {results.summary.gl.total_liquor_limits !== undefined && results.summary.gl.total_liquor_limits > 0 && (
                          <p className="text-sm text-gray-700 ml-4">
                            <strong>Liquor Limits:</strong> {results.summary.gl.total_liquor_limits}
                          </p>
                        )}
                      </div>
                    )}
                  </>
                ) : (
                  <>
                    {results.summary.core && (
                      <p className="text-sm text-gray-700">
                        <strong>Core:</strong> {results.summary.core.match || 0} matched, {results.summary.core.mismatch || 0} mismatched, {results.summary.core.not_found || 0} not found
                      </p>
                    )}
                    {results.summary.coverage && (
                      <p className="text-sm text-gray-700">
                        <strong>Coverage:</strong> {results.summary.coverage.match || 0} matched, {results.summary.coverage.mismatch || 0} mismatched, {results.summary.coverage.not_found || 0} not found
                      </p>
                    )}
                  </>
                )}
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}


function ValidationRow({
  label,
  certValue,
  policyValue,
  status,
  evidence,
}: {
  label: string;
  certValue: string;
  policyValue: string;
  status: string;
  evidence?: string;
}) {
  return (
    <div className="border-2 rounded-lg p-8 bg-white shadow-sm">
      <div className="flex justify-between items-start mb-5">
        <div className="font-bold text-gray-800 text-2xl">{label}</div>
        <span className={`px-6 py-3 rounded-full text-xl font-bold ${
          status === "MATCH" ? "bg-green-100 text-green-700" : 
          status === "MISMATCH" || status === "NOT_FOUND" ? "bg-red-100 text-red-700" : 
          "bg-yellow-100 text-yellow-700"
        }`}>
          {status}
        </span>
      </div>
      <div className="space-y-4">
        <div className="flex justify-between items-center">
          <span className="text-gray-600 font-semibold text-xl">Certificate:</span>
          <span className="text-gray-900 font-bold text-xl">{certValue || "N/A"}</span>
        </div>
        <div className="flex justify-between items-center">
          <span className="text-gray-600 font-semibold text-xl">Policy:</span>
          <span className="text-gray-900 font-bold text-xl">{policyValue || "N/A"}</span>
        </div>
        {evidence && (
          <div className="mt-5 pt-5 border-t-2 border-gray-300">
            <div className="text-gray-700 font-bold text-2xl mb-4">📄 Evidence:</div>
            <div className="text-gray-900 text-xl leading-relaxed bg-blue-50 p-6 rounded-lg font-semibold">
              {evidence}
            </div>
          </div>
        )}
      </div>
    </div>
  );
}

function CoverageItem({ item }: { item: any }) {
  // Check if this is a GL limit item (has cert_limit_key)
  const isGLLimitItem = !!item.cert_limit_key;
  
  // Check if this is an Additional Interests item (has cert_interest_name)
  const isAdditionalInterest = !!item.cert_interest_name && !isGLLimitItem;
  
  // Extract coverage name from various possible fields
  const coverageName = isGLLimitItem
    ? (item.cert_limit_label || item.cert_limit_key || "Limit")
    : isAdditionalInterest 
    ? item.cert_interest_name 
    : (item.cert_building_name || item.cert_bpp_name || item.cert_bi_name || 
       item.cert_ms_name || item.cert_eb_name || item.cert_os_name || 
       item.cert_ed_name || item.cert_pc_name || item.cert_theft_name || 
       item.cert_wind_hail_name || "Coverage");
  
  // Extract cert value from various possible fields
  const certValue = isGLLimitItem
    ? (item.cert_value || item.cert_limit_value || "N/A")
    : isAdditionalInterest
    ? `${item.cert_interest_name}${item.cert_interest_address ? ` - ${item.cert_interest_address}` : ''}`
    : (item.cert_building_value || item.cert_bpp_value || item.cert_bi_value || 
       item.cert_ms_value || item.cert_eb_value || item.cert_os_value || 
       item.cert_ed_value || item.cert_pc_value || item.cert_theft_value || 
       item.cert_wind_hail_value || item.cert_value || "N/A");
  
  // Extract policy value from various possible fields
  // For Money & Securities, check split field if value is null
  const policyValue = isGLLimitItem
    ? (item.policy_value || item.policy_limit_value || "Not found")
    : isAdditionalInterest
    ? `${item.policy_interest_name || 'Not found'}${item.policy_interest_address ? ` - ${item.policy_interest_address}` : ''}${item.policy_interest_type ? ` (${item.policy_interest_type})` : ''}`
    : (item.policy_building_value || item.policy_bpp_value || item.policy_bi_value || 
       (item.policy_ms_value || item.policy_ms_split) || item.policy_eb_value || item.policy_os_value || 
       item.policy_ed_value || item.policy_pc_value || item.policy_theft_value || 
       item.policy_wind_hail_value || item.policy_value || "N/A");
  
  // Extract evidence fields
  const evidenceDecl = item.evidence_declarations || item.evidence_causes_of_loss;
  const evidenceEndorsement = item.evidence_endorsements || item.evidence_deductible_endorsement;
  const evidenceAdditionalInterest = item.evidence; // Additional Interests uses single evidence field
  
  return (
    <div className="border-l-4 pl-6 py-5 bg-white rounded-r shadow-sm" style={{
      borderColor: item.status === "MATCH" ? "#10b981" : (item.status === "MISMATCH" || item.status === "NOT_FOUND") ? "#ef4444" : "#f59e0b"
    }}>
      <div className="flex justify-between items-start mb-5">
        <span className="font-bold text-gray-800 text-2xl">
          {coverageName}
        </span>
        <div className="flex items-center gap-3">
          {item.match_type && item.match_type === "NAME_VARIATION" && (
            <span className="text-sm px-3 py-1 bg-orange-100 text-orange-700 rounded-full font-semibold">
              OCR Variation
            </span>
          )}
          <span className={`font-bold text-xl px-6 py-3 rounded-full ${
            item.status === "MATCH" ? "bg-green-100 text-green-700" : 
            item.status === "MISMATCH" || item.status === "NOT_FOUND" ? "bg-red-100 text-red-700" : 
            "bg-yellow-100 text-yellow-700"
          }`}>
            {item.status}
          </span>
        </div>
      </div>
      <div className="space-y-4">
        <div className="flex justify-between items-center">
          <span className="text-gray-600 font-semibold text-xl">Certificate:</span>
          <span className="font-bold text-gray-900 text-xl">{certValue}</span>
        </div>
        <div className="flex justify-between items-center">
          <span className="text-gray-600 font-semibold text-xl">Policy:</span>
          <span className="font-bold text-gray-900 text-xl">{policyValue}</span>
        </div>
        
        {/* Additional Interests specific: Show match type if NAME_VARIATION */}
        {isAdditionalInterest && item.match_type && item.match_type === "NAME_VARIATION" && (
          <div className="bg-orange-50 border border-orange-200 rounded-lg p-4">
            <div className="text-orange-800 font-semibold text-lg">
              ⚠️ Name Variation Detected: Names are similar but not identical (likely OCR error)
            </div>
          </div>
        )}
        
        {/* Notes for all coverage types */}
        {item.notes && item.notes !== "null" && (
          <div className="bg-gray-50 border border-gray-200 rounded-lg p-4">
            <div className="text-gray-700 font-semibold text-lg mb-2">📝 Notes:</div>
            <div className="text-gray-900 text-base">{item.notes}</div>
          </div>
        )}
        
        {/* Evidence Section */}
        {(evidenceDecl || evidenceEndorsement || evidenceAdditionalInterest) && (
          <div className="mt-6 pt-6 border-t-2 border-gray-300 space-y-5">
            <div className="text-gray-700 font-bold text-2xl mb-4">📄 Evidence Found:</div>
            {evidenceAdditionalInterest && evidenceAdditionalInterest !== "null" && (
              <div className="bg-green-50 p-6 rounded-lg border-l-4 border-green-500">
                <div className="text-green-900 font-bold text-2xl mb-4">📄 From Policy:</div>
                <div className="text-gray-900 text-xl leading-relaxed whitespace-pre-wrap font-semibold">{evidenceAdditionalInterest}</div>
              </div>
            )}
            {evidenceDecl && evidenceDecl !== "null" && (
              <div className="bg-blue-50 p-6 rounded-lg border-l-4 border-blue-500">
                <div className="text-blue-900 font-bold text-2xl mb-4">📄 From Declarations:</div>
                <div className="text-gray-900 text-xl leading-relaxed whitespace-pre-wrap font-semibold">{evidenceDecl}</div>
              </div>
            )}
            {evidenceEndorsement && evidenceEndorsement !== "null" && (
              <div className="bg-purple-50 p-6 rounded-lg border-l-4 border-purple-500">
                <div className="text-purple-900 font-bold text-2xl mb-4">📋 From Endorsement:</div>
                <div className="text-gray-900 text-xl leading-relaxed whitespace-pre-wrap font-semibold">{evidenceEndorsement}</div>
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  );
}

export default function QCNewResultsPage() {
  return (
    <Suspense fallback={
      <div className="min-h-screen bg-gradient-to-br from-blue-50 via-white to-purple-50 flex items-center justify-center">
        <div className="text-center">
          <div className="inline-block animate-spin rounded-full h-16 w-16 border-t-4 border-b-4 border-blue-500 mb-4"></div>
          <h2 className="text-2xl font-semibold text-gray-800 mb-2">Loading...</h2>
        </div>
      </div>
    }>
      <QCNewResultsContent />
    </Suspense>
  );
}

