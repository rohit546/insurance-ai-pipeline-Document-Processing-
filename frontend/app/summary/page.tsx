'use client';

import { useRouter } from 'next/navigation';
import { useAuth } from '@/context/AuthContext';
import { useEffect, useState } from 'react';
import Link from 'next/link';

interface CarrierData {
  id: number;
  name: string;
  propertyPDF: { file: File | null; name: string };
  liabilityPDF: { file: File | null; name: string };
  liquorPDF: { file: File | null; name: string };
  workersCompPDF: { file: File | null; name: string };
}

interface UploadResponse {
  success: boolean;
  uploadId?: string;
  totalCarriers?: number;
  totalFiles?: number;
  carriers?: any[];
  message?: string;
  error?: string;
  detail?: string;
}

export default function SummaryPage() {
  const { user, isLoggedIn, loading: authLoading } = useAuth();
  const router = useRouter();
  const [carriers, setCarriers] = useState<CarrierData[]>([
    { id: 1, name: '', propertyPDF: { file: null, name: '' }, liabilityPDF: { file: null, name: '' }, liquorPDF: { file: null, name: '' }, workersCompPDF: { file: null, name: '' } }
  ]);
  const [nextId, setNextId] = useState(2);
  const [isExecuting, setIsExecuting] = useState(false);
  const [uploadResult, setUploadResult] = useState<UploadResponse | null>(null);
  const [uploadError, setUploadError] = useState<string>('');
  const [apiUrl, setApiUrl] = useState<string>('http://localhost:8000');
  const [isHydrated, setIsHydrated] = useState(false);

  useEffect(() => {
    // Mark as hydrated after first render
    setIsHydrated(true);
  }, []);

  useEffect(() => {
    if (!authLoading && (!isLoggedIn || !user || !user.username) && isHydrated) {
      router.push('/login');
    }
  }, [isLoggedIn, user, isHydrated, authLoading, router]);

  // Set API URL on client side only
  useEffect(() => {
    const isVercel = typeof window !== 'undefined' && window.location.hostname !== 'localhost';
    const url = isVercel
      ? (process.env.NEXT_PUBLIC_API_URL || 'https://deployment-production-7739.up.railway.app')
      : 'http://localhost:8000';
    setApiUrl(url);
  }, []);

  const addCarrier = () => {
    const newCarrier: CarrierData = {
      id: nextId,
      name: '',
      propertyPDF: { file: null, name: '' },
      liabilityPDF: { file: null, name: '' },
      liquorPDF: { file: null, name: '' },
      workersCompPDF: { file: null, name: '' }
    };
    setCarriers([...carriers, newCarrier]);
    setNextId(nextId + 1);
  };

  const removeCarrier = (id: number) => {
    if (carriers.length > 1) {
      setCarriers(carriers.filter(c => c.id !== id));
    } else {
      alert('You must have at least one carrier');
    }
  };

  const handleNameChange = (id: number, value: string) => {
    setCarriers(carriers.map(c => (c.id === id ? { ...c, name: value } : c)));
  };

  const handleFileUpload = (id: number, type: 'property' | 'liability' | 'liquor' | 'workersComp', file: File) => {
    setCarriers(
      carriers.map(c => {
        if (c.id === id) {
          if (type === 'property') {
            return { ...c, propertyPDF: { file, name: file.name } };
          } else if (type === 'liability') {
            return { ...c, liabilityPDF: { file, name: file.name } };
          } else if (type === 'liquor') {
            return { ...c, liquorPDF: { file, name: file.name } };
          } else {
            return { ...c, workersCompPDF: { file, name: file.name } };
          }
        }
        return c;
      })
    );
  };

  const handleExecute = async () => {
    // Ensure user is loaded before proceeding
    if (authLoading) {
      alert('Please wait, loading user information...');
      return;
    }

    if (!user || !user.username) {
      alert('You must be logged in to upload. Please refresh the page and log in again.');
      return;
    }

    // Validate all carriers have names
    const isValid = carriers.every(
      c => c.name.trim()
    );

    if (!isValid) {
      alert('Please fill in all carrier names');
      return;
    }

    // Check if at least one carrier has at least one file
    const hasAnyFiles = carriers.some(
      c => c.propertyPDF.file || c.liabilityPDF.file || c.liquorPDF.file || c.workersCompPDF.file
    );

    if (!hasAnyFiles) {
      alert('Please upload at least one PDF across all carriers');
      return;
    }

    setIsExecuting(true);
    setUploadError('');
    setUploadResult(null);

    try {
      // Create FormData
      const formData = new FormData();

      // Add carriers JSON
      const carriersJson = JSON.stringify({
        carriers: carriers.map((c, idx) => ({
          name: c.name,
          hasProperty: !!c.propertyPDF.file,
          hasLiability: !!c.liabilityPDF.file,
          hasLiquor: !!c.liquorPDF.file,
          hasWorkersComp: !!c.workersCompPDF.file
        }))
      });
      formData.append('carriers_json', carriersJson);

      // Add all files with metadata about which carrier they belong to
      carriers.forEach((carrier, carrierIdx) => {
        if (carrier.propertyPDF.file) {
          formData.append('files', carrier.propertyPDF.file);
          formData.append('file_metadata', JSON.stringify({
            carrierIndex: carrierIdx,
            type: 'property'
          }));
        }
        if (carrier.liabilityPDF.file) {
          formData.append('files', carrier.liabilityPDF.file);
          formData.append('file_metadata', JSON.stringify({
            carrierIndex: carrierIdx,
            type: 'liability'
          }));
        }
        if (carrier.liquorPDF.file) {
          formData.append('files', carrier.liquorPDF.file);
          formData.append('file_metadata', JSON.stringify({
            carrierIndex: carrierIdx,
            type: 'liquor'
          }));
        }
        if (carrier.workersCompPDF.file) {
          formData.append('files', carrier.workersCompPDF.file);
          formData.append('file_metadata', JSON.stringify({
            carrierIndex: carrierIdx,
            type: 'workersComp'
          }));
        }
      });

      // Validate user is loaded before uploading
      if (!user || !user.username) {
        setUploadError('You must be logged in to upload. Please refresh the page and try again.');
        setIsExecuting(false);
        return;
      }

      // Send to backend with extended timeout for large files
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), 120000); // 120 seconds (2 minutes)
      
      console.log('[Summary] Uploading with username:', user.username);
      
      try {
        const response = await fetch(`${apiUrl}/upload-quotes/`, {
          method: 'POST',
          body: formData,
          headers: {
            'ngrok-skip-browser-warning': 'true',
            'X-User-ID': user.username, // Use username directly, we validated it above
          },
          signal: controller.signal,
        });
        
        clearTimeout(timeoutId);

      let data: UploadResponse;
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

      setUploadResult(data);
      console.log('Upload successful:', data);
      } catch (fetchError: any) {
        clearTimeout(timeoutId);
        if (fetchError.name === 'AbortError') {
          throw new Error('Upload timeout: Large files may take up to 2 minutes to process. Please try again or use smaller files.');
        }
        throw fetchError;
      }
    } catch (error: any) {
      const errorMessage = error.message || 'Failed to upload carriers';
      setUploadError(errorMessage);
      console.error('Upload error:', error);
    } finally {
      setIsExecuting(false);
    }
  };

  const handleConfirmExecution = async () => {
    setIsExecuting(true);
    setUploadError('');

    try {
      if (!uploadResult?.uploadId) {
        throw new Error('Missing uploadId. Please execute upload first.');
      }

      // Call backend to analyze PDF quality from GCS
      const response = await fetch(`${apiUrl}/phase1/quality-analysis?uploadId=${encodeURIComponent(uploadResult.uploadId)}`, {
        method: 'GET',
        headers: {
          'ngrok-skip-browser-warning': 'true',
        },
      });

      const data = await response.json();
      if (!response.ok || !data.success) {
        throw new Error(data.message || data.error || data.detail || `Quality analysis failed (${response.status})`);
      }

      console.log('PDF Quality Analysis result:', data);

      // Navigate to next page with uploadId
      router.push(`/summary/confirmed?uploadId=${encodeURIComponent(uploadResult.uploadId)}`);
    } catch (error: any) {
      const errorMessage = error.message || 'Failed to confirm execution';
      setUploadError(errorMessage);
      console.error('Confirmation error:', error);
    } finally {
      setIsExecuting(false);
    }
  };

  if (!isHydrated || authLoading) {
    return (
      <div className="min-h-screen bg-gradient-to-br from-indigo-900 via-purple-900 to-pink-800 flex items-center justify-center">
        <p className="text-white">Loading...</p>
      </div>
    );
  }

  if (!isLoggedIn || !user || !user.username) {
    return (
      <div className="min-h-screen bg-gradient-to-br from-indigo-900 via-purple-900 to-pink-800 flex items-center justify-center">
        <p className="text-white">Redirecting to login...</p>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-gradient-to-br from-indigo-900 via-purple-900 to-pink-800 flex flex-col">
      {/* Header */}
      <header className="bg-black/20 backdrop-blur-md border-b border-white/10">
        <div className="max-w-7xl mx-auto px-8 py-6 flex justify-between items-center">
          <h1 className="text-3xl font-bold text-white">Mckinney and Co</h1>
          <Link href="/dashboard">
            <button className="px-6 py-2 bg-indigo-600 hover:bg-indigo-700 text-white rounded-lg font-semibold transition-colors">
              Back to Dashboard
            </button>
          </Link>
        </div>
      </header>

      {/* Main Content */}
      <main className="flex-1 max-w-6xl mx-auto w-full px-8 py-12">
        <div className="space-y-8">
          {/* Title Section */}
          <div className="text-center">
            <h2 className="text-4xl font-bold text-white mb-2">Multi-Carrier Quote Upload</h2>
            <p className="text-white/80">Upload insurance quotes for multiple carriers and lines</p>
          </div>

          {/* Success Message */}
          {uploadResult && uploadResult.success && (
            <div className="bg-green-500/20 border border-green-500/50 rounded-2xl p-6">
              <h3 className="text-xl font-bold text-green-300 mb-3">✓ Upload Successful!</h3>
              <div className="space-y-2 text-green-200">
                <p>Upload ID: <span className="font-mono">{uploadResult.uploadId}</span></p>
                <p>Total Carriers: {uploadResult.totalCarriers}</p>
                <p>Total Files: {uploadResult.totalFiles}</p>
              </div>
            </div>
          )}

          {/* Error Message */}
          {uploadError && (
            <div className="bg-red-500/20 border border-red-500/50 rounded-2xl p-6">
              <h3 className="text-xl font-bold text-red-300 mb-3">✕ Upload Failed</h3>
              <p className="text-red-200">{uploadError}</p>
            </div>
          )}

          {/* Carriers Container */}
          {!uploadResult?.success && (
            <>
              <div className="space-y-6">
                {carriers.map((carrier, index) => (
                  <div key={carrier.id} className="bg-white/10 backdrop-blur-md rounded-2xl p-8 border border-white/20">
                    {/* Carrier Header */}
                    <div className="flex justify-between items-center mb-6">
                      <h3 className="text-2xl font-bold text-white">Carrier {index + 1}</h3>
                      {carriers.length > 1 && (
                        <button
                          onClick={() => removeCarrier(carrier.id)}
                          className="px-4 py-2 bg-red-500/20 hover:bg-red-500/30 border border-red-500/50 text-red-200 rounded-lg font-semibold transition-colors"
                        >
                          ✕ Remove
                        </button>
                      )}
                    </div>

                    {/* Carrier Name Input */}
                    <div className="mb-8">
                      <label className="block text-white font-medium mb-3">Carrier Name</label>
                      <input
                        type="text"
                        value={carrier.name}
                        onChange={(e) => handleNameChange(carrier.id, e.target.value)}
                        placeholder="e.g., State Farm, Allstate, GEICO"
                        className="w-full px-4 py-3 rounded-lg bg-white/20 border border-white/30 text-white placeholder-white/60 focus:outline-none focus:ring-2 focus:ring-white/50"
                      />
                    </div>

                    {/* PDF Upload Grid */}
                    <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
                      {/* Property PDF */}
                      <div>
                        <label className="block text-white font-medium mb-3">Property PDF</label>
                        <div className="relative">
                          <input
                            type="file"
                            accept=".pdf"
                            onChange={(e) => {
                              if (e.target.files?.[0]) {
                                handleFileUpload(carrier.id, 'property', e.target.files[0]);
                              }
                            }}
                            className="absolute inset-0 opacity-0 cursor-pointer"
                          />
                          <div className="border-2 border-dashed border-white/30 rounded-xl p-8 hover:border-white/50 transition cursor-pointer bg-white/5 hover:bg-white/10">
                            <div className="text-center">
                              {carrier.propertyPDF.file ? (
                                <>
                                  <p className="text-green-300 font-medium">✓ {carrier.propertyPDF.name}</p>
                                  <p className="text-white/60 text-sm mt-1">{(carrier.propertyPDF.file.size / 1024).toFixed(2)} KB</p>
                                </>
                              ) : (
                                <>
                                  <p className="text-white/80 font-medium">📄 Click to upload PDF</p>
                                  <p className="text-white/60 text-sm mt-1">Property Quote</p>
                                </>
                              )}
                            </div>
                          </div>
                        </div>
                      </div>

                      {/* General Liability PDF */}
                      <div>
                        <label className="block text-white font-medium mb-3">General Liability PDF</label>
                        <div className="relative">
                          <input
                            type="file"
                            accept=".pdf"
                            onChange={(e) => {
                              if (e.target.files?.[0]) {
                                handleFileUpload(carrier.id, 'liability', e.target.files[0]);
                              }
                            }}
                            className="absolute inset-0 opacity-0 cursor-pointer"
                          />
                          <div className="border-2 border-dashed border-white/30 rounded-xl p-8 hover:border-white/50 transition cursor-pointer bg-white/5 hover:bg-white/10">
                            <div className="text-center">
                              {carrier.liabilityPDF.file ? (
                                <>
                                  <p className="text-green-300 font-medium">✓ {carrier.liabilityPDF.name}</p>
                                  <p className="text-white/60 text-sm mt-1">{(carrier.liabilityPDF.file.size / 1024).toFixed(2)} KB</p>
                                </>
                              ) : (
                                <>
                                  <p className="text-white/80 font-medium">📄 Click to upload PDF</p>
                                  <p className="text-white/60 text-sm mt-1">General Liability Quote</p>
                                </>
                              )}
                            </div>
                          </div>
                        </div>
                      </div>

                      {/* Liquor PDF */}
                      <div>
                        <label className="block text-white font-medium mb-3">Liquor PDF</label>
                        <div className="relative">
                          <input
                            type="file"
                            accept=".pdf"
                            onChange={(e) => {
                              if (e.target.files?.[0]) {
                                handleFileUpload(carrier.id, 'liquor', e.target.files[0]);
                              }
                            }}
                            className="absolute inset-0 opacity-0 cursor-pointer"
                          />
                          <div className="border-2 border-dashed border-white/30 rounded-xl p-8 hover:border-white/50 transition cursor-pointer bg-white/5 hover:bg-white/10">
                            <div className="text-center">
                              {carrier.liquorPDF.file ? (
                                <>
                                  <p className="text-green-300 font-medium">✓ {carrier.liquorPDF.name}</p>
                                  <p className="text-white/60 text-sm mt-1">{(carrier.liquorPDF.file.size / 1024).toFixed(2)} KB</p>
                                </>
                              ) : (
                                <>
                                  <p className="text-white/80 font-medium">📄 Click to upload PDF</p>
                                  <p className="text-white/60 text-sm mt-1">Liquor/Bar Insurance Quote</p>
                                </>
                              )}
                            </div>
                          </div>
                        </div>
                      </div>

                      {/* Workers Comp PDF */}
                      <div>
                        <label className="block text-white font-medium mb-3">Workers Comp PDF</label>
                        <div className="relative">
                          <input
                            type="file"
                            accept=".pdf"
                            onChange={(e) => {
                              if (e.target.files?.[0]) {
                                handleFileUpload(carrier.id, 'workersComp', e.target.files[0]);
                              }
                            }}
                            className="absolute inset-0 opacity-0 cursor-pointer"
                          />
                          <div className="border-2 border-dashed border-white/30 rounded-xl p-8 hover:border-white/50 transition cursor-pointer bg-white/5 hover:bg-white/10">
                            <div className="text-center">
                              {carrier.workersCompPDF.file ? (
                                <>
                                  <p className="text-green-300 font-medium">✓ {carrier.workersCompPDF.name}</p>
                                  <p className="text-white/60 text-sm mt-1">{(carrier.workersCompPDF.file.size / 1024).toFixed(2)} KB</p>
                                </>
                              ) : (
                                <>
                                  <p className="text-white/80 font-medium">📄 Click to upload PDF</p>
                                  <p className="text-white/60 text-sm mt-1">Workers Compensation Quote</p>
                                </>
                              )}
                            </div>
                          </div>
                        </div>
                      </div>
                    </div>

                    {/* Upload Status */}
                    <div className="mt-6 flex gap-4 text-sm">
                      <div className="flex items-center gap-2">
                        <span className={carrier.name.trim() ? '✓ text-green-300' : '○ text-white/60'}>Carrier Name</span>
                      </div>
                      <div className="flex items-center gap-2">
                        <span className={carrier.propertyPDF.file ? '✓ text-green-300' : '○ text-white/60'}>Property PDF</span>
                      </div>
                      <div className="flex items-center gap-2">
                        <span className={carrier.liabilityPDF.file ? '✓ text-green-300' : '○ text-white/60'}>Liability PDF</span>
                      </div>
                      <div className="flex items-center gap-2">
                        <span className={carrier.liquorPDF.file ? '✓ text-green-300' : '○ text-white/60'}>Liquor PDF</span>
                      </div>
                      <div className="flex items-center gap-2">
                        <span className={carrier.workersCompPDF.file ? '✓ text-green-300' : '○ text-white/60'}>Workers Comp PDF</span>
                      </div>
                    </div>
                  </div>
                ))}
              </div>

              {/* Action Buttons */}
              <div className="flex gap-4">
                <button
                  onClick={addCarrier}
                  className="flex-1 px-6 py-3 bg-white/20 hover:bg-white/30 text-white rounded-lg font-semibold transition-colors border border-white/30"
                >
                  + Add More Carrier
                </button>
                <button
                  onClick={handleExecute}
                  disabled={isExecuting}
                  className="flex-1 px-6 py-3 bg-white text-indigo-900 rounded-lg font-semibold hover:bg-white/90 transition-all duration-300 disabled:opacity-50 disabled:cursor-not-allowed flex items-center justify-center gap-2"
                >
                  {isExecuting ? (
                    <>
                      <span className="animate-spin">⏳</span>
                      Uploading...
                    </>
                  ) : (
                    <>
                      <span>✓</span>
                      Execute
                    </>
                  )}
                </button>
              </div>

              {/* Summary Stats */}
              <div className="bg-white/5 backdrop-blur-md rounded-2xl p-6 border border-white/20">
                <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
                  <div className="text-center">
                    <p className="text-white/60 text-sm mb-1">Total Carriers</p>
                    <p className="text-3xl font-bold text-white">{carriers.length}</p>
                  </div>
                  <div className="text-center">
                    <p className="text-white/60 text-sm mb-1">Complete Carriers</p>
                    <p className="text-3xl font-bold text-green-300">
                      {carriers.filter(c => c.name.trim() && (c.propertyPDF.file || c.liabilityPDF.file || c.liquorPDF.file || c.workersCompPDF.file)).length}
                    </p>
                  </div>
                  <div className="text-center">
                    <p className="text-white/60 text-sm mb-1">Total Files Uploaded</p>
                    <p className="text-3xl font-bold text-white">
                      {carriers.reduce((acc, c) => acc + (c.propertyPDF.file ? 1 : 0) + (c.liabilityPDF.file ? 1 : 0) + (c.liquorPDF.file ? 1 : 0) + (c.workersCompPDF.file ? 1 : 0), 0)}
                    </p>
                  </div>
                </div>
              </div>
            </>
          )}

          {/* Upload Result Details */}
          {uploadResult?.success && uploadResult.carriers && (
            <div className="space-y-4">
              <h3 className="text-2xl font-bold text-white">Uploaded Files</h3>
              {uploadResult.carriers.map((carrier, idx) => (
                <div key={idx} className="bg-white/5 rounded-lg p-6 border border-white/20">
                  <h4 className="text-lg font-semibold text-white mb-4">{carrier.carrierName}</h4>
                  <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
                    {carrier.propertyPDF ? (
                      <div className="bg-white/5 p-4 rounded">
                        <p className="text-white/80 text-sm font-medium mb-2">Property PDF</p>
                        <p className="text-white/60 text-xs break-all">{carrier.propertyPDF.path}</p>
                        <p className="text-white/50 text-xs mt-2">{(carrier.propertyPDF.size / 1024).toFixed(2)} KB</p>
                      </div>
                    ) : (
                      <div className="bg-white/10 p-4 rounded border border-white/20">
                        <p className="text-white/60 text-sm">Property PDF - Not uploaded</p>
                      </div>
                    )}
                    {carrier.liabilityPDF ? (
                      <div className="bg-white/5 p-4 rounded">
                        <p className="text-white/80 text-sm font-medium mb-2">Liability PDF</p>
                        <p className="text-white/60 text-xs break-all">{carrier.liabilityPDF.path}</p>
                        <p className="text-white/50 text-xs mt-2">{(carrier.liabilityPDF.size / 1024).toFixed(2)} KB</p>
                      </div>
                    ) : (
                      <div className="bg-white/10 p-4 rounded border border-white/20">
                        <p className="text-white/60 text-sm">Liability PDF - Not uploaded</p>
                      </div>
                    )}
                    {carrier.liquorPDF ? (
                      <div className="bg-white/5 p-4 rounded">
                        <p className="text-white/80 text-sm font-medium mb-2">Liquor PDF</p>
                        <p className="text-white/60 text-xs break-all">{carrier.liquorPDF.path}</p>
                        <p className="text-white/50 text-xs mt-2">{(carrier.liquorPDF.size / 1024).toFixed(2)} KB</p>
                      </div>
                    ) : (
                      <div className="bg-white/10 p-4 rounded border border-white/20">
                        <p className="text-white/60 text-sm">Liquor PDF - Not uploaded</p>
                      </div>
                    )}
                    {carrier.workersCompPDF ? (
                      <div className="bg-white/5 p-4 rounded">
                        <p className="text-white/80 text-sm font-medium mb-2">Workers Comp PDF</p>
                        <p className="text-white/60 text-xs break-all">{carrier.workersCompPDF.path}</p>
                        <p className="text-white/50 text-xs mt-2">{(carrier.workersCompPDF.size / 1024).toFixed(2)} KB</p>
                      </div>
                    ) : (
                      <div className="bg-white/10 p-4 rounded border border-white/20">
                        <p className="text-white/60 text-sm">Workers Comp PDF - Not uploaded</p>
                      </div>
                    )}
                  </div>
                </div>
              ))}

              {/* Confirm Execution Button */}
              <div className="flex gap-4 mt-8">
                <button
                  onClick={() => setUploadResult(null)}
                  className="flex-1 px-6 py-3 bg-white/20 hover:bg-white/30 text-white rounded-lg font-semibold transition-colors border border-white/30"
                >
                  ← Back to Edit
                </button>
                <button
                  onClick={handleConfirmExecution}
                  disabled={isExecuting}
                  className="flex-1 px-6 py-3 bg-green-500 hover:bg-green-600 text-white rounded-lg font-semibold transition-all duration-300 disabled:opacity-50 disabled:cursor-not-allowed flex items-center justify-center gap-2"
                >
                  {isExecuting ? (
                    <>
                      <span className="animate-spin">⏳</span>
                      Confirming...
                    </>
                  ) : (
                    <>
                      <span>✓</span>
                      Confirm Execution
                    </>
                  )}
                </button>
              </div>
            </div>
          )}
        </div>
      </main>
    </div>
  );
}
