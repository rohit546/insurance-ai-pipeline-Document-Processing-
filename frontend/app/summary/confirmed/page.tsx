'use client';

import { useRouter, useSearchParams } from 'next/navigation';
import { useAuth } from '@/context/AuthContext';
import { useEffect, useState, useRef, Suspense } from 'react';
import Link from 'next/link';

function ConfirmedPageContent() {
  const { user, isLoggedIn } = useAuth();
  const router = useRouter();
  const searchParams = useSearchParams();
  const [isHydrated, setIsHydrated] = useState(false);
  const [isProcessing, setIsProcessing] = useState(true);
  const [isComplete, setIsComplete] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [apiUrl, setApiUrl] = useState<string>('https://insurance-ai-pipeline-document-processing-production.up.railway.app');
  const [statusMessage, setStatusMessage] = useState<string>('Initializing...');
  const [sheetUrl, setSheetUrl] = useState<string | null>(null);
  const pollIntervalRef = useRef<NodeJS.Timeout | null>(null);
  const pollCountRef = useRef<number>(0);
  const isFinalizingRef = useRef<boolean>(false);

  useEffect(() => {
    // Mark as hydrated after first render
    setIsHydrated(true);
  }, []);

  useEffect(() => {
    // Set API URL on client side only
    const isVercel = typeof window !== 'undefined' && window.location.hostname !== 'localhost';
    const url = isVercel
      ? (process.env.NEXT_PUBLIC_API_URL || 'https://insurance-ai-pipeline-document-processing-production.up.railway.app')
      : 'https://insurance-ai-pipeline-document-processing-production.up.railway.app';
    setApiUrl(url);
  }, []);

  useEffect(() => {
    if (!isLoggedIn && isHydrated) {
      router.push('/login');
    }
  }, [isLoggedIn, isHydrated, router]);

  useEffect(() => {
    // Get uploadId from URL params
    const uploadId = searchParams?.get('uploadId');
    
    if (uploadId && isHydrated && isLoggedIn) {
      const maxPolls = 240; // 20 minutes max (240 * 5 seconds)
      pollCountRef.current = 0;
      setIsProcessing(true);
      setError(null);
      setStatusMessage('Checking processing status...');
      
      // Step 1: Poll /upload-status until ready, Step 2: Call /finalize-upload
      const checkStatusAndFinalize = async () => {
        try {
          pollCountRef.current++;
          const currentAttempt = pollCountRef.current;
          
          // Step 1: Check if Phase 3 is complete via status endpoint
          console.log(`Checking upload status (attempt ${currentAttempt}/${maxPolls})...`);
          setStatusMessage(`Checking processing status (${currentAttempt})...`);
          
          const statusResponse = await fetch(
            `${apiUrl}/upload-status/${encodeURIComponent(uploadId)}`,
            {
              method: 'GET',
              headers: {
                'ngrok-skip-browser-warning': 'true',
              },
            }
          );

          // Handle 404 - upload not found yet (might still be uploading)
          if (statusResponse.status === 404) {
            if (currentAttempt >= maxPolls) {
              stopPolling();
              setError('Upload not found. Please try again.');
              setIsProcessing(false);
              return;
            }
            console.log(`Upload not found yet (attempt ${currentAttempt}/${maxPolls}). Waiting...`);
            setStatusMessage('Waiting for upload to complete...');
            return; // Continue polling
          }

          let statusData: any;
          try {
            statusData = await statusResponse.json();
          } catch (parseError) {
            // If JSON parsing fails, keep polling
            if (currentAttempt < 30) {
              console.log(`Failed to parse status response (attempt ${currentAttempt}/${maxPolls})...`);
              return; // Continue polling
            }
            stopPolling();
            setError('Failed to communicate with server');
            setIsProcessing(false);
            return;
          }
          
          // Check if processing is complete
          if (!statusData.ready) {
            // Still processing - show progress
            const completed = statusData.completed_files || 0;
            const expected = statusData.expected_files || 1;
            const percentage = Math.round((completed / expected) * 100);
            
            if (currentAttempt >= maxPolls) {
              stopPolling();
              setError('Processing is taking longer than expected. Please check back in a few minutes.');
              setIsProcessing(false);
              return;
            }
            
            console.log(`Still processing: ${completed}/${expected} files complete (attempt ${currentAttempt}/${maxPolls})`);
            setStatusMessage(`Processing: ${completed}/${expected} files complete (${percentage}%)`);
            return; // Continue polling
          }
          
          // Step 2: Phase 3 is complete! Now call finalize-upload
          // Guard: only call finalize ONCE
          if (isFinalizingRef.current) {
            console.log('Finalize already in progress, skipping...');
            return; // Don't call finalize again while it's running
          }
          isFinalizingRef.current = true;
          console.log('✅ Phase 3 complete! Pushing to Google Sheets...');
          setStatusMessage('Pushing data to Google Sheets...');
          
          const finalizeResponse = await fetch(
            `${apiUrl}/finalize-upload?uploadId=${encodeURIComponent(uploadId)}&sheetName=Insurance Fields Data`,
            {
              method: 'GET',
              headers: {
                'ngrok-skip-browser-warning': 'true',
              },
            }
          );

          let finalizeData: any;
          try {
            finalizeData = await finalizeResponse.json();
          } catch (parseError) {
            // Retry a few times
            isFinalizingRef.current = false; // Allow retry
            if (currentAttempt < 50) {
              console.log(`Failed to parse finalize response, retrying...`);
              return; // Continue polling
            }
            stopPolling();
            setError('Failed to push data to Google Sheets');
            setIsProcessing(false);
            return;
          }
          
          // If backend says finalize is already in progress (lock), just wait
          if (finalizeData.inProgress) {
            console.log('Finalize in progress on server, waiting...');
            setStatusMessage('Finalizing data (in progress)...');
            return; // Keep polling, don't reset flag
          }
          
          // Check finalize result
          if (finalizeResponse.ok && finalizeData.success) {
            console.log('✅ Successfully pushed to Google Sheets!');
            stopPolling();
            setIsComplete(true);
            setSheetUrl(finalizeData.sheetUrl || null);
            setIsProcessing(false);
            return;
          }
          
          // Finalize failed - retry a few times
          isFinalizingRef.current = false; // Allow retry
          const errorMsg = finalizeData.error || finalizeData.detail || '';
          if (currentAttempt < 60) {
            console.log(`Finalize failed, retrying (attempt ${currentAttempt}/${maxPolls})... Error: ${errorMsg}`);
            setStatusMessage('Retrying Google Sheets push...');
            return; // Continue polling
          }
          
          // After many retries, show error
          stopPolling();
          setError(errorMsg || 'Failed to push data to Google Sheets');
          setIsProcessing(false);
          
        } catch (err: any) {
          // Network errors - keep polling for a while
          isFinalizingRef.current = false; // Allow retry
          if (pollCountRef.current < 50) {
            console.log(`Network error, retrying (attempt ${pollCountRef.current}/${maxPolls})...`);
            setStatusMessage('Connection issue, retrying...');
            return; // Continue polling
          }
          
          // After many retries, show error
          stopPolling();
          setError(err.message || 'Failed to communicate with server');
          setIsProcessing(false);
        }
      };
      
      const stopPolling = () => {
        if (pollIntervalRef.current) {
          clearInterval(pollIntervalRef.current);
          pollIntervalRef.current = null;
        }
      };
      
      // Start polling every 3 seconds (faster than before)
      pollIntervalRef.current = setInterval(checkStatusAndFinalize, 3000);
      
      // Also try immediately
      checkStatusAndFinalize();
      
      // Cleanup on unmount
      return () => {
        if (pollIntervalRef.current) {
          clearInterval(pollIntervalRef.current);
          pollIntervalRef.current = null;
        }
      };
    } else if (isHydrated && isLoggedIn) {
      // No uploadId, skip processing
      setIsProcessing(false);
      setIsComplete(true);
    }
  }, [searchParams, isHydrated, isLoggedIn, apiUrl]);

  if (!isHydrated) {
    return (
      <div className="min-h-screen bg-gradient-to-br from-indigo-900 via-purple-900 to-pink-800 flex items-center justify-center">
        <p className="text-white">Loading...</p>
      </div>
    );
  }

  if (!isLoggedIn || !user) {
    return (
      <div className="min-h-screen bg-gradient-to-br from-indigo-900 via-purple-900 to-pink-800 flex items-center justify-center">
        <p className="text-white">Redirecting to login...</p>
      </div>
    );
  }

  // Show loading screen while processing summary/Google Sheets push
  if (isProcessing) {
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

        {/* Loading Content */}
        <main className="flex-1 flex items-center justify-center">
          <div className="text-center">
            <div className="inline-block animate-spin rounded-full h-16 w-16 border-t-4 border-b-4 border-white mb-6"></div>
            <h2 className="text-4xl font-bold text-white mb-4">⏳ Processing your documents...</h2>
            <p className="text-white/80 text-xl mb-4">This may take a minute or two. Please wait.</p>
            <p className="text-yellow-300 text-lg font-semibold mb-6">{statusMessage}</p>
            <div className="bg-white/10 backdrop-blur-md rounded-xl p-6 border border-white/20 max-w-md mx-auto">
              <p className="text-white/90 text-sm">
                💡 The system is performing OCR extraction, LLM analysis, and pushing data to Google Sheets.
              </p>
            </div>
          </div>
        </main>
      </div>
    );
  }

  // Show error state
  if (error) {
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

        {/* Error Content */}
        <main className="flex-1 flex items-center justify-center">
          <div className="text-center bg-red-500/20 backdrop-blur-md rounded-xl p-8 border border-red-500/50 max-w-md">
            <div className="text-6xl mb-4">❌</div>
            <h2 className="text-2xl font-bold text-white mb-4">Error Processing Summary</h2>
            <p className="text-white/80 mb-6">{error}</p>
            <Link href="/dashboard">
              <button className="px-6 py-3 bg-white text-indigo-900 rounded-lg font-semibold hover:bg-white/90 transition-colors">
                Return to Dashboard
              </button>
            </Link>
          </div>
        </main>
      </div>
    );
  }

  // Show success state
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
      <main className="flex-1 max-w-6xl mx-auto w-full px-8 py-12 flex items-center justify-center">
        <div className="text-center">
          <div className="text-6xl mb-6">✓</div>
          <h2 className="text-4xl font-bold text-white mb-4">Execution Confirmed!</h2>
          <p className="text-white/80 text-xl mb-8">Your files have been successfully processed and stored.</p>
          {isComplete && (
            <>
              <p className="text-green-300 text-lg mb-4">✅ Data has been pushed to Google Sheets!</p>
              {sheetUrl && (
                <a 
                  href={sheetUrl} 
                  target="_blank" 
                  rel="noopener noreferrer"
                  className="inline-block mt-4 px-6 py-3 bg-green-600 hover:bg-green-700 text-white rounded-lg font-semibold transition-colors"
                >
                  📊 Open Google Sheet
                </a>
              )}
            </>
          )}
          <div className="mt-6">
            <Link href="/dashboard">
              <button className="px-8 py-3 bg-white text-indigo-900 rounded-lg font-semibold hover:bg-white/90 transition-colors">
                Return to Dashboard
              </button>
            </Link>
          </div>
        </div>
      </main>
    </div>
  );
}

export default function ConfirmedPage() {
  return (
    <Suspense fallback={
      <div className="min-h-screen bg-gradient-to-br from-blue-50 via-white to-purple-50 flex items-center justify-center">
        <div className="text-center">
          <div className="inline-block animate-spin rounded-full h-16 w-16 border-t-4 border-b-4 border-blue-500 mb-4"></div>
          <h2 className="text-2xl font-semibold text-gray-800 mb-2">Loading...</h2>
        </div>
      </div>
    }>
      <ConfirmedPageContent />
    </Suspense>
  );
}
