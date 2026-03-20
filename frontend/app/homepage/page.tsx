'use client';

import { useState } from 'react';

export default function HomePage() {
  const [response, setResponse] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);

  const callBackend = async () => {
    setLoading(true);
    try {
      // Call your FastAPI backend endpoint
      // Check if running on Vercel (production) or locally
      const isVercel = typeof window !== 'undefined' && window.location.hostname !== 'localhost';
      const API_URL = isVercel 
        ? 'https://insurance-backend.duckdns.org'  // HTTPS backend with SSL
        : 'https://insurance-ai-pipeline-document-processing-production.up.railway.app';  // Railway backend
      
      console.log('Calling API:', `${API_URL}/`);
      
      // Add headers to bypass ngrok warning page (free tier)
      const res = await fetch(`${API_URL}/`, {
        headers: {
          'ngrok-skip-browser-warning': 'true',
          'Content-Type': 'application/json',
        },
      });
      
      if (!res.ok) {
        throw new Error(`HTTP error! status: ${res.status}`);
      }
      
      const data = await res.json();
      setResponse(data.message);
    } catch (error: any) {
      const errorMessage = error.message || 'Error connecting to backend';
      setResponse(`Error: ${errorMessage}. Check if backend and ngrok are running.`);
      console.error('API Error:', error);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="min-h-screen bg-gradient-to-br from-indigo-900 via-purple-900 to-pink-800 flex flex-col">
      {/* Header */}
      <header className="w-full py-6 px-8">
        <h1 className="text-3xl font-bold text-white tracking-wide">Mckinney and Co</h1>
      </header>

      {/* Main Content */}
      <main className="flex-1 flex items-center justify-center">
        <div className="text-center space-y-6">
          <h2 className="text-4xl font-bold text-white mb-4">Welcome to Homepage</h2>
          
          <button
            onClick={callBackend}
            disabled={loading}
            className="px-8 py-4 bg-white text-indigo-900 rounded-full font-semibold text-lg hover:bg-indigo-100 hover:scale-110 transition-all duration-300 shadow-2xl hover:shadow-white/50 disabled:opacity-50 disabled:cursor-not-allowed"
          >
            {loading ? 'Calling API...' : 'Call Backend Endpoint'}
          </button>

          {response && (
            <div className="mt-6 p-4 bg-white/10 backdrop-blur-sm rounded-lg border border-white/20">
              <p className="text-white font-medium">Response:</p>
              <p className="text-white/90 mt-2">{response}</p>
            </div>
          )}
        </div>
      </main>
    </div>
  );
}

