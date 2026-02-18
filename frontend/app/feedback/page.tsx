'use client';

import { useState, useEffect } from 'react';
import { useRouter } from 'next/navigation';
import { useAuth } from '@/context/AuthContext';
import Link from 'next/link';

export default function FeedbackPage() {
  const { user } = useAuth();
  const router = useRouter();
  const [subject, setSubject] = useState('');
  const [message, setMessage] = useState('');
  const [loading, setLoading] = useState(false);
  const [success, setSuccess] = useState(false);
  const [error, setError] = useState('');
  const [apiUrl, setApiUrl] = useState<string>('http://localhost:8000');

  // Set API URL based on environment (same pattern as other pages)
  useEffect(() => {
    const isProduction = typeof window !== 'undefined' && window.location.hostname !== 'localhost';
    const url = isProduction
      ? (process.env.NEXT_PUBLIC_API_URL || 'https://deployment-production-7739.up.railway.app')
      : 'http://localhost:8000';
    setApiUrl(url);
  }, []);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    
    if (subject.trim().length < 3) {
      setError('Subject must be at least 3 characters long');
      return;
    }

    if (message.trim().length < 10) {
      setError('Message must be at least 10 characters long');
      return;
    }

    setLoading(true);
    setError('');

    try {
      const formData = new FormData();
      formData.append('subject', subject.trim());
      formData.append('message', message.trim());

      const response = await fetch(`${apiUrl}/feedback/send/`, {
        method: 'POST',
        headers: {
          'X-User-ID': user?.username || 'Anonymous',
        },
        body: formData,
      });

      // Check if response is JSON
      let data;
      try {
        data = await response.json();
      } catch (jsonError) {
        // If not JSON, get text response
        const text = await response.text();
        throw new Error(`Server error (${response.status}): ${text || 'Unknown error'}`);
      }

      if (!response.ok) {
        throw new Error(data.detail || data.message || `Failed to send feedback (${response.status})`);
      }

      setSuccess(true);
      setSubject('');
      setMessage('');
      
      // Redirect to dashboard after 2 seconds
      setTimeout(() => {
        router.push('/dashboard');
      }, 2000);
    } catch (err: any) {
      console.error('Feedback submission error:', err);
      let errorMessage = 'Failed to send feedback. Please try again.';
      
      if (err.message) {
        errorMessage = err.message;
      } else if (err instanceof TypeError && err.message.includes('fetch')) {
        errorMessage = 'Cannot connect to server. Please check your connection and try again.';
      }
      
      setError(errorMessage);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="min-h-screen bg-gradient-to-br from-indigo-900 via-purple-900 to-pink-800 flex flex-col">
      {/* Header */}
      <header className="bg-black/20 backdrop-blur-md border-b border-white/10">
        <div className="max-w-7xl mx-auto px-8 py-6 flex justify-between items-center">
          <Link href="/dashboard">
            <h1 className="text-3xl font-bold text-white cursor-pointer hover:text-indigo-200 transition">
              Mckinney and Co
            </h1>
          </Link>
          <Link href="/dashboard">
            <button className="px-6 py-2 bg-indigo-600 hover:bg-indigo-700 text-white rounded-lg font-semibold transition-colors">
              Back to Dashboard
            </button>
          </Link>
        </div>
      </header>

      {/* Main Content */}
      <main className="flex-1 max-w-2xl mx-auto w-full px-8 py-12">
        <div className="bg-white/10 backdrop-blur-md rounded-2xl p-8 border border-white/20">
          <h2 className="text-3xl font-bold text-white mb-2">Submit Feedback</h2>
          <p className="text-white/70 mb-8">
            We'd love to hear from you! Share your thoughts, report issues, or suggest improvements.
          </p>

          {success ? (
            <div className="text-center py-8">
              <div className="text-6xl mb-4">✅</div>
              <p className="text-xl text-white font-semibold mb-2">
                Thank you for your feedback!
              </p>
              <p className="text-white/70">
                Redirecting to dashboard...
              </p>
            </div>
          ) : (
            <form onSubmit={handleSubmit} className="space-y-6">
              <div>
                <label className="block text-sm font-medium text-white mb-2">
                  Subject *
                </label>
                <input
                  type="text"
                  value={subject}
                  onChange={(e) => setSubject(e.target.value)}
                  placeholder="Brief description of your feedback..."
                  className="w-full px-4 py-3 rounded-lg bg-white/20 border border-white/30 text-white placeholder-white/60 focus:outline-none focus:ring-2 focus:ring-indigo-500 focus:border-transparent"
                  required
                  minLength={3}
                />
              </div>

              <div>
                <label className="block text-sm font-medium text-white mb-2">
                  Message *
                </label>
                <textarea
                  value={message}
                  onChange={(e) => setMessage(e.target.value)}
                  placeholder="Share your detailed feedback, report issues, or suggest improvements..."
                  rows={8}
                  className="w-full px-4 py-3 rounded-lg bg-white/20 border border-white/30 text-white placeholder-white/60 focus:outline-none focus:ring-2 focus:ring-indigo-500 focus:border-transparent resize-none"
                  required
                  minLength={10}
                />
                <p className="text-xs text-white/50 mt-1">
                  {message.length}/500 characters (minimum 10)
                </p>
              </div>

              {error && (
                <div className="bg-red-500/20 border border-red-500/50 text-red-200 px-4 py-3 rounded-lg">
                  {error}
                </div>
              )}

              <div className="flex gap-4">
                <Link
                  href="/dashboard"
                  className="flex-1 px-6 py-3 border border-white/30 text-white rounded-lg hover:bg-white/10 transition text-center"
                >
                  Cancel
                </Link>
                <button
                  type="submit"
                  disabled={loading || subject.trim().length < 3 || message.trim().length < 10}
                  className="flex-1 px-6 py-3 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 transition disabled:opacity-50 disabled:cursor-not-allowed"
                >
                  {loading ? 'Sending...' : 'Send Feedback'}
                </button>
              </div>
            </form>
          )}
        </div>
      </main>
    </div>
  );
}
