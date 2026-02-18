'use client';

import { useRouter } from 'next/navigation';
import { useAuth } from '@/context/AuthContext';
import { useEffect } from 'react';
import Link from 'next/link';

export default function DashboardPage() {
  const { user, isLoggedIn, loading, logout } = useAuth();
  const router = useRouter();

  useEffect(() => {
    // Wait for auth to fully load before making any decisions
    if (!loading) {
      // Only redirect if we're sure the user is not logged in
      if (!isLoggedIn || !user) {
        router.push('/login');
      }
    }
  }, [isLoggedIn, loading, user, router]);

  const handleLogout = () => {
    logout();
    router.push('/');
  };

  // Show loading while auth is being checked
  if (loading) {
    return (
      <div className="min-h-screen bg-gradient-to-br from-indigo-900 via-purple-900 to-pink-800 flex items-center justify-center">
        <p className="text-white">Loading...</p>
      </div>
    );
  }

  // Only show this if we're sure user is not logged in (after loading is complete)
  if (!isLoggedIn || !user) {
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
          <button
            onClick={handleLogout}
            className="px-6 py-2 bg-red-500 hover:bg-red-600 text-white rounded-lg font-semibold transition-colors"
          >
            Logout
          </button>
        </div>
      </header>

      {/* Main Content */}
      <main className="flex-1 max-w-7xl mx-auto w-full px-8 py-12">
        <div className="bg-white/10 backdrop-blur-md rounded-2xl p-8 border border-white/20">
          {/* Welcome Section */}
          <div className="mb-8">
            <h2 className="text-4xl font-bold text-white mb-2">Welcome!</h2>
            <p className="text-white/80 text-lg">
              Logged in as: <span className="font-semibold text-white">{user.username}</span>
            </p>
          </div>

          {/* Dashboard Content - Reordered per requirements */}
          <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
            {/* Card 1: Convenience Store E-FORM (Agent Application Form) - CLICKABLE (External Link) */}
            <a 
              href="https://prefill-insurance-forms-automated-data-prefill-s-production.up.railway.app/" 
              target="_blank" 
              rel="noopener noreferrer"
            >
              <div className="bg-gradient-to-br from-white/10 to-white/5 border border-white/30 rounded-lg p-6 hover:from-white/15 hover:to-white/10 transition cursor-pointer h-full">
                <h3 className="text-xl font-semibold text-white mb-3">🏪 C-store Eform - Agent Application</h3>
                <p className="text-white/70">
                  Complete insurance application form with automated data prefill from property databases.
                </p>
                <div className="mt-4 text-white/60 text-sm flex items-center gap-2">
                  <span>Click to start →</span>
                </div>
              </div>
            </a>

            {/* Card 2: Convenience Store E-FORM (Client Application Form) - CLICKABLE (External Link) */}
            <a 
              href="https://insure-cstore-form-production.up.railway.app/" 
              target="_blank" 
              rel="noopener noreferrer"
            >
              <div className="bg-gradient-to-br from-white/10 to-white/5 border border-white/30 rounded-lg p-6 hover:from-white/15 hover:to-white/10 transition cursor-pointer h-full">
                <h3 className="text-xl font-semibold text-white mb-3">📋 C-store Eform - Client Application</h3>
                <p className="text-white/70">
                  Complete insurance application form with automated data prefill and CRM integration.
                </p>
                <div className="mt-4 text-white/60 text-sm flex items-center gap-2">
                  <span>Click to start →</span>
                </div>
              </div>
            </a>

            {/* Card 3: Cover Sheet - CLICKABLE (External Link) */}
            <a 
              href="https://carrier-submission-tracker-system-for-insurance-production.up.railway.app/login" 
              target="_blank" 
              rel="noopener noreferrer"
            >
              <div className="bg-gradient-to-br from-white/10 to-white/5 border border-white/30 rounded-lg p-6 hover:from-white/15 hover:to-white/10 transition cursor-pointer h-full">
                <h3 className="text-xl font-semibold text-white mb-3">📄 Cover Sheet</h3>
                <p className="text-white/70">
                  Generate and review cover sheet / market summary.
                </p>
                <div className="mt-4 text-white/60 text-sm flex items-center gap-2">
                  <span>Click to start →</span>
                </div>
              </div>
            </a>

            {/* Card 4: Generate Summary - CLICKABLE */}
            <Link href="/summary">
              <div className="bg-gradient-to-br from-white/10 to-white/5 border border-white/30 rounded-lg p-6 hover:from-white/15 hover:to-white/10 transition cursor-pointer h-full">
                <h3 className="text-xl font-semibold text-white mb-3">📝 Generate Summary</h3>
                <p className="text-white/70">
                  AI-Powered policy summary for client presentation.
                </p>
                <div className="mt-4 text-white/60 text-sm flex items-center gap-2">
                  <span>Click to start →</span>
                </div>
              </div>
            </Link>

            {/* Card 5: QC (New) - Unified Certificate + Policy */}
            <Link href="/qc-new">
              <div className="bg-gradient-to-br from-white/10 to-white/5 border border-white/30 rounded-lg p-6 hover:from-white/15 hover:to-white/10 transition cursor-pointer h-full">
                <h3 className="text-xl font-semibold text-white mb-3">🆕 QC Review (New) – Certificate + Policy + Accord </h3>
                <p className="text-white/70">
                  Side by side comparison and quality control review
                </p>
                <div className="mt-4 text-white/60 text-sm flex items-center gap-2">
                  <span>Click to start →</span>
                </div>
              </div>
            </Link>

            {/* Card 6: Non C-Store Application - Restaurant, Spa, Saloon, Shopping Center */}
            <a 
              href="https://noncstoreprefillform-production.up.railway.app/" 
              target="_blank" 
              rel="noopener noreferrer"
            >
              <div className="bg-gradient-to-br from-white/10 to-white/5 border border-white/30 rounded-lg p-6 hover:from-white/15 hover:to-white/10 transition cursor-pointer h-full">
                <h3 className="text-xl font-semibold text-white mb-3">🏢 Non C-Store Application</h3>
                <p className="text-white/70">
                  Complete insurance application form for restaurant, spa, saloon, shopping center and other commercial properties.
                </p>
                <div className="mt-4 text-white/60 text-sm flex items-center gap-2">
                  <span>Click to start →</span>
                </div>
              </div>
            </a>

            {/* Card 7: Feedback */}
            <Link href="/feedback">
              <div className="bg-gradient-to-br from-white/10 to-white/5 border border-white/30 rounded-lg p-6 hover:from-white/15 hover:to-white/10 transition cursor-pointer h-full">
                <h3 className="text-xl font-semibold text-white mb-3">💬 Feedback</h3>
                <p className="text-white/70">
                  Share your thoughts, report issues, or suggest improvements.
                </p>
                <div className="mt-4 text-white/60 text-sm flex items-center gap-2">
                  <span>Click to start →</span>
                </div>
              </div>
            </Link>
          </div>
        </div>
      </main>
    </div>
  );
}
