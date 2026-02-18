'use client'

import { useEffect, useState } from 'react'
import { useRouter, usePathname } from 'next/navigation'

export function EformsAuthProvider({ children }: { children: React.ReactNode }) {
  const [isAuthenticated, setIsAuthenticated] = useState(false)
  const [isLoading, setIsLoading] = useState(true)
  const router = useRouter()
  const pathname = usePathname()

  useEffect(() => {
    // Check if user is authenticated
    const agentProfile = localStorage.getItem('agentProfile')
    
    if (!agentProfile && pathname !== '/cstore/auth') {
      // Not authenticated and not on auth page - redirect to auth
      router.push('/cstore/auth')
    } else if (agentProfile && pathname === '/cstore/auth') {
      // Already authenticated and on auth page - redirect to cstore home
      router.push('/cstore')
    } else {
      setIsAuthenticated(!!agentProfile)
    }
    
    setIsLoading(false)
  }, [pathname, router])

  if (isLoading) {
    return (
      <div className="min-h-screen bg-white flex items-center justify-center">
        <div className="text-center">
          <div className="w-8 h-8 border-2 border-black border-t-transparent rounded-full animate-spin mx-auto"></div>
        </div>
      </div>
    )
  }

  return <>{children}</>
}
