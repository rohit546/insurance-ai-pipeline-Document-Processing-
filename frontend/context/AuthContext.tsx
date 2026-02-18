'use client';

import React, { createContext, useContext, useState, useEffect } from 'react';

interface User {
  username: string;
}

interface AuthContextType {
  user: User | null;
  isLoggedIn: boolean;
  loading: boolean;
  login: (username: string, password: string) => Promise<void>;
  register: (username: string, password: string) => Promise<void>;
  logout: () => void;
}

const AuthContext = createContext<AuthContextType | undefined>(undefined);

export function AuthProvider({ children }: { children: React.ReactNode }) {
  const [user, setUser] = useState<User | null>(null);
  const [isLoggedIn, setIsLoggedIn] = useState(false);
  const [loading, setLoading] = useState(true); // Start as true while checking session

  // Load user from localStorage on mount (shared across all tabs)
  useEffect(() => {
    // Use a small delay to ensure localStorage is accessible
    const loadUser = () => {
      try {
        const storedUser = localStorage.getItem('user');
        if (storedUser) {
          const userData = JSON.parse(storedUser);
          setUser(userData);
          setIsLoggedIn(true);
        }
      } catch (error) {
        console.error('Failed to load stored user:', error);
        // Clear corrupted data
        localStorage.removeItem('user');
      } finally {
        setLoading(false); // Done checking session
      }
    };

    // Load immediately, but also handle SSR case
    if (typeof window !== 'undefined') {
      loadUser();
    } else {
      setLoading(false);
    }
  }, []);

  const getApiUrl = () => {
    if (typeof window !== 'undefined') {
      const hostname = window.location.hostname;
      // Check if running on Vercel or Railway (not localhost)
      const isProduction = hostname !== 'localhost' && hostname !== '127.0.0.1';
      
      // Use environment variable if set, otherwise use default Railway URL
      const apiUrl = isProduction
        ? (process.env.NEXT_PUBLIC_API_URL || 'https://deployment-production-7739.up.railway.app')
        : 'http://localhost:8000';
      
      // Debug logging (only in development)
      if (!isProduction) {
        console.log('[Auth] API URL:', apiUrl);
      }
      
      return apiUrl;
    }
    return 'http://localhost:8000';
  };

  const login = async (username: string, password: string) => {
    setLoading(true);
    try {
      const apiUrl = getApiUrl();
      const loginUrl = `${apiUrl}/login/`;
      
      // Debug logging
      console.log('[Auth] Attempting login to:', loginUrl);
      
      const formData = new FormData();
      formData.append('username', username);
      formData.append('password', password);

      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), 30000); // 30 second timeout

      const response = await fetch(loginUrl, {
        method: 'POST',
        body: formData,
        headers: {
          'ngrok-skip-browser-warning': 'true',
        },
        signal: controller.signal,
      });
      
      console.log('[Auth] Login response status:', response.status, response.statusText);

      clearTimeout(timeoutId);

      if (!response.ok) {
        let errorMsg = 'Login failed';
        try {
          const error = await response.json();
          errorMsg = error.detail || error.error || errorMsg;
        } catch {
          errorMsg = response.statusText || `HTTP ${response.status}`;
        }
        throw new Error(errorMsg);
      }

      const data = await response.json();
      const userData = { username: data.username };

      setUser(userData);
      setIsLoggedIn(true);
      localStorage.setItem('user', JSON.stringify(userData));
    } catch (error: any) {
      if (error.name === 'AbortError') {
        throw new Error('Request timed out. Please check your connection and try again.');
      }
      if (error.message.includes('Failed to fetch') || error.message.includes('NetworkError')) {
        throw new Error('Cannot connect to server. Please check your connection or try again later.');
      }
      throw new Error(error.message || 'Login failed');
    } finally {
      setLoading(false);
    }
  };

  const register = async (username: string, password: string) => {
    setLoading(true);
    try {
      const apiUrl = getApiUrl();
      const formData = new FormData();
      formData.append('username', username);
      formData.append('password', password);

      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), 30000); // 30 second timeout

      const response = await fetch(`${apiUrl}/register/`, {
        method: 'POST',
        body: formData,
        headers: {
          'ngrok-skip-browser-warning': 'true',
        },
        signal: controller.signal,
      });

      clearTimeout(timeoutId);

      if (!response.ok) {
        let errorMsg = 'Registration failed';
        try {
          const error = await response.json();
          errorMsg = error.detail || error.error || errorMsg;
        } catch {
          errorMsg = response.statusText || `HTTP ${response.status}`;
        }
        throw new Error(errorMsg);
      }

      const data = await response.json();
      const userData = { username: data.username };

      setUser(userData);
      setIsLoggedIn(true);
      localStorage.setItem('user', JSON.stringify(userData));
    } catch (error: any) {
      if (error.name === 'AbortError') {
        throw new Error('Request timed out. Please check your connection and try again.');
      }
      if (error.message.includes('Failed to fetch') || error.message.includes('NetworkError')) {
        throw new Error('Cannot connect to server. Please check your connection or try again later.');
      }
      throw new Error(error.message || 'Registration failed');
    } finally {
      setLoading(false);
    }
  };

  const logout = () => {
    setUser(null);
    setIsLoggedIn(false);
    localStorage.removeItem('user');
  };

  return (
    <AuthContext.Provider value={{ user, isLoggedIn, loading, login, register, logout }}>
      {children}
    </AuthContext.Provider>
  );
}

export function useAuth() {
  const context = useContext(AuthContext);
  if (!context) {
    throw new Error('useAuth must be used within AuthProvider');
  }
  return context;
}
