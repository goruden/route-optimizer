"use client"
import { useEffect } from "react"
import { Shell } from "@/components/Shell"
import { useApp } from "@/lib/state"

export default function Home() {
  const { s } = useApp()

  useEffect(() => {
    // Redirect to login if not authenticated
    if (!s.auth.loading && !s.auth.isAuthenticated) {
      window.location.href = (process.env.NEXT_PUBLIC_BASE_PATH ?? "/route-optimizer") + "/login"
    }
  }, [s.auth.isAuthenticated, s.auth.loading])

  // Show loading or redirect
  if (!s.auth.isAuthenticated) {
    return (
      <div className="min-h-screen bg-slate-100 flex items-center justify-center">
        <div className="text-center">
          <div className="w-8 h-8 border-2 border-blue-500 border-t-transparent rounded-full animate-spin mx-auto mb-4"></div>
          <p className="text-slate-600">Redirecting to login...</p>
        </div>
      </div>
    )
  }

  return <Shell />
}