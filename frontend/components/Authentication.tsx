"use client"
import { useState, useEffect } from "react"
import { useApp } from "@/lib/state"
import { Btn, Input } from "./ui"

export default function Authentication() {
  const { s, d } = useApp()
  const [username, setUsername] = useState("")
  const [password, setPassword] = useState("")

  // Mock user credentials
  const MOCK_USERS: Record<string, string> = {
    "test": "test1234!",
    "admin": "admin",
    "user": "user1234!"
  }

  const handleLogin = async () => {
    if (!username || !password) {
      d({ t: "AUTH_LOGIN_FAILURE", error: "Please enter both username and password" })
      return
    }

    d({ t: "AUTH_LOGIN_START" })

    // Simulate API call delay
    setTimeout(() => {
      if (MOCK_USERS[username] === password) {
        d({ t: "AUTH_LOGIN_SUCCESS", user: username })
      } else {
        d({ t: "AUTH_LOGIN_FAILURE", error: "Invalid username or password" })
      }
    }, 1000)
  }

  const handleKeyPress = (e: React.KeyboardEvent) => {
    if (e.key === "Enter") {
      handleLogin()
    }
  }

  useEffect(() => {
    if (s.auth.isAuthenticated) {
      window.location.href = process.env.NEXT_PUBLIC_BASE_PATH ?? "/route-optimizer"
    }
  }, [s.auth.isAuthenticated])

  if (s.auth.isAuthenticated) {
    return (
      <div className="min-h-screen bg-linear-to-br from-blue-50 to-indigo-100 flex items-center justify-center p-4">
        <div className="text-center">
          <div className="w-8 h-8 border-2 border-blue-500 border-t-transparent rounded-full animate-spin mx-auto mb-4"></div>
          <p className="text-slate-600">Redirecting to dashboard...</p>
        </div>
      </div>
    )
  }

  return (
    <div className="min-h-screen bg-linear-to-br from-blue-50 to-indigo-100 flex items-center justify-center p-4">
      <div className="bg-white rounded-2xl shadow-xl p-8 w-full max-w-md">
        <div className="space-y-6">
          <div>
            <h3 className="text-lg font-semibold text-slate-900 mb-4">Sign In</h3>
            
            <Input
              id="username"
              label="Username"
              type="text"
              value={username}
              onChange={(e) => setUsername(e.target.value)}
              onKeyDown={handleKeyPress}
              placeholder="Enter your username"
              className="mb-4"
              disabled={s.auth.loading}
            />

            <Input
              id="password"
              label="Password"
              type="password"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              onKeyDown={handleKeyPress}
              placeholder="Enter your password"
              disabled={s.auth.loading}
            />
          </div>

          {s.auth.error && (
            <div className="bg-red-50 border border-red-200 rounded-xl p-3">
              <p className="text-red-600 text-sm font-medium">{s.auth.error}</p>
            </div>
          )}

          <Btn
            variant="primary"
            className="w-full"
            onClick={handleLogin}
            loading={s.auth.loading}
            disabled={!username || !password}
          >
            {s.auth.loading ? "Signing in..." : "Sign In"}
          </Btn>
        </div>
      </div>
    </div>
  )
}