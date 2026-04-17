"use client";
import { useState, useEffect } from "react";
import { useApp } from "@/lib/state";
import * as api from "@/lib/api";
import { Btn, Input } from "./ui";

const DASHBOARD_PATH =
  process.env.NEXT_PUBLIC_BASE_PATH ?? "/route-optimizer";
const LOGIN_PATH = DASHBOARD_PATH + "/login";

export default function Authentication() {
  const { s, d } = useApp();
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [showPassword, setShowPassword] = useState(false);

  // ── On mount: check if a valid token is already stored ───────
  useEffect(() => {
    if (api.isTokenValid()) {
      // Token exists and hasn't expired — restore session silently
      const saved = localStorage.getItem("vrp_auth");
      if (saved) {
        try {
          const parsed = JSON.parse(saved);
          d({ t: "AUTH_LOGIN_SUCCESS", user: parsed.user ?? "user" });
          return;
        } catch {}
      }
      // Token valid but no stored user info — verify with server
      api.getMe()
        .then(me => d({ t: "AUTH_LOGIN_SUCCESS", user: me.username }))
        .catch(() => {
          api.clearToken();
          d({ t: "AUTH_LOGIN_FAILURE", error: "" });
        });
    } else {
      // Expired or missing token
      api.clearToken();
      d({ t: "AUTH_LOGIN_FAILURE", error: "" });
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // ── Redirect once authenticated ───────────────────────────────
  useEffect(() => {
    if (s.auth.isAuthenticated) {
      window.location.href = DASHBOARD_PATH;
    }
  }, [s.auth.isAuthenticated]);

  const handleLogin = async () => {
    if (!username.trim() || !password) {
      d({ t: "AUTH_LOGIN_FAILURE", error: "Нэр нууц үгийг оруулна уу" });
      return;
    }
    d({ t: "AUTH_LOGIN_START" });
    try {
      const res = await api.login(username.trim(), password);
      d({ t: "AUTH_LOGIN_SUCCESS", user: res.username });
    } catch (e: any) {
      d({ t: "AUTH_LOGIN_FAILURE", error: e.message ?? "Нэвтэрхэд алдаа гарлаа" });
    }
  };

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === "Enter") handleLogin();
  };

  if (s.auth.isAuthenticated) {
    return (
      <div className="min-h-screen bg-white flex items-center justify-center">
        <div className="text-center">
          <div className="w-8 h-8 border-2 border-red-500 border-t-transparent rounded-full animate-spin mx-auto mb-4" />
          <p className="text-black text-xl">Нэвтэрч байна...</p>
        </div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-linear-to-br from-blue-50 to-indigo-100 flex items-center justify-center p-4">
      <div className="bg-white rounded-2xl shadow-xl p-8 w-full max-w-md">

        {/* Logo */}
        <div className="flex flex-col items-center justify-center mb-8 gap-3">
          <img
            src="/route-optimizer/logo_with_cu.svg"
            alt="Premium Logo"
            className="h-10"
          />
          <h1 className="text-3xl font-extrabold bg-linear-to-r from-red-400 to-red-600 bg-clip-text text-transparent">Digital Twin – RPS</h1>
          {/* <p className="text-[12px] text-slate-500 mt-1">Нэвтрэх</p> */}
        </div>

        {/* Form */}
        <div className="space-y-4">
          <Input
            id="username"
            label="Хэрэглэгчийн нэр"
            type="text"
            value={username}
            onChange={e => setUsername(e.target.value)}
            onKeyDown={handleKeyDown}
            placeholder="Username"
            disabled={s.auth.loading}
            autoComplete="username"
            autoFocus
          />

          <div className="relative">
            <Input
              id="password"
              label="Нууц үг"
              type={showPassword ? "text" : "password"}
              value={password}
              onChange={e => setPassword(e.target.value)}
              onKeyDown={handleKeyDown}
              placeholder="Password"
              disabled={s.auth.loading}
              autoComplete="current-password"
            />
            <button
              type="button"
              onClick={() => setShowPassword(v => !v)}
              className="absolute right-3 bottom-2 text-slate-400 hover:text-slate-600 text-[12px]"
              tabIndex={-1}
            >
              {showPassword 
                ? <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" strokeWidth="1.5" stroke="currentColor" className="size-4">
                    <path strokeLinecap="round" strokeLinejoin="round" d="M13.5 10.5V6.75a4.5 4.5 0 1 1 9 0v3.75M3.75 21.75h10.5a2.25 2.25 0 0 0 2.25-2.25v-6.75a2.25 2.25 0 0 0-2.25-2.25H3.75a2.25 2.25 0 0 0-2.25 2.25v6.75a2.25 2.25 0 0 0 2.25 2.25Z" />
                  </svg>

                : <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" strokeWidth="1.5" stroke="currentColor" className="size-4">
                    <path strokeLinecap="round" strokeLinejoin="round" d="M16.5 10.5V6.75a4.5 4.5 0 1 0-9 0v3.75m-.75 11.25h10.5a2.25 2.25 0 0 0 2.25-2.25v-6.75a2.25 2.25 0 0 0-2.25-2.25H6.75a2.25 2.25 0 0 0-2.25 2.25v6.75a2.25 2.25 0 0 0 2.25 2.25Z" />
                  </svg>
              }
            </button>
          </div>

          {s.auth.error && (
            <div className="bg-red-50 border border-red-200 rounded-xl p-3">
              <p className="text-red-600 text-[12px] font-medium">⚠ {s.auth.error}</p>
            </div>
          )}

          <Btn
            variant="primary"
            className="w-full mt-2"
            onClick={handleLogin}
            loading={s.auth.loading}
            disabled={!username || !password || s.auth.loading}
          >
            {s.auth.loading ? "Нэвтэрч байна..." : "Нэвтрэх →"}
          </Btn>
        </div>
      </div>
    </div>
  );
}