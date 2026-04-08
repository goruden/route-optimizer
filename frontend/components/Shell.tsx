"use client";
import { useEffect, useState, useCallback } from "react";
import dynamic from "next/dynamic";
import { useApp } from "@/lib/state";
import * as api from "@/lib/api";
import { useInactivityLogout } from "@/lib/useInactivityLogout";
import { Sidebar } from "./Sidebar";
import { RoutesPanel } from "./RoutesPanel";
import { StopsPanel } from "./StopsPanel";
import { UnservedPanel } from "./UnservedPanel";
import { Toaster, Btn, Confirm } from "./ui";
import { MapIcon, VehicleIcon, LocationIcon, WarningIcon, ClockIcon, MoneyIcon, LightningIcon, RulerIcon, BalanceIcon } from "./icons";

const MapPanel = dynamic(() => import("./MapPanel"), { ssr: false });

const TABS = [
  { key: "map",      icon: <MapIcon size="size-5" />,  label: "Газрын зураг"      },
  { key: "routes",   icon: <VehicleIcon size="size-5" />,  label: "Чиглэл"   },
  { key: "stops",    icon: <LocationIcon size="size-5" />,  label: "Зогсоол"    },
  { key: "unserved", icon: <WarningIcon size="size-5" />, label: "Үлдсэн" },
] as const;

const LOGIN_PATH =
  (process.env.NEXT_PUBLIC_BASE_PATH ?? "/route-optimizer") + "/login";

// ── Warning dialog — shown 5 min before auto-logout ───────────
function InactivityWarning({
  open,
  secondsLeft,
  onStay,
  onLogout,
}: {
  open: boolean;
  secondsLeft: number;
  onStay: () => void;
  onLogout: () => void;
}) {
  if (!open) return null;
  const mins = Math.floor(secondsLeft / 60);
  const secs = secondsLeft % 60;
  const timeStr = mins > 0
    ? `${mins}:${String(secs).padStart(2, "0")}`
    : `${secs}s`;

  return (
    <div className="fixed inset-0 z-9999 flex items-center justify-center p-4">
      <div className="absolute inset-0 bg-slate-900/40 backdrop-blur-sm" />
      <div className="relative bg-white rounded-2xl shadow-2xl p-6 w-full max-w-md text-center">
        <div className="flex items-center justify-center mx-auto text-red-600">
          <ClockIcon size="size-8"/>
        </div>
        <h2 className="text-xl font-extrabold text-slate-900 mb-2">
          Та байна уу?
        </h2>
        <p className="text-sm text-slate-500 mb-1">
          Та үйлдэл хийхгүй байгаа тул таныг автоматаар гаргах гэж байна.
        </p>
        <div
          className={`text-3xl font-mono font-extrabold mb-5 ${
            secondsLeft <= 60 ? "text-red-500" : "text-amber-500"
          }`}
        >
          {timeStr}
        </div>
        <div className="flex gap-3">
          <button
            onClick={onLogout}
            className="flex-1 py-2.5 rounded-xl border border-slate-200 text-[13px] font-semibold text-slate-500 hover:bg-slate-50 transition-colors"
          >
            Гарах
          </button>
          <button
            onClick={onStay}
            className="flex-1 py-2.5 rounded-xl bg-red-500 text-white text-[13px] font-bold hover:bg-red-600 transition-colors shadow-md"
          >
            Үлдэх
          </button>
        </div>
      </div>
    </div>
  );
}

// ═══════════════════════════════════════════════════════════════
//  Shell
// ═══════════════════════════════════════════════════════════════

export function Shell() {
  const { s, d } = useApp();

  // ── Warning dialog state ─────────────────────────────────────
  const [showWarning, setShowWarning]   = useState(false);
  const [countdown,   setCountdown]     = useState(300); // 5 min = 300 s
  const WARNING_MINUTES = 5;
  const TIMEOUT_MINUTES = 30;

  // ── Logout handler ───────────────────────────────────────────
  const handleLogout = useCallback(async () => {
    setShowWarning(false);
    d({ t: "AUTH_LOGOUT" });
    await api.logout();
    window.location.href = LOGIN_PATH;
  }, [d]);

  // ── "Stay logged in" — reset activity + refresh token ────────
  const handleStay = useCallback(async () => {
    setShowWarning(false);
    setCountdown(WARNING_MINUTES * 60);
    await api.refreshToken();
  }, []);

  // ── Token refresh on activity (when < 5 min left) ────────────
  const handleActivity = useCallback(async () => {
    const left = api.tokenSecondsLeft();
    if (left > 0 && left < WARNING_MINUTES * 60) {
      await api.refreshToken();
    }
  }, []);

  // ── Inactivity hook ──────────────────────────────────────────
  const { resetActivity } = useInactivityLogout({
    timeoutMinutes : TIMEOUT_MINUTES,
    warningMinutes : WARNING_MINUTES,
    enabled        : s.auth.isAuthenticated,
    onLogout       : handleLogout,
    onWarning      : () => {
      setCountdown(WARNING_MINUTES * 60);
      setShowWarning(true);
    },
    onActivity     : handleActivity,
  });

  // ── Countdown ticker inside the warning dialog ───────────────
  useEffect(() => {
    if (!showWarning) return;
    const id = setInterval(() => {
      setCountdown(prev => {
        if (prev <= 1) {
          clearInterval(id);
          handleLogout();
          return 0;
        }
        return prev - 1;
      });
    }, 1000);
    return () => clearInterval(id);
  }, [showWarning, handleLogout]);

  // ── Initial data load ────────────────────────────────────────
  useEffect(() => {
    if (!s.auth.isAuthenticated) return;
    api.getHealth()
      .then(r => d({ t: "SET_HEALTH", h: "ok", o: r.osrm === "connected" ? "connected" : "unreachable" }))
      .catch(() => d({ t: "SET_HEALTH", h: "err", o: "unreachable" }));
    api.getDatasets().then(v => d({ t: "SET_DATASETS", v })).catch(() => {});
    api.getJobs().then(v => d({ t: "SET_JOBS", v })).catch(() => {});
    api.getRunGroups().then(v => d({ t: "SET_GROUPS", v })).catch(() => {});
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [s.auth.isAuthenticated]);

  const {
    summary, routeSummary, routeVis, mapData, fleetFilter,
    activeJobId, mainTab, health, osrm, running, warnings,
  } = s;
  const dotColor = health === "ok" ? "#10B981" : health === "err" ? "#EF4444" : "#F59E0B";

  return (
    <div className="flex h-screen bg-slate-50">

      {/* Inactivity warning dialog */}
      <InactivityWarning
        open={showWarning}
        secondsLeft={countdown}
        onStay={handleStay}
        onLogout={handleLogout}
      />

      {/* Sidebar */}
      <div className="w-[20%] max-w-100 bg-white border-r border-slate-200 flex flex-col">
        {/* Logo / header */}
        <div className="px-4 py-3.5 border-b border-slate-200 bg-white">
          <div className="flex items-center justify-between">
            <div>
              <div className="tracking-tight flex items-center gap-3">
                <img
                  src="/route-optimizer/premium_ologo.svg"
                  alt="Premium Logo"
                  className="h-5"
                />
                {/* <img
                  src="/route-optimizer/logo_with_cu.svg"
                  alt="Premium Logo"
                  className="object-cover max-h-8"
                /> */}
                <span className="text-xl font-extrabold bg-linear-to-l from-red-400 to-red-600 bg-clip-text text-transparent">
                  Digital Twin – RPT
                </span>
              </div>
              
            </div>

            {/* User badge //todo drop down or something*/}
            <Confirm
              onConfirm={handleLogout}
              message="Та гарахдаа итгэлтэй байна уу?"
              cancelText="Цуцлах"
              confirmText="Гарах"
            >
              <div className="text-[10px] px-2 py-1 hover:bg-slate-100 rounded-lg cursor-pointer transition-colors text-slate-500 hover:text-slate-900">
                Гарах ⏻
              </div>
            </Confirm>
          </div>
        </div>
        <div className="flex-1 min-h-0 overflow-hidden">
          <Sidebar />
        </div>
      </div>

      {/* Main area */}
      <div className="flex-1 flex flex-col overflow-hidden min-w-0">
        {/* Status bar */}
        <div className="shrink-0 h-11 flex items-center gap-3 px-4 bg-white border-b border-slate-200 shadow-[0_1px_4px_rgba(91,124,250,0.06)] overflow-x-auto">
          <div className="flex items-center gap-2 shrink-0">
            <span className="w-2 h-2 rounded-full inline-block shrink-0" style={{ background: dotColor, boxShadow: health === "ok" ? `0 0 6px ${dotColor}` : "none" }} />
            <span className="text-[11px] text-slate-500 whitespace-nowrap">
              {health === "ok"
                ? `Backend ${osrm === "connected" ? "✓" : "⚠ offline"}`
                : health === "err" ? "Backend unreachable" : "Connecting…"}
            </span>
          </div>
          {summary && <>
            <div className="w-px h-4 bg-slate-200 shrink-0" />
            <Kpi v={summary.total_served}    label="хүргэгдсэн"   c="#10B981" />
            <div className="w-px h-4 bg-slate-200 shrink-0" />
            <Kpi v={summary.total_unserved}  label="үлдсэн" c="#EF4444" />
            <div className="w-px h-4 bg-slate-200 shrink-0" />
            <Kpi v={summary.total_routes}    label="чиглэл"   c="#5B7CFA" />
            <div className="w-px h-4 bg-slate-200 shrink-0" />
            <Kpi
              v={summary.total_man_hours != null
                ? summary.total_man_hours.toLocaleString(undefined, { minimumFractionDigits: 1, maximumFractionDigits: 1 }) + "h"
                : "-"}
              label="хүн цаг" c="#8B5CF6"
            />
            <div className="w-px h-4 bg-slate-200 shrink-0" />
            <Kpi v={summary.total_dist_km.toLocaleString()} label="km" c="#1A1D2E" />
            <div className="w-px h-4 bg-slate-200 shrink-0" />
            <Kpi v={"₮" + Math.round(summary.total_cost).toLocaleString()} label="зардал" c="#F59E0B" />
            <div className="w-px h-4 bg-slate-200 shrink-0" />
            <Kpi
              v={(() => {
                const modeIcons = {
                  cheapest: <div className="flex items-center gap-1 text-orange-600"><MoneyIcon size="size-4" /> Хямд</div>,
                  fastest: <div className="flex items-center gap-1 text-blue-600"><LightningIcon size="size-4" /> Хурдан</div>,
                  shortest: <div className="flex items-center gap-1 text-green-600"><RulerIcon size="size-4" /> Дөт</div>,
                  balanced: <div className="flex items-center gap-1 text-purple-600"><BalanceIcon size="size-4" /> Тэнцвэртэй</div>,
                  geographic: <div className="flex items-center gap-1 text-sky-600"><MapIcon size="size-4" /> Газар зүйн</div>
                };
                return modeIcons[summary.mode as keyof typeof modeIcons] ?? "";
              })()}
              label={''} c="#7B82A0"
            />
          </>}
          {running && <span className="text-[11px] font-semibold text-blue-500 whitespace-nowrap animate-pulse">⏳ Шийдэл бодож байна…</span>}
          {warnings.length > 0 && (
            <span className="text-[11px] font-semibold text-slate-500 whitespace-nowrap" title={warnings.join(" | ")}>
              ⚠ {warnings.length} анхааруулга{warnings.length > 1 ? "ууд" : ""}
            </span>
          )}
          {activeJobId && (
            <a href={api.exportUrl(activeJobId)} download className="ml-auto shrink-0 no-underline">
              <span className="flex items-center gap-1.5 text-[11px] font-bold text-green-500 px-3 py-1 border border-green-500/30 rounded-lg bg-green-500/6 whitespace-nowrap hover:bg-green-500/12 transition-colors">
                ⬇ Татах
              </span>
            </a>
          )}
        </div>

        {/* Visible routes summary bar */}
        <div className="flex items-center gap-3 py-2 px-4 bg-white border-b border-slate-200 shadow-[0_1px_4px_rgba(91,124,250,0.06)] overflow-x-auto">
          <div className="flex items-center gap-2 shrink-0">
            <span className={`w-2 h-2 rounded-full inline-block shrink-0 ${summary && routeSummary && mapData ?
              (routeSummary.filter(route => {
                const mapRoute = mapData.find(m =>
                  m.fleet === route.fleet && m.truck_id === route.truck_id && m.trip_number === route.trip_number
                );
                return mapRoute && routeVis[mapRoute.route_id] !== false && (fleetFilter === "ALL" || route.fleet === fleetFilter);
              }).length > 0 ? "bg-purple-500" : "bg-gray-300") : "bg-gray-300"
            }`} />
            <span className={`text-[11px] whitespace-nowrap ${summary && routeSummary && mapData ?
              (routeSummary.filter(route => {
                const mapRoute = mapData.find(m =>
                  m.fleet === route.fleet && m.truck_id === route.truck_id && m.trip_number === route.trip_number
                );
                return mapRoute && routeVis[mapRoute.route_id] !== false && (fleetFilter === "ALL" || route.fleet === fleetFilter);
              }).length > 0 ? "text-purple-600 font-semibold" : "text-gray-400")
              : "text-gray-400"
            }`}>
              {summary && routeSummary && mapData && (() => {
                const visibleCount = routeSummary.filter(route => {
                  const mapRoute = mapData.find(m =>
                    m.fleet === route.fleet && m.truck_id === route.truck_id && m.trip_number === route.trip_number
                  );
                  return mapRoute && routeVis[mapRoute.route_id] !== false && (fleetFilter === "ALL" || route.fleet === fleetFilter);
                }).length;
                return visibleCount === 0 ? "Чиглэл сонгогдоогүй байна" : "Чиглэлийн тайлан";
              })()}
            </span>
          </div>
          {summary && routeSummary && (() => {
            const visibleRoutes = routeSummary.filter(route => {
              const mapRoute = mapData.find(m =>
                m.fleet === route.fleet && m.truck_id === route.truck_id && m.trip_number === route.trip_number
              );
              return mapRoute && routeVis[mapRoute.route_id] !== false && (fleetFilter === "ALL" || route.fleet === fleetFilter);
            });
            if (visibleRoutes.length === 0) return null;
            const vs = {
              total_served:    visibleRoutes.reduce((a, r) => a + r.stops, 0),
              total_routes:    visibleRoutes.length,
              total_man_hours: visibleRoutes.reduce((a, r) => a + (r.man_hours || 0), 0),
              total_dist_km:   visibleRoutes.reduce((a, r) => a + r.distance_km, 0),
              total_cost:      visibleRoutes.reduce((a, r) => a + r.cost_total, 0),
            };
            return (<>
              <div className="w-px h-4 bg-slate-200 shrink-0" />
              <Kpi v={vs.total_served} label="хүргэгдсэн" c="#10B981" />
              <div className="w-px h-4 bg-slate-200 shrink-0" />
              <Kpi v={vs.total_routes} label="чиглэл" c="#5B7CFA" />
              <div className="w-px h-4 bg-slate-200 shrink-0" />
              <Kpi v={vs.total_man_hours.toLocaleString(undefined, { minimumFractionDigits: 1, maximumFractionDigits: 1 }) + "h"} label="хүн цаг" c="#8B5CF6" />
              <div className="w-px h-4 bg-slate-200 shrink-0" />
              <Kpi v={vs.total_dist_km.toLocaleString()} label="km" c="#1A1D2E" />
              <div className="w-px h-4 bg-slate-200 shrink-0" />
              <Kpi v={"₮" + Math.round(vs.total_cost).toLocaleString()} label="зардал" c="#F59E0B" />
              <div className="w-px h-4 bg-slate-200 shrink-0" />
            </>);
          })()}
        </div>

        {/* Tab bar */}
        <div className="shrink-0 flex bg-white border-b border-slate-200 px-2">
          {TABS.map(tab => {
            const cnt = tab.key === "routes" ? summary?.total_routes
              : tab.key === "stops" ? summary?.total_served
              : tab.key === "unserved" ? summary?.total_unserved
              : undefined;
            const cntColor = tab.key === "unserved" ? "#EF4444" : tab.key === "stops" ? "#10B981" : "#5B7CFA";
            return (
              <button key={tab.key}
                onClick={() => d({ t: "SET_MAIN", v: tab.key })}
                className={`flex items-center gap-1.5 px-3 py-2.5 text-[12px] font-semibold border-b-[2.5px] transition-all duration-150 ${mainTab === tab.key ? "border-red-500 text-red-500" : "border-transparent text-slate-500 hover:text-slate-900"}`}
              >
                {tab.icon} {tab.label}
                {cnt != null && cnt > 0 && (
                  <span className="text-[9px] font-extrabold px-1.5 py-0.5 rounded-full font-mono"
                    style={{ background: cntColor + "18", color: cntColor }}>
                    {cnt}
                  </span>
                )}
              </button>
            );
          })}
        </div>

        {/* Panels */}
        <div className="flex-1 overflow-hidden relative">
          <div className={`absolute inset-0 ${mainTab === "map" ? "block" : "hidden"}`}>
            <MapPanel />
          </div>
          {mainTab === "routes"   && <RoutesPanel />}
          {mainTab === "stops"    && <StopsPanel />}
          {mainTab === "unserved" && <UnservedPanel />}
        </div>
      </div>

      <Toaster />
    </div>
  );
}

function Kpi({ v, label, c }: { v: React.ReactNode; label: string; c: string }) {
  return (
    <div className="flex items-baseline gap-1 shrink-0">
      <span className="font-mono text-[13px] font-bold" style={{ color: c }}>{v}</span>
      <span className="text-[10px] text-slate-500">{label}</span>
    </div>
  );
}