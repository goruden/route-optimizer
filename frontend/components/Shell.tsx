"use client";
import { useEffect, useState } from "react";
import dynamic from "next/dynamic";
import { useApp } from "@/lib/state";
import * as api from "@/lib/api";
import { Sidebar } from "./Sidebar";
import { RoutesPanel } from "./RoutesPanel";
import { StopsPanel } from "./StopsPanel";
import { UnservedPanel } from "./UnservedPanel";
import { Toaster, Btn, Confirm } from "./ui";

const MapPanel = dynamic(() => import("./MapPanel"), { ssr: false });

const TABS = [
  { key: "map", icon: "🗺", label: "Map" },
  { key: "routes", icon: "🚚", label: "Routes" },
  { key: "stops", icon: "📍", label: "Stops" },
  { key: "unserved", icon: "⚠️", label: "Unserved" },
] as const;

export function Shell() {
  const { s, d } = useApp();

  useEffect(() => {
    api.getHealth()
      .then(r => d({ t: "SET_HEALTH", h: "ok", o: r.osrm === "connected" ? "connected" : "unreachable" }))
      .catch(() => d({ t: "SET_HEALTH", h: "err", o: "unreachable" }));
    api.getDatasets().then(v => d({ t: "SET_DATASETS", v })).catch(() => { });
    api.getJobs().then(v => d({ t: "SET_JOBS", v })).catch(() => { });
    api.getRunGroups().then(v => d({ t: "SET_GROUPS", v })).catch(() => { });
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const { summary, routeSummary, routeVis, mapData, fleetFilter, activeJobId, mainTab, health, osrm, running, warnings, auth } = s;
  const dotColor = health === "ok" ? "#10B981" : health === "err" ? "#EF4444" : "#F59E0B";

  const handleLogout = () => {
    d({ t: "AUTH_LOGOUT" });
    window.location.href = (process.env.NEXT_PUBLIC_BASE_PATH ?? "/route-optimizer") + "/login"
  };

  return (
    <div className="flex h-screen overflow-hidden bg-slate-50">
      {/* Sidebar */}
      <div className="w-71.5 shrink-0 bg-white border-r border-slate-200 flex flex-col overflow-hidden shadow-[2px_0_12px_rgba(91,124,250,0.06)]">
        {/* Logo */}
        <div className="shrink-0 px-4 py-3.5 border-b border-slate-200 bg-linear-to-br from-slate-50 to-blue-50">
          <div className="flex items-center justify-between">
            <div>
              <div className="text-[15px] font-extrabold tracking-tight leading-tight">
                <span style={{ background: "linear-gradient(135deg,#3B82F6,#0EA5E9)", WebkitBackgroundClip: "text", WebkitTextFillColor: "transparent" }}>
                  Route Optimizer
                </span>
              </div>
              {/* <div className="text-[9px] font-semibold text-slate-400 mt-0.5 tracking-[0.12em] uppercase">
                OR-Tools · OSRM
              </div> */}
            </div>
            <div className="flex items-center gap-2">
              <Confirm onConfirm={handleLogout} message="Are you sure you want to logout?" cancelText="Cancel" confirmText="Logout">
                <div className="text-[10px] px-2 py-1 hover:bg-slate-100 rounded-lg cursor-pointer transition-colors">
                  Logout ⏻
                </div>
              </Confirm>
            </div>
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
          {/* Health dot */}
          <div className="flex items-center gap-2 shrink-0">
            <span className="w-2 h-2 rounded-full inline-block shrink-0" style={{ background: dotColor, boxShadow: health === "ok" ? `0 0 6px ${dotColor}` : "none" }} />
            <span className="text-[11px] text-slate-500 whitespace-nowrap">
              {health === "ok" ? `API OK · OSRM ${osrm === "connected" ? "✓" : "⚠ offline"}` : health === "err" ? "Backend unreachable" : "Connecting…"}
            </span>
          </div>
          {summary && <>
            <div className="w-px h-4 bg-slate-200 shrink-0" />
            <Kpi v={summary.total_served} label="served" c="#10B981" />
            <div className="w-px h-4 bg-slate-200 shrink-0" />
            <Kpi v={summary.total_unserved} label="unserved" c="#EF4444" />
            <div className="w-px h-4 bg-slate-200 shrink-0" />
            <Kpi v={summary.total_routes} label="routes" c="#5B7CFA" />
            <div className="w-px h-4 bg-slate-200 shrink-0" />
            <Kpi v={summary.total_man_hours != null ? summary.total_man_hours.toLocaleString(undefined, { minimumFractionDigits: 1, maximumFractionDigits: 1 }) + "h" : "-"} label="man-hours" c="#8B5CF6" />
            <div className="w-px h-4 bg-slate-200 shrink-0" />
            <Kpi v={summary.total_dist_km.toLocaleString()} label="km" c="#1A1D2E" />
            <div className="w-px h-4 bg-slate-200 shrink-0" />
            <Kpi v={"₮" + Math.round(summary.total_cost).toLocaleString()} label="cost" c="#F59E0B" />
            <div className="w-px h-4 bg-slate-200 shrink-0" />
            <Kpi v={{ cheapest: "💰", fastest: "⚡", shortest: "📏", balanced: "⚖️", geographic: "🗺" }[summary.mode.replace(" (edited)", "")] ?? ""} label={summary.mode.replace(" (edited)", " ✏").toUpperCase()} c="#7B82A0" />
          </>}

          {running && <span className="text-[11px] font-semibold text-blue-500 whitespace-nowrap anim-pulse">⏳ Solving…</span>}

          {warnings.length > 0 && (
            <span className="text-[11px] font-semibold text-slate-500 whitespace-nowrap" title={warnings.join(" | ")}>
              ⚠ {warnings.length} warning{warnings.length > 1 ? "s" : ""}
            </span>
          )}

          {activeJobId && (
            <a href={api.exportUrl(activeJobId)} download className="ml-auto shrink-0 no-underline">
              <span className="flex items-center gap-1.5 text-[11px] font-bold text-green-500 px-3 py-1 border border-green-500/30 rounded-lg bg-green-500/6 whitespace-nowrap hover:bg-green-500/12 transition-colors">
                ⬇ Excel
              </span>
            </a>
          )}
        </div>
        
        <div className="flex items-center gap-3 py-2 px-4 bg-white border-b border-slate-200 shadow-[0_1px_4px_rgba(91,124,250,0.06)] overflow-x-auto">
          <div className="flex items-center gap-2 shrink-0">
            <span className={`w-2 h-2 rounded-full inline-block shrink-0 ${summary && routeSummary && mapData ? 
              (routeSummary.filter(route => {
                const mapRoute = mapData.find(m => 
                  m.fleet === route.fleet && 
                  m.truck_id === route.truck_id && 
                  m.trip_number === route.trip_number
                );
                return mapRoute && routeVis[mapRoute.route_id] !== false && (fleetFilter === "ALL" || route.fleet === fleetFilter);
              }).length > 0 ? "bg-purple-500" : "bg-gray-300") : "bg-gray-300"
            }`} />
            <span className={`text-[11px] whitespace-nowrap ${summary && routeSummary && mapData ? 
              (routeSummary.filter(route => {
                const mapRoute = mapData.find(m => 
                  m.fleet === route.fleet && 
                  m.truck_id === route.truck_id && 
                  m.trip_number === route.trip_number
                );
                return mapRoute && routeVis[mapRoute.route_id] !== false && (fleetFilter === "ALL" || route.fleet === fleetFilter);
              }).length > 0 ? "text-purple-600 font-semibold" : "text-gray-400")
              : "text-gray-400"
            }`}>
              {summary && routeSummary && mapData &&
                (() => {
                  const visibleCount = routeSummary.filter(route => {
                    const mapRoute = mapData.find(m => 
                      m.fleet === route.fleet && 
                      m.truck_id === route.truck_id && 
                      m.trip_number === route.trip_number
                    );
                    return mapRoute && routeVis[mapRoute.route_id] !== false && (fleetFilter === "ALL" || route.fleet === fleetFilter);
                  }).length;
                  
                  if (visibleCount === 0) {
                    return "No routes selected";
                  } else {
                    return "Route Summary";
                  }
                })()
              }
            </span>
          </div>
          {summary && routeSummary && 
            (() => {
              // Calculate summary for visible routes only
              const visibleRoutes = routeSummary.filter(route => {
                const mapRoute = mapData.find(m => 
                  m.fleet === route.fleet && 
                  m.truck_id === route.truck_id && 
                  m.trip_number === route.trip_number
                );
                return mapRoute && routeVis[mapRoute.route_id] !== false && (fleetFilter === "ALL" || route.fleet === fleetFilter);
              });
              
              if (visibleRoutes.length === 0) return null;
              
              const visibleSummary = {
                total_served: visibleRoutes.reduce((sum, route) => sum + route.stops, 0),
                total_routes: visibleRoutes.length,
                total_man_hours: visibleRoutes.reduce((sum, route) => sum + (route.man_hours || 0), 0),
                total_dist_km: visibleRoutes.reduce((sum, route) => sum + route.distance_km, 0),
                total_cost: visibleRoutes.reduce((sum, route) => sum + route.cost_total, 0)
              };
              
              return (
                <>
                  <div className="w-px h-4 bg-slate-200 shrink-0" />
                  <Kpi v={visibleSummary.total_served} label="served" c="#10B981" />
                  <div className="w-px h-4 bg-slate-200 shrink-0" />
                  <Kpi v={visibleSummary.total_routes} label="routes" c="#5B7CFA" />
                  <div className="w-px h-4 bg-slate-200 shrink-0" />
                  <Kpi v={visibleSummary.total_man_hours != null ? visibleSummary.total_man_hours.toLocaleString(undefined, { minimumFractionDigits: 1, maximumFractionDigits: 1 }) + "h" : "-"} label="man-hours" c="#8B5CF6" />
                  <div className="w-px h-4 bg-slate-200 shrink-0" />
                  <Kpi v={visibleSummary.total_dist_km.toLocaleString()} label="km" c="#1A1D2E" />
                  <div className="w-px h-4 bg-slate-200 shrink-0" />
                  <Kpi v={"₮" + Math.round(visibleSummary.total_cost).toLocaleString()} label="cost" c="#F59E0B" />
                  <div className="w-px h-4 bg-slate-200 shrink-0" />
                </>
              );
            })()
          }
        </div>

        {/* Tab bar */}
        <div className="shrink-0 flex bg-white border-b border-slate-200 px-2">
          {TABS.map(tab => {
            const cnt = tab.key === "routes" ? summary?.total_routes : tab.key === "stops" ? summary?.total_served : tab.key === "unserved" ? summary?.total_unserved : undefined;
            const cntColor = tab.key === "unserved" ? "#EF4444" : tab.key === "stops" ? "#10B981" : "#5B7CFA";
            return (
              <button key={tab.key}
                onClick={() => d({ t: "SET_MAIN", v: tab.key })}
                className={`flex items-center gap-1.5 px-3 py-2.5 text-[12px] font-semibold border-b-[2.5px] transition-all duration-150 ${mainTab === tab.key ? "border-blue-500 text-blue-500" : "border-transparent text-slate-500 hover:text-slate-900"}`}
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
          {mainTab === "routes" && <RoutesPanel />}
          {mainTab === "stops" && <StopsPanel />}
          {mainTab === "unserved" && <UnservedPanel />}
        </div>
      </div>

      <Toaster />
    </div>
  );
}

function Kpi({ v, label, c }: { v: string | number; label: string; c: string }) {
  return (
    <div className="flex items-baseline gap-1 shrink-0">
      <span className="font-mono text-[13px] font-bold" style={{ color: c }}>{v}</span>
      <span className="text-[10px] text-slate-500">{label}</span>
    </div>
  );
}