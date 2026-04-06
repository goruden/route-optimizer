"use client";
import React, { createContext, useContext, useReducer, ReactNode, useEffect } from "react";
import type { Dataset, Store, Vehicle, Job, RunGroup, OptSummary, RouteSummary, StopDetail, UnservedStore, MapRoute, JobResult } from "@/types/vrp";
import * as api from "@/lib/api";

export interface AppState {
  datasets: Dataset[]; activeDatasetId: string | null;
  stores: Store[]; vehicles: Vehicle[];
  jobs: Job[]; activeJobId: string | null; activeGroupId: string | null;
  runGroups: RunGroup[];
  summary: OptSummary | null;
  routeSummary: RouteSummary[]; stopDetails: StopDetail[];
  unserved: UnservedStore[]; mapData: MapRoute[];
  sideTab: "run" | "data" | "history";
  mainTab: "map" | "routes" | "stops" | "unserved";
  selectedStoreNodeId: string | null;
  routeVis: Record<string, boolean>;
  fleetFilter: "ALL" | "DRY" | "COLD";
  running: boolean;
  health: "loading" | "ok" | "err"; osrm: "connected" | "unreachable";
  warnings: string[];
  editMode: boolean;
  auth: {
    isAuthenticated: boolean;
    user: string | null;
    loading: boolean;
    error: string | null;
  };
}

const INIT: AppState = {
  datasets: [], activeDatasetId: null, stores: [], vehicles: [],
  jobs: [], activeJobId: null,
  runGroups: [], activeGroupId: null,
  summary: null,
  routeSummary: [], stopDetails: [], unserved: [], mapData: [],
  sideTab: "run", mainTab: "map", selectedStoreNodeId: null,
  routeVis: {}, fleetFilter: "ALL", running: false,
  health: "loading", osrm: "unreachable", warnings: [],
  editMode: false,
  // Start loading=true so the app waits for token check before showing login
  auth: { isAuthenticated: false, user: null, loading: true, error: null },
};

export type Act =
  | { t: "SET_DATASETS"; v: Dataset[] }  | { t: "SET_DS"; v: string | null }
  | { t: "SET_STORES"; v: Store[] }      | { t: "SET_VEHICLES"; v: Vehicle[] }
  | { t: "SET_JOBS"; v: Job[] }
  | { t: "SET_GROUPS"; v: RunGroup[] }
  | { t: "SET_ACTIVE_GROUP"; v: string | null }
  | { t: "SET_ACTIVE_JOB"; v: string | null }
  | { t: "SET_RESULT"; jobId: string; r: JobResult }
  | { t: "SET_ROUTES"; v: RouteSummary[] }
  | { t: "CLEAR" }
  | { t: "SET_SIDE"; v: AppState["sideTab"] } | { t: "SET_MAIN"; v: AppState["mainTab"] }
  | { t: "SET_SEL"; v: string | null }
  | { t: "SET_RUNNING"; v: boolean }
  | { t: "SET_HEALTH"; h: AppState["health"]; o: AppState["osrm"] }
  | { t: "FLEET"; v: AppState["fleetFilter"] }
  | { t: "TOGGLE_ROUTE"; v: string }
  | { t: "TOGGLE_ALL"; v: boolean }
  | { t: "SET_EDIT"; v: boolean }
  | { t: "AUTH_LOGIN_START" }
  | { t: "AUTH_LOGIN_SUCCESS"; user: string }
  | { t: "AUTH_LOGIN_FAILURE"; error: string }
  | { t: "AUTH_LOGOUT" }
  | { t: "AUTH_SET_STATE"; payload: AppState["auth"] };

function reduce(s: AppState, a: Act): AppState {
  switch (a.t) {
    case "SET_DATASETS":     return { ...s, datasets: a.v };
    case "SET_DS":
      return {
        ...s,
        activeDatasetId: a.v,
        stores:   a.v === null ? [] : s.stores,
        vehicles: a.v === null ? [] : s.vehicles,
      };
    case "SET_STORES":       return { ...s, stores: a.v };
    case "SET_VEHICLES":     return { ...s, vehicles: a.v };
    case "SET_JOBS":         return { ...s, jobs: a.v };
    case "SET_GROUPS":       return { ...s, runGroups: a.v };
    case "SET_ACTIVE_GROUP": return { ...s, activeGroupId: a.v };
    case "SET_ACTIVE_JOB":   return { ...s, activeJobId: a.v };
    case "SET_ROUTES":       return { ...s, routeSummary: a.v };
    case "SET_EDIT":         return { ...s, editMode: a.v };
    case "SET_RUNNING":      return { ...s, running: a.v };
    case "SET_SIDE":         return { ...s, sideTab: a.v };
    case "SET_MAIN":         return { ...s, mainTab: a.v };
    case "SET_SEL":          return { ...s, selectedStoreNodeId: a.v };
    case "SET_HEALTH":       return { ...s, health: a.h, osrm: a.o };
    case "FLEET":            return { ...s, fleetFilter: a.v };
    case "TOGGLE_ROUTE": {
      const cur = s.routeVis[a.v] !== false;
      return { ...s, routeVis: { ...s.routeVis, [a.v]: !cur } };
    }
    case "TOGGLE_ALL": {
      const nxt: Record<string, boolean> = {};
      Object.keys(s.routeVis).forEach(k => { nxt[k] = a.v; });
      return { ...s, routeVis: nxt };
    }
    case "SET_RESULT": {
      const vis: Record<string, boolean> = {};
      a.r.map_data.forEach(r => { vis[r.route_id] = true; });
      return {
        ...s, activeJobId: a.jobId,
        summary: a.r.summary, routeSummary: a.r.route_summary,
        stopDetails: a.r.stop_details, unserved: a.r.unserved,
        mapData: a.r.map_data, routeVis: vis,
        warnings: a.r.summary?.warnings ?? [],
      };
    }
    case "CLEAR":
      return { ...s, activeJobId: null, summary: null, routeSummary: [], stopDetails: [], unserved: [], mapData: [], routeVis: {}, warnings: [] };

    // ── Auth ──────────────────────────────────────────────────
    case "AUTH_LOGIN_START":
      return { ...s, auth: { ...s.auth, loading: true, error: null } };

    case "AUTH_LOGIN_SUCCESS": {
      const newAuth = { isAuthenticated: true, user: a.user, loading: false, error: null };
      // Persist user name alongside the token (token itself stored by api.ts)
      if (typeof window !== "undefined") {
        localStorage.setItem("vrp_auth", JSON.stringify(newAuth));
      }
      return { ...s, auth: newAuth };
    }

    case "AUTH_LOGIN_FAILURE": {
      const newAuth = { isAuthenticated: false, user: null, loading: false, error: a.error };
      return { ...s, auth: newAuth };
    }

    case "AUTH_LOGOUT": {
      if (typeof window !== "undefined") {
        localStorage.removeItem("vrp_auth");
        api.clearToken();
      }
      return {
        ...s,
        auth: { isAuthenticated: false, user: null, loading: false, error: null },
        // Clear all app state on logout
        datasets: [], activeDatasetId: null, stores: [], vehicles: [],
        jobs: [], activeJobId: null, runGroups: [],
        summary: null, routeSummary: [], stopDetails: [], unserved: [],
        mapData: [], routeVis: {}, warnings: [],
      };
    }

    case "AUTH_SET_STATE":
      return { ...s, auth: a.payload };

    default: return s;
  }
}

const Ctx = createContext<{ s: AppState; d: React.Dispatch<Act> } | null>(null);

export function AppProvider({ children }: { children: ReactNode }) {
  const [s, d] = useReducer(reduce, INIT);

  // ── Restore session from localStorage on page load ────────────
  useEffect(() => {
    const token = api.getToken();
    if (!token || !api.isTokenValid()) {
      // No token or expired → go to login state (not an error)
      api.clearToken();
      d({ t: "AUTH_SET_STATE", payload: { isAuthenticated: false, user: null, loading: false, error: null } });
      return;
    }
    // Token looks good — restore user info from localStorage
    const saved = localStorage.getItem("vrp_auth");
    if (saved) {
      try {
        const parsed = JSON.parse(saved);
        if (parsed.isAuthenticated && parsed.user) {
          d({ t: "AUTH_SET_STATE", payload: { ...parsed, loading: false, error: null } });
          return;
        }
      } catch {}
    }
    // Token valid but no stored name — verify with server
    api.getMe()
      .then(me => d({ t: "AUTH_LOGIN_SUCCESS", user: me.username }))
      .catch(() => {
        api.clearToken();
        d({ t: "AUTH_SET_STATE", payload: { isAuthenticated: false, user: null, loading: false, error: null } });
      });
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

  return <Ctx.Provider value={{ s, d }}>{children}</Ctx.Provider>;
}

export function useApp() {
  const c = useContext(Ctx);
  if (!c) throw new Error("useApp outside AppProvider");
  return c;
}

export function stopsForStore(sid: string, stops: StopDetail[]) {
  return stops.filter(d => d.store_id === sid);
}

export function buildBadges(
  stops: StopDetail[], mapData: MapRoute[],
  routeVis: Record<string, boolean>, fleetFilter: string,
): Record<string, Array<{ color: string; label: string }>> {
  const colorMap: Record<string, { color: string; id: string }> = {};
  mapData.forEach(r => {
    colorMap[`${r.fleet}||${r.truck_id}||${r.trip_number}`] = { color: r.color, id: r.route_id };
  });
  const out: Record<string, Array<{ color: string; label: string }>> = {};
  stops.forEach(s => {
    const info = colorMap[`${s.fleet}||${s.truck_id}||${s.trip_number}`];
    if (!info) return;
    if (routeVis[info.id] === false) return;
    if (fleetFilter !== "ALL" && s.fleet !== fleetFilter) return;
    if (!out[s.store_id]) out[s.store_id] = [];
    out[s.store_id].push({ color: info.color, label: `${s.fleet === "DRY" ? "D" : "C"}${s.stop_order}` });
  });
  return out;
}