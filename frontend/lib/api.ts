/**
 * api.ts  –  API client with JWT authentication
 *
 * Token storage:  localStorage["vrp_token"]
 * Token expiry:   localStorage["vrp_token_exp"]  (Unix ms timestamp)
 *
 * Every request automatically adds: Authorization: Bearer <token>
 * On 401 response → token cleared → redirected to /login
 */

import type { Dataset, Store, Vehicle, Job, JobResult } from "@/types/vrp";

const BASE = process.env.NEXT_PUBLIC_API_URL ?? "http://localhost:8067";
export const WS_BASE = BASE.replace(/^https/, "wss").replace(/^http/, "ws");

const LOGIN_PATH =
  (process.env.NEXT_PUBLIC_BASE_PATH ?? "/route-optimizer") + "/login";

// ═══════════════════════════════════════════════════════════
//  Token helpers
// ═══════════════════════════════════════════════════════════

export function getToken(): string | null {
  if (typeof window === "undefined") return null;
  return localStorage.getItem("vrp_token");
}

export function setToken(token: string, expiresInSeconds: number): void {
  const expMs = Date.now() + expiresInSeconds * 1000;
  localStorage.setItem("vrp_token", token);
  localStorage.setItem("vrp_token_exp", String(expMs));
}

export function clearToken(): void {
  localStorage.removeItem("vrp_token");
  localStorage.removeItem("vrp_token_exp");
  localStorage.removeItem("vrp_auth");
}

/** Returns seconds until token expiry (negative = already expired) */
export function tokenSecondsLeft(): number {
  const exp = Number(localStorage.getItem("vrp_token_exp") ?? "0");
  return Math.floor((exp - Date.now()) / 1000);
}

export function isTokenValid(): boolean {
  return getToken() !== null && tokenSecondsLeft() > 0;
}

// ═══════════════════════════════════════════════════════════
//  Core fetch wrapper
// ═══════════════════════════════════════════════════════════

async function req<T>(url: string, opts: RequestInit = {}): Promise<T> {
  const token = getToken();

  // Merge headers — preserve any Content-Type set by caller
  const headers: Record<string, string> = {
    ...(opts.headers as Record<string, string> ?? {}),
  };
  if (token) {
    headers["Authorization"] = `Bearer ${token}`;
  }

  const r = await fetch(BASE + url, { ...opts, headers });

  // 401 → session expired or invalid → kick to login
  if (r.status === 401) {
    clearToken();
    if (typeof window !== "undefined") {
      window.location.href = LOGIN_PATH;
    }
    throw new Error("Session expired — please log in again");
  }

  if (!r.ok) {
    let msg = `HTTP ${r.status}`;
    try {
      const e = await r.json();
      msg = e.detail ?? msg;
    } catch {}
    throw new Error(msg);
  }

  return r.json() as Promise<T>;
}

// ═══════════════════════════════════════════════════════════
//  Auth API
// ═══════════════════════════════════════════════════════════

export interface AuthResponse {
  access_token: string;
  token_type: string;
  expires_in: number;   // seconds
  username: string;
}

/** Log in with username + password. Stores token automatically. */
export async function login(username: string, password: string): Promise<AuthResponse> {
  const r = await fetch(`${BASE}/api/auth/login`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ username, password }),
  });

  if (!r.ok) {
    let msg = "Invalid username or password";
    try {
      const e = await r.json();
      msg = e.detail ?? msg;
    } catch {}
    throw new Error(msg);
  }

  const data: AuthResponse = await r.json();
  setToken(data.access_token, data.expires_in);
  return data;
}

/**
 * Refresh the current token.
 * Called automatically when the token has < 5 minutes left.
 * Silent — does not redirect on failure (token might still be valid
 * for a few more minutes, user will be redirected when it truly expires).
 */
export async function refreshToken(): Promise<boolean> {
  try {
    const data = await req<AuthResponse>("/api/auth/refresh", { method: "POST" });
    setToken(data.access_token, data.expires_in);
    return true;
  } catch {
    return false;
  }
}

/** Log out — clear token server-side (no-op if server is down) + clear local. */
export async function logout(): Promise<void> {
  try {
    await req("/api/auth/logout", { method: "POST" });
  } catch {}
  clearToken();
}

/** Check token validity with the server. */
export const getMe = () => req<{ username: string }>("/api/auth/me");

// ═══════════════════════════════════════════════════════════
//  Existing API — unchanged except they now send Bearer token
// ═══════════════════════════════════════════════════════════

export const getHealth    = () => req<{ status: string; osrm: string; version: string }>("/api/health");
export const getDatasets  = () => req<Dataset[]>("/api/datasets");
export const deleteDataset = (id: string) => req<{ ok: boolean }>(`/api/datasets/${id}`, { method: "DELETE" });
export const getStores    = (id: string) => req<Store[]>(`/api/datasets/${id}/stores`);
export const getVehicles  = (id: string) => req<Vehicle[]>(`/api/datasets/${id}/vehicles`);
export const getJobs      = (limit = 40) => req<Job[]>(`/api/jobs?limit=${limit}`);
export const getJobResult = (id: string) => req<JobResult & Job>(`/api/jobs/${id}`);
export const deleteJob    = (id: string) => req<{ ok: boolean }>(`/api/jobs/${id}`, { method: "DELETE" });
export const exportUrl    = (id: string) => `${BASE}/api/export/${id}`;
export const exportDatasetUrl = (id: string) => `${BASE}/api/datasets/${id}/export`;

export const createDataset = (name: string, storeFile: File, matrixFile?: File) => {
  const fd = new FormData();
  fd.append("name", name); fd.append("store_file", storeFile);
  if (matrixFile) fd.append("matrix_file", matrixFile);
  return req<Dataset>("/api/datasets", { method: "POST", body: fd });
};
export const uploadMatrix = (id: string, f: File) => {
  const fd = new FormData(); fd.append("matrix_file", f);
  return req<{ ok: boolean }>(`/api/datasets/${id}/matrix`, { method: "POST", body: fd });
};
export const updateStore = (dsId: string, sid: number, body: Partial<Store>) =>
  req<Store>(`/api/datasets/${dsId}/stores/${sid}`, { method: "PUT", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) });
export const deleteStore = (dsId: string, sid: number) =>
  req<{ ok: boolean }>(`/api/datasets/${dsId}/stores/${sid}`, { method: "DELETE" });
export const updateVehicle = (dsId: string, vid: number, body: Partial<Vehicle>) =>
  req<Vehicle>(`/api/datasets/${dsId}/vehicles/${vid}`, { method: "PUT", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) });
export const deleteVehicle = (dsId: string, vid: number) =>
  req<{ ok: boolean }>(`/api/datasets/${dsId}/vehicles/${vid}`, { method: "DELETE" });

export const optimize = (p: {
  dataset_id?: string; store_file?: File; matrix_file?: File;
  mode: string; max_trips: number; solver_time: number;
  max_weight_fill?: number; max_volume_fill?: number;
  group_id?: string; version_name?: string;
  season?: string;
  custom_config?: Record<string, any>;
}) => {
  const fd = new FormData();
  fd.append("mode", p.mode);
  fd.append("max_trips", String(p.max_trips));
  fd.append("solver_time", String(p.solver_time));
  if (p.max_weight_fill !== undefined) fd.append("max_weight_fill", String(p.max_weight_fill));
  if (p.max_volume_fill !== undefined) fd.append("max_volume_fill", String(p.max_volume_fill));
  if (p.dataset_id)   fd.append("dataset_id",   String(p.dataset_id));
  if (p.store_file)   fd.append("store_file",   p.store_file);
  if (p.matrix_file)  fd.append("matrix_file",  p.matrix_file);
  if (p.group_id)     fd.append("group_id",     p.group_id);
  if (p.version_name) fd.append("version_name", p.version_name);
  if (p.season)       fd.append("season",       p.season);
  if (p.custom_config) fd.append("custom_config", JSON.stringify(p.custom_config));
  return req<{ job_id: string; status: string }>("/api/optimize", { method: "POST", body: fd });
};

export async function waitForJob(
  jobId: string,
  { intervalMs = 1500, timeoutMs = 900_000 } = {},
): Promise<JobResult & Job> {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    await new Promise(r => setTimeout(r, intervalMs));
    const job = await getJobResult(jobId);
    if (job.status === "done")  return job;
    if (job.status === "error") throw new Error(job.error_msg ?? "Solver failed");
  }
  throw new Error("Optimization timed out");
}

export const buildMatrix = async (options: {
  datasetId?: number; storeFile?: File; matrixFile?: File; saveToDataset?: boolean;
}): Promise<Blob> => {
  const fd = new FormData();
  if (options.datasetId) fd.append("dataset_id", String(options.datasetId));
  if (options.storeFile) fd.append("store_file", options.storeFile);
  if (options.matrixFile) fd.append("matrix_file", options.matrixFile);
  fd.append("save_to_dataset", String(options.saveToDataset ?? false));
  const token = getToken();
  const headers: Record<string, string> = {};
  if (token) headers["Authorization"] = `Bearer ${token}`;
  const r = await fetch(`${BASE}/api/build-matrix`, { method: "POST", body: fd, headers });
  if (!r.ok) throw new Error(`HTTP ${r.status}`);
  return r.blob();
};

export const rebuildDatasetMatrix = async (datasetId: string): Promise<void> => {
  const fd = new FormData();
  fd.append("dataset_id", String(datasetId));
  fd.append("save_to_dataset", "true");
  const token = getToken();
  const headers: Record<string, string> = {};
  if (token) headers["Authorization"] = `Bearer ${token}`;
  const r = await fetch(`${BASE}/api/build-matrix`, { method: "POST", body: fd, headers });
  if (!r.ok) {
    let msg = `HTTP ${r.status}`;
    try { const e = await r.json(); msg = e.detail ?? msg; } catch {}
    throw new Error(msg);
  }
  await r.blob();
};

export const addStore = (dsId: string, body: Record<string, unknown>) =>
  req<Store>(`/api/datasets/${dsId}/stores`, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) });
export const addVehicle = (dsId: string, body: Record<string, unknown>) =>
  req<Vehicle>(`/api/datasets/${dsId}/vehicles`, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) });

export const fmtSec = (s: number) =>
  `${String(Math.floor(s / 3600)).padStart(2, "0")}:${String(Math.floor((s % 3600) / 60)).padStart(2, "0")}`;

import type { RunGroup } from "@/types/vrp";
export const getRunGroups   = () => req<RunGroup[]>("/api/run-groups");
export const createRunGroup = (name: string, datasetId?: string) =>
  req<RunGroup>("/api/run-groups", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ name, dataset_id: datasetId ?? null }) });
export const renameRunGroup = (id: string, name: string) =>
  req<RunGroup>(`/api/run-groups/${id}`, { method: "PATCH", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ name }) });
export const deleteRunGroup = (id: string) =>
  req<{ ok: boolean }>(`/api/run-groups/${id}`, { method: "DELETE" });
export const patchJobVersion = (jobId: string, body: { version_name?: string; group_id?: string }) =>
  req<{ id: string }>(`/api/jobs/${jobId}/version`, { method: "PATCH", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) });
export const forkJob  = (jobId: string) =>
  req<{ id: string; version_name: string }>(`/api/jobs/${jobId}/fork`, { method: "POST", headers: { "Content-Type": "application/json" }, body: "{}" });
export const patchJobResult = (jobId: string, payload: Record<string, unknown>) =>
  req<{ ok: boolean }>(`/api/jobs/${jobId}/result`, { method: "PATCH", headers: { "Content-Type": "application/json" }, body: JSON.stringify(payload) });

export const createManualJob = (data: {
  title: string;
  routes: Array<{ vehicle_id: string; vehicle_name: string; stops: string[]; route_name?: string; truck_number?: string; contractor?: string; }>;
  is_manual: boolean;
  dataset_id?: string;
}) => req<Job>("/api/jobs/manual", {
  method: "POST",
  headers: { "Content-Type": "application/json" },
  body: JSON.stringify(data),
});