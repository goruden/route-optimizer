import type { Dataset, Store, Vehicle, Job, JobResult } from "@/types/vrp"

const BASE = process.env.NEXT_PUBLIC_API_URL ?? "http://localhost:8067";

async function req<T>(url: string, opts: RequestInit = {}): Promise<T> {
  const r = await fetch(BASE + url, opts);
  if (!r.ok) {
    let msg = `HTTP ${r.status}`;
    try { const e = await r.json(); msg = e.detail ?? msg; } catch {}
    throw new Error(msg);
  }
  return r.json() as Promise<T>;
}

export const getHealth    = () => req<{status:string;osrm:string;version:string}>("/api/health");
export const getDatasets  = () => req<Dataset[]>("/api/datasets");
export const deleteDataset= (id:string) => req<{ok:boolean}>(`/api/datasets/${id}`,{method:"DELETE"});
export const getStores    = (id:string) => req<Store[]>(`/api/datasets/${id}/stores`);
export const getVehicles  = (id:string) => req<Vehicle[]>(`/api/datasets/${id}/vehicles`);
export const getJobs      = (limit=40) => req<Job[]>(`/api/jobs?limit=${limit}`);
export const getJobResult = (id:string) => req<JobResult & Job>(`/api/jobs/${id}`);
export const deleteJob    = (id:string) => req<{ok:boolean}>(`/api/jobs/${id}`,{method:"DELETE"});
export const exportUrl    = (id:string) => `${BASE}/api/export/${id}`;

/** Download stores + vehicles (+ matrix if available) as Excel */
export const exportDatasetUrl = (id:string) => `${BASE}/api/datasets/${id}/export`;

export const createDataset = (name:string, storeFile:File, matrixFile?:File) => {
  const fd = new FormData();
  fd.append("name", name); fd.append("store_file", storeFile);
  if (matrixFile) fd.append("matrix_file", matrixFile);
  return req<Dataset>("/api/datasets", {method:"POST", body:fd});
};
export const uploadMatrix = (id:string, f:File) => {
  const fd = new FormData(); fd.append("matrix_file", f);
  return req<{ok:boolean}>(`/api/datasets/${id}/matrix`, {method:"POST", body:fd});
};
export const updateStore = (dsId:string, sid:number, body:Partial<Store>) =>
  req<Store>(`/api/datasets/${dsId}/stores/${sid}`, {method:"PUT", headers:{"Content-Type":"application/json"}, body:JSON.stringify(body)});
export const deleteStore = (dsId:string, sid:number) =>
  req<{ok:boolean}>(`/api/datasets/${dsId}/stores/${sid}`, {method:"DELETE"});
export const updateVehicle = (dsId:string, vid:number, body:Partial<Vehicle>) =>
  req<Vehicle>(`/api/datasets/${dsId}/vehicles/${vid}`, {method:"PUT", headers:{"Content-Type":"application/json"}, body:JSON.stringify(body)});
export const deleteVehicle = (dsId:string, vid:number) =>
  req<{ok:boolean}>(`/api/datasets/${dsId}/vehicles/${vid}`, {method:"DELETE"});

export const optimize = (p:{
  dataset_id?:number; store_file?:File; matrix_file?:File;
  mode:string; max_trips:number; solver_time:number;
  max_weight_fill?:number; max_volume_fill?:number;
  group_id?:string; version_name?:string;
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
  return req<JobResult>("/api/optimize", {method:"POST", body:fd});
};

export const buildMatrix = async (options: {
  datasetId?: number;
  storeFile?: File;
  matrixFile?: File;
  saveToDataset?: boolean;
}): Promise<Blob> => {
  const fd = new FormData();
  if (options.datasetId) fd.append("dataset_id", String(options.datasetId));
  if (options.storeFile) fd.append("store_file", options.storeFile);
  if (options.matrixFile) fd.append("matrix_file", options.matrixFile);
  fd.append("save_to_dataset", String(options.saveToDataset ?? false));
  const r = await fetch(`${BASE}/api/build-matrix`,{method:"POST",body:fd});
  if (!r.ok) throw new Error(`HTTP ${r.status}`);
  return r.blob();
};

/**
 * Rebuild the OSRM-based distance matrix for a dataset and save it in-place.
 * Does NOT download the file.
 */
export const rebuildDatasetMatrix = async (datasetId: string): Promise<void> => {
  const fd = new FormData();
  fd.append("dataset_id", String(datasetId));
  fd.append("save_to_dataset", "true");
  const r = await fetch(`${BASE}/api/build-matrix`, {method:"POST", body:fd});
  if (!r.ok) {
    let msg = `HTTP ${r.status}`;
    try { const e = await r.json(); msg = e.detail ?? msg; } catch {}
    throw new Error(msg);
  }
  await r.blob(); // consume response body
};

export const addStore = (dsId:string, body:Record<string,unknown>) =>
  req<Store>(`/api/datasets/${dsId}/stores`, {method:"POST", headers:{"Content-Type":"application/json"}, body:JSON.stringify(body)});
export const addVehicle = (dsId:string, body:Record<string,unknown>) =>
  req<Vehicle>(`/api/datasets/${dsId}/vehicles`, {method:"POST", headers:{"Content-Type":"application/json"}, body:JSON.stringify(body)});

export const fmtSec = (s:number) => `${String(Math.floor(s/3600)).padStart(2,"0")}:${String(Math.floor((s%3600)/60)).padStart(2,"0")}`;

// ── Run Groups ───────────────────────────────────────────────
import type { RunGroup } from "@/types/vrp"
export const getRunGroups      = () => req<RunGroup[]>("/api/run-groups");
export const createRunGroup    = (name:string, datasetId?:string) =>
  req<RunGroup>("/api/run-groups",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({name,dataset_id:datasetId??null})});
export const renameRunGroup    = (id:string,name:string) =>
  req<RunGroup>(`/api/run-groups/${id}`,{method:"PATCH",headers:{"Content-Type":"application/json"},body:JSON.stringify({name})});
export const deleteRunGroup    = (id:string) =>
  req<{ok:boolean}>(`/api/run-groups/${id}`,{method:"DELETE"});
export const patchJobVersion   = (jobId:string,body:{version_name?:string,group_id?:string}) =>
  req<{id:string}>(`/api/jobs/${jobId}/version`,{method:"PATCH",headers:{"Content-Type":"application/json"},body:JSON.stringify(body)});
export const forkJob           = (jobId:string) =>
  req<{id:string;version_name:string}>(`/api/jobs/${jobId}/fork`,{method:"POST",headers:{"Content-Type":"application/json"},body:"{}"});
export const patchJobResult    = (jobId:string,payload:Record<string,unknown>) =>
  req<{ok:boolean}>(`/api/jobs/${jobId}/result`,{method:"PATCH",headers:{"Content-Type":"application/json"},body:JSON.stringify(payload)});

// ── Manual Routes ───────────────────────────────────────────
export const createManualJob = (data: {
  title: string;
  routes: Array<{
    vehicle_id: string;
    vehicle_name: string;
    stops: string[];
    route_name?: string;
  }>;
  is_manual: boolean;
  dataset_id?: string;
}) => req<Job>("/api/jobs/manual", {
  method: "POST",
  headers: { "Content-Type": "application/json" },
  body: JSON.stringify(data)
});