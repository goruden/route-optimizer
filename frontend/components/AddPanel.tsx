"use client";
import { useState, useEffect, useRef, useCallback } from "react";
import { useApp } from "@/lib/state";
import * as api from "@/lib/api";
import { Btn, showToast } from "./ui";
import type { Vehicle, Store, StopDetail, RouteSummary } from "@/types/vrp";
import * as XLSX from "xlsx";
import { BalanceIcon, BoxIcon, DeleteIcon, GlobalIcon, LocationIcon, MapIcon, SnowflakeIcon, StoreIcon, WarningIcon } from "./icons";

// ── Types ──────────────────────────────────────────────────────────────
export interface StopEntry {
  uid: string;
  storeId: string;
  storeName: string;
  storeMnName: string;
  demandKg: number;
  demandM3: number;
  lat?: number;
  lon?: number;
}

export interface RouteEntry {
  uid: string;
  vehicleId: string;
  routeName: string;
  truckNumber?: string;  // Original truck number from import
  direction?: string;
  stops: StopEntry[];
}

// ── Helpers ────────────────────────────────────────────────────────────
function mkuid() {
  return Math.random().toString(36).slice(2, 9);
}

export function getVehicleLabel(vehicleId: string, vehicles: Vehicle[]): string {
  const v = vehicles.find((x) => x.truck_id === vehicleId);
  if (!v) return vehicleId;
  const sorted = [...vehicles]
    .filter((x) => x.fleet === v.fleet)
    .sort((a, b) => a.truck_id.localeCompare(b.truck_id));
  const idx = sorted.findIndex((x) => x.truck_id === vehicleId);
  return `${v.fleet === "DRY" ? "D" : "C"}${idx + 1}`;
}

function storeToStop(store: Store, fleet: string): StopEntry {
  return {
    uid: mkuid(),
    storeId: store.store_id,
    storeName: store.eng_name || store.store_id,
    storeMnName: store.mn_name || "",
    demandKg: fleet === "DRY" ? store.dry_kg || 0 : store.cold_kg || 0,
    demandM3: fleet === "DRY" ? store.dry_cbm || 0 : store.cold_cbm || 0,
    lat: store.lat,
    lon: store.lon,
  };
}

export function routesFromSolverData(
  routeSummary: RouteSummary[],
  stopDetails: StopDetail[]
): RouteEntry[] {
  return routeSummary.map((rs) => {
    const routeStops = stopDetails
      .filter((s) => s.truck_id === rs.truck_id && s.trip_number === rs.trip_number)
      .sort((a, b) => a.stop_order - b.stop_order)
      .map((s) => ({
        uid: mkuid(),
        storeId: s.store_id,
        storeName: s.eng_name || s.store_id,
        storeMnName: s.mn_name || "",
        demandKg: s.demand_kg,
        demandM3: s.demand_m3,
        lat: s.lat,
        lon: s.lon,
      }));
    return {
      uid: mkuid(),
      vehicleId: rs.truck_id,
      routeName: `${rs.truck_id} T${rs.trip_number}`,
      stops: routeStops,
    };
  });
}

// ── Geo helpers ────────────────────────────────────────────────────────
function haversineKm(lat1: number, lon1: number, lat2: number, lon2: number): number {
  const R = 6371;
  const dLat = ((lat2 - lat1) * Math.PI) / 180;
  const dLon = ((lon2 - lon1) * Math.PI) / 180;
  const a = Math.sin(dLat / 2) ** 2 + Math.cos((lat1 * Math.PI) / 180) * Math.cos((lat2 * Math.PI) / 180) * Math.sin(dLon / 2) ** 2;
  return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}

const DEPOT_COORDS: Record<string, { lat: number; lon: number }> = {
  DRY: { lat: 47.9152, lon: 106.8922 },
  COLD: { lat: 47.9074, lon: 106.9001 },
};

function nearestNeighbourSort(stops: StopEntry[], fleet: string): StopEntry[] {
  if (stops.length <= 2) return [...stops];
  const depot = DEPOT_COORDS[fleet] ?? DEPOT_COORDS.DRY;
  const remaining = stops.filter((s) => s.lat != null && s.lon != null);
  const noGeo = stops.filter((s) => s.lat == null || s.lon == null);
  const ordered: StopEntry[] = [];
  let curLat = depot.lat, curLon = depot.lon;
  while (remaining.length > 0) {
    let bestIdx = 0, bestDist = Infinity;
    remaining.forEach((s, i) => {
      const dist = haversineKm(curLat, curLon, s.lat!, s.lon!);
      if (dist < bestDist) { bestDist = dist; bestIdx = i; }
    });
    const chosen = remaining.splice(bestIdx, 1)[0];
    ordered.push(chosen);
    curLat = chosen.lat!; curLon = chosen.lon!;
  }
  return [...ordered, ...noGeo];
}

function routeDistanceKm(stops: StopEntry[], fleet: string): number {
  if (stops.length === 0) return 0;
  const depot = DEPOT_COORDS[fleet] ?? DEPOT_COORDS.DRY;
  const pts = [{ lat: depot.lat, lon: depot.lon }, ...stops.filter((s) => s.lat != null).map((s) => ({ lat: s.lat!, lon: s.lon! })), { lat: depot.lat, lon: depot.lon }];
  let total = 0;
  for (let i = 0; i < pts.length - 1; i++) total += haversineKm(pts[i].lat, pts[i].lon, pts[i + 1].lat, pts[i + 1].lon);
  return total;
}

// ── Import helpers ─────────────────────────────────────────────────────
function normalizeStoreId(raw: string): string {
  try { return String(parseInt(raw.trim(), 10)); } catch { return raw.trim(); }
}

function isSequentialFormat(data: any[]): boolean {
  if (!data.length) return false;
  const keys = Object.keys(data[0]).map((k) => String(k).toLowerCase());
  // Check for tabular format headers
  if (keys.some((k) => k.includes("car number") || k.includes("truck_id") || k.includes("vehicle_id"))) return false;
  // Check for sequential format indicators
  if (keys.some((k) => k.includes("car number") || k.includes("car"))) return true;
  const firstVal = String(Object.values(data[0])[0] ?? "");
  return firstVal.includes("||");
}

function isMongolianFormat(data: any[]): boolean {
  if (!data.length) return false;
  const keys = Object.keys(data[0]);
  const lowerKeys = keys.map((k) => String(k).toLowerCase());
  // Check for Mongolian column headers
  return lowerKeys.some((k) => k.includes("truck") && k.includes("number")) &&
         lowerKeys.some((k) => k.includes("чиглэл") || k.includes("direction")) &&
         lowerKeys.some((k) => k.includes("салбар") || k.includes("store"));
}

function parseMongolianFormat(data: any[], vehicles: Vehicle[], stores: Store[]): { routes: RouteEntry[]; warnings: string[] } {
  const routes: RouteEntry[] = [], warnings: string[] = [];
  const storeByRaw = new Map<string, Store>(), storeByNorm = new Map<string, Store>();

  for (const s of stores) { storeByRaw.set(s.store_id.trim(), s); storeByNorm.set(normalizeStoreId(s.store_id), s); }

  // Parse multi-row format where truck + direction on one row, store IDs on subsequent rows
  let currentTrucks: string[] = [];
  let currentDirection = "";
  let currentStoreIds: string[] = [];

  for (let i = 0; i < data.length; i++) {
    const row = data[i];
    const keys = Object.keys(row);
    const truckKey = keys.find(k => k.toLowerCase().includes("truck") && k.toLowerCase().includes("number")) || "Truck number";
    const directionKey = keys.find(k => k.toLowerCase().includes("чиглэл") || k.toLowerCase().includes("direction")) || "Чиглэл";
    const storesKey = keys.find(k => k.toLowerCase().includes("салбар") || k.toLowerCase().includes("store")) || "Салбар дэлгүүрүд";

    const truckCell = String(row[truckKey] ?? "").trim();
    const directionCell = String(row[directionKey] ?? "").trim();
    const storesCell = String(row[storesKey] ?? "").trim();

    // Check if this is a truck row (has truck number)
    if (truckCell) {
      // Flush previous route if exists
      if (currentTrucks.length > 0 && currentStoreIds.length > 0) {
        processTrucks(currentTrucks, currentDirection, currentStoreIds, vehicles, stores, storeByRaw, storeByNorm, routes, warnings);
      }

      // Parse truck numbers (space-separated for multiple trucks)
      currentTrucks = truckCell.split(/\s+/).map(t => t.trim()).filter(t => t);
      currentDirection = directionCell;
      currentStoreIds = [];
    }

    // Collect store IDs from stores column (even if empty, check for numeric values)
    if (storesCell !== undefined && storesCell !== null) {
      const storeIds = storesCell
        .split(/[\n,;\s]+/)
        .map(s => s.trim())
        .filter(s => s && s !== "0");
      currentStoreIds.push(...storeIds);
    }

    // Also check all other columns for store IDs (in case they're in a different column)
    for (const key of keys) {
      if (key === truckKey || key === directionKey || key === storesKey) continue;
      const cellValue = row[key];
      if (cellValue !== undefined && cellValue !== null && cellValue !== "") {
        const strValue = String(cellValue).trim();
        if (strValue && strValue !== "0" && /^\d+$/.test(strValue)) {
          currentStoreIds.push(strValue);
        }
      }
    }
  }

  // Flush last route
  if (currentTrucks.length > 0 && currentStoreIds.length > 0) {
    processTrucks(currentTrucks, currentDirection, currentStoreIds, vehicles, stores, storeByRaw, storeByNorm, routes, warnings);
  } else if (currentTrucks.length > 0 && currentStoreIds.length === 0) {
    warnings.push(`No stores found for trucks: ${currentTrucks.join(", ")}`);
  }

  return { routes, warnings };
}

function processTrucks(
  truckNums: string[],
  direction: string,
  storeIds: string[],
  vehicles: Vehicle[],
  stores: Store[],
  storeByRaw: Map<string, Store>,
  storeByNorm: Map<string, Store>,
  routes: RouteEntry[],
  warnings: string[]
) {
  // Find all vehicles for the truck numbers
  const foundVehicles: Vehicle[] = [];
  for (const truckNum of truckNums) {
    // Normalize truck number by removing common suffixes
    const normalizedTruckNum = truckNum.replace(/\s*(УКМ|УКР|УЕТ)$/i, "").trim();

    const vehicle = vehicles.find((v) => {
      const normalizedVehId = v.truck_id.trim().replace(/\s*(УКМ|УКР|УЕТ)$/i, "").trim();
      return normalizedVehId.includes(normalizedTruckNum) ||
             normalizedTruckNum.includes(normalizedVehId) ||
             (v.truck_num && v.truck_num.toString().includes(normalizedTruckNum));
    });

    if (vehicle) {
      foundVehicles.push(vehicle);
    } else {
      warnings.push(`Vehicle "${truckNum}" (normalized: "${normalizedTruckNum}") not found`);
    }
  }

  if (foundVehicles.length === 0) return;

  // Parse stores
  const stops: StopEntry[] = [];
  for (const storeId of storeIds) {
    const store = storeByRaw.get(storeId) ?? storeByNorm.get(normalizeStoreId(storeId));
    if (!store) {
      warnings.push(`Store "${storeId}" not found`);
      continue;
    }
    stops.push(storeToStop(store, foundVehicles[0].fleet || "DRY"));
  }

  if (stops.length === 0) {
    warnings.push(`No valid stores for trucks: ${truckNums.join(", ")}`);
    return;
  }

  // Create one route per direction using the first found vehicle
  const primaryVehicle = foundVehicles[0];
  routes.push({
    uid: mkuid(),
    vehicleId: primaryVehicle.truck_id,
    routeName: direction || `${primaryVehicle.truck_id} Imported`,
    truckNumber: truckNums.join(" "),  // Store original truck number(s) from import
    direction: direction,
    stops: [...stops],
  });
}

function parseSequentialFormat(data: any[], vehicles: Vehicle[], stores: Store[]): { routes: RouteEntry[]; warnings: string[] } {
  const routes: RouteEntry[] = [], warnings: string[] = [];
  
  data.forEach((row, rowIndex) => {
    let carColumn = "", vehiclePlate = "";
    if (Array.isArray(row)) { carColumn = row[row.length - 2] || ""; vehiclePlate = carColumn.split("||")[1]?.trim() || carColumn.trim(); }
    else { carColumn = row["Car Number"] || ""; vehiclePlate = carColumn.split("||")[1]?.trim() || carColumn.trim(); }
    let vehicle = vehicles.find((v) => v.truck_id.trim() === vehiclePlate.trim());
    if (!vehicle) {
      // Try partial match for cases where vehicle plate contains truck ID
      vehicle = vehicles.find((v) => 
        v.truck_id.trim().includes(vehiclePlate.trim()) || 
        vehiclePlate.trim().includes(v.truck_id.trim())
      );
    }
    if (!vehicle) { warnings.push(`Row ${rowIndex + 1}: Vehicle "${vehiclePlate}" not found`); return; }
    const stops: StopEntry[] = [];
    if (Array.isArray(row)) {
      for (let i = 0; i < row.length - 2; i++) {
        const cell = String(row[i] ?? "").trim();
        if (!cell || cell === "0") continue;
        const match = cell.match(/\((\d+)\)/);
        if (!match) continue;
        const storeId = match[1]; // Use the extracted store ID directly
        const store = stores.find((s) => s.store_id === storeId);
        if (!store) { warnings.push(`Row ${rowIndex + 1}: Store "${match[1]}" not found`); continue; }
        stops.push(storeToStop(store, vehicle.fleet || "DRY"));
      }
    }
    if (stops.length > 0) routes.push({ uid: mkuid(), vehicleId: vehicle.truck_id, routeName: `${vehicle.truck_id} Imported`, stops });
    else warnings.push(`Row ${rowIndex + 1}: No valid stops for "${vehiclePlate}"`);
  });
  return { routes, warnings };
}

function parseTabularFormat(data: any[], vehicles: Vehicle[], stores: Store[]): { routes: RouteEntry[]; warnings: string[] } {
  const warnings: string[] = [], newRoutes: RouteEntry[] = [], routeMap = new Map<string, RouteEntry>();
  const storeByRaw = new Map<string, Store>(), storeByNorm = new Map<string, Store>();
  
  for (const s of stores) { storeByRaw.set(s.store_id.trim(), s); storeByNorm.set(normalizeStoreId(s.store_id), s); }
  
  for (const row of data) {
    // Extract truck ID from "Car Number" column
    const truckId = String(row["Car Number"] ?? "").trim();
    if (!truckId) continue;
    
    // Find vehicle
    const vehicle = vehicles.find((v) => v.truck_id.trim() === truckId.trim());
    if (!vehicle) {
      warnings.push(`Vehicle "${truckId}" not found`); 
      continue;
    }
    
    // Create route if not exists
    if (!routeMap.has(truckId)) {
      const entry: RouteEntry = { uid: mkuid(), vehicleId: truckId, routeName: `${truckId} Imported`, stops: [] };
      routeMap.set(truckId, entry); newRoutes.push(entry);
    }
    
    // Process numbered columns (1, 2, 3, etc.) for stores
    const routeEntry = routeMap.get(truckId)!;
    const numberedColumns = Object.keys(row).filter(key => /^\d+$/.test(key));
    
    for (const colKey of numberedColumns) {
      const cell = String(row[colKey] ?? "").trim();
      if (!cell || cell === "0") continue;
      
      // Extract store ID from format "(00001) Store Name"
      const match = cell.match(/\((\d+)\)/);
      if (!match) continue;
      
      const storeId = match[1];
      
      const store = storeByRaw.get(storeId) ?? storeByNorm.get(normalizeStoreId(storeId));
      if (!store) {
        warnings.push(`Store "${storeId}" not found`); 
        continue;
      }
      
      const stop: StopEntry = { 
        uid: mkuid(), 
        storeId: store.store_id, 
        storeName: store.eng_name || store.store_id, 
        storeMnName: store.mn_name || "", 
        demandKg: vehicle.fleet === "DRY" ? store.dry_kg || 0 : store.cold_kg || 0, 
        demandM3: vehicle.fleet === "DRY" ? store.dry_cbm || 0 : store.cold_cbm || 0, 
        lat: store.lat, 
        lon: store.lon 
      };
      
      routeEntry.stops.push(stop);
    }
  }
  
  return { routes: newRoutes, warnings };
}

// ── StopChip ───────────────────────────────────────────────────────────
function StopChip({ stop, order, color, onRemove, onMoveLeft, onMoveRight, canLeft, canRight, isDragging, isDragOver, onDragStart, onDragEnter, onDragOver: onDragOverProp, onDragEnd, onDrop }: {
  stop: StopEntry; order: number; color: string;
  onRemove: () => void; onMoveLeft: () => void; onMoveRight: () => void;
  canLeft: boolean; canRight: boolean;
  isDragging: boolean; isDragOver: boolean;
  onDragStart: () => void; onDragEnter: () => void;
  onDragOver: (e: React.DragEvent) => void; onDragEnd: () => void;
  onDrop: () => void;
}) {
  const [hovered, setHovered] = useState(false);
  return (
    <div className="relative flex flex-col items-center shrink-0 transition-all duration-150" style={{ width: 88, opacity: isDragging ? 0.35 : 1, transform: isDragOver ? "scale(1.06)" : "scale(1)" }}
      draggable onDragStart={onDragStart} onDragEnter={onDragEnter} onDragOver={onDragOverProp} onDragEnd={onDragEnd} onDrop={onDrop}
      onMouseEnter={() => setHovered(true)} onMouseLeave={() => setHovered(false)}>
      {isDragOver && <div className="absolute inset-0 rounded-xl border-2 border-dashed z-10 pointer-events-none" style={{ borderColor: color, background: color + "15" }} />}
      <div className="absolute rounded-xl border-[1.5px] bg-white flex flex-col items-center overflow-hidden transition-all cursor-grab active:cursor-grabbing"
        style={{ width: "100%", borderColor: hovered || isDragOver ? color : color + "55", boxShadow: hovered || isDragOver ? `0 4px 16px ${color}33` : "0 1px 3px rgba(0,0,0,0.06)" }}>
        <div className="w-full flex items-center justify-center py-1 text-[9px] font-extrabold text-white select-none" style={{ background: color }}>#{order}</div>
        <div className="px-1.5 pt-1 pb-0.5 w-full">
          <div className="text-[9px] font-bold text-slate-700 text-center leading-tight truncate" title={stop.storeMnName}>
            {stop.storeMnName.length > 11 ? stop.storeMnName.slice(0, 11) + "…" : stop.storeMnName}
          </div>
        </div>
        <div className="text-[8px] font-mono text-slate-400 pb-0.5">{stop.storeId || "—"}</div>
        <div className="text-[8px] font-mono text-slate-400 pb-1">{stop.demandKg > 0 ? `${stop.demandKg.toFixed(0)}kg` : "—"}</div>
        <div className="flex w-full border-t border-slate-100">
          <button onClick={onMoveLeft} disabled={!canLeft} className="flex-1 h-5 text-[9px] disabled:opacity-20 hover:bg-blue-50 text-slate-400 hover:text-blue-500 transition-colors">◀</button>
          <button onClick={onRemove} className="h-5 w-6 text-[9px] hover:bg-red-50 text-red-400 hover:text-red-600 border-l border-r border-slate-100 transition-colors">✕</button>
          <button onClick={onMoveRight} disabled={!canRight} className="flex-1 h-5 text-[9px] disabled:opacity-20 hover:bg-blue-50 text-slate-400 hover:text-blue-500 transition-colors">▶</button>
        </div>
      </div>
    </div>
  );
}

// ── RouteCard ──────────────────────────────────────────────────────────
function RouteCard({ route, index, vehicles, stores, onRemove, onVehicleChange, onAddStop, onRemoveStop, onMoveStop, onReorderStops }: {
  route: RouteEntry; index: number; vehicles: Vehicle[]; stores: Store[];
  onRemove: () => void; onVehicleChange: (vid: string) => void;
  onAddStop: (store: Store) => void; onRemoveStop: (stopUid: string) => void;
  onMoveStop: (stopUid: string, dir: -1 | 1) => void;
  onReorderStops: (newStops: StopEntry[]) => void;
}) {
  const [search, setSearch] = useState("");
  const [showDrop, setShowDrop] = useState(false);
  const dropRef = useRef<HTMLDivElement>(null);
  const dragIndexRef = useRef<number | null>(null);
  const [dragIndex, setDragIndex] = useState<number | null>(null);
  const [overIndex, setOverIndex] = useState<number | null>(null);

  const handleDragStart = useCallback((idx: number) => { dragIndexRef.current = idx; setDragIndex(idx); }, []);
  const handleDragEnter = useCallback((idx: number) => { if (dragIndexRef.current === null || dragIndexRef.current === idx) return; setOverIndex(idx); }, []);
  const handleDragOver = useCallback((e: React.DragEvent) => { e.preventDefault(); }, []);
  const handleDrop = useCallback((targetIdx: number) => {
    const from = dragIndexRef.current;
    if (from === null || from === targetIdx) return;
    const updated = [...route.stops];
    const [moved] = updated.splice(from, 1);
    updated.splice(targetIdx, 0, moved);
    onReorderStops(updated);
    dragIndexRef.current = null; setDragIndex(null); setOverIndex(null);
  }, [route.stops, onReorderStops]);
  const handleDragEnd = useCallback(() => { dragIndexRef.current = null; setDragIndex(null); setOverIndex(null); }, []);

  const vehicle = vehicles.find((v) => v.truck_id === route.vehicleId);
  const fleet = vehicle?.fleet ?? "Unknown";
  const fleetColor = fleet === "DRY" ? "#ea580c" : fleet==="Unknown" ? "#7c3aed" : "#0284c7";
  const bgColor = fleet === "DRY" ? "#fed7aa" : fleet==="Unknown" ? "#ddd6fe" : "#bae6fd";

  const hasGeoData = route.stops.some((s) => s.lat != null && s.lon != null);
  const currentDist = routeDistanceKm(route.stops, fleet);

  function autoSort() {
    if (!hasGeoData) { showToast("No coordinates available", "error"); return; }
    const sorted = nearestNeighbourSort(route.stops, fleet);
    const newDist = routeDistanceKm(sorted, fleet);
    onReorderStops(sorted);
    showToast(`Reordered · ~${newDist.toFixed(1)} km`, "success");
  }

  const totalKg = route.stops.reduce((a, s) => a + s.demandKg, 0);
  const totalM3 = route.stops.reduce((a, s) => a + s.demandM3, 0);
  const capKg = vehicle?.cap_kg ?? 0, capM3 = vehicle?.cap_m3 ?? 0;
  const utilKg = capKg > 0 ? Math.min(100, (totalKg / capKg) * 100) : 0;
  const utilM3 = capM3 > 0 ? Math.min(100, (totalM3 / capM3) * 100) : 0;
  const usedIds = new Set(route.stops.map((s) => s.storeId));
  const filteredStores = stores.filter((s) => {
    if (usedIds.has(s.store_id)) return false;
    if (!search) return true;
    const q = search.toLowerCase();
    return s.store_id.toLowerCase().includes(q) || (s.eng_name || "").toLowerCase().includes(q) || (s.mn_name || "").toLowerCase().includes(q);
  });

  return (
    <div className="rounded-t-2xl border shadow-sm flex flex-col" style={{ borderColor: fleetColor + "40" }}>
      <div className="flex rounded-t-xl items-center gap-2.5 px-3 py-2" style={{ background: bgColor }}>
        <div className="w-10 h-10 rounded-xl flex items-center justify-center text-[15px] font-extrabold text-white shrink-0 shadow-sm" style={{ background: fleetColor }}>
          {vehicle?.truck_id?.slice(0, 3) || "?"}
        </div>
        <div className="flex flex-col">
          {route.truckNumber && (
            <div className="text-[9px] font-semibold text-slate-600 mt-1 truncate bg-slate-100 rounded-lg px-2 py-1 text-center">{route.truckNumber}</div>
          )}
          {route.direction && (
            <div className="text-[9px] font-semibold text-slate-600 mt-1 truncate px-2">{route.direction}</div>
          )}
        </div>
        <div className="flex-1 min-w-0">
          <select value={route.vehicleId} onChange={(e) => onVehicleChange(e.target.value)}
            className="w-full text-[11px] font-semibold bg-white/70 border border-white rounded-lg px-2 py-1.5 outline-none focus:border-blue-400"
            style={{ color: fleetColor }}>
            <option value="">— Тээврийн хэрэгсэл сонгох —</option>
            {vehicles.map((v) => (<option key={v.truck_id} value={v.truck_id}>{v.truck_id} · {v.fleet} · {v.cap_kg.toLocaleString()}kg | {v.cap_m3.toLocaleString()}m³</option>))}
          </select>
        </div>
        <span className="text-[9px] font-extrabold px-2 py-0.5 rounded-full shrink-0" style={{ background: fleetColor, color: "#fff" }}>{fleet}</span>
        {route.stops.length >= 2 && (
          <div className="flex items-center gap-1 shrink-0">
            <button onClick={autoSort} disabled={!hasGeoData}
              className="flex items-center gap-1 px-2 py-1 rounded-lg text-[9px] font-bold border transition-all disabled:opacity-30 disabled:cursor-not-allowed"
              style={{ background: hasGeoData ? "#ECFDF5" : "#F8FAFC", borderColor: hasGeoData ? "#6EE7B7" : "#CBD5E1", color: hasGeoData ? "#059669" : "#94A3B8" }}>
              ✦ Эрэмбэлэх
            </button>
            <button onClick={() => onReorderStops([...route.stops].reverse())}
              className="flex items-center gap-1 px-2 py-1 rounded-lg text-[9px] font-bold border border-slate-200 bg-white text-slate-500 hover:bg-slate-50 transition-all">
              ⇄ Урвуу
            </button>
          </div>
        )}
        <button onClick={onRemove} className="w-7 h-7 rounded-xl flex items-center justify-center text-[12px] text-red-400 hover:text-red-600 hover:bg-red-50 transition-colors shrink-0"><DeleteIcon size="size-5"/></button>
      </div>

      <div className="bg-white px-3 py-3 flex-1">
        {route.stops.length === 0 && !showDrop && (
          <div className="text-[11px] text-slate-400 italic text-center py-4">Зогсоол байхгүй байна — "+ Нэмэх" товчийг дарж эхлүүлнэ үү</div>
        )}
        {route.stops.length >= 2 && (
          <div className="text-[9px] text-slate-400 mb-2 flex items-center gap-1"><span>↕</span><span>Чирч дараалал өөрчлөх · ◀ ▶ товчнууд · ✦ Хамгийн ойр дарааллаар эрэмбэлэх</span></div>
        )}
        <div className="flex gap-2 flex-wrap" style={{ minHeight: route.stops.length > 0 ? 96 : 0 }} onDragOver={(e) => e.preventDefault()}>
          {route.stops.map((stop, idx) => (
            <div className="flex justify-center gap-2 h-25" key={stop.uid}>
              <StopChip stop={stop} order={idx + 1} color={fleetColor}
                onRemove={() => onRemoveStop(stop.uid)}
                onMoveLeft={() => onMoveStop(stop.uid, -1)}
                onMoveRight={() => onMoveStop(stop.uid, 1)}
                canLeft={idx > 0} canRight={idx < route.stops.length - 1}
                isDragging={dragIndex === idx} isDragOver={overIndex === idx && dragIndex !== idx}
                onDragStart={() => handleDragStart(idx)} onDragEnter={() => handleDragEnter(idx)}
                onDragOver={handleDragOver} onDragEnd={handleDragEnd} onDrop={() => handleDrop(idx)} />
              {idx < route.stops.length - 1 && <div className="self-center text-[18px] select-none text-black font-bold">→</div>}
            </div>
          ))}
          <div className="shrink-0 self-center ml-1" ref={dropRef}>
            <button onClick={() => setShowDrop((d) => !d)}
              className="flex items-center gap-1.5 px-3 py-2 rounded-xl border-[1.5px] border-dashed text-[11px] font-semibold transition-all hover:bg-blue-50"
              style={{ borderColor: fleetColor + "80", color: fleetColor }}>
              <span className="text-[14px]">＋</span> Зогсоол нэмэх
            </button>
          </div>
        </div>
        {route.stops.length >= 2 && hasGeoData && (
          <div className="mt-2"><span className="text-[9px] px-2 py-0.5 rounded-full font-mono font-semibold border flex gap-1" style={{ background: fleetColor + "10", borderColor: fleetColor + "30", color: fleetColor }}><LocationIcon size="size-3"/> Est. ~{currentDist.toFixed(1)} km</span></div>
        )}
      </div>

      {vehicle && (
        <div className="px-3 py-2 grid grid-cols-2 gap-3 border-t" style={{ background: bgColor + "80", borderColor: fleetColor + "20" }}>
          <div>
            <div className="flex justify-between text-[9px] mb-1">
              <span className="text-slate-500 flex gap-1"><BalanceIcon size="size-3" /> Weight</span>
              <span className="font-mono font-bold" style={{ color: utilKg > 90 ? "#EF4444" : fleetColor }}>{totalKg.toFixed(0)} / {capKg.toFixed(0)} kg</span>
            </div>
            <div className="h-1.5 bg-white rounded-full overflow-hidden border border-slate-200">
              <div className="h-full rounded-full transition-all duration-300" style={{ width: `${utilKg}%`, background: utilKg > 90 ? "#EF4444" : fleetColor }} />
            </div>
          </div>
          <div>
            <div className="flex justify-between text-[9px] mb-1">
              <span className="text-slate-500 flex gap-1"><BoxIcon size="size-3"/> Volume</span>
              <span className="font-mono font-bold" style={{ color: utilM3 > 90 ? "#EF4444" : fleetColor }}>{totalM3.toFixed(2)} / {capM3.toFixed(1)} m³</span>
            </div>
            <div className="h-1.5 bg-white rounded-full overflow-hidden border border-slate-200">
              <div className="h-full rounded-full transition-all duration-300" style={{ width: `${utilM3}%`, background: utilM3 > 90 ? "#EF4444" : fleetColor }} />
            </div>
          </div>
        </div>
      )}

      {/* Store search dropdown */}
      {showDrop && (
        <div className="fixed bg-white border border-slate-200 rounded-2xl shadow-2xl z-50 overflow-hidden"
          style={{ width: 260, top: dropRef.current ? `${dropRef.current.getBoundingClientRect().top - 10}px` : "auto", left: dropRef.current ? `${dropRef.current.getBoundingClientRect().right + 10}px` : "auto" }}>
          <div className="p-2 border-b border-slate-100">
            <input autoFocus value={search} onChange={(e) => setSearch(e.target.value)}
              placeholder={`${fleet} дэлгүүр хайх…`}
              className="w-full text-[11px] px-2.5 py-1.5 border border-slate-200 rounded-lg outline-none focus:border-blue-400" />
          </div>
          <div className="max-h-52 overflow-y-auto">
            {filteredStores.length === 0
              ? <div className="text-[11px] text-slate-400 text-center py-5">Тохирох дэлгүүр олдсонгүй</div>
              : filteredStores.map((store) => {
                const demand = fleet === "DRY" ? store.dry_kg : store.cold_kg;
                return (
                  <button key={store.store_id}
                    onMouseDown={(e) => { e.preventDefault(); onAddStop(store); setSearch(""); setShowDrop(false); }}
                    className="w-full flex items-center gap-2.5 px-3 py-2 hover:bg-blue-50 text-left transition-colors border-b border-slate-50 last:border-none">
                    <div className="w-6 h-6 rounded-lg flex items-center justify-center text-[9px] font-extrabold text-white shrink-0" style={{ background: fleetColor }}><StoreIcon /></div>
                    <div className="flex-1 min-w-0">
                      <div className="text-[11px] font-semibold text-slate-800 truncate">{store.eng_name || store.store_id}</div>
                      <div className="text-[9px] text-slate-400">#{store.store_id} · {demand.toFixed(0)}kg</div>
                    </div>
                  </button>
                );
              })
            }
          </div>
        </div>
      )}
    </div>
  );
}

// ── RouteBuilderModal ──────────────────────────────────────────────────
export interface RouteBuilderProps {
  open: boolean; onClose: () => void;
  initialRoutes?: RouteEntry[]; initialTitle?: string;
  mode?: "create" | "edit";
  datasetId?: string; groupId?: string;
}

export function RouteBuilderModal({ open, onClose, initialRoutes = [], initialTitle = "", mode = "create", datasetId: initDsId, groupId: initGroupId }: RouteBuilderProps) {
  const { s, d } = useApp();
  const [loading, setLoading] = useState(false);
  const [title, setTitle] = useState(initialTitle);
  const [routes, setRoutes] = useState<RouteEntry[]>([]);
  const [dsId, setDsId] = useState<string>("");
  const [groupId, setGroupId] = useState("none");
  const [vehicles, setVehicles] = useState<Vehicle[]>([]);
  const [stores, setStores] = useState<Store[]>([]);
  const [dataLoading, setDataLoading] = useState(false);
  const [fleetFilter, setFleetFilter] = useState<"ALL" | "DRY" | "COLD">("ALL");
  const [search, setSearch] = useState("");
  const [importWarnings, setImportWarnings] = useState<string[]>([]);
  const [creatingGroup, setCreatingGroup] = useState(false);
  const [newGroupName, setNewGroupName] = useState("");
  const [showNewGroup, setShowNewGroup] = useState(false);

  // Reset and initialize when modal opens
  useEffect(() => {
    if (!open) return;
    setTitle(initialTitle);
    setRoutes(JSON.parse(JSON.stringify(initialRoutes)));
    // BUG FIX: Properly resolve dataset ID, default to active dataset
    const resolvedDs = initDsId ?? s.activeDatasetId ?? "";
    setDsId(resolvedDs.toString());
    setGroupId(initGroupId ?? "none");
    setImportWarnings([]);
    setFleetFilter("ALL");
    setSearch("");
  }, [open]); // eslint-disable-line

  // BUG FIX: Load data when dsId changes OR when modal first opens with a dsId
  // Using a ref to detect "just opened" case
  const prevOpen = useRef(false);
  useEffect(() => {
    const justOpened = open && !prevOpen.current;
    prevOpen.current = open;

    if (!dsId) { setVehicles([]); setStores([]); return; }
    if (!open) return;

    setDataLoading(true);
    Promise.all([api.getVehicles(dsId), api.getStores(dsId)])
      .then(([v, st]) => { setVehicles(v); setStores(st); })
      .catch(() => showToast("Failed to load dataset data", "error"))
      .finally(() => setDataLoading(false));
  }, [dsId, open]);

  const filteredRoutes = routes.filter((route) => {
    const vehicle = vehicles.find((v) => v.truck_id === route.vehicleId);
    if (fleetFilter === "DRY" && vehicle?.fleet !== "DRY") return false;
    if (fleetFilter === "COLD" && vehicle?.fleet !== "COLD") return false;
    if (search) {
      const q = search.toLowerCase();
      return vehicle?.truck_id.toLowerCase().includes(q) || route.stops.some((st) => st.storeId.toLowerCase().includes(q) || st.storeName.toLowerCase().includes(q) || (st.storeMnName || "").toLowerCase().includes(q));
    }
    return true;
  });

  async function createGroup() {
    if (!newGroupName.trim()) return;
    setCreatingGroup(true);
    try {
      const g = await api.createRunGroup(newGroupName.trim(), dsId || undefined);
      const groups = await api.getRunGroups();
      d({ t: "SET_GROUPS", v: groups });
      setGroupId(g.id);
      setNewGroupName("");
      setShowNewGroup(false);
      showToast(`Group "${g.name}" created!`, "success");
    } catch (e: any) {
      showToast(e.message ?? "Failed to create group", "error");
    } finally {
      setCreatingGroup(false);
    }
  }

  function autoSortAllRoutes() {
    let totalSaved = 0, sortable = 0;
    setRoutes((prev) => prev.map((route) => {
      const v = vehicles.find((x) => x.truck_id === route.vehicleId);
      const fleet = v?.fleet ?? "DRY";
      if (route.stops.length < 3 || !route.stops.some((s) => s.lat != null)) return route;
      sortable++;
      const before = routeDistanceKm(route.stops, fleet);
      const sorted = nearestNeighbourSort(route.stops, fleet);
      totalSaved += before - routeDistanceKm(sorted, fleet);
      return { ...route, stops: sorted };
    }));
    if (sortable === 0) showToast("No routes with coordinates to sort", "error");
    else showToast(`✦ Sorted ${sortable} routes` + (totalSaved > 0.1 ? ` · saved ~${totalSaved.toFixed(1)} km` : ""), "success");
  }

  async function importFile() {
    if (!dsId) { showToast("Please select a dataset first", "error"); return; }

    // Add a small delay to ensure vehicles/stores are loaded
    if (dataLoading || vehicles.length === 0 || stores.length === 0) {
      setTimeout(() => {
        if (vehicles.length === 0 || stores.length === 0) {
          showToast("Dataset still loading - please wait", "error");
        }
      }, 500);
      return;
    }
    const input = document.createElement("input");
    input.type = "file"; input.accept = ".csv,.xlsx";
    input.onchange = async (e) => {
      const file = (e.target as HTMLInputElement).files?.[0];
      if (!file) return;
      try {
        let allData: any[] = [];
        if (file.name.endsWith(".xlsx")) {
          const buffer = await file.arrayBuffer();
          const wb = XLSX.read(buffer, { type: "array" });

          // Import from Dry DC and Cold DC sheets if they exist
          const targetSheets = ["Dry DC", "Cold DC"];
          for (const sheetName of targetSheets) {
            if (wb.SheetNames.includes(sheetName)) {
              const sheetData = XLSX.utils.sheet_to_json(wb.Sheets[sheetName], { defval: "" });
              allData = allData.concat(sheetData);
            }
          }

          // If no target sheets found, use first sheet
          if (allData.length === 0) {
            allData = XLSX.utils.sheet_to_json(wb.Sheets[wb.SheetNames[0]], { defval: "" });
          }
        } else {
          const text = await file.text();
          const lines = text.trim().split("\n");
          const delimiter = lines[0].includes("\t") ? "\t" : ",";
          const headers = lines[0].split(delimiter).map((h) => h.trim());
          for (let i = 1; i < lines.length; i++) {
            const values = lines[i].split(delimiter).map((v) => v.trim());
            const row: any = {};
            headers.forEach((header, index) => { row[header] = values[index] ?? ""; });
            allData.push(row);
          }
        }
        if (!allData.length) { showToast("No data found in file", "error"); return; }

        // Parse only once
        const result = isMongolianFormat(allData)
          ? parseMongolianFormat(allData, vehicles, stores)
          : isSequentialFormat(allData)
          ? parseSequentialFormat(allData, vehicles, stores)
          : parseTabularFormat(allData, vehicles, stores);

        if (!result.routes.length) { setImportWarnings(result.warnings); showToast("No valid routes found", "error"); return; }
        setRoutes((prev) => [...prev, ...result.routes]);
        setImportWarnings(result.warnings);
        showToast(`Imported ${result.routes.length} routes · ${result.routes.reduce((a, r) => a + r.stops.length, 0)} stops`, "success");
      } catch (err) {
        showToast("Failed to parse file", "error");
      }
    };
    input.click();
  }

  function addRoute() {
    setRoutes((prev) => [...prev, { uid: mkuid(), vehicleId: "", routeName: `Route ${prev.length + 1}`, stops: [] }]);
  }
  function removeRoute(uid: string) { setRoutes((prev) => prev.filter((r) => r.uid !== uid)); }
  function updateVehicle(routeUid: string, vehicleId: string) {
    const v = vehicles.find((x) => x.truck_id === vehicleId);
    const fleet = v?.fleet ?? "DRY";
    setRoutes((prev) => prev.map((r) => {
      if (r.uid !== routeUid) return r;
      return { ...r, vehicleId, stops: r.stops.map((stop) => { const st = stores.find((x) => x.store_id === stop.storeId); if (!st) return stop; return { ...stop, demandKg: fleet === "DRY" ? st.dry_kg : st.cold_kg, demandM3: fleet === "DRY" ? st.dry_cbm : st.cold_cbm, lat: st.lat, lon: st.lon }; }) };
    }));
  }
  function addStop(routeUid: string, store: Store) {
    const route = routes.find((r) => r.uid === routeUid);
    if (!route) return;
    const fleet = vehicles.find((x) => x.truck_id === route.vehicleId)?.fleet ?? "DRY";
    setRoutes((prev) => prev.map((r) => r.uid !== routeUid ? r : { ...r, stops: [...r.stops, storeToStop(store, fleet)] }));
  }
  function removeStop(routeUid: string, stopUid: string) { setRoutes((prev) => prev.map((r) => r.uid !== routeUid ? r : { ...r, stops: r.stops.filter((s) => s.uid !== stopUid) })); }
  function moveStop(routeUid: string, stopUid: string, dir: -1 | 1) {
    setRoutes((prev) => prev.map((r) => {
      if (r.uid !== routeUid) return r;
      const idx = r.stops.findIndex((s) => s.uid === stopUid);
      if (idx < 0) return r;
      const nIdx = idx + dir;
      if (nIdx < 0 || nIdx >= r.stops.length) return r;
      const stops = [...r.stops];
      [stops[idx], stops[nIdx]] = [stops[nIdx], stops[idx]];
      return { ...r, stops };
    }));
  }
  function reorderStops(routeUid: string, newStops: StopEntry[]) { setRoutes((prev) => prev.map((r) => r.uid !== routeUid ? r : { ...r, stops: newStops })); }

  async function save() {
    if (!title.trim()) return showToast("Enter a title", "error");
    if (!dsId) return showToast("Select a dataset", "error");
    if (!routes.length) return showToast("Add at least one route", "error");
    for (const r of routes) {
      if (!r.vehicleId) return showToast(`Select a vehicle for "${r.routeName}"`, "error");
      if (!r.stops.length) return showToast(`"${r.routeName}": add at least one stop`, "error");
    }
    setLoading(true);
    try {
      const newJob = await api.createManualJob({
        title: title.trim(),
        routes: routes.map((r) => {
          const vehicle = vehicles.find((v) => v.truck_id === r.vehicleId);
          return {
            vehicle_id: r.vehicleId,
            vehicle_name: getVehicleLabel(r.vehicleId, vehicles),
            stops: r.stops.map((s) => s.storeId),
            route_name: r.routeName,
            truck_number: r.truckNumber,
            contractor: vehicle?.contractor,
          };
        }),
        is_manual: true,
        dataset_id: dsId,
      });
      if (groupId !== "none") await api.patchJobVersion(newJob.id, { group_id: groupId }).catch(() => {});
      const result = await api.getJobResult(newJob.id);
      d({ t: "SET_RESULT", jobId: newJob.id, r: result });
      d({ t: "SET_MAIN", v: "map" });
      const [jobs, groups] = await Promise.all([api.getJobs(), api.getRunGroups()]);
      d({ t: "SET_JOBS", v: jobs }); d({ t: "SET_GROUPS", v: groups });
      showToast(`${routes.length} routes · ${routes.reduce((a, r) => a + r.stops.length, 0)} stops created!`, "success");
      onClose();
    } catch (e: any) {
      showToast(e.message ?? "Failed to save routes", "error");
    } finally {
      setLoading(false);
    }
  }

  if (!open) return null;
  const totalStops = routes.reduce((a, r) => a + r.stops.length, 0);
  const hasAnySortable = routes.some((r) => r.stops.length >= 3 && r.stops.some((s) => s.lat != null));

  return (
    <div className="fixed inset-0 z-9000 flex items-center justify-center p-4">
      <div className="absolute inset-0 bg-slate-900/40 backdrop-blur-sm" onClick={onClose} />
      <div className="relative bg-white rounded-2xl shadow-2xl flex flex-col overflow-hidden" style={{ width: "min(940px, 96vw)", maxHeight: "92vh" }}>

        {/* Header */}
        <div className="shrink-0 flex items-center justify-between px-5 py-4 border-b border-slate-200" style={{ background: "linear-gradient(135deg,#F0F7FF 0%,#E8F4FD 100%)" }}>
          <div className="flex items-center gap-3">
            <div className="w-10 h-10 rounded-xl bg-purple-500 flex items-center justify-center text-white text-[20px] shadow-sm"><MapIcon /></div>
            <div>
              <h2 className="text-[15px] font-extrabold text-slate-900">{mode === "edit" ? "Засварлах" : "Гараар зам бүтээх"}</h2>
              <p className="text-[10px] text-slate-400 mt-0.5">Дэлгүүрүүдийг тээврийн хэрэгслээр хуваарилах · чирч дараалал өөрчлөх · ✦ Хамгийн ойр дарааллаар эрэмбэлэх</p>
            </div>
          </div>
          <button onClick={onClose} className="w-8 h-8 rounded-xl border border-slate-200 bg-white flex items-center justify-center text-[13px] text-slate-500 hover:bg-slate-50">✕</button>
        </div>

        {/* Config bar */}
        <div className="bg-slate-50 border-b border-slate-200 px-5 py-3">
          <div className="grid grid-cols-4 gap-3 mb-3">
            {/* Title */}
            <div className="col-span-2">
              <label className="text-[9px] font-extrabold text-slate-400 uppercase tracking-widest block mb-1">Нэр *</label>
              <input value={title} onChange={(e) => setTitle(e.target.value)} placeholder="жишээ: DRY чиглэлүүд"
                className="w-full text-[12px] font-semibold border border-slate-200 rounded-xl px-3 py-2 outline-none focus:border-blue-500 bg-white" />
            </div>

            {/* Dataset selector — BUG FIX: properly controlled */}
            <div>
              <label className="text-[9px] font-extrabold text-slate-400 uppercase tracking-widest block mb-1">Өгөгдөл *</label>
              <select
                value={dsId}
                onChange={(e) => setDsId(e.target.value)}
                className="w-full text-[11px] border border-slate-200 rounded-xl px-3 py-2 outline-none focus:border-blue-500 bg-white font-medium"
              >
                <option value="">— Өгөгдөл сонгох —</option>
                {s.datasets.map((ds) => (
                  <option key={ds.id} value={ds.id}>{ds.name} ({ds.store_count} дэлгүүр)</option>
                ))}
              </select>
            </div>

            {/* Group selector + create */}
            <div>
              <label className="text-[9px] font-extrabold text-slate-400 uppercase tracking-widest block mb-1">Бүлэг</label>
              <div className="flex gap-1">
                <select value={groupId} onChange={(e) => setGroupId(e.target.value)}
                  className="flex-1 min-w-0 text-[11px] border border-slate-200 rounded-xl px-3 py-2 outline-none focus:border-blue-500 bg-white">
                  <option value="none">— Ганцаарчилсан —</option>
                  {s.runGroups.map((g) => (<option key={g.id} value={g.id}>{g.name}</option>))}
                </select>
                {/* BUG FIX: Add create new group button */}
                {/* <button
                  onClick={() => setShowNewGroup(v => !v)}
                  title="Шинэ бүлэг үүсгэх"
                  className="w-9 shrink-0 flex items-center justify-center rounded-xl border border-blue-200 bg-blue-50 text-blue-500 hover:bg-blue-100 transition-all text-[16px]"
                >
                  +
                </button> */}
              </div>
              {showNewGroup && (
                <div className="mt-1.5 flex gap-1.5 items-center">
                  <input
                    autoFocus
                    value={newGroupName}
                    onChange={e => setNewGroupName(e.target.value)}
                    onKeyDown={e => { if (e.key === "Enter") createGroup(); if (e.key === "Escape") setShowNewGroup(false); }}
                    placeholder="Бүлгийн нэр..."
                    className="flex-1 text-[11px] border border-blue-300 rounded-lg px-2 py-1.5 outline-none focus:border-blue-500 bg-white"
                  />
                  <button onClick={createGroup} disabled={creatingGroup || !newGroupName.trim()}
                    className="text-[11px] font-bold px-2 py-1.5 rounded-lg bg-blue-500 text-white disabled:opacity-40 hover:bg-blue-600 transition-all flex items-center gap-1">
                    {creatingGroup ? <span className="w-3 h-3 border border-white border-t-transparent rounded-full animate-spin" /> : null}
                    Үүсгэх
                  </button>
                </div>
              )}
            </div>
          </div>

          {/* Status row */}
          <div className="flex items-center gap-3">
            {/* Dataset status */}
            {dsId && (
              <div className="flex items-center gap-2 px-2.5 py-1.5 rounded-lg bg-white border border-slate-200 text-[10px]">
                {dataLoading
                  ? <><span className="w-3 h-3 border border-blue-400 border-t-transparent rounded-full animate-spin" /><span className="text-blue-500">Ачаалж байна…</span></>
                  : <><span className="text-green-500">✓</span><span className="text-slate-600 font-semibold">{stores.length} дэлгүүр · {vehicles.length} тээврийн хэрэгсэл</span></>
                }
              </div>
            )}

            {/* Fleet filter */}
            <div className="flex gap-1 p-1 bg-slate-100 rounded-lg">
              {["ALL", "DRY", "COLD"].map((f) => (
                <button key={f} onClick={() => setFleetFilter(f as any)}
                  className={`px-3 py-1 text-[10px] font-semibold rounded-md transition-all ${fleetFilter === f ? "bg-white text-blue-600 shadow-sm border border-blue-200" : "text-slate-500 hover:text-slate-700 hover:bg-white/50"}`}>
                  {f === "ALL" ? <div className="flex gap-1"><GlobalIcon /> All</div> : f === "DRY" ? <div className="flex gap-1"><BoxIcon /> Dry</div> : <div className="flex gap-1"><SnowflakeIcon /> Cold</div>}
                </button>
              ))}
            </div>

            <div className="flex-1 max-w-xs">
              <input value={search} onChange={(e) => setSearch(e.target.value)} placeholder="Чиглэл эсвэл дэлгүүр хайх..."
                className="w-full px-3 py-1.5 text-[11px] border border-slate-200 rounded-lg bg-white focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent" />
            </div>

            <button onClick={importFile}
              className="flex items-center gap-1.5 text-[11px] font-semibold border border-emerald-300 rounded-lg px-3 py-1.5 bg-emerald-50 text-emerald-700 hover:bg-emerald-100 transition-all ml-auto">
              Файл импортлох
            </button>
          </div>
        </div>

        {/* Import warnings */}
        {importWarnings.length > 0 && (
          <div className="shrink-0 mx-4 mt-2 p-2.5 bg-amber-50 border border-amber-200 rounded-xl">
            <div className="text-[11px] font-semibold text-amber-600 mb-1 flex gap-1"><WarningIcon size="size-3"/> {importWarnings.length} анхааруулга:</div>
            <div className="max-h-16 overflow-y-auto space-y-0.5">{importWarnings.map((w, i) => <div key={i} className="text-[10px] text-amber-600">{w}</div>)}</div>
            <button onClick={() => setImportWarnings([])} className="text-[10px] text-amber-500 hover:underline mt-1">Хаах</button>
          </div>
        )}

        {/* Route list */}
        <div className="flex-1 overflow-y-auto p-4 flex flex-col gap-3 min-h-0">
          {!dsId && (
            <div className="flex flex-col items-center justify-center py-16 text-slate-400">
              <p className="font-semibold text-slate-600 mb-1">Эхлэхийн тулд өгөгдөл сонгоно уу</p>
              <p className="text-[11px]">Дээрх өгөгдлөөс сонгож тээврийн хэрэгсэл болон дэлгүүрүүдийг идэвхжүүлнэ үү</p>
            </div>
          )}
          {dsId && dataLoading && (
            <div className="flex flex-col items-center justify-center py-12 text-slate-400">
              <span className="w-8 h-8 border-2 border-blue-400 border-t-transparent rounded-full animate-spin mb-3" />
              <p className="text-[12px] font-semibold text-slate-600">Өгөгдлийг ачаалж байна…</p>
            </div>
          )}
          {dsId && !dataLoading && routes.length === 0 && (
            <div className="flex flex-col items-center justify-center py-16 text-slate-400">
              <p className="font-semibold text-slate-600 mb-1">Чиглэл байхгүй байна</p>
              <p className="text-[11px]">Доорх "+ Чиглэл нэмэх" товч эсвэл дээрх "Файл импортлох" товчийг дарна уу</p>
            </div>
          )}
          {dsId && !dataLoading && filteredRoutes.map((route, i) => (
            <RouteCard key={route.uid} route={route} index={i} vehicles={vehicles} stores={stores}
              onRemove={() => removeRoute(route.uid)}
              onVehicleChange={(vid) => updateVehicle(route.uid, vid)}
              onAddStop={(store) => addStop(route.uid, store)}
              onRemoveStop={(stopUid) => removeStop(route.uid, stopUid)}
              onMoveStop={(stopUid, dir) => moveStop(route.uid, stopUid, dir)}
              onReorderStops={(newStops) => reorderStops(route.uid, newStops)} />
          ))}
        </div>

        {/* Footer */}
        <div className="shrink-0 flex items-center justify-between px-5 py-3.5 border-t border-slate-200 bg-slate-50">
          <div className="flex items-center gap-2">
            <Btn size="sm" variant="ghost" onClick={addRoute} disabled={!dsId || dataLoading}>＋ Чиглэл нэмэх</Btn>
            {hasAnySortable && (
              <Btn size="sm" variant="ghost" onClick={autoSortAllRoutes} title="Бүх чиглэлийг хамгийн ойр дарааллаар эрэмбэлэх">✦ Бүгдийг эрэмбэлэх</Btn>
            )}
            {routes.length > 0 && <span className="text-[10px] text-slate-400">{routes.length} чиглэл · {totalStops} зогсоол</span>}
          </div>
          <div className="flex items-center gap-2">
            <Btn size="sm" variant="ghost" onClick={onClose}>Цуцлах</Btn>
            <Btn size="sm" variant="primary" loading={loading} onClick={save} disabled={!dsId}>
              {mode === "edit" ? "Шинэ хувилбар хадгалах" : "Чиглэл үүсгэх"}
            </Btn>
          </div>
        </div>
      </div>
    </div>
  );
}

// ── AddPanel (thin wrapper) ────────────────────────────────────────────
export function AddPanel({ open, onClose }: { open: boolean; onClose: () => void }) {
  return <RouteBuilderModal open={open} onClose={onClose} mode="create" />;
}