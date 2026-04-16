export interface Dataset {
  id: string; name: string; created_at: string;
  store_count: number; vehicle_count: number; has_matrix: boolean;
}
export interface Store {
  id: number; dataset_id: string;
  store_id: string; node_id: string;
  eng_name: string; mn_name: string;
  address: string; detail_addr: string;
  lat: number; lon: number;
  open_s: number; close_s: number;
  dry_cbm: number; dry_kg: number;
  cold_cbm: number; cold_kg: number;
  has_dry: boolean; has_cold: boolean;
  seasonal_data?: {
    summer: { dry_cbm: number; dry_kg: number; cold_cbm: number; cold_kg: number };
    autumn: { dry_cbm: number; dry_kg: number; cold_cbm: number; cold_kg: number };
    winter: { dry_cbm: number; dry_kg: number; cold_cbm: number; cold_kg: number };
    spring: { dry_cbm: number; dry_kg: number; cold_cbm: number; cold_kg: number };
  };
}
export interface Vehicle {
  id: number; dataset_id: string;
  truck_id: string; truck_num: string; contractor: string; description: string;
  depot: string; fleet: string;
  cap_kg: number; cap_m3: number;
  fuel_cost_km: number; vehicle_cost: number; labor_cost: number;
}
export interface Job {
  id: string; dataset_id: string | null;
  group_id?: string | null; version_name?: string | null; is_manual?: boolean;
  mode: string; max_trips: number; solver_time: number;
  season?: string;
  status: "pending"|"running"|"done"|"error";
  error_msg: string | null;
  created_at: string; completed_at: string | null;
  total_served?: number; total_unserved?: number;
  total_routes?: number; total_cost?: number; total_man_hours?: number;
}
export interface RunGroup {
  id: string; name: string; dataset_id: string|null; created_at: string;
  jobs: Job[];
}
export interface OptSummary {
  mode: string; total_stores: number;
  total_served: number; total_unserved: number;
  total_routes: number; total_dist_km: number;
  total_cost: number; total_man_hours: number; warnings: string[];
}
export interface RouteSummary {
  fleet: string; truck_id: string; truck_num: string; contractor: string; description: string; fuel_cost_km: number; trip_number: number;
  route_type: string; stops: number;
  distance_km: number; duration_min: number;
  load_kg: number; cap_kg: number; util_kg_pct: number;
  load_m3: number; cap_m3: number; util_m3_pct: number;
  cost_fuel: number; cost_fixed: number; cost_labor: number; cost_total: number;
  departs_at: string; returns_at?: string; is_overnight: boolean; man_hours?: number;
}
export interface StopDetail {
  fleet: string; truck_id: string; truck_num: string; contractor: string; trip_number: number; stop_order: number;
  store_id: string; eng_name: string; mn_name: string;
  address: string; detail_addr: string;
  lat: number; lon: number;
  arrival: string; departure: string; delivery_day: string;
  is_rural: boolean; demand_kg: number; demand_m3: number;
}
export interface UnservedStore {
  fleet: string; store_id: string; eng_name: string; mn_name: string;
  address: string; lat: number; lon: number;
  demand_kg: number; demand_m3: number;
  dist_from_Dry_DC_km?: number; dist_from_Cold_DC_km?: number;
  reason: string;
}
export interface MapStop {
  lat: number; lon: number; order: number;
  store_id: string; name: string; mn_name: string;
  arrival: string; day_label: string;
  is_rural: boolean; is_next_day: boolean;
  demand_kg: number; demand_m3: number;
}
export interface MapRoute {
  route_id: string; fleet: "DRY"|"COLD";
  truck_id: string; truck_num: string; contractor: string; trip_number: number;
  is_rural: boolean; color: string; line_style: string;
  stops: MapStop[];
  polyline: [number,number][];
  depot_lat: number; depot_lon: number;
  sched_info: string;
  summary: { distance_km:number; duration_min:number; load_kg:number; load_m3:number; return_at?:string; };
}
export interface JobResult {
  job_id: string; summary: OptSummary;
  route_summary: RouteSummary[];
  stop_details: StopDetail[];
  unserved: UnservedStore[];
  map_data: MapRoute[];
}