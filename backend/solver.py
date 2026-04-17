# ============================================================
#  solver.py  v9.4
# ============================================================

import math
import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple

import numpy as np
from ortools.constraint_solver import pywrapcp, routing_enums_pb2

import config

log = logging.getLogger(__name__)

MAX_ROUTE_TIME = 48 * 3600  # 172 800 s


@dataclass
class SolverConfig:
    mode                  : str   = "cheapest"
    max_trips             : int   = config.MAX_TRIPS_PER_VEHICLE
    solver_time_s         : int   = config.MAX_SOLVER_TIME_SECONDS
    rural_solver_time     : int   = config.MAX_SOLVER_TIME_SECONDS
    max_weight_fill       : float = config.MAX_WEIGHT_FILL_PERCENTAGE
    max_volume_fill       : float = config.MAX_VOLUME_FILL_PERCENTAGE

    reload_time_s         : int   = config.RELOAD_TIME_SECONDS
    service_time_base_s   : int   = config.SERVICE_TIME_SECONDS
    service_time_per_kg_s : float = 0.0

    penalty_unserved      : int   = config.PENALTY_UNSERVED
    vehicle_fixed_cost    : int   = config.VEHICLE_FIXED_COST
    m3_scale              : int   = config.M3_SCALE
    far_threshold_km      : float = config.FAR_THRESHOLD_KM

    max_wait_slack_s      : int   = 18 * 3_600

    backtrack_threshold   : float = 0.70
    backtrack_factor      : float = 1.30
    outbound_threshold    : float = 0.85
    outbound_factor       : float = 25.0

    geo_angular_w         : float = 0.60

    # Urban/rural configuration
    contractor_cost_mult  : float = config.CONTRACTOR_COST_MULT
    fleet_cost_mult       : float = config.FLEET_COST_MULT
    urban_max_cap_m3      : float = config.URBAN_MAX_CAP_M3
    urban_max_cap_kg      : float = config.URBAN_MAX_CAP_KG

    # How strongly to enforce closest-first ordering within a contractor route.
    closest_first_factor  : float = 3.0


def _haversine_m(lat1, lon1, lat2, lon2):
    R = 6_371_000.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    a = (math.sin(math.radians(lat2 - lat1) / 2) ** 2
         + math.cos(p1) * math.cos(p2)
         * math.sin(math.radians(lon2 - lon1) / 2) ** 2)
    return 2.0 * R * math.asin(math.sqrt(max(0.0, a)))


def _bearing(lat1, lon1, lat2, lon2):
    dlon = math.radians(lon2 - lon1)
    r1   = math.radians(lat1)
    r2   = math.radians(lat2)
    x    = math.sin(dlon) * math.cos(r2)
    y    = math.cos(r1) * math.sin(r2) - math.sin(r1) * math.cos(r2) * math.cos(dlon)
    return (math.degrees(math.atan2(x, y)) + 360) % 360


def _angular_diff(a1, a2):
    d = abs(a1 - a2) % 360
    return d if d <= 180 else 360 - d


def _estimate_speed(n1, n2):
    d = _haversine_m(n1["lat"], n1["lon"], n2["lat"], n2["lon"])
    if d < 1_000:   return 20.0
    if d < 5_000:   return 30.0
    if d < 20_000:  return 45.0
    return 70.0


def _speed_factor(hour):
    return config.HOUR_SPEED_FACTOR.get(int(hour) % 24, 1.0)


def _trip_depart_hour(fleet, offset_s):
    start_h = config.FLEET_SCHEDULE[fleet]["start_hour"]
    return int(start_h + offset_s / 3600) % 24


def _normalise_id(k):
    try:
        return str(int(k))
    except (ValueError, TypeError):
        return str(k)


def _build_submatrix(dist_df, dur_df, nodes, depot_name, depart_hour):
    norm_index = [_normalise_id(x) for x in dist_df.index]
    dist_s     = dist_df.copy()
    dist_s.index   = norm_index
    dist_s.columns = norm_index
    dur_s_df       = dur_df.copy()
    dur_s_df.index   = norm_index
    dur_s_df.columns = norm_index
    all_ids = set(norm_index)

    n      = len(nodes)
    dist   = np.zeros((n, n), dtype=np.float64)
    dur    = np.zeros((n, n), dtype=np.float64)
    factor = _speed_factor(depart_hour)

    def _key(nd):
        nid = depot_name if nd["is_depot"] else _normalise_id(nd["node_id"])
        return nid if nid in all_ids else None

    keys = [_key(nd) for nd in nodes]

    for i in range(n):
        for j in range(n):
            ki, kj = keys[i], keys[j]
            if ki and kj and ki in dist_s.index and kj in dist_s.columns:
                d = float(dist_s.at[ki, kj])
                raw_dur = (
                    float(dur_s_df.at[ki, kj])
                    if (ki in dur_s_df.index and kj in dur_s_df.columns)
                    else 0.0
                )
                if raw_dur > 0:
                    t = raw_dur * 60.0
                else:
                    speed_kmh = _estimate_speed(nodes[i], nodes[j])
                    t = d / (speed_kmh * 1000 / 3600)

                if t == 0.0 and i != j:
                    hav = _haversine_m(nodes[i]["lat"], nodes[i]["lon"],
                                       nodes[j]["lat"], nodes[j]["lon"])
                    d = max(d, hav)
                    t = d / (40_000.0 / 3600.0)

                if d < 50:
                    t += 20

                dist[i][j] = d
                dur[i][j]  = t
            else:
                d_m = _haversine_m(nodes[i]["lat"], nodes[i]["lon"],
                                   nodes[j]["lat"], nodes[j]["lon"])
                dist[i][j] = d_m
                dur[i][j]  = d_m / (40_000.0 / 3600.0)

    if factor != 1.0:
        dur = dur / factor

    return dist, dur


def _depot_travel_times(dur_df, dist_df, depot_name, stores, fleet, depart_hour):
    norm_index = [_normalise_id(x) for x in dur_df.index]
    dur_s_df   = dur_df.copy()
    dur_s_df.index   = norm_index
    dur_s_df.columns = norm_index
    all_ids = set(norm_index)

    dk = _normalise_id(depot_name)
    dk = dk if dk in all_ids else None

    factor  = _speed_factor(depart_hour)
    nids    = []
    durs    = []
    skipped_fleet   = 0
    missing_matrix  = 0

    for s in stores:
        if fleet == "DRY"  and not s.get("has_dry", False):
            skipped_fleet += 1
            continue
        if fleet == "COLD" and not s.get("has_cold", False):
            skipped_fleet += 1
            continue

        sk = _normalise_id(s["node_id"])
        if sk not in all_ids:
            missing_matrix += 1
            if missing_matrix <= 3:
                log.warning(f"[{fleet}] Store {s['node_id']} not in matrix — haversine fallback")
        sk = sk if sk in all_ids else None
        nids.append(s["node_id"])

        if dk and sk:
            durs.append(float(dur_s_df.at[dk, sk]) * 60.0 / factor)
        else:
            dep = config.DEPOTS[depot_name]
            d_m = _haversine_m(dep["lat"], dep["lon"], s["lat"], s["lon"])
            durs.append(d_m / (40_000.0 / 3600.0) / factor)

    log.info(f"[{fleet}] _depot_travel_times: {len(stores)} in, {skipped_fleet} skipped, "
             f"{missing_matrix} missing matrix, {len(nids)} out")

    return np.array(durs, dtype=np.float64), nids


def _build_nodes(depot, stores, fleet, travel_s, store_nids, sched, cfg, season="summer"):
    shift_s = sched["start_hour"] * 3600
    id_to_travel = dict(zip(store_nids, travel_s))
    dep_lat = float(depot["lat"])
    dep_lon = float(depot["lon"])

    nodes = [{
        "node_id"   : depot["name"],
        "lat"       : dep_lat,
        "lon"       : dep_lon,
        "tw_open"   : 0,
        "tw_close"  : MAX_ROUTE_TIME,
        "demand_kg" : 0.0,
        "demand_m3" : 0.0,
        "is_depot"  : True,
        "store"     : None,
        "travel_s"  : 0.0,
        "bearing"   : 0.0,
        "service_s" : 0,
        "is_urban"  : False,
    }]

    for s in stores:
        if fleet == "DRY"  and not s.get("has_dry", False):  continue
        if fleet == "COLD" and not s.get("has_cold", False): continue

        t_s        = float(id_to_travel.get(s["node_id"], 0.0))
        wall_open  = int(s.get("open_s", 0))
        wall_close = int(s.get("close_s", 86399))
        is_all_day = (wall_open == 0 and wall_close >= 86_398)

        if is_all_day:
            tw_open  = 0
            tw_close = MAX_ROUTE_TIME
        else:
            tw_open  = max(0, wall_open  - shift_s)
            tw_close = wall_close - shift_s
            if tw_close <= 0 or tw_close <= tw_open:
                tw_open  = 0
                tw_close = MAX_ROUTE_TIME

        if not is_all_day and t_s > tw_close:
            tw_open  += 86_400
            tw_close += 86_400
            tw_open  = min(tw_open,  MAX_ROUTE_TIME)
            tw_close = min(tw_close, MAX_ROUTE_TIME)

        try:
            seasonal_data = s.get("seasonal_data", {})
            if seasonal_data and season in seasonal_data:
                sv = seasonal_data[season]
                dem_kg = float(sv.get("dry_kg"  if fleet == "DRY" else "cold_kg",  0.0))
                dem_m3 = float(sv.get("dry_cbm" if fleet == "DRY" else "cold_cbm", 0.0))
                if dem_kg == 0.0 and dem_m3 == 0.0:
                    dem_kg = float(s.get("dry_kg"  if fleet == "DRY" else "cold_kg",  0.0))
                    dem_m3 = float(s.get("dry_cbm" if fleet == "DRY" else "cold_cbm", 0.0))
            else:
                dem_kg = float(s.get("dry_kg"  if fleet == "DRY" else "cold_kg",  0.0))
                dem_m3 = float(s.get("dry_cbm" if fleet == "DRY" else "cold_cbm", 0.0))
        except (KeyError, ValueError, TypeError) as e:
            log.error(f"[{fleet}] Store {s.get('node_id')} seasonal lookup failed: {e}")
            dem_kg = dem_m3 = 0.0

        if dem_kg == 0.0 and dem_m3 == 0.0:
            continue

        bear = _bearing(dep_lat, dep_lon, float(s["lat"]), float(s["lon"]))
        svc  = int(cfg.service_time_base_s + dem_kg * cfg.service_time_per_kg_s)

        nodes.append({
            "node_id"   : s["node_id"],
            "lat"       : float(s["lat"]),
            "lon"       : float(s["lon"]),
            "tw_open"   : int(tw_open),
            "tw_close"  : int(tw_close),
            "demand_kg" : dem_kg,
            "demand_m3" : dem_m3,
            "is_depot"  : False,
            "store"     : s,
            "travel_s"  : t_s,
            "bearing"   : bear,
            "service_s" : svc,
            "is_urban"  : s.get("is_urban", False),
        })

    return nodes


def _build_sector_routes(nodes, n_vehicles):
    customers = [
        (i, nd["bearing"])
        for i, nd in enumerate(nodes)
        if not nd["is_depot"]
    ]
    customers.sort(key=lambda x: x[1])

    routes  = [[] for _ in range(n_vehicles)]
    per_v   = max(1, math.ceil(len(customers) / n_vehicles))

    for k, (ni, _) in enumerate(customers):
        vi = min(k // per_v, n_vehicles - 1)
        routes[vi].append(ni)

    return routes


def _make_time_cb(manager, dur_s, svc_times):
    def cb(fi, ti):
        ni = manager.IndexToNode(fi)
        nj = manager.IndexToNode(ti)
        turn_penalty = 8 if ni != 0 else 0
        return int(dur_s[ni][nj] + svc_times[ni] + turn_penalty)
    return cb


def _make_antibt_dist_cb(manager, dist_dm, dist_depot, is_depot_mask,
                          threshold, factor, out_threshold, out_factor):
    def cb(fi, ti):
        ni   = manager.IndexToNode(fi)
        nj   = manager.IndexToNode(ti)
        base = int(dist_dm[ni][nj])
        if is_depot_mask[ni] or is_depot_mask[nj]:
            return base
        d_i = dist_depot[ni]
        d_j = dist_depot[nj]
        if d_i > 100:
            ratio = d_j / d_i
            if ratio < out_threshold:
                return int(base * out_factor)
            if ratio < threshold:
                return int(base * factor)
        return base
    return cb


def _make_geo_cb(manager, dist_dm, dist_depot, is_depot_mask,
                 bearings, angular_w, bt_threshold, bt_factor, out_threshold, out_factor):
    def cb(fi, ti):
        ni   = manager.IndexToNode(fi)
        nj   = manager.IndexToNode(ti)
        base = int(dist_dm[ni][nj])
        if is_depot_mask[ni] or is_depot_mask[nj]:
            return base
        ang   = _angular_diff(bearings[ni], bearings[nj])
        extra = int(base * (ang / 180.0) ** 2 * angular_w)
        cost  = base + extra
        d_i = dist_depot[ni]
        d_j = dist_depot[nj]
        if d_i > 100:
            ratio = d_j / d_i
            if ratio < out_threshold:
                return int(cost * out_factor)
            if ratio < bt_threshold:
                return int(cost * bt_factor)
        return cost
    return cb


def _make_raw_dist_cb(manager, dist_dm):
    """True shortest path: pure raw distance, no penalties."""
    def cb(fi, ti):
        ni = manager.IndexToNode(fi)
        nj = manager.IndexToNode(ti)
        return int(dist_dm[ni][nj])
    return cb


def _make_fuel_cb(
    manager,
    dist_dm,
    dist_depot,
    is_depot_mask,
    fpm,
    bt_threshold,
    bt_factor,
    out_threshold,
    out_factor,
    is_contractor    : bool  = False,
    far_threshold_m  : float = 1_000_000,
    closest_first_factor: float = 3.0,
    nodes            : List   = None,
):
    """
    Fuel-only arc cost callback (per-km).
    vehicle_cost and labor_cost are daily fixed costs handled separately.
    """
    node_is_rural = [not nd.get("is_urban", True) for nd in nodes]

    def cb(fi, ti):
        ni = manager.IndexToNode(fi)
        nj = manager.IndexToNode(ti)

        base = int(dist_dm[ni][nj] * fpm)

        if is_depot_mask[ni] or is_depot_mask[nj]:
            return base

        d_i = dist_depot[ni]
        d_j = dist_depot[nj]

        if is_contractor:
            base = int(base * 1.2)

            if node_is_rural[nj]:
                base = int(base * 0.8)

            if d_i > 100:
                if d_j < d_i:
                    return int(base * 50)

            if d_i > far_threshold_m:
                ratio = d_j / d_i if d_i > 0 else 1.0
                if ratio < out_threshold:
                    return int(base * out_factor)

            return int(base)

        else:
            base = int(base * 0.9)

            if node_is_rural[nj]:
                base = int(base * 1.5)

            if d_i > 100:
                if d_j < d_i:
                    return int(base * 50)

            if d_i > far_threshold_m:
                ratio = d_j / d_i if d_i > 0 else 1.0
                if ratio < out_threshold:
                    return int(base * out_factor)
                elif ratio < bt_threshold:
                    return int(base * bt_factor)

            return int(base)

    return cb


# ════════════════════════════════════════════════════════════
#  Unserved diagnosis
# ════════════════════════════════════════════════════════════

def _diagnose(nd, vehicles, dist_mat, nid_to_idx, nodes, sched, cfg):
    dkg    = nd["demand_kg"]
    dm3    = nd["demand_m3"]
    max_kg = max((v["cap_kg"] for v in vehicles), default=0)
    max_m3 = max((v["cap_m3"] for v in vehicles), default=0)
    t_s    = nd.get("travel_s", 0.0)
    svc_s  = nd.get("service_s", cfg.service_time_base_s)

    ni      = nid_to_idx.get(nd["node_id"], 0)
    dist_km = float(dist_mat[0][ni]) / 1000.0 if ni else 0.0

    tw_open  = nd.get("tw_open",  0)
    tw_close = nd.get("tw_close", 0)

    log.warning("[DROP DEBUG] %s | travel=%.2fh | round_trip=%.2fh | 48h_ceiling=%.0fh",
                nd["node_id"], t_s / 3600, (t_s * 2 + svc_s) / 3600, MAX_ROUTE_TIME / 3600)

    if dkg > max_kg * 1.01:
        needed = math.ceil(dkg / 100) * 100
        return (f"⚖️  Demand {dkg:,.0f} kg exceeds the largest vehicle "
                f"({max_kg:,.0f} kg). Split or add ≥{needed:,} kg vehicle.")

    if dm3 > max_m3 * 1.01:
        needed_m3 = round(dm3 * 1.05, 2)
        return (f"📦  Demand {dm3:.2f} m³ exceeds the largest vehicle "
                f"({max_m3:.2f} m³). Split or add ≥{needed_m3:.2f} m³ vehicle.")

    round_trip_s = t_s * 2 + svc_s
    if round_trip_s > MAX_ROUTE_TIME:
        rth = round_trip_s / 3600
        return (f"🚛  Physically unreachable within 48h. "
                f"Round-trip = {rth:.1f}h. Use regional staging depot.")

    if tw_close < MAX_ROUTE_TIME and t_s > tw_close:
        shift_s      = sched["start_hour"] * 3600
        close_wall   = (tw_close + shift_s) // 3600
        earliest_arr = sched["start_hour"] + t_s / 3600
        return (f"⏰  Cannot arrive before store closes at {close_wall:02d}:00. "
                f"Earliest arrival {int(earliest_arr):02d}:{int((earliest_arr%1)*60):02d}.")

    if tw_open >= tw_close:
        raw = nd.get("store") or {}
        ro  = int(raw.get("open_s",  tw_open))
        rc  = int(raw.get("close_s", tw_close))
        return (f"🗂️  Invalid time window: open={ro//3600:02d}:{(ro%3600)//60:02d}, "
                f"close={rc//3600:02d}:{(rc%3600)//60:02d}. Check open_s/close_s.")

    total_cap_kg = sum(v["cap_kg"] for v in vehicles) * cfg.max_trips
    total_cap_m3 = sum(v["cap_m3"] for v in vehicles) * cfg.max_trips
    total_dem_kg = sum(n2["demand_kg"] for n2 in nodes[1:])
    total_dem_m3 = sum(n2["demand_m3"] for n2 in nodes[1:])

    if total_dem_kg > total_cap_kg * 0.95:
        over = (total_dem_kg / total_cap_kg - 1) * 100
        return (f"🏋️  Fleet weight exhausted: {total_dem_kg:,.0f} kg vs "
                f"{total_cap_kg:,.0f} kg ({over:+.1f}% over). Add vehicles or increase max_trips.")

    if total_dem_m3 > total_cap_m3 * 0.95:
        over = (total_dem_m3 / total_cap_m3 - 1) * 100
        return (f"📐  Fleet volume exhausted: {total_dem_m3:.1f} m³ vs "
                f"{total_cap_m3:.1f} m³ ({over:+.1f}% over).")

    if dist_km > cfg.far_threshold_km:
        fits = dkg <= max_kg * cfg.max_weight_fill
        return (f"📍  {dist_km:.0f} km from depot (threshold {cfg.far_threshold_km:.0f} km). "
                f"{'Fits by weight.' if fits else 'Does NOT fit.'} "
                f"Try mode='geographic' or increase solver_time_s.")

    window_h = (tw_close - tw_open) / 3600
    util_pct = (total_dem_kg / total_cap_kg * 100) if total_cap_kg else 0
    return (f"🔧  Dropped during optimisation ({dist_km:.0f} km, "
            f"window {window_h:.1f}h, demand {dkg:.0f} kg/{dm3:.2f} m³, "
            f"util {util_pct:.0f}%). "
            f"Try solver_time_s→180+, max_trips→{cfg.max_trips+1}, or add contractor.")


# ════════════════════════════════════════════════════════════
#  Core OR-Tools solver  (single trip)
# ════════════════════════════════════════════════════════════

def _or_tools_solve(fleet, depot, stores, vehicles, dist_df, dur_df,
                    cfg, trip_num=1, season="summer",
                    trucks_already_used: Optional[Set[str]] = None):
    """
    trucks_already_used: set of truck_ids that have already completed at least
    one trip today.  For these trucks, vehicle_cost and labor_cost are NOT
    charged again (they are daily costs, not per-trip costs).
    Only fuel (arc cost, per-km) repeats on every trip.
    """
    if trucks_already_used is None:
        trucks_already_used = set()

    sched   = config.FLEET_SCHEDULE[fleet]
    shift_s = sched["start_hour"] * 3600

    min_offset  = min(int(v.get("start_offset", 0)) for v in vehicles)
    depart_hour = _trip_depart_hour(fleet, min_offset)

    travel_s, store_nids = _depot_travel_times(
        dur_df, dist_df, depot["name"], stores, fleet, depart_hour
    )
    if not store_nids:
        log.error(f"[{fleet}] No store_nids from _depot_travel_times")
        return {"routes": [], "unserved": [], "nodes": [], "fleet": fleet}

    nodes = _build_nodes(depot, stores, fleet, travel_s, store_nids, sched, cfg, season)
    n_eligible = len(nodes) - 1
    if n_eligible == 0:
        log.error(f"[{fleet}] No eligible nodes after _build_nodes")
        return {"routes": [], "unserved": [], "nodes": nodes, "fleet": fleet}

    n  = len(nodes)
    nv = len(vehicles)

    dist_mat, dur_mat = _build_submatrix(dist_df, dur_df, nodes, depot["name"], depart_hour)
    dist_dm    = (dist_mat / 10.0).astype(np.int64)
    dur_s      = dur_mat.astype(np.int64)
    dist_depot = dist_mat[0, :].copy()

    is_depot_mask = [nd["is_depot"] for nd in nodes]
    bearings      = [nd["bearing"]  for nd in nodes]
    svc_times     = np.array([nd["service_s"] for nd in nodes], dtype=np.int64)

    manager = pywrapcp.RoutingIndexManager(n, nv, [0] * nv, [0] * nv)
    routing = pywrapcp.RoutingModel(manager)

    # ── HARD CONSTRAINT: big trucks cannot serve urban stores ──
    for vi, veh in enumerate(vehicles):
        for ni, nd in enumerate(nodes):
            if nd["is_depot"]:
                continue
            if nd.get("is_urban", False):
                if veh["cap_m3"] > cfg.urban_max_cap_m3 or veh["cap_kg"] > cfg.urban_max_cap_kg:
                    routing.VehicleVar(manager.NodeToIndex(ni)).RemoveValue(vi)

    time_cb_idx = routing.RegisterTransitCallback(
        _make_time_cb(manager, dur_s, svc_times)
    )
    antibt_cb_idx = routing.RegisterTransitCallback(
        _make_antibt_dist_cb(
            manager, dist_dm, dist_depot, is_depot_mask,
            cfg.backtrack_threshold, cfg.backtrack_factor,
            cfg.outbound_threshold,  cfg.outbound_factor,
        )
    )
    geo_cb_idx = routing.RegisterTransitCallback(
        _make_geo_cb(
            manager, dist_dm, dist_depot, is_depot_mask,
            bearings, cfg.geo_angular_w,
            cfg.backtrack_threshold, cfg.backtrack_factor,
            cfg.outbound_threshold,  cfg.outbound_factor,
        )
    )

    # ── Helper: compute the daily fixed cost for a vehicle ──────────────────
    # vehicle_cost and labor_cost are PER-DAY costs.
    # If a truck has already run a trip today, these are already paid — charge 0.
    # Fuel cost is per-km (arc cost) and is always charged, so not included here.
    def _daily_fixed_cost(veh: dict) -> int:
        if veh["truck_id"] in trucks_already_used:
            # Daily overhead already charged on trip 1; only fuel (arc cost) applies.
            return 0
        return int(veh.get("vehicle_cost", 0) + veh.get("labor_cost", 0))

    # ════════════════════════════════════════════════════════
    # MODE: cheapest
    #   Objective: minimise total real cost (fuel + daily overhead).
    #   Fleet is cheap primary (low fixed cost multiplier).
    #   Contractors are expensive fallback (high fixed cost multiplier).
    #   Urban/rural bias applied via _make_fuel_cb arc costs.
    # ════════════════════════════════════════════════════════
    if cfg.mode == "cheapest":
        for vi, veh in enumerate(vehicles):
            fpm = veh["fuel_cost_km"] / 10_000.0
            routing.SetArcCostEvaluatorOfVehicle(
                routing.RegisterTransitCallback(
                    _make_fuel_cb(
                        manager, dist_dm, dist_depot, is_depot_mask,
                        fpm,
                        cfg.backtrack_threshold, cfg.backtrack_factor,
                        cfg.outbound_threshold,  cfg.outbound_factor,
                        is_contractor=veh.get("is_contractor", False),
                        far_threshold_m=cfg.far_threshold_km * 1000,
                        closest_first_factor=cfg.closest_first_factor,
                        nodes=nodes,
                    )
                ),
                vi,
            )

            daily_cost = _daily_fixed_cost(veh)

            if veh.get("is_contractor"):
                fixed = int(daily_cost * cfg.contractor_cost_mult)
                log.debug(f"[{fleet}] Trip {trip_num} {veh['truck_id']} CONTRACTOR "
                          f"daily_cost={'already_paid' if veh['truck_id'] in trucks_already_used else daily_cost} "
                          f"→ fixed={fixed} (×{cfg.contractor_cost_mult})")
            else:
                fixed = int(daily_cost * cfg.fleet_cost_mult)
                log.debug(f"[{fleet}] Trip {trip_num} {veh['truck_id']} FLEET "
                          f"daily_cost={'already_paid' if veh['truck_id'] in trucks_already_used else daily_cost} "
                          f"→ fixed={fixed} (×{cfg.fleet_cost_mult})")

            routing.SetFixedCostOfVehicle(fixed, vi)

        span_coeff = 10

    # ════════════════════════════════════════════════════════
    # MODE: fastest
    #   Objective: minimise total travel + service time.
    #   No contractor/fleet cost discrimination — use any vehicle
    #   that can serve the stop fastest.
    #   Daily fixed cost still charged once per day (not per trip),
    #   but multipliers are ignored so the solver picks on speed alone.
    # ════════════════════════════════════════════════════════
    elif cfg.mode == "fastest":
        routing.SetArcCostEvaluatorOfAllVehicles(time_cb_idx)
        for vi, veh in enumerate(vehicles):
            # No contractor/fleet multiplier — just bare daily cost.
            # This lets contractors compete on equal footing with fleet
            # so the solver can assign whichever truck finishes fastest.
            fixed = _daily_fixed_cost(veh)
            routing.SetFixedCostOfVehicle(fixed, vi)
            log.debug(f"[{fleet}] Trip {trip_num} {veh['truck_id']} fastest "
                      f"fixed={fixed}")
        span_coeff = 50   # strong span penalty → equalise finish times

    # ════════════════════════════════════════════════════════
    # MODE: shortest
    #   Objective: minimise total distance driven.
    #   Pure raw distance callback (no anti-backtrack penalties).
    #   Fixed costs set to zero (don't penalise vehicle use).
    # ════════════════════════════════════════════════════════
    elif cfg.mode == "shortest":
        raw_dist_cb_idx = routing.RegisterTransitCallback(
            _make_raw_dist_cb(manager, dist_dm)
        )
        routing.SetArcCostEvaluatorOfAllVehicles(raw_dist_cb_idx)
        for vi, veh in enumerate(vehicles):
            routing.SetFixedCostOfVehicle(0, vi)   # ← zero: don't penalise vehicle use
            log.debug(f"[{fleet}] Trip {trip_num} {veh['truck_id']} shortest fixed=0")
        span_coeff = 0

    # ════════════════════════════════════════════════════════
    # MODE: balanced
    #   Objective: minimise distance while equalising workload.
    #   Fixed costs charged once per day (no multipliers).
    # ════════════════════════════════════════════════════════
    elif cfg.mode == "balanced":
        routing.SetArcCostEvaluatorOfAllVehicles(antibt_cb_idx)
        for vi, veh in enumerate(vehicles):
            fixed = _daily_fixed_cost(veh)
            routing.SetFixedCostOfVehicle(fixed, vi)
            log.debug(f"[{fleet}] Trip {trip_num} {veh['truck_id']} balanced "
                      f"fixed={fixed}")
        span_coeff = config.BALANCED_SPAN_COEFF

    # ════════════════════════════════════════════════════════
    # MODE: geographic
    #   Objective: cluster deliveries by compass sector.
    #   Angular-weighted arc cost groups geographically close stores.
    #   Fixed costs charged once per day (no multipliers).
    # ════════════════════════════════════════════════════════
    elif cfg.mode == "geographic":
        routing.SetArcCostEvaluatorOfAllVehicles(geo_cb_idx)
        for vi, veh in enumerate(vehicles):
            fixed = _daily_fixed_cost(veh)
            routing.SetFixedCostOfVehicle(fixed, vi)
            log.debug(f"[{fleet}] Trip {trip_num} {veh['truck_id']} geographic "
                      f"fixed={fixed}")
        span_coeff = 20

    else:
        raise ValueError(f"Unknown solver mode: '{cfg.mode}'. "
                         f"Valid: cheapest | fastest | shortest | balanced | geographic")

    # ── Capacity dimensions ──────────────────────────────────────────────────
    def _kg_cb(idx):
        return int(nodes[manager.IndexToNode(idx)]["demand_kg"])

    kg_cb = routing.RegisterUnaryTransitCallback(_kg_cb)
    routing.AddDimensionWithVehicleCapacity(
        kg_cb, 0,
        [int(v["cap_kg"] * cfg.max_weight_fill) for v in vehicles],
        True, "CapKg",
    )

    def _m3_cb(idx):
        return int(nodes[manager.IndexToNode(idx)]["demand_m3"] * cfg.m3_scale)

    m3_cb = routing.RegisterUnaryTransitCallback(_m3_cb)
    routing.AddDimensionWithVehicleCapacity(
        m3_cb, 0,
        [int(v["cap_m3"] * cfg.m3_scale * cfg.max_volume_fill) for v in vehicles],
        True, "CapM3",
    )

    # ── Time dimension ───────────────────────────────────────────────────────
    routing.AddDimension(time_cb_idx, cfg.max_wait_slack_s, MAX_ROUTE_TIME, False, "Time")
    time_dim = routing.GetDimensionOrDie("Time")

    for i, nd in enumerate(nodes):
        if nd["is_depot"]:
            continue
        time_dim.CumulVar(manager.NodeToIndex(i)).SetRange(nd["tw_open"], nd["tw_close"])

    max_h_s = (sched["max_horizon_hour"] - sched["start_hour"]) * 3600

    for vi, veh in enumerate(vehicles):
        start_off = int(veh.get("start_offset", 0))
        time_dim.CumulVar(routing.Start(vi)).SetRange(start_off, start_off)
        time_dim.CumulVar(routing.End(vi)).SetRange(start_off, MAX_ROUTE_TIME)
        time_dim.SetCumulVarSoftUpperBound(routing.End(vi), max_h_s, 50)

    if span_coeff > 0:
        time_dim.SetGlobalSpanCostCoefficient(span_coeff)

    # ── Disjunctions (allow stores to be dropped with penalty) ──────────────
    for i in range(1, n):
        node_idx = manager.NodeToIndex(i)
        if cfg.mode == "shortest":
            # Force solver to serve every store — dropped stops cost more
            # than any detour. Use a very large penalty so no store is ever
            # skipped for the sake of reducing distance.
            routing.AddDisjunction([node_idx], cfg.penalty_unserved * 1000)
        elif nodes[i]["travel_s"] > max_h_s * 0.6:
            routing.AddDisjunction([node_idx], cfg.penalty_unserved * 10)
        else:
            routing.AddDisjunction([node_idx], cfg.penalty_unserved)

    # ── Search parameters ────────────────────────────────────────────────────
    params = pywrapcp.DefaultRoutingSearchParameters()
    if cfg.mode == "fastest":
        params.first_solution_strategy = (
            routing_enums_pb2.FirstSolutionStrategy.PATH_MOST_CONSTRAINED_ARC)
    else:
        params.first_solution_strategy = (
            routing_enums_pb2.FirstSolutionStrategy.PATH_CHEAPEST_ARC)

    params.local_search_metaheuristic = (
        routing_enums_pb2.LocalSearchMetaheuristic.GUIDED_LOCAL_SEARCH)
    params.time_limit.seconds = cfg.solver_time_s
    params.log_search         = False

    if cfg.mode == "geographic":
        sector_routes = _build_sector_routes(nodes, nv)
        hint = routing.ReadAssignmentFromRoutes(sector_routes, True)
        if hint is not None:
            solution = routing.SolveFromAssignmentWithParameters(hint, params)
        else:
            solution = routing.SolveWithParameters(params)
    else:
        solution = routing.SolveWithParameters(params)

    if solution is None:
        log.warning("[%s] Trip %d: No solution (%d stores, %d vehicles, %ds)",
                    fleet, trip_num, n_eligible, nv, cfg.solver_time_s)
        return {
            "routes"  : [],
            "unserved": [
                {"store": nd["store"],
                 "reason": (f"🔧  No feasible solution for trip {trip_num}. "
                            f"Try solver_time_s→{cfg.solver_time_s*2}s or add contractors."),
                 "node": nd}
                for nd in nodes[1:]
            ],
            "nodes": nodes,
            "fleet": fleet,
        }

    nid_to_idx : Dict[str, int] = {nd["node_id"]: i for i, nd in enumerate(nodes)}
    raw_routes  = []
    served_ids  = set()

    for vi, veh in enumerate(vehicles):
        idx = routing.Start(vi)
        if routing.IsEnd(solution.Value(routing.NextVar(idx))):
            continue

        stops        = []
        total_dist_m = 0.0
        total_dur_s  = 0.0
        load_kg      = 0.0
        load_m3      = 0.0
        last_ni      = 0
        last_t       = int(veh.get("start_offset", 0))

        while not routing.IsEnd(idx):
            ni = manager.IndexToNode(idx)
            nd = nodes[ni]

            if not nd["is_depot"]:
                served_ids.add(nd["node_id"])
                t_solver = solution.Value(time_dim.CumulVar(idx))
                arr_wall = t_solver + shift_s

                stops.append({
                    "node_id"    : nd["node_id"],
                    "store"      : nd["store"],
                    "arrival_s"  : float(arr_wall),
                    "depart_s"   : float(arr_wall + nd["service_s"]),
                    "demand_kg"  : float(nd["demand_kg"]),
                    "demand_m3"  : float(nd["demand_m3"]),
                    "lat"        : float(nd["lat"]),
                    "lon"        : float(nd["lon"]),
                    "is_next_day": bool(arr_wall >= 86_400),
                })

                load_kg += nd["demand_kg"]
                load_m3 += nd["demand_m3"]
                last_t   = t_solver + nd["service_s"]
                last_ni  = ni

            nxt = solution.Value(routing.NextVar(idx))
            if not routing.IsEnd(nxt):
                ni2 = manager.IndexToNode(nxt)
                total_dist_m += dist_mat[ni][ni2]
                total_dur_s  += dur_mat[ni][ni2]
            idx = nxt

        if not stops:
            continue

        return_leg_s  = float(dur_mat[last_ni][0])
        return_dist_m = float(dist_mat[last_ni][0])
        total_dist_m += return_dist_m
        total_dur_s  += return_leg_s
        return_time_s = last_t + return_leg_s

        raw_routes.append({
            "truck_id"      : veh["truck_id"],
            "trip_number"   : trip_num,
            "virtual_id"    : f"{veh['truck_id']}_T{trip_num}",
            "vehicle"       : veh,
            "stops"         : stops,
            "total_dist_m"  : float(total_dist_m),
            "total_dur_s"   : float(total_dur_s),
            "load_kg"       : float(load_kg),
            "load_m3"       : float(load_m3),
            "cap_kg"        : float(veh["cap_kg"]),
            "cap_m3"        : float(veh["cap_m3"]),
            "return_time_s" : float(return_time_s),
            "start_offset_s": float(veh.get("start_offset", 0)),
        })

    unserved = [
        {"store": nd["store"],
         "reason": _diagnose(nd, vehicles, dist_mat, nid_to_idx, nodes, sched, cfg),
         "node": nd}
        for nd in nodes[1:]
        if nd["node_id"] not in served_ids
    ]

    return {"routes": raw_routes, "unserved": unserved, "nodes": nodes, "fleet": fleet}


# ════════════════════════════════════════════════════════════
#  Sequential multi-trip solver
# ════════════════════════════════════════════════════════════

def _solve_fleet_multitrip(fleet, depot, stores, vehicles, dist_df, dur_df,
                            cfg, season="summer"):
    """
    Run up to cfg.max_trips sequential trip rounds.

    COST MODEL
    ──────────
    vehicle_cost + labor_cost  →  daily fixed costs, charged ONCE per truck per day.
                                  On trip 2+ for a truck that already ran trip 1,
                                  these costs are 0 (already paid).
    fuel_cost_km               →  per-km arc cost, charged on every trip.

    VEHICLE STRATEGY (cheapest mode)
    ─────────────────────────────────
    1. Fleet vehicles sorted to FRONT (cheap primary, low fixed cost multiplier).
    2. Contractors are EXPENSIVE FALLBACK (high fixed cost multiplier).
    3. Big trucks cannot serve urban stores (hard constraint).
    4. Urban = City column == "UB"; rural = everything else.
    """
    sched     = config.FLEET_SCHEDULE[fleet]
    fleet_key = "has_dry" if fleet == "DRY" else "has_cold"
    dem_field = "dry_kg"  if fleet == "DRY" else "cold_kg"

    log.info(f"[{fleet}] _solve_fleet_multitrip: {len(stores)} total stores, "
             f"{len(vehicles)} vehicles")

    # ── Eligibility filter ────────────────────────────────
    eligible = []
    for s in stores:
        if not s.get(fleet_key, False):
            continue
        if (float(s.get(dem_field, 0.0)) == 0.0
                and float(s.get("dry_cbm" if fleet=="DRY" else "cold_cbm", 0.0)) == 0.0):
            log.debug(f"[{fleet}] Store {s['node_id']} zero demand — skip")
            continue
        eligible.append(s)

    log.info(f"[{fleet}] {len(eligible)} eligible (from {len(stores)} input)")

    if not eligible:
        return {
            "routes"  : [],
            "unserved": [{"store": s, "reason": f"⚙️  No {fleet} demand", "node": None}
                         for s in stores if s.get(fleet_key, False)],
            "nodes"   : [],
            "fleet"   : fleet,
        }

    dep_lat = float(depot["lat"])
    dep_lon = float(depot["lon"])

    # ── Sort stores: closest-first ────────────────────────
    if cfg.mode == "geographic":
        eligible.sort(key=lambda s: _bearing(dep_lat, dep_lon,
                                              float(s["lat"]), float(s["lon"])))
    else:
        eligible.sort(key=lambda s: (
            _haversine_m(dep_lat, dep_lon, float(s["lat"]), float(s["lon"]))
        ))
        log.debug(f"[{fleet}] Stores sorted closest→farthest "
                  f"(nearest: {eligible[0]['node_id'] if eligible else 'n/a'}, "
                  f"farthest: {eligible[-1]['node_id'] if eligible else 'n/a'})")

    # ── Sort vehicles — fleet FIRST (cheap primary), contractors SECOND ──────
    vehicles_sorted = sorted(
        vehicles,
        key=lambda v: (1 if v.get("is_contractor", False) else 0, v["truck_id"]),
    )

    n_fleet       = sum(1 for v in vehicles_sorted if not v.get("is_contractor", False))
    n_contractors = len(vehicles_sorted) - n_fleet
    log.info(f"[{fleet}] Vehicle order: {n_fleet} fleet vehicle(s) first, "
             f"{n_contractors} contractor fallback vehicle(s)")

    remaining         = eligible
    all_routes        = []
    truck_return      = {v["truck_id"]: 0.0 for v in vehicles_sorted}
    # Track which trucks have completed at least one trip today.
    # Used by _or_tools_solve to skip daily fixed costs on subsequent trips.
    trucks_already_used: Set[str] = set()

    def build_available(trip_n):
        min_avail_s = 4 * 3600
        result = []
        for v in vehicles_sorted:
            offset = 0 if trip_n == 1 else int(
                truck_return[v["truck_id"]] + cfg.reload_time_s
            )
            if (MAX_ROUTE_TIME - offset) >= min_avail_s:
                result.append({**v, "start_offset": offset})
            else:
                log.debug(f"[{fleet}] Trip {trip_n}: {v['truck_id']} <4h left — retiring")
        return result

    for trip_num in range(1, cfg.max_trips + 1):
        if not remaining:
            break

        available = build_available(trip_num)
        if not available:
            log.info("[%s] No trucks available for trip %d — stopping.", fleet, trip_num)
            break

        n_avail_fleet = sum(1 for v in available if not v.get("is_contractor", False))
        log.info("[%s] Trip %d/%d: %d stores, %d/%d trucks (%d fleet, %d contractor), "
                 "mode=%s, budget=%ds, already_used=%s",
                 fleet, trip_num, cfg.max_trips, len(remaining),
                 len(available), len(vehicles_sorted),
                 n_avail_fleet, len(available) - n_avail_fleet,
                 cfg.mode, cfg.solver_time_s,
                 trucks_already_used or "none")

        res = _or_tools_solve(
            fleet, depot, remaining, available,
            dist_df, dur_df, cfg, trip_num, season,
            trucks_already_used=trucks_already_used,
        )
        all_routes.extend(res["routes"])

        for route in res["routes"]:
            tid = route["truck_id"]
            truck_return[tid] = route["return_time_s"]
            # Mark this truck as having completed a trip today so the next
            # trip will not charge vehicle_cost / labor_cost again.
            trucks_already_used.add(tid)

        served   = {s["node_id"] for r in res["routes"] for s in r["stops"]}
        remaining = [s for s in remaining if s["node_id"] not in served]
        log.info("[%s] Trip %d: %d served, %d remain",
                 fleet, trip_num, len(served), len(remaining))

    served_all = {s["node_id"] for r in all_routes for s in r["stops"]}
    unserved   = [
        {"store": s,
         "reason": (f"🔁  Not served after {cfg.max_trips} trip(s). "
                    f"Add contractor or increase max_trips to {cfg.max_trips+1}. "
                    f"Fleet covers {len(served_all)}/{len(eligible)} eligible stores."),
         "node": None}
        for s in eligible
        if s["node_id"] not in served_all
    ]

    return {"routes": all_routes, "unserved": unserved, "nodes": [], "fleet": fleet}


# ════════════════════════════════════════════════════════════
#  Public entry point
# ════════════════════════════════════════════════════════════

def solve(stores, vehicles, dist_df, dur_df, cfg, season="summer"):
    """
    Solve CVRPTW for DRY and COLD fleets.

    MODE REFERENCE
    ══════════════
    cheapest   Minimise total real cost: fuel (per km, every trip) +
               vehicle_cost + labor_cost (per day, first trip only).
               Fleet preferred (low multiplier); contractors are expensive
               fallback (high multiplier). Urban/rural bias via arc costs.

    fastest    Minimise total travel + service time. No contractor/fleet
               cost discrimination — solver picks whatever vehicle finishes
               the route fastest. vehicle_cost + labor_cost still charged
               once per day (but without multipliers so cost doesn't distort
               vehicle selection).

    shortest   Minimise total kilometres driven. Anti-backtrack arc cost
               prevents zigzag routes. No contractor/fleet multipliers.

    balanced   Minimise distance while equalising workload across trucks
               (global span penalty). No contractor/fleet multipliers.

    geographic Cluster deliveries by compass sector from depot. Best for
               large sparse networks. No contractor/fleet multipliers.
    """

    # ── Input validation ─────────────────────────────────────────────────────
    for s in stores:
        s["node_id"] = _normalise_id(s.get("node_id"))
        s.setdefault("has_dry",      False)
        s.setdefault("has_cold",     False)
        s.setdefault("dry_kg",       0.0)
        s.setdefault("cold_kg",      0.0)
        s.setdefault("dry_cbm",      0.0)
        s.setdefault("cold_cbm",     0.0)
        s.setdefault("open_s",       0)
        s.setdefault("close_s",      86399)
        s.setdefault("seasonal_data",{})
        try:
            float(s.get("lat", 0.0)); float(s.get("lon", 0.0))
        except (ValueError, TypeError):
            log.error(f"Store {s.get('node_id')} invalid coords")
            s["lat"] = 0.0; s["lon"] = 0.0

    for v in vehicles:
        v["truck_id"] = str(v.get("truck_id", f"UNK_{id(v)}"))
        if "fleet" not in v:
            log.warning(f"Vehicle {v['truck_id']} has no fleet — skipping")
            continue
        v.setdefault("cap_kg",        5000.0)
        v.setdefault("cap_m3",        20.0)
        v.setdefault("fuel_cost_km",  100.0)
        v.setdefault("vehicle_cost",  1000.0)
        v.setdefault("labor_cost",    500.0)
        v.setdefault("is_contractor", False)

    season = season.lower()
    log.info(f"[SOLVER] {len(stores)} stores, {len(vehicles)} vehicles, season={season.upper()}")

    dry_v  = [v for v in vehicles if v.get("fleet") == "DRY"]
    cold_v = [v for v in vehicles if v.get("fleet") == "COLD"]

    depot_dry  = {**config.DEPOTS["Dry DC"],  "name": "Dry DC"}
    depot_cold = {**config.DEPOTS["Cold DC"], "name": "Cold DC"}

    results = {}

    if dry_v:
        results["DRY"] = _solve_fleet_multitrip(
            "DRY", depot_dry, stores, dry_v, dist_df, dur_df, cfg, season)
    else:
        results["DRY"] = {
            "routes": [], "nodes": [], "fleet": "DRY",
            "unserved": [{"store": s,
                          "reason": "⚙️  No DRY vehicles configured.",
                          "node": None}
                         for s in stores if s.get("has_dry")],
        }

    if cold_v:
        results["COLD"] = _solve_fleet_multitrip(
            "COLD", depot_cold, stores, cold_v, dist_df, dur_df, cfg, season)
    else:
        results["COLD"] = {
            "routes": [], "nodes": [], "fleet": "COLD",
            "unserved": [{"store": s,
                          "reason": "⚙️  No COLD vehicles configured.",
                          "node": None}
                         for s in stores if s.get("has_cold")],
        }

    for fleet, res in results.items():
        log.info("[SOLVER RESULT %s] routes=%d, served=%d, unserved=%d",
                 fleet,
                 len(res.get("routes", [])),
                 sum(len(r.get("stops", [])) for r in res.get("routes", [])),
                 len(res.get("unserved", [])))

    return results