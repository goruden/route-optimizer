# ============================================================
#  solver.py  v8.1
#
#  v8.1 fixes geographic mode (was always 0 deliveries in v8):
#
#  ROOT CAUSE (three bugs together):
#   1. SWEEP strategy in OR-Tools Python requires SetSweepArranger()
#      which is NOT exposed in the Python API.  Using it returns
#      "Undefined sweep arranger" error → solver returns None → 0 routes.
#   2. _solve_fleet_multitrip sorted by demand for ALL modes, so stores
#      passed to geographic mode had no angular order at all.
#   3. No initial sector assignment meant GLS had to reshape a random
#      PATH_CHEAPEST_ARC solution into pie slices — impossible within
#      a normal time budget.
#
#  FIX — geographic mode now uses sector partitioning:
#   a. Stores are sorted by bearing (depot→store angle) before solving.
#   b. Stores are divided into N angular sectors, one per vehicle.
#   c. ReadAssignmentFromRoutes() gives OR-Tools a sector-grouped
#      starting solution.  SolveFromAssignmentWithParameters() then
#      refines it with GLS + angular arc cost.
#   d. SWEEP is replaced by PATH_CHEAPEST_ARC (reliable fallback if
#      the sector hint is infeasible due to capacity).
#
#  HOW EACH MODE WORKS (summary at bottom of file).
# ============================================================

import math
import logging
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import numpy as np
from ortools.constraint_solver import pywrapcp, routing_enums_pb2

import config

log = logging.getLogger(__name__)


# ════════════════════════════════════════════════════════════
#  Solver configuration  (one instance per job, thread-safe)
# ════════════════════════════════════════════════════════════

@dataclass
class SolverConfig:
    """
    All solve-time parameters in one object.
    Created fresh per optimize request — no module-level globals mutated.
    Two concurrent jobs each have their own SolverConfig and are safe.
    """
    mode                  : str   = "cheapest"
    max_trips             : int   = 2
    solver_time_s         : int   = 30
    max_weight_fill       : float = 1.0
    max_volume_fill       : float = 1.0

    # Multi-trip gap (seconds between a truck returning and departing again)
    reload_time_s         : int   = 1_800

    # Service time at customer nodes
    # total_service = base_s + demand_kg × per_kg_s
    # Set per_kg_s > 0 to make larger deliveries take longer.
    service_time_base_s   : int   = 300
    service_time_per_kg_s : float = 0.0

    # OR-Tools internals
    penalty_unserved      : int   = 10_000_000
    vehicle_fixed_cost    : int   = 0
    max_wait_slack_s      : int   = 7_200      # max early-arrival wait (2 h)
    m3_scale              : int   = 1_000

    # Anti-backtrack (all modes)
    # Arc i→j costs `backtrack_factor` × more when dist(depot,j)/dist(depot,i)
    # is below `backtrack_threshold`.  Discourages outbound→depot→outbound loops.
    backtrack_threshold   : float = 0.70
    backtrack_factor      : float = 1.30

    # Geographic mode — angular arc penalty weight
    # cost(i→j) += cost(i→j) × (angular_diff / 180)² × geo_angular_w
    # 0.0 = pure distance,  1.0 = double cost for 180° direction change
    geo_angular_w         : float = 0.60

    # Diagnosis
    far_threshold_km      : float = 100.0


# ════════════════════════════════════════════════════════════
#  Geometry helpers
# ════════════════════════════════════════════════════════════

def _haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6_371_000.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    a = (math.sin(math.radians(lat2 - lat1) / 2) ** 2
         + math.cos(p1) * math.cos(p2)
         * math.sin(math.radians(lon2 - lon1) / 2) ** 2)
    return 2.0 * R * math.asin(math.sqrt(max(0.0, a)))


def _bearing(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    True-north compass bearing from point-1 to point-2, in [0, 360).

    Used to determine which angular "pie slice" a store sits in
    relative to the depot.
    """
    dlon = math.radians(lon2 - lon1)
    r1   = math.radians(lat1)
    r2   = math.radians(lat2)
    x    = math.sin(dlon) * math.cos(r2)
    y    = math.cos(r1) * math.sin(r2) - math.sin(r1) * math.cos(r2) * math.cos(dlon)
    return (math.degrees(math.atan2(x, y)) + 360) % 360


def _angular_diff(a1: float, a2: float) -> float:
    """Smallest angle between two bearings, result in [0, 180]."""
    d = abs(a1 - a2) % 360
    return d if d <= 180 else 360 - d


# ════════════════════════════════════════════════════════════
#  Time-dependent speed
# ════════════════════════════════════════════════════════════

def _speed_factor(hour: int) -> float:
    return config.HOUR_SPEED_FACTOR.get(int(hour) % 24, 1.0)


def _trip_depart_hour(fleet: str, offset_s: float) -> int:
    """
    Actual wall-clock departure hour for a specific trip.
    offset_s = shift-relative seconds when this vehicle departs.

    Example: DRY fleet starts 13:00, trip-2 vehicle offset = 7 200 s
             → wall-clock departure = 13 + 2 = 15:00
             → speed factor uses 15:00, not the fleet's fixed 13:00.
    """
    start_h = config.FLEET_SCHEDULE[fleet]["start_hour"]
    return int(start_h + offset_s / 3600) % 24


# ════════════════════════════════════════════════════════════
#  Matrix helpers
# ════════════════════════════════════════════════════════════

def _normalise_id(k) -> str:
    """
    Normalise a node ID to a canonical string so that "00198", 198,
    and "198" all resolve to the same key "198".
    Non-numeric IDs (depot names like "Dry DC") are returned as-is.
    """
    try:
        return str(int(k))
    except (ValueError, TypeError):
        return str(k)

def _matrix_key(node_id: str, all_ids: List[str]) -> Optional[str]:
    return node_id if node_id in all_ids else None


def _build_submatrix(
    dist_df,
    dur_df,
    nodes       : List[Dict],
    depot_name  : str,
    depart_hour : int,
) -> Tuple[np.ndarray, np.ndarray]:
    """
    NxN distance (metres) and speed-adjusted duration (seconds).

    Both DataFrames are re-indexed as strings so integer node IDs
    in the DataFrame match string node_id values in the store dicts
    (and vice-versa).  Without this normalisation every lookup misses
    and falls back to haversine, giving near-zero travel times between
    geographically close stores.
    """
    # ── Normalise DataFrame keys to strings once ────────────────
    norm_index = [_normalise_id(x) for x in dist_df.index]
    dist_s = dist_df.copy(); dist_s.index = norm_index; dist_s.columns = norm_index
    dur_s_df = dur_df.copy(); dur_s_df.index = norm_index; dur_s_df.columns = norm_index
    all_ids = set(norm_index)           # set for O(1) lookup
    n       = len(nodes)
    dist    = np.zeros((n, n), dtype=np.float64)
    dur     = np.zeros((n, n), dtype=np.float64)
    factor  = _speed_factor(depart_hour)

    def _key(nd):
        nid = depot_name if nd["is_depot"] else _normalise_id(nd["node_id"])
        return nid if nid in all_ids else None

    keys = [_key(nd) for nd in nodes]

    for i in range(n):
        for j in range(n):
            ki, kj = keys[i], keys[j]
            if ki and kj and ki in dist_s.index and kj in dist_s.columns:
                d   = float(dist_s.at[ki, kj])
                t   = float(dur_s_df.at[ki, kj]) * 60.0   # dur_df in minutes → seconds

                # Guard: OSRM sometimes emits 0-second arcs for very
                # close nodes.  Fall back to haversine @ 40 km/h so
                # the solver never sees a zero-duration arc between
                # two distinct nodes.
                if t == 0.0 and i != j:
                    log.debug(
                        "Zero duration in matrix %s→%s (dist %.1f m); "
                        "using haversine fallback.",
                        ki, kj, d,
                    )
                    hav = _haversine_m(
                        nodes[i]["lat"], nodes[i]["lon"],
                        nodes[j]["lat"], nodes[j]["lon"],
                    )
                    d = max(d, hav)
                    t = d / (40_000.0 / 3600.0)

                dist[i][j] = d
                dur[i][j]  = t
            else:
                d_m = _haversine_m(
                    nodes[i]["lat"], nodes[i]["lon"],
                    nodes[j]["lat"], nodes[j]["lon"],
                )
                dist[i][j] = d_m
                # 40 km/h is a more realistic urban delivery speed
                # than the previous 60 km/h default.
                dur[i][j]  = d_m / (40_000.0 / 3600.0)

    if factor != 1.0:
        dur = dur / factor

    return dist, dur


def _depot_travel_times(
    dur_df,
    dist_df,
    depot_name  : str,
    stores      : List[Dict],
    fleet       : str,
    depart_hour : int,
) -> Tuple[np.ndarray, List[str]]:
    """One-way travel times (seconds, speed-adjusted) depot → each store."""
    # Normalise keys to strings
    norm_index = [_normalise_id(x) for x in dur_df.index]
    dur_s_df = dur_df.copy(); dur_s_df.index = norm_index; dur_s_df.columns = norm_index
    all_ids = set(norm_index)
    dk = _normalise_id(depot_name)
    dk = dk if dk in all_ids else None
    factor  = _speed_factor(depart_hour)
    nids: List[str]   = []
    durs: List[float] = []

    for s in stores:
        if fleet == "DRY"  and not s["has_dry"]:  continue
        if fleet == "COLD" and not s["has_cold"]: continue

        sk = _normalise_id(s["node_id"])
        sk = sk if sk in all_ids else None
        nids.append(s["node_id"])
        if dk and sk:
            durs.append(float(dur_s_df.at[dk, sk]) * 60.0 / factor)
        else:
            dep = config.DEPOTS[depot_name]
            d_m = _haversine_m(dep["lat"], dep["lon"], s["lat"], s["lon"])
            durs.append(d_m / (40_000.0 / 3600.0) / factor)

    return np.array(durs, dtype=np.float64), nids


# ════════════════════════════════════════════════════════════
#  Node builder
# ════════════════════════════════════════════════════════════

def _build_nodes(
    depot      : Dict,
    stores     : List[Dict],
    fleet      : str,
    travel_s   : np.ndarray,
    store_nids : List[str],
    sched      : Dict,
    cfg        : SolverConfig,
) -> List[Dict]:
    """
    Build node list.  Index 0 is always the depot.

    Each node carries:
      bearing   — compass angle from depot (used by geographic arc cost)
      service_s — total service time = base + demand_kg × per_kg_s
    """
    shift_s  = sched["start_hour"] * 3600
    max_h_s  = (sched["max_horizon_hour"] - sched["start_hour"]) * 3600

    id_to_travel = dict(zip(store_nids, travel_s))
    dep_lat = float(depot["lat"])
    dep_lon = float(depot["lon"])

    nodes: List[Dict] = [{
        "node_id"   : depot["name"],
        "lat"       : dep_lat,
        "lon"       : dep_lon,
        "tw_open"   : 0,
        "tw_close"  : max_h_s,
        "demand_kg" : 0.0,
        "demand_m3" : 0.0,
        "is_depot"  : True,
        "store"     : None,
        "travel_s"  : 0.0,
        "bearing"   : 0.0,
        "service_s" : 0,
    }]

    for s in stores:
        if fleet == "DRY"  and not s["has_dry"]:  continue
        if fleet == "COLD" and not s["has_cold"]: continue

        t_s        = float(id_to_travel.get(s["node_id"], 0.0))
        wall_open  = int(s["open_s"])
        wall_close = int(s["close_s"])
        is_all_day = (wall_open == 0 and wall_close >= 86398)

        if is_all_day:
            tw_open  = 0
            tw_close = max_h_s
        else:
            tw_open  = max(0, wall_open  - shift_s)
            tw_close = min(max_h_s, wall_close - shift_s)

        if tw_close <= 0 or tw_close <= tw_open:
            tw_open  = 0
            tw_close = max_h_s

        dem_kg = float(s["dry_kg"]  if fleet == "DRY" else s["cold_kg"])
        dem_m3 = float(s["dry_cbm"] if fleet == "DRY" else s["cold_cbm"])
        bear   = _bearing(dep_lat, dep_lon, float(s["lat"]), float(s["lon"]))
        svc    = int(cfg.service_time_base_s + dem_kg * cfg.service_time_per_kg_s)

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
        })

    return nodes


# ════════════════════════════════════════════════════════════
#  Geographic sector partitioning
# ════════════════════════════════════════════════════════════

def _build_sector_routes(
    nodes     : List[Dict],
    n_vehicles: int,
) -> List[List[int]]:
    """
    Divide customer nodes into n_vehicles angular sectors and return
    an initial route per vehicle as a list of node indices.

    HOW IT WORKS
    ────────────
    1. Sort all customer nodes by their bearing from the depot (0–360°).
    2. Split the sorted list into n_vehicles equal-size chunks.
       Each chunk is one "pie slice" of the map.
    3. Return [[nodes_for_v0], [nodes_for_v1], ...].

    This list is fed directly to routing.ReadAssignmentFromRoutes()
    as a warm-start hint.  OR-Tools then applies GLS to refine the
    order within and between sectors, but starts from a geographically
    sensible grouping rather than a random one.

    NOTE: This produces equal-count sectors, not equal-angle sectors.
    Equal-count is better in practice because cities have dense clusters
    in some directions and sparse ones in others — a 45° slice toward
    downtown may have 20 stores while a 45° slice toward suburbs has 3.
    """
    customers = [
        (i, nd["bearing"])
        for i, nd in enumerate(nodes)
        if not nd["is_depot"]
    ]
    customers.sort(key=lambda x: x[1])   # sort by bearing angle

    routes   = [[] for _ in range(n_vehicles)]
    per_v    = max(1, math.ceil(len(customers) / n_vehicles))

    for k, (ni, _) in enumerate(customers):
        vi = min(k // per_v, n_vehicles - 1)
        routes[vi].append(ni)

    return routes


# ════════════════════════════════════════════════════════════
#  Arc-cost callbacks
# ════════════════════════════════════════════════════════════

def _make_time_cb(manager, dur_s: np.ndarray, svc_times: np.ndarray):
    """
    Travel time + service time at the FROM node.
    Service time is 0 for the depot (index 0).
    The cumulative value at node j = arrival time at j (before serving j).
    """
    def cb(fi, ti):
        ni = manager.IndexToNode(fi)
        nj = manager.IndexToNode(ti)
        return int(dur_s[ni][nj] + svc_times[ni])
    return cb


def _make_antibt_dist_cb(
    manager,
    dist_dm         : np.ndarray,
    dist_depot      : np.ndarray,
    is_depot_mask   : List[bool],
    threshold       : float,
    factor          : float,
):
    """
    Distance (decimetres) with anti-backtrack surcharge.

    "Backtracking" = moving to a node that is significantly closer to
    the depot than the node you just left, while still mid-route.

    This pattern causes yo-yo routes: depot → far → near → far → depot.
    Adding `factor` × cost to such arcs discourages them without
    making them infeasible (the solver can still use them if needed).

    The floor of 100 m prevents a division-by-zero for nodes very
    close to the depot.
    """
    def cb(fi, ti):
        ni   = manager.IndexToNode(fi)
        nj   = manager.IndexToNode(ti)
        base = int(dist_dm[ni][nj])
        if is_depot_mask[ni] or is_depot_mask[nj]:
            return base
        d_i = dist_depot[ni]
        d_j = dist_depot[nj]
        if d_i > 100 and (d_j / d_i) < threshold:
            return int(base * factor)
        return base
    return cb


def _make_geo_cb(
    manager,
    dist_dm       : np.ndarray,
    dist_depot    : np.ndarray,
    is_depot_mask : List[bool],
    bearings      : List[float],
    angular_w     : float,
    bt_threshold  : float,
    bt_factor     : float,
):
    """
    Geographic arc cost for geographic mode.

    FORMULA
    ───────
    base   = dist(i, j)   [decimetres]
    angle  = angular_diff(bearing_i, bearing_j)   [0–180°]
    extra  = base × (angle / 180)² × angular_w
    cost   = base + extra   (× bt_factor if backtracking)

    WHY IT WORKS
    ────────────
    Two stores facing the same direction from the depot have
    angle ≈ 0 → extra ≈ 0 → arc is cheap.
    Two stores on opposite sides of the depot have angle = 180°
    → extra = base × angular_w → arc is expensive.

    Result: the solver strongly prefers chaining stores in the same
    angular sector.  Combined with the sector-partitioned warm-start
    (ReadAssignmentFromRoutes), this produces clean pie-slice routes.

    The backtrack factor also fires here: going outbound,
    returning near the depot, then going outbound again is expensive
    both angularly AND via the backtrack penalty.
    """
    def cb(fi, ti):
        ni   = manager.IndexToNode(fi)
        nj   = manager.IndexToNode(ti)
        base = int(dist_dm[ni][nj])

        if is_depot_mask[ni] or is_depot_mask[nj]:
            return base   # depot legs: no angular penalty

        ang   = _angular_diff(bearings[ni], bearings[nj])
        extra = int(base * (ang / 180.0) ** 2 * angular_w)
        cost  = base + extra

        d_i = dist_depot[ni]
        d_j = dist_depot[nj]
        if d_i > 100 and (d_j / d_i) < bt_threshold:
            return int(cost * bt_factor)
        return cost
    return cb


def _make_fuel_cb(
    manager,
    dist_dm       : np.ndarray,
    dist_depot    : np.ndarray,
    is_depot_mask : List[bool],
    fpm           : float,
    bt_threshold  : float,
    bt_factor     : float,
):
    """Fuel cost (₮/decimetre × distance) with anti-backtrack penalty."""
    def cb(fi, ti):
        ni   = manager.IndexToNode(fi)
        nj   = manager.IndexToNode(ti)
        base = int(dist_dm[ni][nj] * fpm)
        if is_depot_mask[ni] or is_depot_mask[nj]:
            return base
        d_i = dist_depot[ni]
        d_j = dist_depot[nj]
        if d_i > 100 and (d_j / d_i) < bt_threshold:
            return int(base * bt_factor)
        return base
    return cb


# ════════════════════════════════════════════════════════════
#  Core OR-Tools solver  (single trip)
# ════════════════════════════════════════════════════════════

def _or_tools_solve(
    fleet    : str,
    depot    : Dict,
    stores   : List[Dict],
    vehicles : List[Dict],
    dist_df,
    dur_df,
    cfg      : SolverConfig,
    trip_num : int = 1,
) -> Dict:
    """
    Solve one CVRPTW trip.

    vehicles[i]["start_offset"] = shift-relative seconds when truck i
    becomes available.  Trip 1 → 0.  Trip N+1 → trip-N return + reload.

    The travel-time matrix uses the ACTUAL wall-clock departure hour
    derived from the minimum vehicle offset, so trip-2 at 15:00 gets
    15:00 congestion factors rather than the fleet's fixed start hour.
    """
    sched   = config.FLEET_SCHEDULE[fleet]
    shift_s = sched["start_hour"] * 3600
    max_h_s = (sched["max_horizon_hour"] - sched["start_hour"]) * 3600

    min_offset  = min(int(v.get("start_offset", 0)) for v in vehicles)
    depart_hour = _trip_depart_hour(fleet, min_offset)

    # ── 1. Travel times from depot ─────────────────────────────
    travel_s, store_nids = _depot_travel_times(
        dur_df, dist_df, depot["name"], stores, fleet, depart_hour
    )
    if not store_nids:
        return {"routes": [], "unserved": [], "nodes": [], "fleet": fleet}

    # ── 2. Node list ────────────────────────────────────────────
    nodes = _build_nodes(depot, stores, fleet, travel_s, store_nids, sched, cfg)
    n_eligible = len(nodes) - 1
    if n_eligible == 0:
        return {"routes": [], "unserved": [], "nodes": nodes, "fleet": fleet}

    n  = len(nodes)
    nv = len(vehicles)

    # ── 3. Matrices ─────────────────────────────────────────────
    dist_mat, dur_mat = _build_submatrix(
        dist_df, dur_df, nodes, depot["name"], depart_hour
    )
    dist_dm    = (dist_mat / 10.0).astype(np.int64)
    dur_s      = dur_mat.astype(np.int64)
    dist_depot = dist_mat[0, :].copy()

    is_depot_mask = [nd["is_depot"]  for nd in nodes]
    bearings      = [nd["bearing"]   for nd in nodes]
    svc_times     = np.array([nd["service_s"] for nd in nodes], dtype=np.int64)

    # ── 4. Routing model ────────────────────────────────────────
    manager = pywrapcp.RoutingIndexManager(n, nv, [0]*nv, [0]*nv)
    routing = pywrapcp.RoutingModel(manager)

    # ── 5. Register all callbacks ───────────────────────────────
    # time_cb  — used for the Time dimension in ALL modes
    # dist_cb  — pure distance, used by shortest/balanced
    # antibt_cb — distance + anti-backtrack, used by shortest/balanced
    # geo_cb   — distance + angular + anti-backtrack, geographic mode only
    # fuel_cb  — per-vehicle ₮ cost, cheapest mode only

    time_cb_idx = routing.RegisterTransitCallback(
        _make_time_cb(manager, dur_s, svc_times)
    )
    antibt_cb_idx = routing.RegisterTransitCallback(
        _make_antibt_dist_cb(
            manager, dist_dm, dist_depot, is_depot_mask,
            cfg.backtrack_threshold, cfg.backtrack_factor,
        )
    )
    geo_cb_idx = routing.RegisterTransitCallback(
        _make_geo_cb(
            manager, dist_dm, dist_depot, is_depot_mask,
            bearings, cfg.geo_angular_w,
            cfg.backtrack_threshold, cfg.backtrack_factor,
        )
    )

    # ── 6. Arc costs and span coefficients by mode ──────────────
    #
    #  shortest   — pure km.  No span balancing.  Fastest to solve.
    #               Anti-backtrack prevents yo-yo routes.
    #
    #  fastest    — total time (travel + service per stop).
    #               PATH_MOST_CONSTRAINED_ARC seeds by tightest window
    #               first so hard time-windows are satisfied early.
    #               Moderate span (50) balances driver hours.
    #
    #  cheapest   — per-vehicle fuel ₮ + fixed cost (vehicle+labor)
    #               per truck used.  The fixed cost makes OR-Tools
    #               weigh whether starting a truck at all is worth it.
    #               Anti-backtrack on the fuel callback.
    #
    #  balanced   — same arc cost as shortest but span = 300.
    #               A high span coefficient forces OR-Tools to equalise
    #               route durations across all trucks before caring
    #               about total distance.
    #
    #  geographic — angular arc cost so cross-sector arcs are expensive.
    #               Sector warm-start via ReadAssignmentFromRoutes
    #               (see below).  GLS refines within and between sectors.

    span_coeff = 10   # default

    if cfg.mode == "shortest":
        routing.SetArcCostEvaluatorOfAllVehicles(antibt_cb_idx)
        span_coeff = 0
        routing.SetFixedCostOfAllVehicles(cfg.vehicle_fixed_cost)

    elif cfg.mode == "fastest":
        routing.SetArcCostEvaluatorOfAllVehicles(time_cb_idx)
        span_coeff = 50
        routing.SetFixedCostOfAllVehicles(cfg.vehicle_fixed_cost)

    elif cfg.mode == "balanced":
        routing.SetArcCostEvaluatorOfAllVehicles(antibt_cb_idx)
        span_coeff = 300
        routing.SetFixedCostOfAllVehicles(cfg.vehicle_fixed_cost)

    elif cfg.mode == "geographic":
        routing.SetArcCostEvaluatorOfAllVehicles(geo_cb_idx)
        span_coeff = 20
        routing.SetFixedCostOfAllVehicles(cfg.vehicle_fixed_cost)

    else:   # cheapest
        for vi, veh in enumerate(vehicles):
            fpm = veh["fuel_cost_km"] / 10_000.0   # ₮ / decimetre
            routing.SetArcCostEvaluatorOfVehicle(
                routing.RegisterTransitCallback(
                    _make_fuel_cb(
                        manager, dist_dm, dist_depot, is_depot_mask,
                        fpm, cfg.backtrack_threshold, cfg.backtrack_factor,
                    )
                ),
                vi,
            )
            routing.SetFixedCostOfVehicle(
                int(veh.get("vehicle_cost", 0) + veh.get("labor_cost", 0)), vi
            )
        span_coeff = 10

    # ── 7. Capacity dimensions ──────────────────────────────────
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

    # ── 8. Time-window dimension ────────────────────────────────
    routing.AddDimension(
        time_cb_idx,
        cfg.max_wait_slack_s,
        max_h_s,
        False,
        "Time",
    )
    time_dim = routing.GetDimensionOrDie("Time")

    for i, nd in enumerate(nodes):
        if nd["is_depot"]:
            continue
        time_dim.CumulVar(manager.NodeToIndex(i)).SetRange(
            nd["tw_open"], nd["tw_close"]
        )

    for vi, veh in enumerate(vehicles):
        start_off = int(veh.get("start_offset", 0))
        time_dim.CumulVar(routing.Start(vi)).SetRange(start_off, start_off)
        time_dim.CumulVar(routing.End(vi)).SetRange(start_off, max_h_s)

    if span_coeff > 0:
        time_dim.SetGlobalSpanCostCoefficient(span_coeff)

    # ── 9. Disjunctions ─────────────────────────────────────────
    for i in range(1, n):
        routing.AddDisjunction([manager.NodeToIndex(i)], cfg.penalty_unserved)

    # ── 10. Search parameters ────────────────────────────────────
    params = pywrapcp.DefaultRoutingSearchParameters()

    if cfg.mode == "fastest":
        params.first_solution_strategy = (
            routing_enums_pb2.FirstSolutionStrategy.PATH_MOST_CONSTRAINED_ARC
        )
    else:
        params.first_solution_strategy = (
            routing_enums_pb2.FirstSolutionStrategy.PATH_CHEAPEST_ARC
        )

    params.local_search_metaheuristic = (
        routing_enums_pb2.LocalSearchMetaheuristic.GUIDED_LOCAL_SEARCH
    )
    params.time_limit.seconds = cfg.solver_time_s
    params.log_search          = False

    # ── 11. Geographic mode: sector warm-start ──────────────────
    #
    # WHY:  PATH_CHEAPEST_ARC produces routes based purely on arc cost
    # without regard for angular position.  Even with the geo arc cost,
    # reshaping a bad initial solution into pie slices requires many
    # GLS moves — often too many for a 120 s budget.
    #
    # FIX:  We pre-group stores into angular sectors (one per vehicle),
    # give OR-Tools that grouping as a starting assignment via
    # ReadAssignmentFromRoutes(), then run GLS to:
    #   a) reorder stops within each sector optimally
    #   b) move stores between adjacent sectors if it reduces cost
    #
    # ReadAssignmentFromRoutes(routes, ignore_inactive=True):
    #   ignore_inactive=True  → any infeasible store is simply dropped
    #   (capacity overflow, etc.) rather than making the whole hint fail.
    #
    # SolveFromAssignmentWithParameters() starts GLS from that hint.
    # If the hint is None (shouldn't happen but defensive), fall back to
    # SolveWithParameters() which uses PATH_CHEAPEST_ARC seeding.

    if cfg.mode == "geographic":
        sector_routes = _build_sector_routes(nodes, nv)
        hint = routing.ReadAssignmentFromRoutes(sector_routes, True)
        if hint is not None:
            log.debug(
                "[%s] Trip %d: geographic sector hint built (%d sectors)",
                fleet, trip_num, nv,
            )
            solution = routing.SolveFromAssignmentWithParameters(hint, params)
        else:
            log.warning(
                "[%s] Trip %d: sector hint returned None — falling back to "
                "PATH_CHEAPEST_ARC",
                fleet, trip_num,
            )
            solution = routing.SolveWithParameters(params)
    else:
        solution = routing.SolveWithParameters(params)

    # ── 12. Handle no-solution ──────────────────────────────────
    if solution is None:
        log.warning(
            "[%s] Trip %d: No solution (%d stores, %d vehicles, %ds budget)",
            fleet, trip_num, n_eligible, nv, cfg.solver_time_s,
        )
        return {
            "routes"  : [],
            "unserved": [
                {
                    "store" : nd["store"],
                    "reason": (
                        "Solver found no feasible solution. "
                        "Try increasing solver_time or adding vehicles."
                    ),
                    "node": nd,
                }
                for nd in nodes[1:]
            ],
            "nodes": nodes,
            "fleet": fleet,
        }

    # ── 13. Extract routes ──────────────────────────────────────
    nid_to_idx: Dict[str, int] = {
        nd["node_id"]: i for i, nd in enumerate(nodes)
    }
    raw_routes : List[Dict] = []
    served_ids : set        = set()

    for vi, veh in enumerate(vehicles):
        idx = routing.Start(vi)
        if routing.IsEnd(solution.Value(routing.NextVar(idx))):
            continue

        stops        : List[Dict] = []
        total_dist_m : float      = 0.0
        total_dur_s  : float      = 0.0
        load_kg      : float      = 0.0
        load_m3      : float      = 0.0
        last_ni                   = 0
        last_t                    = int(veh.get("start_offset", 0))

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
                    "is_next_day": bool(arr_wall >= 86400),
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

    # ── 14. Unserved diagnosis ──────────────────────────────────
    unserved = [
        {
            "store" : nd["store"],
            "reason": _diagnose(nd, vehicles, dist_mat, nid_to_idx, nodes, sched, cfg),
            "node"  : nd,
        }
        for nd in nodes[1:]
        if nd["node_id"] not in served_ids
    ]

    return {
        "routes"  : raw_routes,
        "unserved": unserved,
        "nodes"   : nodes,
        "fleet"   : fleet,
    }


# ════════════════════════════════════════════════════════════
#  Sequential multi-trip solver
# ════════════════════════════════════════════════════════════

def _solve_fleet_multitrip(
    fleet    : str,
    depot    : Dict,
    stores   : List[Dict],
    vehicles : List[Dict],
    dist_df,
    dur_df,
    cfg      : SolverConfig,
) -> Dict:
    """
    Runs up to cfg.max_trips sequential trip rounds.

    Pre-sort strategy per mode:
      geographic → sort by bearing (angle from depot).
                   Stores facing the same direction end up adjacent
                   in the list, giving SWEEP-like initial seeding to
                   PATH_CHEAPEST_ARC before the sector hint takes over.
      all others → sort by descending demand then ascending close-time.
                   Heavy, tight-window stores are seeded first.
    """
    sched   = config.FLEET_SCHEDULE[fleet]
    max_h_s = (sched["max_horizon_hour"] - sched["start_hour"]) * 3600

    fleet_key = "has_dry" if fleet == "DRY" else "has_cold"
    dem_field = "dry_kg"  if fleet == "DRY" else "cold_kg"

    eligible = [s for s in stores if s.get(fleet_key)]

    # ── Pre-sort: different strategy for geographic mode ────────
    dep_lat = float(depot["lat"])
    dep_lon = float(depot["lon"])

    if cfg.mode == "geographic":
        eligible.sort(
            key=lambda s: _bearing(dep_lat, dep_lon, float(s["lat"]), float(s["lon"]))
        )
        log.debug("[%s] geographic: stores sorted by bearing.", fleet)
    else:
        eligible.sort(
            key=lambda s: (-s.get(dem_field, 0.0), s.get("close_s", 86399))
        )

    remaining   = eligible
    all_routes  : List[Dict] = []
    truck_return: Dict[str, float] = {v["truck_id"]: 0.0 for v in vehicles}

    for trip_num in range(1, cfg.max_trips + 1):
        if not remaining:
            break

        available: List[Dict] = []
        for v in vehicles:
            offset = 0 if trip_num == 1 else int(
                truck_return[v["truck_id"]] + cfg.reload_time_s
            )
            if offset >= max_h_s:
                log.debug(
                    "[%s] Truck %s skipped trip %d: offset %.2fh > shift end %.2fh",
                    fleet, v["truck_id"], trip_num,
                    offset / 3600, max_h_s / 3600,
                )
                continue
            available.append({**v, "start_offset": offset})

        if not available:
            log.info("[%s] No trucks available for trip %d — stopping.", fleet, trip_num)
            break

        log.info(
            "[%s] Trip %d/%d: %d stores, %d/%d trucks, "
            "mode=%s, depart_hour=%02d:00, budget=%ds",
            fleet, trip_num, cfg.max_trips,
            len(remaining), len(available), len(vehicles),
            cfg.mode,
            _trip_depart_hour(
                fleet, min(int(v["start_offset"]) for v in available)
            ),
            cfg.solver_time_s,
        )

        res = _or_tools_solve(
            fleet, depot, remaining, available,
            dist_df, dur_df, cfg, trip_num,
        )
        all_routes.extend(res["routes"])

        for route in res["routes"]:
            truck_return[route["truck_id"]] = route["return_time_s"]

        served   = {s["node_id"] for r in res["routes"] for s in r["stops"]}
        prev_len = len(remaining)
        remaining = [s for s in remaining if s["node_id"] not in served]
        log.info(
            "[%s] Trip %d: %d served, %d remain (was %d)",
            fleet, trip_num, len(served), len(remaining), prev_len,
        )

    served_all = {s["node_id"] for r in all_routes for s in r["stops"]}
    unserved   = [
        {
            "store" : s,
            "reason": (
                f"Not served after {cfg.max_trips} trip(s). "
                "Increase Max Trips or add more vehicles."
            ),
            "node": None,
        }
        for s in eligible
        if s["node_id"] not in served_all
    ]

    return {
        "routes"  : all_routes,
        "unserved": unserved,
        "nodes"   : [],
        "fleet"   : fleet,
    }


# ════════════════════════════════════════════════════════════
#  Unserved diagnosis
# ════════════════════════════════════════════════════════════

def _diagnose(
    nd          : Dict,
    vehicles    : List[Dict],
    dist_mat    : np.ndarray,
    nid_to_idx  : Dict[str, int],
    nodes       : List[Dict],
    sched       : Dict,
    cfg         : SolverConfig,
) -> str:
    dkg    = nd["demand_kg"]
    dm3    = nd["demand_m3"]
    max_kg = max((v["cap_kg"] for v in vehicles), default=0)
    max_m3 = max((v["cap_m3"] for v in vehicles), default=0)

    if dkg > max_kg:
        return (
            f"Demand {dkg:.0f} kg exceeds largest vehicle "
            f"({max_kg:.0f} kg). Split into multiple orders."
        )
    if dm3 > max_m3:
        return (
            f"Demand {dm3:.2f} m³ exceeds largest vehicle "
            f"({max_m3:.2f} m³). Split into multiple orders."
        )

    ni      = nid_to_idx.get(nd["node_id"], 0)
    dist_km = float(dist_mat[0][ni]) / 1000.0 if ni else 0.0

    if dist_km > cfg.far_threshold_km:
        return (
            f"Very far from depot ({dist_km:.0f} km). "
            "Consider a dedicated run or removing this store."
        )

    tw_open  = nd.get("tw_open",  0)
    tw_close = nd.get("tw_close", 0)

    if tw_open >= tw_close:
        return (
            f"Invalid time window ({tw_open/3600:.1f}h–"
            f"{tw_close/3600:.1f}h). Check store opening hours."
        )

    if nd["travel_s"] > tw_close:
        return (
            f"Travel from depot ({nd['travel_s']/3600:.1f}h) exceeds "
            f"store close window ({tw_close/3600:.1f}h). "
            "Store cannot be reached within its opening hours."
        )

    total_cap_kg = sum(v["cap_kg"] for v in vehicles) * cfg.max_trips
    total_cap_m3 = sum(v["cap_m3"] for v in vehicles) * cfg.max_trips
    total_dem_kg = sum(n2["demand_kg"] for n2 in nodes[1:])
    total_dem_m3 = sum(n2["demand_m3"] for n2 in nodes[1:])

    if total_dem_kg > total_cap_kg * 0.95:
        return (
            f"Fleet weight capacity exhausted "
            f"({total_dem_kg:.0f} kg demand vs {total_cap_kg:.0f} kg fleet). "
            "Add more vehicles."
        )
    if total_dem_m3 > total_cap_m3 * 0.95:
        return (
            f"Fleet volume capacity exhausted "
            f"({total_dem_m3:.1f} m³ demand vs {total_cap_m3:.1f} m³ fleet). "
            "Add more vehicles."
        )

    return (
        f"Dropped by solver ({dist_km:.0f} km, "
        f"window {tw_open/3600:.1f}h–{tw_close/3600:.1f}h). "
        "Try increasing solver time (300 s+) or adding a vehicle."
    )


# ════════════════════════════════════════════════════════════
#  Public entry point
# ════════════════════════════════════════════════════════════

def solve(
    stores  : List[Dict],
    vehicles: List[Dict],
    dist_df,
    dur_df,
    cfg     : SolverConfig,
) -> Dict:
    """
    Solve CVRPTW for DRY and COLD fleets.

    HOW EACH MODE WORKS
    ═══════════════════
    shortest
        Arc cost = distance (decimetres) + anti-backtrack surcharge.
        No span balancing.  The solver finds the fewest total km.
        Anti-backtrack: arcs where next node is ≥30% closer to depot
        than current node cost 30% more — prevents yo-yo loops.

    fastest
        Arc cost = travel time + service time at FROM node.
        Time-window-aware seeding (PATH_MOST_CONSTRAINED_ARC) processes
        the most constrained stores first.  Span=50 balances driver hours.

    cheapest
        Arc cost = fuel ₮ per km (per vehicle, different rates possible).
        Each truck also pays a fixed cost (vehicle_cost + labor_cost)
        when used at all.  The solver trades off using fewer trucks
        (saving fixed cost) vs longer routes (spending more fuel).
        Anti-backtrack applied to fuel cost.

    balanced
        Arc cost = distance + anti-backtrack (same as shortest).
        Span coefficient = 300 (vs 10 default).  A high span coefficient
        forces OR-Tools to equalise route DURATIONS across all trucks
        before it cares about total distance.  Result: trucks finish
        at similar times; no driver gets 10 h while another does 2 h.

    geographic
        Arc cost = distance + angular penalty + anti-backtrack.
        Angular penalty: cost(i→j) += cost(i→j) × (angle/180)² × 0.6
        where angle is the direction-change from the depot's perspective.
        Stores in the same direction are cheap to chain; stores on
        opposite sides are expensive.
        Sector warm-start: stores are pre-sorted by bearing and
        divided into N sectors (one per vehicle).  OR-Tools receives
        this sector grouping as a starting assignment via
        ReadAssignmentFromRoutes(), then GLS refines within sectors.
        Result: compact pie-slice routes — each truck covers one
        geographic wedge of the city.

    Returns:
        {"DRY":  {routes, unserved, nodes, fleet},
         "COLD": {routes, unserved, nodes, fleet}}
    """
    dry_v  = [v for v in vehicles if v["fleet"] == "DRY"]
    cold_v = [v for v in vehicles if v["fleet"] == "COLD"]

    depot_dry  = {**config.DEPOTS["Dry DC"],  "name": "Dry DC"}
    depot_cold = {**config.DEPOTS["Cold DC"], "name": "Cold DC"}

    results: Dict = {}

    if dry_v:
        log.info(
            "[DRY] %d vehicles | departs %02d:00 | mode=%s | trips=%d | budget=%ds",
            len(dry_v),
            config.FLEET_SCHEDULE["DRY"]["start_hour"],
            cfg.mode, cfg.max_trips, cfg.solver_time_s,
        )
        results["DRY"] = _solve_fleet_multitrip(
            "DRY", depot_dry, stores, dry_v, dist_df, dur_df, cfg
        )
    else:
        results["DRY"] = {
            "routes": [], "nodes": [], "fleet": "DRY",
            "unserved": [
                {"store": s, "reason": "No DRY vehicles configured.", "node": None}
                for s in stores if s.get("has_dry")
            ],
        }

    if cold_v:
        log.info(
            "[COLD] %d vehicles | departs %02d:00 | mode=%s | trips=%d | budget=%ds",
            len(cold_v),
            config.FLEET_SCHEDULE["COLD"]["start_hour"],
            cfg.mode, cfg.max_trips, cfg.solver_time_s,
        )
        results["COLD"] = _solve_fleet_multitrip(
            "COLD", depot_cold, stores, cold_v, dist_df, dur_df, cfg
        )
    else:
        results["COLD"] = {
            "routes": [], "nodes": [], "fleet": "COLD",
            "unserved": [
                {"store": s, "reason": "No COLD vehicles configured.", "node": None}
                for s in stores if s.get("has_cold")
            ],
        }

    return results