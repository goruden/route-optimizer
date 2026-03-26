# ============================================================
#  solver.py  v7
#
#  Changes from v6:
#   1. Clustering REMOVED entirely — simpler, faster, no split-fleet
#      edge cases.  solve() calls _solve_fleet_multitrip directly.
#   2. SERVICE TIME BUG FIXED — v6 added service time to every arc
#      (including depot←→customer and depot return).  Now it is only
#      added when leaving a *customer* node, so time-window feasibility
#      is correct.
#   3. _diagnose node lookup FIXED — v6 used nodes.index(nd) (object
#      identity), which silently fell back to index 0 (the depot) on
#      miss.  Now uses a dict keyed by node_id.
#   4. Store pre-sort FIXED — now sorts only fleet-eligible stores
#      inside _solve_fleet_multitrip so the ordering is meaningful.
#   5. Dead rural fields REMOVED — is_rural / has_rural were always
#      False after v6 removed rural logic; now gone completely.
#   6. Vehicle start-time upper bound TIGHTENED — OR-Tools no longer
#      explores impossibly-late departures.
#
#  Architecture:
#    solve()
#      └─ _solve_fleet_multitrip()   (per fleet, all stores, all vehicles)
#           └─ _or_tools_solve()     Trip 1  (offset = 0)
#           └─ _or_tools_solve()     Trip 2  (offset = trip1_return + reload)
#           └─ ...  up to MAX_TRIPS_PER_VEHICLE
# ============================================================

import math
import logging
from collections import defaultdict
from typing import Dict, List, Optional, Tuple

import numpy as np
from ortools.constraint_solver import pywrapcp, routing_enums_pb2

import config

log = logging.getLogger(__name__)


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


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    return _haversine_m(lat1, lon1, lat2, lon2) / 1000.0


# ════════════════════════════════════════════════════════════
#  Time-dependent speed
# ════════════════════════════════════════════════════════════

def _speed_factor(hour: int) -> float:
    """
    Return the speed multiplier for a given hour of day (0–23).

    Applied as:  adjusted_travel_time = osrm_travel_time / speed_factor

    Values > 1.0 → roads faster than OSRM free-flow (e.g. 03:00 clear)
    Values < 1.0 → congestion (e.g. 13:00 rush hour)
    """
    return config.HOUR_SPEED_FACTOR.get(hour % 24, 1.0)


# ════════════════════════════════════════════════════════════
#  Matrix helpers
# ════════════════════════════════════════════════════════════

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
    Build NxN distance (metres) and duration (seconds) sub-matrices.

    Duration is speed-adjusted:
        adjusted_time = osrm_minutes × 60 / speed_factor(depart_hour)

    Haversine fallback at 60 km/h (speed-adjusted) when a node is
    absent from the OSRM matrix.
    """
    all_ids = [str(x) for x in dist_df.index]
    n       = len(nodes)
    dist    = np.zeros((n, n), dtype=np.float64)
    dur     = np.zeros((n, n), dtype=np.float64)
    factor  = _speed_factor(depart_hour)

    def _key(nd: Dict) -> Optional[str]:
        nid = depot_name if nd["is_depot"] else nd["node_id"]
        return _matrix_key(nid, all_ids)

    keys = [_key(nd) for nd in nodes]

    for i in range(n):
        for j in range(n):
            ki, kj = keys[i], keys[j]
            if ki and kj and ki in dist_df.index and kj in dist_df.columns:
                dist[i][j] = float(dist_df.at[ki, kj])
                dur[i][j]  = float(dur_df.at[ki, kj]) * 60.0   # min → s
            else:
                d_m = _haversine_m(
                    nodes[i]["lat"], nodes[i]["lon"],
                    nodes[j]["lat"], nodes[j]["lon"],
                )
                dist[i][j] = d_m
                dur[i][j]  = d_m / (60_000.0 / 3600.0)   # 60 km/h fallback

    if factor != 1.0:
        dur = dur / factor
        log.debug(
            "Speed factor %.3f applied for %02d:00 → travel times %s than base",
            factor, depart_hour, "longer" if factor < 1 else "shorter",
        )

    return dist, dur


def _depot_travel_times(
    dur_df,
    dist_df,
    depot_name  : str,
    stores      : List[Dict],
    fleet       : str,
    depart_hour : int,
) -> Tuple[np.ndarray, List[str]]:
    """
    One-way travel times (seconds, speed-adjusted) from depot to each
    fleet-eligible store.  Returns (array, node_id_list).
    """
    all_ids = [str(x) for x in dur_df.index]
    dk      = _matrix_key(depot_name, all_ids)
    factor  = _speed_factor(depart_hour)
    nids: List[str]   = []
    durs: List[float] = []

    for s in stores:
        if fleet == "DRY"  and not s["has_dry"]:  continue
        if fleet == "COLD" and not s["has_cold"]: continue

        sk = _matrix_key(s["node_id"], all_ids)
        nids.append(s["node_id"])

        if dk and sk and dk in dur_df.index and sk in dur_df.columns:
            durs.append(float(dur_df.at[dk, sk]) * 60.0 / factor)
        else:
            dep = config.DEPOTS[depot_name]
            d_m = _haversine_m(dep["lat"], dep["lon"], s["lat"], s["lon"])
            durs.append(d_m / (60_000.0 / 3600.0) / factor)

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
) -> List[Dict]:
    """
    Build node list.  Index 0 is always the depot.

    Time windows are shift-relative (seconds after fleet departure).
    All-day stores (00:00–23:59) receive the full planning horizon.

    If a store's close window is in the past relative to the shift
    start it is given the full horizon as a fallback (the solver may
    still skip it, but the node is not silently dropped before solving).
    """
    shift_s  = sched["start_hour"] * 3600
    max_h_s  = (sched["max_horizon_hour"] - sched["start_hour"]) * 3600

    id_to_travel = dict(zip(store_nids, travel_s))

    nodes: List[Dict] = [{
        "node_id"  : depot["name"],
        "lat"      : depot["lat"],
        "lon"      : depot["lon"],
        "tw_open"  : 0,
        "tw_close" : max_h_s,
        "demand_kg": 0.0,
        "demand_m3": 0.0,
        "is_depot" : True,
        "store"    : None,
        "travel_s" : 0.0,
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

        # Degenerate window → give full horizon so the solver can decide
        if tw_close <= 0 or tw_close <= tw_open:
            tw_open  = 0
            tw_close = max_h_s

        # NOTE: we intentionally do NOT extend tw_close when travel_s exceeds
        # it.  The solver will skip the store and it will appear in unserved
        # with a clear "time-window" diagnosis rather than silently receiving
        # an out-of-hours delivery.

        nodes.append({
            "node_id"  : s["node_id"],
            "lat"      : s["lat"],
            "lon"      : s["lon"],
            "tw_open"  : int(tw_open),
            "tw_close" : int(tw_close),
            "demand_kg": float(s["dry_kg"]  if fleet == "DRY" else s["cold_kg"]),
            "demand_m3": float(s["dry_cbm"] if fleet == "DRY" else s["cold_cbm"]),
            "is_depot" : False,
            "store"    : s,
            "travel_s" : t_s,
        })

    return nodes


# ════════════════════════════════════════════════════════════
#  Core OR-Tools solver  (single trip pass)
# ════════════════════════════════════════════════════════════

def _or_tools_solve(
    fleet        : str,
    depot        : Dict,
    stores       : List[Dict],
    vehicles     : List[Dict],
    dist_df,
    dur_df,
    mode         : str,
    solver_time_s: int,
    trip_num     : int = 1,
) -> Dict:
    """
    Solve a single-trip CVRPTW for the given stores and vehicle list.

    Vehicle dicts carry "start_offset" (seconds from shift start) that
    encodes when each truck becomes available.  Trip 1 → 0.
    Trip N+1 → trip-N return time + RELOAD_TIME_SECONDS.

    Returns:
        {routes, unserved, nodes, fleet}

    Each route carries "return_time_s" (shift-relative seconds when the
    truck arrives back at depot) so the caller can schedule the next trip.

    SERVICE-TIME NOTE
    ─────────────────
    Service time is added *only* when leaving a customer node, NOT the
    depot.  The time callback is:

        transit(i → j) = travel(i, j) + (SERVICE_TIME if i is customer else 0)

    This means the cumulative time at node j represents:

        arrival_at_j = departure_from_depot
                     + Σ [travel(k→k+1) + service(k)]  for all prior stops k

    Time-window constraints on node j are therefore checked against the
    moment the truck *arrives* (before serving j), which is the correct
    interpretation.  The truck then spends SERVICE_TIME at j before
    departing toward j+1.
    """
    sched       = config.FLEET_SCHEDULE[fleet]
    shift_s     = sched["start_hour"] * 3600
    max_h_s     = (sched["max_horizon_hour"] - sched["start_hour"]) * 3600
    depart_hour = sched["start_hour"]

    # ── 1. Travel times from depot ─────────────────────────────
    travel_s, store_nids = _depot_travel_times(
        dur_df, dist_df, depot["name"], stores, fleet, depart_hour
    )
    if not store_nids:
        return {"routes": [], "unserved": [], "nodes": [], "fleet": fleet}

    # ── 2. Node list ────────────────────────────────────────────
    nodes = _build_nodes(depot, stores, fleet, travel_s, store_nids, sched)
    n_eligible = len(nodes) - 1
    if n_eligible == 0:
        return {"routes": [], "unserved": [], "nodes": nodes, "fleet": fleet}

    n  = len(nodes)
    nv = len(vehicles)

    # ── 3. Distance + duration matrices ────────────────────────
    dist_mat, dur_mat = _build_submatrix(
        dist_df, dur_df, nodes, depot["name"], depart_hour
    )

    # OR-Tools requires integer costs.  Scale distance to decimetres to
    # avoid int32 overflow on long routes (hundreds of km).
    dist_dm = (dist_mat / 10.0).astype(np.int64)
    dur_s   = dur_mat.astype(np.int64)   # pure travel, seconds, no service time

    # Build a lookup so time-callback can detect depot without per-call branching
    is_depot_mask = [nd["is_depot"] for nd in nodes]

    # ── 4. Routing model ────────────────────────────────────────
    manager = pywrapcp.RoutingIndexManager(n, nv, [0] * nv, [0] * nv)
    routing = pywrapcp.RoutingModel(manager)

    # ── 5. Arc-cost callbacks ───────────────────────────────────
    def _dist_cb(fi, ti):
        return int(dist_dm[manager.IndexToNode(fi)][manager.IndexToNode(ti)])

    def _time_cb(fi, ti):
        """
        Travel time + service time at the *from* node.
        Service time is 0 for the depot (node 0).
        """
        ni = manager.IndexToNode(fi)
        nj = manager.IndexToNode(ti)
        svc = 0 if is_depot_mask[ni] else config.SERVICE_TIME_SECONDS
        return int(dur_s[ni][nj] + svc)

    dist_cb_idx = routing.RegisterTransitCallback(_dist_cb)
    time_cb_idx = routing.RegisterTransitCallback(_time_cb)

    if mode == "fastest":
        routing.SetArcCostEvaluatorOfAllVehicles(time_cb_idx)
    elif mode == "cheapest":
        for vi, veh in enumerate(vehicles):
            fpm = veh["fuel_cost_km"] / 10_000.0   # ₮ per decimetre
            def _make_fuel(f):
                def cb(fi, ti):
                    return int(dist_dm[manager.IndexToNode(fi)]
                                      [manager.IndexToNode(ti)] * f)
                return cb
            routing.SetArcCostEvaluatorOfVehicle(
                routing.RegisterTransitCallback(_make_fuel(fpm)), vi
            )
    else:   # "shortest"
        routing.SetArcCostEvaluatorOfAllVehicles(dist_cb_idx)

    routing.SetFixedCostOfAllVehicles(config.VEHICLE_FIXED_COST)

    # ── 6. Weight capacity ──────────────────────────────────────
    def _kg_cb(idx):
        return int(nodes[manager.IndexToNode(idx)]["demand_kg"])

    kg_cb = routing.RegisterUnaryTransitCallback(_kg_cb)
    routing.AddDimensionWithVehicleCapacity(
        kg_cb, 0,
        [int(v["cap_kg"] * config.MAX_WEIGHT_FILL_PERCENTAGE) for v in vehicles],
        True, "CapKg"
    )

    # ── 7. Volume capacity ──────────────────────────────────────
    def _m3_cb(idx):
        return int(nodes[manager.IndexToNode(idx)]["demand_m3"] * config.M3_SCALE)

    m3_cb = routing.RegisterUnaryTransitCallback(_m3_cb)
    routing.AddDimensionWithVehicleCapacity(
        m3_cb, 0,
        [int(v["cap_m3"] * config.M3_SCALE * config.MAX_VOLUME_FILL_PERCENTAGE) for v in vehicles],
        True, "CapM3"
    )

    # ── 8. Time-window dimension ────────────────────────────────
    routing.AddDimension(
        time_cb_idx,
        7_200,    # max waiting slack (2 h) — lets trucks arrive early
        max_h_s,
        False,    # don't force zero start (vehicles start at their offset)
        "Time"
    )
    time_dim = routing.GetDimensionOrDie("Time")

    # Customer time windows
    for i, nd in enumerate(nodes):
        if nd["is_depot"]:
            continue
        ri = manager.NodeToIndex(i)
        time_dim.CumulVar(ri).SetRange(nd["tw_open"], nd["tw_close"])

    # Vehicle windows
    # start: [start_offset, start_offset]  — truck departs exactly when ready
    #        (OR-Tools will respect the 2 h slack to wait at first stop)
    # end:   [start_offset, max_h_s]       — must finish within the shift
    for vi, veh in enumerate(vehicles):
        start_off = int(veh.get("start_offset", 0))
        time_dim.CumulVar(routing.Start(vi)).SetRange(start_off, start_off)
        time_dim.CumulVar(routing.End(vi)).SetRange(start_off, max_h_s)

    # Penalise unequal route lengths → tighter, more balanced loops
    time_dim.SetGlobalSpanCostCoefficient(10)

    # ── 9. Disjunctions (allow dropping with heavy penalty) ─────
    for i in range(1, n):
        routing.AddDisjunction(
            [manager.NodeToIndex(i)], config.PENALTY_UNSERVED
        )

    # ── 10. Search parameters ───────────────────────────────────
    params = pywrapcp.DefaultRoutingSearchParameters()
    params.first_solution_strategy = (
        routing_enums_pb2.FirstSolutionStrategy.PATH_CHEAPEST_ARC
    )
    params.local_search_metaheuristic = (
        routing_enums_pb2.LocalSearchMetaheuristic.GUIDED_LOCAL_SEARCH
    )
    params.time_limit.seconds = solver_time_s
    params.log_search          = False

    solution = routing.SolveWithParameters(params)

    if solution is None:
        log.warning(
            "[%s] Trip %d: No solution (%d stores, %d vehicles, %ds budget)",
            fleet, trip_num, n_eligible, nv, solver_time_s,
        )
        return {
            "routes"  : [],
            "unserved": [
                {
                    "store": nd["store"],
                    "reason": (
                        "Solver found no feasible solution. "
                        "Try increasing solver_time or adding vehicles."
                    ),
                    "node": nd,
                }
                for nd in nodes[1:]
            ],
            "nodes" : nodes,
            "fleet" : fleet,
        }

    # ── 11. Extract routes ──────────────────────────────────────
    # Build a node_id → index map for O(1) lookups used in diagnosis
    nid_to_idx: Dict[str, int] = {
        nd["node_id"]: i for i, nd in enumerate(nodes)
    }

    raw_routes: List[Dict] = []
    served_ids: set        = set()

    for vi, veh in enumerate(vehicles):
        idx = routing.Start(vi)
        if routing.IsEnd(solution.Value(routing.NextVar(idx))):
            continue   # vehicle unused this trip

        stops:        List[Dict] = []
        total_dist_m: float      = 0.0
        total_dur_s:  float      = 0.0
        load_kg:      float      = 0.0
        load_m3:      float      = 0.0

        # Tracks where the truck is when it finally heads back to depot
        last_ni = 0
        last_t  = int(veh.get("start_offset", 0))

        while not routing.IsEnd(idx):
            ni = manager.IndexToNode(idx)
            nd = nodes[ni]

            if not nd["is_depot"]:
                served_ids.add(nd["node_id"])
                t_solver = solution.Value(time_dim.CumulVar(idx))
                arr_wall = t_solver + shift_s   # absolute wall-clock seconds

                stops.append({
                    "node_id"   : nd["node_id"],
                    "store"     : nd["store"],
                    "arrival_s" : float(arr_wall),
                    "depart_s"  : float(arr_wall + config.SERVICE_TIME_SECONDS),
                    "demand_kg" : float(nd["demand_kg"]),
                    "demand_m3" : float(nd["demand_m3"]),
                    "lat"       : float(nd["lat"]),
                    "lon"       : float(nd["lon"]),
                    "is_next_day": bool(arr_wall >= 86400),
                })

                load_kg += nd["demand_kg"]
                load_m3 += nd["demand_m3"]
                last_t   = t_solver + config.SERVICE_TIME_SECONDS
                last_ni  = ni

            nxt = solution.Value(routing.NextVar(idx))
            if not routing.IsEnd(nxt):
                ni2 = manager.IndexToNode(nxt)
                total_dist_m += dist_mat[ni][ni2]
                total_dur_s  += dur_mat[ni][ni2]
            idx = nxt

        if not stops:
            continue

        # Return leg (last customer → depot)
        return_leg_s  = float(dur_mat[last_ni][0])
        return_dist_m = float(dist_mat[last_ni][0])
        total_dist_m += return_dist_m
        total_dur_s  += return_leg_s

        # shift-relative arrival back at depot
        return_time_s = last_t + return_leg_s

        raw_routes.append({
            "truck_id"       : veh["truck_id"],
            "trip_number"    : trip_num,
            "virtual_id"     : f"{veh['truck_id']}_T{trip_num}",
            "vehicle"        : veh,
            "stops"          : stops,
            "total_dist_m"   : float(total_dist_m),
            "total_dur_s"    : float(total_dur_s),
            "load_kg"        : float(load_kg),
            "load_m3"        : float(load_m3),
            "cap_kg"         : float(veh["cap_kg"]),
            "cap_m3"         : float(veh["cap_m3"]),
            "return_time_s"  : float(return_time_s),
            "start_offset_s" : float(veh.get("start_offset", 0)),
        })

    # ── 12. Unserved diagnosis ──────────────────────────────────
    unserved = [
        {
            "store": nd["store"],
            "reason": _diagnose(nd, vehicles, dist_mat, nid_to_idx, nodes, sched),
            "node":  nd,
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
    fleet        : str,
    depot        : Dict,
    stores       : List[Dict],
    vehicles     : List[Dict],
    dist_df,
    dur_df,
    mode         : str,
    solver_time_s: int,
) -> Dict:
    """
    Sequential multi-trip: solve trip 1, then trip 2 with remaining
    stores, and so on up to MAX_TRIPS_PER_VEHICLE.

    WHY SEQUENTIAL?
    ───────────────
    Virtual-vehicle expansion (one copy per trip slot) lets OR-Tools
    schedule T1 and T2 concurrently — physically impossible for a
    single driver.  Sequential solving fixes this:

      Round 1 — all trucks available at offset 0.
      Round N — each truck's offset = previous return time
                + RELOAD_TIME_SECONDS (park + reload).

    This guarantees no two trips from the same truck overlap and that
    the gap between consecutive trips is at least RELOAD_TIME_SECONDS.

    Store pre-sorting
    ─────────────────
    Fleet-eligible stores are sorted before Trip 1 by descending demand
    then ascending close-time.  The solver sees heavy, tight-window stores
    first in PATH_CHEAPEST_ARC seeding, producing better initial solutions.
    """
    sched   = config.FLEET_SCHEDULE[fleet]
    max_h_s = (sched["max_horizon_hour"] - sched["start_hour"]) * 3600

    # Filter to fleet-eligible stores BEFORE sorting so the sort key
    # (dem_field) is always valid for the chosen fleet.
    fleet_key = "has_dry" if fleet == "DRY" else "has_cold"
    dem_field = "dry_kg"  if fleet == "DRY" else "cold_kg"

    eligible_stores = [s for s in stores if s.get(fleet_key)]
    eligible_stores.sort(
        key=lambda s: (-s.get(dem_field, 0.0), s.get("close_s", 86399))
    )

    # Pass the full (unfiltered) store list to OR-Tools so _build_nodes can
    # apply its own fleet filter — but start from the sorted eligible subset.
    # This keeps trip-residual ordering consistent across multiple trips.
    remaining = eligible_stores

    all_routes: List[Dict] = []

    # Shift-relative time at which each truck returns from its last trip.
    # Initialised to 0 (available from shift start).
    truck_return: Dict[str, float] = {v["truck_id"]: 0.0 for v in vehicles}

    for trip_num in range(1, config.MAX_TRIPS_PER_VEHICLE + 1):
        if not remaining:
            break

        # ── Build vehicle list with correct offset for this round ──
        available_vehicles: List[Dict] = []
        for v in vehicles:
            if trip_num == 1:
                offset = 0
            else:
                offset = int(truck_return[v["truck_id"]] + config.RELOAD_TIME_SECONDS)

            if offset >= max_h_s:
                log.debug(
                    "[%s] Truck %s skipped trip %d: available at %.2fh > shift end %.2fh",
                    fleet, v["truck_id"], trip_num,
                    offset / 3600, max_h_s / 3600,
                )
                continue

            available_vehicles.append({**v, "start_offset": offset})

        if not available_vehicles:
            log.info("[%s] No trucks available for trip %d — stopping.", fleet, trip_num)
            break

        log.info(
            "[%s] Trip %d/%d: %d stores, %d/%d trucks, %ds budget",
            fleet, trip_num, config.MAX_TRIPS_PER_VEHICLE,
            len(remaining), len(available_vehicles), len(vehicles), solver_time_s,
        )
        if trip_num > 1:
            offsets_str = ", ".join(
                f"{v['truck_id']}@{v['start_offset']/3600:.2f}h"
                for v in available_vehicles
            )
            log.info("[%s]   Truck availability: %s", fleet, offsets_str)

        res = _or_tools_solve(
            fleet, depot, remaining, available_vehicles,
            dist_df, dur_df, mode, solver_time_s, trip_num,
        )

        all_routes.extend(res["routes"])

        # Record each truck's return time for the next round
        for route in res["routes"]:
            tid = route["truck_id"]
            truck_return[tid] = route["return_time_s"]
            log.debug(
                "[%s] Truck %s trip %d: returns shift+%.2fh (wall %.2fh)",
                fleet, tid, trip_num,
                route["return_time_s"] / 3600,
                route["return_time_s"] / 3600 + sched["start_hour"],
            )

        # Remove served stores from the pool
        served = {
            stop["node_id"]
            for route in res["routes"]
            for stop in route["stops"]
        }
        prev_len  = len(remaining)
        remaining = [s for s in remaining if s["node_id"] not in served]

        log.info(
            "[%s] Trip %d: %d served, %d remain (was %d)",
            fleet, trip_num, len(served), len(remaining), prev_len,
        )

    # Any store still in the eligible list and not covered by any trip
    served_all = {
        stop["node_id"]
        for route in all_routes
        for stop in route["stops"]
    }
    unserved = [
        {
            "store": s,
            "reason": (
                f"Not served after {config.MAX_TRIPS_PER_VEHICLE} trip(s). "
                "Increase Max Trips or add more vehicles."
            ),
            "node": None,
        }
        for s in eligible_stores
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
) -> str:
    """
    Return a human-readable reason why a store was not served.

    Uses nid_to_idx for O(1) node lookup — replaces the fragile
    nodes.index(nd) call in v6 which silently fell back to 0 (depot)
    when the node wasn't found by object identity.
    """
    dkg    = nd["demand_kg"]
    dm3    = nd["demand_m3"]
    max_kg = max((v["cap_kg"] for v in vehicles), default=0)
    max_m3 = max((v["cap_m3"] for v in vehicles), default=0)

    if dkg > max_kg:
        return (
            f"Demand {dkg:.0f} kg exceeds the largest vehicle "
            f"({max_kg:.0f} kg). Split into multiple orders."
        )
    if dm3 > max_m3:
        return (
            f"Demand {dm3:.2f} m³ exceeds the largest vehicle "
            f"({max_m3:.2f} m³). Split into multiple orders."
        )

    ni       = nid_to_idx.get(nd["node_id"], 0)
    dist_km  = float(dist_mat[0][ni]) / 1000.0 if ni else 0.0
    tw_open  = nd.get("tw_open",  0)
    tw_close = nd.get("tw_close", 0)

    if dist_km > config.FAR_THRESHOLD_KM:
        return (
            f"Very far from depot ({dist_km:.0f} km). "
            "Consider a dedicated run or removing this store."
        )

    if tw_open >= tw_close:
        return (
            f"Invalid time window ({tw_open/3600:.1f}h – "
            f"{tw_close/3600:.1f}h). Check store opening hours."
        )

    # Check if travel alone makes arrival impossible within the window
    if nd["travel_s"] > tw_close:
        return (
            f"Travel from depot ({nd['travel_s']/3600:.1f}h) exceeds "
            f"store close window ({tw_close/3600:.1f}h). "
            "Store cannot be reached within its opening hours."
        )

    total_cap_kg = sum(v["cap_kg"] for v in vehicles) * config.MAX_TRIPS_PER_VEHICLE
    total_cap_m3 = sum(v["cap_m3"] for v in vehicles) * config.MAX_TRIPS_PER_VEHICLE
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
    mode    : str = "cheapest",
) -> Dict:
    """
    Solve CVRPTW for DRY and COLD fleets.

    Key behaviours (v7):
    ─────────────────────────────────────────────────────────
    1. No clustering — all fleet-eligible stores solved together,
       giving OR-Tools maximum freedom to form efficient routes.
    2. No rural/urban split — all stores treated equally.
    3. Multi-trip is sequential: trip N+1 for truck X starts only
       after trip N for truck X returns + RELOAD_TIME_SECONDS.
    4. Travel times are speed-adjusted per fleet departure hour:
       DRY (13:00 rush) gets slower times than COLD (03:00 clear).
    5. Service time is correctly applied only at customer nodes.

    Args:
        stores:   list of store dicts from data_loader
        vehicles: list of vehicle dicts from data_loader
        dist_df:  NxN distance DataFrame (metres)
        dur_df:   NxN duration DataFrame  (minutes)
        mode:     "cheapest" | "fastest" | "shortest"

    Returns:
        {"DRY": {routes, unserved, nodes, fleet},
         "COLD": {routes, unserved, nodes, fleet}}
    """
    dry_v  = [v for v in vehicles if v["fleet"] == "DRY"]
    cold_v = [v for v in vehicles if v["fleet"] == "COLD"]

    depot_dry  = {**config.DEPOTS["Dry DC"],  "name": "Dry DC"}
    depot_cold = {**config.DEPOTS["Cold DC"], "name": "Cold DC"}

    t       = config.MAX_SOLVER_TIME_SECONDS
    results: Dict = {}

    # ── DRY ──────────────────────────────────────────────────────
    if dry_v:
        log.info(
            "[DRY] %d vehicles | departs %02d:00 | speed factor %.2f",
            len(dry_v),
            config.FLEET_SCHEDULE["DRY"]["start_hour"],
            _speed_factor(config.FLEET_SCHEDULE["DRY"]["start_hour"]),
        )
        results["DRY"] = _solve_fleet_multitrip(
            "DRY", depot_dry, stores, dry_v, dist_df, dur_df, mode, t
        )
    else:
        results["DRY"] = {
            "routes": [], "nodes": [], "fleet": "DRY",
            "unserved": [
                {"store": s, "reason": "No DRY vehicles configured.", "node": None}
                for s in stores if s.get("has_dry")
            ],
        }

    # ── COLD ─────────────────────────────────────────────────────
    if cold_v:
        log.info(
            "[COLD] %d vehicles | departs %02d:00 | speed factor %.2f",
            len(cold_v),
            config.FLEET_SCHEDULE["COLD"]["start_hour"],
            _speed_factor(config.FLEET_SCHEDULE["COLD"]["start_hour"]),
        )
        results["COLD"] = _solve_fleet_multitrip(
            "COLD", depot_cold, stores, cold_v, dist_df, dur_df, mode, t
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