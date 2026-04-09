# ============================================================
#  solver.py  v9.2
#
#  KEY CHANGES FROM v9.1:
#
#  1. Outbound-sweep enforcement (two-tier backtrack penalty).
#     v9.1's 1.3× surcharge was too weak — OR-Tools still found it
#     cheaper to serve nearby stores on the return leg after far ones.
#     Two-tier now:
#       mild backtrack  (ratio < backtrack_threshold 0.70): 1.3×
#       severe backtrack (ratio < outbound_threshold  0.40): outbound_factor 6×
#     Applied to all arc-cost callbacks (antibt, geo, fuel).
#     Result: routes sweep outward and serve stores on the way out,
#     returning roughly empty. Tune outbound_factor in SolverConfig.
#
#  2. Store business hours enforced for far/next-day stores.
#     v9.1 widened far-store windows to [0, MAX_ROUTE_TIME] which
#     silently removed their actual opening-hour constraint — a truck
#     could legally arrive at 03:00 at a store that opens at 09:00.
#     Fix: if travel_s > tw_close (can't reach today), the window is
#     shifted forward by 86 400 s (one calendar day) so OR-Tools sees
#     the NEXT-DAY opening hours. The is_next_day flag in each stop
#     tells the dispatcher to call ahead.
#     max_wait_slack_s raised to 18 h so vehicles can legally wait
#     overnight outside a store without being blocked by the dimension
#     feasibility check.
#
#  KEY CHANGES FROM v9.0:
#
#  1. Removed shift-horizon caps that silently dropped far stores.
#     MAX_ROUTE_TIME is now a flat 48 h (172 800 s) — OR-Tools can
#     build routes that run past midnight. Stops that arrive after
#     00:00 are flagged is_next_day=True in the output (was already
#     wired; just never reachable before).
#
#  2. _build_nodes: tw_close no longer clamped to max_h_s.
#     All-day stores and far stores get tw_close = MAX_ROUTE_TIME
#     so the solver can schedule them at any hour it needs to.
#     Stores with explicit wall-clock windows keep those windows
#     (a store that closes at 18:00 should not receive at 02:00).
#
#  3. _build_nodes far-store widening now uses MAX_ROUTE_TIME
#     instead of max_h_s — actually gives the solver room to work.
#
#  4. _or_tools_solve: vehicle end CumulVar range extended to
#     MAX_ROUTE_TIME (was max_h_s). Soft overtime penalty kept so
#     the solver still prefers finishing early but won't drop a
#     store just because it pushes the route past shift-end.
#
#  5. _solve_fleet_multitrip: truck-skip guard raised from
#     max_h_s * 1.5 → MAX_ROUTE_TIME so trucks are never skipped
#     just because a previous trip ran long.
#
#  6. _diagnose check 2 (round-trip impossible): threshold raised
#     from max_h_s * 1.5 → MAX_ROUTE_TIME. Only fires if the store
#     is genuinely impossible even within 48 h.
#
#  HOW EACH MODE WORKS → see docstring on solve() at the bottom.
# ============================================================

import math
import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

import numpy as np
from ortools.constraint_solver import pywrapcp, routing_enums_pb2

import config

log = logging.getLogger(__name__)

# Flat 48-hour ceiling — lets routes run past midnight / into next day.
# is_next_day flag in each stop handles downstream display.
MAX_ROUTE_TIME = 48 * 3600  # 172 800 s


# ════════════════════════════════════════════════════════════
#  SolverConfig — all defaults pulled from config.py
# ════════════════════════════════════════════════════════════

@dataclass
class SolverConfig:
    """
    All solve-time parameters in one object.
    Created fresh per optimise request — no module-level globals mutated.
    Two concurrent jobs each have their own SolverConfig and are safe.

    Every default is read from config.py so operators have a single
    place to tune the system.
    """
    mode                  : str   = "cheapest"
    max_trips             : int   = config.MAX_TRIPS_PER_VEHICLE
    solver_time_s         : int   = config.MAX_SOLVER_TIME_SECONDS
    max_weight_fill       : float = config.MAX_WEIGHT_FILL_PERCENTAGE
    max_volume_fill       : float = config.MAX_VOLUME_FILL_PERCENTAGE

    reload_time_s         : int   = config.RELOAD_TIME_SECONDS
    service_time_base_s   : int   = config.SERVICE_TIME_SECONDS
    service_time_per_kg_s : float = 0.0

    penalty_unserved      : int   = config.PENALTY_UNSERVED      # 10_000_000_000
    vehicle_fixed_cost    : int   = config.VEHICLE_FIXED_COST    # 5_000
    m3_scale              : int   = config.M3_SCALE              # 1_000
    far_threshold_km      : float = config.FAR_THRESHOLD_KM      # 1_000

    # Raised from 7 200 s (2 h) to 18 h so vehicles can wait overnight
    # outside a store with a next-day window without hitting OR-Tools'
    # dimension feasibility check.  The time window at each node still
    # pins the actual arrival to business hours.
    max_wait_slack_s      : int   = 18 * 3_600   # 64 800 s

    # Anti-backtrack — two-tier penalty for going back toward the depot.
    #
    # Tier 1 — mild backtrack (ratio < backtrack_threshold):
    #   cost x backtrack_factor  (1.30x)
    #   Catches soft U-turns where the next stop is noticeably closer.
    #
    # Tier 2 — outbound sweep (ratio < outbound_threshold):
    #   cost x outbound_factor   (6.0x default)
    #   Catches return-leg deliveries where the truck has already turned
    #   around and is heading back to the depot while still delivering.
    #   Raise outbound_factor (e.g. 10x) to enforce more strictly;
    #   lower it if too many stores are dropped.
    backtrack_threshold   : float = 0.70   # ratio below which tier-1 fires
    backtrack_factor      : float = 1.30   # tier-1 multiplier
    outbound_threshold    : float = 0.40   # ratio below which tier-2 fires
    outbound_factor       : float = 6.0    # tier-2 multiplier

    # Geographic mode — angular arc penalty weight.
    # cost(i→j) += cost(i→j) x (angle/180)^2 x geo_angular_w
    geo_angular_w         : float = 0.60


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
    """True-north compass bearing (0–360°) from point-1 to point-2."""
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

def _estimate_speed(n1: Dict, n2: Dict) -> float:
    """Distance-heuristic base speed (km/h) between two nodes."""
    d = _haversine_m(n1["lat"], n1["lon"], n2["lat"], n2["lon"])
    if d < 1_000:   return 20.0
    if d < 5_000:   return 30.0
    if d < 20_000:  return 45.0
    return 70.0


def _speed_factor(hour: int) -> float:
    return config.HOUR_SPEED_FACTOR.get(int(hour) % 24, 1.0)


def _trip_depart_hour(fleet: str, offset_s: float) -> int:
    """
    Wall-clock departure hour for a specific trip.
    offset_s = shift-relative seconds when this vehicle departs.
    """
    start_h = config.FLEET_SCHEDULE[fleet]["start_hour"]
    return int(start_h + offset_s / 3600) % 24


# ════════════════════════════════════════════════════════════
#  Matrix helpers
# ════════════════════════════════════════════════════════════

def _normalise_id(k) -> str:
    """
    Normalise node ID to canonical string:
    "00198", 198, "198"  →  "198".
    Non-numeric depot names returned as-is.
    """
    try:
        return str(int(k))
    except (ValueError, TypeError):
        return str(k)


def _build_submatrix(
    dist_df,
    dur_df,
    nodes       : List[Dict],
    depot_name  : str,
    depart_hour : int,
) -> Tuple[np.ndarray, np.ndarray]:
    """
    NxN distance (metres) and speed-adjusted duration (seconds).

    DataFrames are re-indexed as strings so integer node IDs in the
    DataFrame match string node_id values in store dicts.
    """
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

    def _key(nd: Dict) -> Optional[str]:
        nid = depot_name if nd["is_depot"] else _normalise_id(nd["node_id"])
        return nid if nid in all_ids else None

    keys = [_key(nd) for nd in nodes]

    for i in range(n):
        for j in range(n):
            ki, kj = keys[i], keys[j]
            if ki and kj and ki in dist_s.index and kj in dist_s.columns:
                d = float(dist_s.at[ki, kj])
                speed_kmh = _estimate_speed(nodes[i], nodes[j])
                t = d / (speed_kmh * 1000 / 3600)

                if t == 0.0 and i != j:
                    log.debug(
                        "Zero duration in matrix %s→%s (dist %.1f m); "
                        "using haversine fallback.", ki, kj, d,
                    )
                    hav = _haversine_m(
                        nodes[i]["lat"], nodes[i]["lon"],
                        nodes[j]["lat"], nodes[j]["lon"],
                    )
                    d = max(d, hav)
                    t = d / (40_000.0 / 3600.0)

                if d < 50:
                    t += 20  # minimum 20 s for very short hops

                dist[i][j] = d
                dur[i][j]  = t
            else:
                d_m = _haversine_m(
                    nodes[i]["lat"], nodes[i]["lon"],
                    nodes[j]["lat"], nodes[j]["lon"],
                )
                dist[i][j] = d_m
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
    norm_index = [_normalise_id(x) for x in dur_df.index]
    dur_s_df   = dur_df.copy()
    dur_s_df.index   = norm_index
    dur_s_df.columns = norm_index
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
            # dur_df values are in minutes → convert to seconds
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

    v9.1 — shift-horizon cap removed entirely.
    Time windows are based on store wall-clock hours only.
    Stores with no specific window (all-day) get MAX_ROUTE_TIME (48 h)
    so the solver can reach them at whatever hour the route requires.
    Arrivals past midnight are flagged is_next_day=True in the output.

    Why not cap at max_h_s?
      A store 8 h from the depot cannot be served within a 12 h shift
      if you cap tw_close at shift-end — it becomes instantly infeasible
      before OR-Tools even starts. Removing the cap lets the solver
      schedule it late in the day (or overnight) and the is_next_day
      flag tells the dispatcher to call ahead.
    """
    shift_s = sched["start_hour"] * 3600

    id_to_travel = dict(zip(store_nids, travel_s))
    dep_lat = float(depot["lat"])
    dep_lon = float(depot["lon"])

    nodes: List[Dict] = [{
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
    }]

    for s in stores:
        if fleet == "DRY"  and not s["has_dry"]:  continue
        if fleet == "COLD" and not s["has_cold"]: continue

        t_s        = float(id_to_travel.get(s["node_id"], 0.0))
        wall_open  = int(s["open_s"])
        wall_close = int(s["close_s"])
        is_all_day = (wall_open == 0 and wall_close >= 86_398)

        if is_all_day:
            # No specific hours — solver can arrive any time within 48 h.
            tw_open  = 0
            tw_close = MAX_ROUTE_TIME
        else:
            tw_open  = max(0, wall_open  - shift_s)
            tw_close = wall_close - shift_s  # ← no max_h_s cap

            # If the store's closing time is before the shift starts
            # (e.g. a 00:00–06:00 store and shift starts at 07:00),
            # treat it as all-day so the solver can still attempt it.
            if tw_close <= 0 or tw_close <= tw_open:
                log.debug(
                    "[%s] Store %s: wall window %d–%d falls before shift "
                    "start %02d:00 — widening to full 48h window.",
                    fleet, s["node_id"],
                    wall_open // 3600, wall_close // 3600,
                    sched["start_hour"],
                )
                tw_open  = 0
                tw_close = MAX_ROUTE_TIME

        # ── Next-day window for far stores ──────────────────────
        # If one-way travel exceeds today's closing time the store is
        # unreachable within its own hours TODAY.  Instead of wiping the
        # hours constraint (which would let a truck arrive at 03:00), shift
        # the window forward by one calendar day so OR-Tools enforces the
        # store's real opening hours on the next day.
        #
        # Example: store opens 09:00–19:00, shift starts 06:00, travel 14 h.
        #   Today  : tw_open=10800 (9am), tw_close=46800 (7pm)  — truck
        #            arrives at 14h shift-relative = 20:00 → after close.
        #   Next day: tw_open=97200 (9am+24h), tw_close=133200 (7pm+24h)
        #            — truck arrives at 14h, waits 7h, delivers at 09:00. ✓
        #   is_next_day flag is set on the stop so the dispatcher can call ahead.
        #
        # For all-day stores there are no hours to preserve; keep MAX_ROUTE_TIME.
        if not is_all_day and t_s > tw_close:
            tw_open  += 86_400
            tw_close += 86_400
            # Clamp to 48-h ceiling; if still unreachable the solver drops it
            # and _diagnose fires check-2 (physically impossible).
            tw_open  = min(tw_open,  MAX_ROUTE_TIME)
            tw_close = min(tw_close, MAX_ROUTE_TIME)
            log.debug(
                "[%s] Store %s: travel %.1fh > today's close %.1fh — "
                "shifted to next-day window [%.1fh, %.1fh].",
                fleet, s["node_id"],
                t_s / 3600, (tw_close - 86_400) / 3600,
                tw_open / 3600, tw_close / 3600,
            )
        # ── end next-day fix ────────────────────────────────────

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

def _build_sector_routes(nodes: List[Dict], n_vehicles: int) -> List[List[int]]:
    """
    Divide customer nodes into n_vehicles angular sectors and return
    an initial route per vehicle as a list of node indices.

    Equal-count sectors (not equal-angle) handle dense city centres
    where a 45° slice toward downtown may have 20 stores while a 45°
    slice toward suburbs has 3.
    """
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


# ════════════════════════════════════════════════════════════
#  Arc-cost callbacks
# ════════════════════════════════════════════════════════════

def _make_time_cb(manager, dur_s: np.ndarray, svc_times: np.ndarray):
    """Travel time + service time at the FROM node."""
    def cb(fi, ti):
        ni = manager.IndexToNode(fi)
        nj = manager.IndexToNode(ti)
        turn_penalty = 8 if ni != 0 else 0
        return int(dur_s[ni][nj] + svc_times[ni] + turn_penalty)
    return cb


def _make_antibt_dist_cb(
    manager,
    dist_dm          : np.ndarray,
    dist_depot       : np.ndarray,
    is_depot_mask    : List[bool],
    threshold        : float,
    factor           : float,
    out_threshold    : float,
    out_factor       : float,
):
    """
    Distance (decimetres) with two-tier outbound-sweep penalty.

    Tier 1 (mild backtrack):  ratio < threshold  → cost × factor   (1.30×)
    Tier 2 (return-leg deliv): ratio < out_threshold → cost × out_factor (6×)

    Tier 2 is the outbound-sweep enforcement: once a truck has visited
    far stores it becomes very expensive for it to loop back and deliver
    to nearby stores, so routes naturally serve stores on the way OUT.
    """
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
            if ratio < out_threshold:          # Tier 2 — return-leg delivery
                return int(base * out_factor)
            if ratio < threshold:              # Tier 1 — mild backtrack
                return int(base * factor)
        return base
    return cb


def _make_geo_cb(
    manager,
    dist_dm          : np.ndarray,
    dist_depot       : np.ndarray,
    is_depot_mask    : List[bool],
    bearings         : List[float],
    angular_w        : float,
    bt_threshold     : float,
    bt_factor        : float,
    out_threshold    : float,
    out_factor       : float,
):
    """
    Geographic arc cost with two-tier outbound-sweep penalty.

    cost(i→j) = base + base × (angle/180)² × angular_w
    Then Tier-1 or Tier-2 backtrack multiplier applied on top.
    """
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
            if ratio < out_threshold:          # Tier 2
                return int(cost * out_factor)
            if ratio < bt_threshold:           # Tier 1
                return int(cost * bt_factor)
        return cost
    return cb


def _make_fuel_cb(
    manager,
    dist_dm          : np.ndarray,
    dist_depot       : np.ndarray,
    is_depot_mask    : List[bool],
    fpm              : float,
    bt_threshold     : float,
    bt_factor        : float,
    out_threshold    : float,
    out_factor       : float,
):
    """Fuel cost (per decimetre) with two-tier outbound-sweep penalty."""
    def cb(fi, ti):
        ni   = manager.IndexToNode(fi)
        nj   = manager.IndexToNode(ti)
        base = int(dist_dm[ni][nj] * fpm)
        if is_depot_mask[ni] or is_depot_mask[nj]:
            return base
        d_i = dist_depot[ni]
        d_j = dist_depot[nj]
        if d_i > 100:
            ratio = d_j / d_i
            if ratio < out_threshold:          # Tier 2
                return int(base * out_factor)
            if ratio < bt_threshold:           # Tier 1
                return int(base * bt_factor)
        return base
    return cb


# ════════════════════════════════════════════════════════════
#  Unserved diagnosis  (v9.1 — horizon-aware)
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
    """
    Return the most specific reason a store was not served, with
    concrete advice on how to fix each problem.

    Checks (first match wins):
      1. Demand exceeds every available vehicle
      2. Round-trip physically impossible even within 48 h
      3. Store's time window is narrower than travel time
      4. Invalid / zero time window in source data
      5. Fleet total capacity exhausted across all trips
      6. Store is very far but technically reachable
      7. Generic solver drop — tuning advice
    """
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

    log.warning(
        "[DROP DEBUG] %s | travel=%.2fh | round_trip=%.2fh | 48h_ceiling=%.0fh",
        nd["node_id"],
        t_s / 3600,
        (t_s * 2 + svc_s) / 3600,
        MAX_ROUTE_TIME / 3600,
    )

    # 1. Individual demand exceeds every vehicle ──────────────
    if dkg > max_kg * 1.01:
        needed = math.ceil(dkg / 100) * 100
        return (
            f"⚖️  Demand {dkg:,.0f} kg exceeds the largest vehicle "
            f"({max_kg:,.0f} kg capacity). "
            f"Split this store's order across two delivery days, "
            f"or add a vehicle with ≥ {needed:,} kg capacity."
        )
    if dm3 > max_m3 * 1.01:
        needed_m3 = round(dm3 * 1.05, 2)
        return (
            f"📦  Demand {dm3:.2f} m³ exceeds the largest vehicle "
            f"({max_m3:.2f} m³ capacity). "
            f"Split the order or add a vehicle with ≥ {needed_m3:.2f} m³."
        )

    # 2. Round-trip physically impossible even within 48 h ────
    # (Only fires if even a full 48-hour window cannot fit the journey.)
    round_trip_s = t_s * 2 + svc_s
    if round_trip_s > MAX_ROUTE_TIME:
        rth = round_trip_s / 3600
        return (
            f"🚛  Physically unreachable even within a 48-hour window. "
            f"Round-trip = travel {t_s/3600:.1f}h × 2 + "
            f"service {svc_s/60:.0f} min = {rth:.1f}h. "
            f"Options: (a) use a regional staging depot closer to this store, "
            f"(b) air-freight the order, "
            f"(c) schedule a dedicated multi-day run."
        )

    # 3. Time window too narrow for travel ────────────────────
    # Only meaningful if tw_close is a real store-hours constraint
    # (not the 48-h fallback we assign to far/all-day stores).
    if tw_close < MAX_ROUTE_TIME and t_s > tw_close:
        shift_s       = sched["start_hour"] * 3600
        open_wall     = (tw_open  + shift_s) // 3600
        close_wall    = (tw_close + shift_s) // 3600
        earliest_arr  = sched["start_hour"] + t_s / 3600
        return (
            f"⏰  Cannot arrive within the store's opening hours. "
            f"Earliest possible arrival from depot = "
            f"{int(earliest_arr):02d}:{int((earliest_arr % 1)*60):02d}, "
            f"but store closes at {close_wall:02d}:00 "
            f"(open {open_wall:02d}:00 – {close_wall:02d}:00). "
            f"Travel alone takes {t_s/3600:.1f}h. "
            f"Fix: ask store to accept a next-day window, "
            f"or use a closer staging depot."
        )

    # 4. Invalid time window in source data ───────────────────
    if tw_open >= tw_close:
        raw = nd.get("store") or {}
        ro  = int(raw.get("open_s",  tw_open))
        rc  = int(raw.get("close_s", tw_close))
        return (
            f"🗂️  Invalid time window in store data: "
            f"open={ro//3600:02d}:{(ro%3600)//60:02d}, "
            f"close={rc//3600:02d}:{(rc%3600)//60:02d}. "
            f"After adjusting for fleet start "
            f"({sched['start_hour']:02d}:00) the window is zero "
            f"or negative ({tw_open}s – {tw_close}s). "
            f"Check open_s / close_s values in the database."
        )

    # 5. Fleet-wide capacity exhausted ────────────────────────
    total_cap_kg = sum(v["cap_kg"] for v in vehicles) * cfg.max_trips
    total_cap_m3 = sum(v["cap_m3"] for v in vehicles) * cfg.max_trips
    total_dem_kg = sum(n2["demand_kg"] for n2 in nodes[1:])
    total_dem_m3 = sum(n2["demand_m3"] for n2 in nodes[1:])

    if total_dem_kg > total_cap_kg * 0.95:
        over = (total_dem_kg / total_cap_kg - 1) * 100
        return (
            f"🏋️  Fleet weight capacity exhausted. "
            f"Total demand {total_dem_kg:,.0f} kg vs fleet capacity "
            f"{total_cap_kg:,.0f} kg ({over:+.1f}% over, "
            f"{cfg.max_trips} trip(s)). "
            f"Add vehicles, increase max_trips to {cfg.max_trips + 1}, "
            f"or move low-priority stores to a separate day."
        )
    if total_dem_m3 > total_cap_m3 * 0.95:
        over = (total_dem_m3 / total_cap_m3 - 1) * 100
        return (
            f"📐  Fleet volume capacity exhausted. "
            f"Total demand {total_dem_m3:.1f} m³ vs fleet capacity "
            f"{total_cap_m3:.1f} m³ ({over:+.1f}% over, "
            f"{cfg.max_trips} trip(s)). "
            f"Add larger vehicles or increase max_trips."
        )

    # 6. Very far — reachable but risky ───────────────────────
    if dist_km > cfg.far_threshold_km:
        fits = dkg <= max_kg * cfg.max_weight_fill
        return (
            f"📍  Store is {dist_km:.0f} km from depot "
            f"(threshold: {cfg.far_threshold_km:.0f} km). "
            f"{'Fits on a single truck by weight.' if fits else 'Does NOT fit on the largest truck.'} "
            f"The solver may have dropped it to avoid worsening other routes. "
            f"Fix: increase solver_time_s to 180+ s, "
            f"use mode='geographic' to cluster distant stores, "
            f"or assign a dedicated vehicle for far stores."
        )

    # 7. Generic — solver dropped during optimisation ─────────
    window_h  = (tw_close - tw_open) / 3600
    util_pct  = (total_dem_kg / total_cap_kg * 100) if total_cap_kg else 0
    return (
        f"🔧  Dropped by solver during optimisation "
        f"({dist_km:.0f} km from depot, "
        f"window {tw_open/3600:.1f}h – {tw_close/3600:.1f}h "
        f"= {window_h:.1f}h wide, "
        f"demand {dkg:.0f} kg / {dm3:.2f} m³, "
        f"fleet utilisation {util_pct:.0f}%). "
        f"Most likely cause: time-window conflicts with other stores on "
        f"the same route. "
        f"Try: (1) solver_time_s → 180–300 s, "
        f"(2) max_trips → {cfg.max_trips + 1}, "
        f"(3) mode='geographic' for area clustering, "
        f"(4) add one more vehicle."
    )


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

    MAX_ROUTE_TIME (48 h) is the hard ceiling for OR-Tools' time
    dimension. Stops arriving past 86 400 s wall-clock are flagged
    is_next_day=True so dispatchers can call ahead.
    """
    sched   = config.FLEET_SCHEDULE[fleet]
    shift_s = sched["start_hour"] * 3600

    min_offset  = min(int(v.get("start_offset", 0)) for v in vehicles)
    depart_hour = _trip_depart_hour(fleet, min_offset)

    # 1. Travel times from depot ──────────────────────────────
    travel_s, store_nids = _depot_travel_times(
        dur_df, dist_df, depot["name"], stores, fleet, depart_hour
    )
    if not store_nids:
        return {"routes": [], "unserved": [], "nodes": [], "fleet": fleet}

    # 2. Node list ────────────────────────────────────────────
    nodes = _build_nodes(depot, stores, fleet, travel_s, store_nids, sched, cfg)
    n_eligible = len(nodes) - 1
    if n_eligible == 0:
        return {"routes": [], "unserved": [], "nodes": nodes, "fleet": fleet}

    n  = len(nodes)
    nv = len(vehicles)

    # 3. Matrices ─────────────────────────────────────────────
    dist_mat, dur_mat = _build_submatrix(
        dist_df, dur_df, nodes, depot["name"], depart_hour
    )
    dist_dm    = (dist_mat / 10.0).astype(np.int64)
    dur_s      = dur_mat.astype(np.int64)
    dist_depot = dist_mat[0, :].copy()

    is_depot_mask = [nd["is_depot"] for nd in nodes]
    bearings      = [nd["bearing"]  for nd in nodes]
    svc_times     = np.array([nd["service_s"] for nd in nodes], dtype=np.int64)

    # 4. Routing model ─────────────────────────────────────────
    manager = pywrapcp.RoutingIndexManager(n, nv, [0] * nv, [0] * nv)
    routing = pywrapcp.RoutingModel(manager)

    # 5. Register callbacks ────────────────────────────────────
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

    # 6. Arc costs and span coefficients by mode ──────────────
    span_coeff = 10

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
        span_coeff = config.BALANCED_SPAN_COEFF
        routing.SetFixedCostOfAllVehicles(cfg.vehicle_fixed_cost)

    elif cfg.mode == "geographic":
        routing.SetArcCostEvaluatorOfAllVehicles(geo_cb_idx)
        span_coeff = 20
        routing.SetFixedCostOfAllVehicles(cfg.vehicle_fixed_cost)

    else:  # cheapest
        for vi, veh in enumerate(vehicles):
            fpm = veh["fuel_cost_km"] / 10_000.0  # ₮ / decimetre
            routing.SetArcCostEvaluatorOfVehicle(
                routing.RegisterTransitCallback(
                    _make_fuel_cb(
                        manager, dist_dm, dist_depot, is_depot_mask,
                        fpm,
                        cfg.backtrack_threshold, cfg.backtrack_factor,
                        cfg.outbound_threshold,  cfg.outbound_factor,
                    )
                ),
                vi,
            )
            routing.SetFixedCostOfVehicle(
                int(veh.get("vehicle_cost", 0) + veh.get("labor_cost", 0)), vi
            )
        span_coeff = 10

    # 7. Capacity dimensions ───────────────────────────────────
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

    # 8. Time-window dimension ─────────────────────────────────
    # Hard ceiling is MAX_ROUTE_TIME (48 h). There is no shift-horizon
    # cap here — the soft penalty below encourages finishing early
    # without forbidding late routes.
    routing.AddDimension(
        time_cb_idx,
        cfg.max_wait_slack_s,
        MAX_ROUTE_TIME,
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

    max_h_s = (sched["max_horizon_hour"] - sched["start_hour"]) * 3600

    for vi, veh in enumerate(vehicles):
        start_off = int(veh.get("start_offset", 0))

        time_dim.CumulVar(routing.Start(vi)).SetRange(start_off, start_off)

        # Hard upper bound removed — route can extend past shift-end.
        # Soft penalty keeps the solver preferring on-time returns
        # without ever dropping a store purely due to shift length.
        time_dim.CumulVar(routing.End(vi)).SetRange(start_off, MAX_ROUTE_TIME)

        time_dim.SetCumulVarSoftUpperBound(
            routing.End(vi),
            max_h_s,           # prefer finishing within normal shift
            50,                # cost per second overtime (₮/s, tune if needed)
        )

    if span_coeff > 0:
        time_dim.SetGlobalSpanCostCoefficient(span_coeff)

    # 9. Disjunctions ──────────────────────────────────────────
    for i in range(1, n):
        node_idx = manager.NodeToIndex(i)

        # Far stores get an even higher penalty to make them near-mandatory.
        # Because MAX_ROUTE_TIME is now the ceiling (not max_h_s),
        # the solver actually has room to serve them.
        if nodes[i]["travel_s"] > max_h_s * 0.6:
            routing.AddDisjunction([node_idx], cfg.penalty_unserved * 10)
        else:
            routing.AddDisjunction([node_idx], cfg.penalty_unserved)

    # 10. Search parameters ────────────────────────────────────
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
    params.log_search         = False

    # 11. Geographic mode: sector warm-start ──────────────────
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
                "[%s] Trip %d: sector hint returned None — "
                "falling back to PATH_CHEAPEST_ARC",
                fleet, trip_num,
            )
            solution = routing.SolveWithParameters(params)
    else:
        solution = routing.SolveWithParameters(params)

    # 12. Handle no-solution ───────────────────────────────────
    if solution is None:
        log.warning(
            "[%s] Trip %d: No solution found (%d stores, %d vehicles, %ds budget)",
            fleet, trip_num, n_eligible, nv, cfg.solver_time_s,
        )
        return {
            "routes"  : [],
            "unserved": [
                {
                    "store" : nd["store"],
                    "reason": (
                        f"🔧  Solver found no feasible solution for trip {trip_num} "
                        f"({n_eligible} stores, {nv} vehicles, {cfg.solver_time_s}s budget). "
                        f"Try increasing solver_time_s to {cfg.solver_time_s * 2}s "
                        f"or adding more vehicles."
                    ),
                    "node": nd,
                }
                for nd in nodes[1:]
            ],
            "nodes": nodes,
            "fleet": fleet,
        }

    # 13. Extract routes ───────────────────────────────────────
    nid_to_idx: Dict[str, int] = {nd["node_id"]: i for i, nd in enumerate(nodes)}
    raw_routes : List[Dict]    = []
    served_ids : set           = set()

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
                arr_wall = t_solver + shift_s   # absolute wall-clock seconds

                stops.append({
                    "node_id"    : nd["node_id"],
                    "store"      : nd["store"],
                    "arrival_s"  : float(arr_wall),
                    "depart_s"   : float(arr_wall + nd["service_s"]),
                    "demand_kg"  : float(nd["demand_kg"]),
                    "demand_m3"  : float(nd["demand_m3"]),
                    "lat"        : float(nd["lat"]),
                    "lon"        : float(nd["lon"]),
                    # Arrival past midnight → flag for dispatcher
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

    # 14. Unserved diagnosis ───────────────────────────────────
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
    Run up to cfg.max_trips sequential trip rounds.

    Pre-sort strategy:
      geographic → sort by bearing (angle from depot) so stores in the
                   same direction are adjacent — better seed for sectors.
      all others → sort by descending demand then ascending close-time
                   (heavy, tight-window stores seeded first).
    """
    sched   = config.FLEET_SCHEDULE[fleet]

    fleet_key = "has_dry" if fleet == "DRY" else "has_cold"
    dem_field = "dry_kg"  if fleet == "DRY" else "cold_kg"

    eligible = [s for s in stores if s.get(fleet_key)]

    dep_lat = float(depot["lat"])
    dep_lon = float(depot["lon"])

    if cfg.mode == "geographic":
        eligible.sort(
            key=lambda s: _bearing(
                dep_lat, dep_lon, float(s["lat"]), float(s["lon"])
            )
        )
        log.debug("[%s] geographic: stores sorted by bearing.", fleet)
    else:
        eligible.sort(
            key=lambda s: (
                -_haversine_m(dep_lat, dep_lon, float(s["lat"]), float(s["lon"])),  # FAR FIRST
                -s.get(dem_field, 0.0),
                s.get("close_s", 86399),
            )
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
            # Skip trucks whose reload offset exceeds the 48-hour ceiling.
            # This is intentionally generous — a truck that returned at
            # 30 h can still do a second trip if needed.
            if offset >= MAX_ROUTE_TIME:
                log.debug(
                    "[%s] Truck %s skipped trip %d: offset %.2fh exceeds 48h ceiling",
                    fleet, v["truck_id"], trip_num,
                    offset / 3600,
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

        served    = {s["node_id"] for r in res["routes"] for s in r["stops"]}
        prev_len  = len(remaining)
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
                f"🔁  Not served after {cfg.max_trips} trip(s). "
                f"Increase max_trips to {cfg.max_trips + 1} or add more vehicles. "
                f"Current fleet covers {len(served_all)} of "
                f"{len(eligible)} eligible stores."
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
        Arc cost = distance (dm) + anti-backtrack surcharge.
        No span balancing. Finds fewest total km.
        Anti-backtrack: arcs moving ≥30% closer to depot cost 30% more.

    fastest
        Arc cost = travel time + service time at FROM node.
        PATH_MOST_CONSTRAINED_ARC seeds tightest windows first.
        span=50 balances driver hours.

    cheapest
        Arc cost = fuel ₮/km per vehicle (different rates possible).
        Fixed cost (vehicle_cost + labor_cost) charged per truck used.
        Solver trades fewer trucks (less fixed cost) vs longer routes
        (more fuel). Anti-backtrack applied to fuel cost.

    balanced
        Arc cost = distance + anti-backtrack (same as shortest).
        span = BALANCED_SPAN_COEFF (from config, default 300).
        High span forces OR-Tools to equalise route durations before
        caring about total distance. No driver gets 10h while another
        does 2h.

    geographic
        Arc cost = distance + angular penalty + anti-backtrack.
        angular penalty: cost(i→j) += cost × (angle/180)² × geo_angular_w
        Sector warm-start: stores pre-sorted by bearing, divided into N
        sectors (one per vehicle), fed to ReadAssignmentFromRoutes().
        GLS then refines within and between sectors.
        Result: compact pie-slice routes — each truck covers one wedge.

    Next-day delivery
        Routes are no longer capped at the shift horizon. OR-Tools may
        schedule far stores for overnight arrival; those stops are marked
        is_next_day=True so dispatchers can notify the store in advance.
        The soft overtime penalty (50 ₮/s past shift-end) keeps the
        solver preferring on-time finishes without ever dropping a store
        purely because of clock constraints.

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
                {
                    "store" : s,
                    "reason": "⚙️  No DRY vehicles configured. Add a vehicle assigned to 'Dry DC'.",
                    "node"  : None,
                }
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
                {
                    "store" : s,
                    "reason": "⚙️  No COLD vehicles configured. Add a vehicle assigned to 'Cold DC'.",
                    "node"  : None,
                }
                for s in stores if s.get("has_cold")
            ],
        }

    return results