# ============================================================
#  config.py  –  Central configuration for VRP system  v6
#
#  Key changes from v5:
#   • Rural routes REMOVED — all stores treated the same (urban)
#   • CLUSTERING toggle (env var) — compare clustered vs single solve
#   • HOUR_SPEED_FACTOR — Ulaanbaatar traffic pattern by hour
#   • RELOAD_TIME_SECONDS — gap between truck trips at depot
#   • Simplified FLEET_SCHEDULE (no rural_end_hour etc.)
# ============================================================

import os

# ── File & Sheet Names ──────────────────────────────────────
STORE_FILE   = os.getenv("STORE_FILE",  "data/stores.xlsx")
MATRIX_FILE  = os.getenv("MATRIX_FILE", "data/matrix.xlsx")

STORE_SHEET    = "Store"
VEHICLE_SHEET  = "Vehicle"
DURATION_SHEET = "Duration"
DISTANCE_SHEET = "Distance"
DRY_DC_ROUTE_SHEET = "Dry DC"
COLD_DC_ROUTE_SHEET = "Cold DC"

# ── Store Sheet Column Names ────────────────────────────────
COL_STORE_ID  = "Store ID"
COL_USE_YN    = "USE_YN"
COL_ENG_NAME  = "BIZLOC_ENG_NM"
COL_MN_NAME   = "BIZLOC_NM"
COL_ADDR      = "ADDR_1"
COL_DTL_ADDR  = "DTL_ADDR"
COL_LAT       = "LATITUDE"
COL_LON       = "LONGITUDE"
COL_OPEN      = "SalesStartTime"
COL_CLOSE     = "SalesCloseTime"
COL_CITY      = "City"

# Seasonal Average Order Columns
COL_SUMMER_DRY_CBM  = "Summer Average Order CBM (DRY DC)"
COL_SUMMER_DRY_KG   = "Summer Average Order Weight (DRY DC)"
COL_SUMMER_COLD_CBM = "Summer Average Order CBM (COLD DC)"
COL_SUMMER_COLD_KG  = "Summer Average Order Weight (COLD DC)"

COL_AUTUMN_DRY_CBM  = "Autumn Average Order CBM (DRY DC)"
COL_AUTUMN_DRY_KG   = "Autumn Average Order Weight (DRY DC)"
COL_AUTUMN_COLD_CBM = "Autumn Average Order CBM (COLD DC)"
COL_AUTUMN_COLD_KG  = "Autumn Average Order Weight (COLD DC)"

COL_WINTER_DRY_CBM  = "Winter Average Order CBM (DRY DC)"
COL_WINTER_DRY_KG   = "Winter Average Order Weight (DRY DC)"
COL_WINTER_COLD_CBM = "Winter Average Order CBM (COLD DC)"
COL_WINTER_COLD_KG  = "Winter Average Order Weight (COLD DC)"

COL_SPRING_DRY_CBM  = "Spring Average Order CBM (DRY DC)"
COL_SPRING_DRY_KG   = "Spring Average Order Weight (DRY DC)"
COL_SPRING_COLD_CBM = "Spring Average Order CBM (COLD DC)"
COL_SPRING_COLD_KG  = "Spring Average Order Weight (COLD DC)"

# ── Vehicle Sheet Column Names ──────────────────────────────
COL_DEPOT        = "Depot"
COL_TRUCK_ID     = "Truck ID"
COL_CONTRACTOR   = "Subcontractor"
COL_TRUCK_NUM    = "Truck Num"
COL_DESCRIPTION  = "Description"
COL_CAP_KG       = "Capacity_kg"
COL_CAP_M3       = "Capacity_m3"
COL_FUEL_COST    = "Fuel cost per km"
COL_VEHICLE_COST = "Vehicle cost per day"
COL_LABOR_COST   = "Labor cost per day"

# ── Dry Route ──────────────────────────────
COL_DRY_TRUCK_NUM = "Truck number"
COL_DRY_DIRECTION = "Чиглэл"
COL_DRY_STORES    = "Салбар дэлгүүрүүд"

# ── Cold Route ──────────────────────────────
COL_COLD_TRUCK_NUM = "Truck number"
COL_COLD_DIRECTION = "Чиглэл"
COL_COLD_STORES    = "Салбар дэлгүүрүүд"

# ── Depot Locations (Ulaanbaatar, Mongolia) ─────────────────
DEPOTS = {
    "Dry DC":  {"lat": 47.8847516,         "lon": 106.7932466},
    "Cold DC": {"lat": 47.80758101116645,  "lon": 107.19407110357587},
}

DEPOT_VEHICLE_MAP = {
    "Dry DC":  "DRY",
    "Cold DC": "COLD",
}

FLEET_SCHEDULE = {
    "DRY": {
        "start_hour"      : 13,   # 13:00 departure
        "max_horizon_hour": 24,   # hard stop at 24:00 (11h shift)
    },
    "COLD": {
        "start_hour"      : 3,    # 03:00 departure
        "max_horizon_hour": 14,   # hard stop at 14:00 (11h shift)
    },
}


HOUR_SPEED_FACTOR: dict = {
    # Hour : speed_multiplier
    0:  1.30,   # 00:00  clear roads (midnight)
    1:  1.30,   # 01:00
    2:  1.30,   # 02:00
    3:  1.25,   # 03:00  COLD fleet departs
    4:  1.10,   # 04:00
    5:  1.00,   # 05:00  early morning builds
    6:  0.80,   # 06:00  rush starts
    7:  0.65,   # 07:00  peak morning rush
    8:  0.70,   # 08:00  still heavy
    9:  0.85,   # 09:00  easing off
    10: 0.95,   # 10:00
    11: 1.00,   # 11:00  free flow
    12: 0.90,   # 12:00  lunch traffic
    13: 0.85,   # 13:00  DRY fleet departs — moderate congestion
    14: 0.90,   # 14:00
    15: 0.85,   # 15:00
    16: 0.75,   # 16:00  evening rush builds
    17: 0.65,   # 17:00  peak evening rush
    18: 0.70,   # 18:00  still heavy
    19: 0.85,   # 19:00  easing
    20: 0.95,   # 20:00
    21: 1.05,   # 21:00
    22: 1.15,   # 22:00
    23: 1.25,   # 23:00  clear
}

# ── Solver Parameters ───────────────────────────────────────
MAX_TRIPS_PER_VEHICLE   = 2       # How many return-and-reload trips per vehicle per shift
SERVICE_TIME_SECONDS    = 600     # 10 min unloading per stop
RELOAD_TIME_SECONDS     = 1800    # 30 min to reload at depot between trips
                                  # (counting: park, unload return pallets, load new, depart)

MAX_SOLVER_TIME_SECONDS     = 120  # OR-Tools time budget for full solve
MIN_CLUSTER_SOLVER_TIME     = 5    # minimum per cluster (reduced from 30)

# ── Penalties & Fixed Costs ─────────────────────────────────
PENALTY_UNSERVED     = 10_000_000_000  # must exceed any route cost to force serving stores
VEHICLE_FIXED_COST   = 5_000          # small cost per trip to discourage empty trips
# For "balanced" mode: penalty on kg-load span (max_load - min_load) across trucks.
# Higher value → more even loads, possibly longer total distance.
BALANCED_SPAN_COEFF  = 300

# ── Distance & Routing ──────────────────────────────────────
FAR_THRESHOLD_KM = 1000    # flag stores beyond this as "very far"

# ── OSRM ────────────────────────────────────────────────────
OSRM_URL = os.getenv("OSRM_URL", "http://localhost:5000")

# ── Capacity Fill Percentages ─────────────────────────────────────
# Maximum fill percentages for vehicle capacities (0.0 to 1.0)
# These can be overridden from the frontend UI
MAX_WEIGHT_FILL_PERCENTAGE = float(os.getenv("MAX_WEIGHT_FILL_PERCENTAGE", "1.0"))  # 100% by default
MAX_VOLUME_FILL_PERCENTAGE = float(os.getenv("MAX_VOLUME_FILL_PERCENTAGE", "1.0"))  # 100% by default

# ── Season Configuration ─────────────────────────────────────────
# Default season for optimization (can be overridden from frontend)
VALID_SEASONS = ["summer", "autumn", "winter", "spring"]
DEFAULT_SEASON = os.getenv("DEFAULT_SEASON", "summer").lower()

# ── Integer scaling (OR-Tools needs integers) ────────────────
M3_SCALE = 1000   # m³ × 1000 → integer litres for capacity constraints

# ── Urban / Rural Logic ─────────────────────────────────────

# Urban vehicle restrictions
URBAN_MAX_CAP_M3 = 15
URBAN_MAX_CAP_KG = 3000

# Cost bias (NEW STRATEGY)
CONTRACTOR_COST_MULT = 4.0   # expensive fallback
FLEET_COST_MULT      = 0.7   # cheap primary