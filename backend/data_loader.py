# ============================================================
#  data_loader.py  –  Load and validate input data
#  ✅ No structural changes needed for MongoDB.
#     The only real change: validation now accepts a plain set
#     of matrix IDs instead of a DataFrame, because we no longer
#     store DataFrames in SQLAlchemy — we pass the raw sets around.
# ============================================================

import pandas as pd
import numpy as np
import io
import logging
from datetime import time
from typing import Dict, List, Optional, Tuple
import config

log = logging.getLogger(__name__)

# ── Helpers ──────────────────────────────────────────────────
def safe_float(x):
    if isinstance(x, (list, tuple, np.ndarray, pd.Series)):
        return 0.0
    try:
        return float(x)
    except:
        return 0.0

def _norm_id(x) -> str:
    """Strip leading zeros so matrix keys match store IDs."""
    try:
        return str(int(str(x).strip()))
    except Exception:
        return str(x).strip()


def _parse_time_to_seconds(t) -> int:
    """Convert various time formats → seconds since midnight."""
    if t is None or (isinstance(t, float) and np.isnan(t)):
        return 0
    if isinstance(t, time):
        return t.hour * 3600 + t.minute * 60 + t.second
    if isinstance(t, str):
        parts = t.strip().split(":")
        try:
            h, m, s = int(parts[0]), int(parts[1]), int(parts[2]) if len(parts) > 2 else 0
            return h * 3600 + m * 60 + s
        except Exception:
            return 0
    # pandas Timedelta
    try:
        total = int(t.total_seconds())
        return total
    except Exception:
        return 0


# ── Store Loader ─────────────────────────────────────────────
# ✅ UNCHANGED — returns plain list of dicts, perfect for MongoDB

def load_stores(file_bytes: bytes, season: str, sheet: str = config.STORE_SHEET) -> Tuple[List[Dict], List[str]]:
    """Parse store Excel sheet → list of store dicts and warnings.

    Handles multi-level headers where seasons are in row 0 and metrics in row 1.
    Loads all 4 seasons' data (Summer, Autumn, Winter, Spring) regardless of the season parameter.

    Returns:
        Tuple[List[Dict], List[str]]: (stores_list, warnings_list)
        Warnings include stores excluded due to USE_YN status.
    """
    # Read raw data without headers first to detect structure
    df_raw = pd.read_excel(io.BytesIO(file_bytes), sheet_name=sheet, header=None)
    
    # Check if row 1 has seasonal headers (contains "Avarage Order")
    row0 = df_raw.iloc[0].fillna('').astype(str).tolist()
    row1 = df_raw.iloc[1].fillna('').astype(str).tolist()
    
    has_multi_header = any('Avarage Order' in str(c) for c in row0)
    
    warnings = []
    skipped_stores = []
    season = season.lower()
    
    log.info(f"[load_stores] Multi-level header detected: {has_multi_header}")

    # Map seasons to header row 0 values (Excel has typo "Avarage" not "Average")
    season_headers = {
        "summer": "Summer Avarage Order",
        "autumn": "Autumn Avarage Order",
        "winter": "Winter Avarage Order",
        "spring": "Spring Avarage Order",
    }
    season_header = season_headers.get(season, "Summer Avarage Order")

    if has_multi_header:
        # Build flat column names by combining level 0 and level 1
        # For seasonal columns: "Summer Avarage Order | CBM (DRY DC)"
        # For basic columns: just use level 0 name
        flat_cols = []
        col_idx_map = {}  # flat name -> index
        
        current_season = None  # Track current season across merged cells
        
        for i, (h0, h1) in enumerate(zip(row0, row1)):
            h0 = str(h0).strip() if h0 else ''
            h1 = str(h1).strip() if h1 else ''
            
            # Check if this is a new season header
            if 'Avarage Order' in h0:
                current_season = h0
                # Seasonal column - combine with metric from h1
                flat_name = f"{h0} | {h1}" if h1 else h0
            elif current_season and h1 and not h0:
                # Continuation of seasonal columns (merged cell)
                flat_name = f"{current_season} | {h1}"
            else:
                # Basic column - just use h0, reset season tracker
                if h0 and not h0.startswith('Unnamed'):
                    current_season = None
                flat_name = h0 if h0 else h1

            flat_cols.append(flat_name)
            col_idx_map[flat_name] = i
        
        log.info(f"[load_stores] Flat columns created: {flat_cols}")
        
        # Get basic column indices first (needed for dtype mapping)
        store_id_idx = col_idx_map.get(config.COL_STORE_ID)
        lat_idx = col_idx_map.get(config.COL_LAT)
        lon_idx = col_idx_map.get(config.COL_LON)
        use_yn_idx = col_idx_map.get(config.COL_USE_YN)
        eng_name_idx = col_idx_map.get(config.COL_ENG_NAME)
        mn_name_idx = col_idx_map.get(config.COL_MN_NAME)
        addr_idx = col_idx_map.get(config.COL_ADDR)
        dtl_addr_idx = col_idx_map.get(config.COL_DTL_ADDR)
        open_idx = col_idx_map.get(config.COL_OPEN)
        close_idx = col_idx_map.get(config.COL_CLOSE)
        city_idx = col_idx_map.get(config.COL_CITY)
        
        # Read data starting from row 3 (skipping 2 header rows), with flat column names
        # Use dtype to ensure Store ID is read as string to preserve leading zeros
        # Note: when header=None, dtype mapping uses column indices (0-based), not names
        dtype_map = {store_id_idx: str} if store_id_idx is not None else {}
        df_raw_data = pd.read_excel(io.BytesIO(file_bytes), sheet_name=sheet, header=None, skiprows=2, dtype=dtype_map)
        # Assign flat column names
        df_raw_data = df_raw_data.iloc[:, :len(flat_cols)]
        df_raw_data.columns = flat_cols
        if df_raw_data.shape[1] != len(flat_cols):
            raise ValueError(f"Column mismatch: data={df_raw_data.shape[1]}, headers={len(flat_cols)}")
        df = df_raw_data
        
        # Get seasonal column indices for all 4 seasons
        seasonal_indices = {}
        for s_name, s_header in season_headers.items():
            seasonal_indices[s_name] = {
                "dry_cbm": col_idx_map.get(f"{s_header} | CBM (DRY DC)"),
                "dry_kg": col_idx_map.get(f"{s_header} | Weight (DRY DC)"),
                "cold_cbm": col_idx_map.get(f"{s_header} | CBM (COLD DC)"),
                "cold_kg": col_idx_map.get(f"{s_header} | Weight (COLD DC)"),
            }
        
        # For backward compatibility, keep the single season indices
        dry_cbm_idx = seasonal_indices[season]["dry_cbm"]
        dry_kg_idx = seasonal_indices[season]["dry_kg"]
        cold_cbm_idx = seasonal_indices[season]["cold_cbm"]
        cold_kg_idx = seasonal_indices[season]["cold_kg"]

        log.info(f"[load_stores] Season '{season_header}' columns: dry_cbm_idx={dry_cbm_idx}, dry_kg_idx={dry_kg_idx}, cold_cbm_idx={cold_cbm_idx}, cold_kg_idx={cold_kg_idx}")
        log.info(f"[load_stores] Basic columns: lat_idx={lat_idx}, lon_idx={lon_idx}, store_id_idx={store_id_idx}, use_yn_idx={use_yn_idx}")

    else:
        # Simple flat header
        # Read Store ID as string to preserve leading zeros
        dtype_map = {config.COL_STORE_ID: str}
        df = pd.read_excel(io.BytesIO(file_bytes), sheet_name=sheet, dtype=dtype_map)

        season_map = {
            "summer": (config.COL_SUMMER_DRY_CBM, config.COL_SUMMER_DRY_KG, config.COL_SUMMER_COLD_CBM, config.COL_SUMMER_COLD_KG),
            "autumn": (config.COL_AUTUMN_DRY_CBM, config.COL_AUTUMN_DRY_KG, config.COL_AUTUMN_COLD_CBM, config.COL_AUTUMN_COLD_KG),
            "winter": (config.COL_WINTER_DRY_CBM, config.COL_WINTER_DRY_KG, config.COL_WINTER_COLD_CBM, config.COL_WINTER_COLD_KG),
            "spring": (config.COL_SPRING_DRY_CBM, config.COL_SPRING_DRY_KG, config.COL_SPRING_COLD_CBM, config.COL_SPRING_COLD_KG),
        }
        dry_cbm_col, dry_kg_col, cold_cbm_col, cold_kg_col = season_map.get(season, season_map["summer"])
        dry_cbm_idx = dry_kg_idx = cold_cbm_idx = cold_kg_idx = None  # Will use column names
        lat_idx = lon_idx = store_id_idx = use_yn_idx = None
        eng_name_idx = mn_name_idx = addr_idx = dtl_addr_idx = None
        open_idx = close_idx = city_idx = None

    # Convert coordinate columns to numeric
    if has_multi_header:
        if lat_idx is not None:
            df.iloc[:, lat_idx] = pd.to_numeric(df.iloc[:, lat_idx], errors="coerce")
            df.iloc[:, lon_idx] = pd.to_numeric(df.iloc[:, lon_idx], errors="coerce")

            df = df.dropna(subset=[df.columns[lat_idx], df.columns[lon_idx]])
        if lon_idx is not None:
            df.iloc[:, lon_idx] = pd.to_numeric(df.iloc[:, lon_idx], errors="coerce").fillna(0.0)
        if lat_idx is not None and lon_idx is not None:
            df = df[(df.iloc[:, lat_idx].notna()) & (df.iloc[:, lat_idx] != 0) & 
                    (df.iloc[:, lon_idx].notna()) & (df.iloc[:, lon_idx] != 0)]
    else:
        for col in [config.COL_LAT, config.COL_LON]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
        df = df.dropna(subset=[config.COL_LAT, config.COL_LON])

    def get_val(row, idx, col_name, default=""):
        try:
            if has_multi_header:
                if idx is None:
                    return default
                return row.iloc[idx]  # ✅ SAFE (position-based)
            else:
                return row.get(col_name, default)
        except Exception:
            return default

    stores = []
    for _, row in df.iterrows():
        if has_multi_header:
            use_yn = str(get_val(row, use_yn_idx, config.COL_USE_YN, "")).strip().lower()
            raw_id = str(get_val(row, store_id_idx, config.COL_STORE_ID, "")).strip()
            open_val = get_val(row, open_idx, config.COL_OPEN)
            close_val = get_val(row, close_idx, config.COL_CLOSE)
            lat_val = row["LATITUDE"]
            lon_val = row["LONGITUDE"]
            eng_name = str(get_val(row, eng_name_idx, config.COL_ENG_NAME, ""))
            mn_name = str(get_val(row, mn_name_idx, config.COL_MN_NAME, ""))
            addr = str(get_val(row, addr_idx, config.COL_ADDR, ""))
            dtl_addr = str(get_val(row, dtl_addr_idx, config.COL_DTL_ADDR, ""))
            city_val = str(get_val(row, city_idx, config.COL_CITY, "Rural")).strip()
        else:
            use_yn = str(row.get(config.COL_USE_YN, "")).strip().lower()
            raw_id = str(row[config.COL_STORE_ID]).strip()
            open_val = row.get(config.COL_OPEN)
            close_val = row.get(config.COL_CLOSE)
            lat_val = row[config.COL_LAT]
            lon_val = row[config.COL_LON]
            eng_name = str(row.get(config.COL_ENG_NAME, ""))
            mn_name = str(row.get(config.COL_MN_NAME, ""))
            addr = str(row.get(config.COL_ADDR, ""))
            dtl_addr = str(row.get(config.COL_DTL_ADDR, ""))
            city_val = str(row.get(config.COL_CITY, "Rural")).strip()

        if use_yn != "open":
            skipped_stores.append({
                "store_id": raw_id,
                "reason": f"USE_YN={use_yn}"
            })
            continue

        norm = _norm_id(raw_id)
        open_s = _parse_time_to_seconds(open_val)
        close_s = _parse_time_to_seconds(close_val)

        if close_s == 0 or close_s <= open_s:
            close_s = 86399

        # Get seasonal values for all 4 seasons
        seasonal_data = {}
        if has_multi_header:
            vals = list(row.values)
            for s_name, indices in seasonal_indices.items():
                seasonal_data[s_name] = {
                    "dry_cbm": float(vals[indices["dry_cbm"]] if indices["dry_cbm"] is not None and indices["dry_cbm"] < len(vals) and vals[indices["dry_cbm"]] is not None else 0),
                    "dry_kg": float(vals[indices["dry_kg"]] if indices["dry_kg"] is not None and indices["dry_kg"] < len(vals) and vals[indices["dry_kg"]] is not None else 0),
                    "cold_cbm": float(vals[indices["cold_cbm"]] if indices["cold_cbm"] is not None and indices["cold_cbm"] < len(vals) and vals[indices["cold_cbm"]] is not None else 0),
                    "cold_kg": float(vals[indices["cold_kg"]] if indices["cold_kg"] is not None and indices["cold_kg"] < len(vals) and vals[indices["cold_kg"]] is not None else 0),
                }
            # Use selected season for current values (backward compatibility)
            dry_cbm = seasonal_data[season]["dry_cbm"]
            dry_kg = seasonal_data[season]["dry_kg"]
            cold_cbm = seasonal_data[season]["cold_cbm"]
            cold_kg = seasonal_data[season]["cold_kg"]
        else:
            # Simple flat header - only one season supported
            dry_cbm  = float(row.get(dry_cbm_col,  0) or 0)
            dry_kg   = float(row.get(dry_kg_col,   0) or 0)
            cold_cbm = float(row.get(cold_cbm_col, 0) or 0)
            cold_kg  = float(row.get(cold_kg_col,  0) or 0)
            seasonal_data[season] = {"dry_cbm": dry_cbm, "dry_kg": dry_kg, "cold_cbm": cold_cbm, "cold_kg": cold_kg}

        stores.append({
            "store_id"    : raw_id,  # Preserve original format with leading zeros
            "use_yn"      : use_yn,
            "node_id"     : norm,    # Normalized for matrix lookups
            "season"      : season,
            "eng_name"    : str(eng_name),
            "mn_name"     : str(mn_name),
            "address"     : str(addr),
            "detail_addr" : str(dtl_addr),
            "lat"         : safe_float(lat_val),
            "lon"         : safe_float(lon_val),
            "open_s"      : open_s,
            "close_s"     : close_s,
            "dry_cbm"     : dry_cbm,
            "dry_kg"      : dry_kg,
            "cold_cbm"    : cold_cbm,
            "cold_kg"     : cold_kg,
            "has_dry"     : dry_kg > 0 or dry_cbm > 0,
            "has_cold"    : cold_kg > 0 or cold_cbm > 0,
            "seasonal_data": seasonal_data,  # All 4 seasons' data
            "city"        : city_val,
            "is_urban"    : city_val.upper() == "UB",  # UB = Ulaanbaatar (urban), anything else = rural
        })

    if skipped_stores:
        warnings.extend([f"Store '{s['store_id']}' excluded - {s['reason']}" for s in skipped_stores])
        warnings.append(f"Total stores excluded due to USE_YN status: {len(skipped_stores)}")
    
    return stores, warnings


# ── Vehicle Loader ────────────────────────────────────────────
# ✅ UNCHANGED — returns plain list of dicts, perfect for MongoDB

def load_vehicles(file_bytes: bytes, sheet: str = config.VEHICLE_SHEET) -> List[Dict]:
    """Parse vehicle Excel sheet → list of vehicle dicts."""
    df = pd.read_excel(io.BytesIO(file_bytes), sheet_name=sheet)

    for col in [config.COL_CAP_KG, config.COL_CAP_M3,
                config.COL_FUEL_COST, config.COL_VEHICLE_COST, config.COL_LABOR_COST]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    vehicles = []
    for _, row in df.iterrows():
        depot = str(row[config.COL_DEPOT]).strip()
        fleet = config.DEPOT_VEHICLE_MAP.get(depot, "DRY")

        vehicles.append({
            "truck_id"     : str(row[config.COL_TRUCK_ID]).strip(),
            "description"  : str(row.get(config.COL_DESCRIPTION, "")),
            "contractor": str(row.get(config.COL_CONTRACTOR, "")).strip(),
            "is_fleet": str(row.get(config.COL_CONTRACTOR, "")).strip().lower() == "fleet",
            "truck_num"    : str(row.get(config.COL_TRUCK_NUM, "")).strip(),
            "depot"        : depot,
            "fleet"        : fleet,
            "cap_kg"       : float(row[config.COL_CAP_KG]),
            "cap_m3"       : float(row[config.COL_CAP_M3]),
            "fuel_cost_km" : float(row[config.COL_FUEL_COST]),
            "vehicle_cost" : float(row[config.COL_VEHICLE_COST]),
            "labor_cost"   : float(row[config.COL_LABOR_COST]),
        })

        vehicles.sort(key=lambda v: (not v["is_fleet"]))

    return vehicles


# ── Matrix Loader ─────────────────────────────────────────────
# ✅ UNCHANGED — still returns DataFrames used by the solver in memory.
#    The bytes themselves are saved to GridFS via save_matrix_bytes().

def load_matrix(file_bytes: bytes) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Load distance (m) and duration (min) matrices.
    Returns (distance_df, duration_df) with string index/columns.
    """
    try:
        dur_df  = pd.read_excel(io.BytesIO(file_bytes), sheet_name=config.DURATION_SHEET, index_col=0)
        dist_df = pd.read_excel(io.BytesIO(file_bytes), sheet_name=config.DISTANCE_SHEET, index_col=0)
    except Exception as e:
        raise ValueError(f"Failed to read Excel sheets: {e}. Ensure sheets '{config.DURATION_SHEET}' and '{config.DISTANCE_SHEET}' exist.")

    # Check for empty dataframes
    if dur_df.empty or dist_df.empty:
        raise ValueError("Matrix sheets are empty or missing data")
    
    # Check for matching dimensions
    if dur_df.shape != dist_df.shape:
        raise ValueError(f"Matrix dimension mismatch: duration {dur_df.shape} vs distance {dist_df.shape}")

    dur_df.index   = [_norm_id(x) for x in dur_df.index]
    dur_df.columns = [_norm_id(x) for x in dur_df.columns]
    dist_df.index   = [_norm_id(x) for x in dist_df.index]
    dist_df.columns = [_norm_id(x) for x in dist_df.columns]

    return dist_df, dur_df


# ── Validation ────────────────────────────────────────────────
# ⚠️  SMALL CHANGE: accepts a plain set of matrix IDs instead of a
#     full DataFrame — keeps this function framework-agnostic.

def validate_data(stores: List[Dict], vehicles: List[Dict],
                  dist_df: pd.DataFrame, dur_df: pd.DataFrame) -> List[str]:
    """Return a list of warning strings (empty = OK).
    
    dist_df / dur_df are still passed as DataFrames (loaded in memory
    by load_matrix). Nothing changes here — validation logic is identical.
    """
    warnings = []
    matrix_ids = set(dist_df.index)

    missing = [s["node_id"] for s in stores if s["node_id"] not in matrix_ids]
    if missing:
        warnings.append(
            f"{len(missing)} stores not found in distance matrix: "
            f"{missing[:5]}{'...' if len(missing) > 5 else ''}"
        )

    for dc in config.DEPOTS:
        norm = _norm_id(dc)
        if norm not in matrix_ids and dc not in matrix_ids:
            warnings.append(f"Depot '{dc}' not found in distance matrix")

    if not vehicles:
        warnings.append("No vehicles loaded")

    return warnings