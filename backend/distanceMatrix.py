# ============================================================
#  distanceMatrix.py  –  Build distance + duration matrix
#  Includes both DCs as nodes in the matrix
#  Usage: python distanceMatrix.py
# ============================================================

import requests
import pandas as pd
import numpy as np
import config

OSRM_URL = config.OSRM_URL


def build_matrix(store_file=None, matrix_file=None):
    store_file  = store_file  or config.STORE_FILE
    matrix_file = matrix_file or config.MATRIX_FILE

    print("Loading store data...")
    df = pd.read_excel(store_file, sheet_name=config.STORE_SHEET, dtype=str)
    df[config.COL_LAT] = pd.to_numeric(df[config.COL_LAT], errors='coerce')
    df[config.COL_LON] = pd.to_numeric(df[config.COL_LON], errors='coerce')
    df = df.dropna(subset=[config.COL_LAT, config.COL_LON])
    df[config.COL_STORE_ID] = df[config.COL_STORE_ID].astype(str).str.strip()

    # Normalize store IDs (strip leading zeros)
    def norm_id(x):
        try:
            return str(int(str(x).strip()))
        except Exception:
            return str(x).strip()

    df["_id"] = df[config.COL_STORE_ID].apply(norm_id)
    print(f"  Stores loaded: {len(df)}")

    # --- Build node list: DCs first, then stores ---
    nodes = []
    for dc_name, dc_coords in config.DEPOTS.items():
        nodes.append({
            "id" : dc_name,
            "lat": dc_coords["lat"],
            "lon": dc_coords["lon"],
            "type": "dc",
        })
    for _, row in df.iterrows():
        nodes.append({
            "id" : row["_id"],
            "lat": float(row[config.COL_LAT]),
            "lon": float(row[config.COL_LON]),
            "type": "store",
        })

    all_ids = [n["id"] for n in nodes]
    n_total = len(nodes)
    print(f"  Total nodes (DCs + stores): {n_total}")

    # --- Call OSRM table API ---
    coords_str = ";".join([f"{n['lon']},{n['lat']}" for n in nodes])
    url = f"{OSRM_URL}/table/v1/driving/{coords_str}?annotations=distance"

    print(f"  Calling OSRM for {n_total}×{n_total} matrix...")
    try:
        resp = requests.get(url, timeout=120)
        resp.raise_for_status()
        data = resp.json()
    except requests.exceptions.ConnectionError:
        print("\n  ERROR: OSRM server not running!")
        print("  Start it with:")
        print("    docker run -t -i -p 5000:5000 -v \"${PWD}:/data\" osrm/osrm-backend \\")
        print("      osrm-routed --algorithm mld --max-table-size 50000000 /data/mongolia-latest.osrm")
        return None

    if data.get("code") != "Ok":
        print(f"  OSRM error: {data.get('message', 'Unknown error')}")
        return None

    # duration_matrix = np.array(data["durations"])   # seconds
    distance_matrix = np.array(data["distances"])   # meters

    # Convert duration to minutes
    # duration_min = np.round(duration_matrix / 60, 2)

    # print(f"  Matrix shape: {duration_min.shape}")
    print(f"  Saving to {matrix_file}...")

    import os
    os.makedirs(os.path.dirname(matrix_file) if os.path.dirname(matrix_file) else ".", exist_ok=True)

    with pd.ExcelWriter(matrix_file, engine="openpyxl") as writer:
        # pd.DataFrame(duration_min,    index=all_ids, columns=all_ids).to_excel(
        #     writer, sheet_name=config.DURATION_SHEET)
        pd.DataFrame(distance_matrix, index=all_ids, columns=all_ids).to_excel(
            writer, sheet_name=config.DISTANCE_SHEET)

    print(f"  Done! Saved sheets: '{config.DISTANCE_SHEET}'")
    print(f"\n  DC rows in matrix:")
    # for dc_name in config.DEPOTS.keys():
    #     idx = all_ids.index(dc_name)
        # avg_dur = duration_min[idx, len(config.DEPOTS):].mean()
        # print(f"    {dc_name} (row/col {idx}) — avg duration to stores: {avg_dur:.1f} min")

    return matrix_file


if __name__ == "__main__":
    build_matrix()
