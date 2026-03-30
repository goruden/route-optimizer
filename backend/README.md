# VRP Route Optimization System

A production-grade Vehicle Routing Problem (VRP) solver for delivery logistics.
**Stack:** FastAPI · OR-Tools · OSRM · Leaflet

---

## Quick Start

### 1. Install dependencies
```bash
pip install -r requirements.txt
```

### 2. Start OSRM (Docker)
```bash
# One-time: download and process Mongolia map
wget https://download.geofabrik.de/asia/mongolia-latest.osm.pbf
docker run -t -v "${PWD}:/data" osrm/osrm-backend osrm-extract -p /opt/car.lua /data/mongolia-latest.osm.pbf
docker run -t -v "${PWD}:/data" osrm/osrm-backend osrm-partition /data/mongolia-latest.osrm
docker run -t -v "${PWD}:/data" osrm/osrm-backend osrm-customize /data/mongolia-latest.osrm

# Start OSRM (keep running)
docker run -d -p 5000:5000 -v "${PWD}:/data" osrm/osrm-backend \
  osrm-routed --algorithm mld --max-table-size 50000000 /data/mongolia-latest.osrm
```

### 3. Build distance matrix (first time)
```bash
# Option A: via script
cp data/stores.xlsx .
python distanceMatrix.py

# Option B: via API (after step 4)
# Upload your stores file in the web UI and click "Build Matrix via OSRM"
# This downloads matrix.xlsx — re-upload as the matrix file
```

### 4. Start the API
```bash
uvicorn main:app --host 0.0.0.0 --port 8067 --reload
```

### 5. Open the UI
Visit http://localhost:8067

---

## Data Format

### stores.xlsx

**Sheet: Stores**

| Column | Description |
|--------|-------------|
| Store ID | Unique store identifier |
| BIZLOC_ENG_NM | English name |
| BIZLOC_NM | Mongolian name |
| ADDR_1 | Address |
| DTL_ADDR | Detail address |
| LATITUDE | Latitude (decimal degrees) |
| LONGITUDE | Longitude (decimal degrees) |
| Sale start time | Opening time (HH:MM:SS) |
| SalesCloseTime | Closing time (HH:MM:SS) |
| Average Order CBM per day (DRY DC) | Daily dry volume demand (m³) |
| Average Order Weight per day (DRY DC) | Daily dry weight demand (kg) |
| Average Order CBM per day (COLD DC) | Daily cold volume demand (m³) |
| Average Order Weight per day (COLD DC) | Daily cold weight demand (kg) |

**Sheet: Vehicles**

| Column | Description |
|--------|-------------|
| Depot | "Dry DC" or "Cold DC" |
| Truck ID | Vehicle identifier |
| Description | Vehicle description |
| Capacity_kg | Weight capacity (kg) |
| Capacity_m3 | Volume capacity (m³) |
| Fuel cost per km | Fuel cost in ₮ per km |
| Vehicle cost per day | Daily fixed vehicle cost (₮) |
| Labor cost per day | Daily labor cost (₮) |

### matrix.xlsx

Built by `distanceMatrix.py` or the `/api/build-matrix` endpoint.

| Sheet | Content |
|-------|---------|
| Duration | NxN matrix of travel times in **minutes** |
| Distance | NxN matrix of distances in **metres** |

Rows/columns: `Dry DC`, `Cold DC`, then store IDs (normalized — no leading zeros).

---

## Configuration

Edit `config.py` to set:
- `DEPOTS` — actual depot coordinates
- `MAX_TRIPS_PER_VEHICLE` — default 3
- `SERVICE_TIME_SECONDS` — stop service time (default 600 = 10 min)
- `SHIFT_START_HOUR / SHIFT_END_HOUR` — working hours (default 06:00–22:00)
- `FAR_THRESHOLD_KM` — distance beyond which a store is flagged "too far"
- `MAX_SOLVER_TIME_SECONDS` — OR-Tools time budget

---

## API Reference

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/optimize` | POST | Run VRP optimization |
| `/api/export/{job_id}` | GET | Download Excel results |
| `/api/build-matrix` | POST | Build matrix from OSRM |
| `/api/health` | GET | Health check + OSRM status |
| `/docs` | GET | Interactive API docs |

### POST /api/optimize

**Form fields:**
- `store_file` — Excel file (Stores + Vehicles sheets)
- `matrix_file` — Matrix Excel file
- `mode` — `cheapest` | `fastest` | `shortest`
- `max_trips` — integer (default 3)
- `solver_time` — seconds (default 120)

---

## Architecture

```
User Upload
    │
    ▼
data_loader.py ──► Stores, Vehicles, Matrix DataFrames
    │
    ▼
solver.py
  ├── DRY fleet: Dry DC → stores with dry demand
  └── COLD fleet: Cold DC → stores with cold demand
  │   (OR-Tools CVRPTW, multiple trips as virtual vehicles)
    │
    ▼
osrm_client.py ──► Real road geometries per route
    │
    ▼
output_formatter.py
  ├── Route summary (per vehicle/trip)
  ├── Stop details (per stop with times)
  ├── Unserved stores + reasons
  └── Map data (Leaflet polylines)
    │
    ▼
FastAPI (main.py) ──► JSON response + Excel export
    │
    ▼
index.html ──► Map + Tables + Export
```

---

## Unserved Store Reasons

The system diagnoses why each store was not served:

| Reason | Meaning |
|--------|---------|
| `Demand (X kg) exceeds largest vehicle` | Store needs more than any vehicle can carry |
| `Too far from depot (X km)` | Beyond FAR_THRESHOLD_KM — rural grouping needed |
| `Time window too tight` | Cannot reach store before it closes |
| `Invalid time window` | Store open time ≥ close time |
| `Optimization decision` | Solver excluded for combined constraint reasons |
| `No DRY/COLD vehicles available` | No vehicles configured for this fleet |

---

## Optimization Modes

| Mode | Objective | Best for |
|------|-----------|----------|
| `cheapest` | Minimize total cost (fuel + fixed + labor) | Daily operations |
| `fastest` | Minimize total travel time | Time-critical deliveries |
| `shortest` | Minimize total distance | Fuel efficiency |

---

## Production Notes

- **Matrix size**: OSRM handles 600+ stores in one call. For >2000 stores, implement chunking.
- **Solver time**: Increase `solver_time` for better solutions (at cost of latency).
- **Multiple depots**: DRY and COLD fleets are solved independently — each store can receive from both.
- **Multi-trip**: Each vehicle can make up to `max_trips` return trips to its depot per shift.
- **Store with 0 demand**: Automatically skipped.
- **Time windows**: Stores open "00:00:00" to "23:59:59" are treated as "all day".
