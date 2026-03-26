# ============================================================
#  main.py  –  FastAPI + SQLite backend  v2
# ============================================================

import datetime, json, logging, os, uuid
from typing import Optional
import numpy as np


class _NumpySafeEncoder(json.JSONEncoder):
    """Converts numpy scalars to Python native types before JSON serialisation."""
    def default(self, obj):
        if isinstance(obj, np.integer):   return int(obj)
        if isinstance(obj, np.floating):  return float(obj)
        if isinstance(obj, np.bool_):     return bool(obj)
        if isinstance(obj, np.ndarray):   return obj.tolist()
        return super().default(obj)


def _dumps(obj) -> str:
    return json.dumps(obj, cls=_NumpySafeEncoder)

from fastapi import FastAPI, File, Form, HTTPException, UploadFile, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, Response, FileResponse
from sqlalchemy.orm import Session
from pydantic import BaseModel

import config, data_loader
import solver as vrp_solver
import output_formatter, osrm_client
from database import init_db, get_db, Dataset, Store, Vehicle, Job, JobResult, RunGroup

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s  %(levelname)-8s  %(message)s")
log = logging.getLogger(__name__)

init_db()

app = FastAPI(title="VRP Route Optimization System", version="2.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"],
                   allow_methods=["*"], allow_headers=["*"])


# ════════════════════════════════════════════════════════════
#  Pydantic schemas
# ════════════════════════════════════════════════════════════

class StoreUpdate(BaseModel):
    eng_name   : Optional[str]   = None
    mn_name    : Optional[str]   = None
    address    : Optional[str]   = None
    detail_addr: Optional[str]   = None
    lat        : Optional[float] = None
    lon        : Optional[float] = None
    open_s     : Optional[int]   = None
    close_s    : Optional[int]   = None
    dry_cbm    : Optional[float] = None
    dry_kg     : Optional[float] = None
    cold_cbm   : Optional[float] = None
    cold_kg    : Optional[float] = None

class StoreCreate(BaseModel):
    store_id   : str
    eng_name   : str   = ""
    mn_name    : str   = ""
    address    : str   = ""
    detail_addr: str   = ""
    lat        : float
    lon        : float
    open_s     : int   = 0
    close_s    : int   = 86399
    dry_cbm    : float = 0.0
    dry_kg     : float = 0.0
    cold_cbm   : float = 0.0
    cold_kg    : float = 0.0

class VehicleUpdate(BaseModel):
    description  : Optional[str]   = None
    depot        : Optional[str]   = None
    cap_kg       : Optional[float] = None
    cap_m3       : Optional[float] = None
    fuel_cost_km : Optional[float] = None
    vehicle_cost : Optional[float] = None
    labor_cost   : Optional[float] = None

class VehicleCreate(BaseModel):
    truck_id     : str
    description  : str   = ""
    depot        : str
    cap_kg       : float
    cap_m3       : float
    fuel_cost_km : float = 0.0
    vehicle_cost : float = 0.0
    labor_cost   : float = 0.0


# ════════════════════════════════════════════════════════════
#  Health
# ════════════════════════════════════════════════════════════

@app.get("/api/health")
def health():
    osrm_ok = False
    try:
        import requests as rq
        r = rq.get(f"{config.OSRM_URL}/route/v1/driving/106.9,47.9;106.9,47.91",
                   timeout=3)
        osrm_ok = r.status_code == 200
    except Exception:
        pass
    return {"status": "ok", "osrm": "connected" if osrm_ok else "unreachable",
            "osrm_url": config.OSRM_URL, "version": "2.0.0"}


# ════════════════════════════════════════════════════════════
#  Dataset CRUD
# ════════════════════════════════════════════════════════════

@app.get("/api/datasets")
def list_datasets(db: Session = Depends(get_db)):
    rows = db.query(Dataset).order_by(Dataset.created_at.desc()).all()
    return [{"id": d.id, "name": d.name,
             "created_at"   : d.created_at.isoformat(),
             "store_count"  : len(d.stores),
             "vehicle_count": len(d.vehicles),
             "has_matrix"   : d.matrix_bytes is not None}
            for d in rows]


@app.post("/api/datasets")
async def create_dataset(
    name        : str                    = Form(...),
    store_file  : UploadFile             = File(...),
    matrix_file : Optional[UploadFile]   = File(None),
    db          : Session                = Depends(get_db),
):
    store_bytes  = await store_file.read()
    matrix_bytes = await matrix_file.read() if matrix_file else None

    try:
        stores_list   = data_loader.load_stores(store_bytes)
        vehicles_list = data_loader.load_vehicles(store_bytes)
    except Exception as e:
        raise HTTPException(422, f"Parse error: {e}")

    ds = Dataset(name=name, matrix_bytes=matrix_bytes)
    db.add(ds)
    db.flush()

    for s in stores_list:
        db.add(Store(dataset_id=ds.id, **{
            k: s[k] for k in ["store_id","node_id","eng_name","mn_name",
                               "address","detail_addr","lat","lon","open_s",
                               "close_s","dry_cbm","dry_kg","cold_cbm",
                               "cold_kg","has_dry","has_cold"]
        }))
    for v in vehicles_list:
        db.add(Vehicle(dataset_id=ds.id, **{
            k: v[k] for k in ["truck_id","description","depot","fleet",
                               "cap_kg","cap_m3","fuel_cost_km",
                               "vehicle_cost","labor_cost"]
        }))

    db.commit()
    return {"id": ds.id, "name": ds.name,
            "store_count": len(stores_list),
            "vehicle_count": len(vehicles_list)}


@app.delete("/api/datasets/{dataset_id}")
def delete_dataset(dataset_id: int, db: Session = Depends(get_db)):
    ds = db.get(Dataset, dataset_id)
    if not ds:
        raise HTTPException(404, "Dataset not found")
    # Unlink jobs referencing this dataset before deleting
    # (avoids foreign-key constraint; jobs are preserved in history)
    db.query(Job).filter(Job.dataset_id == dataset_id).update({"dataset_id": None})
    db.commit()
    db.delete(ds)
    db.commit()
    return {"ok": True}


@app.post("/api/datasets/{dataset_id}/matrix")
async def upload_matrix_to_dataset(
    dataset_id  : int,
    matrix_file : UploadFile = File(...),
    db          : Session    = Depends(get_db),
):
    ds = db.get(Dataset, dataset_id)
    if not ds:
        raise HTTPException(404, "Dataset not found")
    ds.matrix_bytes = await matrix_file.read()
    db.commit()
    return {"ok": True, "dataset_id": dataset_id}


# ════════════════════════════════════════════════════════════
#  Store CRUD
# ════════════════════════════════════════════════════════════

@app.get("/api/datasets/{dataset_id}/stores")
def list_stores(dataset_id: int, db: Session = Depends(get_db)):
    return [s.to_dict() for s in
            db.query(Store).filter(Store.dataset_id == dataset_id).all()]


@app.post("/api/datasets/{dataset_id}/stores")
def add_store(dataset_id: int, body: StoreCreate, db: Session = Depends(get_db)):
    if not db.get(Dataset, dataset_id):
        raise HTTPException(404, "Dataset not found")
    def norm(x):
        try: return str(int(str(x).strip()))
        except: return str(x).strip()
    s = Store(
        dataset_id=dataset_id, store_id=body.store_id,
        node_id=norm(body.store_id), eng_name=body.eng_name,
        mn_name=body.mn_name, address=body.address,
        detail_addr=body.detail_addr, lat=body.lat, lon=body.lon,
        open_s=body.open_s, close_s=body.close_s,
        dry_cbm=body.dry_cbm, dry_kg=body.dry_kg,
        cold_cbm=body.cold_cbm, cold_kg=body.cold_kg,
        has_dry=body.dry_kg>0 or body.dry_cbm>0,
        has_cold=body.cold_kg>0 or body.cold_cbm>0,
    )
    db.add(s); db.commit(); db.refresh(s)
    return s.to_dict()


@app.put("/api/datasets/{dataset_id}/stores/{sid}")
def update_store(dataset_id: int, sid: int, body: StoreUpdate,
                 db: Session = Depends(get_db)):
    s = db.query(Store).filter(Store.id==sid, Store.dataset_id==dataset_id).first()
    if not s:
        raise HTTPException(404, "Store not found")
    for f, v in body.model_dump(exclude_none=True).items():
        setattr(s, f, v)
    s.has_dry  = (s.dry_kg or 0)>0 or (s.dry_cbm or 0)>0
    s.has_cold = (s.cold_kg or 0)>0 or (s.cold_cbm or 0)>0
    db.commit(); db.refresh(s)
    return s.to_dict()


@app.delete("/api/datasets/{dataset_id}/stores/{sid}")
def delete_store(dataset_id: int, sid: int, db: Session = Depends(get_db)):
    s = db.query(Store).filter(Store.id==sid, Store.dataset_id==dataset_id).first()
    if not s:
        raise HTTPException(404, "Store not found")
    db.delete(s); db.commit()
    return {"ok": True}


@app.delete("/api/datasets/{dataset_id}/stores")
def clear_stores(dataset_id: int, db: Session = Depends(get_db)):
    db.query(Store).filter(Store.dataset_id==dataset_id).delete()
    db.commit()
    return {"ok": True}


# ════════════════════════════════════════════════════════════
#  Vehicle CRUD
# ════════════════════════════════════════════════════════════

@app.get("/api/datasets/{dataset_id}/vehicles")
def list_vehicles(dataset_id: int, db: Session = Depends(get_db)):
    return [v.to_dict() for v in
            db.query(Vehicle).filter(Vehicle.dataset_id==dataset_id).all()]


@app.post("/api/datasets/{dataset_id}/vehicles")
def add_vehicle(dataset_id: int, body: VehicleCreate, db: Session = Depends(get_db)):
    if not db.get(Dataset, dataset_id):
        raise HTTPException(404, "Dataset not found")
    fleet = config.DEPOT_VEHICLE_MAP.get(body.depot, "DRY")
    v = Vehicle(
        dataset_id=dataset_id, truck_id=body.truck_id,
        description=body.description, depot=body.depot, fleet=fleet,
        cap_kg=body.cap_kg, cap_m3=body.cap_m3,
        fuel_cost_km=body.fuel_cost_km,
        vehicle_cost=body.vehicle_cost, labor_cost=body.labor_cost,
    )
    db.add(v); db.commit(); db.refresh(v)
    return v.to_dict()


@app.put("/api/datasets/{dataset_id}/vehicles/{vid}")
def update_vehicle(dataset_id: int, vid: int, body: VehicleUpdate,
                   db: Session = Depends(get_db)):
    v = db.query(Vehicle).filter(Vehicle.id==vid, Vehicle.dataset_id==dataset_id).first()
    if not v:
        raise HTTPException(404, "Vehicle not found")
    for f, val in body.model_dump(exclude_none=True).items():
        setattr(v, f, val)
    if v.depot:
        v.fleet = config.DEPOT_VEHICLE_MAP.get(v.depot, "DRY")
    db.commit(); db.refresh(v)
    return v.to_dict()


@app.delete("/api/datasets/{dataset_id}/vehicles/{vid}")
def delete_vehicle(dataset_id: int, vid: int, db: Session = Depends(get_db)):
    v = db.query(Vehicle).filter(Vehicle.id==vid, Vehicle.dataset_id==dataset_id).first()
    if not v:
        raise HTTPException(404, "Vehicle not found")
    db.delete(v); db.commit()
    return {"ok": True}


@app.delete("/api/datasets/{dataset_id}/vehicles")
def clear_vehicles(dataset_id: int, db: Session = Depends(get_db)):
    db.query(Vehicle).filter(Vehicle.dataset_id==dataset_id).delete()
    db.commit()
    return {"ok": True}


# ════════════════════════════════════════════════════════════
#  Optimize  (DB-backed — results never expire)
# ════════════════════════════════════════════════════════════

@app.post("/api/optimize")
async def optimize(
    dataset_id        : Optional[int]          = Form(None),
    store_file        : Optional[UploadFile]   = File(None),
    matrix_file       : Optional[UploadFile]   = File(None),
    mode              : str                    = Form("cheapest"),
    max_trips         : int                    = Form(3),
    solver_time       : int                    = Form(120),
    rural_solver_time : Optional[int]          = Form(None),
    group_id          : Optional[str]          = Form(None),
    version_name      : Optional[str]          = Form(None),
    max_weight_fill   : float                  = Form(1.0),
    max_volume_fill   : float                  = Form(1.0),
    db                : Session                = Depends(get_db),
):
    if mode not in ("fastest","shortest","cheapest","balanced","geographic"):
        raise HTTPException(400, f"Invalid mode '{mode}'")
    
    # Validate fill percentages (0.0 to 1.0)
    if not (0.0 <= max_weight_fill <= 1.0):
        raise HTTPException(400, "max_weight_fill must be between 0.0 and 1.0")
    if not (0.0 <= max_volume_fill <= 1.0):
        raise HTTPException(400, "max_volume_fill must be between 0.0 and 1.0")

    # ── Resolve data ─────────────────────────────────────────
    if dataset_id:
        ds = db.get(Dataset, dataset_id)
        if not ds:
            raise HTTPException(404, "Dataset not found")
        stores_list   = [s.to_solver_dict() for s in ds.stores]
        vehicles_list = [v.to_solver_dict() for v in ds.vehicles]
        matrix_bytes  = ds.matrix_bytes
        if not matrix_bytes:
            raise HTTPException(422, "No matrix for this dataset — upload it first")
    elif store_file and matrix_file:
        sb = await store_file.read()
        mb = await matrix_file.read()
        stores_list   = data_loader.load_stores(sb)
        vehicles_list = data_loader.load_vehicles(sb)
        matrix_bytes  = mb
        dataset_id    = None
    else:
        raise HTTPException(422, "Provide dataset_id OR both store_file+matrix_file")

    try:
        dist_df, dur_df = data_loader.load_matrix(matrix_bytes)
    except Exception as e:
        raise HTTPException(422, f"Matrix error: {e}")

    warnings = data_loader.validate_data(stores_list, vehicles_list, dist_df, dur_df)

    # ── Create Job record ─────────────────────────────────────
    job_id = str(uuid.uuid4())
    # Resolve version name: explicit > auto-increment within group
    if not version_name and group_id:
        existing = db.query(Job).filter(Job.group_id == group_id).count()
        version_name = f"Auto v{existing + 1}"
    elif not version_name:
        version_name = "Auto v1"

    job = Job(id=job_id, dataset_id=dataset_id, mode=mode,
              max_trips=max_trips, solver_time=solver_time,
              rural_solver_time=rural_solver_time,
              group_id=group_id, version_name=version_name,
              status="running")
    db.add(job); db.commit()

    # ── Solve ─────────────────────────────────────────────────
    config.MAX_TRIPS_PER_VEHICLE   = max_trips
    config.MAX_SOLVER_TIME_SECONDS = solver_time
    
    # Set capacity fill percentages
    config.MAX_WEIGHT_FILL_PERCENTAGE = max_weight_fill
    config.MAX_VOLUME_FILL_PERCENTAGE = max_volume_fill
    
    # Set rural solver time if provided, otherwise use default
    if rural_solver_time is not None:
        config.RURAL_SOLVER_TIME_SECONDS = rural_solver_time

    try:
        result = vrp_solver.solve(stores_list, vehicles_list, dist_df, dur_df, mode=mode)
    except Exception as e:
        job.status = "error"; job.error_msg = str(e); db.commit()
        raise HTTPException(500, f"Solver error: {e}")

    # ── OSRM geometries ───────────────────────────────────────
    route_geometries = {}
    try:
        wps_map = {}
        for fleet, fr in result.items():
            for route in fr["routes"]:
                dc  = config.DEPOTS["Dry DC"] if fleet=="DRY" else config.DEPOTS["Cold DC"]
                wps = [(dc["lat"], dc["lon"])]
                for stop in route["stops"]:
                    wps.append((stop["lat"], stop["lon"]))
                wps.append((dc["lat"], dc["lon"]))
                wps_map[route["virtual_id"]] = wps
        route_geometries = osrm_client.get_route_geometries_batch(wps_map)
    except Exception as e:
        log.warning(f"OSRM geometry skipped: {e}")

    # ── Outputs ───────────────────────────────────────────────
    route_summary = output_formatter.build_route_summary(result)
    stop_details  = output_formatter.build_stop_details(result)
    unserved      = output_formatter.build_unserved(result, dist_df)
    map_data      = output_formatter.build_map_data(result, route_geometries)

    served   = sum(len(r["stops"]) for fr in result.values() for r in fr["routes"])
    unserved_n = sum(len(fr["unserved"]) for fr in result.values())
    total_cost   = sum(r["cost_total"]  for r in route_summary)
    total_dist   = sum(r["distance_km"] for r in route_summary)
    total_man_hours = sum(r.get("man_hours", 0) for r in route_summary)

    summary = {
        "mode": mode, "total_stores": len(stores_list),
        "total_served": served, "total_unserved": unserved_n,
        "total_routes": len(route_summary),
        "total_dist_km": round(total_dist, 1),
        "total_cost": round(total_cost, 0),
        "total_man_hours": round(total_man_hours, 1),
        "warnings": warnings,
    }

    excel_bytes = output_formatter.export_to_excel(route_summary, stop_details, unserved)

    db.add(JobResult(
        job_id        = job_id,
        summary_json  = _dumps(summary),
        routes_json   = _dumps(route_summary),
        stops_json    = _dumps(stop_details),
        unserved_json = _dumps(unserved),
        map_data_json = _dumps(map_data),
        excel_bytes   = excel_bytes,
    ))
    job.status       = "done"
    job.completed_at = datetime.datetime.utcnow()
    db.commit()

    log.info(f"Job {job_id[:8]} done — {served} served, {unserved_n} unserved, mode={mode}")

    return {"job_id": job_id, "summary": summary,
            "route_summary": route_summary, "stop_details": stop_details,
            "unserved": unserved, "map_data": map_data}


class ManualJobCreate(BaseModel):
    title: str
    routes: list[dict]
    is_manual: bool
    dataset_id: int


# ════════════════════════════════════════════════════════════
#  Manual Job Creation
# ════════════════════════════════════════════════════════════

@app.post("/api/jobs/manual")
def create_manual_job(body: ManualJobCreate, db: Session = Depends(get_db)):
    """
    Create a manual job from user-defined routes.
    Fixes: real fleet timing, actual store demand, proper OSRM polylines,
           unserved store computation, per-vehicle trip numbering.
    """
    ds = db.get(Dataset, body.dataset_id)
    if not ds:
        raise HTTPException(404, "Dataset not found")
    if not ds.matrix_bytes:
        raise HTTPException(422, "Dataset must have a distance matrix for manual routing")

    for route in body.routes:
        if not route.get("vehicle_id"):
            raise HTTPException(400, "All routes must have a vehicle_id")
        if not route.get("stops") or len(route["stops"]) == 0:
            raise HTTPException(400, "All routes must have at least one stop")

    try:
        dist_df, dur_df = data_loader.load_matrix(ds.matrix_bytes)
    except Exception as e:
        raise HTTPException(422, f"Matrix error: {e}")

    stores_dict   = {s.store_id: s.to_dict() for s in ds.stores}
    vehicles_dict = {v.truck_id: v.to_dict() for v in ds.vehicles}

    # Create job record
    job_id = str(uuid.uuid4())
    job = Job(
        id=job_id, dataset_id=body.dataset_id,
        version_name=body.title, is_manual=True,
        mode="manual", max_trips=1, solver_time=0,
        status="done",
        created_at=datetime.datetime.utcnow(),
        completed_at=datetime.datetime.utcnow(),
    )
    db.add(job)
    db.flush()

    route_summary:  list = []
    stop_details:   list = []
    wps_map:        dict = {}
    served_dry:     set  = set()
    served_cold:    set  = set()
    total_cost      = 0.0
    total_dist_km   = 0.0
    trip_counter:   dict = {}   # truck_id → trip number

    COLORS = [
        "#5B7CFA","#22D3EE","#34D399","#A78BFA","#F472B6",
        "#38BDF8","#4ADE80","#818CF8","#FB7185","#2DD4BF",
        "#F97316","#EAB308","#84CC16","#DC2626","#D97706",
    ]

    def fmt_wall(s: float) -> str:
        h = int(s // 3600) % 24
        m = int((s % 3600) // 60)
        return f"{h:02d}:{m:02d}"

    def matrix_lookup(from_id: str, to_id: str):
        """Return (dist_m, dur_s). dur_df is in minutes → convert to seconds."""
        if from_id in dist_df.index and to_id in dist_df.columns:
            dist = float(dist_df.loc[from_id, to_id])
            dur  = float(dur_df.loc[from_id, to_id]) * 60.0   # minutes → seconds
            return dist, dur
        return 0.0, 0.0

    for route_idx, route in enumerate(body.routes):
        vehicle_id = route["vehicle_id"]
        stop_ids   = [s.strip() for s in route.get("stops", []) if str(s).strip()]
        route_name = route.get("route_name", f"Route {route_idx + 1}")

        vehicle = vehicles_dict.get(vehicle_id)
        if not vehicle:
            raise HTTPException(400, f"Vehicle {vehicle_id} not found in dataset")

        fleet      = vehicle["fleet"]
        sched      = config.FLEET_SCHEDULE.get(fleet, {"start_hour": 8, "max_horizon_hour": 20})
        start_wall = float(sched["start_hour"] * 3600)   # seconds since midnight

        # Per-vehicle trip numbering (supports multi-trip if same truck listed twice)
        trip_counter[vehicle_id] = trip_counter.get(vehicle_id, 0) + 1
        trip_num = trip_counter[vehicle_id]

        depot_name = "Dry DC" if fleet == "DRY" else "Cold DC"
        depot_cfg  = config.DEPOTS[depot_name]

        # Build node sequence using depot_name (matches matrix key) + store node_id
        valid_stop_ids = [sid for sid in stop_ids if sid in stores_dict]
        node_seq = [depot_name] + [stores_dict[sid]["node_id"] for sid in valid_stop_ids] + [depot_name]

        leg_dists: list = []
        leg_durs:  list = []
        for i in range(len(node_seq) - 1):
            dm, ds_ = matrix_lookup(node_seq[i], node_seq[i + 1])
            leg_dists.append(dm)
            leg_durs.append(ds_)

        total_route_dist = sum(leg_dists)

        # Timing + per-stop data
        current_wall  = start_wall
        route_load_kg = 0.0
        route_load_m3 = 0.0
        route_stops:  list = []

        for si, sid in enumerate(valid_stop_ids):
            store     = stores_dict[sid]
            travel_s  = leg_durs[si] if si < len(leg_durs) else 0.0
            arr_wall  = current_wall + travel_s

            demand_kg = float(store["dry_kg"]  if fleet == "DRY" else store["cold_kg"])
            demand_m3 = float(store["dry_cbm"] if fleet == "DRY" else store["cold_cbm"])
            route_load_kg += demand_kg
            route_load_m3 += demand_m3

            dep_wall     = arr_wall + config.SERVICE_TIME_SECONDS
            current_wall = dep_wall
            day_num      = 1 + int(arr_wall // 86400)

            route_stops.append({
                "fleet"       : fleet,
                "truck_id"    : vehicle_id,
                "trip_number" : trip_num,
                "stop_order"  : si + 1,
                "store_id"    : sid,
                "eng_name"    : store.get("eng_name", ""),
                "mn_name"     : store.get("mn_name",  ""),
                "address"     : store.get("address",  ""),
                "detail_addr" : store.get("detail_addr", ""),
                "lat"         : store["lat"],
                "lon"         : store["lon"],
                "arrival"     : fmt_wall(arr_wall),
                "departure"   : fmt_wall(dep_wall),
                "delivery_day": "Same day" if day_num <= 1 else f"Day {day_num}",
                "is_rural"    : False,
                "demand_kg"   : round(demand_kg, 2),
                "demand_m3"   : round(demand_m3, 3),
            })

            if fleet == "DRY":   served_dry.add(sid)
            else:                served_cold.add(sid)

        # Return to depot
        return_dur  = leg_durs[-1] if leg_durs else 0.0
        return_wall = current_wall + return_dur

        # Costs
        dist_km    = total_route_dist / 1000.0
        fuel_cost  = dist_km * float(vehicle["fuel_cost_km"])
        route_cost = fuel_cost + float(vehicle["vehicle_cost"]) + float(vehicle["labor_cost"])
        total_cost     += route_cost
        total_dist_km  += dist_km
        cap_kg = float(vehicle["cap_kg"])
        cap_m3 = float(vehicle["cap_m3"])

        route_summary.append({
            "fleet"       : fleet,
            "truck_id"    : vehicle_id,
            "trip_number" : trip_num,
            "route_type"  : "manual",
            "stops"       : len(valid_stop_ids),
            "distance_km" : round(dist_km, 1),
            "duration_min": round((return_wall - start_wall) / 60.0, 1),
            "load_kg"     : round(route_load_kg, 2),
            "cap_kg"      : cap_kg,
            "util_kg_pct" : round(route_load_kg / cap_kg * 100, 1) if cap_kg else 0.0,
            "load_m3"     : round(route_load_m3, 3),
            "cap_m3"      : cap_m3,
            "util_m3_pct" : round(route_load_m3 / cap_m3 * 100, 1) if cap_m3 else 0.0,
            "cost_fuel"   : round(fuel_cost, 0),
            "cost_fixed"  : float(vehicle["vehicle_cost"]),
            "cost_labor"  : float(vehicle["labor_cost"]),
            "cost_total"  : round(route_cost, 0),
            "departs_at"  : fmt_wall(start_wall),
            "returns_at"  : fmt_wall(return_wall),
            "is_overnight": return_wall >= 86400,
            "man_hours"   : round((return_wall - start_wall) / 3600.0, 2),
        })
        stop_details.extend(route_stops)

        # OSRM waypoints (lat, lon tuples)
        osrm_wps = [(depot_cfg["lat"], depot_cfg["lon"])]
        for sid in valid_stop_ids:
            st = stores_dict[sid]
            osrm_wps.append((st["lat"], st["lon"]))
        osrm_wps.append((depot_cfg["lat"], depot_cfg["lon"]))
        wps_map[f"{vehicle_id}_T{trip_num}"] = osrm_wps

    # ── OSRM geometries (batch) ───────────────────────────────
    raw_geometries: dict = {}
    try:
        raw_geometries = osrm_client.get_route_geometries_batch(wps_map)
    except Exception as e:
        log.warning(f"OSRM geometry batch failed: {e}")

    # ── Build map_data ────────────────────────────────────────
    map_data: list = []
    color_counters = {"DRY": 0, "COLD": 8}

    for rs in route_summary:
        vid      = rs["truck_id"]
        tnum     = rs["trip_number"]
        fleet    = rs["fleet"]
        rid      = f"{vid}_T{tnum}"
        dc_name  = "Dry DC" if fleet == "DRY" else "Cold DC"
        dc_cfg   = config.DEPOTS[dc_name]

        color_idx = color_counters[fleet] % len(COLORS)
        color_counters[fleet] += 1
        color = COLORS[color_idx]

        route_stop_objs = [
            s for s in stop_details
            if s["truck_id"] == vid and s["trip_number"] == tnum
        ]
        route_stop_objs.sort(key=lambda x: x["stop_order"])

        map_stops = [{
            "lat"        : s["lat"],
            "lon"        : s["lon"],
            "order"      : s["stop_order"],
            "store_id"   : s["store_id"],
            "name"       : s["eng_name"],
            "mn_name"    : s["mn_name"],
            "arrival"    : s["arrival"],
            "day_label"  : "" if s["delivery_day"] == "Same day" else s["delivery_day"],
            "is_rural"   : False,
            "is_next_day": s["delivery_day"] != "Same day",
            "demand_kg"  : s["demand_kg"],
            "demand_m3"  : s["demand_m3"],
        } for s in route_stop_objs]

        # OSRM gives [lon, lat]; Leaflet needs [lat, lon] — flip here
        raw_geo = raw_geometries.get(rid)
        if raw_geo:
            polyline = [[pt[1], pt[0]] for pt in raw_geo]
        else:
            # Straight-line fallback: [lat, lon] list
            polyline = [[dc_cfg["lat"], dc_cfg["lon"]]]
            for ms in map_stops:
                polyline.append([ms["lat"], ms["lon"]])
            polyline.append([dc_cfg["lat"], dc_cfg["lon"]])

        map_data.append({
            "route_id"  : rid,
            "fleet"     : fleet,
            "truck_id"  : vid,
            "trip_number": tnum,
            "is_rural"  : False,
            "color"     : color,
            "line_style": "solid",
            "stops"     : map_stops,
            "polyline"  : polyline,
            "depot_lat" : dc_cfg["lat"],
            "depot_lon" : dc_cfg["lon"],
            "sched_info": f"Manual · Departs {rs['departs_at']} · Returns {rs['returns_at']}",
            "summary"   : {
                "distance_km" : rs["distance_km"],
                "duration_min": rs["duration_min"],
                "load_kg"     : rs["load_kg"],
                "load_m3"     : rs["load_m3"],
                "return_at"   : rs["returns_at"],
                "is_overnight": rs["is_overnight"],
            },
        })

    # ── Unserved stores ───────────────────────────────────────
    unserved: list = []
    for store in ds.stores:
        if store.has_dry and store.store_id not in served_dry:
            unserved.append({
                "fleet"     : "DRY",
                "store_id"  : store.store_id,
                "eng_name"  : store.eng_name or "",
                "mn_name"   : store.mn_name  or "",
                "address"   : store.address  or "",
                "lat"       : store.lat,
                "lon"       : store.lon,
                "demand_kg" : round(float(store.dry_kg),  2),
                "demand_m3" : round(float(store.dry_cbm), 3),
                "reason"    : "Not assigned to any route in manual plan.",
            })
        if store.has_cold and store.store_id not in served_cold:
            unserved.append({
                "fleet"     : "COLD",
                "store_id"  : store.store_id,
                "eng_name"  : store.eng_name or "",
                "mn_name"   : store.mn_name  or "",
                "address"   : store.address  or "",
                "lat"       : store.lat,
                "lon"       : store.lon,
                "demand_kg" : round(float(store.cold_kg),  2),
                "demand_m3" : round(float(store.cold_cbm), 3),
                "reason"    : "Not assigned to any route in manual plan.",
            })

    total_served = sum(r["stops"] for r in route_summary)
    summary = {
        "mode"           : "manual",
        "total_stores"   : len(stores_dict),
        "total_served"   : total_served,
        "total_unserved" : len(unserved),
        "total_routes"   : len(route_summary),
        "total_dist_km"  : round(total_dist_km, 1),
        "total_cost"     : round(total_cost, 0),
        "total_man_hours": round(sum(r.get("man_hours", 0) for r in route_summary), 1),
        "warnings"       : [],
    }

    try:
        excel_bytes = output_formatter.export_to_excel(route_summary, stop_details, unserved)
    except Exception as e:
        log.warning(f"Excel generation failed for manual job {job_id}: {e}")
        excel_bytes = b""

    db.add(JobResult(
        job_id        = job_id,
        summary_json  = _dumps(summary),
        routes_json   = _dumps(route_summary),
        stops_json    = _dumps(stop_details),
        unserved_json = _dumps(unserved),
        map_data_json = _dumps(map_data),
        excel_bytes   = excel_bytes,
    ))
    db.commit()

    log.info(
        f"Manual job {job_id[:8]} done — "
        f"{total_served} served, {len(unserved)} unserved, "
        f"{len(route_summary)} routes, {total_dist_km:.1f}km"
    )
    return job.to_dict()


@app.get("/api/jobs")
def list_jobs(limit: int = 30, db: Session = Depends(get_db)):
    jobs = db.query(Job).order_by(Job.created_at.desc()).limit(limit).all()
    out  = []
    for j in jobs:
        d = j.to_dict()
        if j.result:
            s = j.result.get_summary()
            d["total_served"]   = s.get("total_served")   if s else None
            d["total_unserved"] = s.get("total_unserved") if s else None
            d["total_routes"]   = s.get("total_routes")   if s else None
            d["total_cost"]     = s.get("total_cost")     if s else None
            d["total_man_hours"]= s.get("total_man_hours") if s else None
        out.append(d)
    return out


@app.get("/api/jobs/{job_id}")
def get_job_result(job_id: str, db: Session = Depends(get_db)):
    job = db.get(Job, job_id)
    if not job:
        raise HTTPException(404, "Job not found")
    info = job.to_dict()
    if job.result:
        info.update({
            "summary"      : job.result.get_summary(),
            "route_summary": job.result.get_routes(),
            "stop_details" : job.result.get_stops(),
            "unserved"     : job.result.get_unserved(),
            "map_data"     : job.result.get_map_data(),
        })
    return info


@app.delete("/api/jobs/{job_id}")
def delete_job(job_id: str, db: Session = Depends(get_db)):
    job = db.get(Job, job_id)
    if not job:
        raise HTTPException(404, "Job not found")
    db.delete(job); db.commit()
    return {"ok": True}


# ════════════════════════════════════════════════════════════
#  Excel export  (persistent — no expiry)
# ════════════════════════════════════════════════════════════

@app.get("/api/export/{job_id}")
def export_excel(job_id: str, db: Session = Depends(get_db)):
    job = db.get(Job, job_id)
    if not job or not job.result or not job.result.excel_bytes:
        raise HTTPException(404, "Job result not found. Run optimization first.")
    return Response(
        content = job.result.excel_bytes,
        media_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers = {"Content-Disposition":
                   f'attachment; filename="vrp_{job_id[:8]}_{job.mode}.xlsx"'},
    )


# ════════════════════════════════════════════════════════════
#  Build Matrix
# ════════════════════════════════════════════════════════════

@app.post("/api/build-matrix")
async def build_matrix_endpoint(
    dataset_id      : Optional[int]          = Form(None),
    store_file      : Optional[UploadFile]   = File(None),
    matrix_file     : Optional[UploadFile]   = File(None),
    save_to_dataset : bool                   = Form(True),
    db              : Session                = Depends(get_db),
):
    if dataset_id:
        ds          = db.get(Dataset, dataset_id)
        if not ds:
            raise HTTPException(404, "Dataset not found")
        stores_list = [s.to_solver_dict() for s in ds.stores]
    elif store_file:
        stores_list = data_loader.load_stores(await store_file.read())
        dataset_id  = None
    else:
        raise HTTPException(422, "Provide dataset_id or store_file")

    # If matrix file is uploaded, use it directly
    if matrix_file:
        matrix_bytes = await matrix_file.read()
        
        # Validate matrix file format
        try:
            import pandas as pd, io as _io
            df = pd.read_excel(_io.BytesIO(matrix_bytes), sheet_name=None)
            if config.DURATION_SHEET not in df or config.DISTANCE_SHEET not in df:
                raise HTTPException(400, f"Matrix file must contain '{config.DURATION_SHEET}' and '{config.DISTANCE_SHEET}' sheets")
        except Exception as e:
            raise HTTPException(400, f"Invalid matrix file: {str(e)}")
        
        # Save to dataset if requested
        if dataset_id and save_to_dataset:
            ds = db.get(Dataset, dataset_id)
            if ds:
                ds.matrix_bytes = matrix_bytes
                db.commit()
        
        return Response(
            content    = matrix_bytes,
            media_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers    = {"Content-Disposition": 'attachment; filename="matrix.xlsx"'},
        )
    
    # Otherwise, build matrix via OSRM
    coords, ids = [], []
    for dc_name, dc in config.DEPOTS.items():
        coords.append((dc["lat"], dc["lon"])); ids.append(dc_name)
    for s in stores_list:
        coords.append((s["lat"], s["lon"])); ids.append(s["node_id"])

    try:
        dist_mat, dur_mat = osrm_client.build_matrix_from_osrm(coords)
    except ConnectionError as e:
        raise HTTPException(503, str(e))

    import pandas as pd, io as _io
    buf = _io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        pd.DataFrame((dur_mat/60).round(2), index=ids, columns=ids).to_excel(
            writer, sheet_name=config.DURATION_SHEET)
        pd.DataFrame(dist_mat, index=ids, columns=ids).to_excel(
            writer, sheet_name=config.DISTANCE_SHEET)
    buf.seek(0)
    matrix_bytes = buf.read()

    if dataset_id and save_to_dataset:
        ds = db.get(Dataset, dataset_id)
        if ds:
            ds.matrix_bytes = matrix_bytes
            db.commit()

    return Response(
        content    = matrix_bytes,
        media_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers    = {"Content-Disposition": 'attachment; filename="matrix.xlsx"'},
    )



# ════════════════════════════════════════════════════════════
#  Run Groups  (named version collections)
# ════════════════════════════════════════════════════════════

class RunGroupCreate(BaseModel):
    name      : str
    dataset_id: Optional[int] = None

class RunGroupRename(BaseModel):
    name: str


@app.get("/api/run-groups")
def list_run_groups(db: Session = Depends(get_db)):
    groups = db.query(RunGroup).order_by(RunGroup.created_at.desc()).all()
    out = []
    for g in groups:
        gd = g.to_dict()
        gd["jobs"] = []
        for j in g.jobs:
            jd = j.to_dict()
            if j.result:
                s = j.result.get_summary()
                jd["total_served"]    = s.get("total_served")    if s else None
                jd["total_unserved"]  = s.get("total_unserved")  if s else None
                jd["total_routes"]    = s.get("total_routes")    if s else None
                jd["total_cost"]      = s.get("total_cost")      if s else None
                jd["total_man_hours"] = s.get("total_man_hours") if s else None
            gd["jobs"].append(jd)
        out.append(gd)
    return out


@app.post("/api/run-groups")
def create_run_group(body: RunGroupCreate, db: Session = Depends(get_db)):
    import uuid as _uuid
    g = RunGroup(id=str(_uuid.uuid4()), name=body.name, dataset_id=body.dataset_id)
    db.add(g); db.commit(); db.refresh(g)
    return g.to_dict()


@app.patch("/api/run-groups/{group_id}")
def rename_run_group(group_id: str, body: RunGroupRename, db: Session = Depends(get_db)):
    g = db.get(RunGroup, group_id)
    if not g: raise HTTPException(404, "Group not found")
    g.name = body.name
    db.commit(); db.refresh(g)
    return g.to_dict()


@app.delete("/api/run-groups/{group_id}")
def delete_run_group(group_id: str, db: Session = Depends(get_db)):
    g = db.get(RunGroup, group_id)
    if not g: raise HTTPException(404, "Group not found")
    # Unlink jobs rather than delete them
    for j in g.jobs:
        j.group_id = None
    db.delete(g); db.commit()
    return {"ok": True}


# ── Per-job version management ────────────────────────────────

class JobVersionPatch(BaseModel):
    version_name: Optional[str] = None
    group_id    : Optional[str] = None


@app.patch("/api/jobs/{job_id}/version")
def patch_job_version(job_id: str, body: JobVersionPatch, db: Session = Depends(get_db)):
    job = db.get(Job, job_id)
    if not job: raise HTTPException(404, "Job not found")
    if body.version_name is not None: job.version_name = body.version_name
    if body.group_id     is not None: job.group_id     = body.group_id
    db.commit(); db.refresh(job)
    return job.to_dict()


@app.post("/api/jobs/{job_id}/fork")
def fork_job(job_id: str, db: Session = Depends(get_db)):
    """
    Clone a job result as a new manual-edit version in the same group.
    Returns the new job id so the frontend can load and edit it.
    """
    import uuid as _uuid
    src = db.get(Job, job_id)
    if not src or not src.result: raise HTTPException(404, "Source job/result not found")

    new_id = str(_uuid.uuid4())
    # Count existing versions in group for label
    sibling_count = 0
    if src.group_id:
        sibling_count = db.query(Job).filter(Job.group_id == src.group_id).count()

    new_job = Job(
        id            = new_id,
        dataset_id    = src.dataset_id,
        group_id      = src.group_id,
        version_name  = f"Manual v{sibling_count + 1}",
        is_manual     = True,
        mode          = src.mode,
        max_trips     = src.max_trips,
        solver_time   = src.solver_time,
        status        = "done",
        created_at    = datetime.datetime.utcnow(),
        completed_at  = datetime.datetime.utcnow(),
    )
    db.add(new_job)
    db.flush()

    # Copy the result JSON (copy by value so edits don't affect original)
    db.add(JobResult(
        job_id        = new_id,
        summary_json  = src.result.summary_json,
        routes_json   = src.result.routes_json,
        stops_json    = src.result.stops_json,
        unserved_json = src.result.unserved_json,
        map_data_json = src.result.map_data_json,
        excel_bytes   = src.result.excel_bytes,
    ))
    db.commit()
    return {**new_job.to_dict(), "forked_from": job_id}


@app.patch("/api/jobs/{job_id}/result")
def patch_job_result(job_id: str, body: dict, db: Session = Depends(get_db)):
    """
    Replace the stored result for a manual-edit job.
    Accepts partial payload; unspecified fields are preserved.
    """
    job = db.get(Job, job_id)
    if not job: raise HTTPException(404, "Job not found")
    if not job.result: raise HTTPException(404, "Job has no result to patch")

    r = job.result
    if "summary"       in body: r.summary_json  = _dumps(body["summary"])
    if "route_summary" in body: r.routes_json   = _dumps(body["route_summary"])
    if "stop_details"  in body: r.stops_json    = _dumps(body["stop_details"])
    if "unserved"      in body: r.unserved_json = _dumps(body["unserved"])
    if "map_data"      in body: r.map_data_json = _dumps(body["map_data"])

    # Recompute Excel if route/stop data changed
    if "route_summary" in body or "stop_details" in body:
        try:
            excel = output_formatter.export_to_excel(
                body.get("route_summary", job.result.get_routes()),
                body.get("stop_details",  job.result.get_stops()),
                job.result.get_unserved(),
            )
            r.excel_bytes = excel
        except Exception as e:
            log.warning(f"Excel regen failed for {job_id}: {e}")

    db.commit()
    return {"ok": True}


# ════════════════════════════════════════════════════════════
#  Frontend
# ════════════════════════════════════════════════════════════

@app.get("/")
def serve_frontend():
    if os.path.exists("index.html"):
        return FileResponse("index.html")
    return JSONResponse({"message": "VRP API v2 — see /docs"})


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)