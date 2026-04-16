# ============================================================
#  database.py  v8 — MongoDB (Motor async driver)
# ============================================================

import datetime
import json
from typing import Optional, AsyncGenerator

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase, AsyncIOMotorGridFSBucket
from bson import ObjectId

# ── Connection ────────────────────────────────────────────────

import os

MONGO_URL = os.getenv("MONGO_URL", "mongodb://localhost:27017")
DB_NAME   = "vrp_data"

client: AsyncIOMotorClient = None   # set in startup
db:     AsyncIOMotorDatabase = None  # set in startup
fs:     AsyncIOMotorGridFSBucket = None  # for binary blobs (matrix, excel)


async def connect_db():
    """Call once at app startup (e.g. FastAPI lifespan)."""
    global client, db, fs
    client = AsyncIOMotorClient(MONGO_URL)
    db     = client[DB_NAME]
    fs     = AsyncIOMotorGridFSBucket(db)
    await init_db()


async def close_db():
    """Call once at app shutdown."""
    if client:
        client.close()


def get_db() -> AsyncIOMotorDatabase:
    """FastAPI dependency — returns the db handle directly.
    Motor manages its own connection pool, no teardown needed."""
    return db


# ── Indexes (replaces CREATE TABLE / ALTER TABLE) ─────────────

async def init_db():
    """Create indexes on first run. Safe to call multiple times."""
    await db["jobs"].create_index("dataset_id")
    await db["jobs"].create_index("group_id")
    await db["jobs"].create_index("status")
    await db["jobs"].create_index("created_at")

    await db["run_groups"].create_index("dataset_id")
    await db["run_groups"].create_index("created_at")

    await db["datasets"].create_index("name")
    await db["datasets"].create_index("created_at")

    # stores / vehicles are sub-documents embedded inside datasets,
    # but if you query them frequently you can also store them in their
    # own collections — see design note below.
    await db["stores"].create_index("dataset_id")
    await db["stores"].create_index("store_id")
    await db["stores"].create_index([("dataset_id", 1), ("node_id", 1)])

    await db["vehicles"].create_index("dataset_id")
    await db["vehicles"].create_index("truck_id")


# ── Collection helpers ────────────────────────────────────────
# MongoDB documents use "_id" as the primary key.
# We keep the same UUID strings you used before so the rest of
# the codebase doesn't change (pass them in as "_id").

# ════════════════════════════════════════════════════════════
#  RunGroup
# ════════════════════════════════════════════════════════════

class RunGroupDoc:
    """Thin wrapper that mirrors the old ORM .to_dict() interface."""
    COLLECTION = "run_groups"

    @staticmethod
    def make(group_id: str, name: str, dataset_id: Optional[int] = None) -> dict:
        return {
            "_id"        : group_id,
            "name"       : name,
            "dataset_id" : dataset_id,
            "created_at" : datetime.datetime.utcnow(),
        }

    @staticmethod
    def to_dict(doc: dict) -> dict:
        return {
            "id"         : doc["_id"],
            "name"       : doc.get("name"),
            "dataset_id" : doc.get("dataset_id"),
            "created_at" : doc["created_at"].isoformat() if doc.get("created_at") else None,
        }


# ════════════════════════════════════════════════════════════
#  Dataset
# ════════════════════════════════════════════════════════════

class DatasetDoc:
    """
    Datasets live in the 'datasets' collection.

    Binary blobs (matrix_bytes) are stored in GridFS and referenced
    by a GridFS file _id stored in matrix_file_id.
    """
    COLLECTION = "datasets"

    @staticmethod
    def make(name: str) -> dict:
        return {
            "name"           : name,
            "created_at"     : datetime.datetime.utcnow(),
            "matrix_file_id" : None,   # set after GridFS upload
        }

    @staticmethod
    def to_dict(doc: dict) -> dict:
        return {
            "id"             : doc["_id"],
            "name"           : doc.get("name"),
            "created_at"     : doc["created_at"].isoformat() if doc.get("created_at") else None,
            "matrix_file_id" : str(doc["matrix_file_id"]) if doc.get("matrix_file_id") else None,
        }


# ── GridFS helpers for binary blobs ──────────────────────────

async def save_matrix_bytes(dataset_id, matrix_bytes: bytes) -> str:
    """Upload matrix bytes to GridFS; store returned file_id on dataset doc."""
    filename = f"matrix_{dataset_id}.bin"
    file_id  = await fs.upload_from_stream(filename, matrix_bytes)
    await db[DatasetDoc.COLLECTION].update_one(
        {"_id": dataset_id},
        {"$set": {"matrix_file_id": file_id}}
    )
    return str(file_id)


async def load_matrix_bytes(dataset_id) -> Optional[bytes]:
    """Download matrix bytes from GridFS for a given dataset."""
    doc = await db[DatasetDoc.COLLECTION].find_one({"_id": dataset_id}, {"matrix_file_id": 1})
    if not doc or not doc.get("matrix_file_id"):
        return None
    stream = await fs.open_download_stream(doc["matrix_file_id"])
    return await stream.read()


async def save_excel_bytes(job_id: str, excel_bytes: bytes) -> str:
    """Upload excel output to GridFS; store file_id on job_results doc."""
    filename = f"excel_{job_id}.xlsx"
    file_id  = await fs.upload_from_stream(filename, excel_bytes)
    await db["job_results"].update_one(
        {"_id": job_id},
        {"$set": {"excel_file_id": file_id}},
        upsert=True
    )
    return str(file_id)


async def load_excel_bytes(job_id: str) -> Optional[bytes]:
    """Download excel bytes from GridFS for a given job."""
    doc = await db["job_results"].find_one({"_id": job_id}, {"excel_file_id": 1})
    if not doc or not doc.get("excel_file_id"):
        return None
    stream = await fs.open_download_stream(doc["excel_file_id"])
    return await stream.read()


# ════════════════════════════════════════════════════════════
#  Store
# ════════════════════════════════════════════════════════════

class StoreDoc:
    """Stored in 'stores' collection — one document per store."""
    COLLECTION = "stores"

    @staticmethod
    def make(dataset_id, row: dict) -> dict:
        """row is the dict produced by data_loader.load_stores()"""
        return {
            "dataset_id"  : dataset_id,
            "store_id"    : row["store_id"],
            "node_id"     : row["node_id"],
            "eng_name"    : row.get("eng_name", ""),
            "mn_name"     : row.get("mn_name", ""),
            "address"     : row.get("address", ""),
            "detail_addr" : row.get("detail_addr", ""),
            "lat"         : row["lat"],
            "lon"         : row["lon"],
            "open_s"      : row.get("open_s", 0),
            "close_s"     : row.get("close_s", 86399),
            "dry_cbm"     : row.get("dry_cbm", 0.0),
            "dry_kg"      : row.get("dry_kg", 0.0),
            "cold_cbm"    : row.get("cold_cbm", 0.0),
            "cold_kg"     : row.get("cold_kg", 0.0),
            "has_dry"     : row.get("has_dry", False),
            "has_cold"    : row.get("has_cold", False),
            "seasonal_data": row.get("seasonal_data", {}),
            "city"        : row.get("city", "Rural"),
            "is_urban"    : row.get("is_urban", False),
        }

    @staticmethod
    def to_solver_dict(doc: dict) -> dict:
        return {
            "store_id"    : doc["store_id"],
            "node_id"     : doc["node_id"],
            "eng_name"    : doc.get("eng_name", ""),
            "mn_name"     : doc.get("mn_name", ""),
            "address"     : doc.get("address", ""),
            "detail_addr" : doc.get("detail_addr", ""),
            "lat"         : doc["lat"],
            "lon"         : doc["lon"],
            "open_s"      : doc["open_s"],
            "close_s"     : doc["close_s"],
            "dry_cbm"     : doc["dry_cbm"],
            "dry_kg"      : doc["dry_kg"],
            "cold_cbm"    : doc["cold_cbm"],
            "cold_kg"     : doc["cold_kg"],
            "has_dry"     : doc["has_dry"],
            "has_cold"    : doc["has_cold"],
            "seasonal_data": doc.get("seasonal_data", {}),
            "city"        : doc.get("city", "Rural"),
            "is_urban"    : doc.get("is_urban", False),
        }


# ════════════════════════════════════════════════════════════
#  Vehicle
# ════════════════════════════════════════════════════════════

class VehicleDoc:
    COLLECTION = "vehicles"

    @staticmethod
    def make(dataset_id, row: dict) -> dict:
        """row is the dict produced by data_loader.load_vehicles()"""
        return {
            "dataset_id"   : dataset_id,
            "truck_id"     : row["truck_id"],
            "truck_num"    : row.get("truck_num", ""),
            "description"  : row.get("description", ""),
            "contractor"   : row.get("contractor", ""),
            "is_fleet"    : row.get("is_fleet", False),
            "depot"        : row["depot"],
            "fleet"        : row.get("fleet", "DRY"),
            "cap_kg"       : row["cap_kg"],
            "cap_m3"       : row["cap_m3"],
            "fuel_cost_km" : row["fuel_cost_km"],
            "vehicle_cost" : row["vehicle_cost"],
            "labor_cost"   : row["labor_cost"],
        }

    @staticmethod
    def to_solver_dict(doc: dict) -> dict:
        return {
            "truck_id"     : doc["truck_id"],
            "truck_num"    : doc.get("truck_num", ""),
            "description"  : doc.get("description", ""),
            "contractor"   : doc.get("contractor", ""),
            "is_fleet"    : doc.get("is_fleet", False),
            "depot"        : doc["depot"],
            "fleet"        : doc["fleet"],
            "cap_kg"       : doc["cap_kg"],
            "cap_m3"       : doc["cap_m3"],
            "fuel_cost_km" : doc["fuel_cost_km"],
            "vehicle_cost" : doc["vehicle_cost"],
            "labor_cost"   : doc["labor_cost"],
        }


# ════════════════════════════════════════════════════════════
#  Job
# ════════════════════════════════════════════════════════════

class JobDoc:
    COLLECTION = "jobs"

    @staticmethod
    def make(job_id: str, dataset_id=None, group_id: str = None,
             version_name: str = None, is_manual: bool = False,
             mode: str = None, max_trips: int = None,
             solver_time: int = None, rural_solver_time: int = None,
             season: str = None) -> dict:
        return {
            "_id"              : job_id,
            "dataset_id"       : dataset_id,
            "group_id"         : group_id,
            "version_name"     : version_name,
            "is_manual"        : is_manual,
            "mode"             : mode,
            "max_trips"        : max_trips,
            "solver_time"      : solver_time,
            "rural_solver_time": rural_solver_time,
            "season"           : season,
            "status"           : "pending",
            "error_msg"        : None,
            "created_at"       : datetime.datetime.utcnow(),
            "completed_at"     : None,
        }

    @staticmethod
    def to_dict(doc: dict) -> dict:
        return {
            "id"               : doc["_id"],
            "dataset_id"       : doc.get("dataset_id"),
            "group_id"         : doc.get("group_id"),
            "version_name"     : doc.get("version_name"),
            "is_manual"        : bool(doc.get("is_manual", False)),
            "mode"             : doc.get("mode"),
            "max_trips"        : doc.get("max_trips"),
            "solver_time"      : doc.get("solver_time"),
            "rural_solver_time": doc.get("rural_solver_time"),
            "season"           : doc.get("season"),
            "status"           : doc.get("status"),
            "error_msg"        : doc.get("error_msg"),
            "created_at"       : doc["created_at"].isoformat() if doc.get("created_at") else None,
            "completed_at"     : doc["completed_at"].isoformat() if doc.get("completed_at") else None,
        }


# ════════════════════════════════════════════════════════════
#  JobResult
# ════════════════════════════════════════════════════════════

class JobResultDoc:
    """
    Stored in 'job_results' collection.
    excel_bytes → GridFS (use save_excel_bytes / load_excel_bytes above).
    All other fields stored directly as JSON strings (same as before).
    """
    COLLECTION = "job_results"

    @staticmethod
    def make(job_id: str, summary: dict, routes: list,
             stops: list, unserved: list, map_data: list) -> dict:
        return {
            "_id"          : job_id,
            "summary_json" : json.dumps(summary),
            "routes_json"  : json.dumps(routes),
            "stops_json"   : json.dumps(stops),
            "unserved_json": json.dumps(unserved),
            "map_data_json": json.dumps(map_data),
            "excel_file_id": None,   # set via save_excel_bytes()
        }

    @staticmethod
    def get_summary(doc: dict):  return json.loads(doc.get("summary_json")  or "null")
    @staticmethod
    def get_routes(doc: dict):   return json.loads(doc.get("routes_json")   or "[]")
    @staticmethod
    def get_stops(doc: dict):    return json.loads(doc.get("stops_json")    or "[]")
    @staticmethod
    def get_unserved(doc: dict): return json.loads(doc.get("unserved_json") or "[]")
    @staticmethod
    def get_map_data(doc: dict): return json.loads(doc.get("map_data_json") or "[]")


# ════════════════════════════════════════════════════════════
#  Bulk insert helpers  (replaces session.add / session.commit)
# ════════════════════════════════════════════════════════════

async def bulk_insert_stores(dataset_id, store_rows: list):
    """Insert all stores for a dataset in one shot."""
    if not store_rows:
        return
    docs = [StoreDoc.make(dataset_id, r) for r in store_rows]
    await db[StoreDoc.COLLECTION].insert_many(docs, ordered=False)


async def bulk_insert_vehicles(dataset_id, vehicle_rows: list):
    """Insert all vehicles for a dataset in one shot."""
    if not vehicle_rows:
        return
    docs = [VehicleDoc.make(dataset_id, r) for r in vehicle_rows]
    await db[VehicleDoc.COLLECTION].insert_many(docs, ordered=False)