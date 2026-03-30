# VRP Route Optimization System

A production-grade Vehicle Routing Problem (VRP) solver for delivery logistics optimization.
**Stack:** FastAPI · OR-Tools · OSRM · Next.js · MongoDB · Docker

---

## 🚀 Quick Start (Docker - Recommended)

### Prerequisites
- Docker & Docker Compose
- Git

### 1. Clone Repository
```bash
git clone https://github.com/goruden/route-optimizer.git

cd route
```

### 2. Start All Services
```bash
docker-compose up -d
```

This will start:
- **MongoDB** (port 27017) - Data storage
- **OSRM** (port 5000) - Road routing engine for Mongolia
- **Backend API** (port 8067) - FastAPI + OR-Tools optimization
- **Frontend** (port 3067) - Next.js web interface

### 3. Access the Application
- **Web UI**: http://localhost:3067
- **API Docs**: http://localhost:8067/docs
- **Health Check**: http://localhost:8067/api/health

---

## 🛠️ Manual Installation (Development)

### System Requirements
- Python 3.9+
- Node.js 18+
- MongoDB
- Docker (for OSRM)

### Backend Setup

1. **Install Python Dependencies**
```bash
cd backend
pip install -r requirements.txt
```

2. **Start OSRM Routing Engine**
```bash
# Download Mongolia map data (one-time)
wget https://download.geofabrik.de/asia/mongolia-latest.osm.pbf

# Process OSRM data (one-time)
docker run -t -v "${PWD}/../osrm:/data" osrm/osrm-backend osrm-extract -p /opt/car.lua /data/mongolia-latest.osm.pbf
docker run -t -v "${PWD}/../osrm:/data" osrm/osrm-backend osrm-partition /data/mongolia-latest.osrm
docker run -t -v "${PWD}/../osrm:/data" osrm/osrm-backend osrm-customize /data/mongolia-latest.osrm

# Start OSRM server
docker run -d -p 5000:5000 -v "${PWD}/../osrm:/data" osrm/osrm-backend \
  osrm-routed --algorithm mld --max-table-size 50000000 /data/mongolia-latest.osrm
```

3. **Start MongoDB**
```bash
# Using Docker
docker run -d -p 27017:27017 --name mongodb mongo:7

# Or install locally
mongod
```

4. **Start Backend API**
```bash
cd backend
uvicorn main:app --host 0.0.0.0 --port 8067 --reload
```

### Frontend Setup

1. **Install Node Dependencies**
```bash
cd frontend
npm install
```

2. **Start Development Server**
```bash
npm run dev
```

Access at http://localhost:3067

---

## 📊 Data Preparation

### Required Files

Place your data files in `backend/data/`:

1. **stores.xlsx** - Store locations and demand data
2. **matrix.xlsx** - Distance/time matrix (generated automatically)

### stores.xlsx Format

**Sheet: Store**
| Column | Description |
|--------|-------------|
| Store ID | Unique store identifier |
| BIZLOC_ENG_NM | English name |
| BIZLOC_NM | Mongolian name |
| ADDR_1 | Address |
| LATITUDE | Latitude (decimal degrees) |
| LONGITUDE | Longitude (decimal degrees) |
| Sale start time | Opening time (HH:MM:SS) |
| SalesCloseTime | Closing time (HH:MM:SS) |
| Average Order CBM per day (DRY DC) | Daily dry volume demand (m³) |
| Average Order Weight per day (DRY DC) | Daily dry weight demand (kg) |
| Average Order CBM per day (COLD DC) | Daily cold volume demand (m³) |
| Average Order Weight per day (COLD DC) | Daily cold weight demand (kg) |

**Sheet: Vehicle**
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

### Generate Distance Matrix

**Option A: Via Web UI**
1. Upload your stores.xlsx file
2. Click "Build Matrix via OSRM"
3. Download the generated matrix.xlsx
4. Re-upload as the matrix file

**Option B: Via Script**
```bash
cd backend
python distanceMatrix.py
```

---

## ⚙️ Configuration

### Backend Configuration (`backend/config.py`)

Key settings you may want to adjust:

```python
# Depot Locations (Ulaanbaatar, Mongolia)
DEPOTS = {
    "Dry DC":  {"lat": 47.8847516, "lon": 106.7932466},
    "Cold DC": {"lat": 47.80758101116645, "lon": 107.19407110357587},
}

# Fleet Schedules
FLEET_SCHEDULE = {
    "DRY": {
        "start_hour": 13,      # 13:00 departure
        "max_horizon_hour": 24, # Finish by 24:00
    },
    "COLD": {
        "start_hour": 3,       # 03:00 departure  
        "max_horizon_hour": 14, # Finish by 14:00
    },
}

# Solver Parameters
MAX_TRIPS_PER_VEHICLE = 2      # Trips per vehicle per shift
SERVICE_TIME_SECONDS = 600     # 10 min per stop
MAX_SOLVER_TIME_SECONDS = 120  # OR-Tools time budget

# Clustering (for large datasets)
CLUSTERING = False             # Set True for >100 stores
```

### Environment Variables

Create `.env` file in backend root:

```bash
# Database
MONGO_URL=mongodb://localhost:27017

# OSRM Routing
OSRM_URL=http://localhost:5000

# Optimization
CLUSTERING=false
MAX_CLUSTERS=1

# Capacity Limits
MAX_WEIGHT_FILL_PERCENTAGE=1.0
MAX_VOLUME_FILL_PERCENTAGE=1.0
```

### Frontend Configuration

Create `.env.local` in frontend root:

```bash
NEXT_PUBLIC_API_URL=http://localhost:8067
```

---

## 🎯 Usage

### Web Interface

1. **Upload Data**: Upload your stores.xlsx and matrix.xlsx files
2. **Configure**: Set optimization parameters (mode, max trips, solver time)
3. **Optimize**: Click "Optimize Routes" to run VRP solver
4. **View Results**: 
   - Interactive map with routes
   - Detailed route summaries
   - Unserved stores with reasons
5. **Export**: Download results as Excel file

### API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/optimize` | POST | Run VRP optimization |
| `/api/export/{job_id}` | GET | Download Excel results |
| `/api/build-matrix` | POST | Build matrix from OSRM |
| `/api/health` | GET | Health check + OSRM status |
| `/docs` | GET | Interactive API docs |

### Optimization Modes

| Mode | Objective | Best For |
|------|-----------|----------|
| `cheapest` | Minimize total cost (fuel + fixed + labor) | Daily operations |
| `fastest` | Minimize total travel time | Time-critical deliveries |
| `shortest` | Minimize total distance | Fuel efficiency |

---

## 🏗️ Architecture

```
Frontend (Next.js) → Backend API (FastAPI) → OR-Tools Solver
                                   ↓
                           OSRM (Routing Engine) → MongoDB (Storage)
```

### Key Components

- **data_loader.py**: Excel file parsing and validation
- **solver.py**: OR-Tools VRP optimization with time windows
- **osrm_client.py**: Real road distance/time calculations
- **output_formatter.py**: Results formatting and Excel export
- **clustering.py**: Geographic clustering for large datasets

---

## 🔧 Production Deployment

### Docker Production Setup

1. **Build and Deploy**
```bash
docker-compose -f docker-compose.yml up -d
```

2. **Environment Configuration**
Set production environment variables in `docker-compose.yml`:
```yaml
environment:
  - NODE_ENV=production
  - MONGO_URL=mongodb://mongodb:27017
  - OSRM_URL=http://osrm:5000
```

3. **SSL/TLS**
Configure reverse proxy (nginx/traefik) for HTTPS termination.

4. **Monitoring**
- Health checks configured for all services
- Logs available via `docker-compose logs [service]`

### Scaling Considerations

- **OSRM**: Can be horizontally scaled behind load balancer
- **Backend**: Multiple instances for API load balancing
- **MongoDB**: Replica set for high availability
- **Frontend**: Static asset CDN deployment

---

## 🐛 Troubleshooting

### Common Issues

1. **OSRM Connection Failed**
```bash
# Check OSRM status
curl http://localhost:5000/route/v1/driving/106.9,47.9;106.9,47.91

# Restart OSRM container
docker-compose restart osrm
```

2. **Matrix Build Timeout**
- Reduce number of stores per batch
- Check OSRM server resources
- Verify Mongolia map data is properly processed

3. **Solver Takes Too Long**
- Increase `MAX_SOLVER_TIME_SECONDS` in config
- Enable `CLUSTERING=true` for large datasets
- Reduce number of vehicles/stores

4. **Frontend API Connection**
- Verify `NEXT_PUBLIC_API_URL` in frontend .env.local
- Check backend service is running on correct port
- Ensure no firewall blocking connections

### Logs

```bash
# View all service logs
docker-compose logs

# View specific service
docker-compose logs backend
docker-compose logs frontend
docker-compose logs osrm
```

---

## 📈 Performance Tuning

### Optimization Parameters

- **Solver Time**: Increase for better solutions (costs more time)
- **Clustering**: Enable for >100 stores to improve speed
- **Service Time**: Adjust based on real unloading times
- **Speed Factors**: Tune for local traffic patterns

### Resource Requirements

- **Minimum**: 4GB RAM, 2 CPU cores
- **Recommended**: 8GB RAM, 4 CPU cores
- **Large Scale**: 16GB RAM, 8+ CPU cores

---

## 🤝 Contributing

1. Fork the repository
2. Create feature branch
3. Make changes with tests
4. Submit pull request
<<<<<<< HEAD
=======

---

## 📄 License

[Add your license information here]

---

## 📞 Support

For issues and questions:
- Create GitHub issue
- Check troubleshooting section
- Review API documentation at `/docs`
>>>>>>> 8df7e78 (Add comprehensive README and fix next-env.d.ts)
