# Unified Data Access Platform

A production-grade metadata discovery system for lakehouse table formats. Automatically detects, reads, and normalizes metadata from **Apache Iceberg** and **Delta Lake** tables stored in Amazon S3.

## 📋 Table of Contents

- [Features](#features)
- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Configuration](#configuration)
- [Running the Application](#running-the-application)
- [API Usage](#api-usage)
- [Generating Sample Tables](#generating-sample-tables)
- [Project Structure](#project-structure)
- [How It Works](#how-it-works)

---

## ✨ Features

Currently implemented and working:

- ✅ **Automatic Format Detection**: Detects Iceberg and Delta Lake tables by analyzing S3 directory structure
- ✅ **Native Metadata Reading**: Direct metadata extraction from S3 without requiring compute engines
- ✅ **Unified Schema**: Normalizes different table formats into a consistent internal schema
- ✅ **REST API**: FastAPI-based endpoints for table discovery and querying
- ✅ **SQLite Storage**: Persistent metadata storage with full CRUD operations
- ✅ **AWS S3 Integration**: Direct S3 access using boto3
- ✅ **Docker-based Table Generation**: Containerized Spark environment for generating test tables

---

## 🏗️ Architecture

### System Components

```
┌─────────────────┐
│   REST API      │  ← FastAPI endpoints
│  (Port 8000)    │
└────────┬────────┘
         │
┌────────▼────────────────────────┐
│  Discovery Engine               │
│  - Format Detection             │
│  - Metadata Reading             │
│  - Normalization                │
└────────┬────────────────────────┘
         │
    ┌────┴────┐
    │         │
┌───▼──┐  ┌──▼──────┐
│ S3   │  │ SQLite  │
│Tables│  │ Storage │
└──────┘  └─────────┘
```

### Directory Structure

```
metadata/
├── run_api.py              # API server entry point
├── start_api.ps1           # PowerShell script with AWS credentials
├── requirements.txt        # Python dependencies
├── metadata.db            # SQLite database (auto-created)
│
├── config/
│   └── settings.py        # Configuration settings
│
├── src/
│   ├── main.py                   # MetadataDiscoveryEngine (core logic)
│   │
│   ├── api/                      # REST API Layer
│   │   ├── main.py              # FastAPI app definition
│   │   ├── routes.py            # API endpoints
│   │   └── models.py            # Request/response models (Pydantic)
│   │
│   ├── detectors/               # Format Detection
│   │   └── format_detector.py   # Detects Iceberg/Delta by structure
│   │
│   ├── readers/                 # Metadata Readers
│   │   ├── iceberg_reader.py   # Reads Iceberg metadata.json
│   │   ├── delta_reader.py     # Reads Delta _delta_log
│   │   └── hudi_reader.py      # Hudi reader (not yet tested)
│   │
│   ├── normalizer/              # Schema Normalization
│   │   └── metadata_normalizer.py  # Unifies different formats
│   │
│   ├── models/                  # Domain Models
│   │   └── table_metadata.py   # TableMetadata dataclass
│   │
│   ├── storage/                 # Data Persistence
│   │   └── metadata_store.py   # SQLite CRUD operations
│   │
│   └── utils/                   # Utilities
│       ├── logger.py           # Logging setup
│       ├── exceptions.py       # Custom exceptions
│       └── s3_utils.py         # S3 helper functions
│
└── src/generate_tables/         # Docker-based Table Generator
    ├── Dockerfile
    ├── docker-compose.yml
    ├── generate_tables.py       # PySpark script
    └── README.md
```

---

## 📦 Prerequisites

- **Python**: 3.8 or higher
- **AWS Account**: With S3 access
- **Docker Desktop**: Required for generating sample tables
- **Operating System**: Windows (tested), Linux, or macOS

---

## 🚀 Installation

### Step 1: Clone or Extract Project

```bash
cd C:\Users\ashis\Desktop\metadata
```

### Step 2: Install Python Dependencies

```powershell
pip install -r requirements.txt
```

**Dependencies installed:**
- `boto3` - AWS S3 client
- `fastapi` - Web framework
- `uvicorn` - ASGI server
- `pydantic` - Data validation

---

## ⚙️ Configuration

### AWS Credentials Setup

The application requires AWS credentials to access S3 buckets.

**Option 1: Use the Startup Script (Recommended)**

Edit `start_api.ps1` with your credentials:

```powershell
$env:AWS_ACCESS_KEY_ID="YOUR_ACCESS_KEY"
$env:AWS_SECRET_ACCESS_KEY="YOUR_SECRET_KEY"
$env:AWS_DEFAULT_REGION="us-east-1"
```

**Option 2: Set Environment Variables Manually**

```powershell
$env:AWS_ACCESS_KEY_ID="YOUR_ACCESS_KEY"
$env:AWS_SECRET_ACCESS_KEY="YOUR_SECRET_KEY"
$env:AWS_DEFAULT_REGION="us-east-1"
python run_api.py
```

**Option 3: AWS CLI Configuration**

```powershell
aws configure
```

---

## 🏃 Running the Application

### Start the API Server

**Using Startup Script:**
```powershell
.\start_api.ps1
```

**Or Manually:**
```powershell
$env:AWS_ACCESS_KEY_ID="YOUR_KEY"
$env:AWS_SECRET_ACCESS_KEY="YOUR_SECRET"
$env:AWS_DEFAULT_REGION="us-east-1"
python run_api.py
```

### Access the API

- **API Base URL**: http://localhost:8000
- **Interactive Docs**: http://localhost:8000/docs (Swagger UI)
- **Alternative Docs**: http://localhost:8000/redoc
- **Health Check**: http://localhost:8000/health

---

## 📡 API Usage

### Available Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | Check API health and database status |
| `/api/v1/discover` | POST | Discover and store table metadata from S3 |
| `/api/v1/tables` | GET | List all discovered tables |
| `/api/v1/tables?format=DELTA` | GET | Filter tables by format |
| `/api/v1/tables/{table_name}` | GET | Get specific table details |
| `/api/v1/tables/{table_name}/columns` | GET | Get table columns |
| `/api/v1/tables/{table_name}` | DELETE | Remove table from metadata store |

### Example Usage

#### 1. Health Check

```bash
curl http://localhost:8000/health
```

**Response:**
```json
{
  "status": "healthy",
  "version": "1.0.0",
  "timestamp": "2026-01-11T12:00:00",
  "database_connected": true,
  "tables_count": 2
}
```

#### 2. Discover a Delta Table

```bash
curl -X POST "http://localhost:8000/api/v1/discover" \
  -H "Content-Type: application/json" \
  -d '{"s3_path": "s3://metadataproject/test-data/sample-data/delta/sales_delta/"}'
```

**Response:**
```json
{
  "success": true,
  "message": "Table metadata discovered and stored successfully",
  "data": {
    "table_name": "sales_delta",
    "format": "DELTA",
    "location": "s3://metadataproject/test-data/sample-data/delta/sales_delta/",
    "columns": [
      {
        "name": "order_id",
        "type": "bigint",
        "nullable": false
      },
      {
        "name": "customer_id",
        "type": "bigint",
        "nullable": false
      }
    ],
    "partitions": ["order_date"],
    "supports_time_travel": true
  }
}
```

#### 3. List All Tables

```bash
curl http://localhost:8000/api/v1/tables
```

**Response:**
```json
{
  "success": true,
  "count": 2,
  "tables": ["sales_delta", "sales_iceberg"]
}
```

#### 4. Get Table Details

```bash
curl http://localhost:8000/api/v1/tables/sales_delta
```

#### 5. Filter by Format

```bash
curl "http://localhost:8000/api/v1/tables?format=DELTA"
```

### Using the Interactive Docs

1. Open http://localhost:8000/docs
2. Click on any endpoint to expand it
3. Click **"Try it out"**
4. Fill in the parameters
5. Click **"Execute"**
6. View the response

---

## 🐳 Generating Sample Tables

The project includes a Docker-based tool to generate sample Iceberg and Delta Lake tables and upload them to S3.

### Prerequisites

- Docker Desktop installed and running
- AWS credentials configured in `.env` file

### Steps

1. **Navigate to the generator directory:**

```powershell
cd src/generate_tables
```

2. **Create `.env` file:**

```bash
AWS_ACCESS_KEY_ID=YOUR_ACCESS_KEY
AWS_SECRET_ACCESS_KEY=YOUR_SECRET_KEY
AWS_DEFAULT_REGION=us-east-1
```

3. **Run the generator:**

```powershell
docker compose up --build
```

### What It Does

The Docker container:
1. Starts a Spark 3.5.0 environment with Iceberg and Delta Lake support
2. Generates sample data (sales orders)
3. Creates tables in both formats:
   - **Iceberg**: `output/iceberg/sales_iceberg/`
   - **Delta Lake**: `output/delta/sales_delta/`
4. Uploads all files to S3:
   - `s3://metadataproject/test-data/sample-data/iceberg/sales_iceberg/`
   - `s3://metadataproject/test-data/sample-data/delta/sales_delta/`

### Expected Output

```
✅ Iceberg table created
✅ Delta table created
🚀 Uploading tables to S3...
✅ Uploaded 15 files from output/iceberg/sales_iceberg
✅ Uploaded 8 files from output/delta/sales_delta
🎉 All tables uploaded to S3 successfully!
S3 Location: s3://metadataproject/test-data/sample-data/
```

---

## 🔍 How It Works

### 1. Format Detection

When you request table discovery, the system:

1. **Analyzes S3 Path Structure**
   - Lists objects in the S3 path
   - Looks for format-specific markers

2. **Detection Logic**
   - **Iceberg**: Checks for `metadata/` folder and `.metadata.json` files
   - **Delta Lake**: Checks for `_delta_log/` folder and transaction log files
   - **Hudi**: Checks for `.hoodie/` folder (not fully tested)

**Code Location**: `src/detectors/format_detector.py`

### 2. Metadata Reading

Once format is detected, the appropriate reader extracts metadata:

#### Iceberg Reader (`src/readers/iceberg_reader.py`)
- Finds the latest `metadata.json` file in `metadata/` folder
- Parses JSON to extract:
  - Table schema (columns, types, nullability)
  - Partition spec
  - Table properties
  - Snapshot information

#### Delta Lake Reader (`src/readers/delta_reader.py`)
- Reads the latest transaction log from `_delta_log/`
- Parses JSON lines to extract:
  - Schema from `metaData` action
  - Partition columns
  - Table properties
  - Protocol version

### 3. Normalization

Different table formats are converted to a unified internal model:

**Code Location**: `src/normalizer/metadata_normalizer.py`

```python
@dataclass
class TableMetadata:
    table_name: str              # Extracted from S3 path
    format: str                  # ICEBERG, DELTA, HUDI
    location: str                # Full S3 path
    columns: List[ColumnMetadata]  # Unified column schema
    partitions: List[str]        # Partition column names
    properties: Dict[str, str]   # Table properties
    supports_time_travel: bool   # Format capability
    created_at: datetime
    updated_at: datetime
```

### 4. Storage

Metadata is stored in SQLite with the following schema:

```sql
CREATE TABLE tables (
    table_name TEXT PRIMARY KEY,
    format TEXT NOT NULL,
    location TEXT NOT NULL,
    columns JSON NOT NULL,
    partitions JSON,
    properties JSON,
    supports_time_travel BOOLEAN,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
)
```

**Code Location**: `src/storage/metadata_store.py`

### 5. API Layer

FastAPI provides REST endpoints:

- **Application Setup**: `src/api/main.py` defines the FastAPI app with CORS, exception handlers
- **Endpoints**: `src/api/routes.py` implements all HTTP endpoints
- **Models**: `src/api/models.py` defines Pydantic schemas for validation

---

## 🧪 Testing the System

### Test with Sample Tables

After generating sample tables, test discovery:

```bash
# Test Delta Lake
curl -X POST "http://localhost:8000/api/v1/discover" \
  -H "Content-Type: application/json" \
  -d '{"s3_path": "s3://metadataproject/test-data/sample-data/delta/sales_delta/"}'

# Test Iceberg
curl -X POST "http://localhost:8000/api/v1/discover" \
  -H "Content-Type: application/json" \
  -d '{"s3_path": "s3://metadataproject/test-data/sample-data/iceberg/sales_iceberg/"}'

# List discovered tables
curl http://localhost:8000/api/v1/tables
```

### Verify in S3

Check your S3 bucket to see the uploaded tables:
```
s3://metadataproject/test-data/sample-data/
├── delta/
│   └── sales_delta/
│       ├── _delta_log/
│       └── part-*.parquet
└── iceberg/
    └── sales_iceberg/
        ├── metadata/
        └── data/
```

---

## 🛠️ Technology Stack

### Backend
- **Python 3.12** - Core language
- **FastAPI** - Web framework
- **Uvicorn** - ASGI server
- **Pydantic** - Data validation
- **boto3** - AWS SDK

### Storage
- **SQLite** - Metadata database

### Table Generation
- **Docker** - Containerization
- **Apache Spark 3.5.0** - Data processing
- **Iceberg 1.5.0** - Iceberg table format
- **Delta Lake 3.1.0** - Delta table format

---

## 📝 Notes

### Supported Table Formats

| Format | Detection | Reading | Status |
|--------|-----------|---------|--------|
| **Iceberg** | ✅ Working | ✅ Working | Fully tested |
| **Delta Lake** | ✅ Working | ✅ Working | Fully tested |
| **Hudi** | ⚠️ Partial | ⚠️ Not tested | Implementation exists but untested |

### Known Limitations

1. **Hudi Support**: Code exists but not fully tested with real Hudi tables
2. **Single Region**: Currently configured for `us-east-1` only
3. **Local Storage**: Uses SQLite (suitable for small-medium workloads)

### Database File

The SQLite database `metadata.db` is created automatically in the project root when you first discover a table. It persists all metadata across API restarts.

---

## 🤝 Support

For issues or questions about this project, contact the development team.

---

## 📄 License

Proprietary - All Rights Reserved