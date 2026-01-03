# 🔍 Metastore Viewer - Backend

A FastAPI-based backend for exploring metadata from **Iceberg**, **Delta Lake**, **Hudi**, and **Parquet** tables stored in object storage (S3, Azure, MinIO) without requiring a traditional metastore.

---

## 📋 Project Structure

```
backend/
├── app/
│   ├── __init__.py           # Package initialization
│   ├── main.py               # FastAPI application entry point
│   ├── config.py             # Configuration management
│   │
│   ├── models/               # Pydantic data models
│   │   ├── __init__.py
│   │   └── s3_models.py      # S3 request/response models
│   │
│   ├── services/             # Business logic
│   │   ├── __init__.py
│   │   └── s3_service.py     # S3 operations and connection handling
│   │
│   ├── api/                  # API route handlers
│   │   ├── __init__.py
│   │   └── health.py         # Health check & test endpoints
│   │
│   └── utils/                # Utility functions (future)
│
├── requirements.txt          # Python dependencies
└── README.md                 # This file
```

---

## 🚀 Quick Start

### **Step 1: Setup Python Environment**

```powershell
# Navigate to backend directory
cd backend

# Create virtual environment
python -m venv venv

# Activate virtual environment
.\venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### **Step 2: Configure Environment Variables**

```powershell
# Copy example env file
Copy-Item ..\.env.example .env

# Edit .env file with your AWS credentials
notepad .env
```

**Required Configuration:**
```env
# AWS Credentials (Get from AWS IAM Console)
AWS_ACCESS_KEY_ID=your_access_key_here
AWS_SECRET_ACCESS_KEY=your_secret_key_here
AWS_DEFAULT_REGION=us-east-1

# Application Settings
DEBUG=true
HOST=0.0.0.0
PORT=8000
```

⚠️ **Security Note**: 
- Never commit the `.env` file to version control!
- For production, use IAM roles instead of access keys
- Keep your AWS credentials secure

### **Step 3: Run the Application**

```powershell
# Make sure virtual environment is activated
.\venv\Scripts\activate

# Run the FastAPI server
python -m uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

Or simply:
```powershell
python -m app.main
```

You should see:
```
🚀 Starting Metastore Viewer v1.0.0
✅ Configuration loaded successfully
✅ Environment: development
✅ AWS Region: us-east-1

📚 API Documentation available at:
   Swagger UI: http://0.0.0.0:8000/docs
   ReDoc:      http://0.0.0.0:8000/redoc
```

---

## 🧪 Testing the API

### **1. Check API Health**

Open your browser or use curl:

```powershell
# Browser
http://localhost:8000/docs

# Or using curl
curl http://localhost:8000/api/v1/health
```

Expected Response:
```json
{
  "status": "healthy",
  "timestamp": "2026-01-01T10:30:00",
  "app_name": "Metastore Viewer",
  "version": "1.0.0",
  "environment": "development",
  "aws_region": "us-east-1",
  "has_aws_credentials": true
}
```

### **2. Test S3 Connection**

```powershell
curl http://localhost:8000/api/v1/test-s3-connection
```

Expected Response:
```json
{
  "success": true,
  "message": "Successfully connected to S3. Found 5 buckets.",
  "region": "us-east-1",
  "timestamp": "2026-01-01T10:30:00"
}
```

### **3. List S3 Objects**

Using the Swagger UI (http://localhost:8000/docs):

1. Click on **POST /api/v1/list-s3-objects**
2. Click "Try it out"
3. Enter request body:
```json
{
  "s3_path": "s3://your-bucket-name/path/",
  "aws_region": "us-east-1"
}
```
4. Click "Execute"

Or using curl:
```powershell
curl -X POST "http://localhost:8000/api/v1/list-s3-objects" `
  -H "Content-Type: application/json" `
  -d '{\"s3_path\": \"s3://your-bucket/data/\"}'
```

---

## 📚 API Documentation

Once the server is running, visit:

- **Swagger UI**: http://localhost:8000/docs (Interactive API testing)
- **ReDoc**: http://localhost:8000/redoc (Beautiful documentation)

### Available Endpoints:

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/` | Root endpoint with API info |
| GET | `/api/v1/health` | Health check with config status |
| GET | `/api/v1/test-s3-connection` | Test AWS S3 connectivity |
| POST | `/api/v1/list-s3-objects` | List objects in S3 path |
| GET | `/api/v1/config-status` | View configuration status |

---

## 🔒 Security Best Practices

### **1. AWS Credentials**

**Development (Local Machine):**
```env
# .env file
AWS_ACCESS_KEY_ID=AKIA...
AWS_SECRET_ACCESS_KEY=...
```

**Production (AWS Infrastructure):**
```env
# .env file
USE_IAM_ROLE=true
# Don't set access keys - EC2/ECS will use IAM role automatically
```

### **2. IAM Permissions Required**

Your AWS user/role needs these permissions:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:ListBucket",
        "s3:GetObject",
        "s3:ListAllMyBuckets"
      ],
      "Resource": [
        "arn:aws:s3:::*"
      ]
    }
  ]
}
```

For production, restrict to specific buckets:
```json
"Resource": [
  "arn:aws:s3:::your-data-bucket",
  "arn:aws:s3:::your-data-bucket/*"
]
```

### **3. CORS Configuration**

In `.env`, set allowed frontend origins:
```env
CORS_ORIGINS=http://localhost:3000,http://localhost:5173
```

For production:
```env
CORS_ORIGINS=https://yourdomain.com
```

---

## 🛠️ Development

### **Code Structure Guidelines**

- **`app/config.py`**: All configuration and environment variables
- **`app/models/`**: Pydantic models for request/response validation
- **`app/services/`**: Business logic and external service integrations
- **`app/api/`**: API route handlers (controllers)
- **`app/utils/`**: Helper functions and utilities

### **Adding New Endpoints**

1. Create a new router file in `app/api/`
2. Define route handlers with proper type hints
3. Register router in `app/main.py`

Example:
```python
# app/api/metadata.py
from fastapi import APIRouter

router = APIRouter(prefix="/api/v1/metadata", tags=["Metadata"])

@router.get("/table/{table_name}")
async def get_table_metadata(table_name: str):
    return {"table": table_name}
```

```python
# app/main.py
from app.api import metadata
app.include_router(metadata.router)
```

### **Code Style**

- Use type hints for all function parameters and returns
- Add docstrings to all functions
- Use Pydantic models for data validation
- Log important operations
- Handle errors gracefully

---

## 🐛 Troubleshooting

### **Issue: "AWS credentials not found"**

**Solution:**
1. Check `.env` file exists in backend directory
2. Verify `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` are set
3. Make sure virtual environment is activated
4. Restart the server after changing `.env`

### **Issue: "Access Denied" when listing buckets**

**Solution:**
1. Verify your AWS credentials are correct
2. Check IAM permissions include `s3:ListAllMyBuckets`
3. Try listing a specific bucket you own

### **Issue: Port 8000 already in use**

**Solution:**
```powershell
# Change port in .env
PORT=8001

# Or specify when running
python -m uvicorn app.main:app --port 8001
```

### **Issue: Module import errors**

**Solution:**
```powershell
# Make sure you're in the backend directory
cd backend

# Reinstall dependencies
pip install -r requirements.txt

# Run from backend directory
python -m app.main
```

---

## 📦 Dependencies

Main libraries used:

- **FastAPI**: Modern web framework for building APIs
- **Uvicorn**: ASGI server for running FastAPI
- **Boto3**: AWS SDK for Python (S3 operations)
- **PyIceberg**: Apache Iceberg Python library
- **Delta Lake**: Delta Lake Python bindings
- **PyArrow**: Parquet file reading

See [requirements.txt](requirements.txt) for complete list.

---

## 🗺️ Roadmap

- [x] Basic project structure
- [x] S3 connection and authentication
- [x] Health check endpoints
- [x] S3 object listing
- [ ] Iceberg metadata parsing
- [ ] Delta Lake metadata parsing
- [ ] Hudi metadata parsing
- [ ] Parquet schema reading
- [ ] Table schema visualization
- [ ] Partition information
- [ ] Snapshot/version history
- [ ] Sample data preview

---

## 📄 License

Open source - free for enterprise use.

---

## 👥 Contributing

This is a learning project. Feel free to:
- Report issues
- Suggest features
- Submit pull requests
- Ask questions

---

## 📞 Support

For questions or issues:
1. Check the troubleshooting section
2. Review API documentation at `/docs`
3. Check application logs
4. Verify AWS credentials and permissions

---

**Happy Coding! 🚀**


the flow:
User Side:
1. POST /auth/validate-credentials (with AWS keys)
   → Check if credentials work
   
2. POST /auth/create-session (with AWS keys)
   → Get session_id: "abc-123-xyz"
   
3. GET /datastore/info (with session_id in header)
   → No need to send credentials again!
   
4. GET /datastore/metadata (with session_id)
   → Still no credentials needed!
   
5. 30 minutes later → Session expires → User creates new session

Server Side:
- Stores: session_id → {credentials, expiry_time}
- On request: Check if session_id exists and not expired
- Fast lookup in memory (Redis in production)


