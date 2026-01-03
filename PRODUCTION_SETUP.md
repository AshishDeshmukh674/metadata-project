# 🚀 Production Setup Guide

## Configuration

Your application uses **User Credential Mode** - each user provides their own AWS credentials.

```env
CREDENTIAL_MODE=user
SESSION_TTL_MINUTES=30
REQUIRE_HTTPS=false  # Set true for production
```

---

## 🚀 Quick Start (Development)

### **1. Start the Server**

```powershell
cd backend
.\venv\Scripts\activate
python -m app.main
```

You'll see:
```
🚀 Starting Metastore Viewer v1.0.0
✅ Configuration loaded successfully
   Environment: development
   Credential Mode: USER
   AWS Region: us-east-1
   
📋 USER CREDENTIAL MODE (Production-Ready)
   Users will provide their own AWS credentials
   Backend credentials not required

📝 Available endpoints for users:
   1. POST /api/v1/user/validate-credentials
   2. POST /api/v1/user/create-session
   3. POST /api/v1/user/list-objects-with-session

🔒 Security features enabled:
   - Session TTL: 30 minutes
   - HTTPS required: false (development)
   
📚 API Documentation: http://0.0.0.0:8000/docs
```

### **2. Test with Swagger UI**

Open: http://localhost:8000/docs

Try these endpoints:

**Step 1: Validate Your Credentials**
```
POST /api/v1/user/validate-credentials
{
  "aws_access_key_id": "YOUR_AWS_KEY",
  "aws_secret_access_key": "YOUR_AWS_SECRET",
  "aws_region": "us-east-1"
}
```

**Step 2: Create Session**
```
POST /api/v1/user/create-session
{
  "aws_access_key_id": "YOUR_AWS_KEY",
  "aws_secret_access_key": "YOUR_AWS_SECRET",
  "aws_region": "us-east-1"
}

Response:
{
  "session_id": "abc-123-...",
  "expires_in_minutes": 30
}
```

**Step 3: List Objects Using Session**
```
POST /api/v1/user/list-objects-with-session
Headers:
  X-Session-ID: abc-123-...
Body:
{
  "s3_path": "s3://your-bucket/path/"
}
```

---

## Production Deployment

### **1. Environment Configuration**

Create production `.env`:

```env
# Production configuration
APP_NAME=Metastore Viewer
ENVIRONMENT=production
DEBUG=false
HOST=0.0.0.0
PORT=8000

# Credential mode
CREDENTIAL_MODE=user

# Security settings
SESSION_TTL_MINUTES=30
REQUIRE_HTTPS=true  # ⚠️ IMPORTANT: Set to true!

# AWS
AWS_DEFAULT_REGION=us-east-1

# CORS - restrict to your frontend domain
CORS_ORIGINS=https://yourdomain.com

# Logging
LOG_LEVEL=INFO
```

### **2. HTTPS Setup**

**Option A: Behind Load Balancer (AWS ALB/ELB)**
```
Internet → HTTPS → Load Balancer → HTTP → Your App
```

**Option B: Direct HTTPS with Nginx**
```nginx
server {
    listen 443 ssl;
    server_name api.yourdomain.com;
    
    ssl_certificate /path/to/cert.pem;
    ssl_certificate_key /path/to/key.pem;
    
    location / {
        proxy_pass http://localhost:8000;
        proxy_set_header X-Forwarded-Proto https;
    }
}
```

**Option C: Using Uvicorn with SSL**
```python
# main.py
if __name__ == "__main__":
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=443,
        ssl_keyfile="/path/to/key.pem",
        ssl_certfile="/path/to/cert.pem"
    )
```

### **3. Session Storage (Redis)**

**Install Redis:**
```powershell
# Windows: Download from https://github.com/microsoftarchive/redis/releases
# Linux: sudo apt install redis-server
# Docker: docker run -d -p 6379:6379 redis
```

**Update requirements.txt:**
```txt
redis==5.0.1
```

**Create Redis session storage:**
```python
# app/services/session_store.py
import redis
import json
from app.config import settings

redis_client = redis.Redis(
    host='localhost',
    port=6379,
    decode_responses=True
)

def store_session(session_id: str, credentials: dict, ttl_seconds: int):
    redis_client.setex(
        f"session:{session_id}",
        ttl_seconds,
        json.dumps(credentials)
    )

def get_session(session_id: str) -> dict:
    data = redis_client.get(f"session:{session_id}")
    return json.loads(data) if data else None

def delete_session(session_id: str):
    redis_client.delete(f"session:{session_id}")
```

### **4. Docker Deployment**

**Create Dockerfile:**
```dockerfile
FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application
COPY app/ ./app/

# Expose port
EXPOSE 8000

# Run application
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
```

**docker-compose.yml:**
```yaml
version: '3.8'

services:
  backend:
    build: ./backend
    ports:
      - "8000:8000"
    environment:
      - CREDENTIAL_MODE=user
      - SESSION_TTL_MINUTES=30
      - REQUIRE_HTTPS=true
      - REDIS_HOST=redis
    depends_on:
      - redis
  
  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"
    volumes:
      - redis-data:/data

volumes:
  redis-data:
```

**Run:**
```bash
docker-compose up -d
```

---

## Security Checklist

### **Must Have:**
- [ ] `REQUIRE_HTTPS=true` in production
- [ ] CORS restricted to your frontend domain
- [ ] Use Redis for session storage (not in-memory)
- [ ] Set `SESSION_TTL_MINUTES` to 15-30 minutes
- [ ] Enable rate limiting (use Nginx or FastAPI middleware)
- [ ] Log all access (but never log credentials)
- [ ] Use environment variables (never hardcode)

### **Recommended:**
- [ ] Add API authentication (OAuth/JWT)
- [ ] Implement request logging
- [ ] Add CloudWatch/Datadog monitoring
- [ ] Set up AWS WAF (Web Application Firewall)
- [ ] Enable CloudTrail for AWS API auditing
- [ ] Add health check endpoint monitoring
- [ ] Implement session cleanup job
- [ ] Add user authentication layer

### **Advanced:**
- [ ] AWS Cognito integration
- [ ] Multi-factor authentication (MFA)
- [ ] Encryption at rest for sessions
- [ ] Certificate pinning for mobile apps
- [ ] Anomaly detection for suspicious activity

---

## Monitoring

### **Key Metrics to Track:**

```python
# Add to endpoints
from prometheus_client import Counter, Histogram

session_created = Counter('sessions_created_total', 'Total sessions created')
credential_validation = Counter('credential_validations', 'Credential validation attempts', ['status'])
s3_requests = Histogram('s3_request_duration_seconds', 'S3 request duration')
```

### **CloudWatch Logs:**
```python
import watchtower
import logging

logger = logging.getLogger(__name__)
logger.addHandler(watchtower.CloudWatchLogHandler())
```

---

## Testing Production Setup

### **1. Test HTTPS Enforcement**
```bash
# Should fail in production with REQUIRE_HTTPS=true
curl -k http://localhost:8000/api/v1/user/validate-credentials \
  -d '{"aws_access_key_id":"...","aws_secret_access_key":"..."}'
```

### **2. Test Session Expiration**
```bash
# Create session
SESSION_ID=$(curl -X POST http://localhost:8000/api/v1/user/create-session \
  -H "Content-Type: application/json" \
  -d '{"aws_access_key_id":"...","aws_secret_access_key":"..."}' \
  | jq -r '.session_id')

# Wait for TTL to expire (30+ minutes)
# Then try to use session (should fail)
curl -X POST http://localhost:8000/api/v1/user/list-objects-with-session \
  -H "X-Session-ID: $SESSION_ID" \
  -d '{"s3_path":"s3://bucket/path/"}'
```

### **3. Load Testing**
```bash
# Install Apache Bench
# Windows: https://httpd.apache.org/docs/current/programs/ab.html

# Test endpoint performance
ab -n 1000 -c 10 -T 'application/json' \
  -p credentials.json \
  http://localhost:8000/api/v1/user/validate-credentials
```

---

## Scaling

### **Horizontal Scaling:**
```
          Load Balancer
               |
    +----------+----------+
    |          |          |
  App-1      App-2      App-3
    |          |          |
    +----------+----------+
               |
            Redis
```

**Requirements:**
- Shared Redis for sessions (not in-memory)
- Stateless application (no local file storage)
- Health check endpoint
- Graceful shutdown handling

### **Auto-scaling Rules:**
```yaml
# AWS ECS example
autoScaling:
  minCapacity: 2
  maxCapacity: 10
  targetCPU: 70
  targetMemory: 80
```

---

## User Requirements

### **Minimal Setup:**
Users only need to provide:
1. Their AWS Access Key ID
2. Their AWS Secret Access Key
3. AWS Region (optional, defaults to us-east-1)

### **User Instructions:**

**Getting AWS Credentials:**
```
1. Log into AWS Console
2. Go to IAM → Users → Your Username
3. Security Credentials tab
4. Create Access Key
5. Download credentials (shown only once!)
```

**Required IAM Permissions:**
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:ListBucket",
        "s3:GetObject",
        "s3:GetBucketLocation"
      ],
      "Resource": [
        "arn:aws:s3:::your-bucket",
        "arn:aws:s3:::your-bucket/*"
      ]
    }
  ]
}
```

---

## Summary

Production-ready features:
- User credential mode
- 30-minute session TTL
- HTTPS enforcement
- Session-based authentication

**Next:** Build frontend, deploy to cloud, add Redis for sessions.
