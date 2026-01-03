# 🔍 Metastore Viewer Project

A **production-ready** web-based tool for exploring metadata from **Apache Iceberg**, **Delta Lake**, **Apache Hudi**, and **Parquet** tables stored in object storage (AWS S3, Azure Blob, MinIO) without requiring a traditional metastore.

---

## 🎯 What Problem Does This Solve?

Modern data lakes use self-describing table formats (Iceberg, Delta, Hudi) that store metadata alongside data. However, exploring this metadata typically requires:
- Setting up a metastore (Hive, AWS Glue, Databricks Unity Catalog)
- Registering tables manually
- Maintaining infrastructure

**This tool eliminates that complexity** by reading metadata directly from object storage, providing instant insights into:
- Table schemas (columns, types)
- Partitioning strategies
- Version history and snapshots
- File statistics and storage metrics

---

## 🔐 Architecture: User Credential Mode (Production-Ready)

**Configured for production** where each user provides their own AWS credentials:

```
User (Browser) 
    ↓
Frontend (React)
    ↓ AWS credentials (HTTPS)
Backend API
    ↓ Use user's credentials
AWS S3 (Private buckets)
```

**Users provide:**
- ✅ AWS Access Key ID
- ✅ AWS Secret Access Key
- ✅ S3 Path to explore

**Benefits:**
- 🔒 Per-user access control
- 📊 Audit trail per user
- 🚀 No shared credentials needed
- ✅ Production-ready security

---

## 🏗️ Project Structure

```
metadata/
├── backend/                  # FastAPI backend (Python)
│   ├── app/
│   │   ├── main.py          # Application entry point
│   │   ├── config.py        # Configuration management
│   │   ├── models/          # Data models
│   │   ├── services/        # Business logic (S3, parsers)
│   │   └── api/             # API endpoints
│   ├── requirements.txt
│   └── README.md
│
├── frontend/                 # React frontend (Coming soon)
│   └── (To be created)
│
├── .env.example             # Environment variables template
├── .gitignore               # Git ignore rules
└── README.md                # This file
```

---

## 🚀 Getting Started

### **Backend Setup**

1. **Navigate to backend directory:**
```powershell
cd backend
```

2. **Create virtual environment:**
```powershell
python -m venv venv
.\venv\Scripts\activate
```

3. **Install dependencies:**
```powershell
pip install -r requirements.txt
```

4. **Configure environment (Optional for Approach 1):**
```powershell
# Only needed if using shared service account
Copy-Item ..\.env.example .env
notepad .env
```

5. **Run the server:**
```powershell
python -m app.main
```

6. **Test the API:**
Open browser: http://localhost:8000/docs

See [backend/README.md](backend/README.md) for detailed instructions.

---

## 🔐 Two Ways to Use

### **Approach 1: Shared Backend Credentials**
- Admin configures AWS credentials in `.env`
- Users only provide S3 paths
- Best for: Internal tools

### **Approach 2: User-Provided Credentials** ⭐
- Each user provides their own AWS credentials
- Per-user access control
- Best for: SaaS, multi-tenant

**See [CREDENTIALS_GUIDE.md](CREDENTIALS_GUIDE.md) for complete comparison!**

---

## 📚 Documentation

- **Production Setup**: [PRODUCTION_SETUP.md](PRODUCTION_SETUP.md)
- **Backend Details**: [backend/README.md](backend/README.md)
- **API Docs**: http://localhost:8000/docs (when running)

---

## 🔒 Security

**Production-ready security features:**
- ✅ User-provided credentials (per-user access)
- ✅ Session-based authentication (30-min TTL)
- ✅ HTTPS enforcement (configurable)
- ✅ CORS protection
- ✅ No credentials stored in code
- ✅ Environment-based configuration

**For production:**
- Set `REQUIRE_HTTPS=true`
- Use Redis for session storage
- Restrict CORS to your domain
- Enable CloudWatch logging
- Add rate limiting

See [PRODUCTION_SETUP.md](PRODUCTION_SETUP.md) for security checklist.

---

## 🛣️ Development Roadmap

### Phase 1: Backend Foundation ✅
- [x] Project structure
- [x] FastAPI setup
- [x] User credential authentication
- [x] Session management
- [x] S3 connection & operations
- [x] Health check endpoints
- [x] Production-ready configuration

### Phase 2: Metadata Parsing (Next)
- [ ] Iceberg metadata reader
- [ ] Delta Lake transaction log parser
- [ ] Hudi timeline parser
- [ ] Parquet schema reader
- [ ] Unified metadata API

### Phase 3: Frontend
- [ ] React/Next.js setup
- [ ] Login page (AWS credentials)
- [ ] Session management
- [ ] Table schema viewer
- [ ] Partition explorer
- [ ] Snapshot timeline

### Phase 4: Advanced Features
- [ ] Schema evolution diff
- [ ] Sample data preview
- [ ] Trino query integration
- [ ] Metadata export
- [ ] Redis session storage

---

## 🛠️ Tech Stack

**Backend:**
- Python 3.10+
- FastAPI (web framework)
- Boto3 (AWS SDK)
- PyIceberg (Iceberg support)
- Delta Lake (Delta support)
- PyArrow (Parquet support)

**Frontend (Planned):**
- React + Next.js
- TypeScript
- TanStack Table
- Recharts (visualization)

---

## 📖 Learning Resources

- [Apache Iceberg Documentation](https://iceberg.apache.org/docs/latest/)
- [Delta Lake Documentation](https://docs.delta.io/latest/)
- [Apache Hudi Documentation](https://hudi.apache.org/docs/overview)
- [AWS S3 Best Practices](https://docs.aws.amazon.com/AmazonS3/latest/userguide/)

---

## 🤝 Contributing

This is a learning project. Contributions welcome!

---

## 📄 License

Open source - free for enterprise use.

---

**Made with ❤️ for the data community**
