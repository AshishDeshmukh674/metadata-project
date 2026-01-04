# Spark Integration - Quick Start Guide

## 🚀 Setup (5 minutes)

### 1. Install Dependencies
```powershell
# Activate virtual environment
.\venv\Scripts\Activate.ps1

# Install Spark and related packages
pip install -r requirements.txt
```

**Note:** PySpark requires Java 8 or higher. If you don't have Java:
```powershell
# Check if Java is installed
java -version

# If not installed, download from:
# https://adoptium.net/
# Install Java 11 or 17 (LTS versions)
```

### 2. Test Spark Installation
```powershell
cd backend\scripts
python test_spark_local.py
```

**Expected output:**
```
🌟🌟🌟🌟🌟 SPARK INTEGRATION TEST SUITE 🌟🌟🌟🌟🌟

TEST 1: Spark Session Creation
✅ Spark session created successfully
   Spark version: 3.5.0
   Master: local[*]
   Web UI: http://localhost:4040

... (more tests) ...

🎉 All tests passed! Spark is working correctly.
```

### 3. Access Spark Web UI
While Spark is running, visit: **http://localhost:4040**

You'll see:
- Active jobs and stages
- SQL queries executed
- Memory usage
- Task execution times

---

## 📚 What Got Created

### 1. Configuration
**File:** `app/config/spark_config.py`
- SparkSession manager (singleton pattern)
- Optimized settings for local execution
- AWS S3 configuration
- Memory and parallelism tuning

### 2. Base Service
**File:** `app/services/spark_base.py`
- Common utilities for all Spark services
- DataFrame operations (filter, read, write)
- SQL execution
- Error handling
- Performance monitoring

### 3. Format Services

#### Parquet Service
**File:** `app/services/spark_parquet_service.py`
- Read Parquet files
- Execute SQL queries
- Modify and write back data
- Merge multiple files

#### Delta Lake Service
**File:** `app/services/spark_delta_service.py`
- Read with time travel
- ACID updates
- ACID deletes
- Upserts (MERGE operations)
- Transaction history

### 4. API Endpoints
**File:** `app/api/spark_routes.py`

**Parquet Endpoints:**
- `POST /api/spark/parquet/query` - Query Parquet files
- `POST /api/spark/parquet/modify` - Modify and save
- `POST /api/spark/parquet/merge` - Merge multiple files

**Delta Lake Endpoints:**
- `POST /api/spark/delta/read` - Read with time travel
- `POST /api/spark/delta/update` - ACID updates
- `POST /api/spark/delta/delete` - ACID deletes
- `POST /api/spark/delta/merge` - Upsert operations
- `POST /api/spark/delta/history` - Transaction log

---

## 🎯 Quick Examples

### Example 1: Query Parquet File
```bash
curl -X POST "http://localhost:8000/api/spark/parquet/query" \
  -H "Content-Type: application/json" \
  -d '{
    "aws_access_key_id": "YOUR_KEY",
    "aws_secret_access_key": "YOUR_SECRET",
    "bucket": "my-bucket",
    "key": "data/employees.parquet",
    "sql_query": "SELECT department, AVG(salary) FROM data_table GROUP BY department"
  }'
```

### Example 2: Update Delta Table
```bash
curl -X POST "http://localhost:8000/api/spark/delta/update" \
  -H "Content-Type: application/json" \
  -d '{
    "aws_access_key_id": "YOUR_KEY",
    "aws_secret_access_key": "YOUR_SECRET",
    "bucket": "my-bucket",
    "table_path": "delta/employees",
    "update_expr": {"salary": "salary * 1.1"},
    "condition": "department = '\''Engineering'\''"
  }'
```

### Example 3: Merge (Upsert) Data
```bash
curl -X POST "http://localhost:8000/api/spark/delta/merge" \
  -H "Content-Type: application/json" \
  -d '{
    "aws_access_key_id": "YOUR_KEY",
    "aws_secret_access_key": "YOUR_SECRET",
    "bucket": "my-bucket",
    "table_path": "delta/employees",
    "source_data": [
      {"id": 1, "name": "John", "salary": 120000},
      {"id": 999, "name": "Jane", "salary": 90000}
    ],
    "merge_key": "id"
  }'
```

---

## 🎓 Learning Path

### Day 1: Basics
1. ✅ Read [SPARK_GUIDE.md](SPARK_GUIDE.md)
2. ✅ Run `test_spark_local.py`
3. ✅ Understand lazy evaluation
4. ✅ Explore Spark Web UI

### Day 2: Practice
1. Upload sample data to S3
2. Try API endpoints in Swagger UI (http://localhost:8000/docs)
3. Experiment with SQL queries
4. Monitor execution in Spark UI

### Day 3: Advanced
1. Test with large files (> 100 MB)
2. Compare performance: DuckDB vs Spark
3. Try Delta Lake time travel
4. Implement upserts

---

## 📊 When to Use Spark vs Current Stack

| Use Case | Use This | Reason |
|----------|----------|--------|
| File < 100 MB | DuckDB | Faster for small files |
| File > 100 MB | Spark | Handles large files |
| Simple SELECT | DuckDB | Lower overhead |
| Complex joins/agg | Spark | Better optimization |
| Parquet read-only | DuckDB | Simpler |
| Need to UPDATE | Spark + Delta | ACID guarantees |
| Multiple files | Spark | Parallel processing |
| Hudi tables | Spark | Required |

---

## 🛠️ Troubleshooting

### Issue: "Java not found"
**Solution:**
1. Install Java 11 or 17 from https://adoptium.net/
2. Restart terminal
3. Verify: `java -version`

### Issue: "Out of memory"
**Solution:**
Edit `app/config/spark_config.py`:
```python
conf.set("spark.driver.memory", "8g")  # Increase from 4g
conf.set("spark.executor.memory", "8g")
```

### Issue: "Port 4040 already in use"
**Solution:**
Another Spark session is running. Stop it or use different port:
```python
conf.set("spark.ui.port", "4041")
```

### Issue: Slow performance
**Solution:**
1. Check number of partitions: `df.rdd.getNumPartitions()`
2. Repartition if needed: `df = df.repartition(8)`
3. Cache frequently used data: `df.cache()`

---

## 🎉 Success! What's Next?

### Production Checklist
- [ ] Test with your actual S3 data
- [ ] Benchmark performance vs current stack
- [ ] Implement error handling in your app
- [ ] Add monitoring and logging
- [ ] Document your SQL queries
- [ ] Set up automated tests

### Scaling Options (Future)
1. **Local Spark** (Current) - FREE, up to 20 GB
2. **AWS EMR** - Scale to TBs, ~$50-500/month
3. **Databricks** - Managed, ~$100-1000/month
4. **Your own cluster** - Full control

---

## 📖 Additional Resources

**Official Docs:**
- [PySpark API](https://spark.apache.org/docs/latest/api/python/)
- [Delta Lake](https://docs.delta.io/latest/)
- [Spark SQL Guide](https://spark.apache.org/docs/latest/sql-programming-guide.html)

**Tutorials:**
- [Spark Tutorial](https://spark.apache.org/docs/latest/quick-start.html)
- [Delta Lake Quickstart](https://docs.delta.io/latest/quick-start.html)

**Your Project Files:**
- [SPARK_GUIDE.md](SPARK_GUIDE.md) - Comprehensive guide
- `scripts/test_spark_local.py` - Test suite
- `app/config/spark_config.py` - Configuration
- `app/api/spark_routes.py` - API documentation

---

## 💡 Pro Tips

1. **Always check Spark UI** - See what's actually happening
2. **Filter early** - Push predicates down to file readers
3. **Select only needed columns** - Reduce data transfer
4. **Cache wisely** - Only cache data used multiple times
5. **Partition smart** - Balance between too many and too few
6. **Use Delta Lake** - ACID transactions are worth it

---

**Need help?** Check [SPARK_GUIDE.md](SPARK_GUIDE.md) for detailed explanations!
