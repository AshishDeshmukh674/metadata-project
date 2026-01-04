# Apache Spark Integration Guide

## 🎯 What is Apache Spark?

Apache Spark is a **distributed computing engine** that processes large datasets in parallel. Think of it as having multiple workers processing different parts of your data simultaneously instead of one person doing everything sequentially.

### Key Benefits
- ✅ Handle files larger than your computer's memory
- ✅ Process data 10-100x faster than single-threaded code
- ✅ Unified engine for batch and streaming data
- ✅ Native support for Parquet, Delta Lake, Hudi, Iceberg
- ✅ SQL, Python, and DataFrame APIs

---

## 📚 Core Concepts

### 1. SparkSession
**The entry point to all Spark functionality.**

```python
from app.config.spark_config import get_spark

# Get Spark session (creates if needed, reuses if exists)
spark = get_spark()
```

**What it does:**
- Creates and configures Spark
- Manages resources (CPU, memory)
- Provides access to DataFrame APIs
- Handles distributed execution

---

### 2. DataFrame
**Distributed, immutable table of data.**

```python
# Read data (lazy - no computation yet)
df = spark.read.parquet("s3a://bucket/data.parquet")

# Transform data (still lazy)
df = df.filter(df.age > 25)
df = df.select("name", "salary")

# Action (triggers computation!)
result = df.count()
```

**Key Points:**
- Like pandas DataFrame, but distributed
- Divided into partitions processed in parallel
- Immutable (transformations create new DataFrames)
- Optimized by Catalyst query optimizer

---

### 3. Lazy Evaluation
**Spark builds an execution plan first, executes only when needed.**

#### Transformations (Lazy)
Operations that define what to do:
- `filter()`, `select()`, `join()`, `groupBy()`, `orderBy()`
- Build execution plan (DAG - Directed Acyclic Graph)
- No actual computation

#### Actions (Eager)
Operations that trigger execution:
- `count()`, `collect()`, `show()`, `write()`
- Execute entire plan
- Return results

**Example:**
```python
# All lazy - instant execution
df = spark.read.parquet("file.parquet")  # No file read yet
df = df.filter(df.age > 25)              # No filtering yet
df = df.select("name")                   # No selection yet

# Action - NOW everything executes
df.show()  # Reads file, filters, selects, displays
```

**Why Lazy?**
- Spark optimizes the entire plan before executing
- Eliminates unnecessary operations
- Minimizes data movement

---

### 4. Partitions
**Data is split into chunks processed independently.**

```python
# Check number of partitions
num_partitions = df.rdd.getNumPartitions()

# Repartition for better parallelism
df = df.repartition(8)  # 8 partitions

# Coalesce to reduce partitions (more efficient than repartition)
df = df.coalesce(2)  # 2 partitions
```

**Rules of Thumb:**
- Each partition = 100-200 MB (ideal)
- Num partitions = 2-4x your CPU cores
- Too many = overhead, Too few = no parallelism

---

## 🔄 Data Processing Pipeline

### Standard Spark Workflow

```
┌─────────────┐
│   READ      │  Load data from source
│  (Lazy)     │  - S3, HDFS, local
└──────┬──────┘
       │
       ▼
┌─────────────┐
│ TRANSFORM   │  Apply operations
│  (Lazy)     │  - filter, select, join
└──────┬──────┘  - groupBy, aggregate
       │
       ▼
┌─────────────┐
│  ACTION     │  Execute and get results
│  (Eager)    │  - count, show, write
└─────────────┘
```

---

## 💾 Reading Data

### Parquet
```python
from app.services.spark_parquet_service import spark_parquet_service

result = spark_parquet_service.read_and_query(
    bucket="my-bucket",
    key="data/users.parquet",
    credentials=credentials,
    columns=["id", "name", "salary"],  # Column pruning
    filters={"age": {">": 25}},         # Predicate pushdown
    limit=100
)
```

### Delta Lake
```python
from app.services.spark_delta_service import spark_delta_service

result = spark_delta_service.read_delta_table(
    bucket="my-bucket",
    table_path="delta/users",
    credentials=credentials,
    version=5,  # Time travel to version 5
)
```

---

## ✏️ Modifying Data

### Parquet (Create New File)
```python
result = spark_parquet_service.modify_and_write_back(
    bucket="my-bucket",
    key="data/users.parquet",
    credentials=credentials,
    sql_modification="UPDATE data_table SET salary = salary * 1.1 WHERE department = 'IT'",
    output_key="data/users_modified.parquet"
)
```

### Delta Lake (ACID Updates)
```python
# UPDATE
result = spark_delta_service.update_delta_table(
    bucket="my-bucket",
    table_path="delta/users",
    credentials=credentials,
    update_expr={"salary": "salary * 1.1"},
    condition="department = 'IT'"
)

# DELETE
result = spark_delta_service.delete_from_delta_table(
    bucket="my-bucket",
    table_path="delta/users",
    credentials=credentials,
    condition="is_active = false"
)

# MERGE (Upsert)
result = spark_delta_service.merge_into_delta_table(
    bucket="my-bucket",
    table_path="delta/users",
    credentials=credentials,
    source_data=[
        {"id": 1, "name": "John", "salary": 120000},
        {"id": 999, "name": "Jane", "salary": 90000}
    ],
    merge_key="id"
)
```

---

## 🎛️ Configuration

### Memory Settings
Located in `app/config/spark_config.py`:

```python
# Driver memory (main program)
conf.set("spark.driver.memory", "4g")

# Executor memory (workers)
conf.set("spark.executor.memory", "4g")

# Off-heap memory (better for large datasets)
conf.set("spark.memory.offHeap.enabled", "true")
conf.set("spark.memory.offHeap.size", "2g")
```

### Parallelism
```python
# Default parallelism for operations
conf.set("spark.default.parallelism", "8")

# SQL shuffle partitions (joins, aggregations)
conf.set("spark.sql.shuffle.partitions", "8")
```

### S3 Access
```python
# S3A connector (better than s3://)
conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

# Fast uploads
conf.set("spark.hadoop.fs.s3a.fast.upload", "true")

# Connection pool
conf.set("spark.hadoop.fs.s3a.connection.maximum", "100")
```

---

## 🚀 Performance Tips

### 1. Filter Early
```python
# ❌ Bad - reads all data then filters
df = spark.read.parquet("file.parquet")
df = df.filter(df.age > 25)

# ✅ Good - pushes filter to Parquet reader
df = spark.read.parquet("file.parquet").filter(df.age > 25)
```

### 2. Select Only Needed Columns
```python
# ❌ Bad - reads all 50 columns
df = spark.read.parquet("file.parquet")
result = df.select("name", "salary")

# ✅ Good - reads only 2 columns
df = spark.read.parquet("file.parquet").select("name", "salary")
```

### 3. Use Partitioning
```python
# Write with partitioning
df.write.partitionBy("year", "month").parquet("output")

# Creates structure:
# output/year=2024/month=01/data.parquet
# output/year=2024/month=02/data.parquet

# Query filters automatically skip unnecessary partitions
df = spark.read.parquet("output").filter("year = 2024 AND month = 1")
```

### 4. Cache Frequently Used DataFrames
```python
df = spark.read.parquet("large_file.parquet")
df = df.filter(df.age > 25)

# Cache in memory for repeated use
df.cache()

# Now both operations use cached data
count1 = df.count()
count2 = df.groupBy("department").count().count()
```

### 5. Broadcast Small Tables in Joins
```python
from pyspark.sql.functions import broadcast

# Small table (< 10 MB)
small_df = spark.read.parquet("small.parquet")

# Large table
large_df = spark.read.parquet("large.parquet")

# Broadcast small table to all workers
result = large_df.join(broadcast(small_df), "id")
```

---

## 🔍 Monitoring

### Spark Web UI
**Access at:** http://localhost:4040 (when Spark is running)

**What you see:**
- Jobs and stages
- Task execution times
- Memory usage
- SQL query plans
- Storage (cached DataFrames)

### Logging
```python
# Check Spark session info
from app.config.spark_config import SparkConfig

info = SparkConfig.get_session_info()
print(info)
# {
#   "active": True,
#   "app_name": "MetadataAPI",
#   "spark_version": "3.5.0",
#   "master": "local[*]",
#   "ui_web_url": "http://localhost:4040",
#   "default_parallelism": 8
# }
```

---

## 🆚 Spark vs Current Stack

| Feature | DuckDB/Python | Local Spark | Spark Cluster |
|---------|---------------|-------------|---------------|
| File size limit | RAM size | 2-3x RAM | Unlimited |
| Speed (small files) | ⚡⚡⚡ Fast | ⚡⚡ Good | ⚡ Slower (overhead) |
| Speed (large files) | ❌ Fails | ⚡⚡⚡ Fast | ⚡⚡⚡⚡ Very Fast |
| Setup complexity | Low | Medium | High |
| Cost | Free | Free | $$$ |
| Delta Lake support | Limited | ✅ Full | ✅ Full |
| Hudi support | ❌ No | ✅ Yes | ✅ Yes |
| Streaming | ❌ No | ✅ Yes | ✅ Yes |

---

## 📖 Example: Complete Workflow

```python
from app.services.spark_parquet_service import spark_parquet_service
from app.models.user_credentials import UserAWSCredentials

# Setup credentials
creds = UserAWSCredentials(
    aws_access_key_id="YOUR_KEY",
    aws_secret_access_key="YOUR_SECRET",
    aws_region="us-east-1"
)

# 1. Read and filter data
result = spark_parquet_service.read_and_query(
    bucket="my-bucket",
    key="data/employees.parquet",
    credentials=creds,
    filters={"department": "Engineering", "salary": {">": 100000}},
    limit=100
)

print(f"Found {result['stats']['total_rows']} employees")
print(f"Sample: {result['data'][:3]}")

# 2. Modify data
modify_result = spark_parquet_service.modify_and_write_back(
    bucket="my-bucket",
    key="data/employees.parquet",
    credentials=creds,
    sql_modification="""
        SELECT 
            *,
            salary * 1.1 as new_salary,
            CASE 
                WHEN salary > 150000 THEN 'Senior'
                ELSE 'Junior'
            END as level
        FROM data_table
    """,
    output_key="data/employees_processed.parquet",
    partition_by=["department"]
)

print(f"Wrote {modify_result['final_rows']} rows")

# 3. Merge multiple files
merge_result = spark_parquet_service.merge_multiple_files(
    bucket="my-bucket",
    keys=[
        "data/employees_2024_01.parquet",
        "data/employees_2024_02.parquet",
        "data/employees_2024_03.parquet"
    ],
    credentials=creds,
    output_key="data/employees_q1_2024.parquet",
    deduplicate_on=["employee_id"]
)

print(f"Merged {merge_result['files_merged']} files into {merge_result['total_rows']} rows")
```

---

## 🛠️ Troubleshooting

### Out of Memory
```python
# Reduce driver memory usage
df = df.limit(1000)  # Process less data

# Increase memory in spark_config.py
conf.set("spark.driver.memory", "8g")  # Increase from 4g to 8g
```

### Slow Performance
```python
# Check number of partitions
print(df.rdd.getNumPartitions())

# Repartition if needed
df = df.repartition(8)  # Match CPU cores

# Cache if reusing
df.cache()
```

### Connection Timeout
```python
# Increase S3 timeouts in spark_config.py
conf.set("spark.hadoop.fs.s3a.connection.timeout", "600000")
conf.set("spark.hadoop.fs.s3a.connection.establish.timeout", "10000")
```

---

## 📚 Next Steps

1. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Test Spark integration:**
   ```bash
   python scripts/test_spark_local.py
   ```

3. **Access Spark UI:**
   - Start your app
   - Visit http://localhost:4040
   - See live job execution

4. **Read more:**
   - [Official Spark Documentation](https://spark.apache.org/docs/latest/)
   - [Delta Lake Guide](https://docs.delta.io/latest/index.html)
   - [PySpark API Reference](https://spark.apache.org/docs/latest/api/python/)

---

## 🎓 Learning Resources

- **Spark SQL Guide:** https://spark.apache.org/docs/latest/sql-programming-guide.html
- **DataFrame API:** https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html
- **Performance Tuning:** https://spark.apache.org/docs/latest/sql-performance-tuning.html
- **Delta Lake Quickstart:** https://docs.delta.io/latest/quick-start.html
