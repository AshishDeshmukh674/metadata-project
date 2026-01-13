from pyspark.sql import SparkSession
from pyspark.sql.types import *
import boto3
import os
import shutil

# ---------------------------
# CONFIG
# ---------------------------
S3_BUCKET = "metadataproject"  # Just bucket name, no s3:// prefix
BASE_S3_PATH = "test-data/sample-data"
LOCAL_OUTPUT = "output"

ICEBERG_PATH = f"{LOCAL_OUTPUT}/iceberg/sales_iceberg"
DELTA_PATH   = f"{LOCAL_OUTPUT}/delta/sales_delta"
HUDI_PATH    = f"{LOCAL_OUTPUT}/hudi/sales_hudi"

# ---------------------------
# CLEAN OLD DATA
# ---------------------------
if os.path.exists(LOCAL_OUTPUT):
    # Don't delete the directory itself (it might be a Docker volume)
    # Just clean its contents
    for item in os.listdir(LOCAL_OUTPUT):
        item_path = os.path.join(LOCAL_OUTPUT, item)
        if os.path.isfile(item_path):
            os.remove(item_path)
        elif os.path.isdir(item_path):
            shutil.rmtree(item_path)
else:
    os.makedirs(LOCAL_OUTPUT)

# ---------------------------
# SPARK SESSION
# ---------------------------
spark = SparkSession.builder \
    .appName("UnifiedTableGenerator") \
    .config("spark.jars.packages",
            ",".join([
                "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0",
                "io.delta:delta-spark_2.12:3.1.0",
                "org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0"
            ])) \
    .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
            "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.type", "hadoop") \
    .config("spark.sql.catalog.local.warehouse", LOCAL_OUTPUT) \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar") \
    .getOrCreate()

# ---------------------------
# SAMPLE DATA
# ---------------------------
data = [
    (1, 101, 250.5, "2024-01-01", "IN"),
    (2, 102, 300.0, "2024-01-01", "US"),
    (3, 103, 150.0, "2024-01-02", "IN"),
    (4, 104, 500.0, "2024-01-02", "EU")
]

schema = StructType([
    StructField("order_id", LongType(), False),
    StructField("customer_id", LongType(), False),
    StructField("amount", DoubleType(), False),
    StructField("order_date", StringType(), False),
    StructField("region", StringType(), False),
])

df = spark.createDataFrame(data, schema)

# ---------------------------
# 1️⃣ ICEBERG
# ---------------------------
# Write Iceberg table directly to path
df.write \
  .format("iceberg") \
  .mode("overwrite") \
  .option("path", ICEBERG_PATH) \
  .saveAsTable("local.sales.iceberg_table")

print("✅ Iceberg table created")

# ---------------------------
# 2️⃣ DELTA
# ---------------------------
df.write \
  .format("delta") \
  .partitionBy("order_date") \
  .mode("overwrite") \
  .save(DELTA_PATH)

print("✅ Delta table created")

# ---------------------------
# 3️⃣ HUDI
# ---------------------------
try:
    hudi_options = {
        'hoodie.table.name': 'sales_hudi',
        'hoodie.datasource.write.recordkey.field': 'order_id',
        'hoodie.datasource.write.partitionpath.field': 'order_date',
        'hoodie.datasource.write.table.type': 'COPY_ON_WRITE',
        'hoodie.datasource.write.precombine.field': 'order_id',
        'hoodie.datasource.write.operation': 'insert'
    }

    df.write.format("hudi") \
      .options(**hudi_options) \
      .mode("overwrite") \
      .save(f"file:///{os.path.abspath(HUDI_PATH).replace(chr(92), '/')}")

    print("✅ Hudi table created")
except Exception as e:
    print(f"⚠️  Hudi table creation failed: {e}")
    print("✅ Continuing with Delta and Iceberg tables...")

spark.stop()

# ---------------------------
# UPLOAD TO S3
# ---------------------------
print("\n🚀 Uploading tables to S3...")
s3 = boto3.client("s3")

def upload_dir(local_dir, s3_prefix):
    if not os.path.exists(local_dir):
        print(f"⚠️  Directory not found: {local_dir}")
        return
    
    file_count = 0
    for root, _, files in os.walk(local_dir):
        for file in files:
            full_path = os.path.join(root, file)
            s3_key = os.path.join(
                s3_prefix,
                os.path.relpath(full_path, local_dir)
            ).replace("\\", "/")
            
            try:
                s3.upload_file(full_path, S3_BUCKET, s3_key)
                file_count += 1
            except Exception as e:
                print(f"❌ Failed to upload {file}: {e}")
    
    print(f"✅ Uploaded {file_count} files from {local_dir}")

# Upload Iceberg
upload_dir(ICEBERG_PATH, f"{BASE_S3_PATH}/iceberg/sales_iceberg")

# Upload Delta  
upload_dir(DELTA_PATH, f"{BASE_S3_PATH}/delta/sales_delta")

# Upload Hudi if it exists
if os.path.exists(HUDI_PATH):
    upload_dir(HUDI_PATH, f"{BASE_S3_PATH}/hudi/sales_hudi")

print("\n🎉 All tables uploaded to S3 successfully!")
print(f"S3 Location: s3://{S3_BUCKET}/{BASE_S3_PATH}/")
upload_dir(HUDI_PATH,    f"{BASE_S3_PATH}/hudi/sales_hudi")

print("🚀 All tables uploaded to S3")
