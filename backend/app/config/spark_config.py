"""
Apache Spark Configuration
===========================

WHAT IS SPARK?
--------------
Apache Spark is a distributed computing engine that processes large datasets
in parallel across multiple CPU cores (locally) or machines (in a cluster).

Think of it like this:
- Normal Python: One person doing all the work sequentially
- Spark: Multiple people working on different parts simultaneously

KEY CONCEPTS:
-------------
1. SparkSession: The entry point to Spark - creates and manages everything
2. DataFrame: Like pandas DataFrame, but distributed across partitions
3. Partition: A chunk of data that Spark processes independently
4. Lazy Evaluation: Spark builds a plan first, executes only when needed
5. Action vs Transformation:
   - Transformation: Define what to do (filter, select, join) - lazy
   - Action: Actually execute and get results (count, show, collect)

EXAMPLE FLOW:
-------------
df = spark.read.parquet("s3://bucket/file.parquet")  # Transformation (lazy)
df = df.filter(df.age > 25)                          # Transformation (lazy)
df = df.select("name", "salary")                     # Transformation (lazy)
result = df.count()                                  # Action (executes now!)

Spark will optimize all transformations before executing.
"""

import logging
from typing import Optional
from pyspark.sql import SparkSession
from pyspark import SparkConf

logger = logging.getLogger(__name__)


class SparkConfig:
    """
    Manages Apache Spark configuration and session lifecycle.
    
    This class creates and manages a local Spark session optimized for
    processing data formats like Parquet, Delta, Hudi, and Iceberg.
    """
    
    # Singleton instance
    _spark_session: Optional[SparkSession] = None
    
    @classmethod
    def get_spark_session(cls, app_name: str = "MetadataAPI") -> SparkSession:
        """
        Get or create a Spark session (Singleton pattern).
        
        WHAT THIS DOES:
        ---------------
        1. Creates a SparkSession if none exists
        2. Configures Spark for local mode with optimal settings
        3. Adds support for Delta Lake, Hudi, and Iceberg
        4. Sets up AWS S3 access
        
        Args:
            app_name: Name of your Spark application
            
        Returns:
            SparkSession instance
            
        CONFIGURATION EXPLAINED:
        ------------------------
        """
        if cls._spark_session is not None:
            logger.info("♻️ Reusing existing Spark session")
            return cls._spark_session
        
        logger.info("🚀 Creating new Spark session...")
        
        # Build Spark configuration
        conf = SparkConf()
        
        # ============================================================
        # MASTER: Where Spark runs
        # ============================================================
        # local[*] = Run locally, use all available CPU cores
        # local[4] = Run locally, use 4 cores
        # spark://host:port = Connect to Spark cluster
        conf.set("spark.master", "local[*]")
        
        # ============================================================
        # MEMORY CONFIGURATION
        # ============================================================
        # Driver memory: Memory for the main Spark program
        # Executor memory: Memory for workers (in local mode, same as driver)
        conf.set("spark.driver.memory", "4g")          # 4GB for driver
        conf.set("spark.executor.memory", "4g")        # 4GB per executor
        
        # Off-heap memory for better performance with large datasets
        conf.set("spark.memory.offHeap.enabled", "true")
        conf.set("spark.memory.offHeap.size", "2g")
        
        # ============================================================
        # EXECUTION CONFIGURATION
        # ============================================================
        # Default parallelism: Number of partitions for operations like join
        # Rule of thumb: 2-4 partitions per CPU core
        conf.set("spark.default.parallelism", "8")
        
        # SQL shuffle partitions: Used for joins, aggregations
        # Default is 200 (too high for small datasets)
        conf.set("spark.sql.shuffle.partitions", "8")
        
        # ============================================================
        # PERFORMANCE OPTIMIZATIONS
        # ============================================================
        # Adaptive Query Execution: Spark automatically optimizes queries
        conf.set("spark.sql.adaptive.enabled", "true")
        
        # Dynamic partition pruning: Skip reading unnecessary partitions
        conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
        
        # Broadcast join threshold: Auto-broadcast small tables (10MB)
        conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760")
        
        # ============================================================
        # S3 CONFIGURATION
        # ============================================================
        # Hadoop AWS libraries for S3 access
        conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        
        # S3A fast upload (better performance)
        conf.set("spark.hadoop.fs.s3a.fast.upload", "true")
        conf.set("spark.hadoop.fs.s3a.fast.upload.buffer", "bytebuffer")
        
        # Connection settings
        conf.set("spark.hadoop.fs.s3a.connection.maximum", "100")
        conf.set("spark.hadoop.fs.s3a.threads.max", "20")
        
        # Path style access (compatible with more S3 implementations)
        conf.set("spark.hadoop.fs.s3a.path.style.access", "true")
        
        # ============================================================
        # DELTA LAKE CONFIGURATION
        # ============================================================
        # Enable Delta Lake extensions and catalog
        conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        
        # ============================================================
        # SERIALIZATION (How Spark transfers data)
        # ============================================================
        # Kryo is faster than default Java serialization
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        
        # ============================================================
        # UI CONFIGURATION
        # ============================================================
        # Spark UI port (access at http://localhost:4040)
        conf.set("spark.ui.port", "4040")
        conf.set("spark.ui.enabled", "true")
        
        # ============================================================
        # LOG LEVEL
        # ============================================================
        # Reduce Spark's verbose logging
        conf.set("spark.driver.extraJavaOptions", "-Dlog4j.configuration=file:log4j.properties")
        
        # Create SparkSession with all configurations
        spark = (
            SparkSession.builder
            .appName(app_name)
            .config(conf=conf)
            # Add Delta Lake support (using compatible version 2.4.0 for Spark 3.5.0)
            .config("spark.jars.packages", 
                   "io.delta:delta-core_2.12:2.4.0,"
                   "org.apache.hadoop:hadoop-aws:3.3.4,"
                   "com.amazonaws:aws-java-sdk-bundle:1.12.262")
            .getOrCreate()
        )
        
        # Set log level to reduce noise
        spark.sparkContext.setLogLevel("WARN")
        
        # Cache the session
        cls._spark_session = spark
        
        logger.info("✅ Spark session created successfully")
        logger.info(f"   App Name: {app_name}")
        logger.info(f"   Spark Version: {spark.version}")
        logger.info(f"   Master: {spark.sparkContext.master}")
        logger.info(f"   Web UI: http://localhost:4040")
        
        return spark
    
    @classmethod
    def configure_aws_credentials(
        cls,
        spark: SparkSession,
        access_key: str,
        secret_key: str,
        session_token: Optional[str] = None
    ):
        """
        Configure AWS credentials for S3 access.
        
        WHAT THIS DOES:
        ---------------
        Sets AWS credentials in Spark's Hadoop configuration so Spark
        can read/write files from S3.
        
        Args:
            spark: SparkSession instance
            access_key: AWS Access Key ID
            secret_key: AWS Secret Access Key
            session_token: Optional AWS Session Token (for temporary credentials)
        """
        logger.info("🔐 Configuring AWS credentials for Spark")
        
        hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
        
        # Set AWS credentials
        hadoop_conf.set("fs.s3a.access.key", access_key)
        hadoop_conf.set("fs.s3a.secret.key", secret_key)
        
        if session_token:
            hadoop_conf.set("fs.s3a.session.token", session_token)
            # Use temporary credentials provider
            hadoop_conf.set(
                "fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider"
            )
        else:
            # Use simple credentials provider
            hadoop_conf.set(
                "fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
            )
        
        logger.info("✅ AWS credentials configured")
    
    @classmethod
    def stop_spark_session(cls):
        """
        Stop the Spark session and release resources.
        
        WHEN TO USE:
        ------------
        - When shutting down the application
        - When you want to reconfigure Spark
        - To free up memory
        """
        if cls._spark_session is not None:
            logger.info("🛑 Stopping Spark session...")
            cls._spark_session.stop()
            cls._spark_session = None
            logger.info("✅ Spark session stopped")
    
    @classmethod
    def get_session_info(cls) -> dict:
        """
        Get information about the current Spark session.
        
        Returns:
            Dictionary with session information
        """
        if cls._spark_session is None:
            return {"active": False}
        
        spark = cls._spark_session
        sc = spark.sparkContext
        
        return {
            "active": True,
            "app_name": sc.appName,
            "spark_version": spark.version,
            "master": sc.master,
            "ui_web_url": sc.uiWebUrl,
            "default_parallelism": sc.defaultParallelism,
            "active_jobs": len(sc.statusTracker().getActiveJobIds()),
        }


# Convenience function for quick access
def get_spark() -> SparkSession:
    """
    Quick access to Spark session.
    
    Example usage:
    --------------
    from app.config.spark_config import get_spark
    
    spark = get_spark()
    df = spark.read.parquet("s3a://bucket/file.parquet")
    df.show()
    """
    return SparkConfig.get_spark_session()
