"""
Spark Service Base Class
=========================

SPARK PIPELINE PATTERN:
-----------------------
Every Spark data processing follows this pattern:

1. READ: Load data from source (S3, local, etc.)
   └─> Creates DataFrame (distributed data)

2. TRANSFORM: Apply operations (filter, select, join, aggregate)
   └─> Builds execution plan (lazy evaluation)

3. ACTION: Execute the plan and get results
   └─> Triggers actual computation

This base class provides common utilities for all Spark services.
"""

import logging
from typing import Dict, Any, Optional, List
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType
import time

from app.config.spark_config import SparkConfig
from app.models.user_credentials import UserAWSCredentials

logger = logging.getLogger(__name__)


class SparkServiceBase:
    """
    Base class for all Spark-based services.
    
    Provides common functionality:
    - Spark session management
    - AWS credential configuration
    - Data reading/writing
    - Error handling
    - Performance monitoring
    """
    
    def __init__(self, service_name: str):
        """
        Initialize Spark service.
        
        Args:
            service_name: Name of the service (for logging)
        """
        self.service_name = service_name
        self.spark: Optional[SparkSession] = None
        logger.info(f"✅ {service_name} initialized")
    
    def _get_spark_session(self) -> SparkSession:
        """
        Get or create Spark session.
        
        WHY SINGLETON?
        --------------
        Creating SparkSession is expensive (takes time, uses memory).
        We reuse the same session across all services.
        """
        if self.spark is None:
            self.spark = SparkConfig.get_spark_session(app_name=self.service_name)
        return self.spark
    
    def _configure_credentials(self, credentials: UserAWSCredentials):
        """
        Configure AWS credentials in Spark.
        
        WHAT THIS DOES:
        ---------------
        Tells Spark how to authenticate with AWS S3.
        Must be called before reading/writing S3 data.
        
        Args:
            credentials: User's AWS credentials
        """
        spark = self._get_spark_session()
        SparkConfig.configure_aws_credentials(
            spark=spark,
            access_key=credentials.aws_access_key_id,
            secret_key=credentials.aws_secret_access_key,
            session_token=credentials.aws_session_token
        )
    
    def _build_s3_path(self, bucket: str, key: str) -> str:
        """
        Build S3 path in format Spark understands.
        
        SPARK S3 PATHS:
        ---------------
        - Use s3a:// protocol (not s3://)
        - s3a = S3A connector (newer, faster, more features)
        
        Args:
            bucket: S3 bucket name
            key: S3 object key/path
            
        Returns:
            Full S3A path (e.g., s3a://my-bucket/path/to/file.parquet)
        """
        # Remove leading slash if present
        key = key.lstrip('/')
        return f"s3a://{bucket}/{key}"
    
    def read_parquet(
        self,
        s3_path: str,
        columns: Optional[List[str]] = None
    ) -> DataFrame:
        """
        Read Parquet file from S3.
        
        SPARK OPTIMIZATIONS:
        --------------------
        1. Predicate Pushdown: Spark reads only necessary row groups
        2. Column Pruning: Reads only specified columns
        3. Parallel Reading: Multiple partitions read in parallel
        
        Args:
            s3_path: Full S3A path to Parquet file
            columns: Optional list of columns to read (reads all if None)
            
        Returns:
            Spark DataFrame
            
        Example:
        --------
        df = self.read_parquet("s3a://bucket/data.parquet", ["id", "name"])
        """
        spark = self._get_spark_session()
        
        logger.info(f"📥 Reading Parquet from {s3_path}")
        
        # Read Parquet file
        df = spark.read.parquet(s3_path)
        
        # Select specific columns if requested
        if columns:
            df = df.select(*columns)
            logger.info(f"   Selected columns: {columns}")
        
        logger.info(f"   Schema loaded: {len(df.columns)} columns")
        return df
    
    def write_parquet(
        self,
        df: DataFrame,
        s3_path: str,
        mode: str = "overwrite",
        partition_by: Optional[List[str]] = None,
        compression: str = "snappy"
    ) -> Dict[str, Any]:
        """
        Write DataFrame as Parquet to S3.
        
        WRITE MODES:
        ------------
        - overwrite: Replace existing data
        - append: Add to existing data
        - error: Fail if data exists
        - ignore: Skip if data exists
        
        PARTITIONING:
        -------------
        partition_by=["year", "month"] creates directory structure:
        /data/year=2024/month=01/file1.parquet
        /data/year=2024/month=02/file2.parquet
        
        This speeds up queries that filter by partition columns.
        
        Args:
            df: Spark DataFrame to write
            s3_path: Destination S3A path
            mode: Write mode
            partition_by: Columns to partition by
            compression: Compression codec (snappy, gzip, none)
            
        Returns:
            Dict with write statistics
        """
        logger.info(f"📤 Writing Parquet to {s3_path}")
        logger.info(f"   Mode: {mode}, Compression: {compression}")
        
        start_time = time.time()
        
        writer = df.write.mode(mode).option("compression", compression)
        
        if partition_by:
            logger.info(f"   Partitioning by: {partition_by}")
            writer = writer.partitionBy(*partition_by)
        
        writer.parquet(s3_path)
        
        write_time = time.time() - start_time
        
        logger.info(f"✅ Write completed in {write_time:.2f}s")
        
        return {
            "success": True,
            "rows_written": df.count(),
            "write_time_seconds": write_time,
            "path": s3_path
        }
    
    def execute_sql(
        self,
        df: DataFrame,
        sql_query: str,
        table_name: str = "temp_table"
    ) -> DataFrame:
        """
        Execute SQL query on DataFrame.
        
        HOW IT WORKS:
        -------------
        1. Register DataFrame as temporary SQL table
        2. Execute SQL query against that table
        3. Return result as new DataFrame
        
        Args:
            df: Input DataFrame
            sql_query: SQL query to execute
            table_name: Temporary table name to use in SQL
            
        Returns:
            Result DataFrame
            
        Example:
        --------
        result = self.execute_sql(
            df, 
            "SELECT department, AVG(salary) FROM employees GROUP BY department",
            "employees"
        )
        """
        spark = self._get_spark_session()
        
        logger.info(f"⚡ Executing SQL on table '{table_name}'")
        logger.info(f"   Query: {sql_query[:100]}...")
        
        # Register DataFrame as temporary view
        df.createOrReplaceTempView(table_name)
        
        # Execute SQL
        result_df = spark.sql(sql_query)
        
        logger.info(f"✅ SQL executed successfully")
        return result_df
    
    def get_dataframe_stats(self, df: DataFrame) -> Dict[str, Any]:
        """
        Get statistics about a DataFrame.
        
        USEFUL FOR:
        -----------
        - Understanding data size
        - Checking schema
        - Debugging issues
        
        Args:
            df: Spark DataFrame
            
        Returns:
            Dict with statistics
        """
        logger.info("📊 Collecting DataFrame statistics...")
        
        # Get schema
        schema = df.schema
        columns = [{"name": field.name, "type": str(field.dataType)} 
                  for field in schema.fields]
        
        # Get row count (this is an ACTION - triggers execution)
        row_count = df.count()
        
        # Get partitions (how data is divided)
        num_partitions = df.rdd.getNumPartitions()
        
        stats = {
            "num_rows": row_count,
            "num_columns": len(columns),
            "columns": columns,
            "num_partitions": num_partitions,
            "rows_per_partition": row_count // num_partitions if num_partitions > 0 else 0
        }
        
        logger.info(f"   Rows: {row_count:,}")
        logger.info(f"   Columns: {len(columns)}")
        logger.info(f"   Partitions: {num_partitions}")
        
        return stats
    
    def filter_data(
        self,
        df: DataFrame,
        filter_conditions: Dict[str, Any]
    ) -> DataFrame:
        """
        Apply filters to DataFrame.
        
        FILTER EXAMPLES:
        ----------------
        {"age": 25}              → age = 25
        {"age": {">": 25}}       → age > 25
        {"age": {">=": 25, "<=": 65}}  → age >= 25 AND age <= 65
        {"department": ["Sales", "IT"]}  → department IN ('Sales', 'IT')
        
        Args:
            df: Input DataFrame
            filter_conditions: Dict of column: value/condition
            
        Returns:
            Filtered DataFrame
        """
        logger.info(f"🔍 Applying filters: {filter_conditions}")
        
        for column, condition in filter_conditions.items():
            if isinstance(condition, dict):
                # Complex conditions
                for op, value in condition.items():
                    if op == ">":
                        df = df.filter(F.col(column) > value)
                    elif op == ">=":
                        df = df.filter(F.col(column) >= value)
                    elif op == "<":
                        df = df.filter(F.col(column) < value)
                    elif op == "<=":
                        df = df.filter(F.col(column) <= value)
                    elif op == "!=":
                        df = df.filter(F.col(column) != value)
            elif isinstance(condition, list):
                # IN condition
                df = df.filter(F.col(column).isin(condition))
            else:
                # Equality
                df = df.filter(F.col(column) == condition)
        
        return df
    
    def get_sample_data(
        self,
        df: DataFrame,
        num_rows: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Get sample rows from DataFrame.
        
        IMPORTANT:
        ----------
        limit() is more efficient than take() for large datasets.
        It stops reading once it has enough rows.
        
        Args:
            df: Input DataFrame
            num_rows: Number of rows to sample
            
        Returns:
            List of row dictionaries
        """
        logger.info(f"📋 Getting {num_rows} sample rows...")
        
        # Use limit and collect
        sample_rows = df.limit(num_rows).collect()
        
        # Convert to list of dicts
        result = [row.asDict() for row in sample_rows]
        
        logger.info(f"✅ Retrieved {len(result)} rows")
        return result
    
    def optimize_partitions(
        self,
        df: DataFrame,
        target_partition_size_mb: int = 128
    ) -> DataFrame:
        """
        Optimize DataFrame partitioning.
        
        WHY OPTIMIZE?
        -------------
        Too many partitions = Too much overhead
        Too few partitions = Not enough parallelism
        
        RULE OF THUMB:
        --------------
        - Each partition should be 100-200 MB
        - Number of partitions ≈ 2-4x number of CPU cores
        
        Args:
            df: Input DataFrame
            target_partition_size_mb: Target size per partition in MB
            
        Returns:
            Repartitioned DataFrame
        """
        current_partitions = df.rdd.getNumPartitions()
        
        logger.info(f"🔧 Current partitions: {current_partitions}")
        
        # Estimate optimal partitions based on data size
        # This is a simple heuristic - adjust based on your needs
        spark = self._get_spark_session()
        optimal_partitions = spark.sparkContext.defaultParallelism
        
        if current_partitions != optimal_partitions:
            logger.info(f"   Repartitioning to {optimal_partitions} partitions...")
            df = df.repartition(optimal_partitions)
            logger.info(f"✅ Repartitioned successfully")
        else:
            logger.info(f"   Partitions already optimal")
        
        return df
    
    def handle_errors(self, operation_name: str):
        """
        Context manager for error handling.
        
        Usage:
        ------
        with self.handle_errors("read_parquet"):
            df = self.read_parquet(path)
        """
        from contextlib import contextmanager
        
        @contextmanager
        def error_handler():
            start_time = time.time()
            try:
                logger.info(f"▶️ Starting: {operation_name}")
                yield
                elapsed = time.time() - start_time
                logger.info(f"✅ Completed: {operation_name} ({elapsed:.2f}s)")
            except Exception as e:
                elapsed = time.time() - start_time
                logger.error(f"❌ Failed: {operation_name} ({elapsed:.2f}s)")
                logger.error(f"   Error: {str(e)}")
                raise
        
        return error_handler()
