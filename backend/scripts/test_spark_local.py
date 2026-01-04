"""
Test Spark Integration Locally
===============================
This script tests the Spark integration without needing S3.
It creates sample data locally and processes it with Spark.

Run this to verify Spark is working correctly on your machine.
"""

import sys
import os

# Add parent directory to path so we can import app modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.config.spark_config import get_spark, SparkConfig
from pyspark.sql import functions as F
import pandas as pd
import tempfile
import shutil


def test_spark_session():
    """Test 1: Create Spark session."""
    print("\n" + "="*60)
    print("TEST 1: Spark Session Creation")
    print("="*60)
    
    try:
        spark = get_spark()
        print("✅ Spark session created successfully")
        print(f"   Spark version: {spark.version}")
        print(f"   Master: {spark.sparkContext.master}")
        print(f"   App name: {spark.sparkContext.appName}")
        print(f"   Web UI: {spark.sparkContext.uiWebUrl}")
        return True
    except Exception as e:
        print(f"❌ Failed to create Spark session: {e}")
        return False


def test_dataframe_creation():
    """Test 2: Create and manipulate DataFrames."""
    print("\n" + "="*60)
    print("TEST 2: DataFrame Creation and Operations")
    print("="*60)
    
    try:
        spark = get_spark()
        
        # Create sample data
        data = [
            (1, "Alice", 25, "Engineering", 100000),
            (2, "Bob", 30, "Sales", 80000),
            (3, "Charlie", 35, "Engineering", 120000),
            (4, "Diana", 28, "Marketing", 90000),
            (5, "Eve", 32, "Sales", 85000),
        ]
        
        columns = ["id", "name", "age", "department", "salary"]
        df = spark.createDataFrame(data, columns)
        
        print("✅ DataFrame created")
        print(f"   Rows: {df.count()}")
        print(f"   Columns: {len(df.columns)}")
        
        # Show schema
        print("\n📋 Schema:")
        df.printSchema()
        
        # Show data
        print("\n📊 Data:")
        df.show()
        
        # Test transformations
        print("\n🔧 Testing transformations...")
        
        # Filter
        engineering_df = df.filter(df.department == "Engineering")
        print(f"   Engineering employees: {engineering_df.count()}")
        
        # Aggregation
        avg_salary = df.groupBy("department").agg(
            F.avg("salary").alias("avg_salary")
        )
        print("\n📈 Average salary by department:")
        avg_salary.show()
        
        # SQL
        df.createOrReplaceTempView("employees")
        high_earners = spark.sql("SELECT * FROM employees WHERE salary > 90000")
        print(f"\n💰 High earners (>90k): {high_earners.count()}")
        high_earners.show()
        
        return True
        
    except Exception as e:
        print(f"❌ DataFrame operations failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_parquet_operations():
    """Test 3: Read and write Parquet files."""
    print("\n" + "="*60)
    print("TEST 3: Parquet File Operations")
    print("="*60)
    
    temp_dir = tempfile.mkdtemp()
    
    try:
        spark = get_spark()
        
        # Create sample DataFrame
        data = [
            (i, f"User{i}", 20 + (i % 40), f"Dept{i % 5}", 50000 + (i * 1000))
            for i in range(1, 101)
        ]
        columns = ["id", "name", "age", "department", "salary"]
        df = spark.createDataFrame(data, columns)
        
        # Write Parquet
        parquet_path = os.path.join(temp_dir, "test.parquet")
        print(f"📝 Writing Parquet to {parquet_path}...")
        df.write.mode("overwrite").parquet(parquet_path)
        print("✅ Parquet file written")
        
        # Read Parquet
        print("\n📖 Reading Parquet...")
        read_df = spark.read.parquet(parquet_path)
        print(f"✅ Read {read_df.count()} rows")
        
        # Test column pruning (read only specific columns)
        print("\n📋 Testing column pruning...")
        selected_df = spark.read.parquet(parquet_path).select("id", "name", "salary")
        print(f"✅ Selected {len(selected_df.columns)} columns")
        selected_df.show(5)
        
        # Test predicate pushdown (filter at read time)
        print("\n🔍 Testing predicate pushdown...")
        filtered_df = spark.read.parquet(parquet_path).filter("age > 30 AND salary > 70000")
        print(f"✅ Filtered to {filtered_df.count()} rows")
        
        # Test partitioned write
        print("\n📁 Testing partitioned write...")
        partitioned_path = os.path.join(temp_dir, "partitioned.parquet")
        df.write.mode("overwrite").partitionBy("department").parquet(partitioned_path)
        print("✅ Partitioned Parquet written")
        
        # List partitions
        import glob
        partitions = glob.glob(os.path.join(partitioned_path, "department=*"))
        print(f"   Created {len(partitions)} partitions:")
        for p in partitions[:3]:
            print(f"   - {os.path.basename(p)}")
        
        return True
        
    except Exception as e:
        print(f"❌ Parquet operations failed: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        # Cleanup
        shutil.rmtree(temp_dir, ignore_errors=True)


def test_sql_operations():
    """Test 4: Complex SQL operations."""
    print("\n" + "="*60)
    print("TEST 4: SQL Operations")
    print("="*60)
    
    try:
        spark = get_spark()
        
        # Create sample data
        employees = [
            (1, "Alice", "Engineering", 100000, 2020),
            (2, "Bob", "Sales", 80000, 2019),
            (3, "Charlie", "Engineering", 120000, 2018),
            (4, "Diana", "Marketing", 90000, 2021),
            (5, "Eve", "Sales", 85000, 2020),
            (6, "Frank", "Engineering", 110000, 2019),
            (7, "Grace", "Marketing", 95000, 2020),
        ]
        
        emp_df = spark.createDataFrame(
            employees, 
            ["id", "name", "department", "salary", "hire_year"]
        )
        emp_df.createOrReplaceTempView("employees")
        
        print("✅ Created employees table")
        
        # Test 1: Aggregation
        print("\n📊 Test: Department statistics")
        result = spark.sql("""
            SELECT 
                department,
                COUNT(*) as employee_count,
                AVG(salary) as avg_salary,
                MAX(salary) as max_salary,
                MIN(salary) as min_salary
            FROM employees
            GROUP BY department
            ORDER BY avg_salary DESC
        """)
        result.show()
        
        # Test 2: Window functions
        print("\n📈 Test: Salary rank within department")
        result = spark.sql("""
            SELECT 
                name,
                department,
                salary,
                RANK() OVER (PARTITION BY department ORDER BY salary DESC) as rank_in_dept
            FROM employees
        """)
        result.show()
        
        # Test 3: Complex conditions
        print("\n🔍 Test: Complex filtering")
        result = spark.sql("""
            SELECT 
                name,
                department,
                salary,
                CASE 
                    WHEN salary > 100000 THEN 'Senior'
                    WHEN salary > 80000 THEN 'Mid'
                    ELSE 'Junior'
                END as level
            FROM employees
            WHERE hire_year >= 2019
            ORDER BY salary DESC
        """)
        result.show()
        
        return True
        
    except Exception as e:
        print(f"❌ SQL operations failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_performance():
    """Test 5: Performance with larger dataset."""
    print("\n" + "="*60)
    print("TEST 5: Performance Test (10,000 rows)")
    print("="*60)
    
    temp_dir = tempfile.mkdtemp()
    
    try:
        spark = get_spark()
        import time
        
        # Create larger dataset
        print("📊 Creating 10,000 row dataset...")
        data = [
            (i, f"User{i}", 20 + (i % 50), f"Dept{i % 10}", 40000 + (i * 100))
            for i in range(1, 10001)
        ]
        columns = ["id", "name", "age", "department", "salary"]
        df = spark.createDataFrame(data, columns)
        
        # Test 1: Count
        start = time.time()
        count = df.count()
        count_time = time.time() - start
        print(f"✅ Count: {count:,} rows in {count_time:.3f}s")
        
        # Test 2: Filter
        start = time.time()
        filtered = df.filter("age > 30 AND salary > 50000").count()
        filter_time = time.time() - start
        print(f"✅ Filter: {filtered:,} rows in {filter_time:.3f}s")
        
        # Test 3: Aggregation
        start = time.time()
        agg_result = df.groupBy("department").agg(
            F.count("*").alias("count"),
            F.avg("salary").alias("avg_salary")
        ).count()
        agg_time = time.time() - start
        print(f"✅ Aggregation: {agg_result} groups in {agg_time:.3f}s")
        
        # Test 4: Write to Parquet
        parquet_path = os.path.join(temp_dir, "large.parquet")
        start = time.time()
        df.write.mode("overwrite").parquet(parquet_path)
        write_time = time.time() - start
        print(f"✅ Write Parquet: {write_time:.3f}s")
        
        # Test 5: Read from Parquet
        start = time.time()
        read_df = spark.read.parquet(parquet_path)
        read_count = read_df.count()
        read_time = time.time() - start
        print(f"✅ Read Parquet: {read_count:,} rows in {read_time:.3f}s")
        
        return True
        
    except Exception as e:
        print(f"❌ Performance test failed: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


def run_all_tests():
    """Run all tests."""
    print("\n" + "🌟"*30)
    print("   SPARK INTEGRATION TEST SUITE")
    print("🌟"*30)
    
    tests = [
        ("Spark Session", test_spark_session),
        ("DataFrame Operations", test_dataframe_creation),
        ("Parquet Operations", test_parquet_operations),
        ("SQL Operations", test_sql_operations),
        ("Performance", test_performance),
    ]
    
    results = []
    
    for test_name, test_func in tests:
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"\n❌ {test_name} crashed: {e}")
            import traceback
            traceback.print_exc()
            results.append((test_name, False))
    
    # Summary
    print("\n" + "="*60)
    print("TEST SUMMARY")
    print("="*60)
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for test_name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status} - {test_name}")
    
    print("="*60)
    print(f"Total: {passed}/{total} tests passed")
    
    if passed == total:
        print("\n🎉 All tests passed! Spark is working correctly.")
        print("\n📖 Next steps:")
        print("   1. Read SPARK_GUIDE.md for usage examples")
        print("   2. Try the API endpoints at http://localhost:8000/docs")
        print("   3. Upload sample data to S3 and test with real files")
    else:
        print("\n⚠️ Some tests failed. Check the errors above.")
        print("   Common issues:")
        print("   - Java not installed (Spark requires Java 8+)")
        print("   - Insufficient memory (try reducing data size)")
        print("   - Dependencies not installed (pip install -r requirements.txt)")
    
    print("="*60)
    
    # Stop Spark session
    print("\n🛑 Stopping Spark session...")
    SparkConfig.stop_spark_session()
    print("✅ Cleanup complete")
    
    return passed == total


if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)
