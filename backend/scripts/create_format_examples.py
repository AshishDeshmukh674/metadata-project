"""
Create Example Files for All Data Formats
==========================================
This script creates sample data files for Parquet, Delta, Hudi, and Iceberg formats
that can be uploaded to AWS S3 bucket.

Usage:
    python create_format_examples.py

Output:
    ./sample_data/
        ├── parquet/
        │   └── users.parquet
        ├── delta/
        │   └── users_delta/ (directory with Delta Lake files)
        ├── hudi/
        │   └── users_hudi/ (directory with Hudi files)
        └── iceberg/
            └── users_iceberg/ (directory with Iceberg files)
"""

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from deltalake import write_deltalake
from datetime import datetime, timedelta
import random
import os
import shutil

# Output directory
OUTPUT_DIR = "sample_data"


def create_sample_dataframe(num_rows=500):
    """
    Create a sample DataFrame with various data types for testing.
    
    Args:
        num_rows: Number of rows to generate (default: 500)
        
    Returns:
        pandas DataFrame with sample employee data
    """
    print(f"\n📊 Creating sample data with {num_rows} rows...")
    
    departments = ['Engineering', 'Sales', 'Marketing', 'HR', 'Finance', 'Operations']
    cities = ['New York', 'San Francisco', 'Chicago', 'Austin', 'Boston', 'Seattle']
    
    data = {
        'employee_id': range(1, num_rows + 1),
        'first_name': [f'FirstName{i}' for i in range(1, num_rows + 1)],
        'last_name': [f'LastName{i}' for i in range(1, num_rows + 1)],
        'email': [f'employee{i}@company.com' for i in range(1, num_rows + 1)],
        'department': [random.choice(departments) for _ in range(num_rows)],
        'age': [random.randint(22, 65) for _ in range(num_rows)],
        'salary': [random.randint(40000, 200000) for _ in range(num_rows)],
        'hire_date': [
            (datetime.now() - timedelta(days=random.randint(0, 3650))).strftime('%Y-%m-%d')
            for _ in range(num_rows)
        ],
        'is_active': [random.choice([True, False]) for _ in range(num_rows)],
        'performance_rating': [round(random.uniform(2.0, 5.0), 2) for _ in range(num_rows)],
        'city': [random.choice(cities) for _ in range(num_rows)],
        'years_experience': [random.randint(0, 40) for _ in range(num_rows)]
    }
    
    df = pd.DataFrame(data)
    print(f"✅ Created DataFrame with {len(df)} rows and {len(df.columns)} columns")
    print(f"   Columns: {list(df.columns)}")
    return df


def create_parquet_file(df):
    """Create a Parquet file."""
    print("\n" + "="*60)
    print("📦 CREATING PARQUET FILE")
    print("="*60)
    
    output_path = os.path.join(OUTPUT_DIR, "parquet")
    os.makedirs(output_path, exist_ok=True)
    
    file_path = os.path.join(output_path, "users.parquet")
    
    # Write Parquet file with compression
    table = pa.Table.from_pandas(df)
    pq.write_table(table, file_path, compression='snappy')
    
    file_size = os.path.getsize(file_path) / 1024  # Size in KB
    
    print(f"✅ Parquet file created: {file_path}")
    print(f"   Size: {file_size:.2f} KB")
    print(f"   Compression: snappy")
    print(f"\n📤 Upload command:")
    print(f"   aws s3 cp {file_path} s3://YOUR-BUCKET/parquet/users.parquet")


def create_delta_table(df):
    """Create a Delta Lake table."""
    print("\n" + "="*60)
    print("🔺 CREATING DELTA LAKE TABLE")
    print("="*60)
    
    output_path = os.path.join(OUTPUT_DIR, "delta", "users_delta")
    
    # Remove if exists
    if os.path.exists(output_path):
        shutil.rmtree(output_path)
    
    os.makedirs(output_path, exist_ok=True)
    
    # Write Delta table
    write_deltalake(
        output_path,
        df,
        mode="overwrite",
        partition_by=["department"]
    )
    
    # Calculate total size
    total_size = sum(
        os.path.getsize(os.path.join(dirpath, filename))
        for dirpath, _, filenames in os.walk(output_path)
        for filename in filenames
    ) / 1024
    
    file_count = sum(len(files) for _, _, files in os.walk(output_path))
    
    print(f"✅ Delta Lake table created: {output_path}")
    print(f"   Total size: {total_size:.2f} KB")
    print(f"   Files: {file_count}")
    print(f"   Partitioned by: department")
    print(f"\n📤 Upload command:")
    print(f"   aws s3 sync {output_path} s3://YOUR-BUCKET/delta/users_delta/")


def create_hudi_table(df):
    """Create a Hudi table using Parquet files with Hudi-style layout."""
    print("\n" + "="*60)
    print("🚀 CREATING HUDI TABLE (Copy-on-Write)")
    print("="*60)
    
    output_path = os.path.join(OUTPUT_DIR, "hudi", "users_hudi")
    
    # Remove if exists
    if os.path.exists(output_path):
        shutil.rmtree(output_path)
    
    os.makedirs(output_path, exist_ok=True)
    
    # Create Hudi metadata directory structure
    hoodie_path = os.path.join(output_path, ".hoodie")
    os.makedirs(hoodie_path, exist_ok=True)
    
    # Create a simple hoodie.properties file
    properties_content = """#
#Properties saved on {timestamp}
#{timestamp_comment}
hoodie.table.name=users_hudi
hoodie.table.type=COPY_ON_WRITE
hoodie.table.version=5
hoodie.timeline.layout.version=1
""".format(
        timestamp=datetime.now().strftime("%a %b %d %H:%M:%S UTC %Y"),
        timestamp_comment=datetime.now().strftime("%a %b %d %H:%M:%S UTC %Y")
    )
    
    with open(os.path.join(hoodie_path, "hoodie.properties"), "w") as f:
        f.write(properties_content)
    
    # Partition by department
    for dept in df['department'].unique():
        dept_df = df[df['department'] == dept].copy()
        dept_path = os.path.join(output_path, f"department={dept}")
        os.makedirs(dept_path, exist_ok=True)
        
        # Create Hudi-style filename with UUID
        commit_time = datetime.now().strftime("%Y%m%d%H%M%S")
        file_id = f"data_{dept.lower().replace(' ', '_')}"
        filename = f"{commit_time}_{file_id}_0_0-0-0_{commit_time}.parquet"
        
        file_path = os.path.join(dept_path, filename)
        
        # Add Hudi metadata columns
        dept_df['_hoodie_commit_time'] = commit_time
        dept_df['_hoodie_commit_seqno'] = commit_time + "001"
        dept_df['_hoodie_record_key'] = dept_df['employee_id'].astype(str)
        dept_df['_hoodie_partition_path'] = f"department={dept}"
        dept_df['_hoodie_file_name'] = filename
        
        # Write as parquet
        table = pa.Table.from_pandas(dept_df)
        pq.write_table(table, file_path, compression='snappy')
    
    # Calculate total size
    total_size = sum(
        os.path.getsize(os.path.join(dirpath, filename))
        for dirpath, _, filenames in os.walk(output_path)
        for filename in filenames
    ) / 1024
    
    file_count = sum(len(files) for _, _, files in os.walk(output_path))
    
    print(f"✅ Hudi table created: {output_path}")
    print(f"   Total size: {total_size:.2f} KB")
    print(f"   Files: {file_count}")
    print(f"   Table type: COPY_ON_WRITE")
    print(f"   Partitioned by: department")
    print(f"\n📤 Upload command:")
    print(f"   aws s3 sync {output_path} s3://YOUR-BUCKET/hudi/users_hudi/")


def create_iceberg_table(df):
    """Create an Iceberg table using basic Parquet files with Iceberg-style layout."""
    print("\n" + "="*60)
    print("❄️  CREATING ICEBERG TABLE")
    print("="*60)
    
    output_path = os.path.join(OUTPUT_DIR, "iceberg", "users_iceberg")
    
    # Remove if exists
    if os.path.exists(output_path):
        shutil.rmtree(output_path)
    
    os.makedirs(output_path, exist_ok=True)
    
    # Create Iceberg directory structure
    metadata_path = os.path.join(output_path, "metadata")
    data_path = os.path.join(output_path, "data")
    os.makedirs(metadata_path, exist_ok=True)
    os.makedirs(data_path, exist_ok=True)
    
    # Write data files partitioned by department
    partition_specs = []
    for dept in df['department'].unique():
        dept_df = df[df['department'] == dept].copy()
        dept_dir = os.path.join(data_path, f"department={dept}")
        os.makedirs(dept_dir, exist_ok=True)
        
        # Create data file
        filename = f"00000-0-{dept.lower().replace(' ', '-')}-00001.parquet"
        file_path = os.path.join(dept_dir, filename)
        
        table = pa.Table.from_pandas(dept_df)
        pq.write_table(table, file_path, compression='snappy')
        
        file_size = os.path.getsize(file_path)
        partition_specs.append({
            'partition': f"department={dept}",
            'file': filename,
            'size': file_size,
            'records': len(dept_df)
        })
    
    # Create a simple version-hint.text
    with open(os.path.join(metadata_path, "version-hint.text"), "w") as f:
        f.write("1")
    
    # Create a basic v1.metadata.json
    metadata_content = {
        "format-version": 2,
        "table-uuid": "12345678-1234-1234-1234-123456789012",
        "location": output_path,
        "last-updated-ms": int(datetime.now().timestamp() * 1000),
        "last-column-id": 12,
        "schema": {
            "type": "struct",
            "schema-id": 0,
            "fields": [
                {"id": 1, "name": "employee_id", "required": True, "type": "long"},
                {"id": 2, "name": "first_name", "required": False, "type": "string"},
                {"id": 3, "name": "last_name", "required": False, "type": "string"},
                {"id": 4, "name": "email", "required": False, "type": "string"},
                {"id": 5, "name": "department", "required": False, "type": "string"},
                {"id": 6, "name": "age", "required": False, "type": "long"},
                {"id": 7, "name": "salary", "required": False, "type": "long"},
                {"id": 8, "name": "hire_date", "required": False, "type": "string"},
                {"id": 9, "name": "is_active", "required": False, "type": "boolean"},
                {"id": 10, "name": "performance_rating", "required": False, "type": "double"},
                {"id": 11, "name": "city", "required": False, "type": "string"},
                {"id": 12, "name": "years_experience", "required": False, "type": "long"}
            ]
        },
        "partition-spec": [
            {"source-id": 5, "field-id": 1000, "name": "department", "transform": "identity"}
        ],
        "properties": {
            "write.format.default": "parquet",
            "write.parquet.compression-codec": "snappy"
        }
    }
    
    import json
    with open(os.path.join(metadata_path, "v1.metadata.json"), "w") as f:
        json.dump(metadata_content, f, indent=2)
    
    # Calculate total size
    total_size = sum(
        os.path.getsize(os.path.join(dirpath, filename))
        for dirpath, _, filenames in os.walk(output_path)
        for filename in filenames
    ) / 1024
    
    file_count = sum(len(files) for _, _, files in os.walk(output_path))
    
    print(f"✅ Iceberg table created: {output_path}")
    print(f"   Total size: {total_size:.2f} KB")
    print(f"   Files: {file_count}")
    print(f"   Partitioned by: department")
    print(f"   Partitions: {len(partition_specs)}")
    print(f"\n📤 Upload command:")
    print(f"   aws s3 sync {output_path} s3://YOUR-BUCKET/iceberg/users_iceberg/")


def create_summary():
    """Create a summary file with upload instructions."""
    print("\n" + "="*60)
    print("📝 CREATING SUMMARY")
    print("="*60)
    
    summary_content = """
# Sample Data Files - Upload Instructions
Generated on: {timestamp}

## Directory Structure
```
sample_data/
├── parquet/
│   └── users.parquet
├── delta/
│   └── users_delta/
├── hudi/
│   └── users_hudi/
└── iceberg/
    └── users_iceberg/
```

## Upload Commands

### 1. Parquet File
```bash
aws s3 cp sample_data/parquet/users.parquet s3://YOUR-BUCKET/parquet/users.parquet
```

### 2. Delta Lake Table
```bash
aws s3 sync sample_data/delta/users_delta/ s3://YOUR-BUCKET/delta/users_delta/ --delete
```

### 3. Hudi Table
```bash
aws s3 sync sample_data/hudi/users_hudi/ s3://YOUR-BUCKET/hudi/users_hudi/ --delete
```

### 4. Iceberg Table
```bash
aws s3 sync sample_data/iceberg/users_iceberg/ s3://YOUR-BUCKET/iceberg/users_iceberg/ --delete
```

## Testing the API

After uploading, you can test the metadata extraction using your API:

### Parquet
```bash
curl -X POST "http://localhost:8000/api/parquet/metadata" \\
  -H "Content-Type: application/json" \\
  -d '{{
    "aws_access_key_id": "YOUR_KEY",
    "aws_secret_access_key": "YOUR_SECRET",
    "bucket": "YOUR-BUCKET",
    "key": "parquet/users.parquet",
    "region": "us-east-1"
  }}'
```

### Delta Lake
```bash
curl -X POST "http://localhost:8000/api/delta/metadata" \\
  -H "Content-Type: application/json" \\
  -d '{{
    "aws_access_key_id": "YOUR_KEY",
    "aws_secret_access_key": "YOUR_SECRET",
    "bucket": "YOUR-BUCKET",
    "table_path": "delta/users_delta",
    "region": "us-east-1"
  }}'
```

### Hudi
```bash
curl -X POST "http://localhost:8000/api/hudi/metadata" \\
  -H "Content-Type: application/json" \\
  -d '{{
    "aws_access_key_id": "YOUR_KEY",
    "aws_secret_access_key": "YOUR_SECRET",
    "bucket": "YOUR-BUCKET",
    "table_path": "hudi/users_hudi",
    "region": "us-east-1"
  }}'
```

### Iceberg
```bash
curl -X POST "http://localhost:8000/api/iceberg/metadata" \\
  -H "Content-Type: application/json" \\
  -d '{{
    "aws_access_key_id": "YOUR_KEY",
    "aws_secret_access_key": "YOUR_SECRET",
    "bucket": "YOUR-BUCKET",
    "table_path": "iceberg/users_iceberg",
    "region": "us-east-1"
  }}'
```

## Notes
- Replace `YOUR-BUCKET` with your actual S3 bucket name
- Replace `YOUR_KEY` and `YOUR_SECRET` with your AWS credentials
- All files contain the same sample employee data (500 rows)
- Data includes: employee_id, name, email, department, salary, etc.
- Tables are partitioned by department
""".format(timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    
    summary_path = os.path.join(OUTPUT_DIR, "UPLOAD_INSTRUCTIONS.md")
    with open(summary_path, "w") as f:
        f.write(summary_content)
    
    print(f"✅ Summary created: {summary_path}")


def main():
    """Main function to create all example files."""
    print("\n" + "🌟"*30)
    print("    CREATING EXAMPLE FILES FOR ALL DATA FORMATS")
    print("🌟"*30)
    
    # Clean and create output directory
    if os.path.exists(OUTPUT_DIR):
        print(f"\n🗑️  Cleaning existing {OUTPUT_DIR} directory...")
        shutil.rmtree(OUTPUT_DIR)
    
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    print(f"✅ Created output directory: {OUTPUT_DIR}")
    
    # Create sample data
    df = create_sample_dataframe(num_rows=500)
    
    # Create files for each format
    try:
        create_parquet_file(df.copy())
    except Exception as e:
        print(f"❌ Error creating Parquet file: {e}")
    
    try:
        create_delta_table(df.copy())
    except Exception as e:
        print(f"❌ Error creating Delta table: {e}")
    
    try:
        create_hudi_table(df.copy())
    except Exception as e:
        print(f"❌ Error creating Hudi table: {e}")
    
    try:
        create_iceberg_table(df.copy())
    except Exception as e:
        print(f"❌ Error creating Iceberg table: {e}")
    
    # Create summary
    create_summary()
    
    # Final summary
    print("\n" + "="*60)
    print("🎉 ALL FILES CREATED SUCCESSFULLY!")
    print("="*60)
    print(f"\n📁 Output directory: {OUTPUT_DIR}")
    print(f"\n📖 See {os.path.join(OUTPUT_DIR, 'UPLOAD_INSTRUCTIONS.md')} for upload commands")
    print("\n✨ Next steps:")
    print("   1. Review the generated files in the sample_data/ directory")
    print("   2. Replace 'YOUR-BUCKET' with your actual S3 bucket name")
    print("   3. Use the upload commands from UPLOAD_INSTRUCTIONS.md")
    print("   4. Test the API endpoints with the provided curl commands")
    print("\n" + "="*60 + "\n")


if __name__ == "__main__":
    main()
