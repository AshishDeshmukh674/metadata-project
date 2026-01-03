"""
Create Sample Parquet Files for Testing
========================================
This script creates sample data and uploads it to S3 in various formats.
"""

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import boto3
from datetime import datetime, timedelta
import random
import os

def create_sample_dataframe(num_rows=1000):
    """
    Create a sample DataFrame with various data types.
    
    Args:
        num_rows: Number of rows to generate
        
    Returns:
        pandas DataFrame
    """
    print(f"📊 Creating sample data with {num_rows} rows...")
    
    # Generate sample data
    data = {
        'id': range(1, num_rows + 1),
        'name': [f'User_{i}' for i in range(1, num_rows + 1)],
        'email': [f'user{i}@example.com' for i in range(1, num_rows + 1)],
        'age': [random.randint(18, 80) for _ in range(num_rows)],
        'salary': [random.randint(30000, 150000) for _ in range(num_rows)],
        'department': [random.choice(['Engineering', 'Sales', 'Marketing', 'HR', 'Finance']) 
                      for _ in range(num_rows)],
        'join_date': [datetime.now() - timedelta(days=random.randint(0, 1825)) 
                     for _ in range(num_rows)],
        'is_active': [random.choice([True, False]) for _ in range(num_rows)],
        'rating': [round(random.uniform(1.0, 5.0), 2) for _ in range(num_rows)]
    }
    
    df = pd.DataFrame(data)
    print(f"✅ Created DataFrame with columns: {list(df.columns)}")
    return df


def save_parquet_locally(df, output_path='sample_data.parquet'):
    """
    Save DataFrame as Parquet file locally.
    
    Args:
        df: pandas DataFrame
        output_path: Local file path
    """
    print(f"💾 Saving Parquet file to {output_path}...")
    
    # Convert to PyArrow Table for better control
    table = pa.Table.from_pandas(df)
    
    # Write Parquet file with compression
    pq.write_table(
        table, 
        output_path,
        compression='snappy',  # Good balance of speed and compression
        use_dictionary=True,   # Efficient for string columns
        write_statistics=True  # Include column statistics
    )
    
    # Get file size
    file_size = os.path.getsize(output_path)
    print(f"✅ Parquet file created: {file_size:,} bytes")
    return output_path


def upload_to_s3(local_file, bucket, s3_key, credentials):
    """
    Upload file to S3.
    
    Args:
        local_file: Path to local file
        bucket: S3 bucket name
        s3_key: S3 object key (path)
        credentials: Dict with AWS credentials
    """
    print(f"☁️  Uploading to s3://{bucket}/{s3_key}...")
    
    # Create S3 client
    s3_client = boto3.client(
        's3',
        aws_access_key_id=credentials['aws_access_key_id'],
        aws_secret_access_key=credentials['aws_secret_access_key'],
        region_name=credentials.get('aws_region', 'us-east-1')
    )
    
    # Upload file
    s3_client.upload_file(local_file, bucket, s3_key)
    print(f"✅ Uploaded successfully!")
    print(f"   S3 URI: s3://{bucket}/{s3_key}")


def create_partitioned_parquet(df, output_dir='sample_partitioned'):
    """
    Create partitioned Parquet dataset (useful for testing Hive-style partitions).
    
    Args:
        df: pandas DataFrame
        output_dir: Directory to save partitioned data
    """
    print(f"📁 Creating partitioned Parquet dataset...")
    
    # Add partition columns
    df['year'] = pd.to_datetime(df['join_date']).dt.year
    df['month'] = pd.to_datetime(df['join_date']).dt.month
    
    # Write partitioned dataset
    table = pa.Table.from_pandas(df)
    pq.write_to_dataset(
        table,
        root_path=output_dir,
        partition_cols=['department', 'year'],
        compression='snappy'
    )
    
    print(f"✅ Partitioned dataset created in {output_dir}/")
    print(f"   Partitions: department and year")


def main():
    """Main function to create and upload sample data."""
    
    print("=" * 60)
    print("🚀 Sample Parquet File Generator for Testing")
    print("=" * 60)
    print()
    
    # Step 1: Create sample data
    df = create_sample_dataframe(num_rows=1000)
    print()
    
    # Display sample
    print("📋 Sample data (first 5 rows):")
    print(df.head())
    print()
    
    # Step 2: Save locally
    local_file = save_parquet_locally(df, 'sample_users.parquet')
    print()
    
    # Step 3: Optionally create partitioned dataset
    create_partitioned_parquet(df.copy(), 'sample_users_partitioned')
    print()
    
    # Step 4: Upload to S3 (optional)
    print("=" * 60)
    print("📤 Upload to S3")
    print("=" * 60)
    upload_choice = input("Do you want to upload to S3? (y/n): ").lower()
    
    if upload_choice == 'y':
        print("\n🔐 Enter your AWS credentials:")
        credentials = {
            'aws_access_key_id': input("AWS Access Key ID: "),
            'aws_secret_access_key': input("AWS Secret Access Key: "),
            'aws_region': input("AWS Region (default: us-east-1): ") or 'us-east-1'
        }
        
        bucket = input("\n📦 Enter S3 bucket name: ")
        s3_prefix = input("📁 Enter S3 prefix/folder (e.g., test-data/users/): ")
        
        # Upload single file
        s3_key = f"{s3_prefix}sample_users.parquet"
        upload_to_s3(local_file, bucket, s3_key, credentials)
        
        print("\n" + "=" * 60)
        print("✅ All done!")
        print("=" * 60)
        print(f"\n🎯 Test with your API:")
        print(f"   S3 Path: s3://{bucket}/{s3_prefix}")
        print(f"\n📝 Next steps:")
        print(f"   1. Go to http://localhost:8000/docs")
        print(f"   2. Use POST /api/v1/auth/validate-credentials")
        print(f"   3. Use POST /api/v1/auth/create-session")
        print(f"   4. Use GET /api/v1/datastore/info?s3_path=s3://{bucket}/{s3_prefix}")
    else:
        print("\n✅ Sample files created locally:")
        print(f"   - {local_file}")
        print(f"   - sample_users_partitioned/")
        print("\n📝 To upload manually, use:")
        print(f"   aws s3 cp {local_file} s3://YOUR-BUCKET/path/")


if __name__ == "__main__":
    main()
