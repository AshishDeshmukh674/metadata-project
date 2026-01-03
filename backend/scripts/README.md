# Sample Data Scripts

Scripts to create test Parquet files for the Metastore Viewer.

## Quick Start (Easiest)

**Option 1: Simple Local File**
```bash
cd backend/scripts
python quick_sample.py
```

This creates `test_users.parquet` with 100 sample rows.

**Option 2: Full Featured**
```bash
python create_sample_data.py
```

This creates:
- Single Parquet file with 1000 rows
- Partitioned Parquet dataset
- Option to upload directly to S3

## Upload to S3

### Method 1: AWS CLI
```bash
# Install AWS CLI first: https://aws.amazon.com/cli/

# Upload single file
aws s3 cp test_users.parquet s3://your-bucket/test-data/users/test_users.parquet

# Upload entire folder
aws s3 cp sample_users_partitioned/ s3://your-bucket/test-data/users/ --recursive
```

### Method 2: Using Python (boto3)
```python
import boto3

s3 = boto3.client('s3',
    aws_access_key_id='YOUR_ACCESS_KEY',
    aws_secret_access_key='YOUR_SECRET_KEY'
)

s3.upload_file(
    'test_users.parquet',
    'your-bucket',
    'test-data/users/test_users.parquet'
)
```

### Method 3: AWS Console
1. Go to https://console.aws.amazon.com/s3/
2. Open your bucket
3. Click "Upload"
4. Drag and drop `test_users.parquet`

## Test with API

Once uploaded, test with your Metastore Viewer:

1. **Validate credentials**:
   ```bash
   curl -X POST http://localhost:8000/api/v1/auth/validate-credentials \
     -H "Content-Type: application/json" \
     -d '{
       "aws_access_key_id": "YOUR_KEY",
       "aws_secret_access_key": "YOUR_SECRET",
       "aws_region": "us-east-1"
     }'
   ```

2. **Create session**:
   ```bash
   curl -X POST http://localhost:8000/api/v1/auth/create-session \
     -H "Content-Type: application/json" \
     -d '{
       "aws_access_key_id": "YOUR_KEY",
       "aws_secret_access_key": "YOUR_SECRET",
       "aws_region": "us-east-1"
     }'
   ```

3. **Get datastore info**:
   ```bash
   curl "http://localhost:8000/api/v1/datastore/info?s3_path=s3://your-bucket/test-data/users/" \
     -H "X-Session-ID: your-session-id"
   ```

Or use the interactive docs at: **http://localhost:8000/docs**

## Sample Data Structure

The generated Parquet file contains:

| Column | Type | Example |
|--------|------|---------|
| user_id | int | 1, 2, 3... |
| name | string | "User 1", "User 2"... |
| age | int | 25, 34, 42... |
| salary | int | 45000, 67000... |
| department | string | "IT", "Sales"... |
| join_date | datetime | 2024-01-15... |

## Creating Your Own Data

```python
import pandas as pd

# Create your data
df = pd.DataFrame({
    'col1': [1, 2, 3],
    'col2': ['a', 'b', 'c']
})

# Save as Parquet
df.to_parquet('my_data.parquet', compression='snappy')
```

## Troubleshooting

**Issue**: `ModuleNotFoundError: No module named 'pandas'`
```bash
pip install pandas pyarrow boto3
```

**Issue**: AWS credentials error
- Make sure your AWS credentials have S3 read/write permissions
- Test with: `aws s3 ls s3://your-bucket/`

**Issue**: Permission denied
- Check bucket policy allows your IAM user/role
- Verify bucket region matches your credentials region
