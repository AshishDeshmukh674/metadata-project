"""
Quick Sample Data Creator
=========================
Minimal script to create test Parquet file.
"""

import pandas as pd
import random
from datetime import datetime, timedelta

# Create sample data
print("Creating sample data...")
data = {
    'user_id': range(1, 101),
    'name': [f'User {i}' for i in range(1, 101)],
    'age': [random.randint(20, 60) for _ in range(100)],
    'salary': [random.randint(30000, 120000) for _ in range(100)],
    'department': [random.choice(['IT', 'HR', 'Sales', 'Finance']) for _ in range(100)],
    'join_date': [datetime.now() - timedelta(days=random.randint(0, 1000)) for _ in range(100)]
}

df = pd.DataFrame(data)

# Save as Parquet
filename = 'test_users.parquet'
df.to_parquet(filename, compression='snappy', index=False)

print(f"✅ Created {filename}")
print(f"   Rows: {len(df)}")
print(f"   Columns: {list(df.columns)}")
print("\nFirst 3 rows:")
print(df.head(3))
print("\n📤 To upload to S3:")
print(f"   aws s3 cp {filename} s3://YOUR-BUCKET/test-data/{filename}")
