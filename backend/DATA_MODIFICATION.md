# Natural Language Data Modification Feature

## Overview

This feature allows users to modify Parquet files on S3 using natural language instructions. The system uses **Groq LLM** to convert natural language to SQL, then executes it using **DuckDB**.

## Setup

### 1. Get Groq API Key

1. Visit https://console.groq.com/keys
2. Create an account or log in
3. Generate a new API key
4. Copy the API key

### 2. Configure Environment

Edit `backend/.env` and add your Groq API key:

```env
GROQ_API_KEY=your_actual_groq_api_key_here
GROQ_MODEL=llama-3.3-70b-versatile
```

### 3. Install Dependencies

Dependencies should already be installed, but if needed:

```bash
cd backend
pip install duckdb==0.10.0 pandas==2.2.0 groq==0.4.2
```

## Usage Flow

### Step 1: Create Session

First, authenticate and create a session (same as before):

```bash
POST /api/v1/auth/create-session
Content-Type: application/json

{
  "aws_access_key_id": "AKIA...",
  "aws_secret_access_key": "...",
  "aws_region": "us-east-1"
}
```

Response:
```json
{
  "session_id": "uuid-here",
  "expires_in_minutes": 30
}
```

### Step 2: Preview Changes

Use natural language to describe what you want to do:

```bash
POST /api/v1/datastore/preview-changes
X-Session-ID: your-session-id
Content-Type: application/json

{
  "s3_path": "s3://metadataproject/test-data/test_users.parquet",
  "instruction": "delete users older than 30"
}
```

Response:
```json
{
  "success": true,
  "sql": "DELETE FROM data_table WHERE age > 30",
  "explanation": "Removes all rows where the age column is greater than 30",
  "operation_type": "DELETE",
  "affected_columns": ["age"],
  "requires_backup": true,
  "estimated_impact": "Permanently deletes rows matching the condition",
  "safety_check": {
    "is_safe": true,
    "warnings": [],
    "risk_level": "low"
  },
  "preview_data": {
    "sample_rows": [...],
    "total_rows": 100,
    "preview_size": 10
  }
}
```

### Step 3: Execute Changes

If you approve the SQL, execute it:

```bash
POST /api/v1/datastore/execute-changes
X-Session-ID: your-session-id
Content-Type: application/json

{
  "s3_path": "s3://metadataproject/test-data/test_users.parquet",
  "sql": "DELETE FROM data_table WHERE age > 30",
  "operation_type": "DELETE",
  "create_backup": true
}
```

Response:
```json
{
  "success": true,
  "message": "DELETE executed successfully. 25 rows affected. Data written back to S3.",
  "rows_affected": 25,
  "execution_time_ms": 1234.56,
  "backup_path": "s3://metadataproject/test-data/test_users_backup_20260103_143022.parquet"
}
```

## Example Natural Language Instructions

### SELECT (Read-only)
- "show me all users from California"
- "get users older than 25"
- "find rows where email contains gmail"
- "show the top 10 users by age"

### DELETE
- "delete users older than 30"
- "remove rows where status is inactive"
- "delete all records from 2020"

### UPDATE
- "update state from California to CA"
- "change all NULL emails to unknown@example.com"
- "set status to active where age > 18"

### INSERT (if you have data to add)
- "add a new user with name John, age 25"

## Safety Features

### 1. Preview Before Execute
- Always shows what will happen before executing
- Displays sample rows that will be affected
- Shows SQL that will be executed

### 2. Automatic Backups
- Creates timestamped backup before modifications
- Backup path returned in response
- Can be disabled with `create_backup: false`

### 3. SQL Safety Validation
- Checks for dangerous patterns (DELETE without WHERE, etc.)
- Warns about high-risk operations
- Blocks DROP/TRUNCATE commands

### 4. Session-based Security
- All operations require valid session
- Credentials never stored permanently
- 30-minute session timeout

## API Endpoints

### POST /api/v1/datastore/preview-changes
- **Purpose**: Convert natural language to SQL and preview changes
- **Requires**: X-Session-ID header
- **Safe**: Yes (read-only)

### POST /api/v1/datastore/execute-changes
- **Purpose**: Execute approved SQL on Parquet file
- **Requires**: X-Session-ID header
- **Safe**: No (modifies data, creates backup)

## Architecture

```
User Natural Language
        ↓
Groq LLM (llama-3.3-70b-versatile)
        ↓
SQL Query
        ↓
Safety Validation
        ↓
DuckDB Execution on Parquet
        ↓
Write Back to S3
```

## Supported Table Formats

Currently implemented:
- ✅ **Parquet** - Full support for read/write

Coming soon:
- 🔜 Iceberg
- 🔜 Delta Lake
- 🔜 Hudi

## Error Handling

The system handles:
- Invalid SQL syntax
- S3 access errors
- Schema mismatches
- Network failures
- LLM API errors

All errors include detailed messages for debugging.

## Limitations

1. **Single file operations**: Currently works on single Parquet files
2. **DuckDB SQL syntax**: Uses DuckDB SQL dialect
3. **Memory constraints**: Entire file loaded into memory
4. **Groq API limits**: Subject to Groq rate limits

## Troubleshooting

### "LLM service not configured"
- Make sure `GROQ_API_KEY` is set in `.env`
- Restart the server after adding the key

### "Import groq could not be resolved"
- This is a false warning from VS Code
- Run `pip install groq==0.4.2`
- Restart your Python environment

### "Failed to write to S3"
- Check AWS credentials have write permissions
- Verify S3 path is correct
- Check network connectivity

## Cost Considerations

- **Groq API**: Currently free tier available
- **S3**: Standard S3 costs for read/write operations
- **DuckDB**: Free, runs locally

## Next Steps

1. Get your Groq API key from https://console.groq.com/keys
2. Add it to `.env` file
3. Restart the server
4. Try the `/preview-changes` endpoint
5. Check the API docs at http://localhost:8000/docs
