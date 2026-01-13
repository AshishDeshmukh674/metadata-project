# Table Generator with Docker

Generate Iceberg, Delta, and Hudi tables and upload them to S3 using Docker.

## Prerequisites

1. **Docker Desktop** installed and running on Windows
2. **AWS Credentials** with S3 access permissions

## Setup

1. **Configure AWS Credentials**
   ```bash
   # Copy the example env file
   cp .env.example .env
   
   # Edit .env and add your AWS credentials
   ```

2. **Build the Docker Image**
   ```bash
   docker-compose build
   ```

## Usage

### Run the Table Generator
```bash
docker-compose up
```

This will:
- Create Iceberg, Delta, and Hudi tables locally
- Upload all tables to S3 bucket: `s3://metadataproject/test-data/sample-data/`

### Run and Remove Container After Completion
```bash
docker-compose up --remove-orphans
docker-compose down
```

### View Logs
```bash
docker-compose logs -f
```

## Configuration

Edit `generate_tables.py` to customize:
- `S3_BUCKET`: Target S3 bucket
- `BASE_S3_PATH`: S3 path prefix
- Sample data and schema

## Troubleshooting

### AWS Credentials Not Working
- Ensure `.env` file exists with valid credentials
- Check IAM permissions for S3 access

### Build Fails
```bash
# Clean rebuild
docker-compose down
docker-compose build --no-cache
```

### Check Container Logs
```bash
docker logs table-generator
```

## Output

Local output is saved to `./output/` directory:
- `output/iceberg/sales_iceberg/`
- `output/delta/sales_delta/`
- `output/hudi/sales_hudi/`
