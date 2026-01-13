# Set AWS Credentials
$env:AWS_ACCESS_KEY_ID="YOUR_ACCESS_KEY_HERE"
$env:AWS_SECRET_ACCESS_KEY="YOUR_SECRET_KEY_HERE"
$env:AWS_DEFAULT_REGION="us-east-1"

# Start API
Write-Host "Starting API with AWS credentials..." -ForegroundColor Green
python run_api.py
