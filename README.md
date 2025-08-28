# SumoLogic to CloudZero Adapter

A serverless adapter that extracts billing data from SumoLogic and streams it to CloudZero's AnyTelemetry service. This adapter processes usage data for logs, metrics, and traces, converting them to CloudZero's billing format.

## Overview

The adapter queries SumoLogic for usage data across different tiers and services:
- **Logs**: Continuous, Frequent, and Infrequent tiers (ingestion and scanning)
- **Metrics**: Datapoint ingestion 
- **Traces**: Span ingestion

Data is converted to CloudZero Billing Format (CBF) and streamed to CloudZero using the `replace_drop` operation.

## Environment Variables

### Required Variables

**SumoLogic Configuration:**
- `SUMO_ACCESS_KEY`: Your SumoLogic access key
- `SUMO_SECRET_KEY`: Your SumoLogic secret key
- `SUMO_DEPLOYMENT`: SumoLogic deployment (e.g., "us1", "us2", "eu", "au")
- `SUMO_ORG_ID`: Your SumoLogic organization ID

**CloudZero Configuration:**
- `CZ_AUTH_KEY`: CloudZero API authentication key
- `CZ_ANYCOST_STREAM_CONNECTION_ID`: CloudZero stream connection ID

### Optional Variables

**Credit Rates (defaults shown):**
- `LOG_CONTINUOUS_CREDIT_RATE`: 25
- `LOG_FREQUENT_CREDIT_RATE`: 12
- `LOG_INFREQUENT_CREDIT_RATE`: 5
- `LOG_INFREQUENT_SCAN_CREDIT_RATE`: 0.16
- `METRICS_CREDIT_RATE`: 10
- `TRACING_CREDIT_RATE`: 35
- `COST_PER_CREDIT`: 0.15

**Other Options:**
- `CZ_URL`: CloudZero API endpoint (default: "https://api.cloudzero.com")
- `QUERY_TIME_HOURS`: Hours of historical data to query (default: 24, should not be changed)
- `LOGGING_LEVEL`: Log level - "INFO" or "DEBUG" (default: "INFO")

## Local Development

1. Install dependencies:
```bash
uv sync
```

2. Configure environment variables (copy and modify `test_execute.sh`):
```bash
cp test_execute.sh my_config.sh
# Edit my_config.sh with your actual credentials
chmod +x my_config.sh
./my_config.sh
```

## AWS Lambda Deployment

### Method 1: Using AWS CLI and ZIP

1. **Create deployment package:**
```bash
# Create directory for deployment
mkdir lambda-deployment
cd lambda-deployment

# Copy your code
cp ../sumo_anycost_lambda.py .

# Install dependencies
pip install requests==2.32.4 -t .

# Create ZIP package
zip -r sumo-cz-adapter.zip .
```

2. **Deploy with AWS CLI:**
```bash
# Create the function
aws lambda create-function \
  --function-name sumo-cz-adapter \
  --runtime python3.10 \
  --role arn:aws:iam::YOUR-ACCOUNT:role/lambda-execution-role \
  --handler sumo_anycost_lambda.lambda_handler \
  --zip-file fileb://sumo-cz-adapter.zip \
  --timeout 300

# Set environment variables
aws lambda update-function-configuration \
  --function-name sumo-cz-adapter \
  --environment Variables='{
    "SUMO_ACCESS_KEY":"your-key",
    "SUMO_SECRET_KEY":"your-secret",
    "SUMO_DEPLOYMENT":"us1",
    "SUMO_ORG_ID":"your-org-id",
    "CZ_AUTH_KEY":"your-cz-key",
    "CZ_ANYCOST_STREAM_CONNECTION_ID":"your-stream-id"
  }'
```

### Method 2: Using Container Image

1. **Use the provided Dockerfile:**
The repository includes a `Dockerfile` ready for container deployment. It uses Python 3.13 and installs dependencies from `requirements.txt`.

2. **Build and deploy:**
```bash
# Build image
docker build -t sumo-cz-adapter .

# Tag for ECR
docker tag sumo-cz-adapter:latest YOUR-ACCOUNT.dkr.ecr.us-east-1.amazonaws.com/sumo-cz-adapter:latest

# Push to ECR and create Lambda function
aws lambda create-function \
  --function-name sumo-cz-adapter \
  --package-type Image \
  --code ImageUri=YOUR-ACCOUNT.dkr.ecr.us-east-1.amazonaws.com/sumo-cz-adapter:latest \
  --role arn:aws:iam::YOUR-ACCOUNT:role/lambda-execution-role
```

### Scheduling

**IMPORTANT:** This script must be run exactly once every 24 hours. Running it at any other frequency will cause incorrect data reporting.

Set up EventBridge to run daily:
```bash
aws events put-rule \
  --name sumo-cz-adapter-daily \
  --schedule-expression "rate(24 hours)"

aws lambda add-permission \
  --function-name sumo-cz-adapter \
  --statement-id allow-eventbridge \
  --action lambda:InvokeFunction \
  --principal events.amazonaws.com \
  --source-arn arn:aws:events:us-east-1:YOUR-ACCOUNT:rule/sumo-cz-adapter-daily
```

## Azure Functions Deployment

### Prerequisites
```bash
# Install Azure Functions Core Tools
npm install -g azure-functions-core-tools@4

# Install Azure CLI
# https://docs.microsoft.com/en-us/cli/azure/install-azure-cli
```

### Deployment Steps

1. **Create function app structure:**
```bash
# Initialize function app
func init SumoCZAdapter --python --model V2

cd SumoCZAdapter

# Create HTTP trigger function
func new --name SumoCZFunction --template "Timer trigger" --authlevel anonymous
```

2. **Update `requirements.txt`:**
```txt
requests==2.32.4
azure-functions
```

3. **Replace `function_app.py` content:**
```python
import azure.functions as func
import logging
from sumo_anycost_lambda import lambda_handler

app = func.FunctionApp()

@app.schedule(schedule="0 0 0 * * *", arg_name="myTimer", run_on_startup=False)
def timer_trigger(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')
    
    lambda_handler({}, {})
    logging.info('SumoLogic to CloudZero sync completed')
```

4. **Copy your main file:**
```bash
cp ../sumo_anycost_lambda.py .
```

5. **Deploy:**
```bash
# Create resource group
az group create --name rg-sumo-cz-adapter --location eastus

# Create storage account
az storage account create \
  --name sumoczadapterstorage \
  --location eastus \
  --resource-group rg-sumo-cz-adapter \
  --sku Standard_LRS

# Create function app
az functionapp create \
  --resource-group rg-sumo-cz-adapter \
  --consumption-plan-location eastus \
  --runtime python \
  --runtime-version 3.10 \
  --functions-version 4 \
  --name sumo-cz-adapter \
  --storage-account sumoczadapterstorage

# Deploy function
func azure functionapp publish sumo-cz-adapter

# Set environment variables
az functionapp config appsettings set \
  --name sumo-cz-adapter \
  --resource-group rg-sumo-cz-adapter \
  --settings \
    SUMO_ACCESS_KEY="your-key" \
    SUMO_SECRET_KEY="your-secret" \
    SUMO_DEPLOYMENT="us1" \
    SUMO_ORG_ID="your-org-id" \
    CZ_AUTH_KEY="your-cz-key" \
    CZ_ANYCOST_STREAM_CONNECTION_ID="your-stream-id"
```

## Google Cloud Functions Deployment

### Prerequisites
```bash
# Install Google Cloud CLI
# https://cloud.google.com/sdk/docs/install

# Authenticate
gcloud auth login
gcloud config set project YOUR-PROJECT-ID
```

### Deployment Steps

1. **Create `requirements.txt`:**
```txt
requests==2.32.4
functions-framework
```

2. **Create `main.py` wrapper:**
```python
import functions_framework
from sumo_anycost_lambda import lambda_handler

@functions_framework.cloud_event
def sumo_cz_sync(cloud_event):
    lambda_handler({}, {})
    return "Sync completed"
```

3. **Deploy function:**
```bash
# Deploy with Cloud Scheduler trigger (daily)
gcloud functions deploy sumo-cz-adapter \
  --gen2 \
  --runtime python310 \
  --source . \
  --entry-point sumo_cz_sync \
  --trigger-topic sumo-cz-trigger \
  --timeout 300 \
  --memory 256MB \
  --set-env-vars \
    SUMO_ACCESS_KEY="your-key",\
    SUMO_SECRET_KEY="your-secret",\
    SUMO_DEPLOYMENT="us1",\
    SUMO_ORG_ID="your-org-id",\
    CZ_AUTH_KEY="your-cz-key",\
    CZ_ANYCOST_STREAM_CONNECTION_ID="your-stream-id"
```

4. **Create Cloud Scheduler job:**
```bash
# Create topic for scheduling
gcloud pubsub topics create sumo-cz-trigger

# Create daily scheduled job (IMPORTANT: Must run exactly once every 24 hours)
gcloud scheduler jobs create pubsub sumo-cz-daily \
  --schedule="0 0 * * *" \
  --topic=sumo-cz-trigger \
  --message-body="{}"
```

### Alternative: HTTP Trigger

For HTTP-triggered deployment:

1. **Create `main.py` with HTTP trigger:**
```python
import functions_framework
from sumo_anycost_lambda import lambda_handler

@functions_framework.http
def sumo_cz_sync(request):
    lambda_handler({}, {})
    return "Sync completed"
```

2. **Deploy:**
```bash
gcloud functions deploy sumo-cz-adapter \
  --gen2 \
  --runtime python310 \
  --source . \
  --entry-point sumo_cz_sync \
  --trigger-http \
  --allow-unauthenticated \
  --timeout 300
```

## Dependency Management Notes

**AWS Lambda:**
- Use `pip install -t .` to install packages in the deployment directory
- Or use container images with pip install in Dockerfile
- requests library is not included in Lambda runtime by default

**Azure Functions:**
- Dependencies in `requirements.txt` are automatically installed
- requests library is not included by default

**Google Cloud Functions:**
- Dependencies in `requirements.txt` are automatically installed during deployment
- requests library is not included in runtime by default

## Monitoring and Troubleshooting

- Set `LOGGING_LEVEL=DEBUG` for detailed logging
- Function timeout should be at least 300 seconds due to SumoLogic API query processing time
- Monitor for rate limiting (429 errors) - the adapter includes exponential backoff
- Verify CloudZero stream connection ID is correct for your organization

## Security Considerations

- Store API keys and secrets in your cloud provider's secret management service
- Use IAM roles with minimal required permissions
- Enable function logging and monitoring
- Consider VPC deployment for enhanced security