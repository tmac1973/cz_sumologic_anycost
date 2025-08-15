#!/bin/bash
# you must set these variables
export SUMO_ACCESS_KEY="<set this>"
export SUMO_SECRET_KEY="<set this>"
export SUMO_DEPLOYMENT="<set this>"
export SUMO_ORG_ID="<set this>"
export CZ_ANYCOST_STREAM_CONNECTION_ID="<set this>"
export CZ_AUTH_KEY="<set this>"

# You SHOULD set these variables according to your Sumologic contract
# otherwise they will use default values which may not be correct
# values here are the defaults.
#export LOG_CONTINUOUS_CREDIT_RATE="25"
#export LOG_FREQUENT_CREDIT_RATE="12"
#export LOG_INFREQUENT_CREDIT_RATE="5"
#export LOG_INFREQUENT_SCAN_CREDIT_RATE="0.16"
#export METRICS_CREDIT_RATE="10"
#export TRACING_CREDIT_RATE="35"
#export COST_PER_CREDIT="0.15"

# As of 08/2025 CZ only has the one API endpoint but this may change in the future. If so you can override here
#export CZ_URL='https://api.cloudzero.com'

# This script will default to querying the last hour of data for each run. You can increase that time window here
# If you make it too large though you may generate too much data for the CZ API to ingest in a single go
#export QUERY_TIME_HOURS="1"

# change this to "DEBUG" for more info in logs
export LOGGING_LEVEL="INFO"

python sumo_anycost_lambda.py