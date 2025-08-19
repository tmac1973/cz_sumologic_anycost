# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Python adapter that extracts SumoLogic billing data and forwards it to CloudZero's AnyTelemetry service. It processes usage data for logs (continuous, frequent, infrequent), metrics, and traces, converting them to CloudZero's billing format (CBF) and streaming via API.

**Note:** This is a single-file application designed for serverless deployment, not a distributable Python package.

## Development Commands

**Install dependencies:**
```bash
uv sync --group dev  # Install runtime and dev dependencies
```

**Common tasks (using poethepoet):**
```bash
poe run           # Run the application
poe test          # Run with template env vars (test_execute.sh)
poe test-with-creds  # Run with test credentials (test.sh)
poe type-check    # Check for syntax errors
poe lint          # Run flake8 linting
```

**Direct commands:**
```bash
python sumo_anycost_lambda.py  # Run directly
./test.sh  # Uses hardcoded test credentials
./test_execute.sh  # Template with placeholder values
```

## Architecture

**Core Components:**
- `SumoLogic` class: Handles authentication, API calls, and data extraction from SumoLogic
- `CloudZero` class: Manages authentication and data streaming to CloudZero AnyTelemetry
- `lambda_handler()`: Main orchestration function that processes all data types
- `backoff` decorator: Implements exponential backoff for rate-limited API calls

**Data Flow:**
1. Extract billing data from SumoLogic using predefined queries for each data type
2. Convert raw SumoLogic records to CloudZero Billing Format (CBF)
3. Stream converted data to CloudZero using `replace_drop` operation

**Required Environment Variables:**
- SumoLogic: `SUMO_ACCESS_KEY`, `SUMO_SECRET_KEY`, `SUMO_DEPLOYMENT`, `SUMO_ORG_ID`
- CloudZero: `CZ_AUTH_KEY`, `CZ_ANYCOST_STREAM_CONNECTION_ID`
- Optional: Credit rates, cost per credit, query time window, logging level

**Data Types Processed:**
- Continuous logs (25 credits/GB default)
- Frequent logs (12 credits/GB default) 
- Infrequent logs (5 credits/GB default)
- Infrequent log scans (0.16 credits/GB default)
- Metrics (10 credits/1000 datapoints default)
- Traces (35 credits/GB default)

The application queries the last hour of data by default (configurable via `QUERY_TIME_HOURS`).