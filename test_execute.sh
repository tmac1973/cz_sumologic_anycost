#!/bin/bash
#
# Template execution script for SumoLogic to CloudZero adapter
#
# Usage:
#   ./test_execute.sh                                    # Standard mode (last 24 hours)
#   ./test_execute.sh --days 7                          # Backfill last 7 days
#   ./test_execute.sh --backfill-start 2024-01-01 --backfill-end 2024-01-07  # Backfill date range
#   ./test_execute.sh --days 30 --dry-run               # Preview 30-day backfill
#   ./test_execute.sh --days 7 --verbose                # Backfill with debug logging
#   ./test_execute.sh --days 30 --resume 2024-01-15     # Resume backfill from specific date
#

# =============================================================================
# REQUIRED CREDENTIALS - YOU MUST SET THESE
# =============================================================================
export SUMO_ACCESS_KEY="<set this>"
export SUMO_SECRET_KEY="<set this>"
export SUMO_DEPLOYMENT="<set this>"           # e.g., "us1", "us2", "eu", "au", etc.
export SUMO_ORG_ID="<set this>"
export CZ_ANYCOST_STREAM_CONNECTION_ID="<set this>"
export CZ_AUTH_KEY="<set this>"

# =============================================================================
# OPTIONAL CREDIT RATE CONFIGURATION
# You SHOULD set these variables according to your SumoLogic contract
# Otherwise they will use default values which may not be correct
# Values here are speculative and should be verified with your contract
# =============================================================================
#export LOG_CONTINUOUS_CREDIT_RATE="25"       # Credits per GB for continuous logs
#export LOG_FREQUENT_CREDIT_RATE="12"         # Credits per GB for frequent logs
#export LOG_INFREQUENT_CREDIT_RATE="5"        # Credits per GB for infrequent logs
#export LOG_INFREQUENT_SCAN_CREDIT_RATE="0.16"  # Credits per GB for infrequent scans
#export METRICS_CREDIT_RATE="10"              # Credits per 1000 datapoints
#export TRACING_CREDIT_RATE="35"              # Credits per GB for traces
#export COST_PER_CREDIT="0.15"                # USD cost per credit

# =============================================================================
# OPTIONAL CONFIGURATION OVERRIDES
# =============================================================================
# CloudZero API endpoint (should not need to change)
#export CZ_URL='https://api.cloudzero.com'

# Time window for standard mode (should be 1 day for daily operations)
#export QUERY_TIME_DAYS="1"

# Logging level: DEBUG, INFO, WARNING, ERROR
export LOGGING_LEVEL="INFO"

# =============================================================================
# HELP AND USAGE
# =============================================================================
if [[ "$1" == "-h" ]] || [[ "$1" == "--help" ]]; then
    echo "SumoLogic to CloudZero Adapter Execution Script"
    echo ""
    echo "Before running, you must set the required credentials above in this script."
    echo ""
    echo "Usage:"
    echo "  ./test_execute.sh                                                    # Standard mode (last 24 hours)"
    echo "  ./test_execute.sh --days N                                           # Backfill last N days"
    echo "  ./test_execute.sh --backfill-start YYYY-MM-DD --backfill-end YYYY-MM-DD  # Backfill date range"
    echo "  ./test_execute.sh --dry-run [other options]                         # Preview mode (no uploads)"
    echo "  ./test_execute.sh --verbose [other options]                         # Debug logging"
    echo "  ./test_execute.sh --quiet [other options]                           # Minimal logging"
    echo "  ./test_execute.sh --resume YYYY-MM-DD [other options]               # Resume from date"
    echo ""
    echo "Examples:"
    echo "  ./test_execute.sh                                                    # Daily run (standard)"
    echo "  ./test_execute.sh --days 7                                          # Backfill last week"
    echo "  ./test_execute.sh --days 30 --dry-run                               # Preview month backfill"
    echo "  ./test_execute.sh --backfill-start 2024-01-01 --backfill-end 2024-01-31  # January 2024"
    echo "  ./test_execute.sh --days 30 --resume 2024-01-15 --verbose           # Resume with debug logs"
    echo ""
    echo "Backfill Features:"
    echo "  • Processes data day-by-day to stay within CloudZero API limits"
    echo "  • Automatic data chunking for large datasets (>8MB)"
    echo "  • Progress tracking with ETA estimates"
    echo "  • Resume capability for interrupted backfills"
    echo "  • Dry-run mode to preview operations without uploading"
    echo "  • Comprehensive error handling and retry logic"
    echo ""
    exit 0
fi

# =============================================================================
# CREDENTIAL VALIDATION
# =============================================================================
missing_vars=()
if [[ "$SUMO_ACCESS_KEY" == "<set this>" ]]; then missing_vars+=("SUMO_ACCESS_KEY"); fi
if [[ "$SUMO_SECRET_KEY" == "<set this>" ]]; then missing_vars+=("SUMO_SECRET_KEY"); fi
if [[ "$SUMO_DEPLOYMENT" == "<set this>" ]]; then missing_vars+=("SUMO_DEPLOYMENT"); fi
if [[ "$SUMO_ORG_ID" == "<set this>" ]]; then missing_vars+=("SUMO_ORG_ID"); fi
if [[ "$CZ_ANYCOST_STREAM_CONNECTION_ID" == "<set this>" ]]; then missing_vars+=("CZ_ANYCOST_STREAM_CONNECTION_ID"); fi
if [[ "$CZ_AUTH_KEY" == "<set this>" ]]; then missing_vars+=("CZ_AUTH_KEY"); fi

if [[ ${#missing_vars[@]} -gt 0 ]]; then
    echo "❌ ERROR: Please set the following required variables in this script:"
    printf "   %s\n" "${missing_vars[@]}"
    echo ""
    echo "Edit $0 and replace '<set this>' with your actual values."
    echo "Run '$0 --help' for more information."
    exit 1
fi

# =============================================================================
# EXECUTION
# =============================================================================
echo "🚀 Running SumoLogic to CloudZero Adapter"
echo "📋 Arguments: $*"
echo "📊 Logging Level: $LOGGING_LEVEL"
echo ""

# Use python directly (not uv run) as this is the production template
python sumo_anycost_lambda.py "$@"