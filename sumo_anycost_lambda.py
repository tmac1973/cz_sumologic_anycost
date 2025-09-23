import os
import requests
import json
import time
from datetime import datetime, timedelta, timezone
import logging
from functools import wraps
from enum import Enum
from typing import Dict, List, Optional, Any, Union
import io
import csv
import argparse
import sys
try:
    import cookielib
except ImportError:
    import http.cookiejar as cookielib

LOGGING_LEVEL_STRING = os.environ.get('LOGGING_LEVEL', "INFO")
match LOGGING_LEVEL_STRING:
    case "INFO":
        LOGGING_LEVEL = logging.INFO
    case "DEBUG":
        LOGGING_LEVEL = logging.DEBUG
    case _:
        LOGGING_LEVEL = logging.INFO

logger = logging.getLogger()
if len(logging.getLogger().handlers) > 0:
    # The Lambda environment pre-configures a handler logging to stderr. If a handler is already configured,
    # `.basicConfig` does not execute. Thus we set the level directly.
    logging.getLogger().setLevel(LOGGING_LEVEL)
else:
    logging.basicConfig(level=LOGGING_LEVEL)

def load_environment_variables():
    """Load and validate environment variables"""
    global SUMO_ACCESS_KEY, SUMO_SECRET_KEY, SUMO_ORG_ID, SUMO_DEPLOYMENT
    global CZ_AUTH_KEY, CZ_ANYCOST_STREAM_CONNECTION_ID
    global LOG_CONTINUOUS_CREDIT_RATE, LOG_FREQUENT_CREDIT_RATE, LOG_INFREQUENT_CREDIT_RATE
    global LOG_INFREQUENT_SCAN_CREDIT_RATE, METRICS_CREDIT_RATE, TRACING_CREDIT_RATE
    global COST_PER_CREDIT, QUERY_TIME_DAYS, QUERY_TIME_HOURS, CZ_URL
    global BACKFILL_MODE, BACKFILL_START_DATE, BACKFILL_END_DATE
    global DRY_RUN_MODE, RESUME_MODE, RESUME_DATE, AUTO_RESUME

    try:
        SUMO_ACCESS_KEY = os.environ.get('SUMO_ACCESS_KEY', None)
        if SUMO_ACCESS_KEY is None:
            raise ValueError("SUMO_ACCESS_KEY not set!")
        SUMO_SECRET_KEY = os.environ.get('SUMO_SECRET_KEY', None)
        if SUMO_SECRET_KEY is None:
            raise ValueError("SUMO_SECRET_KEY not set!")
        SUMO_ORG_ID = os.environ.get('SUMO_ORG_ID', None)
        if SUMO_ORG_ID is None:
            raise ValueError("SUMO_ORG_ID not set!")
        SUMO_DEPLOYMENT = os.environ.get('SUMO_DEPLOYMENT', None)
        if SUMO_DEPLOYMENT is None:
            raise ValueError("SUMO_DEPLOYMENT not set!")
        CZ_AUTH_KEY = os.environ.get('CZ_AUTH_KEY', None)
        if CZ_AUTH_KEY is None:
            raise ValueError("CZ_AUTH_KEY not set!")
        CZ_ANYCOST_STREAM_CONNECTION_ID = os.environ.get('CZ_ANYCOST_STREAM_CONNECTION_ID', None)
        if CZ_ANYCOST_STREAM_CONNECTION_ID is None:
            raise ValueError("CZ_ANYCOST_STREAM_CONNECTION_ID not set!")
        LOG_CONTINUOUS_CREDIT_RATE = float(os.environ.get('LOG_CONTINUOUS_CREDIT_RATE', '20'))
        LOG_FREQUENT_CREDIT_RATE = float(os.environ.get('LOG_FREQUENT_CREDIT_RATE', '9'))
        LOG_INFREQUENT_CREDIT_RATE = float(os.environ.get('LOG_INFREQUENT_CREDIT_RATE', '0.4'))
        LOG_INFREQUENT_SCAN_CREDIT_RATE = float(os.environ.get('LOG_INFREQUENT_SCAN_CREDIT_RATE', '0.016'))
        METRICS_CREDIT_RATE = float(os.environ.get('METRICS_CREDIT_RATE', '3'))
        TRACING_CREDIT_RATE = float(os.environ.get('TRACING_CREDIT_RATE', '14'))
        COST_PER_CREDIT = float(os.environ.get('COST_PER_CREDIT', '0.15'))
        QUERY_TIME_DAYS = float(os.environ.get('QUERY_TIME_DAYS', '1'))
        QUERY_TIME_HOURS = float(os.environ.get('QUERY_TIME_HOURS', str(QUERY_TIME_DAYS * 24)))
        CZ_URL = os.environ.get('CZ_URL', 'https://api.cloudzero.com')

        # Backfill mode environment variables
        BACKFILL_MODE = os.environ.get('BACKFILL_MODE', 'false').lower() == 'true'
        BACKFILL_START_DATE = os.environ.get('BACKFILL_START_DATE', None)
        BACKFILL_END_DATE = os.environ.get('BACKFILL_END_DATE', None)

        # Operation mode environment variables
        DRY_RUN_MODE = os.environ.get('DRY_RUN_MODE', 'false').lower() == 'true'
        RESUME_MODE = os.environ.get('RESUME_MODE', 'false').lower() == 'true'
        RESUME_DATE = os.environ.get('RESUME_DATE', None)
        AUTO_RESUME = os.environ.get('AUTO_RESUME', 'false').lower() == 'true'
    except ValueError as e:
        logger.error(f'Missing environmental variable: {e}')
        raise SystemExit(1)

# Initialize these as None - they will be loaded when needed
SUMO_ACCESS_KEY = None
SUMO_SECRET_KEY = None
SUMO_ORG_ID = None
SUMO_DEPLOYMENT = None
CZ_AUTH_KEY = None
CZ_ANYCOST_STREAM_CONNECTION_ID = None
LOG_CONTINUOUS_CREDIT_RATE = None
LOG_FREQUENT_CREDIT_RATE = None
LOG_INFREQUENT_CREDIT_RATE = None
LOG_INFREQUENT_SCAN_CREDIT_RATE = None
METRICS_CREDIT_RATE = None
TRACING_CREDIT_RATE = None
COST_PER_CREDIT = None
QUERY_TIME_DAYS = None
QUERY_TIME_HOURS = None
CZ_URL = None
BACKFILL_MODE = None
BACKFILL_START_DATE = None
BACKFILL_END_DATE = None
DRY_RUN_MODE = None
RESUME_MODE = None
RESUME_DATE = None
AUTO_RESUME = None

def build_queries():
    """Build SumoLogic queries after environment variables are loaded"""
    global CONTINUOUS_LOG_INGESTED, FREQUENT_LOG_INGESTED, INFREQUENT_LOG_INGESTED
    global INFREQUENT_LOG_SCANNED, TRACES_INGESTED, METRICS_INGESTED

    CONTINUOUS_LOG_INGESTED = '_index=sumologic_volume _sourceCategory = "sourcecategory_and_tier_volume"| parse regex "(?<data>\\{[^\\{]+\\})" multi | json field=data "field","dataTier","sizeInBytes","count" as sourcecategory, dataTier, bytes, count |where dataTier matches "Continuous" | where !(sourcecategory matches "*_volume") | bytes/1Gi as gbytes | timeslice 1h | sum(gbytes) as gbytes by _timeslice, sourceCategory, dataTier | gbytes*' + str(
        LOG_CONTINUOUS_CREDIT_RATE) + ' as credits '
    FREQUENT_LOG_INGESTED = '_index=sumologic_volume _sourceCategory = "sourcecategory_and_tier_volume"| parse regex "(?<data>\\{[^\\{]+\\})" multi | json field=data "field","dataTier","sizeInBytes","count" as sourcecategory, dataTier, bytes, count |where dataTier matches "Frequent" | where !(sourcecategory matches "*_volume") | bytes/1Gi as gbytes | timeslice 1h | sum(gbytes) as gbytes by _timeslice, sourceCategory, dataTier | gbytes*' + str(
        LOG_FREQUENT_CREDIT_RATE) + ' as credits '
    INFREQUENT_LOG_INGESTED = '_index=sumologic_volume _sourceCategory = "sourcecategory_and_tier_volume"| parse regex "(?<data>\\{[^\\{]+\\})" multi | json field=data "field","dataTier","sizeInBytes","count" as sourcecategory, dataTier, bytes, count |where dataTier matches "Infrequent" | where !(sourcecategory matches "*_volume") | bytes/1Gi as gbytes | timeslice 1h | sum(gbytes) as gbytes by _timeslice, sourceCategory, dataTier | gbytes*' + str(
        LOG_INFREQUENT_CREDIT_RATE) + ' as credits '
    INFREQUENT_LOG_SCANNED = '_view=sumologic_search_usage_per_query  !(user_name=*sumologic.com) !(status_message="Query Failed") | json field=scanned_bytes_breakdown "Infrequent" as data_scanned_bytes | analytics_tier as datatier |fields data_scanned_bytes, query, is_aggregate, query_type, status_message, user_name, datatier| if (query_type == "View Maintenance", "Scheduled Views", query_type) as query_type| data_scanned_bytes / 1Gi as gbytes| timeslice 1h|sum (gbytes) as gbytes by _timeslice, user_name| fillmissing timeslice (1h) | gbytes * ' + str(
        LOG_INFREQUENT_SCAN_CREDIT_RATE) + ' as credits'
    TRACES_INGESTED = '_index=sumologic_volume _sourceCategory="sourcecategory_tracing_volume"| parse regex "\\"(?<sourcecategory>[^\\"]+)\\"\\:(?<data>\\{[^\\}]*\\})" multi| json field=data "billedBytes","spansCount" as bytes, spans| bytes/1Gi as gbytes  | timeslice 1h | sum(gbytes) as gbytes, sum(spans) as spans by _timeslice, sourcecategory| gbytes*' + str(
        TRACING_CREDIT_RATE) + ' as credits '
    METRICS_INGESTED = '_index=sumologic_volume _sourceCategory="sourcecategory_metrics_volume" datapoints| parse regex "\\"(?<sourcecategory>[^\\"]+)\\"\\:\\{\\"dataPoints\\"\\:(?<datapoints>\\d+)\\}" multi| timeslice 24h | sum(datapoints) as datapoints by sourcecategory, _timeslice| ((queryEndTime() - queryStartTime())/(1000*60)) as duration_in_min| datapoints / duration_in_min as %"DPM" | DPM/1000 as AvgKDPM | AvgKDPM *' + str(
        METRICS_CREDIT_RATE) + ' as credits '

# Initialize query variables
CONTINUOUS_LOG_INGESTED = None
FREQUENT_LOG_INGESTED = None
INFREQUENT_LOG_INGESTED = None
INFREQUENT_LOG_SCANNED = None
TRACES_INGESTED = None
METRICS_INGESTED = None
# API RATE Limit constants
MAX_TRIES = 10
NUMBER_OF_CALLS = 4
# per
PERIOD = 1  # in seconds


def backoff(func):
    @wraps(func)
    def limited(*args, **kwargs):
        delay = PERIOD / NUMBER_OF_CALLS * 2
        tries = 0
        while tries < MAX_TRIES:
            try:
                return func(*args, **kwargs)
            except Exception as e:
                if e.response.status_code == 429:  # rate limited
                    lastException = e
                    tries += 1
                    logger.debug("Rate limited, sleeping for {0}s".format(delay))
                else:
                    raise
            logger.debug(f"delay: {delay} attempts: {tries}")
            time.sleep(delay)
            delay = delay * 2
        logger.error("Rate limited function failed after {0} retries.".format(MAX_TRIES))
        raise lastException

    return limited


def validate_date_string(date_str: str, date_name: str) -> datetime:
    """Validate and parse date string in YYYY-MM-DD format with comprehensive error checking"""
    if not date_str:
        raise ValueError(f"{date_name} is required when BACKFILL_MODE is enabled")

    if not isinstance(date_str, str):
        raise ValueError(f"{date_name} must be a string")

    # Check basic format before parsing
    if len(date_str) != 10 or date_str.count('-') != 2:
        raise ValueError(f"Invalid {date_name} format. Expected YYYY-MM-DD, got: {date_str}")

    try:
        parsed_date = datetime.strptime(date_str, '%Y-%m-%d').replace(tzinfo=timezone.utc)

        # Additional validation checks
        if parsed_date.year < 2020:
            raise ValueError(f"{date_name} year {parsed_date.year} is too old (minimum: 2020)")

        if parsed_date > datetime.now(timezone.utc):
            raise ValueError(f"{date_name} {date_str} is in the future")

        return parsed_date

    except ValueError as e:
        if "does not match format" in str(e):
            # Try to give more specific error messages
            parts = date_str.split('-')
            if len(parts) != 3:
                raise ValueError(f"Invalid {date_name} format. Expected YYYY-MM-DD, got: {date_str}")

            year, month, day = parts
            try:
                year_int = int(year)
                if not (1900 <= year_int <= 2100):
                    raise ValueError(f"Invalid year in {date_name}: {year} (expected 4-digit year)")
            except ValueError:
                raise ValueError(f"Invalid year in {date_name}: {year} (must be a number)")

            try:
                month_int = int(month)
                if not (1 <= month_int <= 12):
                    raise ValueError(f"Invalid month in {date_name}: {month} (expected 01-12)")
            except ValueError:
                raise ValueError(f"Invalid month in {date_name}: {month} (must be a number)")

            try:
                day_int = int(day)
                if not (1 <= day_int <= 31):
                    raise ValueError(f"Invalid day in {date_name}: {day} (expected 01-31)")
            except ValueError:
                raise ValueError(f"Invalid day in {date_name}: {day} (must be a number)")

        # Re-raise the original error if we can't provide a better one
        raise ValueError(f"Invalid {date_name}: {str(e)}")


def validate_backfill_config() -> tuple[Optional[datetime], Optional[datetime]]:
    """Validate backfill configuration and return start/end datetime objects"""
    if not BACKFILL_MODE:
        return None, None

    start_dt = validate_date_string(BACKFILL_START_DATE, "BACKFILL_START_DATE")
    end_dt = validate_date_string(BACKFILL_END_DATE, "BACKFILL_END_DATE")

    if start_dt > end_dt:
        raise ValueError(f"BACKFILL_START_DATE ({start_dt.date()}) must be before or equal to BACKFILL_END_DATE ({end_dt.date()})")

    # Check for reasonable date range limits
    total_days = (end_dt - start_dt).days + 1
    if total_days > 365:
        logger.warning(f"⚠️  Large backfill range: {total_days} days. Consider breaking into smaller chunks.")

    if total_days > 1000:
        raise ValueError(f"Backfill range too large: {total_days} days (maximum: 1000 days)")

    # Validate resume date if specified
    if RESUME_MODE and RESUME_DATE:
        resume_dt = validate_date_string(RESUME_DATE, "RESUME_DATE")
        if resume_dt < start_dt or resume_dt > end_dt:
            raise ValueError(f"RESUME_DATE ({resume_dt.date()}) must be within backfill range "
                           f"({start_dt.date()} to {end_dt.date()})")

    # Set start to beginning of day, end to end of day
    start_dt = start_dt.replace(hour=0, minute=0, second=0, microsecond=0)
    end_dt = end_dt.replace(hour=23, minute=59, second=59, microsecond=999999)

    mode_description = "DRY RUN " if DRY_RUN_MODE else ""
    logger.info(f"Backfill mode enabled: {mode_description}{start_dt.date()} to {end_dt.date()} ({total_days} days)")

    return start_dt, end_dt


def generate_date_range(start_dt: datetime, end_dt: datetime) -> List[tuple[datetime, datetime]]:
    """Generate list of (start, end) datetime tuples for each day in the range"""
    date_ranges = []
    current_date = start_dt.date()
    end_date = end_dt.date()

    while current_date <= end_date:
        day_start = datetime.combine(current_date, datetime.min.time()).replace(tzinfo=timezone.utc)
        day_end = datetime.combine(current_date, datetime.max.time()).replace(tzinfo=timezone.utc)
        date_ranges.append((day_start, day_end))
        current_date += timedelta(days=1)

    return date_ranges


class BackfillProgress:
    """Track and report progress for backfill operations"""

    def __init__(self, total_days: int):
        self.total_days = total_days
        self.current_day = 0
        self.start_time = datetime.now()
        self.daily_stats = []
        self.service_types = [
            "continuous logs", "frequent logs", "infrequent logs",
            "infrequent log scans", "metrics", "traces", "storage"
        ]

    def start_day(self, date: str) -> None:
        """Mark the start of processing a new day"""
        self.current_day += 1
        self.current_date = date
        self.day_start_time = datetime.now()

        elapsed_total = (datetime.now() - self.start_time).total_seconds()

        if self.current_day == 1:
            logger.info(f"📅 Starting day {self.current_day}/{self.total_days}: {date}")
        else:
            # Calculate ETA based on average time per day
            avg_time_per_day = elapsed_total / (self.current_day - 1)
            remaining_days = self.total_days - self.current_day + 1
            eta_seconds = remaining_days * avg_time_per_day
            eta_str = self._format_duration(eta_seconds)

            logger.info(f"📅 Starting day {self.current_day}/{self.total_days}: {date} (ETA: {eta_str})")

    def complete_day(self, day_stats: dict) -> None:
        """Mark completion of a day with statistics"""
        day_duration = (datetime.now() - self.day_start_time).total_seconds()
        day_stats['duration'] = day_duration
        day_stats['date'] = self.current_date
        self.daily_stats.append(day_stats)

        # Log day completion with summary
        total_records = sum(day_stats.get('records', {}).values())
        successful_services = day_stats.get('successful_services', 0)
        total_services = len(self.service_types)

        logger.info(f"✅ Completed {self.current_date}: {total_records} records, "
                   f"{successful_services}/{total_services} services successful "
                   f"({self._format_duration(day_duration)})")

    def get_summary(self) -> dict:
        """Generate final summary statistics"""
        total_duration = (datetime.now() - self.start_time).total_seconds()

        summary = {
            'total_days_processed': len(self.daily_stats),
            'total_duration': total_duration,
            'total_records': 0,
            'total_successful_services': 0,
            'total_failed_services': 0,
            'service_breakdown': {service: {'records': 0, 'successes': 0, 'failures': 0}
                                for service in self.service_types},
            'daily_stats': self.daily_stats
        }

        for day_stat in self.daily_stats:
            summary['total_records'] += sum(day_stat.get('records', {}).values())
            summary['total_successful_services'] += day_stat.get('successful_services', 0)
            summary['total_failed_services'] += day_stat.get('failed_services', 0)

            # Aggregate service breakdown
            for service, count in day_stat.get('records', {}).items():
                if service in summary['service_breakdown']:
                    summary['service_breakdown'][service]['records'] += count

            for service in day_stat.get('successful_services_list', []):
                if service in summary['service_breakdown']:
                    summary['service_breakdown'][service]['successes'] += 1

            for service in day_stat.get('failed_services_list', []):
                if service in summary['service_breakdown']:
                    summary['service_breakdown'][service]['failures'] += 1

        return summary

    def _format_duration(self, seconds: float) -> str:
        """Format duration in human-readable format"""
        if seconds < 60:
            return f"{seconds:.1f}s"
        elif seconds < 3600:
            return f"{seconds/60:.1f}m"
        else:
            hours = int(seconds // 3600)
            minutes = int((seconds % 3600) // 60)
            return f"{hours}h {minutes}m"


class BackfillState:
    """Manage backfill state persistence for automatic resume functionality"""

    def __init__(self, backfill_start: datetime, backfill_end: datetime):
        self.backfill_start = backfill_start
        self.backfill_end = backfill_end
        self.state_file = self._get_state_file_path()
        self.completed_days = set()
        self.last_completed_day = None
        self.start_time = None
        self.is_dry_run = DRY_RUN_MODE

    def _get_state_file_path(self) -> str:
        """Generate state file path based on date range"""
        start_str = self.backfill_start.strftime("%Y%m%d")
        end_str = self.backfill_end.strftime("%Y%m%d")
        return f".backfill_state_{start_str}_to_{end_str}.json"

    def load_existing_state(self) -> bool:
        """Load existing state file if it exists. Returns True if state was loaded."""
        if not os.path.exists(self.state_file):
            return False

        try:
            with open(self.state_file, 'r') as f:
                state_data = json.load(f)

            # Validate state file matches current backfill parameters
            stored_start = datetime.fromisoformat(state_data['backfill_start'])
            stored_end = datetime.fromisoformat(state_data['backfill_end'])

            if (stored_start.date() != self.backfill_start.date() or
                stored_end.date() != self.backfill_end.date()):
                logger.warning("⚠️  Existing state file doesn't match current date range, ignoring")
                return False

            # Load state
            self.completed_days = set(state_data.get('completed_days', []))
            self.last_completed_day = state_data.get('last_completed_day')
            self.start_time = datetime.fromisoformat(state_data['start_time']) if state_data.get('start_time') else None
            self.is_dry_run = state_data.get('is_dry_run', False)

            logger.info(f"📋 Loaded existing state: {len(self.completed_days)} days completed")
            if self.last_completed_day:
                logger.info(f"   Last completed day: {self.last_completed_day}")

            return True

        except (json.JSONDecodeError, KeyError, ValueError) as e:
            logger.warning(f"⚠️  Invalid state file format: {e}, starting fresh")
            return False

    def save_state(self) -> None:
        """Save current state to file"""
        if self.is_dry_run:
            logger.debug("🔍 DRY RUN: Skipping state file save")
            return

        state_data = {
            'backfill_start': self.backfill_start.isoformat(),
            'backfill_end': self.backfill_end.isoformat(),
            'completed_days': sorted(list(self.completed_days)),
            'last_completed_day': self.last_completed_day,
            'start_time': self.start_time.isoformat() if self.start_time else None,
            'is_dry_run': self.is_dry_run,
            'last_updated': datetime.now().isoformat()
        }

        try:
            with open(self.state_file, 'w') as f:
                json.dump(state_data, f, indent=2)
            logger.debug(f"💾 State saved to {self.state_file}")
        except Exception as e:
            logger.warning(f"⚠️  Failed to save state file: {e}")

    def mark_day_completed(self, date: str, day_stats: dict) -> None:
        """Mark a day as fully completed (all services successful)"""
        # Only mark as completed if all services were successful or had no data
        if day_stats.get('failed_services', 0) == 0:
            self.completed_days.add(date)
            self.last_completed_day = date
            self.save_state()
            logger.debug(f"✅ Marked {date} as completed in state")
        else:
            logger.debug(f"⚠️  Day {date} had {day_stats.get('failed_services', 0)} failed services, not marking as completed")

    def get_resume_date(self) -> str:
        """Get the date to resume from (next day after last completed)"""
        if not self.last_completed_day:
            return self.backfill_start.strftime("%Y-%m-%d")

        # Resume from the day after the last completed day
        last_completed = datetime.strptime(self.last_completed_day, "%Y-%m-%d")
        resume_date = last_completed + timedelta(days=1)

        # Ensure we don't go beyond the end date
        if resume_date.date() > self.backfill_end.date():
            return None  # Backfill is complete

        return resume_date.strftime("%Y-%m-%d")

    def is_backfill_complete(self) -> bool:
        """Check if backfill is complete"""
        if not self.completed_days:
            return False

        # Generate all expected days in the range
        expected_days = set()
        current = self.backfill_start
        while current.date() <= self.backfill_end.date():
            expected_days.add(current.strftime("%Y-%m-%d"))
            current += timedelta(days=1)

        # Check if all days are completed
        return expected_days.issubset(self.completed_days)

    def cleanup_state_file(self) -> None:
        """Remove state file when backfill is complete"""
        if os.path.exists(self.state_file):
            try:
                os.remove(self.state_file)
                logger.info(f"🧹 Cleaned up state file: {self.state_file}")
            except Exception as e:
                logger.warning(f"⚠️  Failed to remove state file: {e}")

    def get_progress_summary(self) -> dict:
        """Get progress summary for logging"""
        total_days = (self.backfill_end.date() - self.backfill_start.date()).days + 1
        completed_days = len(self.completed_days)

        return {
            'total_days': total_days,
            'completed_days': completed_days,
            'remaining_days': total_days - completed_days,
            'completion_percentage': (completed_days / total_days * 100) if total_days > 0 else 0,
            'last_completed_day': self.last_completed_day
        }


def parse_command_line_args() -> None:
    """Parse command line arguments and override environment variables for local execution"""
    parser = argparse.ArgumentParser(
        description='SumoLogic to CloudZero billing data adapter',
        epilog='''
Examples:
  %(prog)s                                    # Standard mode (last 24 hours)
  %(prog)s --backfill-start 2024-01-01 --backfill-end 2024-01-31  # Backfill January 2024
  %(prog)s --days 30                         # Backfill last 30 days
  %(prog)s --days 7 --verbose                # Backfill last 7 days with debug logging
  %(prog)s --days 30 --dry-run               # Preview 30-day backfill without uploading
        ''',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    # Backfill mode options
    backfill_group = parser.add_argument_group('backfill options')
    backfill_group.add_argument('--backfill-start', type=str, metavar='YYYY-MM-DD',
                               help='Start date for backfill (enables backfill mode)')
    backfill_group.add_argument('--backfill-end', type=str, metavar='YYYY-MM-DD',
                               help='End date for backfill (enables backfill mode)')
    backfill_group.add_argument('--days', type=int, metavar='N',
                               help='Number of days to backfill from today (alternative to start/end dates)')

    # Operation options
    operation_group = parser.add_argument_group('operation options')
    operation_group.add_argument('--dry-run', action='store_true',
                                 help='Preview backfill without actually uploading data to CloudZero')
    operation_group.add_argument('--resume', nargs='?', const='auto', metavar='YYYY-MM-DD',
                                help='Resume backfill automatically from state file, or from specified date')

    # Logging options
    logging_group = parser.add_argument_group('logging options')
    logging_group.add_argument('--verbose', '-v', action='store_true',
                              help='Enable debug logging')
    logging_group.add_argument('--quiet', '-q', action='store_true',
                              help='Reduce output to essential messages only')

    args = parser.parse_args()

    # Validate argument combinations
    if args.backfill_start and args.days:
        print("ERROR: Cannot use both --backfill-start and --days. Choose one approach.")
        sys.exit(1)

    if args.backfill_end and args.days:
        print("ERROR: Cannot use both --backfill-end and --days. Choose one approach.")
        sys.exit(1)

    if (args.backfill_start or args.backfill_end) and not (args.backfill_start and args.backfill_end):
        print("ERROR: Both --backfill-start and --backfill-end are required when using date ranges")
        sys.exit(1)

    if args.verbose and args.quiet:
        print("ERROR: Cannot use both --verbose and --quiet")
        sys.exit(1)

    # Set logging level
    if args.verbose:
        os.environ['LOGGING_LEVEL'] = 'DEBUG'
    elif args.quiet:
        os.environ['LOGGING_LEVEL'] = 'WARNING'

    # Set dry-run mode
    if args.dry_run:
        os.environ['DRY_RUN_MODE'] = 'true'
        print("🔍 DRY RUN MODE: No data will be uploaded to CloudZero")

    # Set resume mode
    if args.resume:
        os.environ['RESUME_MODE'] = 'true'
        if args.resume == 'auto':
            os.environ['AUTO_RESUME'] = 'true'
            print("🔄 AUTO RESUME MODE: Will resume from state file if available")
        else:
            os.environ['RESUME_DATE'] = args.resume
            print(f"🔄 MANUAL RESUME MODE: Starting from {args.resume}")

    # Set backfill parameters
    if args.backfill_start and args.backfill_end:
        os.environ['BACKFILL_MODE'] = 'true'
        os.environ['BACKFILL_START_DATE'] = args.backfill_start
        os.environ['BACKFILL_END_DATE'] = args.backfill_end
        print(f"📅 Backfill range: {args.backfill_start} to {args.backfill_end}")

    elif args.days:
        # Calculate date range from today backwards
        end_date = datetime.now(timezone.utc).date()
        start_date = end_date - timedelta(days=args.days - 1)  # -1 because we include today
        os.environ['BACKFILL_MODE'] = 'true'
        os.environ['BACKFILL_START_DATE'] = start_date.strftime('%Y-%m-%d')
        os.environ['BACKFILL_END_DATE'] = end_date.strftime('%Y-%m-%d')
        print(f"📅 Backfill period: {args.days} days ({start_date} to {end_date})")


class SumoLogic:
    # number of records to return
    NUM_RECORDS = 1000

    def __init__(self, access_id: str, access_key: str, deployment: str, cookieFile='cookies.txt'):
        self.session = requests.Session()
        self.session.auth = (access_id, access_key)
        self.DEFAULT_VERSION = 'v1'
        self.session.headers = {'content-type': 'application/json', 'accept': 'application/json'}
        self.deployment = deployment
        self.endpoint = self.endpoint_lookup(self.deployment)
        cj = cookielib.FileCookieJar(cookieFile)
        self.session.cookies = cj

    def endpoint_lookup(self, deployment: str) -> str:
        # there are duplicates here because most deployments have 2 names
        endpoints = {'prod': 'https://api.sumologic.com/api',
                     'us1': 'https://api.sumologic.com/api',
                     'us2': 'https://api.us2.sumologic.com/api',
                     'eu': 'https://api.eu.sumologic.com/api',
                     'dub': 'https://api.eu.sumologic.com/api',
                     'ca': 'https://api.ca.sumologic.com/api',
                     'mon': 'https://api.ca.sumologic.com/api',
                     'de': 'https://api.de.sumologic.com/api',
                     'fra': 'https://api.de.sumologic.com/api',
                     'au': 'https://api.au.sumologic.com/api',
                     'syd': 'https://api.au.sumologic.com/api',
                     'jp': 'https://api.jp.sumologic.com/api',
                     'tky': 'https://api.jp.sumologic.com/api',
                     'kr': 'https://api.kr.sumologic.com/api',
                     'fed': 'https://api.fed.sumologic.com/api',
                     }
        deployment_key = str(deployment).lower()
        if deployment_key not in endpoints:
            raise ValueError(
                f"Unsupported SumoLogic deployment: {deployment}. Supported deployments: {', '.join(endpoints.keys())}")
        return endpoints[deployment_key]

    def get_versioned_endpoint(self, version: str) -> str:
        return f'{self.endpoint}/{version}'

    @backoff
    def get(self, method: str, params: Optional[Dict[str, Any]] = None,
            version: Optional[str] = None) -> requests.Response:
        version = version or self.DEFAULT_VERSION
        endpoint = self.get_versioned_endpoint(version)
        r = self.session.get(endpoint + method, params=params)
        if 400 <= r.status_code < 600:
            r.reason = r.text
        r.raise_for_status()
        return r

    @backoff
    def post(self, method: str, params: Dict[str, Any], headers: Optional[Dict[str, str]] = None,
             version: Optional[str] = None) -> requests.Response:
        version = version or self.DEFAULT_VERSION
        endpoint = self.get_versioned_endpoint(version)
        r = self.session.post(endpoint + method, data=json.dumps(params), headers=headers)
        if 400 <= r.status_code < 600:
            r.reason = r.text
        r.raise_for_status()
        return r

    def search_job(self, query: str, from_time: Optional[str] = None, to_time: Optional[str] = None,
                   time_zone: str = 'UTC', by_receipt_time: bool = False) -> Dict[str, Any]:
        params = {'query': query, 'from': from_time, 'to': to_time, 'timeZone': time_zone,
                  'byReceiptTime': by_receipt_time, 'autoParsingMode': 'AutoParse'}
        r = self.post('/search/jobs', params)
        return json.loads(r.text)

    def search_job_status(self, search_job: Dict[str, Any]) -> Dict[str, Any]:
        r = self.get('/search/jobs/' + str(search_job['id']))
        return json.loads(r.text)

    def search_job_messages(self, search_job: Dict[str, Any], limit: Optional[int] = None, offset: int = 0) -> Dict[
        str, Any]:
        params = {'limit': limit, 'offset': offset}
        r = self.get('/search/jobs/' + str(search_job['id']) + '/messages', params)
        return json.loads(r.text)

    def search_job_records(self, search_job: Dict[str, Any], limit: Optional[int] = None, offset: int = 0) -> Dict[
        str, Any]:
        params = {'limit': limit, 'offset': offset}
        r = self.get('/search/jobs/' + str(search_job['id']) + '/records', params)
        return json.loads(r.text)

    def search_job_records_sync(self, query: str, from_time: Optional[str] = None, to_time: Optional[str] = None,
                                time_zone: Optional[str] = None, by_receipt_time: bool = False) -> Union[
        List[Dict[str, Any]], Dict[str, Any]]:
        searchjob = self.search_job(query, from_time=from_time, to_time=to_time, time_zone=time_zone,
                                    by_receipt_time=by_receipt_time)
        status = self.search_job_status(searchjob)
        numrecords = status['recordCount']
        while status['state'] != 'DONE GATHERING RESULTS':
            if status['state'] == 'CANCELLED':
                break
            status = self.search_job_status(searchjob)
            numrecords = status['recordCount']
        if status['state'] == 'DONE GATHERING RESULTS':
            logger.info(f"numrecords {numrecords}")
            jobrecords = []
            iterations = numrecords // self.NUM_RECORDS + 1

            for iteration in range(1, iterations + 1):
                records = self.search_job_records(searchjob, limit=self.NUM_RECORDS,
                                                  offset=((iteration - 1) * self.NUM_RECORDS))
                for record in records['records']:
                    jobrecords.append(record)
            return jobrecords  # returns a list
        else:
            return status

    def search_job_messages_sync(self, query: str, from_time: Optional[str] = None, to_time: Optional[str] = None,
                                 time_zone: Optional[str] = None, by_receipt_time: bool = False) -> Union[
        List[Dict[str, Any]], Dict[str, Any]]:
        searchjob = self.search_job(query, from_time=from_time, to_time=to_time, time_zone=time_zone,
                                    by_receipt_time=by_receipt_time)
        status = self.search_job_status(searchjob)
        nummessages = status['messageCount']
        while status['state'] != 'DONE GATHERING RESULTS':
            if status['state'] == 'CANCELLED':
                break
            status = self.search_job_status(searchjob)
            nummessages = status['messageCount']
        if status['state'] == 'DONE GATHERING RESULTS':
            jobmessages = []
            iterations = nummessages // self.NUM_RECORDS + 1

            for iteration in range(1, iterations + 1):
                messages = self.search_job_messages(searchjob, limit=self.NUM_RECORDS,
                                                    offset=((iteration - 1) * self.NUM_RECORDS))
                for message in messages['messages']:
                    jobmessages.append(message)
            return jobmessages  # returns a list
        else:
            return status


    def export_usage_report(self, from_time: Optional[str] = None, to_time: Optional[str] = None,
                                    group_by: str = "day", report_type: str = "detailed",
                                    include_deployment_charge: bool = False) -> str:
        logger.debug(f"Exporting usage report with dates: {str(from_time)}, {str(to_time)}")
        body = {'startDate': from_time,
                'endDate': to_time,
                'groupBy': group_by,
                'reportType': report_type,
                'includeDeploymentCharge': include_deployment_charge
                }
        r = self.post('/account/usage/report', body)
        return json.loads(r.text)

    def export_usage_report_status(self, job_id: int)-> Union[List[Dict[str, Any]], Dict[str, Any]]:
        r = self.get('/account/usage/report/' + str(job_id) + '/status')
        return json.loads(r.text)

    def export_usage_report_sync(self, from_time: Optional[str] = None, to_time: Optional[str] = None,
                                    group_by: str = "day", report_type: str = "detailed",
                                    include_deployment_charge: bool = False) -> str:
        export_job = self.export_usage_report(from_time, to_time, group_by, report_type, include_deployment_charge)
        status = self.export_usage_report_status(export_job['jobId'])
        logger.debug(status)
        while status['status'] != 'Success':
            if status['status'] == 'CANCELLED':
                break
            status = self.export_usage_report_status(export_job['jobId'])
            logger.debug(status)
        if status['status'] == 'Success':
            return requests.get(status['reportDownloadURL'])
        else:
            return None

    def get_billing_data(self, query: str, use_receipt_time: bool = True,
                         start_datetime: Optional[datetime] = None,
                         end_datetime: Optional[datetime] = None) -> Union[
        List[Dict[str, Any]], Dict[str, Any]]:

        if start_datetime is None:
            start_datetime = datetime.now(timezone.utc) - timedelta(hours=QUERY_TIME_HOURS)
        if end_datetime is None:
            end_datetime = datetime.now(timezone.utc)

        QUERY_START_DATETIME = start_datetime.strftime('%Y-%m-%dT%H:%M:%SZ')
        QUERY_END_DATETIME = end_datetime.strftime('%Y-%m-%dT%H:%M:%SZ')
        results = self.search_job_records_sync(query, QUERY_START_DATETIME, QUERY_END_DATETIME,
                                               by_receipt_time=use_receipt_time)
        return results

    def get_billing_data_api(self) -> List[Dict[str, str]]:

        results = self.export_usage_report_sync()
        # Parse CSV without pandas
        csv_reader = csv.DictReader(io.StringIO(results.text))
        return list(csv_reader)

    def convert_logs_to_cbf(self, records: List[Dict[str, Any]]) -> List[Dict[str, str]]:

        results = []
        for record in records:
            try:
                record_map = record['map']
                logger.debug(record_map)
                results.append({
                    'time/usage_start': datetime.fromtimestamp(float(record_map['_timeslice'].strip()) / 1000).replace(
                        tzinfo=timezone.utc).isoformat(),
                    'resource/id': f"sourcecategory/{record_map['sourcecategory'].lower().replace('/', '|')}",
                    'resource/usage_family': record_map['datatier'],
                    'lineitem/type': "Usage",
                    'lineitem/description': f"{record_map['datatier']} logs ingested by Source Category",
                    'resource/service': f"Logs {record_map['datatier'].lower()} ingest",
                    'resource/account': SUMO_ORG_ID,
                    'resource/region': SUMO_DEPLOYMENT,
                    'usage/units': "credits",
                    'action/operation': "ingest",
                    'usage/amount': f"{float(record_map['credits'].strip()):6f}",
                    'cost/cost': f"{(float(record_map['credits'].strip()) * COST_PER_CREDIT):6f}",
                })
            except(KeyError, Exception) as e:
                logger.warning(f"Skipping record: {e}")
        return results

    def convert_logs_scanned_to_cbf(self, records: List[Dict[str, Any]]) -> List[Dict[str, str]]:

        results = []
        for record in records:
            try:
                record_map = record['map']
                logger.debug(record_map)
                results.append({
                    'time/usage_start': datetime.fromtimestamp(float(record_map['_timeslice'].strip()) / 1000).replace(
                        tzinfo=timezone.utc).isoformat(),
                    'resource/id': f"username/{record_map['user_name'].lower()}",
                    'resource/usage_family': 'infrequent',
                    'lineitem/type': "Usage",
                    'lineitem/description': "Infrequent logs scanned by user",
                    'resource/service': "Logs infrequent scan",
                    'resource/account': SUMO_ORG_ID,
                    'resource/region': SUMO_DEPLOYMENT,
                    'usage/units': "credits",
                    'action/operation': "scan",
                    'usage/amount': f"{float(record_map['credits'].strip()):6f}",
                    'cost/cost': f"{(float(record_map['credits'].strip()) * COST_PER_CREDIT):6f}",
                })
            except(KeyError, Exception) as e:
                logger.warning(f"Skipping record: {e}")
        return results

    def convert_traces_to_cbf(self, records: List[Dict[str, Any]]) -> List[Dict[str, str]]:

        results = []
        for record in records:
            try:
                record_map = record['map']
                logger.debug(record_map)
                results.append({
                    'time/usage_start': datetime.fromtimestamp(float(record_map['_timeslice'].strip()) / 1000).replace(
                        tzinfo=timezone.utc).isoformat(),
                    'resource/id': f"sourcecategory/{record_map['sourcecategory'].lower().replace('/', '|')}",
                    'resource/usage_family': 'traces',
                    'lineitem/type': "Usage",
                    'lineitem/description': "tracing spans ingested by Source Category",
                    'resource/service': "Traces ingest",
                    'resource/account': SUMO_ORG_ID,
                    'resource/region': SUMO_DEPLOYMENT,
                    'usage/units': "credits",
                    'action/operation': "ingest",
                    'usage/amount': f"{float(record_map['credits'].strip()):6f}",
                    'cost/cost': f"{(float(record_map['credits'].strip()) * COST_PER_CREDIT):6f}",
                })
            except(KeyError, Exception) as e:
                logger.warning(f"Skipping record: {e}")
        return results

    def convert_metrics_to_cbf(self, records: List[Dict[str, Any]]) -> List[Dict[str, str]]:

        results = []
        for record in records:
            try:
                record_map = record['map']
                logger.debug(record_map)
                results.append({
                    'time/usage_start': datetime.fromtimestamp(float(record_map['_timeslice'].strip()) / 1000).replace(
                        tzinfo=timezone.utc).isoformat(),
                    'resource/id': f"sourcecategory/{record_map['sourcecategory'].lower().replace('/', '|')}",
                    'resource/usage_family': 'metrics',
                    'lineitem/type': "Usage",
                    'lineitem/description': "daily average 1k datapoints ingested by Source Category",
                    'resource/service': "Metrics ingest",
                    'resource/account': SUMO_ORG_ID,
                    'resource/region': SUMO_DEPLOYMENT,
                    'usage/units': "credits",
                    'action/operation': "ingest",
                    'usage/amount': f"{float(record_map['credits'].strip()):6f}",
                    'cost/cost': f"{(float(record_map['credits'].strip()) * COST_PER_CREDIT):6f}",
                })
            except(KeyError, Exception) as e:
                logger.warning(f"Skipping record: {e}")
        return results

    def convert_storage_to_cbf(self, data: List[Dict[str, str]]) -> List[Dict[str, str]]:
        results = []

        logger.debug(f"convert_storage_to_cbf: Processing {len(data)} rows")

        metric_mappings = {
            "Storage Credits": {
                "resource/id": "log storage",
                "resource/usage_family": "logs",
                "lineitem/description": "log storage",
                "resource/service": "Logs storage",
                "action/operation": "ingest",
            },
            "Infrequent Storage Credits": {
                "resource/id": "infrequent log storage",
                "resource/usage_family": "logs",
                "lineitem/description": "infrequent log storage",
                "resource/service": "Logs infrequent storage",
                "action/operation": "ingest",
            },
        }
        for row in data:
            logger.debug(f"Processing row: Date={row.get('Date', 'N/A')}, Storage Credits={row.get('Storage Credits', 'N/A')}, Infrequent Storage Credits={row.get('Infrequent Storage Credits', 'N/A')}")

            for metric, meta in metric_mappings.items():
                amount_str = row.get(metric, '0')

                # Skip if amount is zero, empty, or not a valid number
                try:
                    amount_float = float(amount_str)
                    if amount_float <= 0:
                        logger.debug(f"Skipping {metric} with amount {amount_str} (zero or negative)")
                        continue
                except (ValueError, TypeError):
                    logger.debug(f"Skipping {metric} with invalid amount: {amount_str}")
                    continue

                # Parse date string to datetime - try multiple formats
                dt = None
                date_str = row.get('Date', 'N/A').strip().strip('"')  # Remove quotes and whitespace

                # Try multiple date formats in order of likelihood
                date_formats = ["%Y-%m-%d", "%m/%d/%y", "%m/%d/%Y", "%Y/%m/%d"]

                for fmt in date_formats:
                    try:
                        dt = datetime.strptime(date_str, fmt)
                        break
                    except ValueError:
                        continue

                if dt is None:
                    logger.warning(f"Could not parse date in convert_storage_to_cbf: {date_str}")
                    continue

                iso_date = dt.date().isoformat()

                logger.debug(f"Adding {metric} record: amount={amount_float}, date={iso_date}")

                results.append({
                    "time/usage_start": str(iso_date),
                    "resource/id": meta["resource/id"],
                    "resource/usage_family": meta["resource/usage_family"],
                    "lineitem/type": "Usage",
                    "lineitem/description": meta["lineitem/description"],
                    "resource/service": meta["resource/service"],
                    "resource/account": SUMO_ORG_ID,
                    "resource/region": SUMO_DEPLOYMENT,
                    "usage/units": "credits",
                    "action/operation": meta["action/operation"],
                    "usage/amount": str(amount_float),
                    "cost/cost": f"{amount_float * COST_PER_CREDIT:.6f}",
                })

        logger.debug(f"convert_storage_to_cbf: Returning {len(results)} records")
        return results

    def get_continuous_logs_cbf(self) -> List[Dict[str, str]]:
        results = self.get_billing_data(CONTINUOUS_LOG_INGESTED)
        return self.convert_logs_to_cbf(results)

    def get_continuous_logs_cbf_for_date(self, start_datetime: datetime, end_datetime: datetime) -> List[Dict[str, str]]:
        results = self.get_billing_data(CONTINUOUS_LOG_INGESTED, start_datetime=start_datetime, end_datetime=end_datetime)
        return self.convert_logs_to_cbf(results)

    def get_frequent_logs_cbf(self) -> List[Dict[str, str]]:
        results = self.get_billing_data(FREQUENT_LOG_INGESTED)
        return self.convert_logs_to_cbf(results)

    def get_frequent_logs_cbf_for_date(self, start_datetime: datetime, end_datetime: datetime) -> List[Dict[str, str]]:
        results = self.get_billing_data(FREQUENT_LOG_INGESTED, start_datetime=start_datetime, end_datetime=end_datetime)
        return self.convert_logs_to_cbf(results)

    def get_infrequent_logs_cbf(self) -> List[Dict[str, str]]:
        results = self.get_billing_data(INFREQUENT_LOG_INGESTED)
        return self.convert_logs_to_cbf(results)

    def get_infrequent_logs_cbf_for_date(self, start_datetime: datetime, end_datetime: datetime) -> List[Dict[str, str]]:
        results = self.get_billing_data(INFREQUENT_LOG_INGESTED, start_datetime=start_datetime, end_datetime=end_datetime)
        return self.convert_logs_to_cbf(results)

    def get_infrequent_logs_scanned_cbf(self) -> List[Dict[str, str]]:
        results = self.get_billing_data(INFREQUENT_LOG_SCANNED, use_receipt_time=False)
        return self.convert_logs_scanned_to_cbf(results)

    def get_infrequent_logs_scanned_cbf_for_date(self, start_datetime: datetime, end_datetime: datetime) -> List[Dict[str, str]]:
        results = self.get_billing_data(INFREQUENT_LOG_SCANNED, use_receipt_time=False, start_datetime=start_datetime, end_datetime=end_datetime)
        return self.convert_logs_scanned_to_cbf(results)

    def get_logs_storage_cbf(self) -> List[Dict[str, str]]:
        data = self.get_billing_data_api()

        # Filter to only include the previous day (last 24 hours) in UTC
        previous_day = (datetime.now(timezone.utc) - timedelta(days=1)).date()

        # Filter data to only include records from the previous day
        filtered_data = []
        for row in data:
            try:
                # Parse date - try multiple formats
                row_date = None
                date_str = row.get('Date', 'N/A').strip().strip('"')  # Remove quotes and whitespace

                # Try multiple date formats in order of likelihood
                date_formats = ["%Y-%m-%d", "%m/%d/%y", "%m/%d/%Y", "%Y/%m/%d"]

                for fmt in date_formats:
                    try:
                        row_date = datetime.strptime(date_str, fmt).date()
                        break
                    except ValueError:
                        continue

                if row_date is None:
                    logger.warning(f"Could not parse date: {date_str}")
                    continue

                if row_date == previous_day:
                    filtered_data.append(row)
            except Exception as e:
                logger.warning(f"Error processing row: {e}")
                continue

        return self.convert_storage_to_cbf(filtered_data)

    def get_logs_storage_cbf_for_date(self, start_datetime: datetime, end_datetime: datetime) -> List[Dict[str, str]]:
        data = self.get_billing_data_api()
        logger.debug(f"get_logs_storage_cbf_for_date: Retrieved {len(data)} rows from API")

        # Filter to only include the specified date range
        target_date = start_datetime.date()
        logger.debug(f"get_logs_storage_cbf_for_date: Looking for target date {target_date}")

        # Filter data to only include records from the target date
        filtered_data = []
        sample_count = 0
        for row in data:
            try:
                # Show first few rows for debugging
                if sample_count < 3:
                    logger.debug(f"Sample row {sample_count + 1}: {row}")
                    sample_count += 1

                # Parse date - try multiple formats
                row_date = None
                date_str = row.get('Date', 'N/A').strip().strip('"')  # Remove quotes and whitespace

                # Try multiple date formats in order of likelihood
                date_formats = ["%Y-%m-%d", "%m/%d/%y", "%m/%d/%Y", "%Y/%m/%d"]

                for fmt in date_formats:
                    try:
                        row_date = datetime.strptime(date_str, fmt).date()
                        logger.debug(f"Parsed '{date_str}' as {fmt} -> {row_date}")
                        break
                    except ValueError:
                        continue

                if row_date is None:
                    logger.debug(f"Could not parse date with any format: '{date_str}'")
                    continue

                logger.debug(f"Checking row: date_str='{date_str}' -> row_date={row_date}, target={target_date}")

                if row_date == target_date:
                    logger.debug(f"Found matching row: Date={date_str}, Storage Credits={row.get('Storage Credits')}, Infrequent Storage Credits={row.get('Infrequent Storage Credits')}")
                    filtered_data.append(row)
            except Exception as e:
                logger.warning(f"Error processing row: {e}")
                continue

        logger.debug(f"get_logs_storage_cbf_for_date: Found {len(filtered_data)} rows matching {target_date}")
        return self.convert_storage_to_cbf(filtered_data)

    def get_metrics_cbf(self) -> List[Dict[str, str]]:
        results = self.get_billing_data(METRICS_INGESTED)
        return self.convert_metrics_to_cbf(results)

    def get_metrics_cbf_for_date(self, start_datetime: datetime, end_datetime: datetime) -> List[Dict[str, str]]:
        results = self.get_billing_data(METRICS_INGESTED, start_datetime=start_datetime, end_datetime=end_datetime)
        return self.convert_metrics_to_cbf(results)

    def get_traces_cbf(self) -> List[Dict[str, str]]:
        results = self.get_billing_data(TRACES_INGESTED)
        logger.debug(results)
        return self.convert_traces_to_cbf(results)

    def get_traces_cbf_for_date(self, start_datetime: datetime, end_datetime: datetime) -> List[Dict[str, str]]:
        results = self.get_billing_data(TRACES_INGESTED, start_datetime=start_datetime, end_datetime=end_datetime)
        logger.debug(results)
        return self.convert_traces_to_cbf(results)


# CloudZero API constants
MAX_PAYLOAD_SIZE_BYTES = 10 * 1024 * 1024  # 10MB limit
SAFE_PAYLOAD_SIZE_BYTES = 8 * 1024 * 1024   # 8MB safe limit to account for JSON overhead


def calculate_payload_size(data: List[Dict[str, str]], operation: str) -> int:
    """Calculate the size of the JSON payload in bytes"""
    payload = {
        "operation": operation,
        "data": data,
        "month": data[0]["time/usage_start"] if data else "",
    }
    return len(json.dumps(payload).encode('utf-8'))


def chunk_data_by_size(data: List[Dict[str, str]], operation: str, max_size: int = SAFE_PAYLOAD_SIZE_BYTES) -> List[List[Dict[str, str]]]:
    """Split data into chunks that fit within the size limit"""
    if not data:
        return []

    chunks = []
    current_chunk = []

    for record in data:
        # Create a test chunk with the current record
        test_chunk = current_chunk + [record]
        test_size = calculate_payload_size(test_chunk, operation)

        if test_size > max_size and current_chunk:
            # Current chunk is at capacity, start a new one
            chunks.append(current_chunk)
            current_chunk = [record]
        else:
            # Add record to current chunk
            current_chunk.append(record)

    # Add the final chunk if it has data
    if current_chunk:
        chunks.append(current_chunk)

    return chunks


def estimate_chunk_count(data: List[Dict[str, str]], operation: str) -> tuple[int, int]:
    """Estimate chunk count and total payload size"""
    if not data:
        return 0, 0

    total_size = calculate_payload_size(data, operation)
    estimated_chunks = max(1, (total_size + SAFE_PAYLOAD_SIZE_BYTES - 1) // SAFE_PAYLOAD_SIZE_BYTES)  # Ceiling division

    return estimated_chunks, total_size


def ensure_dry_run_folder() -> str:
    """Create dry_run folder if it doesn't exist and return the path"""
    dry_run_path = os.path.join(os.getcwd(), "dry_run")
    if not os.path.exists(dry_run_path):
        os.makedirs(dry_run_path)
        logger.info(f"📁 Created dry_run folder: {dry_run_path}")
    return dry_run_path


def write_dry_run_data(data: List[Dict[str, str]], operation: str, service_name: str, date: str) -> str:
    """Write CBF data to JSON file in dry_run folder"""
    dry_run_path = ensure_dry_run_folder()

    # Sanitize service name for filename (replace spaces/special chars with underscores)
    safe_service_name = service_name.replace(' ', '_').replace('/', '_')
    filename = f"{date}_{safe_service_name}.json"
    filepath = os.path.join(dry_run_path, filename)

    # Prepare the data structure that would be sent to CloudZero
    payload = {
        "operation": operation,
        "data": data,
        "month": data[0]["time/usage_start"] if data else "",
        "metadata": {
            "service": service_name,
            "date": date,
            "record_count": len(data),
            "generated_at": datetime.now(timezone.utc).isoformat()
        }
    }

    with open(filepath, 'w') as f:
        json.dump(payload, f, indent=2)

    logger.debug(f"📝 Wrote {len(data)} records to {filepath}")
    return filepath


class CZAnycostOp(Enum):
    REPLACE_HOURLY = 1
    REPLACE_DROP = 2
    SUM = 3


class CloudZero:

    def __init__(self, auth_key: str, endpoint: str, stream_id: str) -> None:
        self.session = requests.Session()
        self.session.headers = {'content-type': 'application/json',
                                'accept': 'application/json',
                                "Authorization": auth_key}
        self.endpoint = endpoint
        self.stream_id = stream_id

    def get(self, method: str, params: Optional[Dict[str, Any]] = None) -> requests.Response:
        r = self.session.get(self.endpoint + method, params=params)
        if 400 <= r.status_code < 600:
            r.reason = r.text
        r.raise_for_status()
        return r

    @backoff
    def post(self, method: str, data: Dict[str, Any], headers: Optional[Dict[str, str]] = None) -> requests.Response:
        r = self.session.post(self.endpoint + method, data=json.dumps(data), headers=headers)
        if 400 <= r.status_code < 600:
            r.reason = r.text
        r.raise_for_status()
        return r

    def post_anycost_stream(self, data: List[Dict[str, str]], operation: CZAnycostOp,
                           service_name: str = None, date: str = None) -> Union[Dict[str, Any], str]:
        if len(data) == 0:
            return 'No anycost data to post'

        # Convert operation enum to string
        match operation:
            case CZAnycostOp.REPLACE_HOURLY:
                opstring = "replace_hourly"
            case CZAnycostOp.REPLACE_DROP:
                opstring = "replace_drop"
            case CZAnycostOp.SUM:
                opstring = "sum"
            case _:
                raise Exception("No CZ operation specified.")

        # Determine chunking strategy first (common to both dry-run and normal mode)
        estimated_chunks, total_size = estimate_chunk_count(data, opstring)
        size_mb = total_size / 1024 / 1024
        needs_chunking = total_size > SAFE_PAYLOAD_SIZE_BYTES

        # Create chunks if needed
        if needs_chunking:
            chunks = chunk_data_by_size(data, opstring)
            num_chunks = len(chunks)
        else:
            chunks = [data]  # Single chunk
            num_chunks = 1

        # Now handle dry-run vs normal mode
        if DRY_RUN_MODE:
            return self._handle_dry_run_mode(chunks, opstring, service_name, date, total_size, size_mb, num_chunks)
        else:
            return self._handle_normal_mode(chunks, opstring, total_size, size_mb, num_chunks)

    def _handle_dry_run_mode(self, chunks: List[List[Dict[str, str]]], opstring: str,
                            service_name: str, date: str, total_size: int, size_mb: float, num_chunks: int) -> Dict[str, Any]:
        """Handle dry-run mode processing"""
        total_records = sum(len(chunk) for chunk in chunks)

        if service_name and date:
            if num_chunks == 1:
                # Single file
                filepath = write_dry_run_data(chunks[0], opstring, service_name, date)
                logger.info(f"🔍 DRY RUN: Would upload {total_records} records, {size_mb:.2f}MB (single request)")
                logger.info(f"📝 Data saved to: {filepath}")
            else:
                # Multiple chunks - write each chunk as a separate file
                logger.info(f"🔍 DRY RUN: Would upload {total_records} records split into {num_chunks} chunks, total {size_mb:.2f}MB")

                filepaths = []
                for chunk_num, chunk in enumerate(chunks, 1):
                    chunk_service_name = f"{service_name}_chunk{chunk_num}"
                    chunk_filepath = write_dry_run_data(chunk, opstring, chunk_service_name, date)
                    filepaths.append(chunk_filepath)
                    chunk_size = calculate_payload_size(chunk, opstring)
                    logger.debug(f"  🔍 Chunk {chunk_num}: {len(chunk)} records, {chunk_size / 1024 / 1024:.2f}MB ({opstring.upper()})")

                logger.info(f"📝 Data saved to {len(filepaths)} files: {filepaths[0]} ... {filepaths[-1]}")
        else:
            # Fallback logging when service_name/date not provided
            if num_chunks == 1:
                logger.info(f"🔍 DRY RUN: Would upload {total_records} records, {size_mb:.2f}MB (single request)")
            else:
                logger.info(f"🔍 DRY RUN: Would upload {total_records} records split into {num_chunks} chunks, total {size_mb:.2f}MB")

        # Return dry-run result
        return {
            "dry_run": True,
            "operation": opstring,
            "records": total_records,
            "size_mb": round(size_mb, 2),
            "chunks": num_chunks
        }

    def _handle_normal_mode(self, chunks: List[List[Dict[str, str]]], opstring: str,
                           total_size: int, size_mb: float, num_chunks: int) -> Union[Dict[str, Any], str]:
        """Handle normal mode processing (actual uploads)"""
        total_records = sum(len(chunk) for chunk in chunks)

        if num_chunks == 1:
            # Single upload - data fits within limit
            logger.debug(f"Single upload: {total_records} records, {size_mb:.2f}MB")
            return self._upload_chunk(chunks[0], opstring, 1, 1)
        else:
            # Chunked upload - data exceeds safe limit
            logger.info(f"Chunked upload: {total_records} records split into {num_chunks} chunks, total {size_mb:.2f}MB")

            # Log chunking strategy information
            if opstring == "replace_drop":
                logger.info(f"  📤 REPLACE_DROP service with {num_chunks} chunks: ALL chunks will use REPLACE_DROP")
            else:
                logger.debug(f"  📤 {opstring.upper()} service with {num_chunks} chunks: ALL chunks will use {opstring.upper()}")

            results = []
            successful_chunks = 0

            for chunk_num, chunk in enumerate(chunks, 1):
                chunk_size = calculate_payload_size(chunk, opstring)
                logger.debug(f"  Uploading chunk {chunk_num}/{num_chunks}: {len(chunk)} records, {chunk_size / 1024 / 1024:.2f}MB ({opstring.upper()})")

                try:
                    result = self._upload_chunk(chunk, opstring, chunk_num, num_chunks)
                    results.append(result)
                    successful_chunks += 1
                    logger.debug(f"    ✓ Chunk {chunk_num} uploaded successfully")

                    # Small delay between chunks to avoid overwhelming the API
                    if chunk_num < num_chunks:
                        time.sleep(0.5)

                except Exception as e:
                    logger.error(f"    ✗ Chunk {chunk_num} failed: {e}")
                    results.append(f"Chunk {chunk_num} failed: {str(e)}")
                    # Continue with remaining chunks even if one fails

            # Return summary of chunked upload
            return {
                "chunked_upload": True,
                "total_chunks": num_chunks,
                "successful_chunks": successful_chunks,
                "failed_chunks": num_chunks - successful_chunks,
                "total_records": total_records,
                "results": results
            }

    def _upload_chunk(self, chunk_data: List[Dict[str, str]], operation: str, chunk_num: int, total_chunks: int) -> Union[Dict[str, Any], str]:
        """Upload a single chunk of data to CloudZero"""
        try:
            payload = {
                "operation": operation,
                "data": chunk_data,
                "month": chunk_data[0]["time/usage_start"],
            }
            r = self.post(f'/v2/connections/billing/anycost/{self.stream_id}/billing_drops', data=payload)
            return json.loads(r.text)
        except Exception as e:
            logger.warning(f'Failed to post anycost stream chunk {chunk_num}/{total_chunks}: {str(e)}')
            raise


def process_day_data(sumo: SumoLogic, cz: CloudZero, day_start: datetime, day_end: datetime,
                    current_date: str, first_service_processed: bool) -> tuple[dict, bool]:
    """Process all service types for a single day. Returns (day_stats, first_service_processed)"""

    # Initialize day statistics
    day_stats = {
        'records': {},
        'successful_services': 0,
        'failed_services': 0,
        'successful_services_list': [],
        'failed_services_list': []
    }

    # Define service types with their data getters
    # Use date-specific methods if dates are provided, otherwise use current methods
    if day_start and day_end:
        service_types = [
            ("continuous logs", lambda: sumo.get_continuous_logs_cbf_for_date(day_start, day_end)),
            ("frequent logs", lambda: sumo.get_frequent_logs_cbf_for_date(day_start, day_end)),
            ("infrequent logs", lambda: sumo.get_infrequent_logs_cbf_for_date(day_start, day_end)),
            ("infrequent log scans", lambda: sumo.get_infrequent_logs_scanned_cbf_for_date(day_start, day_end)),
            ("metrics", lambda: sumo.get_metrics_cbf_for_date(day_start, day_end)),
            ("traces", lambda: sumo.get_traces_cbf_for_date(day_start, day_end)),
            ("storage", lambda: sumo.get_logs_storage_cbf_for_date(day_start, day_end))
        ]
    else:
        # Standard mode - use current methods (previous 24 hours)
        service_types = [
            ("continuous logs", lambda: sumo.get_continuous_logs_cbf()),
            ("frequent logs", lambda: sumo.get_frequent_logs_cbf()),
            ("infrequent logs", lambda: sumo.get_infrequent_logs_cbf()),
            ("infrequent log scans", lambda: sumo.get_infrequent_logs_scanned_cbf()),
            ("metrics", lambda: sumo.get_metrics_cbf()),
            ("traces", lambda: sumo.get_traces_cbf()),
            ("storage", lambda: sumo.get_logs_storage_cbf())
        ]

    # Process each service type
    for service_name, data_getter in service_types:
        logger.info(f"{'  ' if BACKFILL_MODE else ''}Processing {service_name} for {current_date}")
        try:
            data = data_getter()
            if data:
                # Determine the correct operation based on service type:
                # - First service (continuous logs): REPLACE_DROP for all chunks
                # - All other services: SUM for all chunks
                is_first_service = service_name == "continuous logs"

                if is_first_service:
                    operation = CZAnycostOp.REPLACE_DROP
                    logger.debug(f"{'    ' if BACKFILL_MODE else '  '}Using REPLACE_DROP operation (first service: {service_name})")
                else:
                    operation = CZAnycostOp.SUM
                    logger.debug(f"{'    ' if BACKFILL_MODE else '  '}Using SUM operation (subsequent service: {service_name})")

                # Enhanced progress tracking for large datasets
                estimated_chunks, total_size = estimate_chunk_count(data, "sum")  # Use "sum" as default for estimation
                size_mb = total_size / 1024 / 1024

                if size_mb > 1:  # Log size for datasets > 1MB
                    logger.info(f"{'    ' if BACKFILL_MODE else '  '}Dataset size: {len(data)} records, {size_mb:.2f}MB")
                    if estimated_chunks > 1:
                        logger.info(f"{'    ' if BACKFILL_MODE else '  '}Expected to chunk into ~{estimated_chunks} uploads")

                results = cz.post_anycost_stream(data, operation, service_name, current_date)

                # Enhanced result logging
                if isinstance(results, dict) and results.get('dry_run'):
                    # Dry-run mode logging
                    chunks = results.get('chunks', 1)
                    size_mb = results.get('size_mb', 0)
                    if chunks > 1:
                        logger.info(f"{'    ' if BACKFILL_MODE else '  '}🔍 {service_name}: Would upload {len(data)} records in {chunks} chunks ({size_mb}MB)")
                    else:
                        logger.info(f"{'    ' if BACKFILL_MODE else '  '}🔍 {service_name}: Would upload {len(data)} records ({size_mb}MB)")

                elif isinstance(results, dict) and results.get('chunked_upload'):
                    successful_chunks = results.get('successful_chunks', 0)
                    total_chunks = results.get('total_chunks', 0)
                    failed_chunks = results.get('failed_chunks', 0)

                    if failed_chunks > 0:
                        logger.warning(f"{'    ' if BACKFILL_MODE else '  '}⚠ {service_name}: {successful_chunks}/{total_chunks} chunks successful, {failed_chunks} failed")
                    else:
                        logger.info(f"{'    ' if BACKFILL_MODE else '  '}✓ {service_name}: {len(data)} records uploaded in {total_chunks} chunks")
                else:
                    action = "Would upload" if DRY_RUN_MODE else "uploaded"
                    logger.info(f"{'    ' if BACKFILL_MODE else '  '}{'🔍' if DRY_RUN_MODE else '✓'} {service_name}: {len(data)} records {action}")

                logger.debug(f"{'    ' if BACKFILL_MODE else '  '}CloudZero response: {results}")

                # Mark first service as processed after successful upload
                if is_first_service and not (isinstance(results, dict) and results.get('dry_run')):
                    first_service_processed = True
                    logger.debug(f"{'    ' if BACKFILL_MODE else '  '}First service ({service_name}) processing completed for {current_date}")

                # Track successful service
                day_stats['records'][service_name] = len(data)
                day_stats['successful_services'] += 1
                day_stats['successful_services_list'].append(service_name)

            else:
                logger.info(f"{'    ' if BACKFILL_MODE else '  '}- {service_name}: No data for {current_date}")
                # Track service with no data as successful (not failed)
                day_stats['records'][service_name] = 0
                day_stats['successful_services'] += 1
                day_stats['successful_services_list'].append(service_name)

        except Exception as e:
            logger.error(f"{'    ' if BACKFILL_MODE else '  '}✗ {service_name} failed for {current_date}: {e}")
            # Log additional context for debugging
            logger.debug(f"{'    ' if BACKFILL_MODE else '  '}Error details: {type(e).__name__}: {str(e)}")

            # Track failed service
            day_stats['failed_services'] += 1
            day_stats['failed_services_list'].append(service_name)
            # Continue with other services even if one fails

    return day_stats, first_service_processed


def lambda_handler(event: Dict[str, Any], context: Dict[str, Any]) -> None:
    logger.info("=== Starting Lambda Handler ===")

    # Load environment variables and build queries if not already loaded (for AWS Lambda execution)
    if SUMO_ACCESS_KEY is None:
        load_environment_variables()
        build_queries()

    # Validate backfill configuration if enabled
    backfill_start, backfill_end = validate_backfill_config()
    sumo = SumoLogic(SUMO_ACCESS_KEY, SUMO_SECRET_KEY, SUMO_DEPLOYMENT)
    cz = CloudZero(CZ_AUTH_KEY, CZ_URL, CZ_ANYCOST_STREAM_CONNECTION_ID)

    if BACKFILL_MODE:
        logger.info("Running in BACKFILL MODE")

        # Initialize state management for automatic resume functionality
        state = BackfillState(backfill_start, backfill_end)

        # Handle automatic resume - load existing state
        if AUTO_RESUME:
            if state.load_existing_state():
                # Check if backfill is already complete
                if state.is_backfill_complete():
                    logger.info("🎉 Backfill already completed!")
                    state.cleanup_state_file()
                    return

                # Get automatic resume date
                auto_resume_date = state.get_resume_date()
                if auto_resume_date:
                    logger.info(f"🔄 AUTO RESUME: Continuing from {auto_resume_date}")
                    os.environ['RESUME_DATE'] = auto_resume_date
                    # Update global variable
                    global RESUME_DATE
                    RESUME_DATE = auto_resume_date
                else:
                    logger.info("🎉 Auto resume: Backfill is complete!")
                    state.cleanup_state_file()
                    return
            else:
                logger.info("📋 No existing state file found, starting fresh backfill")
                state.start_time = datetime.now()

        elif not state.start_time:
            # Fresh start for non-auto-resume backfills
            state.start_time = datetime.now()

        # Generate daily date ranges for the backfill period
        date_ranges = generate_date_range(backfill_start, backfill_end)
        total_days = len(date_ranges)

        # Show progress if we have existing state
        if state.completed_days:
            progress_summary = state.get_progress_summary()
            logger.info(f"🚀 Resuming backfill: {progress_summary['remaining_days']} remaining of {total_days} days "
                       f"({progress_summary['completion_percentage']:.1f}% complete)")
            logger.info(f"   From {backfill_start.date()} to {backfill_end.date()}")
        else:
            logger.info(f"🚀 Starting backfill: {total_days} days from {backfill_start.date()} to {backfill_end.date()}")

        # Initialize progress tracking
        progress = BackfillProgress(total_days)

        # Handle resume mode - skip days before resume date (both manual and automatic)
        if RESUME_MODE and RESUME_DATE:
            resume_dt = validate_date_string(RESUME_DATE, "RESUME_DATE")
            resume_date = resume_dt.date()

            if AUTO_RESUME:
                logger.info(f"🔄 AUTO RESUME: Processing from {resume_date} onwards")
            else:
                logger.info(f"🔄 MANUAL RESUME: Skipping days before {resume_date}")

            # Filter date ranges to start from resume date
            original_count = len(date_ranges)
            date_ranges = [(start, end) for start, end in date_ranges if start.date() >= resume_date]
            skipped_count = original_count - len(date_ranges)

            if skipped_count > 0:
                logger.info(f"⏭️  Skipped {skipped_count} days, processing {len(date_ranges)} remaining days")
            else:
                logger.warning(f"⚠️  Resume date {resume_date} is not within the backfill range - processing all days")

        # Process each day individually to stay within CloudZero API limits
        for day_num, (day_start, day_end) in enumerate(date_ranges, 1):
            current_date = day_start.date()
            # Adjust day numbering to account for total days (including skipped ones)
            adjusted_day_num = day_num + (total_days - len(date_ranges)) if RESUME_MODE else day_num
            progress.current_day = adjusted_day_num - 1  # Will be incremented in start_day
            progress.start_day(str(current_date))

            # Track whether first service has been processed for this day
            first_service_processed = False

            try:
                # Process all services for this day using unified logic
                day_stats, first_service_processed = process_day_data(
                    sumo, cz, day_start, day_end, str(current_date), first_service_processed
                )

            except Exception as e:
                logger.error(f"Failed processing day {current_date}: {e}")
                # Track the entire day as failed
                day_stats = {
                    'records': {},
                    'successful_services': 0,
                    'failed_services': len(progress.service_types),
                    'successful_services_list': [],
                    'failed_services_list': progress.service_types.copy()
                }
                # Continue with next day even if current day fails

            # Complete the day and update progress
            progress.complete_day(day_stats)

            # Update state tracking for automatic resume
            state.mark_day_completed(str(current_date), day_stats)

        # Generate and display final summary
        summary = progress.get_summary()
        logger.info("=" * 60)
        logger.info("📊 BACKFILL SUMMARY")
        logger.info("=" * 60)
        logger.info(f"📅 Period: {backfill_start.date()} to {backfill_end.date()}")
        logger.info(f"⏱️  Total Duration: {progress._format_duration(summary['total_duration'])}")
        logger.info(f"📁 Days Processed: {summary['total_days_processed']}/{total_days}")
        logger.info(f"📊 Total Records: {summary['total_records']:,}")
        logger.info(f"✅ Successful Services: {summary['total_successful_services']}")
        logger.info(f"❌ Failed Services: {summary['total_failed_services']}")

        # Service breakdown
        logger.info("\n📋 Service Breakdown:")
        for service, stats in summary['service_breakdown'].items():
            if stats['records'] > 0 or stats['successes'] > 0 or stats['failures'] > 0:
                logger.info(f"  {service}: {stats['records']:,} records, "
                           f"{stats['successes']} successes, {stats['failures']} failures")

        # Check if backfill is complete and handle state cleanup
        if state.is_backfill_complete():
            logger.info("🎉 Backfill fully completed! All days processed successfully.")
            state.cleanup_state_file()
        else:
            # Show resume instructions if there were failures
            if summary['total_failed_services'] > 0:
                failed_days = [day_stat['date'] for day_stat in summary['daily_stats']
                              if day_stat.get('failed_services', 0) > 0]
                if failed_days:
                    logger.warning("⚠️  Some services failed. To retry, use: --resume")
                    logger.warning("    The script will automatically resume from the last successful day")

        logger.info("=" * 60)
        completion_msg = f"🎉 {'DRY RUN' if DRY_RUN_MODE else 'Backfill'} completed for {len(date_ranges)} days!"
        if RESUME_MODE and len(date_ranges) < total_days:
            if AUTO_RESUME:
                completion_msg += f" (Auto-resumed, processed {len(date_ranges)} remaining days)"
            else:
                completion_msg += f" (Resumed from {RESUME_DATE}, skipped {total_days - len(date_ranges)} days)"
        logger.info(completion_msg)
    else:
        # Standard mode - unified behavior using same logic as backfill
        logger.info("Running in STANDARD MODE")

        # Track first service processing in standard mode
        first_service_processed = False

        # Generate current date for display (previous 24 hours)
        current_date = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")

        # Process all services for the previous day using unified logic
        # Pass None for day_start/day_end to trigger standard mode methods
        day_stats, first_service_processed = process_day_data(
            sumo, cz, None, None, current_date, first_service_processed
        )

        # Log summary for standard mode
        total_records = sum(day_stats['records'].values())
        if total_records > 0:
            logger.info(f"✅ Standard mode completed: {total_records:,} records processed")
            if day_stats['successful_services'] > 0:
                logger.info(f"  Services processed: {', '.join(day_stats['successful_services_list'])}")
            if day_stats['failed_services'] > 0:
                logger.warning(f"  Failed services: {', '.join(day_stats['failed_services_list'])}")
        else:
            logger.info("✅ Standard mode completed: No data to process")

    logger.info("=== Lambda Handler Completed ===")


def main() -> None:
    # Parse command line arguments first (before environment variables are processed)
    parse_command_line_args()

    # Re-initialize logging in case verbose flag was set
    global logger
    LOGGING_LEVEL_STRING = os.environ.get('LOGGING_LEVEL', "INFO")
    match LOGGING_LEVEL_STRING:
        case "INFO":
            LOGGING_LEVEL = logging.INFO
        case "DEBUG":
            LOGGING_LEVEL = logging.DEBUG
        case _:
            LOGGING_LEVEL = logging.INFO

    if len(logging.getLogger().handlers) > 0:
        logging.getLogger().setLevel(LOGGING_LEVEL)
    else:
        logging.basicConfig(level=LOGGING_LEVEL)

    # Load environment variables and build queries
    load_environment_variables()
    build_queries()

    lambda_handler({}, {})


if __name__ == "__main__":
    main()
