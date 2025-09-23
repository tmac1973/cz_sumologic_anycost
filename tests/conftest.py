"""
Shared test configuration and fixtures for the SumoLogic to CloudZero adapter tests.
"""

import pytest
import os
import json
import tempfile
import shutil
from datetime import datetime, timezone, timedelta
from unittest.mock import Mock, patch
from typing import Dict, List, Any


@pytest.fixture
def test_env_vars():
    """Set up test environment variables"""
    return {
        'SUMO_ACCESS_KEY': 'test_access_key',
        'SUMO_SECRET_KEY': 'test_secret_key',
        'SUMO_DEPLOYMENT': 'us1',
        'SUMO_ORG_ID': 'test_org_id',
        'CZ_AUTH_KEY': 'test_cz_auth',
        'CZ_ANYCOST_STREAM_CONNECTION_ID': 'test_connection_id',
        'LOG_CONTINUOUS_CREDIT_RATE': '20',
        'LOG_FREQUENT_CREDIT_RATE': '9',
        'LOG_INFREQUENT_CREDIT_RATE': '0.4',
        'LOG_INFREQUENT_SCAN_CREDIT_RATE': '0.016',
        'METRICS_CREDIT_RATE': '3',
        'TRACING_CREDIT_RATE': '14',
        'COST_PER_CREDIT': '0.15',
        'QUERY_TIME_HOURS': '24',
        'CZ_URL': 'https://api.cloudzero.com',
        'LOGGING_LEVEL': 'INFO'
    }


@pytest.fixture(autouse=True)
def setup_test_env(test_env_vars):
    """Automatically set up test environment for all tests"""
    original_env = {}

    # Save original values
    for key in test_env_vars:
        original_env[key] = os.environ.get(key)

    # Set test values
    for key, value in test_env_vars.items():
        os.environ[key] = value

    # Load environment and build queries
    from sumo_anycost_lambda import load_environment_variables, build_queries
    load_environment_variables()
    build_queries()

    yield

    # Restore original values
    for key, value in original_env.items():
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = value


@pytest.fixture
def temp_dir():
    """Create a temporary directory for test files"""
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    shutil.rmtree(temp_dir)


@pytest.fixture
def sample_date():
    """Standard test date"""
    return datetime(2025, 9, 22, 12, 0, 0, tzinfo=timezone.utc)


# Raw SumoLogic query results (actual structure from search_job_records)
@pytest.fixture
def sample_sumologic_raw_response():
    return [
        {
            'map': {
                '_timeslice': '1758549600000',  # Timestamp in milliseconds
                'gbytes': '0.0039791446179151535',  # Actual GB calculation
                'credits': '0.07958289235830307',   # Actual credit calculation
                'sourcecategory': 'platform/app-state-machine/cost-adaptor-azure-ingest-cost-reports',
                'datatier': 'Continuous'
            }
        },
        {
            'map': {
                '_timeslice': '1758542400000',
                'gbytes': '0.004857266321778297',
                'credits': '0.09714532643556595',
                'sourcecategory': 'platform/app-state-machine/cloud-connector-unload-stm',
                'datatier': 'Continuous'
            }
        }
    ]


# CBF format output (actual production format)
@pytest.fixture
def sample_continuous_logs_cbf():
    return [
        {
            'time/usage_start': '2025-09-22T09:00:00+00:00',
            'resource/id': 'sourcecategory/platform|app-state-machine|cost-adaptor-azure-ingest-cost-reports',
            'resource/usage_family': 'Continuous',
            'lineitem/type': 'Usage',
            'lineitem/description': 'Continuous logs ingested by Source Category',
            'resource/service': 'Logs continuous ingest',
            'resource/account': 'test_org_id',
            'resource/region': 'us1',
            'usage/units': 'credits',
            'action/operation': 'ingest',
            'usage/amount': '0.079583',
            'cost/cost': '0.011937'
        }
    ]


@pytest.fixture
def sample_infrequent_logs_cbf():
    return [
        {
            'time/usage_start': '2025-09-22T11:00:00+00:00',
            'resource/id': 'sourcecategory/platform|app|connections-anycost',
            'resource/usage_family': 'Infrequent',
            'lineitem/type': 'Usage',
            'lineitem/description': 'Infrequent logs ingested by Source Category',
            'resource/service': 'Logs infrequent ingest',
            'resource/account': 'test_org_id',
            'resource/region': 'us1',
            'usage/units': 'credits',
            'action/operation': 'ingest',
            'usage/amount': '0.000957',
            'cost/cost': '0.000144'
        }
    ]


@pytest.fixture
def sample_infrequent_scan_cbf():
    return [
        {
            'time/usage_start': '2025-09-22T11:00:00+00:00',
            'resource/id': 'username/test_user',
            'resource/usage_family': 'infrequent',
            'lineitem/type': 'Usage',
            'lineitem/description': 'Infrequent logs scanned by user',
            'resource/service': 'Logs infrequent scan',
            'resource/account': 'test_org_id',
            'resource/region': 'us1',
            'usage/units': 'credits',
            'action/operation': 'scan',
            'usage/amount': '0.005000',
            'cost/cost': '0.000750'
        }
    ]


@pytest.fixture
def sample_metrics_cbf():
    return [
        {
            'time/usage_start': '2025-09-21T19:00:00+00:00',
            'resource/id': 'sourcecategory/aws|kinesis_firehose_all_metrics|prod',
            'resource/usage_family': 'metrics',
            'lineitem/type': 'Usage',
            'lineitem/description': 'daily average 1k datapoints ingested by Source Category',
            'resource/service': 'Metrics ingest',
            'resource/account': 'test_org_id',
            'resource/region': 'us1',
            'usage/units': 'credits',
            'action/operation': 'ingest',
            'usage/amount': '112.070312',
            'cost/cost': '16.810547'
        }
    ]


@pytest.fixture
def sample_traces_cbf():
    return [
        {
            'time/usage_start': '2025-09-22T12:00:00+00:00',
            'resource/id': 'sourcecategory/platform|app|otlp',
            'resource/usage_family': 'traces',
            'lineitem/type': 'Usage',
            'lineitem/description': 'tracing spans ingested by Source Category',
            'resource/service': 'Traces ingest',
            'resource/account': 'test_org_id',
            'resource/region': 'us1',
            'usage/units': 'credits',
            'action/operation': 'ingest',
            'usage/amount': '0.659678',
            'cost/cost': '0.098952'
        }
    ]


@pytest.fixture
def sample_storage_cbf():
    return [
        {
            'time/usage_start': '2025-09-20',
            'resource/id': 'log storage',
            'resource/usage_family': 'logs',
            'lineitem/type': 'Usage',
            'lineitem/description': 'log storage',
            'resource/service': 'Logs storage',
            'resource/account': 'test_org_id',
            'resource/region': 'us1',
            'usage/units': 'credits',
            'action/operation': 'ingest',
            'usage/amount': '130.56',
            'cost/cost': '19.584000'
        },
        {
            'time/usage_start': '2025-09-20',
            'resource/id': 'infrequent log storage',
            'resource/usage_family': 'logs',
            'lineitem/type': 'Usage',
            'lineitem/description': 'infrequent log storage',
            'resource/service': 'Logs infrequent storage',
            'resource/account': 'test_org_id',
            'resource/region': 'us1',
            'usage/units': 'credits',
            'action/operation': 'ingest',
            'usage/amount': '97.08',
            'cost/cost': '14.562000'
        }
    ]


# SumoLogic usage report CSV data
@pytest.fixture
def sample_usage_report_data():
    return [
        {
            'Date': '2025-09-20',
            'Total Credits': '833.76',
            'Continuous Ingest (GB)': '27.11',
            'Continuous Ingest Credits': '542.21',
            'Storage (GB)': '13130.55',
            'Storage Credits': '87.97',
            'Frequent Ingest (GB)': '0.0',
            'Frequent Ingest Credits': '0.0',
            'Infrequent Ingest (GB)': '63.9',
            'Infrequent Ingest Credits': '25.56',
            'Infrequent Storage (GB)': '5890.76',
            'Infrequent Storage Credits': '8.84',
            'Metrics Ingest (DPM)': '51816.64',
            'Metrics Ingest Credits': '155.45',
            'Tracing Ingest (GB)': '0.98',
            'Tracing Ingest Credits': '13.73'
        }
    ]


# Mock API responses
@pytest.fixture
def sumologic_search_job_response():
    return {
        'id': '12345678',
        'state': 'DONE',
        'recordCount': 100,
        'messageCount': 0
    }


@pytest.fixture
def sumologic_search_job_pending():
    return {
        'id': '12345678',
        'state': 'RUNNING',
        'recordCount': 0,
        'messageCount': 0
    }


@pytest.fixture
def cloudzero_success_response():
    return {
        'success': True,
        'message': 'Data uploaded successfully'
    }


@pytest.fixture
def mock_sumo_api():
    """Mock SumoLogic API calls"""
    with patch('sumo_anycost_lambda.SumoLogic') as mock_class:
        mock_instance = Mock()
        mock_class.return_value = mock_instance
        yield mock_instance


@pytest.fixture
def mock_cloudzero_api():
    """Mock CloudZero API calls"""
    with patch('sumo_anycost_lambda.CloudZero') as mock_class:
        mock_instance = Mock()
        mock_class.return_value = mock_instance
        yield mock_instance


@pytest.fixture
def mock_requests():
    """Mock requests library"""
    with patch('sumo_anycost_lambda.requests') as mock_requests:
        yield mock_requests