"""
Unit tests for the SumoLogic class.
"""

import pytest
import json
import requests
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timezone, timedelta

from sumo_anycost_lambda import SumoLogic
from tests.fixtures.test_data import SUMOLOGIC_AUTH_ERROR, SUMOLOGIC_RATE_LIMIT_ERROR


class TestSumoLogicInit:
    """Test SumoLogic class initialization"""

    def test_init_with_valid_credentials(self):
        """Test successful initialization with valid credentials"""
        sumo = SumoLogic('test_access_id', 'test_access_key', 'us1')

        assert sumo.deployment == 'us1'
        assert sumo.endpoint == 'https://api.sumologic.com/api/'
        assert sumo.session is not None
        assert sumo.session.auth == ('test_access_id', 'test_access_key')

    def test_endpoint_lookup(self):
        """Test endpoint lookup for different deployments"""
        sumo = SumoLogic('test', 'test', 'us1')
        assert sumo.endpoint_lookup('us1') == 'https://api.sumologic.com/api/'

        sumo = SumoLogic('test', 'test', 'us2')
        assert sumo.endpoint_lookup('us2') == 'https://api.us2.sumologic.com/api/'

        sumo = SumoLogic('test', 'test', 'eu')
        assert sumo.endpoint_lookup('eu') == 'https://api.eu.sumologic.com/api/'


class TestSumoLogicAPIRequests:
    """Test SumoLogic API request methods"""

    @pytest.fixture
    def sumo_client(self):
        return SumoLogic('test_access_id', 'test_access_key', 'us1')

    @patch('sumo_anycost_lambda.requests.Session')
    def test_get_request_success(self, mock_session_class, sumo_client):
        """Test successful GET request"""
        mock_session = Mock()
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = '{"result": "success"}'
        mock_session.get.return_value = mock_response
        mock_session_class.return_value = mock_session

        # Replace the session
        sumo_client.session = mock_session

        result = sumo_client.get('/test/endpoint')

        mock_session.get.assert_called_once()
        assert result == mock_response

    @patch('sumo_anycost_lambda.requests.Session')
    def test_post_request_success(self, mock_session_class, sumo_client):
        """Test successful POST request"""
        mock_session = Mock()
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = '{"result": "success"}'
        mock_session.post.return_value = mock_response
        mock_session_class.return_value = mock_session

        sumo_client.session = mock_session

        result = sumo_client.post('/test/endpoint', {'test': 'data'})

        mock_session.post.assert_called_once()
        assert result == mock_response


class TestSumoLogicQueryExecution:
    """Test SumoLogic query execution methods"""

    @pytest.fixture
    def sumo_client(self):
        return SumoLogic('test_access_id', 'test_access_key', 'us1')

    def test_search_job_creation(self, sumo_client, mock_requests):
        """Test search job creation"""
        mock_response = Mock()
        mock_response.text = '{"id": "12345", "state": "RUNNING"}'
        mock_response.status_code = 200

        with patch.object(sumo_client, 'post', return_value=mock_response):
            result = sumo_client.search_job('test query')

            assert result == {"id": "12345", "state": "RUNNING"}

    def test_search_job_status_done(self, sumo_client, sumologic_search_job_response):
        """Test search job status checking when done"""
        mock_response = Mock()
        mock_response.text = json.dumps(sumologic_search_job_response)
        mock_response.status_code = 200

        with patch.object(sumo_client, 'get', return_value=mock_response):
            result = sumo_client.search_job_status({'id': '12345'})

            assert result['state'] == 'DONE'
            assert result['recordCount'] == 100

    def test_search_job_records_retrieval(self, sumo_client, sample_sumologic_raw_response):
        """Test retrieving search job records"""
        mock_response = Mock()
        mock_response.text = json.dumps({
            'records': sample_sumologic_raw_response
        })
        mock_response.status_code = 200

        with patch.object(sumo_client, 'get', return_value=mock_response):
            result = sumo_client.search_job_records({'id': '12345'})

            assert result['records'] == sample_sumologic_raw_response

    def test_search_job_records_sync_success(self, sumo_client, sample_sumologic_raw_response, sumologic_search_job_response):
        """Test synchronous search job execution - success case"""
        # Mock the job creation
        job_response = {"id": "12345", "state": "RUNNING"}

        # Mock the status checks (first running, then done)
        status_running = {"id": "12345", "state": "RUNNING", "recordCount": 0}
        status_done = sumologic_search_job_response

        # Mock the records retrieval
        records_response = {"records": sample_sumologic_raw_response}

        with patch.object(sumo_client, 'search_job', return_value=job_response), \
             patch.object(sumo_client, 'search_job_status', side_effect=[status_running, status_done]), \
             patch.object(sumo_client, 'search_job_records', return_value=records_response), \
             patch('time.sleep'):  # Mock sleep to speed up tests

            result = sumo_client.search_job_records_sync('test query')

            assert result == sample_sumologic_raw_response

    @patch('time.sleep')
    def test_search_job_records_sync_timeout(self, mock_sleep, sumo_client):
        """Test synchronous search job execution - timeout case"""
        # Mock job that never completes
        job_response = {"id": "12345", "state": "RUNNING"}
        status_running = {"id": "12345", "state": "RUNNING", "recordCount": 0}

        with patch.object(sumo_client, 'search_job', return_value=job_response), \
             patch.object(sumo_client, 'search_job_status', return_value=status_running):

            result = sumo_client.search_job_records_sync('test query')

            # Should return the status when timeout occurs
            assert result == status_running


class TestSumoLogicDataConversion:
    """Test SumoLogic data conversion methods"""

    @pytest.fixture
    def sumo_client(self):
        return SumoLogic('test_access_id', 'test_access_key', 'us1')

    def test_convert_logs_to_cbf(self, sumo_client, sample_sumologic_raw_response, sample_continuous_logs_cbf):
        """Test conversion of raw log data to CBF format"""
        result = sumo_client.convert_logs_to_cbf(sample_sumologic_raw_response)

        assert len(result) == 2

        # Check first record structure
        record = result[0]
        assert 'time/usage_start' in record
        assert 'resource/id' in record
        assert 'resource/usage_family' in record
        assert 'usage/amount' in record
        assert 'cost/cost' in record

        # Check slash replacement in source category
        assert 'platform|app-state-machine|cost-adaptor-azure-ingest-cost-reports' in record['resource/id']

        # Check numeric conversions
        assert float(record['usage/amount']) > 0
        assert float(record['cost/cost']) > 0

    def test_convert_logs_to_cbf_empty_input(self, sumo_client):
        """Test CBF conversion with empty input"""
        result = sumo_client.convert_logs_to_cbf([])
        assert result == []

    def test_convert_logs_to_cbf_invalid_record(self, sumo_client):
        """Test CBF conversion with invalid record (missing required fields)"""
        invalid_records = [
            {'map': {'_timeslice': '1234567890', 'datatier': 'Continuous'}},  # Missing credits
            {'map': {'credits': '1.0', 'datatier': 'Continuous'}},  # Missing timeslice
            {'invalid': 'record'}  # No map field
        ]

        result = sumo_client.convert_logs_to_cbf(invalid_records)

        # Should skip invalid records
        assert len(result) == 0

    def test_convert_logs_scanned_to_cbf(self, sumo_client):
        """Test conversion of scan data to CBF format"""
        scan_records = [
            {
                'map': {
                    '_timeslice': '1726761600000',
                    'user_name': 'test_user',
                    'credits': '0.005000'
                }
            }
        ]

        result = sumo_client.convert_logs_scanned_to_cbf(scan_records)

        assert len(result) == 1
        record = result[0]
        assert record['resource/id'] == 'username/test_user'
        assert record['resource/service'] == 'Logs infrequent scan'
        assert record['action/operation'] == 'scan'

    def test_convert_traces_to_cbf(self, sumo_client):
        """Test conversion of trace data to CBF format"""
        trace_records = [
            {
                'map': {
                    '_timeslice': '1726761600000',
                    'sourcecategory': 'platform/app/otlp',
                    'credits': '0.659678'
                }
            }
        ]

        result = sumo_client.convert_traces_to_cbf(trace_records)

        assert len(result) == 1
        record = result[0]
        assert record['resource/id'] == 'sourcecategory/platform|app|otlp'
        assert record['resource/service'] == 'Traces ingest'
        assert record['resource/usage_family'] == 'traces'

    def test_convert_metrics_to_cbf(self, sumo_client):
        """Test conversion of metrics data to CBF format"""
        metrics_records = [
            {
                'map': {
                    '_timeslice': '1726761600000',
                    'sourcecategory': 'aws/kinesis_firehose_metrics/prod',
                    'credits': '112.070312'
                }
            }
        ]

        result = sumo_client.convert_metrics_to_cbf(metrics_records)

        assert len(result) == 1
        record = result[0]
        assert record['resource/id'] == 'sourcecategory/aws|kinesis_firehose_metrics|prod'
        assert record['resource/service'] == 'Metrics ingest'
        assert record['lineitem/description'] == 'daily average 1k datapoints ingested by Source Category'


class TestSumoLogicUsageReport:
    """Test SumoLogic usage report functionality"""

    @pytest.fixture
    def sumo_client(self):
        return SumoLogic('test_access_id', 'test_access_key', 'us1')

    def test_export_usage_report_success(self, sumo_client):
        """Test successful usage report export"""
        mock_response = Mock()
        mock_response.text = '{"jobId": "67890", "status": "InProgress"}'
        mock_response.status_code = 200

        with patch.object(sumo_client, 'post', return_value=mock_response):
            result = sumo_client.export_usage_report()

            assert result["jobId"] == "67890"

    def test_export_usage_report_sync(self, sumo_client, sample_usage_report_data):
        """Test synchronous usage report export"""
        # Mock job creation
        job_response = {"jobId": "67890", "status": "InProgress"}

        # Mock status check
        status_response = {"status": "Success"}

        # Mock CSV content
        csv_content = '''Date,Storage Credits,Infrequent Storage Credits
2025-09-20,87.97,8.84'''

        mock_csv_response = Mock()
        mock_csv_response.text = csv_content

        with patch.object(sumo_client, 'export_usage_report', return_value=job_response), \
             patch.object(sumo_client, 'export_usage_report_status', return_value=status_response), \
             patch.object(sumo_client, 'get', return_value=mock_csv_response), \
             patch('time.sleep'):  # Mock sleep

            result = sumo_client.export_usage_report_sync()

            assert len(result) == 1
            assert result[0]['Date'] == '2025-09-20'
            assert result[0]['Storage Credits'] == '87.97'

    def test_convert_storage_to_cbf(self, sumo_client, sample_storage_cbf):
        """Test conversion of storage data to CBF format"""
        storage_data = [
            {
                'Date': '2025-09-20',
                'Storage Credits': '130.56',
                'Infrequent Storage Credits': '97.08'
            }
        ]

        result = sumo_client.convert_storage_to_cbf(storage_data)

        assert len(result) == 2  # One record for each storage type

        # Check regular storage record
        regular_storage = next(r for r in result if r['resource/id'] == 'log storage')
        assert regular_storage['usage/amount'] == '130.56'
        assert regular_storage['resource/service'] == 'Logs storage'

        # Check infrequent storage record
        infrequent_storage = next(r for r in result if r['resource/id'] == 'infrequent log storage')
        assert infrequent_storage['usage/amount'] == '97.08'
        assert infrequent_storage['resource/service'] == 'Logs infrequent storage'


class TestSumoLogicDateHandling:
    """Test SumoLogic date handling and parsing"""

    @pytest.fixture
    def sumo_client(self):
        return SumoLogic('test_access_id', 'test_access_key', 'us1')

    def test_get_logs_storage_cbf_for_date_valid_date(self, sumo_client, sample_date):
        """Test storage data retrieval for specific date with valid data"""
        csv_data = [
            {
                'Date': '2025-09-22',  # Matches sample_date
                'Storage Credits': '100.0',
                'Infrequent Storage Credits': '50.0'
            },
            {
                'Date': '2025-09-21',  # Different date - should be filtered out
                'Storage Credits': '200.0',
                'Infrequent Storage Credits': '75.0'
            }
        ]

        with patch.object(sumo_client, 'export_usage_report_sync', return_value=csv_data):
            result = sumo_client.get_logs_storage_cbf_for_date(sample_date, sample_date)

            assert len(result) == 2  # Two storage types for the matching date
            for record in result:
                assert record['time/usage_start'] == '2025-09-22'

    def test_get_logs_storage_cbf_for_date_no_data(self, sumo_client, sample_date):
        """Test storage data retrieval when no data exists for date"""
        csv_data = [
            {
                'Date': '2025-09-21',  # Different date
                'Storage Credits': '100.0',
                'Infrequent Storage Credits': '50.0'
            }
        ]

        with patch.object(sumo_client, 'export_usage_report_sync', return_value=csv_data):
            result = sumo_client.get_logs_storage_cbf_for_date(sample_date, sample_date)

            assert len(result) == 0

    def test_date_format_parsing_various_formats(self, sumo_client):
        """Test parsing of various date formats in storage data"""
        test_cases = [
            {'Date': '2025-09-22', 'Storage Credits': '100.0', 'Infrequent Storage Credits': '50.0'},
            {'Date': '"2025-09-22"', 'Storage Credits': '100.0', 'Infrequent Storage Credits': '50.0'},  # Quoted
            {'Date': ' 2025-09-22 ', 'Storage Credits': '100.0', 'Infrequent Storage Credits': '50.0'},  # Whitespace
            {'Date': '09/22/25', 'Storage Credits': '100.0', 'Infrequent Storage Credits': '50.0'},  # Different format
        ]

        target_date = datetime(2025, 9, 22).date()

        for case in test_cases:
            with patch.object(sumo_client, 'export_usage_report_sync', return_value=[case]):
                start_datetime = datetime(2025, 9, 22, tzinfo=timezone.utc)
                end_datetime = datetime(2025, 9, 22, tzinfo=timezone.utc)
                result = sumo_client.get_logs_storage_cbf_for_date(start_datetime, end_datetime)

                # Should successfully parse and return data
                assert len(result) > 0, f"Failed to parse date format: {case['Date']}"


class TestSumoLogicErrorHandling:
    """Test SumoLogic error handling"""

    @pytest.fixture
    def sumo_client(self):
        return SumoLogic('test_access_id', 'test_access_key', 'us1')

    def test_api_error_handling(self, sumo_client):
        """Test handling of API errors"""
        mock_response = Mock()
        mock_response.status_code = 400
        mock_response.text = '{"error": "Bad Request"}'
        mock_response.raise_for_status.side_effect = requests.HTTPError("400 Client Error")

        with patch.object(sumo_client, 'session') as mock_session:
            mock_session.get.return_value = mock_response

            with pytest.raises(requests.HTTPError):
                sumo_client.get('/test/endpoint')

    def test_rate_limit_handling(self, sumo_client):
        """Test handling of rate limit responses"""
        mock_response = Mock()
        mock_response.status_code = 429
        mock_response.text = '{"message": "Rate limit exceeded"}'
        mock_response.raise_for_status.side_effect = requests.HTTPError("429 Too Many Requests")

        with patch.object(sumo_client, 'session') as mock_session:
            mock_session.get.return_value = mock_response

            with pytest.raises(requests.HTTPError):
                sumo_client.get('/test/endpoint')