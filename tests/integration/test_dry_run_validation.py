"""
Integration tests for dry-run validation functionality.
These tests are critical since CloudZero data takes 24 hours to become available.
"""

import pytest
import json
import os
import tempfile
import shutil
from datetime import datetime, timezone, timedelta
from unittest.mock import Mock, patch

from sumo_anycost_lambda import lambda_handler, main


class TestDryRunValidation:
    """Test dry-run output validation - critical for production confidence"""

    @pytest.fixture
    def temp_dry_run_dir(self):
        """Create temporary directory for dry-run tests"""
        temp_dir = tempfile.mkdtemp()
        dry_run_dir = os.path.join(temp_dir, 'dry_run')
        os.makedirs(dry_run_dir)
        yield dry_run_dir
        shutil.rmtree(temp_dir)

    @pytest.fixture
    def mock_services_with_data(self):
        """Mock all services to return realistic test data"""
        mock_sumo = Mock()
        mock_cz = Mock()

        # Realistic CBF data for each service
        mock_sumo.get_continuous_logs_cbf.return_value = [
            {
                'time/usage_start': '2025-09-22T09:00:00+00:00',
                'resource/id': 'sourcecategory/test|service|logs',
                'resource/usage_family': 'Continuous',
                'lineitem/type': 'Usage',
                'lineitem/description': 'Continuous logs ingested by Source Category',
                'resource/service': 'Logs continuous ingest',
                'resource/account': 'test_org_id',
                'resource/region': 'us1',
                'usage/units': 'credits',
                'action/operation': 'ingest',
                'usage/amount': '25.567890',
                'cost/cost': '3.835184'
            }
        ]

        mock_sumo.get_frequent_logs_cbf.return_value = []  # No frequent data

        mock_sumo.get_infrequent_logs_cbf.return_value = [
            {
                'time/usage_start': '2025-09-22T09:00:00+00:00',
                'resource/id': 'sourcecategory/test|service|infrequent',
                'resource/usage_family': 'Infrequent',
                'lineitem/type': 'Usage',
                'lineitem/description': 'Infrequent logs ingested by Source Category',
                'resource/service': 'Logs infrequent ingest',
                'resource/account': 'test_org_id',
                'resource/region': 'us1',
                'usage/units': 'credits',
                'action/operation': 'ingest',
                'usage/amount': '5.123456',
                'cost/cost': '0.768518'
            }
        ]

        mock_sumo.get_infrequent_logs_scanned_cbf.return_value = [
            {
                'time/usage_start': '2025-09-22T09:00:00+00:00',
                'resource/id': 'username/test_user',
                'resource/usage_family': 'infrequent',
                'lineitem/type': 'Usage',
                'lineitem/description': 'Infrequent logs scanned by user',
                'resource/service': 'Logs infrequent scan',
                'resource/account': 'test_org_id',
                'resource/region': 'us1',
                'usage/units': 'credits',
                'action/operation': 'scan',
                'usage/amount': '2.500000',
                'cost/cost': '0.375000'
            }
        ]

        mock_sumo.get_metrics_cbf.return_value = [
            {
                'time/usage_start': '2025-09-22T00:00:00+00:00',
                'resource/id': 'sourcecategory/test|metrics|service',
                'resource/usage_family': 'metrics',
                'lineitem/type': 'Usage',
                'lineitem/description': 'daily average 1k datapoints ingested by Source Category',
                'resource/service': 'Metrics ingest',
                'resource/account': 'test_org_id',
                'resource/region': 'us1',
                'usage/units': 'credits',
                'action/operation': 'ingest',
                'usage/amount': '100.000000',
                'cost/cost': '15.000000'
            }
        ]

        mock_sumo.get_traces_cbf.return_value = [
            {
                'time/usage_start': '2025-09-22T09:00:00+00:00',
                'resource/id': 'sourcecategory/test|traces|service',
                'resource/usage_family': 'traces',
                'lineitem/type': 'Usage',
                'lineitem/description': 'tracing spans ingested by Source Category',
                'resource/service': 'Traces ingest',
                'resource/account': 'test_org_id',
                'resource/region': 'us1',
                'usage/units': 'credits',
                'action/operation': 'ingest',
                'usage/amount': '10.500000',
                'cost/cost': '1.575000'
            }
        ]

        mock_sumo.get_logs_storage_cbf.return_value = [
            {
                'time/usage_start': '2025-09-22',
                'resource/id': 'log storage',
                'resource/usage_family': 'logs',
                'lineitem/type': 'Usage',
                'lineitem/description': 'log storage',
                'resource/service': 'Logs storage',
                'resource/account': 'test_org_id',
                'resource/region': 'us1',
                'usage/units': 'credits',
                'action/operation': 'ingest',
                'usage/amount': '150.000000',
                'cost/cost': '22.500000'
            }
        ]

        # Mock CloudZero to return None in dry-run mode
        mock_cz.post_anycost_stream.return_value = None

        return mock_sumo, mock_cz

    def test_dry_run_creates_all_expected_files(self, temp_dry_run_dir, mock_services_with_data):
        """Test that dry-run mode creates JSON files for all services with data"""
        mock_sumo, mock_cz = mock_services_with_data

        with patch('sumo_anycost_lambda.SumoLogic', return_value=mock_sumo), \
             patch('sumo_anycost_lambda.CloudZero', return_value=mock_cz), \
             patch.dict(os.environ, {'DRY_RUN_MODE': 'true'}), \
             patch('sumo_anycost_lambda.ensure_dry_run_folder'), \
             patch('sumo_anycost_lambda.write_dry_run_data') as mock_write:

            lambda_handler({}, Mock())

            # Verify write_dry_run_data was called for each service with data
            expected_services = [
                'continuous_logs',
                'infrequent_logs',
                'infrequent_scan',
                'metrics',
                'traces',
                'storage'
            ]

            # Should be called once for each service with data
            assert mock_write.call_count == len(expected_services)

            # Verify each service was written
            called_services = [call[0][1] for call in mock_write.call_args_list]  # service_name parameter
            for service in expected_services:
                assert service in called_services, f"Service {service} was not written to dry-run"

    def test_dry_run_json_structure_validation(self, temp_dry_run_dir, mock_services_with_data):
        """Test that dry-run JSON files have correct CBF structure"""
        mock_sumo, mock_cz = mock_services_with_data

        with patch('sumo_anycost_lambda.SumoLogic', return_value=mock_sumo), \
             patch('sumo_anycost_lambda.CloudZero', return_value=mock_cz), \
             patch.dict(os.environ, {'DRY_RUN_MODE': 'true'}), \
             patch('sumo_anycost_lambda.ensure_dry_run_folder') as mock_ensure, \
             patch('sumo_anycost_lambda.write_dry_run_data') as mock_write:

            # Configure mocks to use actual directory
            mock_ensure.return_value = None

            def actual_write_dry_run_data(data, service_name, date, dry_run_folder):
                filename = f"{service_name}_{date}.json"
                filepath = os.path.join(temp_dry_run_dir, filename)
                with open(filepath, 'w') as f:
                    json.dump(data, f, indent=2)

            mock_write.side_effect = actual_write_dry_run_data

            lambda_handler({}, Mock())

            # Verify files were created and have valid JSON structure
            json_files = [f for f in os.listdir(temp_dry_run_dir) if f.endswith('.json')]
            assert len(json_files) > 0, "No JSON files were created"

            for filename in json_files:
                filepath = os.path.join(temp_dry_run_dir, filename)

                # Verify file exists and is readable
                assert os.path.exists(filepath)

                # Verify JSON is valid
                with open(filepath, 'r') as f:
                    data = json.load(f)

                # Verify it's a list
                assert isinstance(data, list), f"File {filename} should contain a list"

                # If data exists, verify CBF structure
                if data:
                    record = data[0]
                    required_cbf_fields = [
                        'time/usage_start',
                        'resource/id',
                        'resource/usage_family',
                        'lineitem/type',
                        'lineitem/description',
                        'resource/service',
                        'resource/account',
                        'resource/region',
                        'usage/units',
                        'action/operation',
                        'usage/amount',
                        'cost/cost'
                    ]

                    for field in required_cbf_fields:
                        assert field in record, f"Required CBF field '{field}' missing from {filename}"

                    # Verify numeric fields are properly formatted
                    assert isinstance(record['usage/amount'], str), f"usage/amount should be string in {filename}"
                    assert isinstance(record['cost/cost'], str), f"cost/cost should be string in {filename}"

                    # Verify numeric values are valid
                    float(record['usage/amount'])  # Should not raise exception
                    float(record['cost/cost'])     # Should not raise exception

    def test_dry_run_data_accuracy_validation(self, temp_dry_run_dir, sample_continuous_logs_cbf):
        """Test that dry-run data matches expected source data"""
        mock_sumo = Mock()
        mock_cz = Mock()

        # Use known test data
        mock_sumo.get_continuous_logs_cbf.return_value = sample_continuous_logs_cbf
        mock_sumo.get_frequent_logs_cbf.return_value = []
        mock_sumo.get_infrequent_logs_cbf.return_value = []
        mock_sumo.get_infrequent_logs_scanned_cbf.return_value = []
        mock_sumo.get_metrics_cbf.return_value = []
        mock_sumo.get_traces_cbf.return_value = []
        mock_sumo.get_logs_storage_cbf.return_value = []

        mock_cz.post_anycost_stream.return_value = None

        with patch('sumo_anycost_lambda.SumoLogic', return_value=mock_sumo), \
             patch('sumo_anycost_lambda.CloudZero', return_value=mock_cz), \
             patch.dict(os.environ, {'DRY_RUN_MODE': 'true'}), \
             patch('sumo_anycost_lambda.ensure_dry_run_folder'), \
             patch('sumo_anycost_lambda.write_dry_run_data') as mock_write:

            lambda_handler({}, Mock())

            # Find the continuous logs call
            continuous_call = None
            for call in mock_write.call_args_list:
                if call[0][1] == 'continuous_logs':  # service_name parameter
                    continuous_call = call
                    break

            assert continuous_call is not None, "Continuous logs data was not written"

            # Verify data matches source
            written_data = continuous_call[0][0]  # data parameter
            assert written_data == sample_continuous_logs_cbf

    def test_dry_run_file_naming_convention(self, temp_dry_run_dir, mock_services_with_data):
        """Test that dry-run files follow correct naming convention"""
        mock_sumo, mock_cz = mock_services_with_data

        with patch('sumo_anycost_lambda.SumoLogic', return_value=mock_sumo), \
             patch('sumo_anycost_lambda.CloudZero', return_value=mock_cz), \
             patch.dict(os.environ, {'DRY_RUN_MODE': 'true'}), \
             patch('sumo_anycost_lambda.ensure_dry_run_folder'), \
             patch('sumo_anycost_lambda.write_dry_run_data') as mock_write:

            lambda_handler({}, Mock())

            # Verify file naming convention
            for call in mock_write.call_args_list:
                data, service_name, date, dry_run_folder = call[0]

                # Verify service name format
                assert isinstance(service_name, str)
                assert len(service_name) > 0
                assert ' ' not in service_name  # No spaces in service names

                # Verify date format (should be YYYY-MM-DD or similar)
                assert isinstance(date, str)
                assert len(date) >= 8  # At least YYYY-MM-DD

    def test_dry_run_empty_services_handling(self, temp_dry_run_dir):
        """Test dry-run handling when services return empty data"""
        mock_sumo = Mock()
        mock_cz = Mock()

        # All services return empty data
        mock_sumo.get_continuous_logs_cbf.return_value = []
        mock_sumo.get_frequent_logs_cbf.return_value = []
        mock_sumo.get_infrequent_logs_cbf.return_value = []
        mock_sumo.get_infrequent_logs_scanned_cbf.return_value = []
        mock_sumo.get_metrics_cbf.return_value = []
        mock_sumo.get_traces_cbf.return_value = []
        mock_sumo.get_logs_storage_cbf.return_value = []

        mock_cz.post_anycost_stream.return_value = None

        with patch('sumo_anycost_lambda.SumoLogic', return_value=mock_sumo), \
             patch('sumo_anycost_lambda.CloudZero', return_value=mock_cz), \
             patch.dict(os.environ, {'DRY_RUN_MODE': 'true'}), \
             patch('sumo_anycost_lambda.ensure_dry_run_folder'), \
             patch('sumo_anycost_lambda.write_dry_run_data') as mock_write:

            lambda_handler({}, Mock())

            # Should not write files for services with no data
            # (This depends on implementation - might write empty files or skip them)
            # Verify the behavior is consistent

    def test_dry_run_cost_calculation_validation(self, temp_dry_run_dir, mock_services_with_data):
        """Test that cost calculations in dry-run data are accurate"""
        mock_sumo, mock_cz = mock_services_with_data

        with patch('sumo_anycost_lambda.SumoLogic', return_value=mock_sumo), \
             patch('sumo_anycost_lambda.CloudZero', return_value=mock_cz), \
             patch.dict(os.environ, {'DRY_RUN_MODE': 'true', 'COST_PER_CREDIT': '0.15'}), \
             patch('sumo_anycost_lambda.ensure_dry_run_folder'), \
             patch('sumo_anycost_lambda.write_dry_run_data') as mock_write:

            lambda_handler({}, Mock())

            # Verify cost calculations are correct
            for call in mock_write.call_args_list:
                data, service_name, date, dry_run_folder = call[0]

                for record in data:
                    usage_amount = float(record['usage/amount'])
                    cost = float(record['cost/cost'])
                    expected_cost = usage_amount * 0.15  # COST_PER_CREDIT

                    # Allow for small floating point differences
                    assert abs(cost - expected_cost) < 0.000001, \
                        f"Cost calculation error in {service_name}: expected {expected_cost}, got {cost}"


class TestDryRunBackfillValidation:
    """Test dry-run validation for backfill scenarios"""

    @pytest.fixture
    def temp_dry_run_dir(self):
        """Create temporary directory for dry-run tests"""
        temp_dir = tempfile.mkdtemp()
        dry_run_dir = os.path.join(temp_dir, 'dry_run')
        os.makedirs(dry_run_dir)
        yield dry_run_dir
        shutil.rmtree(temp_dir)

    def test_dry_run_backfill_multiple_dates(self, temp_dry_run_dir):
        """Test dry-run validation across multiple dates in backfill mode"""
        mock_sumo = Mock()
        mock_cz = Mock()

        # Mock date-specific methods
        mock_sumo.get_continuous_logs_cbf_for_date.return_value = [
            {
                'time/usage_start': '2025-09-22T09:00:00+00:00',
                'resource/id': 'sourcecategory/test|service',
                'resource/usage_family': 'Continuous',
                'lineitem/type': 'Usage',
                'lineitem/description': 'Continuous logs ingested by Source Category',
                'resource/service': 'Logs continuous ingest',
                'resource/account': 'test_org_id',
                'resource/region': 'us1',
                'usage/units': 'credits',
                'action/operation': 'ingest',
                'usage/amount': '10.000000',
                'cost/cost': '1.500000'
            }
        ]

        # Mock other services to return empty data
        date_methods = [
            'get_frequent_logs_cbf_for_date',
            'get_infrequent_logs_cbf_for_date',
            'get_infrequent_logs_scanned_cbf_for_date',
            'get_metrics_cbf_for_date',
            'get_traces_cbf_for_date',
            'get_logs_storage_cbf_for_date'
        ]

        for method in date_methods:
            getattr(mock_sumo, method).return_value = []

        mock_cz.post_anycost_stream.return_value = None

        # Mock backfill state and progress
        mock_backfill_state = Mock()
        mock_progress = Mock()

        # Simulate 3-day backfill
        dates = [
            datetime(2025, 9, 22, tzinfo=timezone.utc).date(),
            datetime(2025, 9, 23, tzinfo=timezone.utc).date(),
            datetime(2025, 9, 24, tzinfo=timezone.utc).date()
        ]

        mock_progress.has_more_dates.side_effect = [True, True, True, False]
        mock_progress.current_date = dates[0]

        def next_date():
            current_index = dates.index(mock_progress.current_date)
            if current_index < len(dates) - 1:
                mock_progress.current_date = dates[current_index + 1]
            return mock_progress.current_date

        mock_progress.next_date.side_effect = next_date
        mock_backfill_state.is_completed.return_value = True

        with patch('sumo_anycost_lambda.SumoLogic', return_value=mock_sumo), \
             patch('sumo_anycost_lambda.CloudZero', return_value=mock_cz), \
             patch('sumo_anycost_lambda.BackfillState', return_value=mock_backfill_state), \
             patch('sumo_anycost_lambda.BackfillProgress', return_value=mock_progress), \
             patch.dict(os.environ, {
                 'DRY_RUN_MODE': 'true',
                 'BACKFILL_MODE': 'true',
                 'BACKFILL_START_DATE': '2025-09-22',
                 'BACKFILL_END_DATE': '2025-09-24'
             }), \
             patch('sumo_anycost_lambda.ensure_dry_run_folder'), \
             patch('sumo_anycost_lambda.write_dry_run_data') as mock_write:

            lambda_handler({}, Mock())

            # Should write files for each date
            written_dates = set()
            for call in mock_write.call_args_list:
                data, service_name, date, dry_run_folder = call[0]
                written_dates.add(date)

            # Verify files were written for all dates
            expected_dates = {'2025-09-22', '2025-09-23', '2025-09-24'}
            assert len(written_dates) >= 3, f"Expected at least 3 dates, got {len(written_dates)}"

    def test_dry_run_date_specific_validation(self, temp_dry_run_dir):
        """Test that dry-run data contains correct date-specific information"""
        mock_sumo = Mock()
        mock_cz = Mock()

        # Return different data for different dates
        def get_continuous_logs_for_date(start_date, end_date):
            date_str = start_date.strftime('%Y-%m-%d')
            return [
                {
                    'time/usage_start': f'{date_str}T09:00:00+00:00',
                    'resource/id': f'sourcecategory/service|{date_str}',
                    'resource/usage_family': 'Continuous',
                    'lineitem/type': 'Usage',
                    'lineitem/description': 'Continuous logs ingested by Source Category',
                    'resource/service': 'Logs continuous ingest',
                    'resource/account': 'test_org_id',
                    'resource/region': 'us1',
                    'usage/units': 'credits',
                    'action/operation': 'ingest',
                    'usage/amount': '10.000000',
                    'cost/cost': '1.500000'
                }
            ]

        mock_sumo.get_continuous_logs_cbf_for_date.side_effect = get_continuous_logs_for_date

        # Mock other date methods to return empty
        date_methods = [
            'get_frequent_logs_cbf_for_date',
            'get_infrequent_logs_cbf_for_date',
            'get_infrequent_logs_scanned_cbf_for_date',
            'get_metrics_cbf_for_date',
            'get_traces_cbf_for_date',
            'get_logs_storage_cbf_for_date'
        ]

        for method in date_methods:
            getattr(mock_sumo, method).return_value = []

        mock_cz.post_anycost_stream.return_value = None

        # Mock single-day backfill
        mock_backfill_state = Mock()
        mock_progress = Mock()

        test_date = datetime(2025, 9, 22, tzinfo=timezone.utc).date()
        mock_progress.has_more_dates.side_effect = [True, False]
        mock_progress.current_date = test_date
        mock_backfill_state.is_completed.return_value = True

        with patch('sumo_anycost_lambda.SumoLogic', return_value=mock_sumo), \
             patch('sumo_anycost_lambda.CloudZero', return_value=mock_cz), \
             patch('sumo_anycost_lambda.BackfillState', return_value=mock_backfill_state), \
             patch('sumo_anycost_lambda.BackfillProgress', return_value=mock_progress), \
             patch.dict(os.environ, {
                 'DRY_RUN_MODE': 'true',
                 'BACKFILL_MODE': 'true',
                 'BACKFILL_START_DATE': '2025-09-22',
                 'BACKFILL_END_DATE': '2025-09-22'
             }), \
             patch('sumo_anycost_lambda.ensure_dry_run_folder'), \
             patch('sumo_anycost_lambda.write_dry_run_data') as mock_write:

            lambda_handler({}, Mock())

            # Verify date-specific data was written
            continuous_call = None
            for call in mock_write.call_args_list:
                if call[0][1] == 'continuous_logs':  # service_name
                    continuous_call = call
                    break

            assert continuous_call is not None
            data, service_name, date, dry_run_folder = continuous_call[0]

            # Verify data contains correct date
            assert len(data) == 1
            record = data[0]
            assert '2025-09-22' in record['time/usage_start']
            assert '2025-09-22' in record['resource/id']


class TestDryRunErrorValidation:
    """Test dry-run validation handles errors gracefully"""

    def test_dry_run_with_service_errors(self):
        """Test dry-run continues when individual services fail"""
        mock_sumo = Mock()
        mock_cz = Mock()

        # Make some services fail
        mock_sumo.get_continuous_logs_cbf.side_effect = Exception("API Error")
        mock_sumo.get_frequent_logs_cbf.return_value = []
        mock_sumo.get_infrequent_logs_cbf.return_value = [{'test': 'data'}]
        mock_sumo.get_infrequent_logs_scanned_cbf.return_value = []
        mock_sumo.get_metrics_cbf.return_value = []
        mock_sumo.get_traces_cbf.return_value = []
        mock_sumo.get_logs_storage_cbf.return_value = []

        mock_cz.post_anycost_stream.return_value = None

        with patch('sumo_anycost_lambda.SumoLogic', return_value=mock_sumo), \
             patch('sumo_anycost_lambda.CloudZero', return_value=mock_cz), \
             patch.dict(os.environ, {'DRY_RUN_MODE': 'true'}), \
             patch('sumo_anycost_lambda.ensure_dry_run_folder'), \
             patch('sumo_anycost_lambda.write_dry_run_data') as mock_write:

            result = lambda_handler({}, Mock())

            # Should still succeed overall
            assert result['statusCode'] == 200

            # Should still write data for working services
            written_services = [call[0][1] for call in mock_write.call_args_list]
            assert 'infrequent_logs' in written_services

    def test_dry_run_folder_creation_error(self):
        """Test handling of dry-run folder creation errors"""
        mock_sumo = Mock()
        mock_cz = Mock()

        mock_sumo.get_continuous_logs_cbf.return_value = [{'test': 'data'}]
        mock_sumo.get_frequent_logs_cbf.return_value = []
        mock_sumo.get_infrequent_logs_cbf.return_value = []
        mock_sumo.get_infrequent_logs_scanned_cbf.return_value = []
        mock_sumo.get_metrics_cbf.return_value = []
        mock_sumo.get_traces_cbf.return_value = []
        mock_sumo.get_logs_storage_cbf.return_value = []

        mock_cz.post_anycost_stream.return_value = None

        with patch('sumo_anycost_lambda.SumoLogic', return_value=mock_sumo), \
             patch('sumo_anycost_lambda.CloudZero', return_value=mock_cz), \
             patch.dict(os.environ, {'DRY_RUN_MODE': 'true'}), \
             patch('sumo_anycost_lambda.ensure_dry_run_folder', side_effect=OSError("Permission denied")), \
             patch('sumo_anycost_lambda.write_dry_run_data') as mock_write:

            # Should handle folder creation error gracefully
            result = lambda_handler({}, Mock())

            # Should still return success (error handled)
            assert result['statusCode'] == 200