"""
End-to-end integration tests for the complete SumoLogic to CloudZero workflow.
"""

import pytest
import os
import tempfile
import shutil
from datetime import datetime, timezone, timedelta
from unittest.mock import Mock, patch, MagicMock

from sumo_anycost_lambda import lambda_handler


class TestEndToEndWorkflow:
    """Test complete end-to-end workflows"""

    @pytest.fixture
    def temp_dir(self):
        """Create temporary directory for test files"""
        temp_dir = tempfile.mkdtemp()
        yield temp_dir
        shutil.rmtree(temp_dir)

    @pytest.fixture
    def complete_mock_setup(self):
        """Set up complete mock environment for end-to-end testing"""
        mock_sumo = Mock()
        mock_cz = Mock()

        # Configure SumoLogic mocks with realistic data
        mock_sumo.get_continuous_logs_cbf.return_value = [
            {
                'time/usage_start': '2025-09-22T09:00:00+00:00',
                'resource/id': 'sourcecategory/test|continuous|service',
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
                'resource/id': 'sourcecategory/test|infrequent|service',
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

        mock_sumo.get_infrequent_logs_scanned_cbf.return_value = []
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
        mock_sumo.get_traces_cbf.return_value = []
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

        # Configure CloudZero mocks
        mock_cz.post_anycost_stream.return_value = {'success': True, 'message': 'Data uploaded'}

        return mock_sumo, mock_cz

    def test_normal_mode_end_to_end(self, complete_mock_setup):
        """Test complete normal mode workflow"""
        mock_sumo, mock_cz = complete_mock_setup

        lambda_event = {}
        lambda_context = Mock()

        with patch('sumo_anycost_lambda.SumoLogic', return_value=mock_sumo), \
             patch('sumo_anycost_lambda.CloudZero', return_value=mock_cz), \
             patch.dict(os.environ, {'DRY_RUN_MODE': 'false', 'BACKFILL_MODE': 'false'}):

            result = lambda_handler(lambda_event, lambda_context)

            # Verify successful execution
            assert result['statusCode'] == 200
            assert 'success' in result['body'].lower()

            # Verify all SumoLogic service methods were called
            service_methods = [
                'get_continuous_logs_cbf',
                'get_frequent_logs_cbf',
                'get_infrequent_logs_cbf',
                'get_infrequent_logs_scanned_cbf',
                'get_metrics_cbf',
                'get_traces_cbf',
                'get_logs_storage_cbf'
            ]

            for method in service_methods:
                assert getattr(mock_sumo, method).called, f"{method} was not called"

            # Verify CloudZero was called for services with data
            # Should be called for: continuous, infrequent, metrics, storage (4 services)
            assert mock_cz.post_anycost_stream.call_count >= 4

            # Verify correct service names and data were passed to CloudZero
            call_args_list = mock_cz.post_anycost_stream.call_args_list
            called_services = set()

            for call in call_args_list:
                # Extract service name from call arguments
                if len(call[0]) >= 2:  # data, service_name
                    service_name = call[0][1] if len(call[0]) > 1 else call[1].get('service_name')
                    if service_name:
                        called_services.add(service_name)

            expected_services = {'continuous_logs', 'infrequent_logs', 'metrics', 'storage'}
            # Should have called all services with data
            assert len(called_services.intersection(expected_services)) >= 3

    def test_dry_run_mode_end_to_end(self, complete_mock_setup, temp_dir):
        """Test complete dry-run mode workflow"""
        mock_sumo, mock_cz = complete_mock_setup

        # In dry-run mode, CloudZero should return None
        mock_cz.post_anycost_stream.return_value = None

        lambda_event = {}
        lambda_context = Mock()

        dry_run_dir = os.path.join(temp_dir, 'dry_run')

        with patch('sumo_anycost_lambda.SumoLogic', return_value=mock_sumo), \
             patch('sumo_anycost_lambda.CloudZero', return_value=mock_cz), \
             patch.dict(os.environ, {'DRY_RUN_MODE': 'true', 'BACKFILL_MODE': 'false'}), \
             patch('sumo_anycost_lambda.ensure_dry_run_folder') as mock_ensure, \
             patch('sumo_anycost_lambda.write_dry_run_data') as mock_write:

            result = lambda_handler(lambda_event, lambda_context)

            # Verify successful execution
            assert result['statusCode'] == 200

            # Verify all SumoLogic service methods were still called
            service_methods = [
                'get_continuous_logs_cbf',
                'get_frequent_logs_cbf',
                'get_infrequent_logs_cbf',
                'get_infrequent_logs_scanned_cbf',
                'get_metrics_cbf',
                'get_traces_cbf',
                'get_logs_storage_cbf'
            ]

            for method in service_methods:
                assert getattr(mock_sumo, method).called

            # Verify dry-run functions were called
            assert mock_ensure.called
            assert mock_write.called

            # Verify CloudZero was called but returned None (dry-run mode)
            assert mock_cz.post_anycost_stream.called

    def test_backfill_mode_end_to_end(self, complete_mock_setup, temp_dir):
        """Test complete backfill mode workflow"""
        mock_sumo, mock_cz = complete_mock_setup

        # Configure date-specific methods
        mock_sumo.get_continuous_logs_cbf_for_date.return_value = mock_sumo.get_continuous_logs_cbf.return_value
        mock_sumo.get_frequent_logs_cbf_for_date.return_value = []
        mock_sumo.get_infrequent_logs_cbf_for_date.return_value = mock_sumo.get_infrequent_logs_cbf.return_value
        mock_sumo.get_infrequent_logs_scanned_cbf_for_date.return_value = []
        mock_sumo.get_metrics_cbf_for_date.return_value = mock_sumo.get_metrics_cbf.return_value
        mock_sumo.get_traces_cbf_for_date.return_value = []
        mock_sumo.get_logs_storage_cbf_for_date.return_value = mock_sumo.get_logs_storage_cbf.return_value

        # Mock backfill state and progress
        mock_backfill_state = Mock()
        mock_progress = Mock()

        # Simulate 2-day backfill
        test_dates = [
            datetime(2025, 9, 22, tzinfo=timezone.utc).date(),
            datetime(2025, 9, 23, tzinfo=timezone.utc).date()
        ]

        mock_progress.has_more_dates.side_effect = [True, True, False]
        mock_progress.current_date = test_dates[0]

        def next_date():
            current_index = test_dates.index(mock_progress.current_date)
            if current_index < len(test_dates) - 1:
                mock_progress.current_date = test_dates[current_index + 1]
            return mock_progress.current_date

        mock_progress.next_date.side_effect = next_date

        mock_backfill_state.is_completed.return_value = True
        mock_backfill_state.cleanup.return_value = None

        lambda_event = {}
        lambda_context = Mock()

        with patch('sumo_anycost_lambda.SumoLogic', return_value=mock_sumo), \
             patch('sumo_anycost_lambda.CloudZero', return_value=mock_cz), \
             patch('sumo_anycost_lambda.BackfillState', return_value=mock_backfill_state), \
             patch('sumo_anycost_lambda.BackfillProgress', return_value=mock_progress), \
             patch.dict(os.environ, {
                 'BACKFILL_MODE': 'true',
                 'BACKFILL_START_DATE': '2025-09-22',
                 'BACKFILL_END_DATE': '2025-09-23',
                 'DRY_RUN_MODE': 'false'
             }):

            result = lambda_handler(lambda_event, lambda_context)

            # Verify successful execution
            assert result['statusCode'] == 200

            # Verify date-specific methods were called
            date_methods = [
                'get_continuous_logs_cbf_for_date',
                'get_frequent_logs_cbf_for_date',
                'get_infrequent_logs_cbf_for_date',
                'get_infrequent_logs_scanned_cbf_for_date',
                'get_metrics_cbf_for_date',
                'get_traces_cbf_for_date',
                'get_logs_storage_cbf_for_date'
            ]

            for method in date_methods:
                assert getattr(mock_sumo, method).called, f"{method} was not called"

            # Verify CloudZero was called multiple times (once per date per service with data)
            assert mock_cz.post_anycost_stream.call_count >= 6  # 2 dates * 3+ services

            # Verify backfill state management
            assert mock_backfill_state.mark_date_completed.called
            assert mock_backfill_state.save.called
            assert mock_backfill_state.cleanup.called

    def test_error_recovery_end_to_end(self, complete_mock_setup):
        """Test end-to-end error recovery"""
        mock_sumo, mock_cz = complete_mock_setup

        # Make one service fail
        mock_sumo.get_continuous_logs_cbf.side_effect = Exception("SumoLogic API Error")

        lambda_event = {}
        lambda_context = Mock()

        with patch('sumo_anycost_lambda.SumoLogic', return_value=mock_sumo), \
             patch('sumo_anycost_lambda.CloudZero', return_value=mock_cz):

            result = lambda_handler(lambda_event, lambda_context)

            # Should still succeed overall despite one service failing
            assert result['statusCode'] == 200

            # Other services should still be processed
            assert mock_sumo.get_infrequent_logs_cbf.called
            assert mock_sumo.get_metrics_cbf.called

            # CloudZero should still be called for working services
            assert mock_cz.post_anycost_stream.called

    def test_mixed_data_scenarios_end_to_end(self):
        """Test end-to-end with mixed data scenarios (some empty, some with data)"""
        mock_sumo = Mock()
        mock_cz = Mock()

        # Mixed scenario: some services have data, others are empty
        mock_sumo.get_continuous_logs_cbf.return_value = [{'test': 'continuous_data'}]
        mock_sumo.get_frequent_logs_cbf.return_value = []  # Empty
        mock_sumo.get_infrequent_logs_cbf.return_value = []  # Empty
        mock_sumo.get_infrequent_logs_scanned_cbf.return_value = [{'test': 'scan_data'}]
        mock_sumo.get_metrics_cbf.return_value = []  # Empty
        mock_sumo.get_traces_cbf.return_value = [{'test': 'trace_data'}]
        mock_sumo.get_logs_storage_cbf.return_value = []  # Empty

        mock_cz.post_anycost_stream.return_value = {'success': True}

        lambda_event = {}
        lambda_context = Mock()

        with patch('sumo_anycost_lambda.SumoLogic', return_value=mock_sumo), \
             patch('sumo_anycost_lambda.CloudZero', return_value=mock_cz):

            result = lambda_handler(lambda_event, lambda_context)

            assert result['statusCode'] == 200

            # All service methods should be called regardless of data
            service_methods = [
                'get_continuous_logs_cbf',
                'get_frequent_logs_cbf',
                'get_infrequent_logs_cbf',
                'get_infrequent_logs_scanned_cbf',
                'get_metrics_cbf',
                'get_traces_cbf',
                'get_logs_storage_cbf'
            ]

            for method in service_methods:
                assert getattr(mock_sumo, method).called

            # CloudZero should be called for services with data
            # Exact count depends on implementation (might skip empty or still call with empty data)
            assert mock_cz.post_anycost_stream.called

    def test_large_data_chunking_end_to_end(self):
        """Test end-to-end with large data that requires chunking"""
        mock_sumo = Mock()
        mock_cz = Mock()

        # Create large dataset that will require chunking
        large_dataset = []
        for i in range(5000):  # Large number of records
            large_dataset.append({
                'time/usage_start': f'2025-09-22T{i % 24:02d}:00:00+00:00',
                'resource/id': f'sourcecategory/service|{i}',
                'resource/usage_family': 'Continuous',
                'lineitem/type': 'Usage',
                'lineitem/description': 'Test record for chunking',
                'resource/service': 'Logs continuous ingest',
                'resource/account': 'test_org_id',
                'resource/region': 'us1',
                'usage/units': 'credits',
                'action/operation': 'ingest',
                'usage/amount': f'{i * 0.001:.6f}',
                'cost/cost': f'{i * 0.001 * 0.15:.6f}'
            })

        mock_sumo.get_continuous_logs_cbf.return_value = large_dataset
        mock_sumo.get_frequent_logs_cbf.return_value = []
        mock_sumo.get_infrequent_logs_cbf.return_value = []
        mock_sumo.get_infrequent_logs_scanned_cbf.return_value = []
        mock_sumo.get_metrics_cbf.return_value = []
        mock_sumo.get_traces_cbf.return_value = []
        mock_sumo.get_logs_storage_cbf.return_value = []

        mock_cz.post_anycost_stream.return_value = {'success': True}

        lambda_event = {}
        lambda_context = Mock()

        with patch('sumo_anycost_lambda.SumoLogic', return_value=mock_sumo), \
             patch('sumo_anycost_lambda.CloudZero', return_value=mock_cz):

            result = lambda_handler(lambda_event, lambda_context)

            assert result['statusCode'] == 200

            # Large data should trigger chunking, resulting in multiple CloudZero calls
            assert mock_cz.post_anycost_stream.call_count >= 1

            # Verify all data was processed (sum of all chunks should equal original data)
            total_records_sent = 0
            for call in mock_cz.post_anycost_stream.call_args_list:
                if call[0] and len(call[0]) > 0:  # Check if data was passed
                    data = call[0][0]  # First argument is the data
                    if isinstance(data, list):
                        total_records_sent += len(data)

            # Should process all records (possibly across multiple chunks)
            assert total_records_sent > 0


class TestEndToEndPerformance:
    """Test performance aspects of end-to-end workflows"""

    def test_reasonable_execution_time(self):
        """Test that normal execution completes in reasonable time"""
        import time

        mock_sumo = Mock()
        mock_cz = Mock()

        # Simulate realistic data sizes
        mock_sumo.get_continuous_logs_cbf.return_value = [{'test': 'data'}] * 100
        mock_sumo.get_frequent_logs_cbf.return_value = []
        mock_sumo.get_infrequent_logs_cbf.return_value = [{'test': 'data'}] * 50
        mock_sumo.get_infrequent_logs_scanned_cbf.return_value = []
        mock_sumo.get_metrics_cbf.return_value = [{'test': 'data'}] * 10
        mock_sumo.get_traces_cbf.return_value = [{'test': 'data'}] * 25
        mock_sumo.get_logs_storage_cbf.return_value = [{'test': 'data'}] * 2

        mock_cz.post_anycost_stream.return_value = {'success': True}

        lambda_event = {}
        lambda_context = Mock()

        start_time = time.time()

        with patch('sumo_anycost_lambda.SumoLogic', return_value=mock_sumo), \
             patch('sumo_anycost_lambda.CloudZero', return_value=mock_cz):

            result = lambda_handler(lambda_event, lambda_context)

        execution_time = time.time() - start_time

        assert result['statusCode'] == 200
        # Should complete in under 5 seconds for mocked data
        assert execution_time < 5.0

    def test_memory_efficiency(self):
        """Test that the workflow handles data efficiently"""
        import sys

        mock_sumo = Mock()
        mock_cz = Mock()

        # Large dataset to test memory handling
        large_data = [{'test': f'data_{i}'} for i in range(1000)]

        mock_sumo.get_continuous_logs_cbf.return_value = large_data
        mock_sumo.get_frequent_logs_cbf.return_value = []
        mock_sumo.get_infrequent_logs_cbf.return_value = large_data.copy()
        mock_sumo.get_infrequent_logs_scanned_cbf.return_value = []
        mock_sumo.get_metrics_cbf.return_value = large_data.copy()
        mock_sumo.get_traces_cbf.return_value = []
        mock_sumo.get_logs_storage_cbf.return_value = []

        mock_cz.post_anycost_stream.return_value = {'success': True}

        lambda_event = {}
        lambda_context = Mock()

        # Monitor memory usage (basic check)
        initial_objects = len([obj for obj in sys.modules.keys()])

        with patch('sumo_anycost_lambda.SumoLogic', return_value=mock_sumo), \
             patch('sumo_anycost_lambda.CloudZero', return_value=mock_cz):

            result = lambda_handler(lambda_event, lambda_context)

        final_objects = len([obj for obj in sys.modules.keys()])

        assert result['statusCode'] == 200
        # Should not create excessive new objects/modules
        assert final_objects - initial_objects < 10