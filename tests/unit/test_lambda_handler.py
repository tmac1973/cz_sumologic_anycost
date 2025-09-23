"""
Unit tests for the lambda_handler and main orchestration functions.
"""

import pytest
import os
import sys
from datetime import datetime, timezone, timedelta
from unittest.mock import Mock, patch, MagicMock

from sumo_anycost_lambda import lambda_handler, main


class TestLambdaHandler:
    """Test lambda_handler function"""

    @pytest.fixture
    def lambda_event(self):
        """Standard Lambda event for testing"""
        return {}

    @pytest.fixture
    def lambda_context(self):
        """Mock Lambda context"""
        context = Mock()
        context.function_name = 'test_function'
        context.function_version = '1'
        context.invoked_function_arn = 'arn:aws:lambda:us-east-1:123456789012:function:test'
        context.memory_limit_in_mb = '128'
        context.remaining_time_in_millis = lambda: 30000
        return context

    def test_lambda_handler_success_normal_mode(self, lambda_event, lambda_context):
        """Test successful lambda handler execution in normal mode"""
        mock_sumo = Mock()
        mock_cz = Mock()

        # Mock all service methods to return sample data
        mock_sumo.get_continuous_logs_cbf.return_value = [{'test': 'data'}]
        mock_sumo.get_frequent_logs_cbf.return_value = []
        mock_sumo.get_infrequent_logs_cbf.return_value = [{'test': 'data'}]
        mock_sumo.get_infrequent_logs_scanned_cbf.return_value = []
        mock_sumo.get_metrics_cbf.return_value = [{'test': 'data'}]
        mock_sumo.get_traces_cbf.return_value = []
        mock_sumo.get_logs_storage_cbf.return_value = [{'test': 'data'}]

        mock_cz.post_anycost_stream.return_value = {'success': True}

        with patch('sumo_anycost_lambda.SumoLogic', return_value=mock_sumo), \
             patch('sumo_anycost_lambda.CloudZero', return_value=mock_cz), \
             patch.dict(os.environ, {'DRY_RUN_MODE': 'false', 'BACKFILL_MODE': 'false'}):

            result = lambda_handler(lambda_event, lambda_context)

            assert result['statusCode'] == 200
            assert 'success' in result['body']

    def test_lambda_handler_success_dry_run_mode(self, lambda_event, lambda_context):
        """Test successful lambda handler execution in dry-run mode"""
        mock_sumo = Mock()
        mock_cz = Mock()

        mock_sumo.get_continuous_logs_cbf.return_value = [{'test': 'data'}]
        mock_sumo.get_frequent_logs_cbf.return_value = []
        mock_sumo.get_infrequent_logs_cbf.return_value = []
        mock_sumo.get_infrequent_logs_scanned_cbf.return_value = []
        mock_sumo.get_metrics_cbf.return_value = []
        mock_sumo.get_traces_cbf.return_value = []
        mock_sumo.get_logs_storage_cbf.return_value = []

        # In dry-run mode, post_anycost_stream returns None
        mock_cz.post_anycost_stream.return_value = None

        with patch('sumo_anycost_lambda.SumoLogic', return_value=mock_sumo), \
             patch('sumo_anycost_lambda.CloudZero', return_value=mock_cz), \
             patch.dict(os.environ, {'DRY_RUN_MODE': 'true', 'BACKFILL_MODE': 'false'}):

            result = lambda_handler(lambda_event, lambda_context)

            assert result['statusCode'] == 200
            assert 'dry-run' in result['body'].lower()

    def test_lambda_handler_backfill_mode(self, lambda_event, lambda_context):
        """Test lambda handler execution in backfill mode"""
        mock_sumo = Mock()
        mock_cz = Mock()
        mock_backfill_state = Mock()

        mock_sumo.get_continuous_logs_cbf_for_date.return_value = [{'test': 'data'}]
        mock_sumo.get_frequent_logs_cbf_for_date.return_value = []
        mock_sumo.get_infrequent_logs_cbf_for_date.return_value = []
        mock_sumo.get_infrequent_logs_scanned_cbf_for_date.return_value = []
        mock_sumo.get_metrics_cbf_for_date.return_value = []
        mock_sumo.get_traces_cbf_for_date.return_value = []
        mock_sumo.get_logs_storage_cbf_for_date.return_value = []

        mock_cz.post_anycost_stream.return_value = {'success': True}

        # Mock backfill progress
        mock_progress = Mock()
        mock_progress.has_more_dates.side_effect = [True, False]  # One iteration
        mock_progress.current_date = datetime(2025, 8, 1, tzinfo=timezone.utc).date()

        mock_backfill_state.is_completed.return_value = True

        with patch('sumo_anycost_lambda.SumoLogic', return_value=mock_sumo), \
             patch('sumo_anycost_lambda.CloudZero', return_value=mock_cz), \
             patch('sumo_anycost_lambda.BackfillState', return_value=mock_backfill_state), \
             patch('sumo_anycost_lambda.BackfillProgress', return_value=mock_progress), \
             patch.dict(os.environ, {
                 'BACKFILL_MODE': 'true',
                 'BACKFILL_START_DATE': '2025-08-01',
                 'BACKFILL_END_DATE': '2025-08-31'
             }):

            result = lambda_handler(lambda_event, lambda_context)

            assert result['statusCode'] == 200
            assert 'backfill' in result['body'].lower()

    def test_lambda_handler_error_handling(self, lambda_event, lambda_context):
        """Test lambda handler error handling"""
        with patch('sumo_anycost_lambda.SumoLogic', side_effect=Exception("Test error")):
            result = lambda_handler(lambda_event, lambda_context)

            assert result['statusCode'] == 500
            assert 'error' in result['body'].lower()

    def test_lambda_handler_missing_environment_variables(self, lambda_event, lambda_context):
        """Test lambda handler with missing environment variables"""
        # Clear critical environment variables
        with patch.dict(os.environ, {}, clear=True):
            result = lambda_handler(lambda_event, lambda_context)

            assert result['statusCode'] == 500
            assert 'error' in result['body'].lower()


class TestMainFunction:
    """Test main function and command-line argument parsing"""

    def test_main_with_no_arguments(self):
        """Test main function with no command-line arguments"""
        mock_sumo = Mock()
        mock_cz = Mock()

        mock_sumo.get_continuous_logs_cbf.return_value = []
        mock_sumo.get_frequent_logs_cbf.return_value = []
        mock_sumo.get_infrequent_logs_cbf.return_value = []
        mock_sumo.get_infrequent_logs_scanned_cbf.return_value = []
        mock_sumo.get_metrics_cbf.return_value = []
        mock_sumo.get_traces_cbf.return_value = []
        mock_sumo.get_logs_storage_cbf.return_value = []

        with patch('sumo_anycost_lambda.SumoLogic', return_value=mock_sumo), \
             patch('sumo_anycost_lambda.CloudZero', return_value=mock_cz), \
             patch('sys.argv', ['sumo_anycost_lambda.py']):

            # Should run without error
            main()

    def test_main_with_days_argument(self):
        """Test main function with --days argument"""
        mock_sumo = Mock()
        mock_cz = Mock()
        mock_backfill_state = Mock()

        # Mock backfill methods
        mock_sumo.get_continuous_logs_cbf_for_date.return_value = []
        mock_sumo.get_frequent_logs_cbf_for_date.return_value = []
        mock_sumo.get_infrequent_logs_cbf_for_date.return_value = []
        mock_sumo.get_infrequent_logs_scanned_cbf_for_date.return_value = []
        mock_sumo.get_metrics_cbf_for_date.return_value = []
        mock_sumo.get_traces_cbf_for_date.return_value = []
        mock_sumo.get_logs_storage_cbf_for_date.return_value = []

        # Mock progress to complete after one iteration
        mock_progress = Mock()
        mock_progress.has_more_dates.side_effect = [True, False]
        mock_progress.current_date = datetime(2025, 8, 1, tzinfo=timezone.utc).date()

        mock_backfill_state.is_completed.return_value = True

        with patch('sumo_anycost_lambda.SumoLogic', return_value=mock_sumo), \
             patch('sumo_anycost_lambda.CloudZero', return_value=mock_cz), \
             patch('sumo_anycost_lambda.BackfillState', return_value=mock_backfill_state), \
             patch('sumo_anycost_lambda.BackfillProgress', return_value=mock_progress), \
             patch('sys.argv', ['sumo_anycost_lambda.py', '--days', '7']):

            main()

            # Verify backfill methods were called
            assert mock_sumo.get_continuous_logs_cbf_for_date.called

    def test_main_with_dry_run_argument(self):
        """Test main function with --dry-run argument"""
        mock_sumo = Mock()
        mock_cz = Mock()

        mock_sumo.get_continuous_logs_cbf.return_value = [{'test': 'data'}]
        mock_sumo.get_frequent_logs_cbf.return_value = []
        mock_sumo.get_infrequent_logs_cbf.return_value = []
        mock_sumo.get_infrequent_logs_scanned_cbf.return_value = []
        mock_sumo.get_metrics_cbf.return_value = []
        mock_sumo.get_traces_cbf.return_value = []
        mock_sumo.get_logs_storage_cbf.return_value = []

        # In dry-run mode, should return None (no API calls)
        mock_cz.post_anycost_stream.return_value = None

        with patch('sumo_anycost_lambda.SumoLogic', return_value=mock_sumo), \
             patch('sumo_anycost_lambda.CloudZero', return_value=mock_cz), \
             patch('sys.argv', ['sumo_anycost_lambda.py', '--dry-run']), \
             patch.dict(os.environ, {'DRY_RUN_MODE': 'true'}):

            main()

            # Verify dry-run mode was used
            assert mock_cz.post_anycost_stream.called

    def test_main_with_backfill_date_range(self):
        """Test main function with --backfill-start and --backfill-end arguments"""
        mock_sumo = Mock()
        mock_cz = Mock()
        mock_backfill_state = Mock()

        mock_sumo.get_continuous_logs_cbf_for_date.return_value = []
        mock_sumo.get_frequent_logs_cbf_for_date.return_value = []
        mock_sumo.get_infrequent_logs_cbf_for_date.return_value = []
        mock_sumo.get_infrequent_logs_scanned_cbf_for_date.return_value = []
        mock_sumo.get_metrics_cbf_for_date.return_value = []
        mock_sumo.get_traces_cbf_for_date.return_value = []
        mock_sumo.get_logs_storage_cbf_for_date.return_value = []

        mock_progress = Mock()
        mock_progress.has_more_dates.side_effect = [True, False]
        mock_progress.current_date = datetime(2025, 8, 1, tzinfo=timezone.utc).date()

        mock_backfill_state.is_completed.return_value = True

        with patch('sumo_anycost_lambda.SumoLogic', return_value=mock_sumo), \
             patch('sumo_anycost_lambda.CloudZero', return_value=mock_cz), \
             patch('sumo_anycost_lambda.BackfillState', return_value=mock_backfill_state), \
             patch('sumo_anycost_lambda.BackfillProgress', return_value=mock_progress), \
             patch('sys.argv', [
                 'sumo_anycost_lambda.py',
                 '--backfill-start', '2025-08-01',
                 '--backfill-end', '2025-08-31'
             ]):

            main()

    def test_main_with_resume_argument(self):
        """Test main function with --resume argument"""
        mock_sumo = Mock()
        mock_cz = Mock()
        mock_backfill_state = Mock()

        # Mock existing state
        mock_existing_state = Mock()
        mock_existing_state.start_date = datetime(2025, 8, 1, tzinfo=timezone.utc).date()
        mock_existing_state.end_date = datetime(2025, 8, 31, tzinfo=timezone.utc).date()
        mock_existing_state.current_date = datetime(2025, 8, 15, tzinfo=timezone.utc).date()
        mock_existing_state.completed_dates = []

        mock_backfill_state.load.return_value = mock_existing_state
        mock_backfill_state.return_value = mock_existing_state

        mock_sumo.get_continuous_logs_cbf_for_date.return_value = []
        mock_sumo.get_frequent_logs_cbf_for_date.return_value = []
        mock_sumo.get_infrequent_logs_cbf_for_date.return_value = []
        mock_sumo.get_infrequent_logs_scanned_cbf_for_date.return_value = []
        mock_sumo.get_metrics_cbf_for_date.return_value = []
        mock_sumo.get_traces_cbf_for_date.return_value = []
        mock_sumo.get_logs_storage_cbf_for_date.return_value = []

        mock_progress = Mock()
        mock_progress.has_more_dates.side_effect = [True, False]
        mock_progress.current_date = datetime(2025, 8, 15, tzinfo=timezone.utc).date()

        mock_existing_state.is_completed.return_value = True

        with patch('sumo_anycost_lambda.SumoLogic', return_value=mock_sumo), \
             patch('sumo_anycost_lambda.CloudZero', return_value=mock_cz), \
             patch('sumo_anycost_lambda.BackfillState', mock_backfill_state), \
             patch('sumo_anycost_lambda.BackfillProgress', return_value=mock_progress), \
             patch('sys.argv', ['sumo_anycost_lambda.py', '--resume']):

            main()

            # Verify state loading was attempted
            assert mock_backfill_state.load.called


class TestDataProcessingOrchestration:
    """Test the orchestration of data processing across all services"""

    def test_all_services_processed_normal_mode(self):
        """Test that all services are processed in normal mode"""
        mock_sumo = Mock()
        mock_cz = Mock()

        # Set up return values for all service methods
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
            getattr(mock_sumo, method).return_value = [{'test': 'data'}]

        mock_cz.post_anycost_stream.return_value = {'success': True}

        lambda_event = {}
        lambda_context = Mock()

        with patch('sumo_anycost_lambda.SumoLogic', return_value=mock_sumo), \
             patch('sumo_anycost_lambda.CloudZero', return_value=mock_cz):

            lambda_handler(lambda_event, lambda_context)

            # Verify all service methods were called
            for method in service_methods:
                assert getattr(mock_sumo, method).called, f"{method} was not called"

            # Verify CloudZero was called for each service type
            assert mock_cz.post_anycost_stream.call_count >= len(service_methods)

    def test_backfill_date_iteration(self):
        """Test that backfill properly iterates through date ranges"""
        mock_sumo = Mock()
        mock_cz = Mock()

        # Mock date-specific methods
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
            getattr(mock_sumo, method).return_value = []

        mock_cz.post_anycost_stream.return_value = {'success': True}

        # Mock backfill state and progress
        mock_backfill_state = Mock()
        mock_progress = Mock()

        # Simulate 3-day backfill
        mock_progress.has_more_dates.side_effect = [True, True, True, False]
        mock_progress.current_date = datetime(2025, 8, 1, tzinfo=timezone.utc).date()

        mock_backfill_state.is_completed.return_value = True

        lambda_event = {}
        lambda_context = Mock()

        with patch('sumo_anycost_lambda.SumoLogic', return_value=mock_sumo), \
             patch('sumo_anycost_lambda.CloudZero', return_value=mock_cz), \
             patch('sumo_anycost_lambda.BackfillState', return_value=mock_backfill_state), \
             patch('sumo_anycost_lambda.BackfillProgress', return_value=mock_progress), \
             patch.dict(os.environ, {
                 'BACKFILL_MODE': 'true',
                 'BACKFILL_START_DATE': '2025-08-01',
                 'BACKFILL_END_DATE': '2025-08-03'
             }):

            lambda_handler(lambda_event, lambda_context)

            # Verify date-specific methods were called multiple times
            for method in date_methods:
                # Should be called once per day for each service
                assert getattr(mock_sumo, method).call_count >= 3, f"{method} not called enough times"

    def test_error_handling_continues_processing(self):
        """Test that error in one service doesn't stop processing of others"""
        mock_sumo = Mock()
        mock_cz = Mock()

        # Make continuous logs fail, but others succeed
        mock_sumo.get_continuous_logs_cbf.side_effect = Exception("API Error")
        mock_sumo.get_frequent_logs_cbf.return_value = []
        mock_sumo.get_infrequent_logs_cbf.return_value = [{'test': 'data'}]
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

            # Should still return success despite one service failing
            assert result['statusCode'] == 200

            # Other services should still be processed
            assert mock_sumo.get_infrequent_logs_cbf.called

    def test_empty_data_handling(self):
        """Test handling of services that return no data"""
        mock_sumo = Mock()
        mock_cz = Mock()

        # All services return empty data
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
            getattr(mock_sumo, method).return_value = []

        # CloudZero should return None for empty data
        mock_cz.post_anycost_stream.return_value = None

        lambda_event = {}
        lambda_context = Mock()

        with patch('sumo_anycost_lambda.SumoLogic', return_value=mock_sumo), \
             patch('sumo_anycost_lambda.CloudZero', return_value=mock_cz):

            result = lambda_handler(lambda_event, lambda_context)

            # Should still succeed with empty data
            assert result['statusCode'] == 200

            # All service methods should still be called
            for method in service_methods:
                assert getattr(mock_sumo, method).called