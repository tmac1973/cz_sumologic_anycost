"""
Unit tests for the CloudZero class.
"""

import pytest
import json
import requests
import os
import tempfile
import shutil
from unittest.mock import Mock, patch, mock_open
from datetime import datetime, timezone

from sumo_anycost_lambda import CloudZero, CZAnycostOp
from tests.fixtures.test_data import CLOUDZERO_API_ERROR, LARGE_CBF_RECORDS


class TestCloudZeroInit:
    """Test CloudZero class initialization"""

    def test_init_with_valid_credentials(self):
        """Test successful initialization with valid credentials"""
        cz = CloudZero('test_auth_key', 'https://api.cloudzero.com', 'test_connection_id')

        assert cz.stream_id == 'test_connection_id'
        assert cz.endpoint == 'https://api.cloudzero.com'
        assert cz.session is not None

    def test_init_sets_headers(self):
        """Test that initialization sets correct headers"""
        cz = CloudZero('test_auth_key', 'https://api.cloudzero.com', 'test_connection_id')

        expected_headers = {
            'Authorization': 'test_auth_key',
            'content-type': 'application/json',
            'accept': 'application/json'
        }

        for key, value in expected_headers.items():
            assert cz.session.headers.get(key) == value


class TestCloudZeroChunking:
    """Test CloudZero data chunking functionality"""

    @pytest.fixture
    def cz_client(self):
        return CloudZero('test_auth_key', 'https://api.cloudzero.com', 'test_connection_id')

    def test_chunk_data_small_payload(self, cz_client, sample_continuous_logs_cbf):
        """Test chunking with small payload that doesn't need chunking"""
        from sumo_anycost_lambda import chunk_data_by_size

        chunks = chunk_data_by_size(sample_continuous_logs_cbf, "replace_drop")

        assert len(chunks) == 1
        assert len(chunks[0]) == len(sample_continuous_logs_cbf)

    def test_chunk_data_large_payload(self, cz_client):
        """Test chunking with large payload that needs chunking"""
        from sumo_anycost_lambda import chunk_data_by_size

        # Use large test data
        chunks = chunk_data_by_size(LARGE_CBF_RECORDS, "replace_drop")

        assert len(chunks) > 1  # Should be split into multiple chunks

        # Verify all records are included
        total_records = sum(len(chunk) for chunk in chunks)
        assert total_records == len(LARGE_CBF_RECORDS)

        # Verify no chunk is too large
        from sumo_anycost_lambda import calculate_payload_size
        for chunk in chunks:
            size = calculate_payload_size(chunk, "replace_drop")
            assert size <= 8 * 1024 * 1024  # 8MB limit

    def test_calculate_payload_size(self, cz_client, sample_continuous_logs_cbf):
        """Test payload size calculation"""
        from sumo_anycost_lambda import calculate_payload_size

        size = calculate_payload_size(sample_continuous_logs_cbf, "replace_drop")

        assert size > 0
        assert isinstance(size, int)

    def test_chunk_data_empty_input(self, cz_client):
        """Test chunking with empty input"""
        from sumo_anycost_lambda import chunk_data_by_size

        chunks = chunk_data_by_size([], "replace_drop")

        assert len(chunks) == 0


class TestCloudZeroNormalMode:
    """Test CloudZero normal mode data transmission"""

    @pytest.fixture
    def cz_client(self):
        return CloudZero('test_auth_key', 'https://api.cloudzero.com', 'test_connection_id')

    def test_post_anycost_stream_success(self, cz_client, sample_continuous_logs_cbf, cloudzero_success_response):
        """Test successful data transmission in normal mode"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = json.dumps(cloudzero_success_response)

        with patch.object(cz_client.session, 'post', return_value=mock_response):
            result = cz_client.post_anycost_stream(
                sample_continuous_logs_cbf,
                CZAnycostOp.REPLACE_DROP,
                service_name='continuous_logs',
                date='2025-09-22'
            )

            assert result is not None

    def test_post_anycost_stream_chunked(self, cz_client, cloudzero_success_response):
        """Test data transmission with chunking required"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = json.dumps(cloudzero_success_response)

        with patch.object(cz_client.session, 'post', return_value=mock_response):
            result = cz_client.post_anycost_stream(
                LARGE_CBF_RECORDS,
                CZAnycostOp.REPLACE_DROP,
                service_name='test_large',
                date='2025-09-22'
            )

            # Should handle chunking without errors
            assert result is not None

    def test_post_anycost_stream_api_error(self, cz_client, sample_continuous_logs_cbf):
        """Test handling of API errors during transmission"""
        mock_response = Mock()
        mock_response.status_code = 400
        mock_response.text = json.dumps(CLOUDZERO_API_ERROR)
        mock_response.raise_for_status.side_effect = requests.HTTPError("400 Client Error")

        with patch.object(cz_client.session, 'post', return_value=mock_response):
            with pytest.raises(Exception):  # Should propagate the error
                cz_client.post_anycost_stream(
                    sample_continuous_logs_cbf,
                    service_name='continuous_logs',
                    date='2025-09-22'
                )

    def test_post_anycost_stream_empty_data(self, cz_client):
        """Test transmission with empty data"""
        result = cz_client.post_anycost_stream(
            [],
            CZAnycostOp.REPLACE_DROP,
            service_name='empty_test',
            date='2025-09-22'
        )

        # Should handle empty data gracefully
        assert result == 'No anycost data to post'


class TestCloudZeroDryRunMode:
    """Test CloudZero dry-run mode functionality"""

    @pytest.fixture
    def cz_client(self):
        return CloudZero('test_auth_key', 'https://api.cloudzero.com', 'test_connection_id')

    @pytest.fixture
    def temp_dir(self):
        """Create temporary directory for dry-run tests"""
        temp_dir = tempfile.mkdtemp()
        yield temp_dir
        shutil.rmtree(temp_dir)

    def test_ensure_dry_run_folder_creates_directory(self, cz_client, temp_dir):
        """Test that dry-run folder is created if it doesn't exist"""
        from sumo_anycost_lambda import ensure_dry_run_folder

        # The function creates a dry_run folder in current working directory
        # Let's test that it creates and returns the correct path
        dry_run_path = ensure_dry_run_folder()

        # Verify it was created and returns the correct path
        assert dry_run_path is not None
        assert os.path.exists(dry_run_path)
        assert os.path.isdir(dry_run_path)
        assert 'dry_run' in dry_run_path

        # Cleanup - remove the directory we created for the test
        if os.path.exists(dry_run_path) and os.listdir(dry_run_path) == []:
            os.rmdir(dry_run_path)

    def test_ensure_dry_run_folder_existing_directory(self, cz_client, temp_dir):
        """Test that existing dry-run folder is not modified"""
        from sumo_anycost_lambda import ensure_dry_run_folder

        # First call to create the directory
        dry_run_path = ensure_dry_run_folder()

        # Create a test file in the directory
        test_file = os.path.join(dry_run_path, 'test.txt')
        with open(test_file, 'w') as f:
            f.write('test content')

        # Second call should not modify existing directory
        dry_run_path_2 = ensure_dry_run_folder()

        # Verify directory still exists and file is unchanged
        assert dry_run_path == dry_run_path_2  # Should return same path
        assert os.path.exists(dry_run_path)
        assert os.path.exists(test_file)
        with open(test_file, 'r') as f:
            assert f.read() == 'test content'

        # Cleanup
        if os.path.exists(test_file):
            os.remove(test_file)
        if os.path.exists(dry_run_path) and os.listdir(dry_run_path) == []:
            os.rmdir(dry_run_path)

    def test_write_dry_run_data(self, cz_client, sample_continuous_logs_cbf, temp_dir):
        """Test writing dry-run data to JSON file"""
        from sumo_anycost_lambda import write_dry_run_data

        # The function creates files in current working dir + dry_run
        # Let's test it creates the file and returns the correct path
        filepath = write_dry_run_data(
            sample_continuous_logs_cbf,
            "replace_drop",  # operation parameter
            'continuous_logs',  # service_name parameter
            '2025-09-22'  # date parameter
        )

        # Verify file was created (function should return the full path)
        assert filepath is not None
        assert os.path.exists(filepath)
        assert '2025-09-22_continuous_logs.json' in filepath  # Actual filename format
        assert 'dry_run' in filepath  # Should be in dry_run directory

        # Verify file content structure
        with open(filepath, 'r') as f:
            data = json.load(f)
            assert 'operation' in data
            assert 'data' in data
            assert data['operation'] == 'replace_drop'
            assert data['data'] == sample_continuous_logs_cbf

        # Cleanup
        if os.path.exists(filepath):
            os.remove(filepath)
        # Also cleanup the dry_run directory if it was created for this test
        dry_run_dir = os.path.dirname(filepath)
        if os.path.exists(dry_run_dir) and os.listdir(dry_run_dir) == []:
            os.rmdir(dry_run_dir)

    def test_write_dry_run_data_multiple_services(self, cz_client, sample_continuous_logs_cbf, sample_metrics_cbf, temp_dir):
        """Test writing dry-run data for multiple services"""
        from sumo_anycost_lambda import write_dry_run_data

        # Write data for multiple services
        continuous_filepath = write_dry_run_data(sample_continuous_logs_cbf, "replace_drop", 'continuous_logs', '2025-09-22')
        metrics_filepath = write_dry_run_data(sample_metrics_cbf, "replace_drop", 'metrics', '2025-09-22')

        # Verify both files were created
        assert os.path.exists(continuous_filepath)
        assert os.path.exists(metrics_filepath)
        assert '2025-09-22_continuous_logs.json' in continuous_filepath
        assert '2025-09-22_metrics.json' in metrics_filepath

        # Verify content structure
        with open(continuous_filepath, 'r') as f:
            data = json.load(f)
            assert data['data'] == sample_continuous_logs_cbf

        with open(metrics_filepath, 'r') as f:
            data = json.load(f)
            assert data['data'] == sample_metrics_cbf

        # Cleanup
        for filepath in [continuous_filepath, metrics_filepath]:
            if os.path.exists(filepath):
                os.remove(filepath)
        # Also cleanup the dry_run directory if it was created for this test
        dry_run_dir = os.path.dirname(continuous_filepath)
        if os.path.exists(dry_run_dir) and os.listdir(dry_run_dir) == []:
            os.rmdir(dry_run_dir)

    def test_post_anycost_stream_dry_run_mode(self, cz_client, sample_continuous_logs_cbf, temp_dir):
        """Test that dry-run mode writes files instead of making API calls"""

        with patch('sumo_anycost_lambda.DRY_RUN_MODE', True), \
             patch('sumo_anycost_lambda.write_dry_run_data', return_value='/fake/path.json') as mock_write:

            result = cz_client.post_anycost_stream(
                sample_continuous_logs_cbf,
                CZAnycostOp.REPLACE_DROP,
                service_name='continuous_logs',
                date='2025-09-22'
            )

            # Verify dry-run write function was called
            mock_write.assert_called_once_with(
                sample_continuous_logs_cbf,
                "replace_drop",
                'continuous_logs',
                '2025-09-22'
            )

            # Should return dry-run result, not None
            assert result is not None
            assert result.get('dry_run') == True

    def test_write_dry_run_data_empty_data(self, cz_client, temp_dir):
        """Test writing empty data in dry-run mode"""
        from sumo_anycost_lambda import write_dry_run_data

        filepath = write_dry_run_data([], "replace_drop", 'empty_service', '2025-09-22')

        # File should still be created with empty array in data field
        assert os.path.exists(filepath)
        assert '2025-09-22_empty_service.json' in filepath

        with open(filepath, 'r') as f:
            data = json.load(f)
            assert 'data' in data
            assert data['data'] == []  # The empty array should be in the data field
            assert data['operation'] == 'replace_drop'

        # Cleanup
        if os.path.exists(filepath):
            os.remove(filepath)
        # Also cleanup the dry_run directory if it was created for this test
        dry_run_dir = os.path.dirname(filepath)
        if os.path.exists(dry_run_dir) and os.listdir(dry_run_dir) == []:
            os.rmdir(dry_run_dir)


class TestCloudZeroHelperMethods:
    """Test CloudZero helper methods"""

    @pytest.fixture
    def cz_client(self):
        return CloudZero('test_auth_key', 'https://api.cloudzero.com', 'test_connection_id')

    def test_handle_normal_mode(self, cz_client, sample_continuous_logs_cbf, cloudzero_success_response):
        """Test _handle_normal_mode method"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = json.dumps(cloudzero_success_response)

        chunks = [sample_continuous_logs_cbf]  # Single chunk

        with patch.object(cz_client.session, 'post', return_value=mock_response):
            result = cz_client._handle_normal_mode(chunks)

            assert result is not None

    def test_handle_dry_run_mode(self, cz_client, sample_continuous_logs_cbf, temp_dir):
        """Test _handle_dry_run_mode method"""
        dry_run_path = os.path.join(temp_dir, 'dry_run')
        os.makedirs(dry_run_path)

        chunks = [sample_continuous_logs_cbf]

        with patch('sumo_anycost_lambda.write_dry_run_data') as mock_write:
            cz_client._handle_dry_run_mode(
                chunks,
                service_name='test_service',
                date='2025-09-22'
            )

            mock_write.assert_called_once()


class TestCloudZeroIntegration:
    """Integration tests for CloudZero functionality"""

    @pytest.fixture
    def cz_client(self):
        return CloudZero('test_auth_key', 'https://api.cloudzero.com', 'test_connection_id')

    def test_full_workflow_normal_mode(self, cz_client, sample_continuous_logs_cbf, cloudzero_success_response):
        """Test complete workflow in normal mode"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = json.dumps(cloudzero_success_response)

        with patch.object(cz_client.session, 'post', return_value=mock_response) as mock_post:
            result = cz_client.post_anycost_stream(
                sample_continuous_logs_cbf,
                CZAnycostOp.REPLACE_DROP,
                service_name='continuous_logs',
                date='2025-09-22'
            )

            # Verify API was called
            mock_post.assert_called()
            assert result is not None

    @patch.dict(os.environ, {'DRY_RUN_MODE': 'true'})
    def test_full_workflow_dry_run_mode(self, cz_client, sample_continuous_logs_cbf, temp_dir):
        """Test complete workflow in dry-run mode"""
        dry_run_path = os.path.join(temp_dir, 'dry_run')

        with patch('sumo_anycost_lambda.ensure_dry_run_folder') as mock_ensure, \
             patch('sumo_anycost_lambda.write_dry_run_data') as mock_write:

            result = cz_client.post_anycost_stream(
                sample_continuous_logs_cbf,
                CZAnycostOp.REPLACE_DROP,
                service_name='continuous_logs',
                date='2025-09-22'
            )

            # Verify dry-run workflow was followed
            mock_ensure.assert_called_once()
            mock_write.assert_called_once()
            assert result is None

    def test_error_recovery(self, cz_client, sample_continuous_logs_cbf):
        """Test error recovery in normal mode"""
        # First call fails, second succeeds
        error_response = Mock()
        error_response.status_code = 500
        error_response.raise_for_status.side_effect = requests.HTTPError("500 Server Error")

        success_response = Mock()
        success_response.status_code = 200
        success_response.text = '{"success": true}'

        with patch.object(cz_client.session, 'post', side_effect=[error_response, success_response]):
            # Should handle the first error and potentially retry
            with pytest.raises(Exception):  # First call should raise error
                cz_client.post_anycost_stream(
                    sample_continuous_logs_cbf,
                    service_name='continuous_logs',
                    date='2025-09-22'
                )