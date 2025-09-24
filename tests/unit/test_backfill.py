"""
Unit tests for backfill management functionality.
"""

import pytest
import json
import os
import tempfile
import shutil
from datetime import datetime, timezone, timedelta
from unittest.mock import Mock, patch, mock_open

from sumo_anycost_lambda import BackfillState, BackfillProgress


class TestBackfillState:
    """Test BackfillState class functionality"""

    @pytest.fixture
    def temp_dir(self):
        """Create temporary directory for test files"""
        temp_dir = tempfile.mkdtemp()
        yield temp_dir
        shutil.rmtree(temp_dir)

    @pytest.fixture
    def sample_state_data(self):
        """Sample backfill state data"""
        return {
            'start_date': '2025-08-01',
            'end_date': '2025-08-31',
            'current_date': '2025-08-15',
            'completed_dates': ['2025-08-01', '2025-08-02', '2025-08-03'],
            'failed_dates': [],
            'total_records_uploaded': 15000,
            'last_updated': '2025-08-15T12:00:00Z'
        }

    def test_create_new_state(self, temp_dir):
        """Test creating a new backfill state"""
        state_file = os.path.join(temp_dir, '.backfill_state_test.json')
        start_date = datetime(2025, 8, 1, tzinfo=timezone.utc).date()
        end_date = datetime(2025, 8, 31, tzinfo=timezone.utc).date()

        state = BackfillState(datetime.combine(start_date, datetime.min.time().replace(tzinfo=timezone.utc)),
                             datetime.combine(end_date, datetime.min.time().replace(tzinfo=timezone.utc)))

        assert state.backfill_start.date() == start_date
        assert state.backfill_end.date() == end_date

    def test_load_existing_state(self, temp_dir):
        """Test loading an existing backfill state"""
        start_date = datetime(2025, 8, 1, tzinfo=timezone.utc)
        end_date = datetime(2025, 8, 31, tzinfo=timezone.utc)

        # Create state instance and manually create state file
        state = BackfillState(start_date, end_date)
        state_file = state.state_file

        # Create state data matching the actual format
        state_data = {
            'backfill_start': start_date.isoformat(),
            'backfill_end': end_date.isoformat(),
            'completed_days': ['2025-08-01', '2025-08-02', '2025-08-03'],
            'last_completed_day': '2025-08-03',
            'start_time': datetime.now().isoformat(),
            'is_dry_run': False,
            'last_updated': datetime.now().isoformat()
        }

        # Write state file
        with open(state_file, 'w') as f:
            json.dump(state_data, f)

        # Test loading
        loaded = state.load_existing_state()

        assert loaded == True
        assert state.backfill_start == start_date
        assert state.backfill_end == end_date
        assert len(state.completed_days) == 3
        assert '2025-08-01' in state.completed_days
        assert state.last_completed_day == '2025-08-03'

        # Cleanup
        if os.path.exists(state_file):
            os.remove(state_file)

    def test_save_state(self, temp_dir):
        """Test saving backfill state to file"""
        start_date = datetime(2025, 8, 1, tzinfo=timezone.utc)
        end_date = datetime(2025, 8, 31, tzinfo=timezone.utc)

        # Mock DRY_RUN_MODE to be False so state actually saves
        with patch('sumo_anycost_lambda.DRY_RUN_MODE', False):
            state = BackfillState(start_date, end_date)
            state.completed_days.add('2025-08-01')
            state.last_completed_day = '2025-08-01'
            state.start_time = datetime(2025, 8, 1, 12, 0, tzinfo=timezone.utc)

            state.save_state()

            # Verify file was created and contains correct data
            assert os.path.exists(state.state_file)
            with open(state.state_file, 'r') as f:
                data = json.load(f)
                assert data['backfill_start'] == start_date.isoformat()
                assert data['backfill_end'] == end_date.isoformat()
                assert '2025-08-01' in data['completed_days']
                assert data['last_completed_day'] == '2025-08-01'

            # Cleanup
            if os.path.exists(state.state_file):
                os.remove(state.state_file)

    def test_mark_date_completed(self, temp_dir):
        """Test marking a date as completed"""
        start_date = datetime(2025, 8, 1, tzinfo=timezone.utc)
        end_date = datetime(2025, 8, 5, tzinfo=timezone.utc)

        with patch('sumo_anycost_lambda.DRY_RUN_MODE', False):
            state = BackfillState(start_date, end_date)
            completion_date = '2025-08-01'
            day_stats = {'failed_services': 0, 'successful_services': 3}

            state.mark_day_completed(completion_date, day_stats)

            assert completion_date in state.completed_days
            assert state.last_completed_day == completion_date

            # Cleanup
            if os.path.exists(state.state_file):
                os.remove(state.state_file)

    def test_mark_date_failed(self, temp_dir):
        """Test that a date with failed services is not marked as completed"""
        start_date = datetime(2025, 8, 1, tzinfo=timezone.utc)
        end_date = datetime(2025, 8, 5, tzinfo=timezone.utc)

        with patch('sumo_anycost_lambda.DRY_RUN_MODE', False):
            state = BackfillState(start_date, end_date)
            failed_date = '2025-08-01'
            day_stats = {'failed_services': 2, 'successful_services': 1}  # Has failures

            state.mark_day_completed(failed_date, day_stats)

            # Date should NOT be marked as completed due to failures
            assert failed_date not in state.completed_days
            assert state.last_completed_day != failed_date

            # Cleanup
            if os.path.exists(state.state_file):
                os.remove(state.state_file)

    def test_is_completed(self, temp_dir):
        """Test checking if backfill is completed"""
        start_date = datetime(2025, 8, 1, tzinfo=timezone.utc)
        end_date = datetime(2025, 8, 3, tzinfo=timezone.utc)  # 3-day range

        with patch('sumo_anycost_lambda.DRY_RUN_MODE', False):
            state = BackfillState(start_date, end_date)

            # Not completed initially
            assert not state.is_backfill_complete()

            # Mark all dates as completed
            state.mark_day_completed('2025-08-01', {'failed_services': 0})
            state.mark_day_completed('2025-08-02', {'failed_services': 0})
            state.mark_day_completed('2025-08-03', {'failed_services': 0})

            # Should be completed now
            assert state.is_backfill_complete()

            # Cleanup
            if os.path.exists(state.state_file):
                os.remove(state.state_file)

    def test_cleanup_state_file(self, temp_dir):
        """Test cleaning up state file after completion"""
        start_date = datetime(2025, 8, 1, tzinfo=timezone.utc)
        end_date = datetime(2025, 8, 1, tzinfo=timezone.utc)  # Single day

        with patch('sumo_anycost_lambda.DRY_RUN_MODE', False):
            state = BackfillState(start_date, end_date)
            state.save_state()  # Create the file

            assert os.path.exists(state.state_file)

            state.cleanup_state_file()

            assert not os.path.exists(state.state_file)

    def test_get_progress_percentage(self, temp_dir):
        """Test calculating progress percentage"""
        start_date = datetime(2025, 8, 1, tzinfo=timezone.utc)
        end_date = datetime(2025, 8, 10, tzinfo=timezone.utc)  # 10-day range

        with patch('sumo_anycost_lambda.DRY_RUN_MODE', False):
            state = BackfillState(start_date, end_date)

            # No progress initially
            progress = state.get_progress_summary()
            assert progress['completion_percentage'] == 0.0
            assert progress['total_days'] == 10
            assert progress['completed_days'] == 0

            # Complete half the dates
            for i in range(5):
                date = f"2025-08-{i+1:02d}"
                state.mark_day_completed(date, {'failed_services': 0})

            progress = state.get_progress_summary()
            assert progress['completion_percentage'] == 50.0
            assert progress['completed_days'] == 5

            # Cleanup
            if os.path.exists(state.state_file):
                os.remove(state.state_file)

    def test_load_nonexistent_file(self, temp_dir):
        """Test loading a non-existent state file"""
        start_date = datetime(2025, 8, 1, tzinfo=timezone.utc)
        end_date = datetime(2025, 8, 5, tzinfo=timezone.utc)

        state = BackfillState(start_date, end_date)
        # Remove the state file to simulate non-existent
        if os.path.exists(state.state_file):
            os.remove(state.state_file)

        result = state.load_existing_state()

        assert result == False  # Should return False for non-existent file

    def test_load_corrupted_file(self, temp_dir):
        """Test loading a corrupted state file"""
        start_date = datetime(2025, 8, 1, tzinfo=timezone.utc)
        end_date = datetime(2025, 8, 5, tzinfo=timezone.utc)

        state = BackfillState(start_date, end_date)
        # Create corrupted JSON file
        with open(state.state_file, 'w') as f:
            f.write('invalid json content')

        result = state.load_existing_state()

        assert result == False  # Should return False for corrupted file

        # Cleanup
        if os.path.exists(state.state_file):
            os.remove(state.state_file)


class TestBackfillProgress:
    """Test BackfillProgress class functionality"""

    def test_create_progress_tracker(self):
        """Test creating a backfill progress tracker"""
        total_days = 5

        progress = BackfillProgress(total_days)

        assert progress.total_days == total_days
        assert progress.current_day == 0
        assert hasattr(progress, 'start_time')
        assert hasattr(progress, 'daily_stats')

    def test_get_date_range(self):
        """Test getting the complete date range"""
        start_date = datetime(2025, 8, 1, tzinfo=timezone.utc).date()
        end_date = datetime(2025, 8, 5, tzinfo=timezone.utc).date()

        progress = BackfillProgress(start_date, end_date)
        date_range = progress.get_date_range()

        expected_dates = [
            datetime(2025, 8, 1, tzinfo=timezone.utc).date(),
            datetime(2025, 8, 2, tzinfo=timezone.utc).date(),
            datetime(2025, 8, 3, tzinfo=timezone.utc).date(),
            datetime(2025, 8, 4, tzinfo=timezone.utc).date(),
            datetime(2025, 8, 5, tzinfo=timezone.utc).date(),
        ]

        assert date_range == expected_dates

    def test_get_remaining_dates(self):
        """Test getting remaining dates to process"""
        start_date = datetime(2025, 8, 1, tzinfo=timezone.utc).date()
        end_date = datetime(2025, 8, 5, tzinfo=timezone.utc).date()

        progress = BackfillProgress(start_date, end_date)
        completed_dates = [
            datetime(2025, 8, 1, tzinfo=timezone.utc).date(),
            datetime(2025, 8, 2, tzinfo=timezone.utc).date(),
        ]

        remaining = progress.get_remaining_dates(completed_dates)

        expected_remaining = [
            datetime(2025, 8, 3, tzinfo=timezone.utc).date(),
            datetime(2025, 8, 4, tzinfo=timezone.utc).date(),
            datetime(2025, 8, 5, tzinfo=timezone.utc).date(),
        ]

        assert remaining == expected_remaining

    def test_next_date(self):
        """Test getting the next date to process"""
        start_date = datetime(2025, 8, 1, tzinfo=timezone.utc).date()
        end_date = datetime(2025, 8, 5, tzinfo=timezone.utc).date()

        progress = BackfillProgress(start_date, end_date)

        # First date
        assert progress.current_date == start_date

        # Move to next date
        next_date = progress.next_date()
        assert next_date == datetime(2025, 8, 2, tzinfo=timezone.utc).date()
        assert progress.current_date == next_date

    def test_has_more_dates(self):
        """Test checking if there are more dates to process"""
        start_date = datetime(2025, 8, 1, tzinfo=timezone.utc).date()
        end_date = datetime(2025, 8, 2, tzinfo=timezone.utc).date()  # 2-day range

        progress = BackfillProgress(start_date, end_date)

        # Should have more dates initially
        assert progress.has_more_dates()

        # Move to next date
        progress.next_date()
        assert progress.has_more_dates()

        # Move past end date
        progress.next_date()
        assert not progress.has_more_dates()

    def test_reset_to_date(self):
        """Test resetting progress to a specific date"""
        start_date = datetime(2025, 8, 1, tzinfo=timezone.utc).date()
        end_date = datetime(2025, 8, 5, tzinfo=timezone.utc).date()

        progress = BackfillProgress(start_date, end_date)

        # Move forward
        progress.next_date()
        progress.next_date()

        # Reset to specific date
        reset_date = datetime(2025, 8, 4, tzinfo=timezone.utc).date()
        progress.reset_to_date(reset_date)

        assert progress.current_date == reset_date

    def test_single_date_range(self):
        """Test progress with single date range"""
        single_date = datetime(2025, 8, 1, tzinfo=timezone.utc).date()

        progress = BackfillProgress(single_date, single_date)

        date_range = progress.get_date_range()
        assert len(date_range) == 1
        assert date_range[0] == single_date

        assert progress.has_more_dates()
        progress.next_date()
        assert not progress.has_more_dates()

    def test_invalid_date_range(self):
        """Test error handling for invalid date range"""
        start_date = datetime(2025, 8, 5, tzinfo=timezone.utc).date()
        end_date = datetime(2025, 8, 1, tzinfo=timezone.utc).date()  # End before start

        with pytest.raises(ValueError):
            BackfillProgress(start_date, end_date)


class TestBackfillIntegration:
    """Integration tests for backfill functionality"""

    @pytest.fixture
    def temp_dir(self):
        """Create temporary directory for test files"""
        temp_dir = tempfile.mkdtemp()
        yield temp_dir
        shutil.rmtree(temp_dir)

    def test_complete_backfill_workflow(self, temp_dir):
        """Test a complete backfill workflow"""
        state_file = os.path.join(temp_dir, '.backfill_state_test.json')
        start_date = datetime(2025, 8, 1, tzinfo=timezone.utc).date()
        end_date = datetime(2025, 8, 3, tzinfo=timezone.utc).date()

        # Create initial state
        state = BackfillState(state_file, start_date, end_date)
        progress = BackfillProgress(start_date, end_date)

        # Process each date
        total_records = 0
        while progress.has_more_dates():
            current_date = progress.current_date

            # Simulate processing (would normally call SumoLogic/CloudZero here)
            records_processed = 1000  # Simulate 1000 records per day

            # Mark as completed
            state.mark_date_completed(current_date, records_processed)
            total_records += records_processed

            # Save state
            state.save()

            # Move to next date
            progress.next_date()

        # Verify completion
        assert state.is_completed()
        assert state.total_records_uploaded == total_records
        assert len(state.completed_dates) == 3

        # Cleanup
        state.cleanup()
        assert not os.path.exists(state_file)

    def test_resume_interrupted_backfill(self, temp_dir, sample_state_data):
        """Test resuming an interrupted backfill"""
        state_file = os.path.join(temp_dir, '.backfill_state_test.json')

        # Create partially completed state
        with open(state_file, 'w') as f:
            json.dump(sample_state_data, f)

        # Load existing state
        state = BackfillState.load(state_file)
        assert state is not None

        # Create progress tracker from existing state
        progress = BackfillProgress(state.start_date, state.end_date)
        progress.reset_to_date(state.current_date)

        # Get remaining dates
        remaining_dates = progress.get_remaining_dates(state.completed_dates)

        assert len(remaining_dates) > 0  # Should have dates to process
        assert len(state.completed_dates) == 3  # Already completed some dates

    def test_backfill_with_failures(self, temp_dir):
        """Test backfill workflow with some failures"""
        state_file = os.path.join(temp_dir, '.backfill_state_test.json')
        start_date = datetime(2025, 8, 1, tzinfo=timezone.utc).date()
        end_date = datetime(2025, 8, 5, tzinfo=timezone.utc).date()

        state = BackfillState(state_file, start_date, end_date)
        progress = BackfillProgress(start_date, end_date)

        # Process with some failures
        while progress.has_more_dates():
            current_date = progress.current_date

            # Simulate failure for certain dates
            if current_date.day % 2 == 0:  # Fail on even days
                state.mark_date_failed(current_date, "Simulated API error")
            else:
                state.mark_date_completed(current_date, 1000)

            state.save()
            progress.next_date()

        # Verify results
        assert len(state.completed_dates) == 3  # Days 1, 3, 5
        assert len(state.failed_dates) == 2    # Days 2, 4
        assert not state.is_completed()  # Not completed due to failures

    def test_state_file_naming_convention(self, temp_dir):
        """Test state file naming follows expected convention"""
        start_date = datetime(2025, 8, 1, tzinfo=timezone.utc).date()
        end_date = datetime(2025, 8, 31, tzinfo=timezone.utc).date()

        expected_filename = '.backfill_state_20250801_to_20250831.json'
        actual_filename = BackfillState.generate_filename(start_date, end_date)

        assert actual_filename == expected_filename

    def test_concurrent_backfill_protection(self, temp_dir):
        """Test protection against concurrent backfill operations"""
        state_file = os.path.join(temp_dir, '.backfill_state_test.json')
        start_date = datetime(2025, 8, 1, tzinfo=timezone.utc).date()
        end_date = datetime(2025, 8, 5, tzinfo=timezone.utc).date()

        # Create first backfill state
        state1 = BackfillState(state_file, start_date, end_date)
        state1.save()

        # Attempt to create second backfill for same date range
        existing_state = BackfillState.load(state_file)

        assert existing_state is not None
        assert existing_state.start_date == start_date
        assert existing_state.end_date == end_date