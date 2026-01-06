"""Tests for data transfer module."""

import pytest
from unittest.mock import Mock, MagicMock, call
from db2pgpy.data_transfer import DataTransfer


class TestDataTransfer:
    """Test DataTransfer functionality."""

    def test_transfer_table_transfers_data_in_batches(self):
        """Test transfer_table fetches and inserts data in batches."""
        # Mock connectors
        mock_db2 = Mock()
        mock_pg = Mock()
        
        # Mock DB2 batch fetching
        mock_db2.fetch_table_data.return_value = iter([
            [{'id': 1, 'name': 'Alice'}, {'id': 2, 'name': 'Bob'}],
            [{'id': 3, 'name': 'Charlie'}]
        ])
        
        # Mock progress tracker
        mock_progress = Mock()
        
        transfer = DataTransfer(mock_db2, mock_pg, batch_size=2)
        result = transfer.transfer_table('users', mock_progress)
        
        # Verify DB2 fetch was called
        mock_db2.fetch_table_data.assert_called_once_with('users', 2)
        
        # Verify PostgreSQL inserts (2 batches)
        assert mock_pg.bulk_insert.call_count == 2
        
        # Verify progress updates
        assert mock_progress.update_progress.call_count >= 2
        
        # Verify result
        assert result['rows_transferred'] == 3
        assert 'time_taken' in result

    def test_transfer_table_handles_empty_table(self):
        """Test transfer_table handles tables with no data."""
        mock_db2 = Mock()
        mock_pg = Mock()
        mock_db2.fetch_table_data.return_value = iter([])
        
        mock_progress = Mock()
        
        transfer = DataTransfer(mock_db2, mock_pg, batch_size=100)
        result = transfer.transfer_table('empty_table', mock_progress)
        
        assert result['rows_transferred'] == 0
        mock_pg.bulk_insert.assert_not_called()

    def test_transfer_table_updates_progress_tracker(self):
        """Test transfer_table updates progress tracker correctly."""
        mock_db2 = Mock()
        mock_pg = Mock()
        
        mock_db2.fetch_table_data.return_value = iter([
            [{'id': 1}, {'id': 2}, {'id': 3}]
        ])
        
        mock_progress = Mock()
        
        transfer = DataTransfer(mock_db2, mock_pg, batch_size=10)
        transfer.transfer_table('test_table', mock_progress)
        
        # Progress should be updated
        mock_progress.update_progress.assert_called()
        call_args = mock_progress.update_progress.call_args
        assert call_args[0][0] == 'test_table'
        assert call_args[1]['rows_transferred'] == 3

    def test_transfer_tables_processes_multiple_tables(self):
        """Test transfer_tables processes a list of tables."""
        mock_db2 = Mock()
        mock_pg = Mock()
        
        # Mock different data for each table
        def fetch_side_effect(table_name, batch_size):
            if table_name == 'users':
                return iter([[{'id': 1}]])
            elif table_name == 'orders':
                return iter([[{'id': 1}, {'id': 2}]])
            return iter([])
        
        mock_db2.fetch_table_data.side_effect = fetch_side_effect
        
        mock_progress = Mock()
        
        transfer = DataTransfer(mock_db2, mock_pg, batch_size=10)
        results = transfer.transfer_tables(['users', 'orders'], mock_progress)
        
        # Verify both tables were processed
        assert len(results) == 2
        assert 'users' in results
        assert 'orders' in results
        assert results['users']['rows_transferred'] == 1
        assert results['orders']['rows_transferred'] == 2

    def test_transfer_table_respects_batch_size(self):
        """Test transfer uses configured batch size."""
        mock_db2 = Mock()
        mock_pg = Mock()
        mock_db2.fetch_table_data.return_value = iter([[{'id': 1}]])
        
        mock_progress = Mock()
        
        transfer = DataTransfer(mock_db2, mock_pg, batch_size=500)
        transfer.transfer_table('test', mock_progress)
        
        # Verify batch size was passed to fetch
        mock_db2.fetch_table_data.assert_called_with('test', 500)

    def test_transfer_table_returns_statistics(self):
        """Test transfer_table returns complete statistics."""
        mock_db2 = Mock()
        mock_pg = Mock()
        mock_db2.fetch_table_data.return_value = iter([[{'id': 1}]])
        
        mock_progress = Mock()
        
        transfer = DataTransfer(mock_db2, mock_pg, batch_size=10)
        result = transfer.transfer_table('test', mock_progress)
        
        assert 'rows_transferred' in result
        assert 'time_taken' in result
        assert isinstance(result['rows_transferred'], int)
        assert isinstance(result['time_taken'], (int, float))
