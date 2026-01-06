"""Tests for migrator orchestration module."""

import pytest
from unittest.mock import Mock, MagicMock, call
from db2pgpy.migrator import Migrator


class TestMigrator:
    """Test Migrator orchestration functionality."""

    def test_run_migration_full_mode_executes_all_phases(self):
        """Test run_migration in full mode executes all phases."""
        # Mock all dependencies
        mock_schema_extractor = Mock()
        mock_schema_converter = Mock()
        mock_data_transfer = Mock()
        mock_validator = Mock()
        mock_db2_conn = Mock()
        mock_pg_conn = Mock()
        mock_progress = Mock()
        mock_logger = Mock()
        
        # Mock schema extraction
        mock_schema_extractor.extract_table_schema.return_value = {
            'columns': [{'name': 'id', 'type': 'INTEGER', 'nullable': False}]
        }
        mock_schema_extractor.extract_primary_keys.return_value = ['id']
        mock_schema_extractor.extract_foreign_keys.return_value = []
        mock_schema_extractor.extract_indexes.return_value = []
        
        # Mock schema conversion
        mock_schema_converter.generate_create_table_ddl.return_value = 'CREATE TABLE test...'
        
        # Mock data transfer
        mock_data_transfer.transfer_table.return_value = {'rows_transferred': 100, 'time_taken': 1.5}
        
        # Mock validation
        mock_validator.validate_row_counts.return_value = (True, 100, 100)
        
        migrator = Migrator(
            schema_extractor=mock_schema_extractor,
            schema_converter=mock_schema_converter,
            data_transfer=mock_data_transfer,
            validator=mock_validator,
            db2_connector=mock_db2_conn,
            pg_connector=mock_pg_conn,
            progress_tracker=mock_progress,
            logger=mock_logger
        )
        
        config = {
            'tables': ['test_table']
        }
        
        result = migrator.run_migration(config, mode='full')
        
        # Verify all phases were executed
        mock_schema_extractor.extract_table_schema.assert_called()
        mock_schema_converter.generate_create_table_ddl.assert_called()
        mock_pg_conn.execute_ddl.assert_called()
        mock_data_transfer.transfer_table.assert_called()
        mock_validator.validate_row_counts.assert_called()
        
        assert result['status'] == 'completed'

    def test_run_migration_schema_only_mode_skips_data_transfer(self):
        """Test run_migration in schema_only mode skips data transfer."""
        mock_schema_extractor = Mock()
        mock_schema_converter = Mock()
        mock_data_transfer = Mock()
        mock_validator = Mock()
        mock_db2_conn = Mock()
        mock_pg_conn = Mock()
        mock_progress = Mock()
        mock_logger = Mock()
        
        mock_schema_extractor.extract_table_schema.return_value = {'columns': []}
        mock_schema_extractor.extract_primary_keys.return_value = []
        mock_schema_extractor.extract_foreign_keys.return_value = []
        mock_schema_extractor.extract_indexes.return_value = []
        
        mock_schema_converter.generate_create_table_ddl.return_value = 'CREATE TABLE...'
        
        migrator = Migrator(
            schema_extractor=mock_schema_extractor,
            schema_converter=mock_schema_converter,
            data_transfer=mock_data_transfer,
            validator=mock_validator,
            db2_connector=mock_db2_conn,
            pg_connector=mock_pg_conn,
            progress_tracker=mock_progress,
            logger=mock_logger
        )
        
        config = {
            'tables': ['test_table']
        }
        
        result = migrator.run_migration(config, mode='schema_only')
        
        # Data transfer should NOT be called
        mock_data_transfer.transfer_table.assert_not_called()
        
        # Schema operations should be called
        mock_schema_converter.generate_create_table_ddl.assert_called()

    def test_run_migration_handles_multiple_tables(self):
        """Test run_migration processes multiple tables."""
        mock_schema_extractor = Mock()
        mock_schema_converter = Mock()
        mock_data_transfer = Mock()
        mock_validator = Mock()
        mock_db2_conn = Mock()
        mock_pg_conn = Mock()
        mock_progress = Mock()
        mock_logger = Mock()
        
        mock_schema_extractor.extract_table_schema.return_value = {'columns': []}
        mock_schema_extractor.extract_primary_keys.return_value = []
        mock_schema_extractor.extract_foreign_keys.return_value = []
        mock_schema_extractor.extract_indexes.return_value = []
        
        mock_schema_converter.generate_create_table_ddl.return_value = 'CREATE TABLE...'
        mock_data_transfer.transfer_table.return_value = {'rows_transferred': 0, 'time_taken': 0}
        mock_validator.validate_row_counts.return_value = (True, 0, 0)
        
        migrator = Migrator(
            schema_extractor=mock_schema_extractor,
            schema_converter=mock_schema_converter,
            data_transfer=mock_data_transfer,
            validator=mock_validator,
            db2_connector=mock_db2_conn,
            pg_connector=mock_pg_conn,
            progress_tracker=mock_progress,
            logger=mock_logger
        )
        
        config = {
            'tables': ['table1', 'table2', 'table3']
        }
        
        result = migrator.run_migration(config, mode='full')
        
        # Should process all 3 tables
        assert mock_schema_extractor.extract_table_schema.call_count == 3
        assert mock_data_transfer.transfer_table.call_count == 3

    def test_run_migration_logs_each_phase(self):
        """Test run_migration logs progress through each phase."""
        mock_schema_extractor = Mock()
        mock_schema_converter = Mock()
        mock_data_transfer = Mock()
        mock_validator = Mock()
        mock_db2_conn = Mock()
        mock_pg_conn = Mock()
        mock_progress = Mock()
        mock_logger = Mock()
        
        mock_schema_extractor.extract_table_schema.return_value = {'columns': []}
        mock_schema_extractor.extract_primary_keys.return_value = []
        mock_schema_extractor.extract_foreign_keys.return_value = []
        mock_schema_extractor.extract_indexes.return_value = []
        
        mock_schema_converter.generate_create_table_ddl.return_value = 'CREATE...'
        mock_data_transfer.transfer_table.return_value = {'rows_transferred': 0, 'time_taken': 0}
        
        migrator = Migrator(
            schema_extractor=mock_schema_extractor,
            schema_converter=mock_schema_converter,
            data_transfer=mock_data_transfer,
            validator=mock_validator,
            db2_connector=mock_db2_conn,
            pg_connector=mock_pg_conn,
            progress_tracker=mock_progress,
            logger=mock_logger
        )
        
        config = {'tables': ['test']}
        migrator.run_migration(config, mode='full')
        
        # Logger should be called multiple times for different phases
        assert mock_logger.info.call_count >= 3

    def test_run_migration_validation_disabled(self):
        """Test run_migration skips validation when disabled."""
        mock_schema_extractor = Mock()
        mock_schema_converter = Mock()
        mock_data_transfer = Mock()
        mock_validator = Mock()
        mock_db2_conn = Mock()
        mock_pg_conn = Mock()
        mock_progress = Mock()
        mock_logger = Mock()
        
        mock_schema_extractor.extract_table_schema.return_value = {'columns': []}
        mock_schema_extractor.extract_primary_keys.return_value = []
        mock_schema_extractor.extract_foreign_keys.return_value = []
        mock_schema_extractor.extract_indexes.return_value = []
        
        mock_schema_converter.generate_create_table_ddl.return_value = 'CREATE...'
        mock_data_transfer.transfer_table.return_value = {'rows_transferred': 0, 'time_taken': 0}
        
        migrator = Migrator(
            schema_extractor=mock_schema_extractor,
            schema_converter=mock_schema_converter,
            data_transfer=mock_data_transfer,
            validator=mock_validator,
            db2_connector=mock_db2_conn,
            pg_connector=mock_pg_conn,
            progress_tracker=mock_progress,
            logger=mock_logger
        )
        
        config = {
            'tables': ['test'],
            'validate': False
        }
        
        migrator.run_migration(config, mode='full')
        
        # Validator should not be called
        mock_validator.validate_row_counts.assert_not_called()

    def test_run_migration_returns_summary(self):
        """Test run_migration returns comprehensive summary."""
        mock_schema_extractor = Mock()
        mock_schema_converter = Mock()
        mock_data_transfer = Mock()
        mock_validator = Mock()
        mock_db2_conn = Mock()
        mock_pg_conn = Mock()
        mock_progress = Mock()
        mock_logger = Mock()
        
        mock_schema_extractor.extract_table_schema.return_value = {'columns': []}
        mock_schema_extractor.extract_primary_keys.return_value = []
        mock_schema_extractor.extract_foreign_keys.return_value = []
        mock_schema_extractor.extract_indexes.return_value = []
        
        mock_schema_converter.generate_create_table_ddl.return_value = 'CREATE...'
        mock_data_transfer.transfer_table.return_value = {'rows_transferred': 100, 'time_taken': 1.5}
        mock_validator.validate_row_counts.return_value = (True, 100, 100)
        
        migrator = Migrator(
            schema_extractor=mock_schema_extractor,
            schema_converter=mock_schema_converter,
            data_transfer=mock_data_transfer,
            validator=mock_validator,
            db2_connector=mock_db2_conn,
            pg_connector=mock_pg_conn,
            progress_tracker=mock_progress,
            logger=mock_logger
        )
        
        config = {'tables': ['test']}
        result = migrator.run_migration(config, mode='full')
        
        assert 'status' in result
        assert 'tables_migrated' in result
        assert 'total_time' in result
