"""Integration tests for full migration flow."""

import pytest
import tempfile
import shutil
from pathlib import Path
from unittest.mock import Mock, MagicMock, patch
from db2pgpy.migrator import Migrator
from db2pgpy.extractors.schema import SchemaExtractor
from db2pgpy.converters.schema import SchemaConverter
from db2pgpy.converters.types import TypeConverter
from db2pgpy.data_transfer import DataTransfer
from db2pgpy.validator import Validator
from db2pgpy.progress import ProgressTracker
from db2pgpy.connectors.db2 import DB2Connector
from db2pgpy.connectors.postgres import PostgresConnector
from db2pgpy.logger import setup_logger


class TestMigrationFlow:
    """Test complete migration workflow with mocked databases."""
    
    @pytest.fixture
    def temp_dir(self):
        """Create temporary directory for test artifacts."""
        temp_dir = tempfile.mkdtemp()
        yield temp_dir
        shutil.rmtree(temp_dir)
    
    @pytest.fixture
    def mock_db2_connector(self):
        """Create mock DB2 connector."""
        mock = Mock(spec=DB2Connector)
        mock.execute_query = MagicMock(return_value=[])
        mock.fetch_table_data = MagicMock(return_value=[])
        mock.get_table_row_count = MagicMock(return_value=100)
        mock.get_table_schema = MagicMock(return_value=[
            {'name': 'ID', 'type': 'INTEGER'},
            {'name': 'NAME', 'type': 'VARCHAR'}
        ])
        return mock
    
    @pytest.fixture
    def mock_pg_connector(self):
        """Create mock PostgreSQL connector."""
        mock = Mock(spec=PostgresConnector)
        mock.execute_ddl = MagicMock()
        mock.bulk_insert = MagicMock()
        mock.get_table_row_count = MagicMock(return_value=100)
        return mock
    
    @pytest.fixture
    def mock_logger(self):
        """Create mock logger."""
        return setup_logger("test_migration", level="DEBUG")
    
    @pytest.fixture
    def migrator_components(self, mock_db2_connector, mock_pg_connector, temp_dir, mock_logger):
        """Setup migrator with all components."""
        # Setup schema extractor
        schema_extractor = SchemaExtractor(mock_db2_connector)
        
        # Mock the extractor methods
        schema_extractor.extract_table_schema = MagicMock(return_value={
            'columns': [
                {'name': 'ID', 'type': 'INTEGER', 'nullable': False, 'default': None, 'length': None},
                {'name': 'NAME', 'type': 'VARCHAR', 'nullable': True, 'default': None, 'length': 100}
            ]
        })
        schema_extractor.extract_primary_keys = MagicMock(return_value=['ID'])
        schema_extractor.extract_foreign_keys = MagicMock(return_value=[])
        schema_extractor.extract_indexes = MagicMock(return_value=[])
        
        # Setup converters
        type_converter = TypeConverter()
        schema_converter = SchemaConverter(type_converter)
        
        # Setup data transfer
        data_transfer = DataTransfer(mock_db2_connector, mock_pg_connector, batch_size=100)
        
        # Mock data transfer method
        data_transfer.transfer_table = MagicMock()
        
        # Setup validator
        validator = Validator(mock_db2_connector, mock_pg_connector)
        
        # Mock validator methods
        validator.validate_row_counts = MagicMock(return_value=(True, 100, 100))
        
        # Setup progress tracker
        state_file = Path(temp_dir) / 'test_state.json'
        progress_tracker = ProgressTracker(str(state_file))
        
        # Create migrator
        migrator = Migrator(
            schema_extractor=schema_extractor,
            schema_converter=schema_converter,
            data_transfer=data_transfer,
            validator=validator,
            db2_connector=mock_db2_connector,
            pg_connector=mock_pg_connector,
            progress_tracker=progress_tracker,
            logger=mock_logger
        )
        
        return {
            'migrator': migrator,
            'schema_extractor': schema_extractor,
            'schema_converter': schema_converter,
            'data_transfer': data_transfer,
            'validator': validator,
            'progress_tracker': progress_tracker,
            'db2_connector': mock_db2_connector,
            'pg_connector': mock_pg_connector
        }
    
    def test_full_migration_flow(self, migrator_components):
        """Test complete migration flow with schema and data."""
        migrator = migrator_components['migrator']
        pg_connector = migrator_components['pg_connector']
        data_transfer = migrator_components['data_transfer']
        
        config = {
            'tables': ['CUSTOMERS', 'ORDERS'],
            'validate': True
        }
        
        result = migrator.run_migration(config, mode='full')
        
        # Verify migration completed
        assert result['status'] == 'completed'
        assert result['tables_migrated'] == 2
        
        # Verify DDL was executed
        assert pg_connector.execute_ddl.called
        assert pg_connector.execute_ddl.call_count >= 4  # 2 tables + 2 PKs
        
        # Verify data transfer was called
        assert data_transfer.transfer_table.called
        assert data_transfer.transfer_table.call_count == 2
    
    def test_schema_only_migration(self, migrator_components):
        """Test schema-only migration mode."""
        migrator = migrator_components['migrator']
        pg_connector = migrator_components['pg_connector']
        data_transfer = migrator_components['data_transfer']
        
        config = {
            'tables': ['CUSTOMERS'],
            'validate': False
        }
        
        result = migrator.run_migration(config, mode='schema_only')
        
        # Verify schema was created
        assert result['status'] == 'completed'
        assert result['tables_migrated'] == 1
        assert pg_connector.execute_ddl.called
        
        # Verify data transfer was NOT called
        assert not data_transfer.transfer_table.called
    
    def test_data_only_migration(self, migrator_components):
        """Test data-only migration mode."""
        migrator = migrator_components['migrator']
        data_transfer = migrator_components['data_transfer']
        
        config = {
            'tables': ['CUSTOMERS'],
            'validate': True
        }
        
        result = migrator.run_migration(config, mode='data_only')
        
        # Verify data was transferred
        assert result['status'] == 'completed'
        assert data_transfer.transfer_table.called
    
    def test_resume_capability(self, migrator_components, temp_dir):
        """Test resume from saved state."""
        progress_tracker = migrator_components['progress_tracker']
        
        # Simulate partial migration
        progress_tracker.set_phase('data_transfer')
        progress_tracker.update_table_progress('CUSTOMERS', 50, 100)
        progress_tracker.mark_completed('schema')
        
        # Verify state was saved
        state_file = Path(temp_dir) / 'test_state.json'
        assert state_file.exists()
        
        # Load new progress tracker from same file
        new_tracker = ProgressTracker(str(state_file))
        
        # Verify state was loaded
        assert new_tracker.get_phase() == 'data_transfer'
        assert new_tracker.is_completed('schema')
        table_progress = new_tracker.get_table_progress('CUSTOMERS')
        assert table_progress is not None
        assert table_progress['rows_migrated'] == 50
        assert table_progress['total_rows'] == 100
    
    def test_validation_phase(self, migrator_components):
        """Test validation after migration."""
        migrator = migrator_components['migrator']
        validator = migrator_components['validator']
        
        config = {
            'tables': ['CUSTOMERS'],
            'validate': True
        }
        
        result = migrator.run_migration(config, mode='full')
        
        # Verify validation was called
        assert validator.validate_row_counts.called
        assert result['status'] == 'completed'
    
    def test_migration_without_validation(self, migrator_components):
        """Test migration with validation disabled."""
        migrator = migrator_components['migrator']
        validator = migrator_components['validator']
        
        config = {
            'tables': ['CUSTOMERS'],
            'validate': False
        }
        
        result = migrator.run_migration(config, mode='full')
        
        # Verify validation was NOT called
        assert not validator.validate_row_counts.called
        assert result['status'] == 'completed'
    
    def test_multiple_tables_migration(self, migrator_components):
        """Test migration of multiple tables."""
        migrator = migrator_components['migrator']
        data_transfer = migrator_components['data_transfer']
        
        config = {
            'tables': ['CUSTOMERS', 'ORDERS', 'PRODUCTS'],
            'validate': True
        }
        
        result = migrator.run_migration(config, mode='full')
        
        # Verify all tables were migrated
        assert result['status'] == 'completed'
        assert result['tables_migrated'] == 3
        assert data_transfer.transfer_table.call_count == 3
    
    def test_migration_time_tracking(self, migrator_components):
        """Test that migration tracks execution time."""
        migrator = migrator_components['migrator']
        
        config = {
            'tables': ['CUSTOMERS'],
            'validate': False
        }
        
        result = migrator.run_migration(config, mode='full')
        
        # Verify time was tracked
        assert 'total_time' in result
        assert result['total_time'] >= 0
    
    def test_progress_summary(self, migrator_components):
        """Test progress tracker summary."""
        progress_tracker = migrator_components['progress_tracker']
        
        # Add some progress data
        progress_tracker.update_table_progress('TABLE1', 100, 100)
        progress_tracker.update_table_progress('TABLE2', 50, 200)
        progress_tracker.mark_completed('schema')
        
        summary = progress_tracker.get_summary()
        
        # Verify summary
        assert summary['total_tables'] == 2
        assert summary['completed_tables'] == 1  # Only TABLE1 is complete
        assert summary['total_rows'] == 300
        assert summary['migrated_rows'] == 150
        assert summary['overall_percentage'] == 50.0
        assert 'schema' in summary['completed_phases']


class TestComponentIntegration:
    """Test integration between components."""
    
    def test_extractor_to_converter_flow(self):
        """Test data flow from extractor to converter."""
        # Mock DB2 connector
        mock_db2 = Mock()
        mock_db2.execute_query = MagicMock(return_value=[
            {'COLNAME': 'ID', 'TYPENAME': 'INTEGER', 'NULLS': 'N', 'DEFAULT': None, 'LENGTH': None},
            {'COLNAME': 'NAME', 'TYPENAME': 'VARCHAR', 'NULLS': 'Y', 'DEFAULT': None, 'LENGTH': 100}
        ])
        
        # Extract schema
        extractor = SchemaExtractor(mock_db2)
        schema_info = extractor.extract_table_schema('TEST_TABLE')
        
        # Convert schema
        type_converter = TypeConverter()
        schema_converter = SchemaConverter(type_converter)
        ddl = schema_converter.generate_create_table_ddl('TEST_TABLE', schema_info)
        
        # Verify DDL was generated
        assert 'CREATE TABLE' in ddl
        assert 'TEST_TABLE' in ddl
        assert 'ID' in ddl
        assert 'NAME' in ddl
    
    def test_validator_integration(self):
        """Test validator with mocked connectors."""
        # Mock connectors
        mock_db2 = Mock()
        mock_db2.get_table_row_count = MagicMock(return_value=100)
        
        mock_pg = Mock()
        mock_pg.get_table_row_count = MagicMock(return_value=100)
        
        # Test validation
        validator = Validator(mock_db2, mock_pg)
        is_valid, db2_count, pg_count = validator.validate_row_counts('TEST_TABLE')
        
        # Verify results
        assert is_valid is True
        assert db2_count == 100
        assert pg_count == 100
    
    def test_validation_report_generation(self):
        """Test validation report generation."""
        # Mock connectors
        mock_db2 = Mock()
        mock_pg = Mock()
        
        validator = Validator(mock_db2, mock_pg)
        
        # Sample results
        results = {
            'TABLE1': {
                'row_count': {
                    'is_valid': True,
                    'db2_count': 100,
                    'pg_count': 100
                }
            },
            'TABLE2': {
                'row_count': {
                    'is_valid': False,
                    'db2_count': 200,
                    'pg_count': 195
                }
            }
        }
        
        report = validator.generate_validation_report(results)
        
        # Verify report content
        assert 'VALIDATION REPORT' in report
        assert 'TABLE1' in report
        assert 'TABLE2' in report
        assert 'PASS' in report
        assert 'FAIL' in report
