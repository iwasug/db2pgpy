"""Tests for validator module."""

import pytest
from unittest.mock import Mock
from db2pgpy.validator import Validator


class TestValidator:
    """Test Validator functionality."""

    def test_validate_row_counts_returns_true_when_equal(self):
        """Test validate_row_counts returns True when counts match."""
        mock_db2 = Mock()
        mock_pg = Mock()
        
        mock_db2.get_table_row_count.return_value = 100
        mock_pg.get_table_row_count.return_value = 100
        
        validator = Validator(mock_db2, mock_pg)
        is_valid, db2_count, pg_count = validator.validate_row_counts('users')
        
        assert is_valid is True
        assert db2_count == 100
        assert pg_count == 100

    def test_validate_row_counts_returns_false_when_different(self):
        """Test validate_row_counts returns False when counts differ."""
        mock_db2 = Mock()
        mock_pg = Mock()
        
        mock_db2.get_table_row_count.return_value = 100
        mock_pg.get_table_row_count.return_value = 95
        
        validator = Validator(mock_db2, mock_pg)
        is_valid, db2_count, pg_count = validator.validate_row_counts('users')
        
        assert is_valid is False
        assert db2_count == 100
        assert pg_count == 95

    def test_validate_table_structure_compares_columns(self):
        """Test validate_table_structure compares column structure."""
        mock_db2 = Mock()
        mock_pg = Mock()
        
        db2_schema = [
            {'name': 'id', 'type': 'INTEGER', 'nullable': False},
            {'name': 'name', 'type': 'VARCHAR(100)', 'nullable': True}
        ]
        
        pg_schema = [
            {'name': 'id', 'type': 'INTEGER', 'nullable': False},
            {'name': 'name', 'type': 'VARCHAR(100)', 'nullable': True}
        ]
        
        mock_db2.get_table_schema.return_value = db2_schema
        mock_pg.get_table_schema.return_value = pg_schema
        
        validator = Validator(mock_db2, mock_pg)
        result = validator.validate_table_structure('users')
        
        assert result['is_valid'] is True
        assert result['column_count_match'] is True

    def test_validate_table_structure_detects_mismatch(self):
        """Test validate_table_structure detects schema mismatch."""
        mock_db2 = Mock()
        mock_pg = Mock()
        
        db2_schema = [
            {'name': 'id', 'type': 'INTEGER', 'nullable': False},
            {'name': 'name', 'type': 'VARCHAR(100)', 'nullable': True}
        ]
        
        pg_schema = [
            {'name': 'id', 'type': 'INTEGER', 'nullable': False}
        ]
        
        mock_db2.get_table_schema.return_value = db2_schema
        mock_pg.get_table_schema.return_value = pg_schema
        
        validator = Validator(mock_db2, mock_pg)
        result = validator.validate_table_structure('users')
        
        assert result['is_valid'] is False
        assert result['column_count_match'] is False

    def test_validate_sample_data_compares_rows(self):
        """Test validate_sample_data compares sample rows."""
        mock_db2 = Mock()
        mock_pg = Mock()
        
        sample_data = [
            {'id': 1, 'name': 'Alice'},
            {'id': 2, 'name': 'Bob'}
        ]
        
        mock_db2.fetch_sample_data.return_value = sample_data
        mock_pg.fetch_sample_data.return_value = sample_data
        
        validator = Validator(mock_db2, mock_pg)
        result = validator.validate_sample_data('users', sample_size=2)
        
        assert result['is_valid'] is True
        assert result['samples_matched'] == 2
        assert result['total_samples'] == 2

    def test_validate_sample_data_detects_differences(self):
        """Test validate_sample_data detects data differences."""
        mock_db2 = Mock()
        mock_pg = Mock()
        
        db2_data = [
            {'id': 1, 'name': 'Alice'},
            {'id': 2, 'name': 'Bob'}
        ]
        
        pg_data = [
            {'id': 1, 'name': 'Alice'},
            {'id': 2, 'name': 'Robert'}  # Different
        ]
        
        mock_db2.fetch_sample_data.return_value = db2_data
        mock_pg.fetch_sample_data.return_value = pg_data
        
        validator = Validator(mock_db2, mock_pg)
        result = validator.validate_sample_data('users', sample_size=2)
        
        assert result['is_valid'] is False
        assert result['samples_matched'] == 1
        assert result['total_samples'] == 2

    def test_generate_validation_report_creates_text(self):
        """Test generate_validation_report creates text report."""
        mock_db2 = Mock()
        mock_pg = Mock()
        
        validator = Validator(mock_db2, mock_pg)
        
        validation_results = {
            'users': {
                'row_count': {'is_valid': True, 'db2_count': 100, 'pg_count': 100},
                'structure': {'is_valid': True, 'column_count_match': True},
                'sample_data': {'is_valid': True, 'samples_matched': 10, 'total_samples': 10}
            }
        }
        
        report = validator.generate_validation_report(validation_results)
        
        assert 'users' in report
        assert 'row_count' in report.lower() or 'Row Count' in report
        assert 'PASS' in report or 'pass' in report

    def test_generate_validation_report_shows_failures(self):
        """Test generate_validation_report shows failures."""
        mock_db2 = Mock()
        mock_pg = Mock()
        
        validator = Validator(mock_db2, mock_pg)
        
        validation_results = {
            'orders': {
                'row_count': {'is_valid': False, 'db2_count': 100, 'pg_count': 95}
            }
        }
        
        report = validator.generate_validation_report(validation_results)
        
        assert 'orders' in report
        assert 'FAIL' in report or 'fail' in report or 'False' in report
