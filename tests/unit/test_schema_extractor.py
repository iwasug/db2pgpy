"""Tests for DB2 schema extractor."""

import pytest
from unittest.mock import Mock, MagicMock
from db2pgpy.extractors.schema import SchemaExtractor


class TestSchemaExtractor:
    """Test SchemaExtractor functionality."""

    def test_extract_table_schema_returns_columns_info(self):
        """Test extract_table_schema returns dict with columns, types, nullable, defaults."""
        # Mock DB2Connector
        mock_connector = Mock()
        mock_connector.execute_query.return_value = [
            {
                'COLNAME': 'id',
                'TYPENAME': 'INTEGER',
                'NULLS': 'N',
                'DEFAULT': None,
                'LENGTH': 4
            },
            {
                'COLNAME': 'name',
                'TYPENAME': 'VARCHAR',
                'NULLS': 'Y',
                'DEFAULT': None,
                'LENGTH': 100
            }
        ]

        extractor = SchemaExtractor(mock_connector)
        result = extractor.extract_table_schema('test_table')

        assert 'columns' in result
        assert len(result['columns']) == 2
        assert result['columns'][0]['name'] == 'id'
        assert result['columns'][0]['type'] == 'INTEGER'
        assert result['columns'][0]['nullable'] is False
        assert result['columns'][1]['name'] == 'name'
        assert result['columns'][1]['nullable'] is True

    def test_extract_primary_keys_returns_pk_columns(self):
        """Test extract_primary_keys returns list of PK column names."""
        mock_connector = Mock()
        mock_connector.execute_query.return_value = [
            {'COLNAME': 'id'},
            {'COLNAME': 'tenant_id'}
        ]

        extractor = SchemaExtractor(mock_connector)
        result = extractor.extract_primary_keys('test_table')

        assert result == ['id', 'tenant_id']
        mock_connector.execute_query.assert_called_once()

    def test_extract_foreign_keys_returns_fk_definitions(self):
        """Test extract_foreign_keys returns list of FK definitions."""
        mock_connector = Mock()
        mock_connector.execute_query.return_value = [
            {
                'CONSTNAME': 'fk_user',
                'COLNAME': 'user_id',
                'REFTABNAME': 'users',
                'REFCOLNAME': 'id'
            }
        ]

        extractor = SchemaExtractor(mock_connector)
        result = extractor.extract_foreign_keys('test_table')

        assert len(result) == 1
        assert result[0]['constraint_name'] == 'fk_user'
        assert result[0]['column'] == 'user_id'
        assert result[0]['referenced_table'] == 'users'
        assert result[0]['referenced_column'] == 'id'

    def test_extract_indexes_returns_index_definitions(self):
        """Test extract_indexes returns list of index definitions."""
        mock_connector = Mock()
        mock_connector.execute_query.return_value = [
            {
                'INDNAME': 'idx_email',
                'COLNAME': 'email',
                'UNIQUERULE': 'U',
                'COLSEQ': 1
            },
            {
                'INDNAME': 'idx_name_date',
                'COLNAME': 'name',
                'UNIQUERULE': 'D',
                'COLSEQ': 1
            },
            {
                'INDNAME': 'idx_name_date',
                'COLNAME': 'created_date',
                'UNIQUERULE': 'D',
                'COLSEQ': 2
            }
        ]

        extractor = SchemaExtractor(mock_connector)
        result = extractor.extract_indexes('test_table')

        assert len(result) == 2
        assert result[0]['name'] == 'idx_email'
        assert result[0]['columns'] == ['email']
        assert result[0]['unique'] is True
        assert result[1]['name'] == 'idx_name_date'
        assert result[1]['columns'] == ['name', 'created_date']
        assert result[1]['unique'] is False

    def test_schema_extractor_handles_empty_results(self):
        """Test schema extractor handles empty query results gracefully."""
        mock_connector = Mock()
        mock_connector.execute_query.return_value = []

        extractor = SchemaExtractor(mock_connector)
        
        schema = extractor.extract_table_schema('empty_table')
        assert schema['columns'] == []
        
        pks = extractor.extract_primary_keys('empty_table')
        assert pks == []
        
        fks = extractor.extract_foreign_keys('empty_table')
        assert fks == []
        
        indexes = extractor.extract_indexes('empty_table')
        assert indexes == []
