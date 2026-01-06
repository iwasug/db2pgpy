"""Unit tests for additional extractors (views, procedures, sequences)."""

import pytest
from unittest.mock import Mock, MagicMock
from db2pgpy.extractors.views import ViewExtractor
from db2pgpy.extractors.procedures import ProcedureExtractor
from db2pgpy.extractors.sequences import SequenceExtractor


class TestViewExtractor:
    """Test ViewExtractor class."""
    
    def test_extract_views_returns_list(self):
        """Test that extract_views returns a list of view definitions."""
        # Mock connector
        mock_connector = Mock()
        mock_connector.execute_query = MagicMock(return_value=[
            {
                'VIEWNAME': 'CUSTOMER_VIEW',
                'VIEWSCHEMA': 'MYSCHEMA',
                'DEFINITION': 'SELECT * FROM CUSTOMERS WHERE ACTIVE = 1'
            },
            {
                'VIEWNAME': 'ORDER_VIEW',
                'VIEWSCHEMA': 'MYSCHEMA',
                'DEFINITION': 'SELECT * FROM ORDERS WHERE STATUS = \'OPEN\''
            }
        ])
        
        extractor = ViewExtractor(mock_connector)
        
        # Mock the column extraction
        extractor._extract_view_columns = MagicMock(return_value=[
            {'name': 'ID', 'type': 'INTEGER', 'nullable': False},
            {'name': 'NAME', 'type': 'VARCHAR', 'nullable': True}
        ])
        
        views = extractor.extract_views()
        
        assert len(views) == 2
        assert views[0]['name'] == 'CUSTOMER_VIEW'
        assert views[0]['schema'] == 'MYSCHEMA'
        assert 'CUSTOMERS' in views[0]['definition']
        assert len(views[0]['columns']) == 2
        
    def test_extract_views_with_schema_filter(self):
        """Test extract_views with schema filter."""
        mock_connector = Mock()
        mock_connector.execute_query = MagicMock(return_value=[])
        
        extractor = ViewExtractor(mock_connector)
        views = extractor.extract_views(schema='TESTSCHEMA')
        
        # Verify the query was called with schema filter
        call_args = mock_connector.execute_query.call_args[0][0]
        assert 'TESTSCHEMA' in call_args
        assert isinstance(views, list)
    
    def test_extract_view_columns(self):
        """Test extracting columns for a specific view."""
        mock_connector = Mock()
        mock_connector.execute_query = MagicMock(return_value=[
            {'COLNAME': 'ID', 'TYPENAME': 'INTEGER', 'NULLS': 'N'},
            {'COLNAME': 'NAME', 'TYPENAME': 'VARCHAR', 'NULLS': 'Y'}
        ])
        
        extractor = ViewExtractor(mock_connector)
        columns = extractor._extract_view_columns('TEST_VIEW')
        
        assert len(columns) == 2
        assert columns[0]['name'] == 'ID'
        assert columns[0]['nullable'] is False
        assert columns[1]['nullable'] is True


class TestProcedureExtractor:
    """Test ProcedureExtractor class."""
    
    def test_extract_procedures_returns_list(self):
        """Test that extract_procedures returns a list of procedure definitions."""
        mock_connector = Mock()
        mock_connector.execute_query = MagicMock(return_value=[
            {
                'ROUTINENAME': 'GET_CUSTOMER',
                'ROUTINESCHEMA': 'MYSCHEMA',
                'TEXT': 'CREATE PROCEDURE GET_CUSTOMER...',
                'LANGUAGE': 'SQL',
                'SPECIFICNAME': 'GET_CUSTOMER_001'
            },
            {
                'ROUTINENAME': 'UPDATE_ORDER',
                'ROUTINESCHEMA': 'MYSCHEMA',
                'TEXT': 'CREATE PROCEDURE UPDATE_ORDER...',
                'LANGUAGE': 'SQL',
                'SPECIFICNAME': 'UPDATE_ORDER_001'
            }
        ])
        
        extractor = ProcedureExtractor(mock_connector)
        
        # Mock parameter extraction
        extractor._extract_procedure_params = MagicMock(return_value=[
            {'name': 'P_ID', 'type': 'INTEGER', 'direction': 'IN', 'ordinal': 1, 'length': None}
        ])
        
        procedures = extractor.extract_procedures()
        
        assert len(procedures) == 2
        assert procedures[0]['name'] == 'GET_CUSTOMER'
        assert procedures[0]['language'] == 'SQL'
        assert len(procedures[0]['parameters']) == 1
    
    def test_extract_procedures_with_schema_filter(self):
        """Test extract_procedures with schema filter."""
        mock_connector = Mock()
        mock_connector.execute_query = MagicMock(return_value=[])
        
        extractor = ProcedureExtractor(mock_connector)
        procedures = extractor.extract_procedures(schema='TESTSCHEMA')
        
        call_args = mock_connector.execute_query.call_args[0][0]
        assert 'TESTSCHEMA' in call_args
        assert isinstance(procedures, list)
    
    def test_extract_procedure_params(self):
        """Test extracting parameters for a procedure."""
        mock_connector = Mock()
        mock_connector.execute_query = MagicMock(return_value=[
            {'PARMNAME': 'P_ID', 'TYPENAME': 'INTEGER', 'ROWTYPE': 'I', 'ORDINAL': 1, 'LENGTH': None},
            {'PARMNAME': 'P_NAME', 'TYPENAME': 'VARCHAR', 'ROWTYPE': 'O', 'ORDINAL': 2, 'LENGTH': 100},
            {'PARMNAME': 'P_STATUS', 'TYPENAME': 'INTEGER', 'ROWTYPE': 'B', 'ORDINAL': 3, 'LENGTH': None}
        ])
        
        extractor = ProcedureExtractor(mock_connector)
        params = extractor._extract_procedure_params('TEST_PROC_001')
        
        assert len(params) == 3
        assert params[0]['direction'] == 'IN'
        assert params[1]['direction'] == 'OUT'
        assert params[2]['direction'] == 'INOUT'


class TestSequenceExtractor:
    """Test SequenceExtractor class."""
    
    def test_extract_sequences_returns_list(self):
        """Test that extract_sequences returns a list of sequence definitions."""
        mock_connector = Mock()
        mock_connector.execute_query = MagicMock(return_value=[
            {
                'SEQNAME': 'CUSTOMER_ID_SEQ',
                'SEQSCHEMA': 'MYSCHEMA',
                'START': 1,
                'INCREMENT': 1,
                'MINVALUE': 1,
                'MAXVALUE': 999999999,
                'CYCLE': 'N',
                'CACHE': 20,
                'ORDERED': 'N',
                'DATATYPEID': 4,
                'NEXTCACHEFIRSTVALUE': 100,
                'SEQTYPE': 'S'
            },
            {
                'SEQNAME': 'ORDER_ID_SEQ',
                'SEQSCHEMA': 'MYSCHEMA',
                'START': 1000,
                'INCREMENT': 10,
                'MINVALUE': 1000,
                'MAXVALUE': 9999999999,
                'CYCLE': 'Y',
                'CACHE': 50,
                'ORDERED': 'Y',
                'DATATYPEID': 8,
                'NEXTCACHEFIRSTVALUE': 1000,
                'SEQTYPE': 'S'
            }
        ])
        
        extractor = SequenceExtractor(mock_connector)
        sequences = extractor.extract_sequences()
        
        assert len(sequences) == 2
        assert sequences[0]['name'] == 'CUSTOMER_ID_SEQ'
        assert sequences[0]['start'] == 1
        assert sequences[0]['increment'] == 1
        assert sequences[0]['cycle'] is False
        assert sequences[1]['cycle'] is True
        assert sequences[1]['increment'] == 10
    
    def test_extract_sequences_with_schema_filter(self):
        """Test extract_sequences with schema filter."""
        mock_connector = Mock()
        mock_connector.execute_query = MagicMock(return_value=[])
        
        extractor = SequenceExtractor(mock_connector)
        sequences = extractor.extract_sequences(schema='TESTSCHEMA')
        
        call_args = mock_connector.execute_query.call_args[0][0]
        assert 'TESTSCHEMA' in call_args
        assert isinstance(sequences, list)
    
    def test_extract_sequences_parses_values_correctly(self):
        """Test that sequence values are parsed correctly."""
        mock_connector = Mock()
        mock_connector.execute_query = MagicMock(return_value=[
            {
                'SEQNAME': 'TEST_SEQ',
                'SEQSCHEMA': 'PUBLIC',
                'START': 100,
                'INCREMENT': 5,
                'MINVALUE': 1,
                'MAXVALUE': 10000,
                'CYCLE': 'Y',
                'CACHE': 10,
                'ORDERED': 'Y',
                'DATATYPEID': 4,
                'NEXTCACHEFIRSTVALUE': 105,
                'SEQTYPE': 'S'
            }
        ])
        
        extractor = SequenceExtractor(mock_connector)
        sequences = extractor.extract_sequences()
        
        seq = sequences[0]
        assert seq['start'] == 100
        assert seq['increment'] == 5
        assert seq['min_value'] == 1
        assert seq['max_value'] == 10000
        assert seq['cycle'] is True
        assert seq['ordered'] is True
        assert seq['cache'] == 10
