"""Unit tests for PL/SQL to PL/pgSQL converter."""

import pytest
import tempfile
import shutil
from pathlib import Path
from db2pgpy.converters.plsql import PLSQLConverter


class TestPLSQLConverter:
    """Test PLSQLConverter class."""
    
    @pytest.fixture
    def temp_output_dir(self):
        """Create temporary directory for test outputs."""
        temp_dir = tempfile.mkdtemp()
        yield temp_dir
        shutil.rmtree(temp_dir)
    
    @pytest.fixture
    def converter(self, temp_output_dir):
        """Create converter instance with temp output dir."""
        return PLSQLConverter(output_dir=temp_output_dir)
    
    def test_convert_simple_procedure(self, converter):
        """Test conversion of a simple procedure."""
        procedure_def = {
            'name': 'SIMPLE_PROC',
            'definition': '''
                CREATE PROCEDURE SIMPLE_PROC (IN p_id INTEGER)
                LANGUAGE SQL
                BEGIN ATOMIC
                    SET v_name = 'Test';
                END
            '''
        }
        
        converted, warnings = converter.convert_procedure(procedure_def)
        
        assert 'LANGUAGE plpgsql' in converted
        assert 'BEGIN' in converted
        assert 'v_name :=' in converted
        assert '-- Converted from DB2 procedure: SIMPLE_PROC' in converted
    
    def test_convert_procedure_with_assignment(self, converter):
        """Test conversion of SET statements to := assignment."""
        procedure_def = {
            'name': 'TEST_ASSIGNMENT',
            'definition': 'SET my_var = 100;'
        }
        
        converted, warnings = converter.convert_procedure(procedure_def)
        
        assert 'my_var :=' in converted
        assert 'SET my_var =' not in converted
    
    def test_convert_procedure_with_data_types(self, converter):
        """Test conversion of data types."""
        procedure_def = {
            'name': 'TYPE_TEST',
            'definition': '''
                DECLARE v_amount DECIMAL(10,2);
                DECLARE v_name VARCHAR(100);
                DECLARE v_id INTEGER;
            '''
        }
        
        converted, warnings = converter.convert_procedure(procedure_def)
        
        assert 'NUMERIC(10,2)' in converted
        assert 'VARCHAR(100)' in converted
        assert 'INTEGER' in converted
    
    def test_convert_procedure_detects_complex_cursor(self, converter):
        """Test detection of complex cursor usage."""
        procedure_def = {
            'name': 'CURSOR_PROC',
            'definition': '''
                DECLARE my_cursor CURSOR FOR SELECT * FROM table1;
                OPEN my_cursor;
                FETCH my_cursor INTO v_id;
            '''
        }
        
        converted, warnings = converter.convert_procedure(procedure_def)
        
        assert len(warnings) > 0
        assert any('cursor' in w.lower() for w in warnings)
        assert any('Manual review required' in w for w in warnings)
    
    def test_convert_procedure_detects_dynamic_sql(self, converter):
        """Test detection of dynamic SQL."""
        procedure_def = {
            'name': 'DYNAMIC_PROC',
            'definition': 'EXECUTE IMMEDIATE sql_statement;'
        }
        
        converted, warnings = converter.convert_procedure(procedure_def)
        
        assert len(warnings) > 0
        assert any('EXECUTE IMMEDIATE' in w for w in warnings)
    
    def test_convert_function(self, converter):
        """Test conversion of a function."""
        function_def = {
            'name': 'CALC_TOTAL',
            'definition': '''
                CREATE FUNCTION CALC_TOTAL(p_amount INTEGER)
                RETURNS INTEGER
                LANGUAGE SQL
                BEGIN ATOMIC
                    RETURN p_amount * 2;
                END
            '''
        }
        
        converted, warnings = converter.convert_function(function_def)
        
        assert 'LANGUAGE plpgsql' in converted
        assert 'RETURNS INTEGER' in converted
        assert '-- Converted from DB2 function: CALC_TOTAL' in converted
    
    def test_convert_procedure_with_call_statement(self, converter):
        """Test conversion of CALL to PERFORM."""
        procedure_def = {
            'name': 'CALL_TEST',
            'definition': 'CALL other_procedure(p_id);'
        }
        
        converted, warnings = converter.convert_procedure(procedure_def)
        
        assert 'PERFORM other_procedure' in converted
        assert 'CALL' not in converted or 'CALL' in '-- Converted'
    
    def test_convert_procedure_with_current_timestamp(self, converter):
        """Test conversion of DB2 date/time functions."""
        procedure_def = {
            'name': 'DATE_TEST',
            'definition': '''
                SET v_date = CURRENT DATE;
                SET v_time = CURRENT TIME;
                SET v_timestamp = CURRENT TIMESTAMP;
            '''
        }
        
        converted, warnings = converter.convert_procedure(procedure_def)
        
        assert 'CURRENT_DATE' in converted
        assert 'CURRENT_TIME' in converted
        assert 'CURRENT_TIMESTAMP' in converted
    
    def test_convert_procedure_with_value_function(self, converter):
        """Test conversion of VALUE to COALESCE."""
        procedure_def = {
            'name': 'VALUE_TEST',
            'definition': 'SET v_result = VALUE(col1, col2);'
        }
        
        converted, warnings = converter.convert_procedure(procedure_def)
        
        assert 'COALESCE(col1, col2)' in converted
    
    def test_save_failed_conversion(self, converter, temp_output_dir):
        """Test that failed conversions are saved to files."""
        procedure_def = {
            'name': 'COMPLEX_PROC',
            'definition': 'CURSOR my_cursor;'  # Complex feature
        }
        
        converted, warnings = converter.convert_procedure(procedure_def)
        
        # Check that file was created
        output_file = Path(temp_output_dir) / 'COMPLEX_PROC_conversion.sql'
        assert output_file.exists()
        
        content = output_file.read_text()
        assert 'CONVERSION FAILED' in content
        assert 'ORIGINAL DB2 CODE' in content
        assert 'ATTEMPTED CONVERSION' in content
    
    def test_get_conversion_summary(self, converter):
        """Test conversion summary statistics."""
        results = [
            ('converted1', []),  # Clean
            ('converted2', ['warning1']),  # With warnings
            ('converted3', []),  # Clean
            ('converted4', ['warning1', 'warning2']),  # With warnings
        ]
        
        summary = converter.get_conversion_summary(results)
        
        assert summary['total'] == 4
        assert summary['clean_conversions'] == 2
        assert summary['conversions_with_warnings'] == 2
        assert summary['success_rate'] == 50.0
    
    def test_get_conversion_summary_empty(self, converter):
        """Test summary with no conversions."""
        summary = converter.get_conversion_summary([])
        
        assert summary['total'] == 0
        assert summary['success_rate'] == 0
    
    def test_convert_procedure_with_no_definition(self, converter):
        """Test handling of procedure with no definition."""
        procedure_def = {
            'name': 'EMPTY_PROC',
            'definition': ''
        }
        
        converted, warnings = converter.convert_procedure(procedure_def)
        
        assert converted == ''
        assert len(warnings) > 0
        assert 'No definition found' in warnings[0]
    
    def test_detect_multiple_complex_features(self, converter):
        """Test detection of multiple complex features."""
        procedure_def = {
            'name': 'VERY_COMPLEX',
            'definition': '''
                DECLARE my_cursor CURSOR;
                WHILE condition DO
                    EXECUTE IMMEDIATE sql_text;
                END WHILE;
            '''
        }
        
        converted, warnings = converter.convert_procedure(procedure_def)
        
        # Should detect cursor, while loop, and dynamic SQL
        assert len(warnings) >= 3
        assert any('cursor' in w.lower() for w in warnings)
        assert any('WHILE loop' in w for w in warnings)
        assert any('EXECUTE IMMEDIATE' in w for w in warnings)
