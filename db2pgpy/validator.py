"""Validator module for post-migration validation."""

from typing import Dict, Any, Tuple, List
from db2pgpy.connectors.db2 import DB2Connector
from db2pgpy.connectors.postgres import PostgresConnector


class Validator:
    """Validate data migration between DB2 and PostgreSQL."""
    
    def __init__(self, db2_connector: DB2Connector, pg_connector: PostgresConnector, 
                 db2_schema: str = "MAXIMO", pg_schema: str = "public"):
        """Initialize Validator.
        
        Args:
            db2_connector: DB2 database connector
            pg_connector: PostgreSQL database connector
            db2_schema: DB2 schema name (default: MAXIMO)
            pg_schema: PostgreSQL schema name (default: public)
        """
        self.db2_connector = db2_connector
        self.pg_connector = pg_connector
        self.db2_schema = db2_schema
        self.pg_schema = pg_schema
    
    def validate_row_counts(self, table_name: str) -> Tuple[bool, int, int]:
        """Validate row counts match between DB2 and PostgreSQL.
        
        Args:
            table_name: Name of the table to validate
            
        Returns:
            Tuple of (is_valid, db2_count, pg_count)
        """
        db2_count = self.db2_connector.get_table_row_count(table_name, self.db2_schema)
        pg_count = self.pg_connector.get_table_row_count(table_name, self.pg_schema)
        
        is_valid = db2_count == pg_count
        
        return (is_valid, db2_count, pg_count)
    
    def validate_table_structure(self, table_name: str) -> Dict[str, Any]:
        """Validate table structure matches between DB2 and PostgreSQL.
        
        Args:
            table_name: Name of the table to validate
            
        Returns:
            Dictionary with validation results
        """
        db2_schema = self.db2_connector.get_table_schema(table_name)
        pg_schema = self.pg_connector.get_table_schema(table_name)
        
        column_count_match = len(db2_schema) == len(pg_schema)
        
        # Basic validation: check column count and names
        is_valid = column_count_match
        if column_count_match:
            db2_names = [col['name'] for col in db2_schema]
            pg_names = [col['name'] for col in pg_schema]
            is_valid = db2_names == pg_names
        
        return {
            'is_valid': is_valid,
            'column_count_match': column_count_match,
            'db2_columns': len(db2_schema),
            'pg_columns': len(pg_schema)
        }
    
    def validate_sample_data(self, table_name: str, sample_size: int = 100) -> Dict[str, Any]:
        """Validate sample data matches between DB2 and PostgreSQL.
        
        Args:
            table_name: Name of the table to validate
            sample_size: Number of rows to sample
            
        Returns:
            Dictionary with validation results
        """
        db2_data = self.db2_connector.fetch_sample_data(table_name, sample_size)
        pg_data = self.pg_connector.fetch_sample_data(table_name, sample_size)
        
        samples_matched = 0
        total_samples = min(len(db2_data), len(pg_data))
        
        for i in range(total_samples):
            if db2_data[i] == pg_data[i]:
                samples_matched += 1
        
        is_valid = samples_matched == total_samples
        
        return {
            'is_valid': is_valid,
            'samples_matched': samples_matched,
            'total_samples': total_samples
        }
    
    def generate_validation_report(self, results: Dict[str, Dict[str, Any]]) -> str:
        """Generate text validation report.
        
        Args:
            results: Validation results dictionary
            
        Returns:
            Text report string
        """
        report_lines = []
        report_lines.append("=" * 60)
        report_lines.append("VALIDATION REPORT")
        report_lines.append("=" * 60)
        report_lines.append("")
        
        for table_name, table_results in results.items():
            report_lines.append(f"Table: {table_name}")
            report_lines.append("-" * 40)
            
            for validation_type, validation_result in table_results.items():
                is_valid = validation_result.get('is_valid', False)
                status = "PASS" if is_valid else "FAIL"
                
                report_lines.append(f"  {validation_type}: {status}")
                
                # Add details
                if validation_type == 'row_count':
                    db2_count = validation_result.get('db2_count', 0)
                    pg_count = validation_result.get('pg_count', 0)
                    report_lines.append(f"    DB2: {db2_count}, PostgreSQL: {pg_count}")
                
            report_lines.append("")
        
        return "\n".join(report_lines)
