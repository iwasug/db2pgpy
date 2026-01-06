"""Schema converter module for generating PostgreSQL DDL."""

from typing import Dict, Any, List
from db2pgpy.converters.types import TypeConverter


class SchemaConverter:
    """Convert DB2 schema definitions to PostgreSQL DDL."""
    
    def __init__(self, type_converter: TypeConverter):
        """Initialize SchemaConverter.
        
        Args:
            type_converter: Type converter instance for DB2 to PostgreSQL type mapping
        """
        self.type_converter = type_converter
    
    def _convert_default_value(self, default_value: str) -> str:
        """
        Convert DB2 default value to PostgreSQL equivalent.
        
        Args:
            default_value: DB2 default value string
            
        Returns:
            PostgreSQL compatible default value
        """
        if not default_value:
            return default_value
        
        # Store original for case-insensitive comparison
        default_upper = default_value.upper().strip()
        
        # Convert CURRENT TIMESTAMP to CURRENT_TIMESTAMP
        default_value = default_value.replace('CURRENT TIMESTAMP', 'CURRENT_TIMESTAMP')
        
        # Convert CURRENT DATE to CURRENT_DATE
        default_value = default_value.replace('CURRENT DATE', 'CURRENT_DATE')
        
        # Convert CURRENT TIME to CURRENT_TIME
        default_value = default_value.replace('CURRENT TIME', 'CURRENT_TIME')
        
        # Convert CURRENT TIMEZONE to CURRENT_TIMESTAMP (closest equivalent)
        default_value = default_value.replace('CURRENT TIMEZONE', 'CURRENT_TIMESTAMP')
        
        # Convert USER to CURRENT_USER
        if default_upper == 'USER':
            default_value = 'CURRENT_USER'
        
        # Convert SESSION_USER (DB2) to SESSION_USER (PostgreSQL) - already compatible
        # Convert CURRENT SCHEMA to CURRENT_SCHEMA - add parentheses
        if default_upper == 'CURRENT SCHEMA':
            default_value = 'CURRENT_SCHEMA()'
        
        # Handle NULL explicitly
        if default_upper == 'NULL':
            default_value = 'NULL'
        
        return default_value
    
    def generate_create_table_ddl(self, table_name: str, schema_info: Dict[str, Any]) -> str:
        """Generate PostgreSQL CREATE TABLE statement.
        
        Args:
            table_name: Name of the table
            schema_info: Schema information with columns list
            
        Returns:
            PostgreSQL CREATE TABLE DDL statement
        """
        columns = schema_info.get('columns', [])
        
        column_defs = []
        for col in columns:
            # Check if column is IDENTITY (auto-increment)
            if col.get('is_identity', False):
                # Convert IDENTITY columns to SERIAL/BIGSERIAL
                pg_type = self.type_converter.convert(col['type'])
                if 'BIGINT' in pg_type.upper():
                    pg_type = 'BIGSERIAL'
                elif 'INTEGER' in pg_type.upper() or 'INT' in pg_type.upper():
                    pg_type = 'SERIAL'
                elif 'SMALLINT' in pg_type.upper():
                    pg_type = 'SMALLSERIAL'
                else:
                    # Fallback: use GENERATED ALWAYS AS IDENTITY
                    pg_type = f"{pg_type} GENERATED ALWAYS AS IDENTITY"
                
                col_def = f'"{col["name"]}" {pg_type}'
            else:
                # Convert type normally
                pg_type = self.type_converter.convert(col['type'])
                
                # Build column definition with quoted column name to preserve case
                col_def = f'"{col["name"]}" {pg_type}'
                
                # Add DEFAULT clause (only for non-identity columns)
                if col.get('default') is not None:
                    pg_default = self._convert_default_value(col['default'])
                    col_def += f" DEFAULT {pg_default}"
            
            # Add NOT NULL constraint
            if not col.get('nullable', True):
                col_def += " NOT NULL"
            
            column_defs.append(col_def)
        
        columns_str = ',\n    '.join(column_defs) if column_defs else ''
        
        # Quote table name to preserve case
        if columns_str:
            return f'CREATE TABLE "{table_name}" (\n    {columns_str}\n);'
        else:
            return f'CREATE TABLE "{table_name}" ();'
    
    def generate_primary_key_ddl(self, table_name: str, pk_columns: List[str]) -> str:
        """Generate ALTER TABLE ADD PRIMARY KEY statement.
        
        Args:
            table_name: Name of the table
            pk_columns: List of primary key column names
            
        Returns:
            PostgreSQL ALTER TABLE DDL statement
        """
        # Quote each column name to preserve case
        columns_str = ', '.join([f'"{col}"' for col in pk_columns])
        return f'ALTER TABLE "{table_name}" ADD PRIMARY KEY ({columns_str});'
    
    def generate_foreign_key_ddl(self, table_name: str, fk_info: Dict[str, str]) -> str:
        """Generate ALTER TABLE ADD FOREIGN KEY statement.
        
        Args:
            table_name: Name of the table
            fk_info: Foreign key information
            
        Returns:
            PostgreSQL ALTER TABLE DDL statement
        """
        constraint_name = fk_info['constraint_name']
        column = fk_info['column']
        ref_table = fk_info['referenced_table']
        ref_column = fk_info['referenced_column']
        
        return (f'ALTER TABLE "{table_name}" '
                f'ADD CONSTRAINT "{constraint_name}" '
                f'FOREIGN KEY ("{column}") '
                f'REFERENCES "{ref_table}"("{ref_column}");')
    
    def generate_index_ddl(self, index_info: Dict[str, Any]) -> str:
        """Generate CREATE INDEX statement.
        
        Args:
            index_info: Index information with name, table, columns, unique flag
            
        Returns:
            PostgreSQL CREATE INDEX DDL statement
        """
        index_name = index_info['name']
        table_name = index_info['table']
        columns = index_info['columns']
        is_unique = index_info.get('unique', False)
        
        unique_clause = 'UNIQUE ' if is_unique else ''
        # Quote each column name to preserve case
        columns_str = ', '.join([f'"{col}"' for col in columns])
        
        return f'CREATE {unique_clause}INDEX "{index_name}" ON "{table_name}" ({columns_str});'
