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
            # Convert type
            pg_type = self.type_converter.convert_type(
                col['type'], 
                col.get('length')
            )
            
            # Build column definition
            col_def = f"{col['name']} {pg_type}"
            
            # Add NOT NULL constraint
            if not col.get('nullable', True):
                col_def += " NOT NULL"
            
            # Add DEFAULT clause
            if col.get('default') is not None:
                col_def += f" DEFAULT {col['default']}"
            
            column_defs.append(col_def)
        
        columns_str = ',\n    '.join(column_defs) if column_defs else ''
        
        if columns_str:
            return f"CREATE TABLE {table_name} (\n    {columns_str}\n);"
        else:
            return f"CREATE TABLE {table_name} ();"
    
    def generate_primary_key_ddl(self, table_name: str, pk_columns: List[str]) -> str:
        """Generate ALTER TABLE ADD PRIMARY KEY statement.
        
        Args:
            table_name: Name of the table
            pk_columns: List of primary key column names
            
        Returns:
            PostgreSQL ALTER TABLE DDL statement
        """
        columns_str = ', '.join(pk_columns)
        return f"ALTER TABLE {table_name} ADD PRIMARY KEY ({columns_str});"
    
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
        
        return (f"ALTER TABLE {table_name} "
                f"ADD CONSTRAINT {constraint_name} "
                f"FOREIGN KEY ({column}) "
                f"REFERENCES {ref_table}({ref_column});")
    
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
        columns_str = ', '.join(columns)
        
        return f"CREATE {unique_clause}INDEX {index_name} ON {table_name} ({columns_str});"
