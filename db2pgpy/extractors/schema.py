"""DB2 schema extractor module."""

from typing import List, Dict, Any, Optional
from db2pgpy.connectors.db2 import DB2Connector


class SchemaExtractor:
    """Extract schema information from DB2 SYSCAT tables."""
    
    def __init__(self, connector: DB2Connector):
        """Initialize SchemaExtractor.
        
        Args:
            connector: DB2 database connector instance
        """
        self.connector = connector
    
    def extract_table_schema(self, table_name: str) -> Dict[str, Any]:
        """Extract table schema information from DB2.
        
        Args:
            table_name: Name of the table to extract
            
        Returns:
            Dictionary with columns, types, nullable, defaults
        """
        query = f"""
            SELECT COLNAME, TYPENAME, NULLS, DEFAULT, LENGTH, IDENTITY
            FROM SYSCAT.COLUMNS
            WHERE TABNAME = '{table_name}'
            ORDER BY COLNO
        """
        
        rows = self.connector.execute_query(query)
        
        columns = []
        for row in rows:
            columns.append({
                'name': row['COLNAME'],
                'type': row['TYPENAME'],
                'nullable': row['NULLS'] == 'Y',
                'default': row['DEFAULT'],
                'length': row.get('LENGTH'),
                'is_identity': row.get('IDENTITY') == 'Y'
            })
        
        return {'columns': columns}
    
    def extract_primary_keys(self, table_name: str) -> List[str]:
        """Extract primary key columns for a table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            List of primary key column names
        """
        query = f"""
            SELECT COLNAME
            FROM SYSCAT.KEYCOLUSE
            WHERE TABNAME = '{table_name}'
            AND CONSTNAME IN (
                SELECT CONSTNAME FROM SYSCAT.TABCONST
                WHERE TABNAME = '{table_name}' AND TYPE = 'P'
            )
            ORDER BY COLSEQ
        """
        
        rows = self.connector.execute_query(query)
        return [row['COLNAME'] for row in rows]
    
    def extract_foreign_keys(self, table_name: str) -> List[Dict[str, str]]:
        """Extract foreign key definitions for a table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            List of foreign key definitions
        """
        query = f"""
            SELECT 
                r.CONSTNAME, 
                fk.COLNAME,
                r.REFTABNAME,
                r.REFKEYNAME,
                pk.COLNAME as REFCOLNAME
            FROM SYSCAT.REFERENCES r
            JOIN SYSCAT.KEYCOLUSE fk 
                ON r.CONSTNAME = fk.CONSTNAME 
                AND r.TABNAME = fk.TABNAME
            JOIN SYSCAT.KEYCOLUSE pk 
                ON r.REFKEYNAME = pk.CONSTNAME 
                AND fk.COLSEQ = pk.COLSEQ
            WHERE r.TABNAME = '{table_name}'
            ORDER BY r.CONSTNAME, fk.COLSEQ
        """
        
        rows = self.connector.execute_query(query)
        
        foreign_keys = []
        for row in rows:
            foreign_keys.append({
                'constraint_name': row['CONSTNAME'],
                'column': row['COLNAME'],
                'referenced_table': row['REFTABNAME'],
                'referenced_column': row['REFCOLNAME']
            })
        
        return foreign_keys
    
    def extract_indexes(self, table_name: str) -> List[Dict[str, Any]]:
        """Extract index definitions for a table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            List of index definitions
        """
        query = f"""
            SELECT 
                i.INDNAME, i.UNIQUERULE, k.COLNAME, k.COLSEQ
            FROM SYSCAT.INDEXES i
            JOIN SYSCAT.INDEXCOLUSE k ON i.INDNAME = k.INDNAME
            WHERE i.TABNAME = '{table_name}'
            ORDER BY i.INDNAME, k.COLSEQ
        """
        
        rows = self.connector.execute_query(query)
        
        # Group columns by index name
        indexes_dict = {}
        for row in rows:
            index_name = row['INDNAME']
            if index_name not in indexes_dict:
                indexes_dict[index_name] = {
                    'name': index_name,
                    'columns': [],
                    'unique': row['UNIQUERULE'] == 'U'
                }
            indexes_dict[index_name]['columns'].append(row['COLNAME'])
        
        return list(indexes_dict.values())
